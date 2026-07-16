use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::domain::{Evidence, ExecutorKind, ExecutorMode, Principal, Task, TaskStatus};
use crate::error::{MambaError, Result};
use crate::executor::{ExecutionRequest, TerminalExecutor};

#[derive(Clone)]
pub struct WorkerOptions {
    pub server_url: String,
    pub token: String,
    pub executor: ExecutorKind,
    pub workspace: PathBuf,
    pub model: Option<String>,
    pub command: Option<PathBuf>,
    pub task_id: Option<String>,
    pub timeout_seconds: u64,
    pub data_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerOutcomeStatus {
    Idle,
    Planned,
    Crashed,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkerOutcome {
    pub status: WorkerOutcomeStatus,
    pub principal: String,
    pub task_id: Option<String>,
    pub run_id: Option<String>,
    pub summary: String,
    pub log_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
struct InboxItem {
    flow_id: String,
    flow_title: String,
    task: Task,
    #[serde(default)]
    blocked_by: Vec<String>,
}

pub struct RemoteWorker {
    options: WorkerOptions,
    control_plane: ControlPlaneClient,
}

impl RemoteWorker {
    pub fn new(options: WorkerOptions) -> Result<Self> {
        if options.token.trim().is_empty() {
            return Err(MambaError::Validation(
                "MAMBA_TOKEN is required for a remote worker".into(),
            ));
        }
        if !options.workspace.is_dir() {
            return Err(MambaError::InvalidWorkspace(options.workspace.clone()));
        }
        if options.timeout_seconds == 0 {
            return Err(MambaError::Validation(
                "worker timeout must be greater than zero".into(),
            ));
        }
        fs::create_dir_all(options.data_dir.join("worker-runs"))?;
        let control_plane = ControlPlaneClient::new(&options.server_url, &options.token)?;
        Ok(Self {
            options,
            control_plane,
        })
    }

    pub async fn run_once(&self) -> Result<WorkerOutcome> {
        let principal = self.control_plane.me().await?;
        let inbox = self.control_plane.inbox().await?;
        let Some(item) = select_task(&inbox, &principal, self.options.task_id.as_deref()) else {
            return Ok(WorkerOutcome {
                status: WorkerOutcomeStatus::Idle,
                principal: principal.name,
                task_id: self.options.task_id.clone(),
                run_id: None,
                summary: "no eligible unplanned task in remote inbox".into(),
                log_path: None,
            });
        };
        let mut task = item.task.clone();
        if task.status == TaskStatus::Assigned {
            task = self.control_plane.task_action(&task.id, "accept").await?;
        }
        if task.status == TaskStatus::Accepted {
            task = self.control_plane.task_action(&task.id, "start").await?;
        }
        if task.status != TaskStatus::InProgress {
            return Err(MambaError::InvalidTransition(format!(
                "remote worker cannot plan task {} while it is {:?}",
                task.id, task.status
            )));
        }

        let run_id = format!("WRUN-{}", Uuid::new_v4().simple());
        let log_path = self
            .options
            .data_dir
            .join("worker-runs")
            .join(&task.id)
            .join(format!("{run_id}.json"));
        self.control_plane
            .heartbeat(
                &task.id,
                Some(format!(
                    "{} read-only planning flight {} took off",
                    self.options.executor, run_id
                )),
            )
            .await?;
        let prompt = worker_prompt(&principal, item, &task);
        let result = TerminalExecutor::run(ExecutionRequest {
            kind: self.options.executor.clone(),
            command: self.options.command.clone(),
            workspace: self.options.workspace.clone(),
            model: self.options.model.clone(),
            mode: ExecutorMode::Plan,
            prompt,
            output_schema: None,
            timeout_seconds: self.options.timeout_seconds,
            log_path: log_path.clone(),
        })
        .await;

        match result {
            Ok(output) => {
                let summary = truncate(&output.summary, 4_000);
                let uri = plan_evidence_uri(&principal, &task);
                self.control_plane
                    .evidence(&task.id, "agent_plan", &uri, &summary)
                    .await?;
                self.control_plane
                    .heartbeat(
                        &task.id,
                        Some(format!("read-only planning flight {run_id} landed")),
                    )
                    .await?;
                Ok(WorkerOutcome {
                    status: WorkerOutcomeStatus::Planned,
                    principal: principal.name,
                    task_id: Some(task.id),
                    run_id: Some(run_id),
                    summary,
                    log_path: Some(log_path),
                })
            }
            Err(error) => {
                let summary = truncate(&error.to_string(), 1_000);
                let uri = format!("worker://{}/{}/crash/{run_id}", principal.id, task.id);
                self.control_plane
                    .evidence(&task.id, "worker_blackbox", &uri, &summary)
                    .await?;
                self.control_plane
                    .block(
                        &task.id,
                        &format!("remote planning flight crashed: {summary}"),
                    )
                    .await?;
                Ok(WorkerOutcome {
                    status: WorkerOutcomeStatus::Crashed,
                    principal: principal.name,
                    task_id: Some(task.id),
                    run_id: Some(run_id),
                    summary,
                    log_path: Some(log_path),
                })
            }
        }
    }
}

#[derive(Clone)]
struct ControlPlaneClient {
    client: Client,
    api_base: Url,
    token: String,
}

impl ControlPlaneClient {
    fn new(server_url: &str, token: &str) -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut api_base = Url::parse(server_url.trim())
            .map_err(|_| MambaError::Validation("invalid MambaFlow server URL".into()))?;
        if !matches!(api_base.scheme(), "http" | "https") {
            return Err(MambaError::Validation(
                "MambaFlow server URL must use http or https".into(),
            ));
        }
        if !api_base.username().is_empty() || api_base.password().is_some() {
            return Err(MambaError::Validation(
                "MambaFlow server URL must not contain credentials; use MAMBA_TOKEN".into(),
            ));
        }
        api_base.set_query(None);
        api_base.set_fragment(None);
        {
            let mut segments = api_base.path_segments_mut().map_err(|_| {
                MambaError::Validation("MambaFlow server URL cannot be used as an API base".into())
            })?;
            segments.pop_if_empty().push("api").push("v1");
        }
        if !api_base.path().ends_with('/') {
            api_base.set_path(&format!("{}/", api_base.path()));
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .user_agent(concat!("MambaFlow-Worker/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| {
                MambaError::ExternalConnector("could not initialize remote worker client".into())
            })?;
        Ok(Self {
            client,
            api_base,
            token: token.to_string(),
        })
    }

    async fn me(&self) -> Result<Principal> {
        self.request(Method::GET, &["me"], None).await
    }

    async fn inbox(&self) -> Result<Vec<InboxItem>> {
        self.request(Method::GET, &["inbox"], None).await
    }

    async fn task_action(&self, task_id: &str, action: &str) -> Result<Task> {
        self.request(Method::POST, &["tasks", task_id, action], Some(json!({})))
            .await
    }

    async fn heartbeat(&self, task_id: &str, note: Option<String>) -> Result<Task> {
        self.request(
            Method::POST,
            &["tasks", task_id, "heartbeat"],
            Some(json!({ "note": note })),
        )
        .await
    }

    async fn block(&self, task_id: &str, reason: &str) -> Result<Task> {
        self.request(
            Method::POST,
            &["tasks", task_id, "block"],
            Some(json!({ "reason": reason })),
        )
        .await
    }

    async fn evidence(
        &self,
        task_id: &str,
        kind: &str,
        uri: &str,
        summary: &str,
    ) -> Result<Evidence> {
        self.request(
            Method::POST,
            &["tasks", task_id, "evidence"],
            Some(json!({ "kind": kind, "uri": uri, "summary": summary })),
        )
        .await
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<Value>,
    ) -> Result<T> {
        let mut url = self.api_base.clone();
        {
            let mut path = url.path_segments_mut().map_err(|_| {
                MambaError::Validation("MambaFlow server URL cannot form an endpoint".into())
            })?;
            path.pop_if_empty();
            for segment in segments {
                path.push(segment);
            }
        }
        let mut request = self.client.request(method, url).bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|error| {
            MambaError::ExternalConnector(format!("control plane request failed: {error}"))
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(control_plane_error(status, response).await);
        }
        response.json().await.map_err(|_| {
            MambaError::ExternalConnector("control plane returned an invalid JSON response".into())
        })
    }
}

fn select_task<'a>(
    inbox: &'a [InboxItem],
    principal: &Principal,
    requested_task: Option<&str>,
) -> Option<&'a InboxItem> {
    inbox
        .iter()
        .filter(|item| {
            requested_task.is_none_or(|requested| {
                item.task.id == requested || item.task.key.eq_ignore_ascii_case(requested)
            })
        })
        .filter(|item| item.blocked_by.is_empty())
        .filter(|item| {
            matches!(
                item.task.status,
                TaskStatus::Assigned | TaskStatus::Accepted
            ) || (requested_task.is_some() && item.task.status == TaskStatus::InProgress)
        })
        .filter(|item| {
            !item
                .task
                .evidence
                .iter()
                .any(|evidence| evidence.uri == plan_evidence_uri(principal, &item.task))
        })
        .min_by_key(|item| match item.task.status {
            TaskStatus::InProgress => 0,
            TaskStatus::Accepted => 1,
            TaskStatus::Assigned => 2,
            _ => 3,
        })
}

fn worker_prompt(principal: &Principal, item: &InboxItem, task: &Task) -> String {
    format!(
        "MambaFlow remote PASS for a read-only planning flight.\n\
         Worker principal: {} ({})\n\
         Flow: {} - {}\n\
         Task: {} - {}\n\
         Description: {}\n\
         Acceptance criteria:\n- {}\n\n\
         Inspect the workspace read-only. Do not modify files. Return a concrete implementation \
         plan, affected files or documents, verification steps, risks, and questions for the human owner.",
        principal.name,
        principal.id,
        item.flow_id,
        item.flow_title,
        task.id,
        task.title,
        task.description,
        task.acceptance_criteria.join("\n- ")
    )
}

fn plan_evidence_uri(principal: &Principal, task: &Task) -> String {
    format!("worker://{}/{}/plan", principal.id, task.id)
}

async fn control_plane_error(status: StatusCode, response: reqwest::Response) -> MambaError {
    let message = response
        .json::<Value>()
        .await
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .map(|message| truncate(&message, 300))
        .unwrap_or_else(|| "request was rejected".into());
    MambaError::ExternalConnector(format!(
        "control plane returned HTTP {}: {message}",
        status.as_u16()
    ))
}

fn truncate(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::extract::{Path, State};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use chrono::{Duration as ChronoDuration, Utc};
    use tempfile::tempdir;

    use super::*;
    use crate::domain::{Assignment, AssignmentTarget, Estimate, PrincipalKind, TargetKind};

    #[derive(Clone)]
    struct MockState {
        principal: Principal,
        task: Arc<Mutex<Task>>,
        actions: Arc<Mutex<Vec<String>>>,
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_plans_one_remote_task_and_returns_evidence() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let principal = test_principal();
        let task = test_task(&principal);
        let state = MockState {
            principal: principal.clone(),
            task: Arc::new(Mutex::new(task)),
            actions: Arc::new(Mutex::new(Vec::new())),
        };
        let router = Router::new()
            .route("/api/v1/me", get(mock_me))
            .route("/api/v1/inbox", get(mock_inbox))
            .route("/api/v1/tasks/{task}/{action}", post(mock_task_action))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let executable = directory.path().join("fake-codex");
        fs::write(
            &executable,
            r#"#!/bin/sh
result=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then
    shift
    result="$1"
  fi
  shift
done
printf '%s' 'Inspect src/gateway.rs, add contract tests, and verify routing.' > "$result"
printf '%s\n' '{"thread_id":"remote-thread"}'
"#,
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();

        let worker = RemoteWorker::new(WorkerOptions {
            server_url: format!("http://{address}"),
            token: "worker-token".into(),
            executor: ExecutorKind::Codex,
            workspace: directory.path().to_path_buf(),
            model: None,
            command: Some(executable),
            task_id: None,
            timeout_seconds: 10,
            data_dir: directory.path().join("data"),
        })
        .unwrap();
        let outcome = worker.run_once().await.unwrap();
        assert_eq!(outcome.status, WorkerOutcomeStatus::Planned);
        assert!(outcome.summary.contains("contract tests"));
        assert!(outcome.log_path.unwrap().is_file());
        assert_eq!(
            state.actions.lock().unwrap().as_slice(),
            ["accept", "start", "heartbeat", "evidence", "heartbeat"]
        );
        let task = state.task.lock().unwrap();
        assert_eq!(task.status, TaskStatus::InProgress);
        assert_eq!(task.evidence.len(), 1);
        assert_eq!(task.evidence[0].kind, "agent_plan");
        server.abort();
    }

    async fn mock_me(State(state): State<MockState>, headers: HeaderMap) -> Json<Principal> {
        assert_eq!(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer worker-token")
        );
        Json(state.principal)
    }

    async fn mock_inbox(State(state): State<MockState>) -> Json<Value> {
        Json(json!([{
            "flow_id": "FLOW-1",
            "flow_title": "Ship gateway",
            "task": state.task.lock().unwrap().clone()
        }]))
    }

    async fn mock_task_action(
        State(state): State<MockState>,
        Path((_task_id, action)): Path<(String, String)>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        state.actions.lock().unwrap().push(action.clone());
        let mut task = state.task.lock().unwrap();
        match action.as_str() {
            "accept" => task.status = TaskStatus::Accepted,
            "start" => task.status = TaskStatus::InProgress,
            "evidence" => task.evidence.push(Evidence {
                id: "EVD-1".into(),
                kind: body["kind"].as_str().unwrap().into(),
                uri: body["uri"].as_str().unwrap().into(),
                summary: body["summary"].as_str().unwrap().into(),
                created_by: state.principal.name.clone(),
                created_at: Utc::now(),
            }),
            "block" => {
                task.status = TaskStatus::Blocked;
                task.blocker = body["reason"].as_str().map(str::to_string);
            }
            "heartbeat" => task.last_heartbeat = Some(Utc::now()),
            _ => panic!("unexpected action: {action}"),
        }
        Json(if action == "evidence" {
            serde_json::to_value(task.evidence.last().unwrap()).unwrap()
        } else {
            serde_json::to_value(&*task).unwrap()
        })
    }

    fn test_principal() -> Principal {
        Principal {
            id: "AGT-1".into(),
            name: "Remote Codex".into(),
            kind: PrincipalKind::Agent,
            team_id: Some("TEAM-1".into()),
            owner_id: Some("HUM-1".into()),
            capabilities: vec!["backend".into()],
            capacity_percent: 100,
            executor: None,
            active: true,
            created_at: Utc::now(),
        }
    }

    fn test_task(principal: &Principal) -> Task {
        let now = Utc::now();
        Task {
            id: "TSK-1".into(),
            key: "gateway".into(),
            title: "Implement gateway".into(),
            description: "Route model requests".into(),
            required_capabilities: vec!["backend".into()],
            depends_on: vec![],
            requires_human: false,
            acceptance_criteria: vec!["routing is tested".into()],
            assignment: Some(Assignment {
                owner: AssignmentTarget {
                    kind: TargetKind::Agent,
                    id: principal.id.clone(),
                    name: principal.name.clone(),
                },
                copilots: vec![],
                score: 1.0,
                rationale: vec![],
            }),
            estimate: Estimate {
                effort_hours: 1.0,
                p50_hours: 1.0,
                p80_hours: 2.0,
                confidence: "medium".into(),
                rationale: vec![],
                earliest_start: now,
                p50_finish: now + ChronoDuration::hours(1),
                p80_finish: now + ChronoDuration::hours(2),
            },
            status: TaskStatus::Assigned,
            blocker: None,
            last_heartbeat: None,
            evidence: vec![],
            external_artifacts: vec![],
        }
    }
}
