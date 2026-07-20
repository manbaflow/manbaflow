use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::capability::CapabilityAdapter;
use crate::domain::{
    CapabilityPack, Evidence, ExecutorKind, ExecutorMode, FailureClass, FlightLease,
    FlightLeaseStatus, FlowMessage, FuelUsage, MessageInboxItem, Principal, RemoteFlightReport,
    Task, TaskStatus,
};
use crate::error::{MambaError, Result};
use crate::executor::{ExecutionRequest, TerminalExecutor};
use crate::worktree::{IsolatedWorktree, WorktreeArtifact, sha256_file};

#[derive(Clone)]
pub struct WorkerOptions {
    pub server_url: String,
    pub token: String,
    pub executor: ExecutorKind,
    pub mode: ExecutorMode,
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
    Executed,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingFlightResult {
    landed: bool,
    report: RemoteFlightReport,
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
        match self.options.mode {
            ExecutorMode::Plan => self.run_plan_once().await,
            ExecutorMode::Execute => self.run_execute_once().await,
        }
    }

    async fn run_plan_once(&self) -> Result<WorkerOutcome> {
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
        let pending_messages = self.control_plane.messages().await?;
        let thread = self.control_plane.flow_messages(&item.flow_id).await?;
        let instructions = task_message_context(&thread, item);
        for message in relevant_inbox_messages(&pending_messages, item)
            .filter(|message| message.needs_acknowledgement())
        {
            self.control_plane.ack_message(&message.message.id).await?;
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
        let prompt = worker_prompt(&principal, item, &task, &instructions);
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

    async fn run_execute_once(&self) -> Result<WorkerOutcome> {
        let principal = self.control_plane.me().await?;
        let leases = self.control_plane.flight_leases().await?;
        let Some(selected_lease) = select_lease(
            &leases,
            &self.options.executor,
            self.options.task_id.as_deref(),
        ) else {
            return Ok(WorkerOutcome {
                status: WorkerOutcomeStatus::Idle,
                principal: principal.name,
                task_id: self.options.task_id.clone(),
                run_id: None,
                summary: "no authorized write flight lease for this worker and executor".into(),
                log_path: None,
            });
        };
        let inbox = self.control_plane.inbox().await?;
        let mut lease = selected_lease.clone();
        let item = inbox
            .iter()
            .find(|item| item.task.id == lease.task_id)
            .ok_or_else(|| {
                MambaError::InvalidTransition(format!(
                    "leased task {} is not in the worker inbox",
                    lease.task_id
                ))
            })?;
        if !item.blocked_by.is_empty() {
            return Err(MambaError::InvalidTransition(format!(
                "leased task {} still has incomplete dependencies",
                lease.task_id
            )));
        }
        let pending_messages = self.control_plane.messages().await?;
        let thread = self.control_plane.flow_messages(&item.flow_id).await?;
        let instructions = task_message_context(&thread, item);
        for message in relevant_inbox_messages(&pending_messages, item)
            .filter(|message| message.needs_acknowledgement())
        {
            self.control_plane.ack_message(&message.message.id).await?;
        }

        let run_id = match lease.status {
            FlightLeaseStatus::Authorized => {
                let run_id = format!("WRUN-{}", Uuid::new_v4().simple());
                lease = self.control_plane.claim_flight(&lease.id, &run_id).await?;
                run_id
            }
            FlightLeaseStatus::Active => lease.run_id.clone().ok_or_else(|| {
                MambaError::InvalidTransition(format!(
                    "active flight lease {} has no run ID",
                    lease.id
                ))
            })?,
            _ => unreachable!("select_lease only returns open leases"),
        };
        let run_dir = self
            .options
            .data_dir
            .join("worker-runs")
            .join(&lease.task_id)
            .join(&run_id);
        fs::create_dir_all(&run_dir)?;
        let log_path = run_dir.join("blackbox.json");
        let patch_path = run_dir.join("changes.patch");
        let pending_path = run_dir.join("flight-report.json");
        if pending_path.is_file() {
            let pending: PendingFlightResult = serde_json::from_slice(&fs::read(&pending_path)?)?;
            let finished = self
                .control_plane
                .finish_flight(&lease.id, pending.landed, &pending.report)
                .await?;
            return Ok(WorkerOutcome {
                status: if finished.status == FlightLeaseStatus::Landed {
                    WorkerOutcomeStatus::Executed
                } else {
                    WorkerOutcomeStatus::Crashed
                },
                principal: principal.name,
                task_id: Some(lease.task_id),
                run_id: Some(run_id),
                summary: pending.report.summary,
                log_path: log_path.is_file().then_some(log_path),
            });
        }
        let worktree_root = self
            .options
            .data_dir
            .join("worker-worktrees")
            .join(format!("{}-{run_id}", lease.id));
        let started_at = Utc::now();
        let prompt = execute_prompt(&principal, item, &lease, &instructions);
        let context_bytes = prompt.len() as u64;

        let (result, artifact) = {
            let worktree = if worktree_root.exists() {
                IsolatedWorktree::resume(&self.options.workspace, worktree_root)
            } else {
                IsolatedWorktree::create(&self.options.workspace, worktree_root)
            };
            match worktree {
                Ok(mut worktree) => {
                    let execution = TerminalExecutor::run(ExecutionRequest {
                        kind: self.options.executor.clone(),
                        command: self.options.command.clone(),
                        workspace: worktree.workspace().to_path_buf(),
                        model: self.options.model.clone(),
                        mode: ExecutorMode::Execute,
                        prompt,
                        output_schema: None,
                        timeout_seconds: self.options.timeout_seconds,
                        log_path: log_path.clone(),
                    })
                    .await;
                    let collected = worktree.collect(&patch_path);
                    let cleanup = worktree.cleanup();
                    let artifact = collected.and_then(|artifact| cleanup.map(|_| artifact));
                    (execution, artifact)
                }
                Err(error) => {
                    write_setup_blackbox(&log_path, &run_id, &error)?;
                    (
                        Err(error),
                        Ok(WorktreeArtifact {
                            base_revision: "unavailable".into(),
                            changed_files: vec![],
                            patch_path: None,
                            patch_sha256: None,
                        }),
                    )
                }
            }
        };

        let (landed, summary, artifact, cost_usd, failure_class) = match (result, artifact) {
            (Ok(output), Ok(artifact)) => {
                let suffix = match artifact.changed_files.len() {
                    0 => "no file changes".to_string(),
                    1 => "1 changed file captured in the isolated patch".to_string(),
                    count => format!("{count} changed files captured in the isolated patch"),
                };
                (
                    true,
                    truncate(&format!("{}; {suffix}", output.summary), 4_000),
                    artifact,
                    output.cost_usd,
                    None,
                )
            }
            (Err(error), Ok(artifact)) => {
                let failure = classify_worker_error(&error);
                (
                    false,
                    truncate(&error.to_string(), 4_000),
                    artifact,
                    None,
                    Some(failure),
                )
            }
            (Ok(output), Err(error)) => {
                let failure = classify_worker_error(&error);
                (
                    false,
                    truncate(&format!("artifact collection failed: {error}"), 4_000),
                    empty_artifact(),
                    output.cost_usd,
                    Some(failure),
                )
            }
            (Err(execution), Err(collection)) => {
                let failure = classify_worker_error(&execution);
                (
                    false,
                    truncate(
                        &format!("{execution}; artifact collection failed: {collection}"),
                        4_000,
                    ),
                    empty_artifact(),
                    None,
                    Some(failure),
                )
            }
        };
        if !log_path.is_file() {
            write_setup_blackbox(&log_path, &run_id, &MambaError::Validation(summary.clone()))?;
        }
        let finished_at = Utc::now();
        let report = RemoteFlightReport {
            run_id: run_id.clone(),
            executor: self.options.executor.clone(),
            summary: summary.clone(),
            base_revision: artifact.base_revision,
            changed_files: artifact.changed_files,
            patch_sha256: artifact.patch_sha256,
            log_sha256: sha256_file(&log_path)?,
            started_at,
            finished_at,
            fuel: FuelUsage {
                duration_seconds: finished_at
                    .signed_duration_since(started_at)
                    .num_seconds()
                    .max(0) as u64,
                context_bytes,
                tokens: None,
                tool_calls: None,
                cost_usd,
            },
            failure_class,
            budget_exhaustions: Vec::new(),
            deliverables: Vec::new(),
            contract_violations: Vec::new(),
        };
        fs::write(
            &pending_path,
            serde_json::to_vec_pretty(&PendingFlightResult {
                landed,
                report: report.clone(),
            })?,
        )?;
        let finished = self
            .control_plane
            .finish_flight(&lease.id, landed, &report)
            .await?;
        Ok(WorkerOutcome {
            status: if finished.status == FlightLeaseStatus::Landed {
                WorkerOutcomeStatus::Executed
            } else {
                WorkerOutcomeStatus::Crashed
            },
            principal: principal.name,
            task_id: Some(lease.task_id),
            run_id: Some(run_id),
            summary,
            log_path: Some(log_path),
        })
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

    async fn messages(&self) -> Result<Vec<MessageInboxItem>> {
        self.request(Method::GET, &["messages"], None).await
    }

    async fn flow_messages(&self, flow_id: &str) -> Result<Vec<FlowMessage>> {
        self.request(Method::GET, &["flows", flow_id, "messages"], None)
            .await
    }

    async fn ack_message(&self, message_id: &str) -> Result<crate::domain::FlowMessage> {
        self.request(
            Method::POST,
            &["messages", message_id, "ack"],
            Some(json!({})),
        )
        .await
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

    async fn flight_leases(&self) -> Result<Vec<FlightLease>> {
        self.request(Method::GET, &["flight-leases"], None).await
    }

    async fn claim_flight(&self, lease_id: &str, run_id: &str) -> Result<FlightLease> {
        self.request(
            Method::POST,
            &["flight-leases", lease_id, "claim"],
            Some(json!({ "run_id": run_id })),
        )
        .await
    }

    async fn finish_flight(
        &self,
        lease_id: &str,
        landed: bool,
        report: &RemoteFlightReport,
    ) -> Result<FlightLease> {
        self.request(
            Method::POST,
            &["flight-leases", lease_id, "finish"],
            Some(json!({ "landed": landed, "report": report })),
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

fn select_lease<'a>(
    leases: &'a [FlightLease],
    executor: &ExecutorKind,
    requested_task: Option<&str>,
) -> Option<&'a FlightLease> {
    leases
        .iter()
        .filter(|lease| {
            matches!(
                lease.status,
                FlightLeaseStatus::Authorized | FlightLeaseStatus::Active
            )
        })
        .filter(|lease| &lease.executor == executor)
        .filter(|lease| requested_task.is_none_or(|task| lease.task_id == task))
        .min_by_key(|lease| {
            (
                if lease.status == FlightLeaseStatus::Active {
                    0
                } else {
                    1
                },
                lease.issued_at,
            )
        })
}

fn worker_prompt(
    principal: &Principal,
    item: &InboxItem,
    task: &Task,
    instructions: &str,
) -> String {
    format!(
        "MambaFlow remote PASS for a read-only planning flight.\n\
         Worker principal: {} ({})\n\
         Flow: {} - {}\n\
         Task: {} - {}\n\
         Description: {}\n\
         Explicit Flow instructions:\n{}\n\n\
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
        instructions,
        task.acceptance_criteria.join("\n- ")
    )
}

fn execute_prompt(
    principal: &Principal,
    item: &InboxItem,
    lease: &FlightLease,
    instructions: &str,
) -> String {
    let pack = lease
        .manifest
        .as_ref()
        .map_or(CapabilityPack::General, |manifest| manifest.capability_pack);
    let output_contract = lease.manifest.as_ref().map(|manifest| {
        if manifest.output_contract.allowed_extensions.is_empty() {
            "task-scoped files".to_string()
        } else {
            format!(
                "files with these extensions only: {}",
                manifest.output_contract.allowed_extensions.join(", ")
            )
        }
    });
    let execution_contract = CapabilityAdapter::execution_directive(pack);
    format!(
        "MambaFlow remote PASS for a Human-authorized {pack:?} flight.\n\
         Flight lease: {}\n\
         Authorized by: {}\n\
         Worker principal: {} ({})\n\
         Flow: {} - {}\n\
         Task: {} - {}\n\
         Description: {}\n\
         Explicit Flow instructions:\n{}\n\n\
         Acceptance criteria:\n- {}\n\n\
         Output contract: {}\n\n\
         {} Implement only this task. Report deliverables, verification, and remaining risks for \
         Human review.",
        lease.id,
        lease.authorized_by,
        principal.name,
        principal.id,
        item.flow_id,
        item.flow_title,
        item.task.id,
        item.task.title,
        item.task.description,
        instructions,
        item.task.acceptance_criteria.join("\n- "),
        output_contract.unwrap_or_else(|| "task-scoped files".into()),
        execution_contract,
    )
}

fn relevant_inbox_messages<'a>(
    messages: &'a [MessageInboxItem],
    item: &'a InboxItem,
) -> impl Iterator<Item = &'a MessageInboxItem> {
    messages.iter().filter(|message| {
        message.message.flow_id == item.flow_id
            && message
                .message
                .task_id
                .as_deref()
                .is_none_or(|task_id| task_id == item.task.id)
    })
}

fn task_message_context(messages: &[FlowMessage], item: &InboxItem) -> String {
    let lines = messages
        .iter()
        .filter(|message| {
            message.flow_id == item.flow_id
                && message
                    .task_id
                    .as_deref()
                    .is_none_or(|task_id| task_id == item.task.id)
        })
        .map(|message| {
            format!(
                "- [{}] {}: {}",
                message.kind, message.sender_name, message.body
            )
        })
        .collect::<Vec<_>>();
    if lines.is_empty() {
        "- No explicit instructions".into()
    } else {
        lines.join("\n")
    }
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

fn write_setup_blackbox(path: &std::path::Path, run_id: &str, error: &MambaError) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(&json!({
            "run_id": run_id,
            "phase": "isolated_worktree_setup",
            "error": error.to_string(),
            "at": Utc::now(),
        }))?,
    )?;
    Ok(())
}

fn empty_artifact() -> WorktreeArtifact {
    WorktreeArtifact {
        base_revision: "unavailable".into(),
        changed_files: vec![],
        patch_path: None,
        patch_sha256: None,
    }
}

fn classify_worker_error(error: &MambaError) -> FailureClass {
    match error {
        MambaError::ExecutorTimeout(_) => FailureClass::Timeout,
        MambaError::PermissionDenied(_) => FailureClass::Permission,
        MambaError::InvalidWorkspace(_) | MambaError::Io(_) => FailureClass::Resource,
        MambaError::Validation(_) | MambaError::InvalidExecutorOutput(_) | MambaError::Json(_) => {
            FailureClass::Validation
        }
        MambaError::ExecutorUnavailable(_)
        | MambaError::ExecutorFailed { .. }
        | MambaError::ExternalConnector(_) => FailureClass::Tool,
        _ => FailureClass::Unknown,
    }
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
        lease: Arc<Mutex<Option<FlightLease>>>,
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
            lease: Arc::new(Mutex::new(None)),
        };
        let router = Router::new()
            .route("/api/v1/me", get(mock_me))
            .route("/api/v1/inbox", get(mock_inbox))
            .route("/api/v1/messages", get(mock_messages))
            .route("/api/v1/flows/{flow}/messages", get(mock_flow_messages))
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
            mode: ExecutorMode::Plan,
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

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_executes_in_isolated_worktree_and_finishes_lease() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let repository = directory.path().join("repo");
        fs::create_dir_all(&repository).unwrap();
        std::process::Command::new("git")
            .arg("-C")
            .arg(&repository)
            .args(["init", "-q"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .arg("-C")
            .arg(&repository)
            .args(["config", "user.email", "test@example.com"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .arg("-C")
            .arg(&repository)
            .args(["config", "user.name", "Test"])
            .status()
            .unwrap();
        fs::write(repository.join("README.md"), "base\n").unwrap();
        std::process::Command::new("git")
            .arg("-C")
            .arg(&repository)
            .args(["add", "README.md"])
            .status()
            .unwrap();
        std::process::Command::new("git")
            .arg("-C")
            .arg(&repository)
            .args(["commit", "-qm", "base"])
            .status()
            .unwrap();

        let principal = test_principal();
        let mut task = test_task(&principal);
        task.status = TaskStatus::Accepted;
        let now = Utc::now();
        let lease = FlightLease {
            id: "LEASE-1".into(),
            flow_id: "FLOW-1".into(),
            task_id: task.id.clone(),
            principal_id: principal.id.clone(),
            principal_name: principal.name.clone(),
            authorized_by: "Engineer".into(),
            executor: ExecutorKind::Codex,
            status: FlightLeaseStatus::Authorized,
            issued_at: now,
            expires_at: now + ChronoDuration::hours(1),
            claimed_at: None,
            finished_at: None,
            run_id: None,
            report: None,
            manifest: None,
            parent_lease_id: None,
            root_lease_id: Some("LEASE-1".into()),
            attempt: 1,
        };
        let state = MockState {
            principal: principal.clone(),
            task: Arc::new(Mutex::new(task)),
            actions: Arc::new(Mutex::new(Vec::new())),
            lease: Arc::new(Mutex::new(Some(lease))),
        };
        let router = Router::new()
            .route("/api/v1/me", get(mock_me))
            .route("/api/v1/inbox", get(mock_inbox))
            .route("/api/v1/messages", get(mock_messages))
            .route("/api/v1/flows/{flow}/messages", get(mock_flow_messages))
            .route("/api/v1/flight-leases", get(mock_flight_leases))
            .route(
                "/api/v1/flight-leases/{lease}/claim",
                post(mock_claim_flight),
            )
            .route(
                "/api/v1/flight-leases/{lease}/finish",
                post(mock_finish_flight),
            )
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
printf '%s\n' 'isolated change' > generated.txt
printf '%s' 'Implemented the authorized task and ran checks.' > "$result"
printf '%s\n' '{"thread_id":"execute-thread"}'
"#,
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();

        let data_dir = directory.path().join("worker-data");
        let worker = RemoteWorker::new(WorkerOptions {
            server_url: format!("http://{address}"),
            token: "worker-token".into(),
            executor: ExecutorKind::Codex,
            mode: ExecutorMode::Execute,
            workspace: repository.clone(),
            model: None,
            command: Some(executable),
            task_id: None,
            timeout_seconds: 10,
            data_dir: data_dir.clone(),
        })
        .unwrap();
        let outcome = worker.run_once().await.unwrap();

        assert_eq!(outcome.status, WorkerOutcomeStatus::Executed);
        assert!(!repository.join("generated.txt").exists());
        assert!(
            outcome
                .log_path
                .as_ref()
                .unwrap()
                .parent()
                .unwrap()
                .join("changes.patch")
                .is_file()
        );
        assert_eq!(
            state.actions.lock().unwrap().as_slice(),
            ["claim", "finish"]
        );
        let lease = state.lease.lock().unwrap().clone().unwrap();
        assert_eq!(lease.status, FlightLeaseStatus::Landed);
        let report = lease.report.unwrap();
        assert_eq!(report.changed_files, ["generated.txt"]);
        assert!(report.patch_sha256.is_some());
        assert_eq!(report.log_sha256.len(), 64);
        assert_eq!(
            fs::read_dir(data_dir.join("worker-worktrees"))
                .unwrap()
                .count(),
            0
        );

        {
            let mut lease = state.lease.lock().unwrap();
            let lease = lease.as_mut().unwrap();
            lease.status = FlightLeaseStatus::Active;
            lease.finished_at = None;
            lease.report = None;
        }
        state.actions.lock().unwrap().clear();
        let resumed = worker.run_once().await.unwrap();
        assert_eq!(resumed.status, WorkerOutcomeStatus::Executed);
        assert_eq!(state.actions.lock().unwrap().as_slice(), ["finish"]);
        assert_eq!(
            state.lease.lock().unwrap().as_ref().unwrap().status,
            FlightLeaseStatus::Landed
        );
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

    async fn mock_messages() -> Json<Vec<MessageInboxItem>> {
        Json(Vec::new())
    }

    async fn mock_flow_messages() -> Json<Vec<FlowMessage>> {
        Json(Vec::new())
    }

    async fn mock_flight_leases(State(state): State<MockState>) -> Json<Vec<FlightLease>> {
        Json(state.lease.lock().unwrap().clone().into_iter().collect())
    }

    async fn mock_claim_flight(
        State(state): State<MockState>,
        Path(lease_id): Path<String>,
        Json(body): Json<Value>,
    ) -> Json<FlightLease> {
        state.actions.lock().unwrap().push("claim".into());
        let mut guard = state.lease.lock().unwrap();
        let lease = guard.as_mut().unwrap();
        assert_eq!(lease.id, lease_id);
        lease.status = FlightLeaseStatus::Active;
        lease.run_id = body["run_id"].as_str().map(str::to_string);
        lease.claimed_at = Some(Utc::now());
        Json(lease.clone())
    }

    #[derive(Deserialize)]
    struct MockFinishInput {
        landed: bool,
        report: RemoteFlightReport,
    }

    async fn mock_finish_flight(
        State(state): State<MockState>,
        Path(lease_id): Path<String>,
        Json(body): Json<MockFinishInput>,
    ) -> Json<FlightLease> {
        state.actions.lock().unwrap().push("finish".into());
        let mut guard = state.lease.lock().unwrap();
        let lease = guard.as_mut().unwrap();
        assert_eq!(lease.id, lease_id);
        lease.status = if body.landed {
            FlightLeaseStatus::Landed
        } else {
            FlightLeaseStatus::Crashed
        };
        lease.finished_at = Some(Utc::now());
        lease.report = Some(body.report);
        Json(lease.clone())
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
            directory_username: None,
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
