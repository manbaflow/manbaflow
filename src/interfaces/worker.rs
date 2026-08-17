use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::capability::CapabilityAdapter;
use crate::domain::{
    CapabilityPack, Evidence, ExecutionSandboxReport, ExecutorKind, ExecutorMode, FailureClass,
    FlightLease, FlightLeaseStatus, Flow, FlowMessage, FuelUsage, MessageInboxItem, PlanDraft,
    PlanningRequest, PlanningStatus, Principal, ProviderCredential, RemoteFlightReport, Task,
    TaskStatus,
};
use crate::error::{RelayError, Result};
use crate::executor::{ExecutionRequest, TerminalExecutor};
use crate::planner::run_plan_executor;
use crate::sandbox::{DockerSandboxConfig, ResolvedDockerSandbox, SandboxBackend};
use crate::worktree::{IsolatedWorktree, WorktreeArtifact, sha256_file};

#[derive(Clone)]
/// 领取响应：请求本身 + 提出人的模型凭据（可能没配）。
#[derive(Debug, Deserialize)]
struct ClaimedPlanning {
    request: PlanningRequest,
    #[serde(default)]
    provider: Option<ProviderCredential>,
}

/// 把凭据写进当前进程环境，执行器子进程会继承。
///
/// 每种 CLI 认的变量名不同，这里按 provider 分派；未知的 provider 两套都设，
/// 让 CLI 自己挑——总比什么都不设、跑出一个看不懂的鉴权错误强。
fn apply_provider_env(provider: &ProviderCredential) {
    let anthropic = || {
        unsafe { std::env::set_var("ANTHROPIC_API_KEY", &provider.api_key) };
        if let Some(base) = &provider.base_url {
            unsafe { std::env::set_var("ANTHROPIC_BASE_URL", base) };
        }
    };
    let openai = || {
        unsafe { std::env::set_var("OPENAI_API_KEY", &provider.api_key) };
        if let Some(base) = &provider.base_url {
            unsafe { std::env::set_var("OPENAI_BASE_URL", base) };
        }
    };
    match provider.provider.as_str() {
        "anthropic" => anthropic(),
        "openai" => openai(),
        _ => {
            anthropic();
            openai();
        }
    }
}

pub struct WorkerOptions {
    pub server_url: String,
    pub token: String,
    pub executor: ExecutorKind,
    pub mode: ExecutorMode,
    pub workspace: PathBuf,
    /// 本机能处理哪些仓库：键接受仓库 ID（REPO-xxx）或 GitLab 项目路径
    /// （group/project），值是本地 checkout 的绝对路径。
    ///
    /// 不自动 clone：IsolatedWorktree::create 要求一个已存在且干净的本地仓库，
    /// 自动 clone 意味着要在 Worker 主机上管理 Git 凭据，那一层单独评估。
    /// 显式映射的好处是行为明确——领不到活时能立刻说清是哪个仓库没配。
    pub repositories: BTreeMap<String, PathBuf>,
    pub model: Option<String>,
    pub command: Option<PathBuf>,
    pub task_id: Option<String>,
    pub timeout_seconds: u64,
    pub data_dir: PathBuf,
    pub sandbox: SandboxBackend,
    pub docker: Option<DockerSandboxConfig>,
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

struct PendingArtifact {
    path: String,
    media_type: String,
    content: Vec<u8>,
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
    sandbox: WorkerSandbox,
}

enum WorkerSandbox {
    Process,
    Docker(ResolvedDockerSandbox),
}

impl RemoteWorker {
    pub fn new(mut options: WorkerOptions) -> Result<Self> {
        if options.token.trim().is_empty() {
            return Err(RelayError::Validation(
                "RELAY_TOKEN is required for a remote worker".into(),
            ));
        }
        // 配了仓库映射就按映射走，--workspace 只是单仓库时的兼容写法。
        if options.repositories.is_empty() && !options.workspace.is_dir() {
            return Err(RelayError::InvalidWorkspace(options.workspace.clone()));
        }
        for (key, path) in &options.repositories {
            if !path.is_dir() {
                return Err(RelayError::Validation(format!(
                    "仓库 {key} 映射到的本地路径不存在：{}",
                    path.display()
                )));
            }
        }
        if options.timeout_seconds == 0 {
            return Err(RelayError::Validation(
                "worker timeout must be greater than zero".into(),
            ));
        }
        fs::create_dir_all(options.data_dir.join("worker-runs"))?;
        let sandbox = match options.sandbox {
            SandboxBackend::Process => {
                if options.docker.is_some() {
                    return Err(RelayError::Validation(
                        "Docker sandbox configuration requires --sandbox docker".into(),
                    ));
                }
                WorkerSandbox::Process
            }
            SandboxBackend::Docker => WorkerSandbox::Docker(
                options
                    .docker
                    .take()
                    .ok_or_else(|| {
                        RelayError::Validation(
                            "Docker sandbox backend requires Docker configuration".into(),
                        )
                    })?
                    .resolve()?,
            ),
        };
        let control_plane = ControlPlaneClient::new(&options.server_url, &options.token)?;
        Ok(Self {
            options,
            control_plane,
            sandbox,
        })
    }

    pub async fn run_once(&self) -> Result<WorkerOutcome> {
        match self.options.mode {
            ExecutorMode::Plan => self.run_plan_once().await,
            ExecutorMode::Decompose => self.run_decompose_once().await,
            ExecutorMode::Execute => self.run_execute_once().await,
        }
    }

    /// 领一条排队中的需求拆解，在本机跑模型，把方案回传。
    ///
    /// 和 `run_plan_once` 的区别在输入：那个的输入是已分配的任务，这个的输入是
    /// 一句需求。提示词由控制面在建请求时拼好带过来——它才知道团队、人员和产能。
    async fn run_decompose_once(&self) -> Result<WorkerOutcome> {
        let principal = self.control_plane.me().await?;
        // 队列里存的是 PlannerKind 的字符串形式（claude-code / codex）。
        let wanted = match self.options.executor {
            ExecutorKind::ClaudeCode => "claude-code",
            ExecutorKind::Codex => "codex",
        };
        let requests = self.control_plane.planning_requests().await?;
        let now = Utc::now();
        let candidate = requests.into_iter().find(|request| {
            request.planner == wanted
                && match request.status {
                    PlanningStatus::Queued => true,
                    // 上一个领取者失联，租约过期后可以接手。
                    PlanningStatus::Claimed => request.lease_expires_at.is_some_and(|at| at <= now),
                    _ => false,
                }
        });
        let Some(request) = candidate else {
            return Ok(WorkerOutcome {
                status: WorkerOutcomeStatus::Idle,
                principal: principal.name,
                task_id: None,
                run_id: None,
                summary: format!("没有等待 {wanted} 拆解的需求"),
                log_path: None,
            });
        };

        let claimed = self
            .control_plane
            .claim_planning(
                &request.id,
                self.options.timeout_seconds.saturating_add(120),
            )
            .await?;
        let request = claimed.request;
        // 提出人自己配的 baseURL / Key：设进环境变量，执行器子进程继承。
        // 没配就沿用 Worker 自身的环境，让本机已登录的 CLI 继续可用。
        if let Some(provider) = &claimed.provider {
            apply_provider_env(provider);
        }

        let run_id = format!("WRUN-{}", Uuid::new_v4().simple());
        let log_path = self
            .options
            .data_dir
            .join("worker-runs")
            .join(&request.id)
            .join(format!("{run_id}.json"));
        // 拆解只读，不需要仓库工作副本；用 Worker 的默认工作区即可。
        let workspace = self.options.workspace.clone();

        let outcome = run_plan_executor(
            self.options.executor.clone(),
            self.options.command.clone(),
            self.options.model.clone(),
            request.prompt.clone(),
            &workspace,
            log_path.clone(),
            self.options.timeout_seconds,
        )
        .await;

        match outcome {
            Ok(plan) => {
                let flow = self
                    .control_plane
                    .submit_planning(&request.id, &plan)
                    .await?;
                Ok(WorkerOutcome {
                    status: WorkerOutcomeStatus::Planned,
                    principal: principal.name,
                    task_id: None,
                    run_id: Some(run_id),
                    summary: format!(
                        "{} 已拆解为 {}（{} 个任务）",
                        request.id,
                        flow.id,
                        flow.tasks.len()
                    ),
                    log_path: Some(log_path),
                })
            }
            Err(error) => {
                // 失败也要回传：否则请求会一直挂在 claimed，直到租约过期才有人再试。
                let reason = error.to_string();
                self.control_plane
                    .fail_planning(&request.id, &reason)
                    .await?;
                Err(error)
            }
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
            return Err(RelayError::InvalidTransition(format!(
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
        let result = self
            .run_executor(
                ExecutionRequest {
                    kind: self.options.executor.clone(),
                    command: self.options.command.clone(),
                    workspace: self.options.workspace.clone(),
                    model: self.options.model.clone(),
                    mode: ExecutorMode::Plan,
                    prompt,
                    output_schema: None,
                    timeout_seconds: self.options.timeout_seconds,
                    log_path: log_path.clone(),
                },
                &format!("relay-{run_id}"),
            )
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

    /// 这次任务应该在哪个本地目录里执行。
    ///
    /// manifest 带了仓库就必须能映射到本地路径，映射不上返回 None——调用方据此
    /// 跳过该租约。没带仓库的走 --workspace，兼容单仓库的老用法。
    fn workspace_for(&self, lease: &FlightLease) -> Option<PathBuf> {
        let Some(repository) = lease.manifest.as_ref().and_then(|m| m.repository.as_ref()) else {
            return Some(self.options.workspace.clone());
        };
        self.options
            .repositories
            .get(&repository.id)
            .or_else(|| {
                self.options
                    .repositories
                    .get(&repository.gitlab_project_path)
            })
            .cloned()
    }

    async fn run_execute_once(&self) -> Result<WorkerOutcome> {
        let principal = self.control_plane.me().await?;
        let leases = self.control_plane.flight_leases().await?;
        let Some(selected_lease) = select_lease(
            &leases,
            &self.options.executor,
            self.options.task_id.as_deref(),
            |lease| self.workspace_for(lease).is_some(),
        ) else {
            // 区分「没有活」和「有活但本机没配这个仓库」——后者只看
            // 「worker 空转」是查不出来的。
            let unmapped: Vec<_> = leases
                .iter()
                .filter(|lease| lease.executor == self.options.executor)
                .filter(|lease| {
                    matches!(
                        lease.status,
                        FlightLeaseStatus::Authorized | FlightLeaseStatus::Active
                    )
                })
                .filter_map(|lease| lease.manifest.as_ref()?.repository.as_ref())
                .map(|repository| {
                    format!("{}({})", repository.name, repository.gitlab_project_path)
                })
                .collect();
            let summary = if unmapped.is_empty() {
                "no authorized write flight lease for this worker and executor".to_string()
            } else {
                format!(
                    "有 {} 个任务在等，但本机没有配置对应仓库：{}。用 --repo <仓库>=<本地路径> 指定。",
                    unmapped.len(),
                    unmapped.join("、")
                )
            };
            return Ok(WorkerOutcome {
                status: WorkerOutcomeStatus::Idle,
                principal: principal.name,
                task_id: self.options.task_id.clone(),
                run_id: None,
                summary,
                log_path: None,
            });
        };
        let inbox = self.control_plane.inbox().await?;
        let mut lease = selected_lease.clone();
        let item = inbox
            .iter()
            .find(|item| item.task.id == lease.task_id)
            .ok_or_else(|| {
                RelayError::InvalidTransition(format!(
                    "leased task {} is not in the worker inbox",
                    lease.task_id
                ))
            })?;
        if !item.blocked_by.is_empty() {
            return Err(RelayError::InvalidTransition(format!(
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
                RelayError::InvalidTransition(format!(
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

        let pack = lease
            .manifest
            .as_ref()
            .map_or(CapabilityPack::General, |manifest| manifest.capability_pack);
        // select_lease 已经保证能解析出来，这里不会是 None。
        let source_workspace = self
            .workspace_for(&lease)
            .unwrap_or_else(|| self.options.workspace.clone());
        let (result, artifact) = {
            let worktree = if worktree_root.exists() {
                IsolatedWorktree::resume(&source_workspace, worktree_root)
            } else {
                IsolatedWorktree::create(&source_workspace, worktree_root)
            };
            match worktree {
                Ok(mut worktree) => {
                    let execution = self
                        .run_executor(
                            ExecutionRequest {
                                kind: self.options.executor.clone(),
                                command: self.options.command.clone(),
                                workspace: worktree.workspace().to_path_buf(),
                                model: self.options.model.clone(),
                                mode: ExecutorMode::Execute,
                                prompt,
                                output_schema: None,
                                timeout_seconds: self.options.timeout_seconds,
                                log_path: log_path.clone(),
                            },
                            &format!("relay-{run_id}"),
                        )
                        .await;
                    let collected = worktree.collect(&patch_path).and_then(|artifact| {
                        let files = if pack == CapabilityPack::Office {
                            collect_staged_files(worktree.workspace(), &artifact.changed_files)?
                        } else {
                            Vec::new()
                        };
                        Ok((artifact, files))
                    });
                    let cleanup = worktree.cleanup();
                    let artifact = collected.and_then(|artifact| cleanup.map(|_| artifact));
                    (execution, artifact)
                }
                Err(error) => {
                    write_setup_blackbox(&log_path, &run_id, &error)?;
                    (
                        Err(error),
                        Ok((
                            WorktreeArtifact {
                                base_revision: "unavailable".into(),
                                changed_files: vec![],
                                patch_path: None,
                                patch_sha256: None,
                            },
                            Vec::new(),
                        )),
                    )
                }
            }
        };

        let (mut landed, mut summary, artifact, staged_files, cost_usd, mut failure_class) =
            match (result, artifact) {
                (Ok(output), Ok((artifact, staged_files))) => {
                    let suffix = match artifact.changed_files.len() {
                        0 => "no file changes".to_string(),
                        1 => "1 changed file captured in the isolated patch".to_string(),
                        count => format!("{count} changed files captured in the isolated patch"),
                    };
                    (
                        true,
                        truncate(&format!("{}; {suffix}", output.summary), 4_000),
                        artifact,
                        staged_files,
                        output.cost_usd,
                        None,
                    )
                }
                (Err(error), Ok((artifact, staged_files))) => {
                    let failure = classify_worker_error(&error);
                    (
                        false,
                        truncate(&error.to_string(), 4_000),
                        artifact,
                        staged_files,
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
                        Vec::new(),
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
                        Vec::new(),
                        None,
                        Some(failure),
                    )
                }
            };
        if landed {
            for file in staged_files {
                if let Err(error) = self
                    .control_plane
                    .stage_artifact(&lease.id, &file.path, &file.media_type, file.content)
                    .await
                {
                    landed = false;
                    failure_class = Some(FailureClass::Tool);
                    summary = truncate(
                        &format!("{summary}; artifact staging failed: {error}"),
                        4_000,
                    );
                    break;
                }
            }
        }
        if !log_path.is_file() {
            write_setup_blackbox(&log_path, &run_id, &RelayError::Validation(summary.clone()))?;
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
            sandbox: Some(self.sandbox_report()),
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

    async fn run_executor(
        &self,
        request: ExecutionRequest,
        container_name: &str,
    ) -> Result<crate::executor::ExecutionOutput> {
        match &self.sandbox {
            WorkerSandbox::Process => TerminalExecutor::run(request).await,
            WorkerSandbox::Docker(sandbox) => {
                TerminalExecutor::run_in_docker(request, sandbox, container_name).await
            }
        }
    }

    fn sandbox_report(&self) -> ExecutionSandboxReport {
        match &self.sandbox {
            WorkerSandbox::Process => ExecutionSandboxReport {
                backend: "process".into(),
                image: None,
                image_id: None,
                network: "host".into(),
                root_read_only: false,
                user: None,
                cpus_millis: None,
                memory_bytes: None,
                pids_limit: None,
                forwarded_environment: Vec::new(),
            },
            WorkerSandbox::Docker(sandbox) => sandbox.report(),
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
            .map_err(|_| RelayError::Validation("invalid Relay server URL".into()))?;
        if !matches!(api_base.scheme(), "http" | "https") {
            return Err(RelayError::Validation(
                "Relay server URL must use http or https".into(),
            ));
        }
        if !api_base.username().is_empty() || api_base.password().is_some() {
            return Err(RelayError::Validation(
                "Relay server URL must not contain credentials; use RELAY_TOKEN".into(),
            ));
        }
        api_base.set_query(None);
        api_base.set_fragment(None);
        {
            let mut segments = api_base.path_segments_mut().map_err(|_| {
                RelayError::Validation("Relay server URL cannot be used as an API base".into())
            })?;
            segments.pop_if_empty().push("api").push("v1");
        }
        if !api_base.path().ends_with('/') {
            api_base.set_path(&format!("{}/", api_base.path()));
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .user_agent(concat!("Relay-Worker/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| {
                RelayError::ExternalConnector("could not initialize remote worker client".into())
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

    async fn planning_requests(&self) -> Result<Vec<PlanningRequest>> {
        self.request(Method::GET, &["planning-requests"], None)
            .await
    }

    async fn claim_planning(&self, id: &str, lease_seconds: u64) -> Result<ClaimedPlanning> {
        self.request(
            Method::POST,
            &["planning-requests", id, "claim"],
            Some(json!({ "lease_seconds": lease_seconds })),
        )
        .await
    }

    async fn submit_planning(&self, id: &str, plan: &PlanDraft) -> Result<Flow> {
        self.request(
            Method::POST,
            &["planning-requests", id, "submit"],
            Some(json!({ "plan": plan })),
        )
        .await
    }

    async fn fail_planning(&self, id: &str, reason: &str) -> Result<PlanningRequest> {
        self.request(
            Method::POST,
            &["planning-requests", id, "fail"],
            Some(json!({ "reason": reason })),
        )
        .await
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

    async fn stage_artifact(
        &self,
        lease_id: &str,
        path: &str,
        media_type: &str,
        content: Vec<u8>,
    ) -> Result<crate::domain::StagedArtifact> {
        let mut url = self.api_base.clone();
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                RelayError::Validation("Relay server URL cannot form an endpoint".into())
            })?;
            segments.pop_if_empty();
            for segment in ["flight-leases", lease_id, "artifacts"] {
                segments.push(segment);
            }
        }
        url.query_pairs_mut().append_pair("path", path);
        let response = self
            .client
            .put(url)
            .bearer_auth(&self.token)
            .header(reqwest::header::CONTENT_TYPE, media_type)
            .body(content)
            .send()
            .await
            .map_err(|error| {
                RelayError::ExternalConnector(format!("artifact staging request failed: {error}"))
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(control_plane_error(status, response).await);
        }
        response.json().await.map_err(|_| {
            RelayError::ExternalConnector(
                "control plane returned an invalid staged artifact".into(),
            )
        })
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
                RelayError::Validation("Relay server URL cannot form an endpoint".into())
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
            RelayError::ExternalConnector(format!("control plane request failed: {error}"))
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(control_plane_error(status, response).await);
        }
        response.json().await.map_err(|_| {
            RelayError::ExternalConnector("control plane returned an invalid JSON response".into())
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
    runnable: impl Fn(&FlightLease) -> bool,
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
        // 跑不了的直接不领：领了再失败会让任务白白进入 active 又崩掉。
        .filter(|lease| runnable(lease))
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
        "Relay remote PASS for a read-only planning flight.\n\
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
        "Relay remote PASS for a Human-authorized {pack:?} flight.\n\
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

fn collect_staged_files(
    workspace: &Path,
    changed_files: &[String],
) -> Result<Vec<PendingArtifact>> {
    let workspace = workspace.canonicalize()?;
    let mut total_bytes = 0usize;
    let mut files = Vec::with_capacity(changed_files.len());
    for path in changed_files {
        let source = workspace.join(path);
        let metadata = fs::symlink_metadata(&source).map_err(|_| {
            RelayError::Validation(format!(
                "Office deliverable is missing or was deleted: {path}"
            ))
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(RelayError::Validation(format!(
                "Office deliverable must be a regular file: {path}"
            )));
        }
        let canonical = source.canonicalize()?;
        if !canonical.starts_with(&workspace) {
            return Err(RelayError::Validation(format!(
                "Office deliverable escapes the isolated workspace: {path}"
            )));
        }
        let content = fs::read(canonical)?;
        total_bytes = total_bytes.checked_add(content.len()).ok_or_else(|| {
            RelayError::Validation("Office deliverables exceed the staging budget".into())
        })?;
        if total_bytes > crate::application::app::artifacts::MAX_ARTIFACT_BYTES {
            return Err(RelayError::Validation(format!(
                "Office deliverables exceed the {} byte staging budget",
                crate::application::app::artifacts::MAX_ARTIFACT_BYTES
            )));
        }
        files.push(PendingArtifact {
            path: path.clone(),
            media_type: office_media_type(path).into(),
            content,
        });
    }
    Ok(files)
}

fn office_media_type(path: &str) -> &'static str {
    match Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("md" | "txt") => "text/plain",
        Some("html") => "text/html",
        Some("pdf") => "application/pdf",
        Some("docx") => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        Some("csv") => "text/csv",
        Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        Some("pptx") => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        Some("eml") => "message/rfc822",
        Some("ics") => "text/calendar",
        _ => "application/octet-stream",
    }
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

async fn control_plane_error(status: StatusCode, response: reqwest::Response) -> RelayError {
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
    RelayError::ExternalConnector(format!(
        "control plane returned HTTP {}: {message}",
        status.as_u16()
    ))
}

fn truncate(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn write_setup_blackbox(path: &std::path::Path, run_id: &str, error: &RelayError) -> Result<()> {
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

fn classify_worker_error(error: &RelayError) -> FailureClass {
    match error {
        RelayError::ExecutorTimeout(_) => FailureClass::Timeout,
        RelayError::PermissionDenied(_) => FailureClass::Permission,
        RelayError::InvalidWorkspace(_) | RelayError::Io(_) => FailureClass::Resource,
        RelayError::Validation(_) | RelayError::InvalidExecutorOutput(_) | RelayError::Json(_) => {
            FailureClass::Validation
        }
        RelayError::ExecutorUnavailable(_)
        | RelayError::ExecutorFailed { .. }
        | RelayError::ExternalConnector(_) => FailureClass::Tool,
        _ => FailureClass::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::{FuelBudget, RepositoryRef};

    fn worker_with_repos(
        directory: &std::path::Path,
        repositories: BTreeMap<String, PathBuf>,
    ) -> RemoteWorker {
        RemoteWorker::new(WorkerOptions {
            repositories,
            server_url: "http://127.0.0.1:1".into(),
            token: "worker-token".into(),
            executor: ExecutorKind::Codex,
            mode: ExecutorMode::Execute,
            workspace: directory.to_path_buf(),
            model: None,
            command: None,
            task_id: None,
            timeout_seconds: 10,
            data_dir: directory.join("data"),
            sandbox: SandboxBackend::Process,
            docker: None,
        })
        .unwrap()
    }

    fn lease_for_repository(repository: Option<RepositoryRef>) -> FlightLease {
        FlightLease {
            id: "LEASE-1".into(),
            flow_id: "FLOW-1".into(),
            task_id: "TSK-1".into(),
            principal_id: "HUM-1".into(),
            principal_name: "李伟".into(),
            authorized_by: "陈静".into(),
            executor: ExecutorKind::Codex,
            status: FlightLeaseStatus::Authorized,
            issued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            claimed_at: None,
            finished_at: None,
            run_id: None,
            report: None,
            manifest: repository.map(|repository| FlightManifest {
                id: "MANIFEST-1".into(),
                repository: Some(repository),
                objective: "实现功能".into(),
                landing_conditions: vec!["测试通过".into()],
                context_refs: Vec::new(),
                tool_permissions: Vec::new(),
                fuel: FuelBudget::default(),
                recovery: RecoveryPolicy::default(),
                resources: Vec::new(),
                capability_pack: CapabilityPack::General,
                output_contract: OutputContract::default(),
                declared_by: "陈静".into(),
                declared_at: Utc::now(),
            }),
            parent_lease_id: None,
            root_lease_id: None,
            attempt: 1,
        }
    }

    fn repository_ref() -> RepositoryRef {
        RepositoryRef {
            id: "REPO-abc".into(),
            name: "web-app".into(),
            gitlab_project_path: "acme/web-app".into(),
            branch: "main".into(),
        }
    }

    #[test]
    fn workspace_resolves_by_repository_id_or_project_path() {
        let directory = tempdir().unwrap();
        let checkout = directory.path().join("web-app");
        fs::create_dir_all(&checkout).unwrap();

        for key in ["REPO-abc", "acme/web-app"] {
            let worker = worker_with_repos(
                directory.path(),
                BTreeMap::from([(key.to_string(), checkout.clone())]),
            );
            assert_eq!(
                worker.workspace_for(&lease_for_repository(Some(repository_ref()))),
                Some(checkout.clone()),
                "应当能用 {key} 解析出本地路径"
            );
        }
    }

    #[test]
    fn unmapped_repository_yields_no_workspace() {
        let directory = tempdir().unwrap();
        let worker = worker_with_repos(directory.path(), BTreeMap::new());
        // 解析不出来就不能跑；调用方据此跳过，而不是领了再崩。
        assert_eq!(
            worker.workspace_for(&lease_for_repository(Some(repository_ref()))),
            None
        );
    }

    #[test]
    fn manifest_without_repository_falls_back_to_workspace_flag() {
        let directory = tempdir().unwrap();
        let worker = worker_with_repos(directory.path(), BTreeMap::new());
        assert_eq!(
            worker.workspace_for(&lease_for_repository(None)),
            Some(directory.path().to_path_buf()),
            "没绑定仓库的老用法要继续可用"
        );
    }

    #[test]
    fn leases_for_unmapped_repositories_are_not_claimed() {
        let leases = vec![lease_for_repository(Some(repository_ref()))];
        // 本机跑不了这个仓库时，select_lease 不应该把它选出来。
        assert!(select_lease(&leases, &ExecutorKind::Codex, None, |_| false).is_none());
        assert!(select_lease(&leases, &ExecutorKind::Codex, None, |_| true).is_some());
    }
    use std::sync::{Arc, Mutex};

    use axum::body::Bytes;
    use axum::extract::{Path, Query, State};
    use axum::http::HeaderMap;
    use axum::routing::{get, post, put};
    use axum::{Json, Router};
    use chrono::{Duration as ChronoDuration, Utc};
    use tempfile::tempdir;

    use super::*;
    use crate::domain::{
        Assignment, AssignmentTarget, Estimate, FlightManifest, OutputContract, PrincipalKind,
        RecoveryPolicy, StagedArtifact, TargetKind,
    };

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
            repositories: BTreeMap::new(),
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
            sandbox: SandboxBackend::Process,
            docker: None,
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
            manifest: Some(FlightManifest {
                id: "MANIFEST-1".into(),
                repository: None,
                objective: "produce a reviewable Office document".into(),
                landing_conditions: vec!["generated.txt is staged".into()],
                context_refs: Vec::new(),
                tool_permissions: Vec::new(),
                fuel: Default::default(),
                recovery: RecoveryPolicy::default(),
                resources: Vec::new(),
                capability_pack: CapabilityPack::Office,
                output_contract: OutputContract::for_pack(CapabilityPack::Office),
                declared_by: "Engineer".into(),
                declared_at: now,
            }),
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
            .route(
                "/api/v1/flight-leases/{lease}/artifacts",
                put(mock_stage_artifact),
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
            repositories: BTreeMap::new(),
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
            sandbox: SandboxBackend::Process,
            docker: None,
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
            ["claim", "stage", "finish"]
        );
        let lease = state.lease.lock().unwrap().clone().unwrap();
        assert_eq!(lease.status, FlightLeaseStatus::Landed);
        let report = lease.report.unwrap();
        assert_eq!(report.changed_files, ["generated.txt"]);
        assert!(report.patch_sha256.is_some());
        assert_eq!(report.log_sha256.len(), 64);
        assert_eq!(report.sandbox.as_ref().unwrap().backend, "process");
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

    async fn mock_stage_artifact(
        State(state): State<MockState>,
        Path(lease_id): Path<String>,
        Query(query): Query<std::collections::BTreeMap<String, String>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Json<StagedArtifact> {
        state.actions.lock().unwrap().push("stage".into());
        assert_eq!(lease_id, "LEASE-1");
        assert_eq!(query.get("path").map(String::as_str), Some("generated.txt"));
        assert_eq!(
            headers
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/plain")
        );
        assert_eq!(body.as_ref(), b"isolated change\n");
        Json(StagedArtifact {
            id: "ART-1".into(),
            flight_lease_id: lease_id,
            flow_id: "FLOW-1".into(),
            task_id: state.task.lock().unwrap().id.clone(),
            path: "generated.txt".into(),
            kind: crate::domain::DeliverableKind::Document,
            media_type: "text/plain".into(),
            sha256: "a".repeat(64),
            size_bytes: body.len() as u64,
            staged_by: state.principal.name.clone(),
            staged_at: Utc::now(),
        })
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
