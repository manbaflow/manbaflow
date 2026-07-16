use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand, ValueEnum};
use manbaflow::domain::{
    ExecutorConfig, ExecutorKind, ExecutorMode, Flow, PrincipalKind, Task, TrackingAttention,
    TrackingEscalation,
};
use manbaflow::gitlab::GitLabClient;
use manbaflow::planner::PlannerKind;
use manbaflow::worker::{RemoteWorker, WorkerOptions, WorkerOutcome, WorkerOutcomeStatus};
use manbaflow::{MambaApp, Result};
use serde::Serialize;
use serde_json::json;

#[derive(Parser)]
#[command(
    name = "mamba",
    version,
    about = "Enterprise Human-Agent Flow control plane"
)]
struct Cli {
    #[arg(long, global = true, default_value = ".mambaflow")]
    data_dir: PathBuf,

    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// 打开全屏组织塔台
    Tui {
        #[arg(long = "as")]
        actor: Option<String>,
        #[arg(long, default_value = ".")]
        workspace: PathBuf,
    },
    /// 启动支持远程 Inbox 的 Human-Agent Control Plane
    Serve {
        #[arg(long, default_value = "127.0.0.1:7777")]
        bind: SocketAddr,
        #[arg(long, default_value_t = 30)]
        tracker_interval: u64,
        #[arg(long, default_value_t = 24)]
        stale_hours: u64,
        #[arg(long, default_value_t = 4)]
        escalate_after_hours: u64,
    },
    /// 初始化和查看组织塔台
    Org {
        #[command(subcommand)]
        command: OrgCommand,
    },
    /// 管理团队与能力
    Team {
        #[command(subcommand)]
        command: TeamCommand,
    },
    /// 注册 Human、本地 Agent 和远程 Personal Agent
    Principal {
        #[command(subcommand)]
        command: PrincipalCommand,
    },
    /// 提交管理需求并生成 Flow 草案
    Demand {
        #[command(subcommand)]
        command: DemandCommand,
    },
    /// 查看和批准 Flow
    Flow {
        #[command(subcommand)]
        command: FlowCommand,
    },
    /// 推进任务、添加证据或调用执行终端
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    /// 查看某个 Human 或 Agent 的工作收件箱
    Inbox {
        #[arg(long = "for")]
        target: String,
    },
    /// 扫描 Todo 风险并查看塔台 Attention
    Track {
        #[command(subcommand)]
        command: TrackCommand,
    },
    /// 连接 GitLab 项目并同步 MR/Pipeline 交付物
    Gitlab {
        #[command(subcommand)]
        command: GitLabCommand,
    },
    /// 在同事工作站运行只读 Personal Agent 航班
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
    /// 从 Flow Ledger 查看完整事件时间线
    Timeline { flow: String },
    /// 检查本机执行终端
    Executor {
        #[command(subcommand)]
        command: ExecutorCommand,
    },
    /// 初始化一套牢大、佐巴扬与两个副驾的演示阵容
    Demo {
        #[arg(long, default_value = ".")]
        workspace: PathBuf,
    },
}

#[derive(Subcommand)]
enum OrgCommand {
    /// 初始化本地组织
    Init {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 查看组织基本信息
    Show,
    /// 查看团队、Human 和 Agent 关系
    Chart,
}

#[derive(Subcommand)]
enum TeamCommand {
    /// 新增团队
    Add {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "")]
        capabilities: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 列出团队
    List,
}

#[derive(Subcommand)]
enum PrincipalCommand {
    /// 注册 Human 或 Agent
    Add(PrincipalAdd),
    /// 列出所有 Human 和 Agent
    List,
    /// 管理远程 API Bearer Token
    Token {
        #[command(subcommand)]
        command: CredentialCommand,
    },
}

#[derive(Subcommand)]
enum CredentialCommand {
    /// 为 Principal 签发只显示一次的 Token
    Issue {
        #[arg(long = "for")]
        target: String,
        #[arg(long, default_value = "remote client")]
        label: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 查看 Token 元数据，不显示 Secret
    List {
        #[arg(long = "for")]
        target: String,
        #[arg(long)]
        all: bool,
    },
    /// 撤销 Token
    Revoke {
        credential: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
}

#[derive(Subcommand)]
enum GitLabCommand {
    /// 验证 GitLab 项目和 Token 权限
    Check {
        #[arg(long)]
        project: String,
        #[arg(long)]
        url: Option<String>,
    },
    /// 把 Merge Request 和最新 Pipeline 同步到任务黑匣子
    Sync {
        #[arg(long)]
        task: String,
        #[arg(long)]
        project: String,
        #[arg(long)]
        mr: u64,
        #[arg(long)]
        by: String,
        #[arg(long)]
        url: Option<String>,
    },
}

#[derive(Subcommand)]
enum WorkerCommand {
    /// 领取并规划一个远程任务后退出
    Once(WorkerArgs),
    /// 持续轮询远程 Inbox，串行执行只读规划航班
    Run {
        #[command(flatten)]
        worker: WorkerArgs,
        #[arg(long, default_value_t = 30)]
        poll_seconds: u64,
    },
}

#[derive(Args)]
struct WorkerArgs {
    /// Control Plane 根地址；也可使用 MAMBA_SERVER
    #[arg(long)]
    server: Option<String>,
    #[arg(long, value_enum)]
    executor: ExecutorKindArg,
    #[arg(long, default_value = ".")]
    workspace: PathBuf,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    executable: Option<PathBuf>,
    /// 只处理指定 Task；默认选择第一个尚未规划的任务
    #[arg(long)]
    task: Option<String>,
    #[arg(long, default_value_t = 900)]
    timeout: u64,
}

#[derive(Args)]
struct PrincipalAdd {
    #[arg(long)]
    name: String,
    #[arg(long, value_enum)]
    kind: PrincipalKindArg,
    #[arg(long)]
    team: Option<String>,
    #[arg(long)]
    owner: Option<String>,
    #[arg(long, default_value = "")]
    capabilities: String,
    #[arg(long, default_value_t = 100)]
    capacity: u8,
    #[arg(long, value_enum)]
    executor: Option<ExecutorKindArg>,
    #[arg(long)]
    workspace: Option<PathBuf>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    executable: Option<PathBuf>,
    #[arg(long, default_value = "admin")]
    by: String,
}

#[derive(Subcommand)]
enum DemandCommand {
    /// 生成 PRD、任务 DAG、匹配和工期草案
    Create {
        summary: String,
        #[arg(long)]
        requester: String,
        #[arg(long, value_enum, default_value = "local")]
        planner: PlannerKindArg,
        #[arg(long, default_value = ".")]
        workspace: PathBuf,
        #[arg(long, default_value_t = 300)]
        timeout: u64,
    },
}

#[derive(Subcommand)]
enum FlowCommand {
    /// 列出 Flow
    List,
    /// 查看 PRD、任务、匹配和关键路径
    Show { flow: String },
    /// 由 Human 批准 Flow 并发送 WorkRequest
    Approve {
        flow: String,
        #[arg(long)]
        by: String,
    },
}

#[derive(Subcommand)]
enum TaskCommand {
    /// 查看任务详情
    Show { task: String },
    /// Owner、Copilot 或 Human Owner 接单
    Accept {
        task: String,
        #[arg(long)]
        by: String,
    },
    /// 拒绝 WorkRequest
    Reject {
        task: String,
        #[arg(long)]
        by: String,
        #[arg(long)]
        reason: String,
    },
    /// 协商基础工作量并重算 P50/P80
    Negotiate {
        task: String,
        #[arg(long)]
        by: String,
        #[arg(long)]
        hours: f64,
    },
    /// 在依赖完成后开始任务
    Start {
        task: String,
        #[arg(long)]
        by: String,
    },
    /// 写入进度航点
    Heartbeat {
        task: String,
        #[arg(long)]
        by: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// 标记阻塞并留下原因
    Block {
        task: String,
        #[arg(long)]
        by: String,
        #[arg(long)]
        reason: String,
    },
    /// 添加可审计交付证据
    Evidence {
        task: String,
        #[arg(long)]
        by: String,
        #[arg(long)]
        kind: String,
        #[arg(long)]
        uri: String,
        #[arg(long)]
        summary: String,
    },
    /// 调用已分配的 Claude Code 或 Codex 终端
    Run {
        task: String,
        #[arg(long)]
        by: String,
        #[arg(long)]
        executor: Option<String>,
        #[arg(long, value_enum, default_value = "plan")]
        mode: ExecutorModeArg,
        #[arg(long, default_value_t = 900)]
        timeout: u64,
    },
    /// 带 Evidence 提交人工验收
    Submit {
        task: String,
        #[arg(long)]
        by: String,
    },
    /// 由注册 Human 完成最终验收
    Complete {
        task: String,
        #[arg(long)]
        by: String,
    },
}

#[derive(Subcommand)]
enum ExecutorCommand {
    /// 检查 CLI 是否安装并输出版本
    Check {
        #[arg(value_enum)]
        kind: ExecutorKindArg,
        #[arg(long)]
        executable: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum TrackCommand {
    /// 扫描未接单、失联、阻塞、待验收和超期任务
    Scan {
        #[arg(long, default_value_t = 24)]
        stale_hours: u64,
        #[arg(long, default_value_t = 4)]
        escalate_after_hours: u64,
        #[arg(long, default_value = "tower://local")]
        by: String,
    },
    /// 查看活动提醒；使用 --all 包含已解除记录
    List {
        #[arg(long)]
        flow: Option<String>,
        #[arg(long)]
        all: bool,
    },
    /// 查看指定 Human 收到的活动升级呼叫
    Inbox {
        #[arg(long = "for")]
        target: String,
        #[arg(long)]
        all: bool,
    },
    /// 由接收人确认已经接手处理
    Ack {
        escalation: String,
        #[arg(long)]
        by: String,
    },
}

#[derive(Clone, ValueEnum)]
enum PrincipalKindArg {
    Human,
    Agent,
}

#[derive(Clone, ValueEnum)]
enum ExecutorKindArg {
    ClaudeCode,
    Codex,
}

#[derive(Clone, ValueEnum)]
enum PlannerKindArg {
    Local,
    ClaudeCode,
    Codex,
}

#[derive(Clone, ValueEnum)]
enum ExecutorModeArg {
    Plan,
    Execute,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let mut app = MambaApp::open(&cli.data_dir)?;
    let command = cli.command.unwrap_or_else(|| Command::Tui {
        actor: None,
        workspace: PathBuf::from("."),
    });
    match command {
        Command::Tui { actor, workspace } => {
            manbaflow::tui::run(
                &mut app,
                manbaflow::tui::TuiOptions {
                    workspace: absolute_path(workspace)?,
                    actor,
                },
            )
            .await?;
        }
        Command::Serve {
            bind,
            tracker_interval,
            stale_hours,
            escalate_after_hours,
        } => {
            manbaflow::server::run(
                app,
                manbaflow::server::ServerOptions {
                    bind,
                    tracker_interval_seconds: tracker_interval,
                    stale_after_hours: stale_hours,
                    escalate_after_hours,
                },
            )
            .await?;
        }
        Command::Org { command } => match command {
            OrgCommand::Init { name, by } => {
                let org = app.init_organization(&name, &by)?;
                output(
                    &org,
                    cli.json,
                    format!("塔台已启用：{} ({})", org.name, org.id),
                );
            }
            OrgCommand::Show => {
                let org = app.state().organization()?;
                output(org, cli.json, format!("{} ({})", org.name, org.id));
            }
            OrgCommand::Chart => print_chart(&app, cli.json)?,
        },
        Command::Team { command } => match command {
            TeamCommand::Add {
                name,
                capabilities,
                by,
            } => {
                let team = app.create_team(&name, &capabilities, &by)?;
                output(
                    &team,
                    cli.json,
                    format!("已建立球队 {} ({})", team.name, team.id),
                );
            }
            TeamCommand::List => output(
                &app.state().teams.values().collect::<Vec<_>>(),
                cli.json,
                app.state()
                    .teams
                    .values()
                    .map(|team| {
                        format!(
                            "{}\t{}\t{}",
                            team.id,
                            team.name,
                            team.capabilities.join(",")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
        },
        Command::Principal { command } => match command {
            PrincipalCommand::Add(args) => {
                let kind = PrincipalKind::from(args.kind);
                let executor = if let Some(value) = args.executor {
                    Some(ExecutorConfig {
                        kind: value.into(),
                        workspace: absolute_path(
                            args.workspace.unwrap_or_else(|| PathBuf::from(".")),
                        )?,
                        model: args.model,
                        command: args.executable,
                    })
                } else {
                    None
                };
                let principal = app.register_principal(
                    &args.name,
                    kind,
                    args.team.as_deref(),
                    args.owner.as_deref(),
                    &args.capabilities,
                    args.capacity,
                    executor,
                    &args.by,
                )?;
                output(
                    &principal,
                    cli.json,
                    format!("{} 加入轮换 ({})", principal.name, principal.id),
                );
            }
            PrincipalCommand::List => output(
                &app.state().principals.values().collect::<Vec<_>>(),
                cli.json,
                app.state()
                    .principals
                    .values()
                    .map(|principal| {
                        format!(
                            "{}\t{}\t{:?}\t{}%\t{}",
                            principal.id,
                            principal.name,
                            principal.kind,
                            principal.capacity_percent,
                            principal
                                .executor
                                .as_ref()
                                .map(|executor| executor.kind.to_string())
                                .unwrap_or_else(|| match principal.kind {
                                    PrincipalKind::Human => "human".into(),
                                    PrincipalKind::Agent => "remote-worker".into(),
                                })
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            PrincipalCommand::Token { command } => match command {
                CredentialCommand::Issue { target, label, by } => {
                    let issued = app.issue_api_credential(&target, &label, &by)?;
                    output(
                        &issued,
                        cli.json,
                        format!(
                            "{}\nToken 只显示一次：{}",
                            issued.credential.id, issued.token
                        ),
                    );
                }
                CredentialCommand::List { target, all } => {
                    let principal = app.state().principal(&target)?;
                    let credentials = app
                        .state()
                        .credentials
                        .values()
                        .filter(|credential| credential.principal_id == principal.id)
                        .filter(|credential| all || credential.is_active())
                        .collect::<Vec<_>>();
                    let text = credentials
                        .iter()
                        .map(|credential| {
                            format!(
                                "{}\t{}\t{}\t{}",
                                credential.id,
                                if credential.is_active() {
                                    "active"
                                } else {
                                    "revoked"
                                },
                                credential.label,
                                credential.created_at
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    output(&credentials, cli.json, text);
                }
                CredentialCommand::Revoke { credential, by } => {
                    let credential = app.revoke_api_credential(&credential, &by)?;
                    output(&credential, cli.json, format!("{} 已撤销", credential.id));
                }
            },
        },
        Command::Demand { command } => match command {
            DemandCommand::Create {
                summary,
                requester,
                planner,
                workspace,
                timeout,
            } => {
                let workspace = absolute_path(workspace)?;
                let flow = app
                    .create_demand(&summary, &requester, planner.into(), &workspace, timeout)
                    .await?;
                output(&flow, cli.json, flow_summary(&flow));
            }
        },
        Command::Flow { command } => match command {
            FlowCommand::List => output(
                &app.state().flows.values().collect::<Vec<_>>(),
                cli.json,
                app.state()
                    .flows
                    .values()
                    .map(flow_summary)
                    .collect::<Vec<_>>()
                    .join("\n\n"),
            ),
            FlowCommand::Show { flow } => {
                let flow = app.state().flow(&flow)?;
                output(flow, cli.json, detailed_flow(flow));
            }
            FlowCommand::Approve { flow, by } => {
                let flow = app.approve_flow(&flow, &by)?;
                output(
                    &flow,
                    cli.json,
                    format!("{} 已批准，{} 个任务完成传球", flow.id, flow.tasks.len()),
                );
            }
        },
        Command::Task { command } => match command {
            TaskCommand::Show { task } => {
                let (_, task) = app.state().find_task(&task)?;
                output(task, cli.json, task_details(task));
            }
            TaskCommand::Accept { task, by } => {
                let task = app.accept_task(&task, &by)?;
                output(&task, cli.json, format!("{} 接球：{}", by, task.title));
            }
            TaskCommand::Reject { task, by, reason } => {
                let task = app.reject_task(&task, &by, &reason)?;
                output(
                    &task,
                    cli.json,
                    format!("{} 已退回任务：{}", task.id, reason),
                );
            }
            TaskCommand::Negotiate { task, by, hours } => {
                let task = app.negotiate_task(&task, &by, hours)?;
                output(
                    &task,
                    cli.json,
                    format!("{} 工期已协商为 {:.1}h", task.id, hours),
                );
            }
            TaskCommand::Start { task, by } => {
                let task = app.start_task(&task, &by)?;
                output(&task, cli.json, format!("{} 已起飞", task.id));
            }
            TaskCommand::Heartbeat { task, by, note } => {
                let task = app.heartbeat_task(&task, &by, note)?;
                output(&task, cli.json, format!("{} 航点已记录", task.id));
            }
            TaskCommand::Block { task, by, reason } => {
                let task = app.block_task(&task, &by, &reason)?;
                output(
                    &task,
                    cli.json,
                    format!("{} 等待塔台处理：{}", task.id, reason),
                );
            }
            TaskCommand::Evidence {
                task,
                by,
                kind,
                uri,
                summary,
            } => {
                let evidence = app.add_evidence(&task, &by, &kind, &uri, &summary)?;
                output(
                    &evidence,
                    cli.json,
                    format!("证据已加入黑匣子：{}", evidence.id),
                );
            }
            TaskCommand::Run {
                task,
                by,
                executor,
                mode,
                timeout,
            } => {
                let record = app
                    .run_task(&task, &by, executor.as_deref(), mode.into(), timeout)
                    .await?;
                output(
                    &record,
                    cli.json,
                    format!(
                        "{} 安全落地，记录：{}\n{}",
                        record.executor,
                        record.log_path.display(),
                        record.summary
                    ),
                );
            }
            TaskCommand::Submit { task, by } => {
                let task = app.submit_task(&task, &by)?;
                output(&task, cli.json, format!("{} 已提交验收", task.id));
            }
            TaskCommand::Complete { task, by } => {
                let task = app.complete_task(&task, &by)?;
                output(
                    &task,
                    cli.json,
                    format!("{} 已确认落地。Mamba Out.", task.id),
                );
            }
        },
        Command::Inbox { target } => {
            let inbox = app.inbox(&target)?;
            let value = inbox
                .iter()
                .map(|(flow, task)| json!({ "flow": flow.id, "task": task }))
                .collect::<Vec<_>>();
            let text = inbox
                .iter()
                .map(|(flow, task)| format!("{}\t{}\t{}", flow.id, task.id, task_summary(task)))
                .collect::<Vec<_>>()
                .join("\n");
            output(&value, cli.json, text);
        }
        Command::Track { command } => match command {
            TrackCommand::Scan {
                stale_hours,
                escalate_after_hours,
                by,
            } => {
                let scan = app.scan_tracking_with_policy(stale_hours, escalate_after_hours, &by)?;
                let text = format!(
                    "塔台扫描 {} 个 Todo：新增 {}，解除 {}，活动 {}，升级 {}\n{}",
                    scan.scanned_tasks,
                    scan.raised.len(),
                    scan.resolved.len(),
                    scan.active.len(),
                    scan.escalated.len(),
                    scan.active
                        .iter()
                        .map(tracking_attention_line)
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                output(&scan, cli.json, text.trim_end().to_string());
            }
            TrackCommand::List { flow, all } => {
                if let Some(flow_id) = &flow {
                    app.state().flow(flow_id)?;
                }
                let mut attentions = app
                    .state()
                    .attentions
                    .values()
                    .filter(|attention| all || attention.is_active())
                    .filter(|attention| {
                        flow.as_deref()
                            .is_none_or(|flow_id| attention.flow_id == flow_id)
                    })
                    .collect::<Vec<_>>();
                attentions.sort_by(|left, right| {
                    right
                        .severity
                        .cmp(&left.severity)
                        .then_with(|| right.raised_at.cmp(&left.raised_at))
                        .then_with(|| left.id.cmp(&right.id))
                });
                let text = attentions
                    .iter()
                    .map(|attention| tracking_attention_line(attention))
                    .collect::<Vec<_>>()
                    .join("\n");
                output(&attentions, cli.json, text);
            }
            TrackCommand::Inbox { target, all } => {
                let escalations = app.escalation_inbox(&target, all)?;
                let text = escalations
                    .iter()
                    .map(|escalation| tracking_escalation_line(escalation))
                    .collect::<Vec<_>>()
                    .join("\n");
                output(&escalations, cli.json, text);
            }
            TrackCommand::Ack { escalation, by } => {
                let escalation = app.acknowledge_escalation(&escalation, &by)?;
                output(
                    &escalation,
                    cli.json,
                    format!("{} 已收到呼叫 {}", by, escalation.id),
                );
            }
        },
        Command::Gitlab { command } => match command {
            GitLabCommand::Check { project, url } => {
                let client = GitLabClient::from_env(url.as_deref())?;
                let project = client.check_project(&project).await?;
                let text = format!(
                    "GitLab ready: {} (project {})\n{}",
                    project.path_with_namespace, project.id, project.web_url
                );
                output(&project, cli.json, text);
            }
            GitLabCommand::Sync {
                task,
                project,
                mr,
                by,
                url,
            } => {
                app.authorize_task_actor(&task, &by)?;
                let client = GitLabClient::from_env(url.as_deref())?;
                let snapshot = client.merge_request_snapshot(&project, mr).await?;
                let changed =
                    app.sync_external_artifacts(&task, &by, snapshot.artifacts.clone())?;
                let artifact_lines = snapshot
                    .artifacts
                    .iter()
                    .map(|artifact| {
                        format!(
                            "{} #{}\t{}\t{}",
                            artifact.kind,
                            artifact.external_id,
                            artifact.status,
                            if artifact.verified {
                                "verified"
                            } else {
                                "pending"
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let text = format!(
                    "{} / MR !{} 已同步到 {}，{} 个快照发生变化\n{}",
                    snapshot.project.path_with_namespace,
                    snapshot.merge_request_iid,
                    task,
                    changed.len(),
                    artifact_lines
                );
                output(
                    &json!({"snapshot": snapshot, "changed": changed}),
                    cli.json,
                    text,
                );
            }
        },
        Command::Worker { command } => match command {
            WorkerCommand::Once(args) => {
                let worker = remote_worker(args, app.data_dir())?;
                let outcome = worker.run_once().await?;
                output(&outcome, cli.json, worker_outcome_text(&outcome));
            }
            WorkerCommand::Run {
                worker: args,
                poll_seconds,
            } => {
                if poll_seconds == 0 {
                    return Err(manbaflow::MambaError::Validation(
                        "worker poll interval must be greater than zero".into(),
                    ));
                }
                let worker = remote_worker(args, app.data_dir())?;
                loop {
                    let outcome = worker.run_once().await?;
                    output(&outcome, cli.json, worker_outcome_text(&outcome));
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(poll_seconds)) => {}
                    }
                }
            }
        },
        Command::Timeline { flow } => {
            let events = app.timeline(&flow)?;
            let text = events
                .iter()
                .map(|event| {
                    format!(
                        "#{:<4} {} {:<28} {}",
                        event.sequence,
                        event.occurred_at.format("%Y-%m-%d %H:%M:%S UTC"),
                        event.kind,
                        event.actor
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            output(&events, cli.json, text);
        }
        Command::Executor { command } => match command {
            ExecutorCommand::Check { kind, executable } => {
                let kind: ExecutorKind = kind.into();
                let command = executable.unwrap_or_else(|| match kind {
                    ExecutorKind::ClaudeCode => PathBuf::from("claude"),
                    ExecutorKind::Codex => PathBuf::from("codex"),
                });
                let result = std::process::Command::new(&command)
                    .arg("--version")
                    .output()?;
                if !result.status.success() {
                    return Err(manbaflow::MambaError::ExecutorFailed {
                        code: result.status.code(),
                        message: String::from_utf8_lossy(&result.stderr).into_owned(),
                    });
                }
                let version = String::from_utf8_lossy(&result.stdout).trim().to_string();
                output(
                    &json!({"executor": kind, "command": command, "version": version}),
                    cli.json,
                    format!("{} ready: {}", kind, version),
                );
            }
        },
        Command::Demo { workspace } => bootstrap_demo(&mut app, &workspace, cli.json)?,
    }
    Ok(())
}

fn bootstrap_demo(app: &mut MambaApp, workspace: &Path, json_output: bool) -> Result<()> {
    let workspace = absolute_path(workspace)?;
    let org = app.init_organization("Mamba Labs", "admin")?;
    let team = app.create_team(
        "洛杉矶研发队",
        "product,backend,rust,llm-platform,security,quality,observability,operations",
        "admin",
    )?;
    let leader = app.register_principal(
        "牢大",
        PrincipalKind::Human,
        Some(&team.id),
        None,
        "product,llm-platform,operations",
        80,
        None,
        "admin",
    )?;
    let engineer = app.register_principal(
        "佐巴扬",
        PrincipalKind::Human,
        Some(&team.id),
        None,
        "backend,rust,llm-platform,security,quality,observability",
        100,
        None,
        "admin",
    )?;
    let codex = app.register_principal(
        "Codex 副驾",
        PrincipalKind::Agent,
        Some(&team.id),
        Some(&engineer.id),
        "backend,rust,llm-platform,security,quality,observability",
        100,
        Some(ExecutorConfig {
            kind: ExecutorKind::Codex,
            workspace: workspace.clone(),
            model: None,
            command: None,
        }),
        "admin",
    )?;
    let claude = app.register_principal(
        "Claude Code 副驾",
        PrincipalKind::Agent,
        Some(&team.id),
        Some(&leader.id),
        "product,llm-platform,operations,backend",
        100,
        Some(ExecutorConfig {
            kind: ExecutorKind::ClaudeCode,
            workspace,
            model: None,
            command: None,
        }),
        "admin",
    )?;
    output(
        &json!({
            "organization": org,
            "team": team,
            "humans": [leader, engineer],
            "agents": [codex, claude]
        }),
        json_output,
        "演示阵容就位：牢大与佐巴扬带队，Codex 和 Claude Code 已进入轮换。".into(),
    );
    Ok(())
}

fn print_chart(app: &MambaApp, json_output: bool) -> Result<()> {
    let org = app.state().organization()?;
    let data = json!({
        "organization": org,
        "teams": app.state().teams.values().collect::<Vec<_>>(),
        "principals": app.state().principals.values().collect::<Vec<_>>()
    });
    let mut lines = vec![format!("{} ({})", org.name, org.id)];
    for team in app.state().teams.values() {
        lines.push(format!("├─ {} ({})", team.name, team.id));
        for principal in app
            .state()
            .principals
            .values()
            .filter(|principal| principal.team_id.as_deref() == Some(team.id.as_str()))
        {
            let terminal = principal
                .executor
                .as_ref()
                .map(|executor| format!(" [{}]", executor.kind))
                .unwrap_or_default();
            lines.push(format!(
                "│  └─ {} {:?}{}",
                principal.name, principal.kind, terminal
            ));
        }
    }
    output(&data, json_output, lines.join("\n"));
    Ok(())
}

fn output(value: &impl Serialize, json_output: bool, text: String) {
    if json_output {
        match serde_json::to_string_pretty(value) {
            Ok(value) => println!("{value}"),
            Err(_) => println!("{{\"error\":\"output serialization failed\"}}"),
        }
    } else if text.is_empty() {
        println!("(empty)");
    } else {
        println!("{text}");
    }
}

fn flow_summary(flow: &Flow) -> String {
    format!(
        "{}\t{:?}\t{}\nP50 {} · P80 {} · {} tasks",
        flow.id,
        flow.status,
        flow.prd.title,
        flow.p50_finish.format("%Y-%m-%d %H:%M UTC"),
        flow.p80_finish.format("%Y-%m-%d %H:%M UTC"),
        flow.tasks.len()
    )
}

fn detailed_flow(flow: &Flow) -> String {
    let mut lines = vec![flow_summary(flow), format!("\n{}", flow.prd.summary)];
    lines.push(format!("\n关键路径: {}", flow.critical_path.join(" -> ")));
    lines.push("\n任务: ".into());
    lines.extend(flow.tasks.iter().map(task_summary));
    lines.join("\n")
}

fn task_summary(task: &Task) -> String {
    let owner = task
        .assignment
        .as_ref()
        .map(|assignment| assignment.owner.name.as_str())
        .unwrap_or("未分配");
    format!(
        "{}\t{:?}\t{}\t{}\tP50 {:.1}h/P80 {:.1}h",
        task.id, task.status, owner, task.title, task.estimate.p50_hours, task.estimate.p80_hours
    )
}

fn task_details(task: &Task) -> String {
    let mut lines = vec![task_summary(task)];
    if !task.external_artifacts.is_empty() {
        lines.push("\n外部交付物:".into());
        lines.extend(task.external_artifacts.iter().map(|artifact| {
            format!(
                "{}:{} #{}\t{}\t{}\n{}",
                artifact.provider,
                artifact.kind,
                artifact.external_id,
                artifact.status,
                if artifact.verified {
                    "verified"
                } else {
                    "pending"
                },
                artifact.url
            )
        }));
    }
    lines.join("\n")
}

fn remote_worker(args: WorkerArgs, data_dir: &Path) -> Result<RemoteWorker> {
    let token = std::env::var("MAMBA_TOKEN").map_err(|_| {
        manbaflow::MambaError::Validation("MAMBA_TOKEN is required for a remote worker".into())
    })?;
    let server_url = args
        .server
        .or_else(|| std::env::var("MAMBA_SERVER").ok())
        .unwrap_or_else(|| "http://127.0.0.1:7777".into());
    RemoteWorker::new(WorkerOptions {
        server_url,
        token,
        executor: args.executor.into(),
        workspace: absolute_path(args.workspace)?,
        model: args.model,
        command: args.executable,
        task_id: args.task,
        timeout_seconds: args.timeout,
        data_dir: data_dir.to_path_buf(),
    })
}

fn worker_outcome_text(outcome: &WorkerOutcome) -> String {
    let task = outcome.task_id.as_deref().unwrap_or("-");
    match outcome.status {
        WorkerOutcomeStatus::Idle => format!("塔台静默：{}", outcome.summary),
        WorkerOutcomeStatus::Planned => format!(
            "{} 只读航班安全落地 · {}\n{}\n黑匣子：{}",
            outcome.principal,
            task,
            outcome.summary,
            outcome
                .log_path
                .as_deref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".into())
        ),
        WorkerOutcomeStatus::Crashed => format!(
            "{} 规划航班坠机 · {}\n{}\n黑匣子：{}",
            outcome.principal,
            task,
            outcome.summary,
            outcome
                .log_path
                .as_deref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".into())
        ),
    }
}

fn tracking_attention_line(attention: &TrackingAttention) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
        attention.id,
        if attention.is_active() {
            "active"
        } else {
            "resolved"
        },
        attention.severity,
        attention.kind,
        attention.flow_id,
        attention.task_id,
        attention.summary
    )
}

fn tracking_escalation_line(escalation: &TrackingEscalation) -> String {
    let status = if !escalation.is_active() {
        "resolved"
    } else if escalation.needs_acknowledgement() {
        "waiting_ack"
    } else {
        "acknowledged"
    };
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        escalation.id,
        status,
        escalation.recipient_name,
        escalation.flow_id,
        escalation.task_id,
        escalation.reason
    )
}

fn absolute_path(path: impl AsRef<Path>) -> Result<PathBuf> {
    Ok(if path.as_ref().is_absolute() {
        path.as_ref().to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    })
}

impl From<PrincipalKindArg> for PrincipalKind {
    fn from(value: PrincipalKindArg) -> Self {
        match value {
            PrincipalKindArg::Human => Self::Human,
            PrincipalKindArg::Agent => Self::Agent,
        }
    }
}

impl From<ExecutorKindArg> for ExecutorKind {
    fn from(value: ExecutorKindArg) -> Self {
        match value {
            ExecutorKindArg::ClaudeCode => Self::ClaudeCode,
            ExecutorKindArg::Codex => Self::Codex,
        }
    }
}

impl From<PlannerKindArg> for PlannerKind {
    fn from(value: PlannerKindArg) -> Self {
        match value {
            PlannerKindArg::Local => Self::Local,
            PlannerKindArg::ClaudeCode => Self::ClaudeCode,
            PlannerKindArg::Codex => Self::Codex,
        }
    }
}

impl From<ExecutorModeArg> for ExecutorMode {
    fn from(value: ExecutorModeArg) -> Self {
        match value {
            ExecutorModeArg::Plan => Self::Plan,
            ExecutorModeArg::Execute => Self::Execute,
        }
    }
}
