use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use manbaflow::calendar::{parse_workdays, summary as calendar_summary};
use manbaflow::dashboard::DashboardSnapshot;
use manbaflow::domain::{
    CapabilityPack, ExecutorConfig, ExecutorKind, ExecutorMode, FlightManifestDraft, Flow,
    FlowChangeRequest, FlowMessage, FlowMessageKind, NotificationConnector, NotificationDelivery,
    NotificationEndpoint, NotificationStatus, OrganizationRole, PrincipalKind, RecoveryAction,
    Task, TrackingAttention, TrackingEscalation, WorkCalendar,
};
use manbaflow::gitlab::GitLabClient;
use manbaflow::ids::new_id;
use manbaflow::planner::PlannerKind;
use manbaflow::sandbox::{DockerSandboxConfig, SandboxBackend, SandboxNetwork};
use manbaflow::showcase::seed_showcase;
use manbaflow::tenant::{TenantCatalog, TenantRecord, database_url_from_env, validate_slug};
use manbaflow::worker::{RemoteWorker, WorkerOptions, WorkerOutcome, WorkerOutcomeStatus};
use manbaflow::{MambaApp, MambaError, Result};
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

    /// 选择一个 Tenant ID 或 slug；默认使用根 Ledger
    #[arg(long, global = true)]
    tenant: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// 幂等初始化生产组织、首个团队、管理员和登录 Token
    Setup(SetupArgs),
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
        /// 明确确认非 loopback 监听的 HTTP 跳点由可信 TLS 代理保护
        #[arg(long)]
        allow_insecure_public_http: bool,
        #[arg(long, default_value_t = 30)]
        tracker_interval: u64,
        #[arg(long, default_value_t = 24)]
        stale_hours: u64,
        #[arg(long, default_value_t = 4)]
        escalate_after_hours: u64,
        #[arg(long, default_value_t = 15)]
        notification_interval: u64,
    },
    /// 创建、列举和选择相互隔离的企业 Tenant
    Tenant {
        #[command(subcommand)]
        command: TenantCommand,
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
    /// 在 Flow 内向团队、Human 或 Agent 传球并跟踪回执
    Message {
        #[command(subcommand)]
        command: MessageCommand,
    },
    /// 查看某个 Human 或 Agent 的工作收件箱
    Inbox {
        #[arg(long = "for")]
        target: String,
    },
    /// 查看管理员 Flow、风险、待办和航班看板
    Dashboard {
        #[arg(long = "as")]
        actor: String,
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
    /// 管理企业消息 Webhook 和可靠通知 Outbox
    Notification {
        #[command(subcommand)]
        command: NotificationCommand,
    },
    /// 在同事工作站运行 Personal Agent 航班
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
    /// 检查 Ledger 健康度并创建一致性备份
    Ops {
        #[command(subcommand)]
        command: OpsCommand,
    },
    /// 初始化一套牢大、佐巴扬与两个副驾的演示阵容
    Demo {
        #[arg(long, default_value = ".")]
        workspace: PathBuf,
        /// 同时生成三条可回放的管理员 Showcase Flow
        #[arg(long)]
        showcase: bool,
    },
}

#[derive(Args)]
struct SetupArgs {
    /// 企业或组织名称
    #[arg(long)]
    organization: String,
    /// 首位 Tenant 管理员
    #[arg(long)]
    administrator: String,
    /// 管理员所属的首个团队
    #[arg(long, default_value = "Core Team")]
    team: String,
    /// 首个团队和管理员具备的业务能力
    #[arg(long, default_value = "product,delivery,operations,backend,quality")]
    capabilities: String,
    /// 首次浏览器 Token 有效天数
    #[arg(long, default_value_t = 30)]
    token_ttl_days: u32,
    /// 撤销现有 bootstrap-admin Token 并签发新 Token
    #[arg(long)]
    rotate_token: bool,
    /// 管理员固定 UTC 偏移，例如 +08:00
    #[arg(long, default_value = "+00:00")]
    utc_offset: String,
    #[arg(long, default_value = "mon,tue,wed,thu,fri")]
    workdays: String,
    #[arg(long, default_value = "09:00")]
    day_start: String,
    #[arg(long, default_value = "18:00")]
    day_end: String,
}

#[derive(Subcommand)]
enum TenantCommand {
    /// 创建一个独立事件账本并登记到 Control Plane
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        slug: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 列出 Control Plane 中的所有 Tenant
    List,
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
    /// 配置工作时间和请假区间
    Calendar {
        #[command(subcommand)]
        command: CalendarCommand,
    },
    /// 管理远程 API Bearer Token
    Token {
        #[command(subcommand)]
        command: CredentialCommand,
    },
    /// 管理 Principal 的组织角色与权限
    Role {
        #[command(subcommand)]
        command: RoleCommand,
    },
    /// 管理 Slack、飞书或 Teams 用户与 Human Principal 的身份绑定
    Identity {
        #[command(subcommand)]
        command: IdentityCommand,
    },
}

#[derive(Subcommand)]
enum IdentityCommand {
    /// 将一个供应商用户绑定到 Human Principal
    Bind {
        #[arg(long = "for")]
        target: String,
        #[arg(long)]
        provider: String,
        #[arg(long)]
        external_user: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 查看外部身份绑定
    List {
        #[arg(long = "for")]
        target: Option<String>,
        #[arg(long)]
        provider: Option<String>,
        #[arg(long)]
        all: bool,
    },
    /// 停用一个身份绑定，历史交互回执仍保留
    Unbind {
        binding: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
}

#[derive(Subcommand)]
enum CalendarCommand {
    /// 查看 Principal 的工作日历和请假记录
    Show {
        #[arg(long = "for")]
        target: String,
    },
    /// 设置固定 UTC 偏移、工作日和每日工作时间
    Set {
        #[arg(long = "for")]
        target: String,
        #[arg(long)]
        utc_offset: String,
        #[arg(long, default_value = "mon,tue,wed,thu,fri")]
        days: String,
        #[arg(long, default_value = "09:00")]
        start: String,
        #[arg(long, default_value = "18:00")]
        end: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 登记不可用时间并重排受影响 Flow
    TimeOffAdd {
        #[arg(long = "for")]
        target: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        until: String,
        #[arg(long)]
        reason: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 取消请假并恢复排期
    TimeOffCancel {
        #[arg(long = "for")]
        target: String,
        block: String,
        #[arg(long, default_value = "admin")]
        by: String,
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
        #[arg(long, default_value_t = 30)]
        ttl_days: u32,
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
enum RoleCommand {
    /// 查看 Principal 的角色绑定
    List {
        #[arg(long = "for")]
        target: String,
        #[arg(long, default_value = "admin")]
        by: String,
        #[arg(long)]
        all: bool,
    },
    /// 授予组织角色
    Grant {
        #[arg(long = "for")]
        target: String,
        #[arg(long)]
        role: OrganizationRoleArg,
        #[arg(long)]
        by: String,
    },
    /// 撤销一个角色绑定
    Revoke {
        binding: String,
        #[arg(long)]
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
enum NotificationCommand {
    /// 注册一个使用环境变量密钥签名的 Webhook Endpoint
    EndpointAdd {
        #[arg(long)]
        name: String,
        #[arg(long)]
        url: String,
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "work_request.sent,flow_message.posted,task.blocked,task.submitted,tracking.escalation_raised,flow_change.proposed,flow_change.applied,flow_change.rejected,remote_flight.crashed,flow.completed"
        )]
        events: Vec<String>,
        #[arg(long)]
        secret_env: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 注册飞书、Slack 或 Teams 原生 Connector；Webhook URL 只从环境读取
    ConnectorAdd {
        #[arg(long)]
        name: String,
        #[arg(long, value_enum)]
        provider: NotificationConnectorArg,
        #[arg(long)]
        url_env: String,
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "work_request.sent,flow_message.posted,task.blocked,task.submitted,tracking.escalation_raised,flow_change.proposed,flow_change.applied,flow_change.rejected,remote_flight.crashed,flow.completed"
        )]
        events: Vec<String>,
        /// 飞书机器人开启签名校验时使用；Slack/Teams 不需要
        #[arg(long)]
        secret_env: Option<String>,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 查看 Webhook Endpoint
    EndpointList {
        #[arg(long)]
        all: bool,
    },
    /// 停用 Endpoint；已经落地的 Outbox 记录仍保留
    EndpointDisable {
        endpoint: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
    /// 查看通知投递记录
    Deliveries {
        #[arg(long)]
        all: bool,
    },
    /// 立即投递 Outbox；force 会忽略失败退避时间
    Dispatch {
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        force: bool,
        #[arg(long, default_value = "tower://cli-notification-dispatcher")]
        by: String,
    },
    /// 发送一条经过 Outbox 审计的测试卡片
    Test {
        endpoint: String,
        #[arg(long, default_value = "admin")]
        by: String,
    },
}

#[derive(Subcommand)]
enum WorkerCommand {
    /// 领取并执行一个远程规划或写入航班后退出
    Once(WorkerArgs),
    /// 持续轮询远程 Inbox 或 Flight Lease，串行执行航班
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
    /// 只读规划或消费 Human 授权的写入租约
    #[arg(long, value_enum, default_value = "plan")]
    mode: ExecutorModeArg,
    #[arg(long, default_value = ".")]
    workspace: PathBuf,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    executable: Option<PathBuf>,
    /// 只处理指定 Task；默认选择第一个可执行任务
    #[arg(long)]
    task: Option<String>,
    #[arg(long, default_value_t = 900)]
    timeout: u64,
    /// 执行进程隔离后端；生产 Remote Worker 应使用 docker
    #[arg(long, value_enum, default_value = "docker")]
    sandbox: SandboxBackendArg,
    #[arg(long, default_value = "docker")]
    sandbox_runtime: PathBuf,
    #[arg(long, default_value = "manbaflow-agent-runtime:0.1.0")]
    sandbox_image: String,
    /// none 禁止联网；云端模型需要显式选择 bridge 并由出口策略约束
    #[arg(long, value_enum, default_value = "none")]
    sandbox_network: SandboxNetworkArg,
    #[arg(long, default_value_t = 2_000)]
    sandbox_cpus_millis: u32,
    #[arg(long, default_value_t = 4_096)]
    sandbox_memory_mb: u64,
    #[arg(long, default_value_t = 256)]
    sandbox_pids: u32,
    #[arg(long, default_value_t = 512)]
    sandbox_tmpfs_mb: u64,
    /// 容器内非 root UID:GID；默认沿用 Worker 宿主用户
    #[arg(long)]
    sandbox_user: Option<String>,
    /// 仅按变量名转发；可重复，例如 --sandbox-env OPENAI_API_KEY
    #[arg(long = "sandbox-env")]
    sandbox_environment: Vec<String>,
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
    /// 为运行中的 Flow 生成 append-only 变更与影响预览
    ChangePropose {
        flow: String,
        summary: String,
        #[arg(long)]
        by: String,
        #[arg(long, value_enum, default_value = "local")]
        planner: PlannerKindArg,
        #[arg(long, default_value = ".")]
        workspace: PathBuf,
        #[arg(long, default_value_t = 300)]
        timeout: u64,
    },
    /// 查看 Flow 的变更历史与待审批影响
    Changes {
        flow: String,
        #[arg(long = "as")]
        actor: String,
    },
    /// Requester 批准影响预览并追加新任务
    ChangeApprove {
        change: String,
        #[arg(long)]
        by: String,
    },
    /// Requester 拒绝一份变更预览
    ChangeReject {
        change: String,
        #[arg(long)]
        by: String,
        #[arg(long)]
        reason: String,
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
    /// 查看当前任务可改派的 Human、Agent 和团队
    ReassignmentCandidates {
        task: String,
        #[arg(long)]
        by: String,
    },
    /// Demand Requester 改派任务并动态重算整条 Flow
    Reassign {
        task: String,
        #[arg(long)]
        by: String,
        #[arg(long = "to")]
        owner: String,
        #[arg(long = "copilot")]
        copilots: Vec<String>,
        #[arg(long)]
        reason: String,
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
    /// Human 为自己的远程 Agent 签发一次性写入 Flight Lease
    Authorize {
        task: String,
        #[arg(long)]
        by: String,
        #[arg(long)]
        agent: String,
        #[arg(long, value_enum)]
        executor: ExecutorKindArg,
        #[arg(long, default_value_t = 3_600)]
        ttl_seconds: u64,
        #[arg(long, value_enum)]
        pack: Option<CapabilityPackArg>,
        /// JSON 格式 FlightManifestDraft；省略时由塔台按任务生成
        #[arg(long)]
        manifest: Option<PathBuf>,
    },
    /// Human 在远程 Agent 起飞前撤销写入租约
    RevokeLease {
        lease: String,
        #[arg(long)]
        by: String,
    },
    /// 查看塔台按坠机类型与 FlightManifest 给出的恢复动作
    RecoveryOptions {
        lease: String,
        #[arg(long)]
        by: String,
    },
    /// Human 选择监督树恢复动作；可复飞、换执行器、缩小范围、转人工或停飞
    Recover {
        lease: String,
        #[arg(long)]
        by: String,
        #[arg(long, value_enum)]
        action: RecoveryActionArg,
        #[arg(long)]
        reason: String,
        #[arg(long, value_enum)]
        executor: Option<ExecutorKindArg>,
        #[arg(long)]
        objective: Option<String>,
        #[arg(long, default_value_t = 3_600)]
        ttl_seconds: u64,
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
enum MessageCommand {
    /// 发送关联 Flow 或 Task 的结构化指令
    Send {
        flow: String,
        body: String,
        #[arg(long)]
        task: Option<String>,
        #[arg(long)]
        by: String,
        #[arg(long = "to", required = true)]
        recipients: Vec<String>,
        #[arg(long, value_enum, default_value = "command")]
        kind: FlowMessageKindArg,
        /// 消息无需接收方显式确认
        #[arg(long)]
        no_ack: bool,
    },
    /// 查看某个 Principal 收到的 Flow 消息
    Inbox {
        #[arg(long = "for")]
        target: String,
        #[arg(long)]
        all: bool,
    },
    /// 查看当前 Principal 有权读取的 Flow 对话
    Thread {
        flow: String,
        #[arg(long = "as")]
        actor: String,
    },
    /// 确认已经收到一条要求回执的消息
    Ack {
        message: String,
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
enum OpsCommand {
    /// 执行 SQLite quick_check 并报告 Schema、WAL、事件与凭据状态
    Doctor,
    /// 使用 SQLite VACUUM INTO 创建不覆盖已有文件的一致性快照
    Backup {
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// 把本地 SQLite Tenant Fleet 原样迁移到共享 PostgreSQL 数据面
    MigratePostgres {
        /// 保存目标 PostgreSQL URL 的环境变量名
        #[arg(long, default_value = "MAMBA_TARGET_DATABASE_URL")]
        target_env: String,
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

#[derive(Clone, ValueEnum)]
enum SandboxBackendArg {
    Process,
    Docker,
}

#[derive(Clone, ValueEnum)]
enum SandboxNetworkArg {
    None,
    Bridge,
}

#[derive(Clone, ValueEnum)]
enum RecoveryActionArg {
    Retry,
    SwitchExecutor,
    ReduceScope,
    HumanHandoff,
    Ground,
    Fork,
}

#[derive(Clone, ValueEnum)]
enum CapabilityPackArg {
    General,
    Coding,
    Office,
}

#[derive(Clone, ValueEnum)]
enum FlowMessageKindArg {
    Command,
    Question,
    Update,
    Decision,
}

#[derive(Clone, ValueEnum)]
enum NotificationConnectorArg {
    Feishu,
    Slack,
    Teams,
}

#[derive(Clone, ValueEnum)]
enum OrganizationRoleArg {
    TenantAdmin,
    OrganizationAdmin,
    Manager,
    Member,
    Auditor,
}

impl From<NotificationConnectorArg> for NotificationConnector {
    fn from(value: NotificationConnectorArg) -> Self {
        match value {
            NotificationConnectorArg::Feishu => Self::Feishu,
            NotificationConnectorArg::Slack => Self::Slack,
            NotificationConnectorArg::Teams => Self::Teams,
        }
    }
}

impl From<OrganizationRoleArg> for OrganizationRole {
    fn from(value: OrganizationRoleArg) -> Self {
        match value {
            OrganizationRoleArg::TenantAdmin => Self::TenantAdmin,
            OrganizationRoleArg::OrganizationAdmin => Self::OrganizationAdmin,
            OrganizationRoleArg::Manager => Self::Manager,
            OrganizationRoleArg::Member => Self::Member,
            OrganizationRoleArg::Auditor => Self::Auditor,
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let command = cli.command.unwrap_or_else(|| Command::Tui {
        actor: None,
        workspace: PathBuf::from("."),
    });
    if cli.tenant.is_some() && matches!(&command, Command::Serve { .. } | Command::Tenant { .. }) {
        return Err(MambaError::Validation(
            "--tenant selects a CLI Ledger and cannot be combined with serve or tenant management"
                .into(),
        ));
    }
    let allow_postgres_bootstrap = matches!(
        &command,
        Command::Setup(_)
            | Command::Org {
                command: OrgCommand::Init { .. }
            }
    );
    let mut app = open_selected_app(
        &cli.data_dir,
        cli.tenant.as_deref(),
        allow_postgres_bootstrap,
    )?;
    match command {
        Command::Setup(args) => {
            let setup = app.setup_installation(manbaflow::app::InstallationSetupOptions {
                organization_name: args.organization,
                team_name: args.team,
                administrator_name: args.administrator,
                capabilities: args.capabilities,
                token_ttl_days: args.token_ttl_days,
                rotate_token: args.rotate_token,
            })?;
            let utc_offset = parse_utc_offset(&args.utc_offset)?;
            let workdays = parse_workdays(&args.workdays)?;
            let day_start = parse_clock_minute(&args.day_start, false)?;
            let day_end = parse_clock_minute(&args.day_end, true)?;
            let current = app.state().work_calendar(&setup.administrator.id)?.clone();
            let calendar = if current.utc_offset_minutes == utc_offset
                && current.working_days == workdays
                && current.day_start_minute == day_start
                && current.day_end_minute == day_end
            {
                current
            } else {
                app.configure_work_calendar(
                    &setup.administrator.id,
                    utc_offset,
                    workdays,
                    day_start,
                    day_end,
                    &setup.administrator.id,
                )?
            };
            if cli.tenant.is_none() {
                let mut catalog = TenantCatalog::configured(&cli.data_dir)?;
                catalog.adopt_default(app.state().tenant()?)?;
            }
            let token_message = setup.token.as_deref().map_or_else(
                || {
                    "现有 bootstrap-admin Token 保持有效；如已遗失，请加 --rotate-token 重新签发"
                        .to_string()
                },
                |token| format!("管理员 Token 只显示一次：{token}"),
            );
            output(
                &json!({"setup": setup, "calendar": calendar}),
                cli.json,
                format!(
                    "生产塔台就位：{} · {} · {}\n{}\nConsole：http://127.0.0.1:7777/console",
                    setup.organization.name,
                    setup.team.name,
                    setup.administrator.name,
                    token_message
                ),
            );
        }
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
            allow_insecure_public_http,
            tracker_interval,
            stale_hours,
            escalate_after_hours,
            notification_interval,
        } => {
            manbaflow::server::run_fleet(
                &cli.data_dir,
                manbaflow::server::ServerOptions {
                    bind,
                    allow_insecure_public_http,
                    tracker_interval_seconds: tracker_interval,
                    stale_after_hours: stale_hours,
                    escalate_after_hours,
                    notification_interval_seconds: notification_interval,
                },
            )
            .await?;
        }
        Command::Tenant { command } => {
            let tenant = app.state().tenant()?.clone();
            let mut catalog = TenantCatalog::configured(&cli.data_dir)?;
            catalog.adopt_default(&tenant)?;
            match command {
                TenantCommand::Create { name, slug, by } => {
                    let slug = validate_slug(&slug)?;
                    if catalog.find(&slug)?.is_some() {
                        return Err(MambaError::Validation(format!(
                            "tenant already exists: {slug}"
                        )));
                    }
                    let relative_path = PathBuf::from("tenants").join(&slug);
                    let tenant_data_dir = cli.data_dir.join(&relative_path);
                    if tenant_data_dir.join("flow.db").exists() {
                        return Err(MambaError::Validation(format!(
                            "unregistered tenant Ledger already exists: {}",
                            tenant_data_dir.display()
                        )));
                    }
                    let mut tenant_app = if let Some(database_url) = database_url_from_env()? {
                        MambaApp::open_postgres(&tenant_data_dir, &database_url, &new_id("TEN"))?
                    } else {
                        MambaApp::open(&tenant_data_dir)?
                    };
                    tenant_app.init_organization(&name, &by)?;
                    let tenant = tenant_app.state().tenant()?.clone();
                    let record = catalog.register(&tenant, &slug, &relative_path)?;
                    output(
                        &record,
                        cli.json,
                        format!(
                            "Tenant 已起飞：{} ({})，CLI 使用 --tenant {} 进入",
                            record.name, record.id, record.slug
                        ),
                    );
                }
                TenantCommand::List => {
                    let records = catalog.list()?;
                    output(&records, cli.json, format_tenant_records(&records));
                }
            }
        }
        Command::Org { command } => match command {
            OrgCommand::Init { name, by } => {
                let org = app.init_organization(&name, &by)?;
                let mut catalog = TenantCatalog::configured(&cli.data_dir)?;
                catalog.adopt_default(app.state().tenant()?)?;
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
                            "{}\t{}\t{:?}\t{}%\t{}\t{}",
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
                                }),
                            app.state()
                                .work_calendar(&principal.id)
                                .map(calendar_summary)
                                .unwrap_or_else(|_| "calendar unavailable".into())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            PrincipalCommand::Calendar { command } => match command {
                CalendarCommand::Show { target } => {
                    let calendar = app.state().work_calendar(&target)?;
                    output(calendar, cli.json, calendar_text(calendar));
                }
                CalendarCommand::Set {
                    target,
                    utc_offset,
                    days,
                    start,
                    end,
                    by,
                } => {
                    let calendar = app.configure_work_calendar(
                        &target,
                        parse_utc_offset(&utc_offset)?,
                        parse_workdays(&days)?,
                        parse_clock_minute(&start, false)?,
                        parse_clock_minute(&end, true)?,
                        &by,
                    )?;
                    output(
                        &calendar,
                        cli.json,
                        format!("{} 工作日历已更新：{}", target, calendar_summary(&calendar)),
                    );
                }
                CalendarCommand::TimeOffAdd {
                    target,
                    from,
                    until,
                    reason,
                    by,
                } => {
                    let block = app.add_time_off(
                        &target,
                        parse_datetime(&from)?,
                        parse_datetime(&until)?,
                        &reason,
                        &by,
                    )?;
                    output(
                        &block,
                        cli.json,
                        format!(
                            "{} 已登记不可用：{} 到 {} ({})",
                            block.id, block.starts_at, block.ends_at, block.reason
                        ),
                    );
                }
                CalendarCommand::TimeOffCancel { target, block, by } => {
                    let block = app.cancel_time_off(&target, &block, &by)?;
                    output(
                        &block,
                        cli.json,
                        format!("{} 已取消，相关 Flow 已重新排期", block.id),
                    );
                }
            },
            PrincipalCommand::Token { command } => match command {
                CredentialCommand::Issue {
                    target,
                    label,
                    ttl_days,
                    by,
                } => {
                    let issued =
                        app.issue_api_credential_with_ttl(&target, &label, &by, ttl_days)?;
                    output(
                        &issued,
                        cli.json,
                        format!(
                            "{} · {} 到期\nToken 只显示一次：{}",
                            issued.credential.id,
                            issued.credential.expires_at.as_ref().unwrap(),
                            issued.token
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
            PrincipalCommand::Identity { command } => match command {
                IdentityCommand::Bind {
                    target,
                    provider,
                    external_user,
                    by,
                } => {
                    let binding =
                        app.bind_external_identity(&provider, &external_user, &target, &by)?;
                    output(
                        &binding,
                        cli.json,
                        format!(
                            "{} 已绑定 {}:{} -> {}",
                            binding.id, binding.provider, binding.external_user_id, target
                        ),
                    );
                }
                IdentityCommand::List {
                    target,
                    provider,
                    all,
                } => {
                    let principal_id = target
                        .as_deref()
                        .map(|target| app.state().principal(target).map(|value| value.id.clone()))
                        .transpose()?;
                    let provider = provider.map(|value| value.trim().to_ascii_lowercase());
                    let mut bindings = app
                        .state()
                        .external_identities
                        .values()
                        .filter(|binding| all || binding.is_active())
                        .filter(|binding| {
                            principal_id
                                .as_deref()
                                .is_none_or(|id| binding.principal_id == id)
                        })
                        .filter(|binding| {
                            provider
                                .as_deref()
                                .is_none_or(|value| binding.provider == value)
                        })
                        .collect::<Vec<_>>();
                    bindings.sort_by_key(|binding| binding.bound_at);
                    let text = bindings
                        .iter()
                        .map(|binding| {
                            format!(
                                "{}\t{}\t{}:{}\t{}",
                                binding.id,
                                if binding.is_active() {
                                    "ACTIVE"
                                } else {
                                    "UNBOUND"
                                },
                                binding.provider,
                                binding.external_user_id,
                                binding.principal_id
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    output(&bindings, cli.json, text);
                }
                IdentityCommand::Unbind { binding, by } => {
                    let binding = app.unbind_external_identity(&binding, &by)?;
                    output(
                        &binding,
                        cli.json,
                        format!("{} 已解绑，历史交互回执保留", binding.id),
                    );
                }
            },
            PrincipalCommand::Role { command } => match command {
                RoleCommand::List { target, by, all } => {
                    let bindings = app.role_bindings(&target, &by, all)?;
                    let text = bindings
                        .iter()
                        .map(|binding| {
                            format!(
                                "{}\t{}\t{}\t{}",
                                binding.id,
                                if binding.is_active() {
                                    "ACTIVE"
                                } else {
                                    "REVOKED"
                                },
                                binding.role,
                                binding.principal_id
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    output(&bindings, cli.json, text);
                }
                RoleCommand::Grant { target, role, by } => {
                    let binding = app.grant_role(&target, role.into(), &by)?;
                    output(
                        &binding,
                        cli.json,
                        format!("{} 已获得 {}", binding.principal_id, binding.role),
                    );
                }
                RoleCommand::Revoke { binding, by } => {
                    let binding = app.revoke_role(&binding, &by)?;
                    output(&binding, cli.json, format!("{} 已撤销", binding.id));
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
            FlowCommand::ChangePropose {
                flow,
                summary,
                by,
                planner,
                workspace,
                timeout,
            } => {
                let workspace = absolute_path(workspace)?;
                let change = app
                    .propose_flow_change(&flow, &by, &summary, planner.into(), &workspace, timeout)
                    .await?;
                output(&change, cli.json, flow_change_summary(&change));
            }
            FlowCommand::Changes { flow, actor } => {
                let changes = app.flow_changes(&flow, &actor)?;
                let text = changes
                    .iter()
                    .map(flow_change_summary)
                    .collect::<Vec<_>>()
                    .join("\n\n");
                output(&changes, cli.json, text);
            }
            FlowCommand::ChangeApprove { change, by } => {
                let change = app.approve_flow_change(&change, &by)?;
                output(
                    &change,
                    cli.json,
                    format!("{} 已批准并进入正式 Flow", change.id),
                );
            }
            FlowCommand::ChangeReject { change, by, reason } => {
                let change = app.reject_flow_change(&change, &by, &reason)?;
                output(
                    &change,
                    cli.json,
                    format!("{} 已拒绝：{}", change.id, reason),
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
            TaskCommand::ReassignmentCandidates { task, by } => {
                let candidates = app.reassignment_candidates(&task, &by)?;
                let text = candidates
                    .iter()
                    .map(|candidate| {
                        format!("{:?}\t{}\t{}", candidate.kind, candidate.id, candidate.name)
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                output(&candidates, cli.json, text);
            }
            TaskCommand::Reassign {
                task,
                by,
                owner,
                copilots,
                reason,
            } => {
                let flow = app.reassign_task(&task, &by, &owner, &copilots, &reason)?;
                let task = flow.task(&task).expect("reassigned task remains in flow");
                output(
                    &flow,
                    cli.json,
                    format!(
                        "{} 已换防给 {}；Flow P80 更新为 {}",
                        task.id,
                        task.assignment.as_ref().unwrap().owner.name,
                        flow.p80_finish.format("%Y-%m-%d %H:%M UTC")
                    ),
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
            TaskCommand::Authorize {
                task,
                by,
                agent,
                executor,
                ttl_seconds,
                pack,
                manifest,
            } => {
                let mut manifest = manifest
                    .map(|path| {
                        let bytes = std::fs::read(path)?;
                        Ok::<_, MambaError>(serde_json::from_slice::<FlightManifestDraft>(&bytes)?)
                    })
                    .transpose()?
                    .unwrap_or_default();
                if let Some(pack) = pack {
                    manifest.capability_pack = Some(pack.into());
                }
                let lease = app.authorize_remote_flight_with_manifest(
                    &task,
                    &by,
                    &agent,
                    executor.into(),
                    ttl_seconds,
                    manifest,
                )?;
                output(
                    &lease,
                    cli.json,
                    format!(
                        "{} 已放行 {}，租约 {}，{} 前必须起飞",
                        lease.authorized_by,
                        lease.principal_name,
                        lease.id,
                        lease.expires_at.format("%Y-%m-%d %H:%M:%S UTC")
                    ),
                );
            }
            TaskCommand::RevokeLease { lease, by } => {
                let lease = app.revoke_remote_flight(&lease, &by)?;
                output(&lease, cli.json, format!("{} 已撤销租约 {}", by, lease.id));
            }
            TaskCommand::RecoveryOptions { lease, by } => {
                let options = app.recovery_options(&lease, &by)?;
                output(
                    &options,
                    cli.json,
                    format!("租约 {lease} 可选恢复动作：{options:?}"),
                );
            }
            TaskCommand::Recover {
                lease,
                by,
                action,
                reason,
                executor,
                objective,
                ttl_seconds,
            } => {
                let recovered = app.recover_remote_flight(
                    &lease,
                    &by,
                    action.into(),
                    &reason,
                    executor.map(Into::into),
                    objective,
                    ttl_seconds,
                )?;
                let message = recovered.as_ref().map_or_else(
                    || format!("租约 {lease} 已转人工或永久停飞"),
                    |child| format!("租约 {lease} 已分叉复飞，新航班 {}", child.id),
                );
                output(&recovered, cli.json, message);
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
        Command::Message { command } => match command {
            MessageCommand::Send {
                flow,
                body,
                task,
                by,
                recipients,
                kind,
                no_ack,
            } => {
                let message = app.post_flow_message(
                    &flow,
                    task.as_deref(),
                    &by,
                    kind.into(),
                    &recipients,
                    &body,
                    !no_ack,
                )?;
                output(
                    &message,
                    cli.json,
                    format!(
                        "{} 已传球给 {}：{}",
                        message.id,
                        message
                            .recipients
                            .iter()
                            .map(|recipient| recipient.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", "),
                        message.body
                    ),
                );
            }
            MessageCommand::Inbox { target, all } => {
                let messages = app.message_inbox(&target, all)?;
                let text = messages
                    .iter()
                    .map(|item| {
                        flow_message_line(
                            &item.message,
                            if item.needs_acknowledgement() {
                                "WAITING_ACK"
                            } else {
                                "RECEIVED"
                            },
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                output(&messages, cli.json, text);
            }
            MessageCommand::Thread { flow, actor } => {
                let messages = app.flow_messages(&flow, &actor)?;
                let text = messages
                    .iter()
                    .map(|message| flow_message_line(message, "THREAD"))
                    .collect::<Vec<_>>()
                    .join("\n");
                output(&messages, cli.json, text);
            }
            MessageCommand::Ack { message, by } => {
                let message = app.acknowledge_flow_message(&message, &by)?;
                output(
                    &message,
                    cli.json,
                    format!("{} 已收到指令 {}", by, message.id),
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
        Command::Dashboard { actor } => {
            let dashboard = app.admin_dashboard(&actor)?;
            output(&dashboard, cli.json, dashboard_text(&dashboard));
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
        Command::Notification { command } => match command {
            NotificationCommand::EndpointAdd {
                name,
                url,
                events,
                secret_env,
                by,
            } => {
                let endpoint =
                    app.register_notification_endpoint(&name, &url, &events, &secret_env, &by)?;
                output(&endpoint, cli.json, notification_endpoint_text(&endpoint));
            }
            NotificationCommand::ConnectorAdd {
                name,
                provider,
                url_env,
                events,
                secret_env,
                by,
            } => {
                let endpoint = app.register_notification_connector(
                    &name,
                    provider.into(),
                    &url_env,
                    &events,
                    secret_env.as_deref(),
                    &by,
                )?;
                output(&endpoint, cli.json, notification_endpoint_text(&endpoint));
            }
            NotificationCommand::EndpointList { all } => {
                let mut endpoints = app
                    .state()
                    .notification_endpoints
                    .values()
                    .filter(|endpoint| all || endpoint.active)
                    .collect::<Vec<_>>();
                endpoints.sort_by_key(|endpoint| endpoint.created_at);
                let text = endpoints
                    .iter()
                    .map(|endpoint| notification_endpoint_text(endpoint))
                    .collect::<Vec<_>>()
                    .join("\n");
                output(&endpoints, cli.json, text);
            }
            NotificationCommand::EndpointDisable { endpoint, by } => {
                let endpoint = app.disable_notification_endpoint(&endpoint, &by)?;
                output(
                    &endpoint,
                    cli.json,
                    format!("{} 已停用，Outbox 历史保留", endpoint.id),
                );
            }
            NotificationCommand::Deliveries { all } => {
                let mut deliveries = app
                    .state()
                    .notification_deliveries
                    .values()
                    .filter(|delivery| {
                        all || matches!(
                            delivery.status,
                            NotificationStatus::Pending | NotificationStatus::Failed
                        )
                    })
                    .collect::<Vec<_>>();
                deliveries.sort_by_key(|delivery| std::cmp::Reverse(delivery.queued_at));
                let text = deliveries
                    .iter()
                    .map(|delivery| notification_delivery_text(delivery))
                    .collect::<Vec<_>>()
                    .join("\n");
                output(&deliveries, cli.json, text);
            }
            NotificationCommand::Dispatch { limit, force, by } => {
                let summary = app.dispatch_notifications(limit, force, &by).await?;
                output(
                    &summary,
                    cli.json,
                    format!(
                        "Outbox 投递 {} · LANDED {} · CRASHED {}",
                        summary.attempted, summary.delivered, summary.failed
                    ),
                );
            }
            NotificationCommand::Test { endpoint, by } => {
                let delivery = app.test_notification_endpoint(&endpoint, &by).await?;
                if delivery.status != NotificationStatus::Delivered {
                    return Err(MambaError::ExternalConnector(format!(
                        "{} test delivery crashed: {}",
                        delivery.id,
                        delivery.last_error.as_deref().unwrap_or("unknown error")
                    )));
                }
                output(
                    &delivery,
                    cli.json,
                    format!("{} 测试传球已安全落地", delivery.id),
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
        Command::Ops { command } => match command {
            OpsCommand::Doctor => {
                let health = app.storage_health()?;
                output(
                    &health,
                    cli.json,
                    format!(
                        "Ledger OK · schema {} · {} · {} events · {} active credentials",
                        health.schema_version,
                        health.journal_mode.to_uppercase(),
                        health.event_count,
                        health.active_credentials
                    ),
                );
            }
            OpsCommand::Backup {
                output: destination,
            } => {
                let destination = destination.unwrap_or_else(|| {
                    app.data_dir().join("backups").join(format!(
                        "mambaflow-{}.sqlite",
                        Utc::now().format("%Y%m%dT%H%M%SZ")
                    ))
                });
                let path = app.backup_storage(&destination)?;
                output(
                    &json!({"backup": path}),
                    cli.json,
                    format!("Ledger 快照已落地：{}", path.display()),
                );
            }
            OpsCommand::MigratePostgres { target_env } => {
                if app.uses_shared_storage() {
                    return Err(MambaError::Validation(
                        "PostgreSQL migration source must be a local SQLite data directory".into(),
                    ));
                }
                let database_url = std::env::var(&target_env).map_err(|_| {
                    MambaError::Validation(format!(
                        "target database URL environment variable is missing: {target_env}"
                    ))
                })?;
                let report =
                    manbaflow::migration::sqlite_fleet_to_postgres(&cli.data_dir, &database_url)?;
                output(
                    &report,
                    cli.json,
                    format!(
                        "PostgreSQL 数据面就位：{} Tenant，迁移 {}，幂等复核 {}，{} events，{} credentials，{} artifacts",
                        report.tenants,
                        report.migrated_tenants,
                        report.replayed_tenants,
                        report.events,
                        report.credentials,
                        report.artifacts
                    ),
                );
            }
        },
        Command::Demo {
            workspace,
            showcase,
        } => bootstrap_demo(&mut app, &workspace, showcase, cli.json).await?,
    }
    Ok(())
}

fn selected_tenant_data_dir(root: &Path, selector: Option<&str>) -> Result<PathBuf> {
    let Some(selector) = selector else {
        return Ok(root.to_path_buf());
    };
    let catalog = TenantCatalog::open(root)?;
    let record = catalog
        .find(selector)?
        .ok_or_else(|| MambaError::NotFound {
            entity: "tenant",
            id: selector.to_string(),
        })?;
    if !record.active {
        return Err(MambaError::PermissionDenied(format!(
            "tenant {} is inactive",
            record.id
        )));
    }
    catalog.data_dir(&record)
}

fn open_selected_app(
    root: &Path,
    selector: Option<&str>,
    allow_postgres_bootstrap: bool,
) -> Result<MambaApp> {
    let Some(database_url) = database_url_from_env()? else {
        return MambaApp::open(selected_tenant_data_dir(root, selector)?);
    };
    let catalog = TenantCatalog::postgres(root, &database_url)?;
    let record = if let Some(selector) = selector {
        catalog
            .find(selector)?
            .ok_or_else(|| MambaError::NotFound {
                entity: "tenant",
                id: selector.to_string(),
            })?
    } else if let Some(record) = catalog.default_tenant()? {
        record
    } else if allow_postgres_bootstrap {
        return MambaApp::open_postgres(root, &database_url, &new_id("TEN"));
    } else {
        return Err(MambaError::TenantNotInitialized);
    };
    if !record.active {
        return Err(MambaError::PermissionDenied(format!(
            "tenant {} is inactive",
            record.id
        )));
    }
    MambaApp::open_postgres(catalog.data_dir(&record)?, &database_url, &record.id)
}

fn format_tenant_records(records: &[TenantRecord]) -> String {
    if records.is_empty() {
        return "No tenants registered".into();
    }
    records
        .iter()
        .map(|record| {
            format!(
                "{} {:<18} {} ({})",
                if record.is_default { "*" } else { " " },
                record.slug,
                record.name,
                record.id
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn bootstrap_demo(
    app: &mut MambaApp,
    workspace: &Path,
    include_showcase: bool,
    json_output: bool,
) -> Result<()> {
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
    let workdays = parse_workdays("mon,tue,wed,thu,fri")?;
    app.configure_work_calendar(
        &leader.id,
        8 * 60,
        workdays.clone(),
        9 * 60,
        18 * 60,
        "admin",
    )?;
    app.configure_work_calendar(&engineer.id, 8 * 60, workdays, 9 * 60, 18 * 60, "admin")?;
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
            workspace: workspace.clone(),
            model: None,
            command: None,
        }),
        "admin",
    )?;
    let showcase = if include_showcase {
        Some(seed_showcase(app, &workspace, &leader.name).await?)
    } else {
        None
    };
    output(
        &json!({
            "organization": org,
            "team": team,
            "humans": [leader, engineer],
            "agents": [codex, claude],
            "showcase": showcase,
        }),
        json_output,
        if include_showcase {
            "演示塔台就位：3 条 Flow 已覆盖执行、阻塞、待验收、完成和远程 Flight Lease。".into()
        } else {
            "演示阵容就位：牢大与佐巴扬带队，Codex 和 Claude Code 已进入轮换。".into()
        },
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

fn calendar_text(calendar: &WorkCalendar) -> String {
    let mut lines = vec![calendar_summary(calendar)];
    lines.extend(calendar.time_off.iter().map(|block| {
        format!(
            "{}\t{}\t{} -> {}\t{}",
            block.id,
            if block.is_active() {
                "ACTIVE"
            } else {
                "CANCELLED"
            },
            block.starts_at,
            block.ends_at,
            block.reason
        )
    }));
    lines.join("\n")
}

fn notification_endpoint_text(endpoint: &NotificationEndpoint) -> String {
    let destination = endpoint
        .url_env
        .as_ref()
        .map(|name| format!("url=${name}"))
        .unwrap_or_else(|| endpoint.url.clone());
    let secret = if endpoint.secret_env.is_empty() {
        String::new()
    } else {
        format!("\tsecret=${}", endpoint.secret_env)
    };
    format!(
        "{}\t{}\t{}\t{}\t{}{}\t{}",
        endpoint.id,
        if endpoint.active {
            "ACTIVE"
        } else {
            "DISABLED"
        },
        endpoint.name,
        endpoint.connector.as_str(),
        destination,
        secret,
        endpoint.event_kinds.join(",")
    )
}

fn notification_delivery_text(delivery: &NotificationDelivery) -> String {
    format!(
        "{}\t{:?}\t{}\t{}\tattempts={}{}",
        delivery.id,
        delivery.status,
        delivery.source_event_kind,
        delivery.flow_id.as_deref().unwrap_or("organization"),
        delivery.attempts,
        delivery
            .last_error
            .as_ref()
            .map(|error| format!("\t{error}"))
            .unwrap_or_default()
    )
}

fn parse_datetime(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| {
            MambaError::Validation(format!(
                "invalid RFC3339 datetime `{value}`; example: 2026-07-20T09:00:00+08:00"
            ))
        })
}

fn parse_utc_offset(value: &str) -> Result<i32> {
    let value = value.trim();
    let sign = match value.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => {
            return Err(MambaError::Validation(
                "UTC offset must start with + or -, for example +08:00".into(),
            ));
        }
    };
    let (hours, minutes) = value[1..]
        .split_once(':')
        .ok_or_else(|| MambaError::Validation("UTC offset must use +HH:MM".into()))?;
    let hours = hours
        .parse::<i32>()
        .map_err(|_| MambaError::Validation("UTC offset hours must be numeric".into()))?;
    let minutes = minutes
        .parse::<i32>()
        .map_err(|_| MambaError::Validation("UTC offset minutes must be numeric".into()))?;
    if hours > 14 || minutes > 59 || (hours == 14 && minutes != 0) {
        return Err(MambaError::Validation(
            "UTC offset must be between -14:00 and +14:00".into(),
        ));
    }
    Ok(sign * (hours * 60 + minutes))
}

fn parse_clock_minute(value: &str, allow_end_of_day: bool) -> Result<u16> {
    if allow_end_of_day && value == "24:00" {
        return Ok(24 * 60);
    }
    let (hours, minutes) = value
        .split_once(':')
        .ok_or_else(|| MambaError::Validation("clock time must use HH:MM".into()))?;
    let hours = hours
        .parse::<u16>()
        .map_err(|_| MambaError::Validation("clock hours must be numeric".into()))?;
    let minutes = minutes
        .parse::<u16>()
        .map_err(|_| MambaError::Validation("clock minutes must be numeric".into()))?;
    if hours > 23 || minutes > 59 {
        return Err(MambaError::Validation(
            "clock time must be within 00:00..23:59".into(),
        ));
    }
    Ok(hours * 60 + minutes)
}

fn flow_change_summary(change: &FlowChangeRequest) -> String {
    let risks = if change.impact.risks.is_empty() {
        String::new()
    } else {
        format!("\n风险: {}", change.impact.risks.join(" · "))
    };
    format!(
        "{}\t{:?}\t+{} tasks\t{}\nP80 progress {:+.1}h + scope {:+.1}h = net {:+.1}h{}",
        change.id,
        change.status,
        change.new_tasks.len(),
        change.summary,
        change.impact.baseline_p80_delta_hours,
        change.impact.scope_p80_delta_hours,
        change.impact.net_p80_delta_hours,
        risks
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

fn dashboard_text(dashboard: &DashboardSnapshot) -> String {
    let metrics = &dashboard.metrics;
    let mut lines = vec![format!(
        "管理看板  Flow {}/{} active · Task {}/{} landed · Risk {} · Review {} · Flight {} · Outbox {}/{} failed",
        metrics.active_flows,
        metrics.total_flows,
        metrics.completed_tasks,
        metrics.total_tasks,
        metrics.at_risk_tasks,
        metrics.awaiting_human,
        metrics.open_flights,
        metrics.pending_notifications,
        metrics.failed_notifications,
    )];
    lines.push("\nFLOW BOARD".into());
    lines.extend(dashboard.flows.iter().map(|flow| {
        format!(
            "{:?}\t{:>3}%\t{}\t{}\tP80 {}",
            flow.health,
            flow.progress_percent,
            flow.id,
            flow.title,
            flow.p80_finish.format("%m-%d %H:%M UTC")
        )
    }));
    lines.push("\nACTION QUEUE".into());
    lines.extend(dashboard.action_items.iter().take(10).map(|action| {
        format!(
            "{:?}\t{}\t{}\t{}\t{}",
            action.priority, action.owner, action.task_id, action.task_title, action.reason
        )
    }));
    if !dashboard.flights.is_empty() {
        lines.push("\nFLIGHT DECK".into());
        lines.extend(dashboard.flights.iter().take(5).map(|flight| {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                flight.status,
                flight.executor,
                flight.sandbox_backend.as_deref().unwrap_or("pending"),
                flight.principal,
                flight.task_id,
                flight.id
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
    let sandbox: SandboxBackend = args.sandbox.into();
    let docker = (sandbox == SandboxBackend::Docker).then(|| DockerSandboxConfig {
        runtime: args.sandbox_runtime,
        image: args.sandbox_image,
        network: args.sandbox_network.into(),
        cpus_millis: args.sandbox_cpus_millis,
        memory_mb: args.sandbox_memory_mb,
        pids_limit: args.sandbox_pids,
        tmpfs_mb: args.sandbox_tmpfs_mb,
        user: args.sandbox_user,
        environment: args.sandbox_environment,
    });
    RemoteWorker::new(WorkerOptions {
        server_url,
        token,
        executor: args.executor.into(),
        mode: args.mode.into(),
        workspace: absolute_path(args.workspace)?,
        model: args.model,
        command: args.executable,
        task_id: args.task,
        timeout_seconds: args.timeout,
        data_dir: data_dir.to_path_buf(),
        sandbox,
        docker,
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
        WorkerOutcomeStatus::Executed => format!(
            "{} 写入航班安全落地 · {}\n{}\n隔离黑匣子：{}",
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
            "{} 航班坠机 · {}\n{}\n黑匣子：{}",
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

fn flow_message_line(message: &FlowMessage, status: &str) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        message.id,
        status,
        message.kind,
        message.sender_name,
        message
            .recipients
            .iter()
            .map(|recipient| recipient.name.as_str())
            .collect::<Vec<_>>()
            .join(","),
        message.body
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

impl From<SandboxBackendArg> for SandboxBackend {
    fn from(value: SandboxBackendArg) -> Self {
        match value {
            SandboxBackendArg::Process => Self::Process,
            SandboxBackendArg::Docker => Self::Docker,
        }
    }
}

impl From<SandboxNetworkArg> for SandboxNetwork {
    fn from(value: SandboxNetworkArg) -> Self {
        match value {
            SandboxNetworkArg::None => Self::None,
            SandboxNetworkArg::Bridge => Self::Bridge,
        }
    }
}

impl From<RecoveryActionArg> for RecoveryAction {
    fn from(value: RecoveryActionArg) -> Self {
        match value {
            RecoveryActionArg::Retry => Self::Retry,
            RecoveryActionArg::SwitchExecutor => Self::SwitchExecutor,
            RecoveryActionArg::ReduceScope => Self::ReduceScope,
            RecoveryActionArg::HumanHandoff => Self::HumanHandoff,
            RecoveryActionArg::Ground => Self::Ground,
            RecoveryActionArg::Fork => Self::Fork,
        }
    }
}

impl From<CapabilityPackArg> for CapabilityPack {
    fn from(value: CapabilityPackArg) -> Self {
        match value {
            CapabilityPackArg::General => Self::General,
            CapabilityPackArg::Coding => Self::Coding,
            CapabilityPackArg::Office => Self::Office,
        }
    }
}

impl From<FlowMessageKindArg> for FlowMessageKind {
    fn from(value: FlowMessageKindArg) -> Self {
        match value {
            FlowMessageKindArg::Command => Self::Command,
            FlowMessageKindArg::Question => Self::Question,
            FlowMessageKindArg::Update => Self::Update,
            FlowMessageKindArg::Decision => Self::Decision,
        }
    }
}
