use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Organization {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Team {
    pub id: String,
    pub name: String,
    pub capabilities: Vec<String>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Human,
    Agent,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationRole {
    TenantAdmin,
    OrganizationAdmin,
    Manager,
    Member,
    Auditor,
    Agent,
}

impl std::fmt::Display for OrganizationRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TenantAdmin => write!(f, "tenant_admin"),
            Self::OrganizationAdmin => write!(f, "organization_admin"),
            Self::Manager => write!(f, "manager"),
            Self::Member => write!(f, "member"),
            Self::Auditor => write!(f, "auditor"),
            Self::Agent => write!(f, "agent"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RoleBinding {
    pub id: String,
    pub tenant_id: String,
    pub organization_id: String,
    pub principal_id: String,
    pub role: OrganizationRole,
    pub granted_by: String,
    pub granted_at: DateTime<Utc>,
    pub revoked_by: Option<String>,
    pub revoked_at: Option<DateTime<Utc>>,
}

impl RoleBinding {
    pub fn is_active(&self) -> bool {
        self.revoked_at.is_none()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    ClaudeCode,
    Codex,
}

impl std::fmt::Display for ExecutorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClaudeCode => write!(f, "claude-code"),
            Self::Codex => write!(f, "codex"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutorConfig {
    pub kind: ExecutorKind,
    pub workspace: PathBuf,
    pub model: Option<String>,
    pub command: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Principal {
    pub id: String,
    pub name: String,
    pub kind: PrincipalKind,
    pub team_id: Option<String>,
    pub owner_id: Option<String>,
    pub capabilities: Vec<String>,
    pub capacity_percent: u8,
    pub executor: Option<ExecutorConfig>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Workday {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AvailabilityBlock {
    pub id: String,
    pub principal_id: String,
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub reason: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub cancelled_by: Option<String>,
    pub cancelled_at: Option<DateTime<Utc>>,
}

impl AvailabilityBlock {
    pub fn is_active(&self) -> bool {
        self.cancelled_at.is_none()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkCalendar {
    pub principal_id: String,
    pub utc_offset_minutes: i32,
    pub working_days: Vec<Workday>,
    pub day_start_minute: u16,
    pub day_end_minute: u16,
    pub time_off: Vec<AvailabilityBlock>,
    pub updated_by: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NotificationEndpoint {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub connector: NotificationConnector,
    #[serde(default)]
    pub url_env: Option<String>,
    // Kept for replaying generic endpoints created before connector credentials
    // were moved out of the Ledger.
    #[serde(default)]
    pub url: String,
    pub event_kinds: Vec<String>,
    #[serde(default)]
    pub secret_env: String,
    pub active: bool,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub disabled_by: Option<String>,
    pub disabled_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NotificationConnector {
    #[default]
    Generic,
    Feishu,
    Slack,
    Teams,
}

impl NotificationConnector {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Feishu => "feishu",
            Self::Slack => "slack",
            Self::Teams => "teams",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NotificationStatus {
    Pending,
    Delivered,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NotificationDelivery {
    pub id: String,
    pub organization_id: String,
    pub endpoint_id: String,
    pub source_event_kind: String,
    pub flow_id: Option<String>,
    pub actor: String,
    pub payload: serde_json::Value,
    pub status: NotificationStatus,
    pub attempts: u32,
    pub queued_at: DateTime<Utc>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub response_status: Option<u16>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalIdentityBinding {
    pub id: String,
    pub provider: String,
    pub external_user_id: String,
    pub principal_id: String,
    pub bound_by: String,
    pub bound_at: DateTime<Utc>,
    pub unbound_by: Option<String>,
    pub unbound_at: Option<DateTime<Utc>>,
}

impl ExternalIdentityBinding {
    pub fn is_active(&self) -> bool {
        self.unbound_at.is_none()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExternalInteractionAction {
    #[serde(rename = "task.accept")]
    TaskAccept,
    #[serde(rename = "task.reject")]
    TaskReject,
    #[serde(rename = "message.ack")]
    MessageAck,
    #[serde(rename = "escalation.ack")]
    EscalationAck,
}

impl ExternalInteractionAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TaskAccept => "task.accept",
            Self::TaskReject => "task.reject",
            Self::MessageAck => "message.ack",
            Self::EscalationAck => "escalation.ack",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalInteractionReceipt {
    pub id: String,
    pub provider: String,
    pub delivery_id: String,
    pub external_user_id: String,
    pub principal_id: String,
    pub action: ExternalInteractionAction,
    pub target_id: String,
    pub reason: Option<String>,
    pub flow_id: Option<String>,
    pub processed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalInteractionResult {
    pub duplicate: bool,
    pub receipt: ExternalInteractionReceipt,
}

impl WorkCalendar {
    pub fn always_available(principal_id: String, created_at: DateTime<Utc>) -> Self {
        Self {
            principal_id,
            utc_offset_minutes: 0,
            working_days: vec![
                Workday::Monday,
                Workday::Tuesday,
                Workday::Wednesday,
                Workday::Thursday,
                Workday::Friday,
                Workday::Saturday,
                Workday::Sunday,
            ],
            day_start_minute: 0,
            day_end_minute: 24 * 60,
            time_off: Vec::new(),
            updated_by: "system".into(),
            updated_at: created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiCredential {
    pub id: String,
    pub principal_id: String,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

impl ApiCredential {
    pub fn is_active(&self) -> bool {
        self.revoked_at.is_none()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IssuedCredential {
    pub credential: ApiCredential,
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Demand {
    pub id: String,
    pub flow_id: String,
    pub requester: String,
    pub summary: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct PrdDraft {
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub goals: Vec<String>,
    #[serde(default)]
    pub non_goals: Vec<String>,
    pub acceptance_criteria: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct TaskDraft {
    pub key: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub effort_hours: f64,
    #[serde(default)]
    pub requires_human: bool,
    pub acceptance_criteria: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct PlanDraft {
    pub prd: PrdDraft,
    pub tasks: Vec<TaskDraft>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    Human,
    Agent,
    Team,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AssignmentTarget {
    pub kind: TargetKind,
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Assignment {
    pub owner: AssignmentTarget,
    pub copilots: Vec<AssignmentTarget>,
    pub score: f64,
    pub rationale: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Estimate {
    pub effort_hours: f64,
    pub p50_hours: f64,
    pub p80_hours: f64,
    pub confidence: String,
    pub rationale: Vec<String>,
    pub earliest_start: DateTime<Utc>,
    pub p50_finish: DateTime<Utc>,
    pub p80_finish: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Proposed,
    Assigned,
    Accepted,
    InProgress,
    Blocked,
    Submitted,
    Completed,
    Rejected,
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Rejected | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Evidence {
    pub id: String,
    pub kind: String,
    pub uri: String,
    pub summary: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalArtifact {
    pub id: String,
    pub provider: String,
    pub kind: String,
    pub project: String,
    pub external_id: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    pub title: String,
    pub url: String,
    pub status: String,
    pub revision: Option<String>,
    pub verified: bool,
    pub synced_at: DateTime<Utc>,
}

impl ExternalArtifact {
    pub fn same_snapshot(&self, other: &Self) -> bool {
        self.id == other.id
            && self.provider == other.provider
            && self.kind == other.kind
            && self.project == other.project
            && self.external_id == other.external_id
            && self.parent_id == other.parent_id
            && self.title == other.title
            && self.url == other.url
            && self.status == other.status
            && self.revision == other.revision
            && self.verified == other.verified
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: String,
    pub key: String,
    pub title: String,
    pub description: String,
    pub required_capabilities: Vec<String>,
    pub depends_on: Vec<String>,
    pub requires_human: bool,
    pub acceptance_criteria: Vec<String>,
    pub assignment: Option<Assignment>,
    pub estimate: Estimate,
    pub status: TaskStatus,
    pub blocker: Option<String>,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub evidence: Vec<Evidence>,
    #[serde(default)]
    pub external_artifacts: Vec<ExternalArtifact>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FlowStatus {
    Draft,
    Approved,
    Active,
    Completed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Flow {
    pub id: String,
    pub demand: Demand,
    pub prd: PrdDraft,
    pub tasks: Vec<Task>,
    pub status: FlowStatus,
    pub planner: String,
    pub p50_finish: DateTime<Utc>,
    pub p80_finish: DateTime<Utc>,
    pub critical_path: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub approved_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FlowScheduleRevision {
    pub task_estimates: BTreeMap<String, Estimate>,
    pub p50_finish: DateTime<Utc>,
    pub p80_finish: DateTime<Utc>,
    pub critical_path: Vec<String>,
    pub reason: String,
    pub revised_by: String,
    pub revised_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FlowChangeStatus {
    Proposed,
    Applied,
    Rejected,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FlowChangeImpact {
    pub added_task_ids: Vec<String>,
    pub added_task_titles: Vec<String>,
    pub affected_task_ids: Vec<String>,
    pub official_p80_finish: DateTime<Utc>,
    pub baseline_p80_finish: DateTime<Utc>,
    pub proposed_p80_finish: DateTime<Utc>,
    pub baseline_p80_delta_hours: f64,
    pub scope_p80_delta_hours: f64,
    pub net_p80_delta_hours: f64,
    pub risks: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FlowChangeRequest {
    pub id: String,
    pub flow_id: String,
    pub summary: String,
    pub requested_by_id: String,
    pub requested_by_name: String,
    pub planner: String,
    pub proposed_prd: PrdDraft,
    pub new_tasks: Vec<Task>,
    pub preview_schedule: FlowScheduleRevision,
    pub base_task_statuses: BTreeMap<String, TaskStatus>,
    pub base_p80_finish: DateTime<Utc>,
    pub impact: FlowChangeImpact,
    pub status: FlowChangeStatus,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolved_by: Option<String>,
    pub rejection_reason: Option<String>,
}

impl Flow {
    pub fn task(&self, task_id: &str) -> Option<&Task> {
        self.tasks
            .iter()
            .find(|task| task.id == task_id || task.key == task_id)
    }

    pub fn task_mut(&mut self, task_id: &str) -> Option<&mut Task> {
        self.tasks
            .iter_mut()
            .find(|task| task.id == task_id || task.key == task_id)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FlowMessageKind {
    Command,
    Question,
    Update,
    Decision,
}

impl std::fmt::Display for FlowMessageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Command => write!(f, "command"),
            Self::Question => write!(f, "question"),
            Self::Update => write!(f, "update"),
            Self::Decision => write!(f, "decision"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageAcknowledgement {
    pub recipient_id: String,
    pub acknowledged_by_id: String,
    pub acknowledged_by_name: String,
    pub acknowledged_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FlowMessage {
    pub id: String,
    pub flow_id: String,
    pub task_id: Option<String>,
    pub kind: FlowMessageKind,
    pub sender_id: String,
    pub sender_name: String,
    pub recipients: Vec<AssignmentTarget>,
    pub body: String,
    pub requires_ack: bool,
    pub acknowledgements: Vec<MessageAcknowledgement>,
    pub created_at: DateTime<Utc>,
}

impl FlowMessage {
    pub fn recipient_is_acknowledged(&self, recipient_id: &str) -> bool {
        self.acknowledgements
            .iter()
            .any(|acknowledgement| acknowledgement.recipient_id == recipient_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MessageInboxItem {
    pub message: FlowMessage,
    pub pending_recipient_ids: Vec<String>,
}

impl MessageInboxItem {
    pub fn needs_acknowledgement(&self) -> bool {
        self.message.requires_ack && !self.pending_recipient_ids.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AttentionKind {
    AcceptanceWaiting,
    StaleHeartbeat,
    Blocked,
    ReviewWaiting,
    Overdue,
}

impl std::fmt::Display for AttentionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AcceptanceWaiting => write!(f, "acceptance_waiting"),
            Self::StaleHeartbeat => write!(f, "stale_heartbeat"),
            Self::Blocked => write!(f, "blocked"),
            Self::ReviewWaiting => write!(f, "review_waiting"),
            Self::Overdue => write!(f, "overdue"),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AttentionSeverity {
    Warning,
    Critical,
}

impl std::fmt::Display for AttentionSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warning => write!(f, "warning"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackingAttention {
    pub id: String,
    pub flow_id: String,
    pub task_id: String,
    pub kind: AttentionKind,
    pub severity: AttentionSeverity,
    pub summary: String,
    pub raised_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

impl TrackingAttention {
    pub fn is_active(&self) -> bool {
        self.resolved_at.is_none()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackingEscalation {
    pub id: String,
    pub attention_id: String,
    pub flow_id: String,
    pub task_id: String,
    pub recipient_id: String,
    pub recipient_name: String,
    pub reason: String,
    pub raised_at: DateTime<Utc>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
}

impl TrackingEscalation {
    pub fn is_active(&self) -> bool {
        self.resolved_at.is_none()
    }

    pub fn needs_acknowledgement(&self) -> bool {
        self.is_active() && self.acknowledged_at.is_none()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackingScan {
    pub scanned_at: DateTime<Utc>,
    pub scanned_tasks: usize,
    pub raised: Vec<TrackingAttention>,
    pub resolved: Vec<TrackingAttention>,
    pub active: Vec<TrackingAttention>,
    pub escalated: Vec<TrackingEscalation>,
    pub resolved_escalations: Vec<TrackingEscalation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorMode {
    Plan,
    Execute,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionRecord {
    pub id: String,
    pub flow_id: String,
    pub task_id: String,
    pub executor: ExecutorKind,
    pub mode: ExecutorMode,
    pub principal_id: String,
    pub workspace: PathBuf,
    pub log_path: PathBuf,
    pub session_id: Option<String>,
    pub cost_usd: Option<f64>,
    pub summary: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FlightLeaseStatus {
    Authorized,
    Active,
    Landed,
    Crashed,
    Revoked,
}

impl FlightLeaseStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Landed | Self::Crashed | Self::Revoked)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteFlightReport {
    pub run_id: String,
    pub executor: ExecutorKind,
    pub summary: String,
    pub base_revision: String,
    pub changed_files: Vec<String>,
    pub patch_sha256: Option<String>,
    pub log_sha256: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FlightLease {
    pub id: String,
    pub flow_id: String,
    pub task_id: String,
    pub principal_id: String,
    pub principal_name: String,
    pub authorized_by: String,
    pub executor: ExecutorKind,
    pub status: FlightLeaseStatus,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub run_id: Option<String>,
    pub report: Option<RemoteFlightReport>,
}

impl FlightLease {
    pub fn is_claimable_at(&self, now: DateTime<Utc>) -> bool {
        self.status == FlightLeaseStatus::Authorized && self.expires_at > now
    }
}
