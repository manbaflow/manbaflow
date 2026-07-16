use std::path::PathBuf;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
