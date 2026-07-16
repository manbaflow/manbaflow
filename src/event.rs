use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::{
    AttentionKind, Demand, Estimate, Evidence, ExecutionRecord, Flow, Organization, Principal,
    Team, TrackingAttention, TrackingEscalation,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum DomainEvent {
    OrganizationInitialized {
        organization: Organization,
    },
    TeamCreated {
        team: Team,
    },
    PrincipalRegistered {
        principal: Principal,
    },
    DemandCreated {
        demand: Demand,
    },
    PlanGenerated {
        flow: Flow,
    },
    FlowApproved {
        flow_id: String,
        approved_by: String,
        approved_at: DateTime<Utc>,
    },
    WorkRequestSent {
        flow_id: String,
        task_id: String,
        target_id: String,
    },
    TaskAccepted {
        flow_id: String,
        task_id: String,
        accepted_by: String,
        accepted_at: DateTime<Utc>,
    },
    TaskRejected {
        flow_id: String,
        task_id: String,
        rejected_by: String,
        reason: String,
    },
    TaskEstimateNegotiated {
        flow_id: String,
        task_id: String,
        negotiated_by: String,
        estimate: Estimate,
    },
    TaskStarted {
        flow_id: String,
        task_id: String,
        started_by: String,
        started_at: DateTime<Utc>,
    },
    TaskHeartbeat {
        flow_id: String,
        task_id: String,
        actor: String,
        note: Option<String>,
        at: DateTime<Utc>,
    },
    TaskBlocked {
        flow_id: String,
        task_id: String,
        actor: String,
        reason: String,
        at: DateTime<Utc>,
    },
    EvidenceAdded {
        flow_id: String,
        task_id: String,
        evidence: Evidence,
    },
    TaskSubmitted {
        flow_id: String,
        task_id: String,
        submitted_by: String,
        at: DateTime<Utc>,
    },
    TaskCompleted {
        flow_id: String,
        task_id: String,
        completed_by: String,
        at: DateTime<Utc>,
    },
    TrackingAttentionRaised {
        attention: TrackingAttention,
    },
    TrackingAttentionResolved {
        flow_id: String,
        task_id: String,
        attention_id: String,
        kind: AttentionKind,
        resolved_at: DateTime<Utc>,
        reason: String,
    },
    TrackingEscalationRaised {
        escalation: TrackingEscalation,
    },
    TrackingEscalationAcknowledged {
        flow_id: String,
        task_id: String,
        escalation_id: String,
        acknowledged_by: String,
        acknowledged_at: DateTime<Utc>,
    },
    TrackingEscalationResolved {
        flow_id: String,
        task_id: String,
        escalation_id: String,
        resolved_at: DateTime<Utc>,
        reason: String,
    },
    ExecutorStarted {
        flow_id: String,
        task_id: String,
        execution_id: String,
        principal_id: String,
        executor: String,
        mode: String,
        at: DateTime<Utc>,
    },
    ExecutorFinished {
        record: ExecutionRecord,
    },
    ExecutorFailed {
        flow_id: String,
        task_id: String,
        execution_id: String,
        reason: String,
        log_path: Option<String>,
        at: DateTime<Utc>,
    },
    FlowCompleted {
        flow_id: String,
        completed_by: String,
        at: DateTime<Utc>,
    },
}

impl DomainEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::OrganizationInitialized { .. } => "organization.initialized",
            Self::TeamCreated { .. } => "team.created",
            Self::PrincipalRegistered { .. } => "principal.registered",
            Self::DemandCreated { .. } => "demand.created",
            Self::PlanGenerated { .. } => "plan.generated",
            Self::FlowApproved { .. } => "flow.approved",
            Self::WorkRequestSent { .. } => "work_request.sent",
            Self::TaskAccepted { .. } => "task.accepted",
            Self::TaskRejected { .. } => "task.rejected",
            Self::TaskEstimateNegotiated { .. } => "task.estimate_negotiated",
            Self::TaskStarted { .. } => "task.started",
            Self::TaskHeartbeat { .. } => "task.heartbeat",
            Self::TaskBlocked { .. } => "task.blocked",
            Self::EvidenceAdded { .. } => "task.evidence_added",
            Self::TaskSubmitted { .. } => "task.submitted",
            Self::TaskCompleted { .. } => "task.completed",
            Self::TrackingAttentionRaised { .. } => "tracking.attention_raised",
            Self::TrackingAttentionResolved { .. } => "tracking.attention_resolved",
            Self::TrackingEscalationRaised { .. } => "tracking.escalation_raised",
            Self::TrackingEscalationAcknowledged { .. } => "tracking.escalation_acknowledged",
            Self::TrackingEscalationResolved { .. } => "tracking.escalation_resolved",
            Self::ExecutorStarted { .. } => "executor.started",
            Self::ExecutorFinished { .. } => "executor.finished",
            Self::ExecutorFailed { .. } => "executor.failed",
            Self::FlowCompleted { .. } => "flow.completed",
        }
    }

    pub fn flow_id(&self) -> Option<&str> {
        match self {
            Self::DemandCreated { demand } => Some(&demand.flow_id),
            Self::PlanGenerated { flow } => Some(&flow.id),
            Self::FlowApproved { flow_id, .. }
            | Self::WorkRequestSent { flow_id, .. }
            | Self::TaskAccepted { flow_id, .. }
            | Self::TaskRejected { flow_id, .. }
            | Self::TaskEstimateNegotiated { flow_id, .. }
            | Self::TaskStarted { flow_id, .. }
            | Self::TaskHeartbeat { flow_id, .. }
            | Self::TaskBlocked { flow_id, .. }
            | Self::EvidenceAdded { flow_id, .. }
            | Self::TaskSubmitted { flow_id, .. }
            | Self::TaskCompleted { flow_id, .. }
            | Self::TrackingAttentionResolved { flow_id, .. }
            | Self::TrackingEscalationAcknowledged { flow_id, .. }
            | Self::TrackingEscalationResolved { flow_id, .. }
            | Self::ExecutorStarted { flow_id, .. }
            | Self::ExecutorFailed { flow_id, .. }
            | Self::FlowCompleted { flow_id, .. } => Some(flow_id),
            Self::TrackingAttentionRaised { attention } => Some(&attention.flow_id),
            Self::TrackingEscalationRaised { escalation } => Some(&escalation.flow_id),
            Self::ExecutorFinished { record } => Some(&record.flow_id),
            Self::OrganizationInitialized { .. }
            | Self::TeamCreated { .. }
            | Self::PrincipalRegistered { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub sequence: i64,
    pub id: String,
    pub organization_id: String,
    pub flow_id: Option<String>,
    pub actor: String,
    pub kind: String,
    pub event: DomainEvent,
    pub occurred_at: DateTime<Utc>,
}
