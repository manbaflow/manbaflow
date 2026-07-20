use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::{
    ApiCredential, Assignment, AttentionKind, AvailabilityBlock, Demand, Estimate, Evidence,
    ExecutionRecord, ExternalArtifact, ExternalIdentityBinding, ExternalInteractionReceipt,
    FlightLease, FlightRecoveryDecision, Flow, FlowChangeRequest, FlowMessage,
    FlowScheduleRevision, GitLabWriteRequest, GitLabWriteResult, MessageAcknowledgement,
    NotificationDelivery, NotificationEndpoint, OfficeReleaseRequest, OfficeReleaseResult,
    Organization, PrdDraft, Principal, RemoteFlightReport, ResourceLease, RoleBinding,
    StagedArtifact, Task, Team, Tenant, TrackingAttention, TrackingEscalation, WorkCalendar,
};

pub const CURRENT_EVENT_VERSION: u16 = 2;

fn default_event_version() -> u16 {
    CURRENT_EVENT_VERSION
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum DomainEvent {
    TenantInitialized {
        tenant: Tenant,
    },
    OrganizationInitialized {
        organization: Organization,
    },
    TeamCreated {
        team: Team,
    },
    TeamDirectoryUpdated {
        team_id: String,
        name: String,
        external_id: Option<String>,
        active: bool,
        updated_by: String,
        updated_at: DateTime<Utc>,
    },
    PrincipalRegistered {
        principal: Principal,
    },
    PrincipalDirectoryUpdated {
        principal_id: String,
        name: String,
        user_name: String,
        team_id: Option<String>,
        active: bool,
        updated_by: String,
        updated_at: DateTime<Utc>,
    },
    RoleGranted {
        binding: RoleBinding,
    },
    RoleRevoked {
        binding_id: String,
        revoked_by: String,
        revoked_at: DateTime<Utc>,
    },
    ExternalIdentityBound {
        binding: ExternalIdentityBinding,
    },
    ExternalIdentityUnbound {
        binding_id: String,
        unbound_by: String,
        unbound_at: DateTime<Utc>,
    },
    WorkCalendarConfigured {
        calendar: WorkCalendar,
    },
    TimeOffAdded {
        block: AvailabilityBlock,
    },
    TimeOffCancelled {
        principal_id: String,
        block_id: String,
        cancelled_by: String,
        cancelled_at: DateTime<Utc>,
    },
    NotificationEndpointRegistered {
        endpoint: NotificationEndpoint,
    },
    NotificationEndpointDisabled {
        endpoint_id: String,
        disabled_by: String,
        disabled_at: DateTime<Utc>,
    },
    NotificationQueued {
        delivery: Box<NotificationDelivery>,
    },
    NotificationDelivered {
        delivery_id: String,
        flow_id: Option<String>,
        response_status: u16,
        delivered_at: DateTime<Utc>,
    },
    NotificationFailed {
        delivery_id: String,
        flow_id: Option<String>,
        response_status: Option<u16>,
        error: String,
        attempted_at: DateTime<Utc>,
    },
    ApiCredentialIssued {
        credential: ApiCredential,
    },
    ApiCredentialRevoked {
        credential_id: String,
        principal_id: String,
        revoked_at: DateTime<Utc>,
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
    FlowMessagePosted {
        message: FlowMessage,
    },
    FlowMessageAcknowledged {
        flow_id: String,
        message_id: String,
        acknowledgements: Vec<MessageAcknowledgement>,
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
    TaskReassigned {
        flow_id: String,
        task_id: String,
        previous_assignment: Option<Assignment>,
        assignment: Assignment,
        reassigned_by: String,
        reason: String,
        at: DateTime<Utc>,
    },
    FlowRescheduled {
        flow_id: String,
        revision: FlowScheduleRevision,
    },
    FlowChangeProposed {
        request: Box<FlowChangeRequest>,
    },
    FlowChangeApplied {
        flow_id: String,
        request_id: String,
        prd: PrdDraft,
        new_tasks: Vec<Task>,
        revision: FlowScheduleRevision,
        applied_by: String,
        applied_at: DateTime<Utc>,
    },
    FlowChangeRejected {
        flow_id: String,
        request_id: String,
        rejected_by: String,
        reason: String,
        rejected_at: DateTime<Utc>,
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
    ExternalArtifactSynced {
        flow_id: String,
        task_id: String,
        artifact: ExternalArtifact,
    },
    ExternalDeliveryProcessed {
        provider: String,
        delivery_id: String,
        binding_key: String,
        occurred_at: DateTime<Utc>,
        processed_at: DateTime<Utc>,
    },
    ExternalInteractionProcessed {
        receipt: ExternalInteractionReceipt,
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
    RemoteFlightAuthorized {
        lease: Box<FlightLease>,
    },
    ResourceLeaseAcquired {
        lease: ResourceLease,
    },
    ResourceLeaseReleased {
        flow_id: String,
        task_id: String,
        resource_lease_id: String,
        released_at: DateTime<Utc>,
        reason: String,
    },
    RemoteFlightClaimed {
        flow_id: String,
        task_id: String,
        lease_id: String,
        run_id: String,
        claimed_at: DateTime<Utc>,
    },
    FlightArtifactStaged {
        artifact: StagedArtifact,
    },
    OfficeReleaseRequested {
        request: OfficeReleaseRequest,
    },
    OfficeReleaseApproved {
        flow_id: String,
        task_id: String,
        release_id: String,
        approved_by: String,
        approved_at: DateTime<Utc>,
    },
    OfficeReleaseRejected {
        flow_id: String,
        task_id: String,
        release_id: String,
        rejected_by: String,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    OfficeReleaseDispatchClaimed {
        flow_id: String,
        task_id: String,
        release_id: String,
        dispatch_id: String,
        claimed_at: DateTime<Utc>,
    },
    OfficeReleaseSucceeded {
        flow_id: String,
        task_id: String,
        release_id: String,
        dispatch_id: String,
        result: OfficeReleaseResult,
    },
    OfficeReleaseFailed {
        flow_id: String,
        task_id: String,
        release_id: String,
        dispatch_id: String,
        error: String,
        indeterminate: bool,
        failed_at: DateTime<Utc>,
    },
    OfficeReleaseRetryApproved {
        flow_id: String,
        task_id: String,
        release_id: String,
        approved_by: String,
        approved_at: DateTime<Utc>,
    },
    OfficeReleaseDispatchExpired {
        flow_id: String,
        task_id: String,
        release_id: String,
        dispatch_id: String,
        retry_safe: bool,
        expired_at: DateTime<Utc>,
    },
    GitLabWriteRequested {
        request: GitLabWriteRequest,
    },
    GitLabWriteApproved {
        flow_id: String,
        task_id: String,
        write_id: String,
        approved_by: String,
        approved_at: DateTime<Utc>,
    },
    GitLabWriteRejected {
        flow_id: String,
        task_id: String,
        write_id: String,
        rejected_by: String,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    GitLabWriteDispatchClaimed {
        flow_id: String,
        task_id: String,
        write_id: String,
        dispatch_id: String,
        claimed_at: DateTime<Utc>,
    },
    GitLabWriteSucceeded {
        flow_id: String,
        task_id: String,
        write_id: String,
        dispatch_id: String,
        result: GitLabWriteResult,
    },
    GitLabWriteFailed {
        flow_id: String,
        task_id: String,
        write_id: String,
        dispatch_id: String,
        error: String,
        indeterminate: bool,
        failed_at: DateTime<Utc>,
    },
    GitLabWriteRetryApproved {
        flow_id: String,
        task_id: String,
        write_id: String,
        approved_by: String,
        approved_at: DateTime<Utc>,
    },
    GitLabWriteDispatchExpired {
        flow_id: String,
        task_id: String,
        write_id: String,
        dispatch_id: String,
        expired_at: DateTime<Utc>,
    },
    RemoteFlightRevoked {
        flow_id: String,
        task_id: String,
        lease_id: String,
        revoked_by: String,
        revoked_at: DateTime<Utc>,
    },
    RemoteFlightExpired {
        flow_id: String,
        task_id: String,
        lease_id: String,
        expired_at: DateTime<Utc>,
    },
    RemoteFlightFinished {
        flow_id: String,
        task_id: String,
        lease_id: String,
        landed: bool,
        report: RemoteFlightReport,
        finished_at: DateTime<Utc>,
    },
    FlightRecoveryDecided {
        flow_id: String,
        task_id: String,
        decision: FlightRecoveryDecision,
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
            Self::TenantInitialized { .. } => "tenant.initialized",
            Self::OrganizationInitialized { .. } => "organization.initialized",
            Self::TeamCreated { .. } => "team.created",
            Self::TeamDirectoryUpdated { .. } => "team.directory_updated",
            Self::PrincipalRegistered { .. } => "principal.registered",
            Self::PrincipalDirectoryUpdated { .. } => "principal.directory_updated",
            Self::RoleGranted { .. } => "authority.role_granted",
            Self::RoleRevoked { .. } => "authority.role_revoked",
            Self::ExternalIdentityBound { .. } => "external_identity.bound",
            Self::ExternalIdentityUnbound { .. } => "external_identity.unbound",
            Self::WorkCalendarConfigured { .. } => "calendar.configured",
            Self::TimeOffAdded { .. } => "calendar.time_off_added",
            Self::TimeOffCancelled { .. } => "calendar.time_off_cancelled",
            Self::NotificationEndpointRegistered { .. } => "notification.endpoint_registered",
            Self::NotificationEndpointDisabled { .. } => "notification.endpoint_disabled",
            Self::NotificationQueued { .. } => "notification.queued",
            Self::NotificationDelivered { .. } => "notification.delivered",
            Self::NotificationFailed { .. } => "notification.failed",
            Self::ApiCredentialIssued { .. } => "api_credential.issued",
            Self::ApiCredentialRevoked { .. } => "api_credential.revoked",
            Self::DemandCreated { .. } => "demand.created",
            Self::PlanGenerated { .. } => "plan.generated",
            Self::FlowApproved { .. } => "flow.approved",
            Self::WorkRequestSent { .. } => "work_request.sent",
            Self::FlowMessagePosted { .. } => "flow_message.posted",
            Self::FlowMessageAcknowledged { .. } => "flow_message.acknowledged",
            Self::TaskAccepted { .. } => "task.accepted",
            Self::TaskRejected { .. } => "task.rejected",
            Self::TaskEstimateNegotiated { .. } => "task.estimate_negotiated",
            Self::TaskReassigned { .. } => "task.reassigned",
            Self::FlowRescheduled { .. } => "flow.rescheduled",
            Self::FlowChangeProposed { .. } => "flow_change.proposed",
            Self::FlowChangeApplied { .. } => "flow_change.applied",
            Self::FlowChangeRejected { .. } => "flow_change.rejected",
            Self::TaskStarted { .. } => "task.started",
            Self::TaskHeartbeat { .. } => "task.heartbeat",
            Self::TaskBlocked { .. } => "task.blocked",
            Self::EvidenceAdded { .. } => "task.evidence_added",
            Self::ExternalArtifactSynced { .. } => "task.external_artifact_synced",
            Self::ExternalDeliveryProcessed { .. } => "external.delivery_processed",
            Self::ExternalInteractionProcessed { .. } => "external_interaction.processed",
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
            Self::RemoteFlightAuthorized { .. } => "remote_flight.authorized",
            Self::ResourceLeaseAcquired { .. } => "resource_lease.acquired",
            Self::ResourceLeaseReleased { .. } => "resource_lease.released",
            Self::RemoteFlightClaimed { .. } => "remote_flight.claimed",
            Self::FlightArtifactStaged { .. } => "remote_flight.artifact_staged",
            Self::OfficeReleaseRequested { .. } => "office_release.requested",
            Self::OfficeReleaseApproved { .. } => "office_release.approved",
            Self::OfficeReleaseRejected { .. } => "office_release.rejected",
            Self::OfficeReleaseDispatchClaimed { .. } => "office_release.dispatch_claimed",
            Self::OfficeReleaseSucceeded { .. } => "office_release.succeeded",
            Self::OfficeReleaseFailed {
                indeterminate: true,
                ..
            } => "office_release.indeterminate",
            Self::OfficeReleaseFailed {
                indeterminate: false,
                ..
            } => "office_release.failed",
            Self::OfficeReleaseRetryApproved { .. } => "office_release.retry_approved",
            Self::OfficeReleaseDispatchExpired {
                retry_safe: true, ..
            } => "office_release.dispatch_recovered",
            Self::OfficeReleaseDispatchExpired {
                retry_safe: false, ..
            } => "office_release.dispatch_indeterminate",
            Self::GitLabWriteRequested { .. } => "gitlab_write.requested",
            Self::GitLabWriteApproved { .. } => "gitlab_write.approved",
            Self::GitLabWriteRejected { .. } => "gitlab_write.rejected",
            Self::GitLabWriteDispatchClaimed { .. } => "gitlab_write.dispatch_claimed",
            Self::GitLabWriteSucceeded { .. } => "gitlab_write.succeeded",
            Self::GitLabWriteFailed {
                indeterminate: true,
                ..
            }
            | Self::GitLabWriteDispatchExpired { .. } => "gitlab_write.indeterminate",
            Self::GitLabWriteFailed {
                indeterminate: false,
                ..
            } => "gitlab_write.failed",
            Self::GitLabWriteRetryApproved { .. } => "gitlab_write.retry_approved",
            Self::RemoteFlightRevoked { .. } => "remote_flight.revoked",
            Self::RemoteFlightExpired { .. } => "remote_flight.expired",
            Self::RemoteFlightFinished { landed: true, .. } => "remote_flight.landed",
            Self::RemoteFlightFinished { landed: false, .. } => "remote_flight.crashed",
            Self::FlightRecoveryDecided { .. } => "remote_flight.recovery_decided",
            Self::FlowCompleted { .. } => "flow.completed",
        }
    }

    pub fn flow_id(&self) -> Option<&str> {
        match self {
            Self::DemandCreated { demand } => Some(&demand.flow_id),
            Self::PlanGenerated { flow } => Some(&flow.id),
            Self::FlowApproved { flow_id, .. }
            | Self::WorkRequestSent { flow_id, .. }
            | Self::FlowMessageAcknowledged { flow_id, .. }
            | Self::TaskAccepted { flow_id, .. }
            | Self::TaskRejected { flow_id, .. }
            | Self::TaskEstimateNegotiated { flow_id, .. }
            | Self::TaskReassigned { flow_id, .. }
            | Self::FlowRescheduled { flow_id, .. }
            | Self::FlowChangeApplied { flow_id, .. }
            | Self::FlowChangeRejected { flow_id, .. }
            | Self::TaskStarted { flow_id, .. }
            | Self::TaskHeartbeat { flow_id, .. }
            | Self::TaskBlocked { flow_id, .. }
            | Self::EvidenceAdded { flow_id, .. }
            | Self::ExternalArtifactSynced { flow_id, .. }
            | Self::TaskSubmitted { flow_id, .. }
            | Self::TaskCompleted { flow_id, .. }
            | Self::TrackingAttentionResolved { flow_id, .. }
            | Self::TrackingEscalationAcknowledged { flow_id, .. }
            | Self::TrackingEscalationResolved { flow_id, .. }
            | Self::ExecutorStarted { flow_id, .. }
            | Self::ExecutorFailed { flow_id, .. }
            | Self::RemoteFlightClaimed { flow_id, .. }
            | Self::RemoteFlightRevoked { flow_id, .. }
            | Self::RemoteFlightExpired { flow_id, .. }
            | Self::RemoteFlightFinished { flow_id, .. }
            | Self::ResourceLeaseReleased { flow_id, .. }
            | Self::FlightRecoveryDecided { flow_id, .. }
            | Self::OfficeReleaseApproved { flow_id, .. }
            | Self::OfficeReleaseRejected { flow_id, .. }
            | Self::OfficeReleaseDispatchClaimed { flow_id, .. }
            | Self::OfficeReleaseSucceeded { flow_id, .. }
            | Self::OfficeReleaseFailed { flow_id, .. }
            | Self::OfficeReleaseRetryApproved { flow_id, .. }
            | Self::OfficeReleaseDispatchExpired { flow_id, .. }
            | Self::GitLabWriteApproved { flow_id, .. }
            | Self::GitLabWriteRejected { flow_id, .. }
            | Self::GitLabWriteDispatchClaimed { flow_id, .. }
            | Self::GitLabWriteSucceeded { flow_id, .. }
            | Self::GitLabWriteFailed { flow_id, .. }
            | Self::GitLabWriteRetryApproved { flow_id, .. }
            | Self::GitLabWriteDispatchExpired { flow_id, .. }
            | Self::FlowCompleted { flow_id, .. } => Some(flow_id),
            Self::NotificationQueued { delivery } => delivery.flow_id.as_deref(),
            Self::NotificationDelivered { flow_id, .. }
            | Self::NotificationFailed { flow_id, .. } => flow_id.as_deref(),
            Self::TrackingAttentionRaised { attention } => Some(&attention.flow_id),
            Self::TrackingEscalationRaised { escalation } => Some(&escalation.flow_id),
            Self::FlowMessagePosted { message } => Some(&message.flow_id),
            Self::FlowChangeProposed { request } => Some(&request.flow_id),
            Self::ExecutorFinished { record } => Some(&record.flow_id),
            Self::RemoteFlightAuthorized { lease } => Some(&lease.flow_id),
            Self::FlightArtifactStaged { artifact } => Some(&artifact.flow_id),
            Self::OfficeReleaseRequested { request } => Some(&request.flow_id),
            Self::GitLabWriteRequested { request } => Some(&request.flow_id),
            Self::ResourceLeaseAcquired { lease } => Some(&lease.flow_id),
            Self::ExternalInteractionProcessed { receipt } => receipt.flow_id.as_deref(),
            Self::TenantInitialized { .. }
            | Self::OrganizationInitialized { .. }
            | Self::TeamCreated { .. }
            | Self::TeamDirectoryUpdated { .. }
            | Self::PrincipalRegistered { .. }
            | Self::PrincipalDirectoryUpdated { .. }
            | Self::RoleGranted { .. }
            | Self::RoleRevoked { .. }
            | Self::ExternalIdentityBound { .. }
            | Self::ExternalIdentityUnbound { .. }
            | Self::WorkCalendarConfigured { .. }
            | Self::TimeOffAdded { .. }
            | Self::TimeOffCancelled { .. }
            | Self::NotificationEndpointRegistered { .. }
            | Self::NotificationEndpointDisabled { .. }
            | Self::ApiCredentialIssued { .. }
            | Self::ApiCredentialRevoked { .. }
            | Self::ExternalDeliveryProcessed { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    #[serde(default = "default_event_version")]
    pub event_version: u16,
    pub sequence: i64,
    pub id: String,
    pub organization_id: String,
    pub flow_id: Option<String>,
    pub actor: String,
    pub kind: String,
    pub event: DomainEvent,
    pub occurred_at: DateTime<Utc>,
}
