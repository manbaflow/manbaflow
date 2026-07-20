use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::domain::{
    ApiCredential, ExecutionRecord, ExternalIdentityBinding, ExternalInteractionReceipt,
    FlightLease, FlightLeaseStatus, FlightRecoveryDecision, Flow, FlowChangeRequest,
    FlowChangeStatus, FlowMessage, FlowScheduleRevision, FlowStatus, NotificationDelivery,
    NotificationEndpoint, NotificationStatus, Organization, OrganizationRole, Principal,
    PrincipalKind, ResourceLease, ResourceLeaseStatus, RoleBinding, TargetKind, TaskStatus, Team,
    Tenant, TrackingAttention, TrackingEscalation, WorkCalendar,
};
use crate::error::{MambaError, Result};
use crate::event::{DomainEvent, EventEnvelope};

#[derive(Clone, Debug, Default)]
pub struct OrganizationState {
    pub tenant: Option<Tenant>,
    pub organization: Option<Organization>,
    pub teams: BTreeMap<String, Team>,
    pub principals: BTreeMap<String, Principal>,
    pub external_identities: BTreeMap<String, ExternalIdentityBinding>,
    pub external_interactions: BTreeMap<String, ExternalInteractionReceipt>,
    pub calendars: BTreeMap<String, WorkCalendar>,
    pub notification_endpoints: BTreeMap<String, NotificationEndpoint>,
    pub notification_deliveries: BTreeMap<String, NotificationDelivery>,
    pub credentials: BTreeMap<String, ApiCredential>,
    pub role_bindings: BTreeMap<String, RoleBinding>,
    pub external_deliveries: BTreeMap<String, DateTime<Utc>>,
    pub external_binding_clocks: BTreeMap<String, DateTime<Utc>>,
    pub flows: BTreeMap<String, Flow>,
    pub messages: BTreeMap<String, FlowMessage>,
    pub flow_changes: BTreeMap<String, FlowChangeRequest>,
    pub executions: BTreeMap<String, ExecutionRecord>,
    pub flight_leases: BTreeMap<String, FlightLease>,
    pub resource_leases: BTreeMap<String, ResourceLease>,
    pub flight_recoveries: BTreeMap<String, FlightRecoveryDecision>,
    pub attentions: BTreeMap<String, TrackingAttention>,
    pub escalations: BTreeMap<String, TrackingEscalation>,
    pub last_sequence: i64,
}

impl OrganizationState {
    pub fn replay(events: &[EventEnvelope]) -> Result<Self> {
        let mut state = Self::default();
        for envelope in events {
            state.apply(envelope)?;
        }
        Ok(state)
    }

    pub fn apply(&mut self, envelope: &EventEnvelope) -> Result<()> {
        let expected_sequence = self.last_sequence.checked_add(1).ok_or_else(|| {
            MambaError::Validation("event sequence exceeded the supported range".into())
        })?;
        if envelope.sequence != expected_sequence {
            return Err(MambaError::Validation(format!(
                "event sequence {} is not the expected next sequence {}",
                envelope.sequence, expected_sequence
            )));
        }

        match &envelope.event {
            DomainEvent::TenantInitialized { tenant } => {
                if self.tenant.is_some() {
                    return Err(MambaError::TenantAlreadyInitialized);
                }
                self.tenant = Some(tenant.clone());
            }
            DomainEvent::OrganizationInitialized { organization } => {
                if self.organization.is_some() {
                    return Err(MambaError::OrganizationAlreadyInitialized);
                }
                self.organization = Some(organization.clone());
            }
            DomainEvent::TeamCreated { team } => {
                self.teams.insert(team.id.clone(), team.clone());
            }
            DomainEvent::TeamDirectoryUpdated {
                team_id,
                name,
                external_id,
                active,
                ..
            } => {
                if self
                    .teams
                    .values()
                    .any(|team| team.id != *team_id && team.name.eq_ignore_ascii_case(name))
                {
                    return Err(MambaError::Validation(format!(
                        "team already exists: {name}"
                    )));
                }
                if external_id.as_ref().is_some_and(|external_id| {
                    self.teams.values().any(|team| {
                        team.id != *team_id
                            && team.directory_external_id.as_ref() == Some(external_id)
                    })
                }) {
                    return Err(MambaError::Validation(
                        "directory Group externalId already exists".into(),
                    ));
                }
                let team = self
                    .teams
                    .get_mut(team_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "team",
                        id: team_id.clone(),
                    })?;
                team.name = name.clone();
                team.directory_external_id = external_id.clone();
                team.active = *active;
            }
            DomainEvent::PrincipalRegistered { principal } => {
                self.principals
                    .insert(principal.id.clone(), principal.clone());
                self.calendars
                    .entry(principal.id.clone())
                    .or_insert_with(|| {
                        WorkCalendar::always_available(principal.id.clone(), principal.created_at)
                    });
            }
            DomainEvent::PrincipalDirectoryUpdated {
                principal_id,
                name,
                user_name,
                team_id,
                active,
                ..
            } => {
                if let Some(team_id) = team_id {
                    self.team(team_id)?;
                }
                if self.principals.values().any(|principal| {
                    principal.id != *principal_id && principal.name.eq_ignore_ascii_case(name)
                }) {
                    return Err(MambaError::Validation(format!(
                        "principal already exists: {name}"
                    )));
                }
                if self.principals.values().any(|principal| {
                    principal.id != *principal_id
                        && principal
                            .directory_username
                            .as_deref()
                            .is_some_and(|current| current.eq_ignore_ascii_case(user_name))
                }) {
                    return Err(MambaError::Validation(format!(
                        "directory userName already exists: {user_name}"
                    )));
                }
                let principal =
                    self.principals
                        .get_mut(principal_id)
                        .ok_or_else(|| MambaError::NotFound {
                            entity: "principal",
                            id: principal_id.clone(),
                        })?;
                principal.name = name.clone();
                principal.directory_username = Some(user_name.clone());
                principal.team_id = team_id.clone();
                principal.active = *active;
            }
            DomainEvent::RoleGranted { binding } => {
                let tenant = self.tenant()?;
                let organization = self.organization()?;
                if binding.tenant_id != tenant.id || binding.organization_id != organization.id {
                    return Err(MambaError::Validation(
                        "role binding scope does not match the active tenant and organization"
                            .into(),
                    ));
                }
                self.principal(&binding.principal_id)?;
                if self.role_bindings.values().any(|existing| {
                    existing.is_active()
                        && existing.principal_id == binding.principal_id
                        && existing.role == binding.role
                }) {
                    return Err(MambaError::Validation(format!(
                        "principal {} already has role {}",
                        binding.principal_id, binding.role
                    )));
                }
                self.role_bindings
                    .insert(binding.id.clone(), binding.clone());
            }
            DomainEvent::RoleRevoked {
                binding_id,
                revoked_by,
                revoked_at,
            } => {
                let binding =
                    self.role_bindings
                        .get_mut(binding_id)
                        .ok_or_else(|| MambaError::NotFound {
                            entity: "role binding",
                            id: binding_id.clone(),
                        })?;
                if !binding.is_active() {
                    return Err(MambaError::InvalidTransition(format!(
                        "role binding {binding_id} is already revoked"
                    )));
                }
                binding.revoked_by = Some(revoked_by.clone());
                binding.revoked_at = Some(*revoked_at);
            }
            DomainEvent::ExternalIdentityBound { binding } => {
                if self.external_identities.contains_key(&binding.id) {
                    return Err(MambaError::Validation(format!(
                        "external identity binding already exists: {}",
                        binding.id
                    )));
                }
                let principal = self.principal(&binding.principal_id)?;
                if principal.kind != PrincipalKind::Human || !principal.active {
                    return Err(MambaError::PermissionDenied(
                        "external identities can only bind to an active Human principal".into(),
                    ));
                }
                if self.external_identities.values().any(|candidate| {
                    candidate.is_active()
                        && candidate.provider == binding.provider
                        && (candidate.external_user_id == binding.external_user_id
                            || candidate.principal_id == binding.principal_id)
                }) {
                    return Err(MambaError::Validation(format!(
                        "active {} identity is already bound",
                        binding.provider
                    )));
                }
                self.external_identities
                    .insert(binding.id.clone(), binding.clone());
            }
            DomainEvent::ExternalIdentityUnbound {
                binding_id,
                unbound_by,
                unbound_at,
            } => {
                let binding = self
                    .external_identities
                    .get_mut(binding_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "external identity binding",
                        id: binding_id.clone(),
                    })?;
                if !binding.is_active() {
                    return Err(MambaError::InvalidTransition(format!(
                        "external identity binding {binding_id} is already inactive"
                    )));
                }
                binding.unbound_by = Some(unbound_by.clone());
                binding.unbound_at = Some(*unbound_at);
            }
            DomainEvent::WorkCalendarConfigured { calendar } => {
                self.principal(&calendar.principal_id)?;
                crate::calendar::validate(calendar)?;
                self.calendars
                    .insert(calendar.principal_id.clone(), calendar.clone());
            }
            DomainEvent::TimeOffAdded { block } => {
                self.principal(&block.principal_id)?;
                let calendar = self.calendars.get_mut(&block.principal_id).ok_or_else(|| {
                    MambaError::NotFound {
                        entity: "work calendar",
                        id: block.principal_id.clone(),
                    }
                })?;
                if calendar
                    .time_off
                    .iter()
                    .any(|existing| existing.id == block.id)
                {
                    return Err(MambaError::Validation(format!(
                        "time off block already exists: {}",
                        block.id
                    )));
                }
                calendar.time_off.push(block.clone());
            }
            DomainEvent::TimeOffCancelled {
                principal_id,
                block_id,
                cancelled_by,
                cancelled_at,
            } => {
                let calendar =
                    self.calendars
                        .get_mut(principal_id)
                        .ok_or_else(|| MambaError::NotFound {
                            entity: "work calendar",
                            id: principal_id.clone(),
                        })?;
                let block = calendar
                    .time_off
                    .iter_mut()
                    .find(|block| block.id == *block_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "time off block",
                        id: block_id.clone(),
                    })?;
                if !block.is_active() {
                    return Err(MambaError::InvalidTransition(format!(
                        "time off block {block_id} is already cancelled"
                    )));
                }
                block.cancelled_by = Some(cancelled_by.clone());
                block.cancelled_at = Some(*cancelled_at);
            }
            DomainEvent::NotificationEndpointRegistered { endpoint } => {
                if self.notification_endpoints.contains_key(&endpoint.id) {
                    return Err(MambaError::Validation(format!(
                        "notification endpoint already exists: {}",
                        endpoint.id
                    )));
                }
                self.notification_endpoints
                    .insert(endpoint.id.clone(), endpoint.clone());
            }
            DomainEvent::NotificationEndpointDisabled {
                endpoint_id,
                disabled_by,
                disabled_at,
            } => {
                let endpoint = self
                    .notification_endpoints
                    .get_mut(endpoint_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "notification endpoint",
                        id: endpoint_id.clone(),
                    })?;
                if !endpoint.active {
                    return Err(MambaError::InvalidTransition(format!(
                        "notification endpoint {endpoint_id} is already disabled"
                    )));
                }
                endpoint.active = false;
                endpoint.disabled_by = Some(disabled_by.clone());
                endpoint.disabled_at = Some(*disabled_at);
                for delivery in self
                    .notification_deliveries
                    .values_mut()
                    .filter(|delivery| delivery.endpoint_id == *endpoint_id)
                    .filter(|delivery| delivery.status != NotificationStatus::Delivered)
                {
                    delivery.status = NotificationStatus::Cancelled;
                    delivery.last_error = Some("notification endpoint disabled".into());
                }
            }
            DomainEvent::NotificationQueued { delivery } => {
                let endpoint = self
                    .notification_endpoints
                    .get(&delivery.endpoint_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "notification endpoint",
                        id: delivery.endpoint_id.clone(),
                    })?;
                if !endpoint.active || self.notification_deliveries.contains_key(&delivery.id) {
                    return Err(MambaError::Validation(format!(
                        "notification delivery {} cannot be queued",
                        delivery.id
                    )));
                }
                self.notification_deliveries
                    .insert(delivery.id.clone(), delivery.as_ref().clone());
            }
            DomainEvent::NotificationDelivered {
                delivery_id,
                response_status,
                delivered_at,
                ..
            } => {
                let delivery = self.notification_delivery_mut(delivery_id)?;
                if matches!(
                    delivery.status,
                    NotificationStatus::Delivered | NotificationStatus::Cancelled
                ) {
                    return Err(MambaError::InvalidTransition(format!(
                        "notification delivery {delivery_id} is already delivered"
                    )));
                }
                delivery.status = NotificationStatus::Delivered;
                delivery.attempts += 1;
                delivery.last_attempt_at = Some(*delivered_at);
                delivery.delivered_at = Some(*delivered_at);
                delivery.response_status = Some(*response_status);
                delivery.last_error = None;
            }
            DomainEvent::NotificationFailed {
                delivery_id,
                response_status,
                error,
                attempted_at,
                ..
            } => {
                let delivery = self.notification_delivery_mut(delivery_id)?;
                if matches!(
                    delivery.status,
                    NotificationStatus::Delivered | NotificationStatus::Cancelled
                ) {
                    return Err(MambaError::InvalidTransition(format!(
                        "delivered notification {delivery_id} cannot fail"
                    )));
                }
                delivery.status = NotificationStatus::Failed;
                delivery.attempts += 1;
                delivery.last_attempt_at = Some(*attempted_at);
                delivery.response_status = *response_status;
                delivery.last_error = Some(error.clone());
            }
            DomainEvent::ApiCredentialIssued { credential } => {
                self.principal(&credential.principal_id)?;
                self.credentials
                    .insert(credential.id.clone(), credential.clone());
            }
            DomainEvent::ApiCredentialRevoked {
                credential_id,
                principal_id,
                revoked_at,
            } => {
                let credential = self.credentials.get_mut(credential_id).ok_or_else(|| {
                    MambaError::NotFound {
                        entity: "API credential",
                        id: credential_id.clone(),
                    }
                })?;
                if credential.principal_id != *principal_id {
                    return Err(MambaError::Validation(format!(
                        "API credential {} does not belong to principal {}",
                        credential_id, principal_id
                    )));
                }
                credential.revoked_at = Some(*revoked_at);
            }
            DomainEvent::DemandCreated { .. } => {}
            DomainEvent::PlanGenerated { flow } => {
                self.flows.insert(flow.id.clone(), flow.clone());
            }
            DomainEvent::FlowApproved {
                flow_id,
                approved_at,
                ..
            } => {
                let flow = self.flow_mut(flow_id)?;
                flow.status = FlowStatus::Approved;
                flow.approved_at = Some(*approved_at);
            }
            DomainEvent::WorkRequestSent {
                flow_id, task_id, ..
            } => {
                self.task_mut(flow_id, task_id)?.status = TaskStatus::Assigned;
            }
            DomainEvent::FlowMessagePosted { message } => {
                self.flow(&message.flow_id)?;
                if let Some(task_id) = &message.task_id {
                    self.flow(&message.flow_id)?.task(task_id).ok_or_else(|| {
                        MambaError::NotFound {
                            entity: "task",
                            id: task_id.clone(),
                        }
                    })?;
                }
                self.principal(&message.sender_id)?;
                for recipient in &message.recipients {
                    match recipient.kind {
                        TargetKind::Human | TargetKind::Agent => {
                            self.principal(&recipient.id)?;
                        }
                        TargetKind::Team => {
                            self.team(&recipient.id)?;
                        }
                    }
                }
                self.messages.insert(message.id.clone(), message.clone());
            }
            DomainEvent::FlowMessageAcknowledged {
                flow_id,
                message_id,
                acknowledgements,
            } => {
                for acknowledgement in acknowledgements {
                    self.principal(&acknowledgement.acknowledged_by_id)?;
                }
                let message =
                    self.messages
                        .get_mut(message_id)
                        .ok_or_else(|| MambaError::NotFound {
                            entity: "flow message",
                            id: message_id.clone(),
                        })?;
                if message.flow_id != *flow_id {
                    return Err(MambaError::Validation(format!(
                        "flow message {message_id} does not belong to flow {flow_id}"
                    )));
                }
                for acknowledgement in acknowledgements {
                    if !message
                        .recipients
                        .iter()
                        .any(|recipient| recipient.id == acknowledgement.recipient_id)
                    {
                        return Err(MambaError::Validation(format!(
                            "flow message {message_id} has no recipient {}",
                            acknowledgement.recipient_id
                        )));
                    }
                    if let Some(existing) = message
                        .acknowledgements
                        .iter_mut()
                        .find(|existing| existing.recipient_id == acknowledgement.recipient_id)
                    {
                        *existing = acknowledgement.clone();
                    } else {
                        message.acknowledgements.push(acknowledgement.clone());
                    }
                }
            }
            DomainEvent::TaskAccepted {
                flow_id,
                task_id,
                accepted_at,
                ..
            } => {
                let flow = self.flow_mut(flow_id)?;
                flow.status = FlowStatus::Active;
                let task = flow.task_mut(task_id).ok_or_else(|| MambaError::NotFound {
                    entity: "task",
                    id: task_id.clone(),
                })?;
                task.status = TaskStatus::Accepted;
                task.last_heartbeat = Some(*accepted_at);
            }
            DomainEvent::TaskRejected {
                flow_id, task_id, ..
            } => {
                self.task_mut(flow_id, task_id)?.status = TaskStatus::Rejected;
            }
            DomainEvent::TaskEstimateNegotiated {
                flow_id,
                task_id,
                estimate,
                ..
            } => {
                self.task_mut(flow_id, task_id)?.estimate = estimate.clone();
                self.refresh_flow_finish(flow_id)?;
            }
            DomainEvent::TaskReassigned {
                flow_id,
                task_id,
                previous_assignment,
                assignment,
                ..
            } => {
                let task = self.task_mut(flow_id, task_id)?;
                if task.assignment.as_ref() != previous_assignment.as_ref() {
                    return Err(MambaError::Validation(format!(
                        "task {task_id} reassignment does not match its previous owner"
                    )));
                }
                task.assignment = Some(assignment.clone());
                task.status = TaskStatus::Assigned;
                task.blocker = None;
                task.last_heartbeat = None;
            }
            DomainEvent::FlowRescheduled { flow_id, revision } => {
                self.apply_schedule_revision(flow_id, revision)?;
            }
            DomainEvent::FlowChangeProposed { request } => {
                self.flow(&request.flow_id)?;
                self.principal(&request.requested_by_id)?;
                self.flow_changes
                    .insert(request.id.clone(), request.as_ref().clone());
            }
            DomainEvent::FlowChangeApplied {
                flow_id,
                request_id,
                prd,
                new_tasks,
                applied_by,
                applied_at,
                revision,
            } => {
                let request =
                    self.flow_changes
                        .get(request_id)
                        .ok_or_else(|| MambaError::NotFound {
                            entity: "flow change request",
                            id: request_id.clone(),
                        })?;
                if request.flow_id != *flow_id || request.status != FlowChangeStatus::Proposed {
                    return Err(MambaError::InvalidTransition(format!(
                        "flow change request {request_id} cannot be applied"
                    )));
                }
                let flow = self.flow_mut(flow_id)?;
                for task in new_tasks {
                    if flow.task(&task.id).is_some()
                        || flow.tasks.iter().any(|existing| existing.key == task.key)
                    {
                        return Err(MambaError::Validation(format!(
                            "flow change request adds duplicate task {}",
                            task.key
                        )));
                    }
                    flow.tasks.push(task.clone());
                }
                flow.prd = prd.clone();
                self.apply_schedule_revision(flow_id, revision)?;
                let request = self.flow_changes.get_mut(request_id).unwrap();
                request.status = FlowChangeStatus::Applied;
                request.resolved_at = Some(*applied_at);
                request.resolved_by = Some(applied_by.clone());
            }
            DomainEvent::FlowChangeRejected {
                flow_id,
                request_id,
                rejected_by,
                reason,
                rejected_at,
            } => {
                let request =
                    self.flow_changes
                        .get_mut(request_id)
                        .ok_or_else(|| MambaError::NotFound {
                            entity: "flow change request",
                            id: request_id.clone(),
                        })?;
                if request.flow_id != *flow_id || request.status != FlowChangeStatus::Proposed {
                    return Err(MambaError::InvalidTransition(format!(
                        "flow change request {request_id} cannot be rejected"
                    )));
                }
                request.status = FlowChangeStatus::Rejected;
                request.resolved_at = Some(*rejected_at);
                request.resolved_by = Some(rejected_by.clone());
                request.rejection_reason = Some(reason.clone());
            }
            DomainEvent::TaskStarted {
                flow_id,
                task_id,
                started_at,
                ..
            } => {
                let task = self.task_mut(flow_id, task_id)?;
                task.status = TaskStatus::InProgress;
                task.blocker = None;
                task.last_heartbeat = Some(*started_at);
            }
            DomainEvent::TaskHeartbeat {
                flow_id,
                task_id,
                at,
                ..
            } => {
                self.task_mut(flow_id, task_id)?.last_heartbeat = Some(*at);
            }
            DomainEvent::TaskBlocked {
                flow_id,
                task_id,
                reason,
                at,
                ..
            } => {
                let task = self.task_mut(flow_id, task_id)?;
                task.status = TaskStatus::Blocked;
                task.blocker = Some(reason.clone());
                task.last_heartbeat = Some(*at);
            }
            DomainEvent::EvidenceAdded {
                flow_id,
                task_id,
                evidence,
            } => {
                self.task_mut(flow_id, task_id)?
                    .evidence
                    .push(evidence.clone());
            }
            DomainEvent::ExternalArtifactSynced {
                flow_id,
                task_id,
                artifact,
            } => {
                let artifacts = &mut self.task_mut(flow_id, task_id)?.external_artifacts;
                if let Some(parent_id) = &artifact.parent_id {
                    artifacts.retain(|existing| {
                        existing.id == artifact.id
                            || existing.provider != artifact.provider
                            || existing.kind != artifact.kind
                            || existing.project != artifact.project
                            || existing
                                .parent_id
                                .as_ref()
                                .is_some_and(|existing_parent| existing_parent != parent_id)
                    });
                }
                if let Some(existing) = artifacts
                    .iter_mut()
                    .find(|existing| existing.id == artifact.id)
                {
                    *existing = artifact.clone();
                } else {
                    artifacts.push(artifact.clone());
                }
            }
            DomainEvent::ExternalDeliveryProcessed {
                provider,
                delivery_id,
                binding_key,
                occurred_at,
                ..
            } => {
                self.external_deliveries
                    .insert(format!("{provider}:{delivery_id}"), *occurred_at);
                self.external_binding_clocks
                    .entry(binding_key.clone())
                    .and_modify(|current| *current = (*current).max(*occurred_at))
                    .or_insert(*occurred_at);
            }
            DomainEvent::ExternalInteractionProcessed { receipt } => {
                let key = format!("{}:{}", receipt.provider, receipt.delivery_id);
                if self.external_interactions.contains_key(&key) {
                    return Err(MambaError::Validation(format!(
                        "external interaction already processed: {key}"
                    )));
                }
                self.external_interactions.insert(key, receipt.clone());
            }
            DomainEvent::TaskSubmitted {
                flow_id,
                task_id,
                at,
                ..
            } => {
                let task = self.task_mut(flow_id, task_id)?;
                task.status = TaskStatus::Submitted;
                task.last_heartbeat = Some(*at);
            }
            DomainEvent::TaskCompleted {
                flow_id,
                task_id,
                at,
                ..
            } => {
                let task = self.task_mut(flow_id, task_id)?;
                task.status = TaskStatus::Completed;
                task.blocker = None;
                task.last_heartbeat = Some(*at);
            }
            DomainEvent::TrackingAttentionRaised { attention } => {
                self.flow(&attention.flow_id)?
                    .task(&attention.task_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "task",
                        id: attention.task_id.clone(),
                    })?;
                self.attentions
                    .insert(attention.id.clone(), attention.clone());
            }
            DomainEvent::TrackingAttentionResolved {
                flow_id,
                task_id,
                attention_id,
                kind,
                resolved_at,
                ..
            } => {
                let attention =
                    self.attentions
                        .get_mut(attention_id)
                        .ok_or_else(|| MambaError::NotFound {
                            entity: "tracking attention",
                            id: attention_id.clone(),
                        })?;
                if attention.flow_id != *flow_id
                    || attention.task_id != *task_id
                    || attention.kind != *kind
                {
                    return Err(MambaError::Validation(format!(
                        "tracking attention {} resolution does not match its source",
                        attention_id
                    )));
                }
                attention.resolved_at = Some(*resolved_at);
            }
            DomainEvent::TrackingEscalationRaised { escalation } => {
                let attention = self
                    .attentions
                    .get(&escalation.attention_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "tracking attention",
                        id: escalation.attention_id.clone(),
                    })?;
                if attention.flow_id != escalation.flow_id
                    || attention.task_id != escalation.task_id
                {
                    return Err(MambaError::Validation(format!(
                        "tracking escalation {} does not match its attention",
                        escalation.id
                    )));
                }
                self.escalations
                    .insert(escalation.id.clone(), escalation.clone());
            }
            DomainEvent::TrackingEscalationAcknowledged {
                flow_id,
                task_id,
                escalation_id,
                acknowledged_by,
                acknowledged_at,
            } => {
                let escalation = self.escalation_mut(escalation_id, flow_id, task_id)?;
                escalation.acknowledged_at = Some(*acknowledged_at);
                escalation.acknowledged_by = Some(acknowledged_by.clone());
            }
            DomainEvent::TrackingEscalationResolved {
                flow_id,
                task_id,
                escalation_id,
                resolved_at,
                ..
            } => {
                self.escalation_mut(escalation_id, flow_id, task_id)?
                    .resolved_at = Some(*resolved_at);
            }
            DomainEvent::ExecutorStarted { .. } | DomainEvent::ExecutorFailed { .. } => {}
            DomainEvent::ExecutorFinished { record } => {
                self.executions.insert(record.id.clone(), record.clone());
            }
            DomainEvent::RemoteFlightAuthorized { lease } => {
                self.flow(&lease.flow_id)?
                    .task(&lease.task_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "task",
                        id: lease.task_id.clone(),
                    })?;
                self.principal(&lease.principal_id)?;
                self.flight_leases
                    .insert(lease.id.clone(), lease.as_ref().clone());
            }
            DomainEvent::ResourceLeaseAcquired { lease } => {
                let flight = self
                    .flight_leases
                    .get(&lease.flight_lease_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "flight lease",
                        id: lease.flight_lease_id.clone(),
                    })?;
                if flight.flow_id != lease.flow_id
                    || flight.task_id != lease.task_id
                    || flight.principal_id != lease.principal_id
                {
                    return Err(MambaError::Validation(
                        "resource lease does not match its flight".into(),
                    ));
                }
                if self.resource_leases.values().any(|active| {
                    active.id != lease.id && active.conflicts_with(&lease.claim, lease.issued_at)
                }) {
                    return Err(MambaError::InvalidTransition(format!(
                        "resource {:?}:{} is already leased",
                        lease.claim.kind, lease.claim.key
                    )));
                }
                self.resource_leases.insert(lease.id.clone(), lease.clone());
            }
            DomainEvent::ResourceLeaseReleased {
                flow_id,
                task_id,
                resource_lease_id,
                released_at,
                reason,
            } => {
                let lease = self
                    .resource_leases
                    .get_mut(resource_lease_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "resource lease",
                        id: resource_lease_id.clone(),
                    })?;
                if lease.flow_id != *flow_id || lease.task_id != *task_id {
                    return Err(MambaError::Validation(
                        "resource release does not match its flight scope".into(),
                    ));
                }
                if lease.status != ResourceLeaseStatus::Active {
                    return Err(MambaError::InvalidTransition(format!(
                        "resource lease {resource_lease_id} is already released"
                    )));
                }
                lease.status = ResourceLeaseStatus::Released;
                lease.released_at = Some(*released_at);
                lease.release_reason = Some(reason.clone());
            }
            DomainEvent::RemoteFlightClaimed {
                flow_id,
                task_id,
                lease_id,
                run_id,
                claimed_at,
            } => {
                let lease = self.flight_lease_mut(lease_id, flow_id, task_id)?;
                lease.status = FlightLeaseStatus::Active;
                lease.run_id = Some(run_id.clone());
                lease.claimed_at = Some(*claimed_at);
            }
            DomainEvent::RemoteFlightRevoked {
                flow_id,
                task_id,
                lease_id,
                revoked_at,
                ..
            } => {
                let lease = self.flight_lease_mut(lease_id, flow_id, task_id)?;
                lease.status = FlightLeaseStatus::Revoked;
                lease.finished_at = Some(*revoked_at);
            }
            DomainEvent::RemoteFlightExpired {
                flow_id,
                task_id,
                lease_id,
                expired_at,
            } => {
                let lease = self.flight_lease_mut(lease_id, flow_id, task_id)?;
                lease.status = FlightLeaseStatus::Expired;
                lease.finished_at = Some(*expired_at);
            }
            DomainEvent::RemoteFlightFinished {
                flow_id,
                task_id,
                lease_id,
                landed,
                report,
                finished_at,
            } => {
                let lease = self.flight_lease_mut(lease_id, flow_id, task_id)?;
                lease.status = if *landed {
                    FlightLeaseStatus::Landed
                } else {
                    FlightLeaseStatus::Crashed
                };
                lease.finished_at = Some(*finished_at);
                lease.report = Some(report.clone());
            }
            DomainEvent::FlightRecoveryDecided { decision, .. } => {
                if !self.flight_leases.contains_key(&decision.parent_lease_id) {
                    return Err(MambaError::NotFound {
                        entity: "parent flight lease",
                        id: decision.parent_lease_id.clone(),
                    });
                }
                self.flight_recoveries
                    .insert(decision.id.clone(), decision.clone());
            }
            DomainEvent::FlowCompleted { flow_id, at, .. } => {
                let flow = self.flow_mut(flow_id)?;
                flow.status = FlowStatus::Completed;
                flow.completed_at = Some(*at);
            }
        }

        self.last_sequence = envelope.sequence;
        Ok(())
    }

    pub fn organization(&self) -> Result<&Organization> {
        self.organization
            .as_ref()
            .ok_or(MambaError::OrganizationNotInitialized)
    }

    pub fn tenant(&self) -> Result<&Tenant> {
        self.tenant.as_ref().ok_or(MambaError::TenantNotInitialized)
    }

    pub fn roles_for(&self, principal_id: &str) -> Vec<OrganizationRole> {
        let mut roles = self
            .role_bindings
            .values()
            .filter(|binding| binding.is_active() && binding.principal_id == principal_id)
            .map(|binding| binding.role)
            .collect::<Vec<_>>();
        roles.sort();
        roles.dedup();
        roles
    }

    pub fn has_role(&self, principal_id: &str, role: OrganizationRole) -> bool {
        self.role_bindings.values().any(|binding| {
            binding.is_active() && binding.principal_id == principal_id && binding.role == role
        })
    }

    pub fn flow(&self, id: &str) -> Result<&Flow> {
        self.flows.get(id).ok_or_else(|| MambaError::NotFound {
            entity: "flow",
            id: id.to_string(),
        })
    }

    pub fn flow_mut(&mut self, id: &str) -> Result<&mut Flow> {
        self.flows.get_mut(id).ok_or_else(|| MambaError::NotFound {
            entity: "flow",
            id: id.to_string(),
        })
    }

    pub fn principal(&self, id_or_name: &str) -> Result<&Principal> {
        self.principals
            .get(id_or_name)
            .or_else(|| {
                self.principals
                    .values()
                    .find(|principal| principal.name.eq_ignore_ascii_case(id_or_name))
            })
            .ok_or_else(|| MambaError::NotFound {
                entity: "principal",
                id: id_or_name.to_string(),
            })
    }

    pub fn external_identity(
        &self,
        provider: &str,
        external_user_id: &str,
    ) -> Result<&ExternalIdentityBinding> {
        self.external_identities
            .values()
            .find(|binding| {
                binding.is_active()
                    && binding.provider == provider
                    && binding.external_user_id == external_user_id
            })
            .ok_or_else(|| MambaError::NotFound {
                entity: "active external identity",
                id: format!("{provider}:{external_user_id}"),
            })
    }

    pub fn work_calendar(&self, id_or_name: &str) -> Result<&WorkCalendar> {
        let principal = self.principal(id_or_name)?;
        self.calendars
            .get(&principal.id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "work calendar",
                id: principal.id.clone(),
            })
    }

    fn notification_delivery_mut(&mut self, id: &str) -> Result<&mut NotificationDelivery> {
        self.notification_deliveries
            .get_mut(id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "notification delivery",
                id: id.to_string(),
            })
    }

    pub fn team(&self, id_or_name: &str) -> Result<&Team> {
        self.teams
            .get(id_or_name)
            .or_else(|| {
                self.teams
                    .values()
                    .find(|team| team.name.eq_ignore_ascii_case(id_or_name))
            })
            .ok_or_else(|| MambaError::NotFound {
                entity: "team",
                id: id_or_name.to_string(),
            })
    }

    pub fn find_task(&self, task_id: &str) -> Result<(&Flow, &crate::domain::Task)> {
        self.flows
            .values()
            .find_map(|flow| flow.task(task_id).map(|task| (flow, task)))
            .ok_or_else(|| MambaError::NotFound {
                entity: "task",
                id: task_id.to_string(),
            })
    }

    pub fn active_attentions(&self) -> impl Iterator<Item = &TrackingAttention> {
        self.attentions
            .values()
            .filter(|attention| attention.is_active())
    }

    pub fn active_escalations(&self) -> impl Iterator<Item = &TrackingEscalation> {
        self.escalations
            .values()
            .filter(|escalation| escalation.is_active())
    }

    fn flight_lease_mut(
        &mut self,
        lease_id: &str,
        flow_id: &str,
        task_id: &str,
    ) -> Result<&mut FlightLease> {
        let lease = self
            .flight_leases
            .get_mut(lease_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "flight lease",
                id: lease_id.to_string(),
            })?;
        if lease.flow_id != flow_id || lease.task_id != task_id {
            return Err(MambaError::Validation(format!(
                "flight lease {lease_id} does not match its task"
            )));
        }
        Ok(lease)
    }

    fn escalation_mut(
        &mut self,
        escalation_id: &str,
        flow_id: &str,
        task_id: &str,
    ) -> Result<&mut TrackingEscalation> {
        let escalation =
            self.escalations
                .get_mut(escalation_id)
                .ok_or_else(|| MambaError::NotFound {
                    entity: "tracking escalation",
                    id: escalation_id.to_string(),
                })?;
        if escalation.flow_id != flow_id || escalation.task_id != task_id {
            return Err(MambaError::Validation(format!(
                "tracking escalation {} event does not match its source",
                escalation_id
            )));
        }
        Ok(escalation)
    }

    fn task_mut(&mut self, flow_id: &str, task_id: &str) -> Result<&mut crate::domain::Task> {
        self.flow_mut(flow_id)?
            .task_mut(task_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "task",
                id: task_id.to_string(),
            })
    }

    fn refresh_flow_finish(&mut self, flow_id: &str) -> Result<()> {
        let flow = self.flow_mut(flow_id)?;
        if let Some(value) = flow.tasks.iter().map(|task| task.estimate.p50_finish).max() {
            flow.p50_finish = value;
        }
        if let Some(value) = flow.tasks.iter().map(|task| task.estimate.p80_finish).max() {
            flow.p80_finish = value;
        }
        Ok(())
    }

    fn apply_schedule_revision(
        &mut self,
        flow_id: &str,
        revision: &FlowScheduleRevision,
    ) -> Result<()> {
        let flow = self.flow_mut(flow_id)?;
        for task in &mut flow.tasks {
            let estimate = revision.task_estimates.get(&task.id).ok_or_else(|| {
                MambaError::Validation(format!(
                    "flow schedule revision has no estimate for task {}",
                    task.id
                ))
            })?;
            task.estimate = estimate.clone();
        }
        if revision.task_estimates.len() != flow.tasks.len() {
            return Err(MambaError::Validation(format!(
                "flow schedule revision for {flow_id} has unexpected tasks"
            )));
        }
        flow.p50_finish = revision.p50_finish;
        flow.p80_finish = revision.p80_finish;
        flow.critical_path = revision.critical_path.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::event::CURRENT_EVENT_VERSION;

    #[test]
    fn replay_rejects_event_stream_gaps() {
        let organization = Organization {
            id: "ORG-1".into(),
            name: "Mamba".into(),
            created_at: Utc::now(),
        };
        let envelope = EventEnvelope {
            event_version: CURRENT_EVENT_VERSION,
            sequence: 2,
            id: "EVT-2".into(),
            organization_id: organization.id.clone(),
            flow_id: None,
            actor: "admin".into(),
            kind: "organization.initialized".into(),
            event: DomainEvent::OrganizationInitialized { organization },
            occurred_at: Utc::now(),
        };

        let error = OrganizationState::replay(&[envelope]).unwrap_err();
        assert!(
            matches!(error, MambaError::Validation(message) if message.contains("expected next sequence 1"))
        );
    }
}
