use std::collections::BTreeMap;

use crate::domain::{
    ExecutionRecord, Flow, FlowStatus, Organization, Principal, TaskStatus, Team, TrackingAttention,
};
use crate::error::{MambaError, Result};
use crate::event::{DomainEvent, EventEnvelope};

#[derive(Clone, Debug, Default)]
pub struct OrganizationState {
    pub organization: Option<Organization>,
    pub teams: BTreeMap<String, Team>,
    pub principals: BTreeMap<String, Principal>,
    pub flows: BTreeMap<String, Flow>,
    pub executions: BTreeMap<String, ExecutionRecord>,
    pub attentions: BTreeMap<String, TrackingAttention>,
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
        if envelope.sequence <= self.last_sequence {
            return Err(MambaError::Validation(format!(
                "event sequence {} is not after {}",
                envelope.sequence, self.last_sequence
            )));
        }

        match &envelope.event {
            DomainEvent::OrganizationInitialized { organization } => {
                if self.organization.is_some() {
                    return Err(MambaError::OrganizationAlreadyInitialized);
                }
                self.organization = Some(organization.clone());
            }
            DomainEvent::TeamCreated { team } => {
                self.teams.insert(team.id.clone(), team.clone());
            }
            DomainEvent::PrincipalRegistered { principal } => {
                self.principals
                    .insert(principal.id.clone(), principal.clone());
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
            DomainEvent::ExecutorStarted { .. } | DomainEvent::ExecutorFailed { .. } => {}
            DomainEvent::ExecutorFinished { record } => {
                self.executions.insert(record.id.clone(), record.clone());
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
}
