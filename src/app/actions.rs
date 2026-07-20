use chrono::{DateTime, Utc};

use super::MambaApp;
use super::policy::ensure_status;
use crate::domain::{
    FlowMessage, MessageAcknowledgement, Principal, PrincipalKind, Task, TaskStatus,
    TrackingEscalation,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;

impl MambaApp {
    pub fn accept_task(&mut self, task_id: &str, actor: &str) -> Result<Task> {
        let (_, task_id, event) = self.prepare_task_accept(task_id, actor, Utc::now())?;
        self.commit(actor, vec![event])?;
        Ok(self.state.find_task(&task_id)?.1.clone())
    }

    pub fn reject_task(&mut self, task_id: &str, actor: &str, reason: &str) -> Result<Task> {
        let (_, task_id, event) = self.prepare_task_reject(task_id, actor, reason)?;
        self.commit(actor, vec![event])?;
        Ok(self.state.find_task(&task_id)?.1.clone())
    }

    pub fn acknowledge_flow_message(
        &mut self,
        message_id: &str,
        actor: &str,
    ) -> Result<FlowMessage> {
        let principal = self.state.principal(actor)?.clone();
        let (_, event) = self.prepare_message_ack(message_id, &principal, Utc::now())?;
        let Some(event) = event else {
            return Ok(self.state.messages[message_id].clone());
        };
        self.commit(&principal.name, vec![event])?;
        Ok(self.state.messages[message_id].clone())
    }

    pub fn acknowledge_escalation(
        &mut self,
        escalation_id: &str,
        actor: &str,
    ) -> Result<TrackingEscalation> {
        let principal = self.state.principal(actor)?.clone();
        let (_, event) = self.prepare_escalation_ack(escalation_id, &principal, Utc::now())?;
        self.commit(&principal.name, vec![event])?;
        self.state
            .escalations
            .get(escalation_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "tracking escalation",
                id: escalation_id.to_string(),
            })
    }

    pub(super) fn prepare_task_accept(
        &self,
        task_id: &str,
        actor: &str,
        accepted_at: DateTime<Utc>,
    ) -> Result<(String, String, DomainEvent)> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::Assigned])?;
        self.ensure_task_actor(&task, actor)?;
        let flow_id = flow.id;
        let task_id = task.id;
        Ok((
            flow_id.clone(),
            task_id.clone(),
            DomainEvent::TaskAccepted {
                flow_id,
                task_id,
                accepted_by: actor.to_string(),
                accepted_at,
            },
        ))
    }

    pub(super) fn prepare_task_reject(
        &self,
        task_id: &str,
        actor: &str,
        reason: &str,
    ) -> Result<(String, String, DomainEvent)> {
        let reason = reason.trim();
        if reason.is_empty() || reason.chars().count() > 500 || reason.chars().any(char::is_control)
        {
            return Err(MambaError::Validation(
                "task rejection reason must contain 1 to 500 printable characters".into(),
            ));
        }
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::Assigned])?;
        self.ensure_task_actor(&task, actor)?;
        let flow_id = flow.id;
        let task_id = task.id;
        Ok((
            flow_id.clone(),
            task_id.clone(),
            DomainEvent::TaskRejected {
                flow_id,
                task_id,
                rejected_by: actor.to_string(),
                reason: reason.to_string(),
            },
        ))
    }

    pub(super) fn prepare_message_ack(
        &self,
        message_id: &str,
        principal: &Principal,
        acknowledged_at: DateTime<Utc>,
    ) -> Result<(String, Option<DomainEvent>)> {
        let message = self
            .state
            .messages
            .get(message_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "flow message",
                id: message_id.to_string(),
            })?;
        if !message.requires_ack {
            return Err(MambaError::InvalidTransition(format!(
                "flow message {} does not require acknowledgement",
                message.id
            )));
        }
        let represented = self.message_recipient_ids(&message, principal);
        if represented.is_empty() {
            return Err(MambaError::PermissionDenied(format!(
                "{} is not a recipient of flow message {}",
                principal.name, message.id
            )));
        }
        let acknowledgements = represented
            .into_iter()
            .filter(|recipient_id| !message.recipient_is_acknowledged(recipient_id))
            .map(|recipient_id| MessageAcknowledgement {
                recipient_id,
                acknowledged_by_id: principal.id.clone(),
                acknowledged_by_name: principal.name.clone(),
                acknowledged_at,
            })
            .collect::<Vec<_>>();
        let flow_id = message.flow_id;
        let event = (!acknowledgements.is_empty()).then(|| DomainEvent::FlowMessageAcknowledged {
            flow_id: flow_id.clone(),
            message_id: message.id,
            acknowledgements,
        });
        Ok((flow_id, event))
    }

    pub(super) fn prepare_escalation_ack(
        &self,
        escalation_id: &str,
        principal: &Principal,
        acknowledged_at: DateTime<Utc>,
    ) -> Result<(String, DomainEvent)> {
        if principal.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "tracking escalation acknowledgement requires a human".into(),
            ));
        }
        let escalation = self
            .state
            .escalations
            .get(escalation_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "tracking escalation",
                id: escalation_id.to_string(),
            })?;
        if escalation.recipient_id != principal.id {
            return Err(MambaError::PermissionDenied(format!(
                "{} is not the recipient of escalation {}",
                principal.name, escalation.id
            )));
        }
        if !escalation.is_active() {
            return Err(MambaError::InvalidTransition(format!(
                "escalation {} is already resolved",
                escalation.id
            )));
        }
        if escalation.acknowledged_at.is_some() {
            return Err(MambaError::InvalidTransition(format!(
                "escalation {} is already acknowledged",
                escalation.id
            )));
        }
        let flow_id = escalation.flow_id;
        Ok((
            flow_id.clone(),
            DomainEvent::TrackingEscalationAcknowledged {
                flow_id,
                task_id: escalation.task_id,
                escalation_id: escalation.id,
                acknowledged_by: principal.name.clone(),
                acknowledged_at,
            },
        ))
    }
}
