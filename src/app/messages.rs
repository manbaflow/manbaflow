use std::collections::BTreeSet;

use chrono::Utc;

use super::MambaApp;
use crate::domain::{
    AssignmentTarget, FlowMessage, FlowMessageKind, MessageInboxItem, PrincipalKind, TargetKind,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl MambaApp {
    #[allow(clippy::too_many_arguments)]
    pub fn post_flow_message(
        &mut self,
        flow_id: &str,
        task_id: Option<&str>,
        sender: &str,
        kind: FlowMessageKind,
        recipients: &[String],
        body: &str,
        requires_ack: bool,
    ) -> Result<FlowMessage> {
        let sender = self.state.principal(sender)?.clone();
        if !sender.active {
            return Err(MambaError::PermissionDenied(format!(
                "principal {} is inactive",
                sender.name
            )));
        }
        let flow = self.state.flow(flow_id)?.clone();
        if !self.principal_has_flow_access(&flow, &sender) {
            return Err(MambaError::PermissionDenied(format!(
                "{} cannot access flow {}",
                sender.name, flow.id
            )));
        }
        let task_id = task_id
            .map(|value| {
                flow.task(value)
                    .map(|task| task.id.clone())
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "task",
                        id: value.to_string(),
                    })
            })
            .transpose()?;
        let body = body.trim();
        if body.is_empty() || body.chars().count() > 4_000 {
            return Err(MambaError::Validation(
                "flow message body must contain 1 to 4000 characters".into(),
            ));
        }
        if recipients.is_empty() || recipients.len() > 32 {
            return Err(MambaError::Validation(
                "flow message must target between 1 and 32 recipients".into(),
            ));
        }

        let requester = self.state.principal(&flow.demand.requester)?;
        let sender_is_requester = sender.id == requester.id;
        let mut recipient_ids = BTreeSet::new();
        let mut resolved = Vec::new();
        for recipient in recipients {
            let target = if let Ok(principal) = self.state.principal(recipient) {
                if !principal.active {
                    return Err(MambaError::Validation(format!(
                        "recipient {} is inactive",
                        principal.name
                    )));
                }
                AssignmentTarget {
                    kind: match principal.kind {
                        PrincipalKind::Human => TargetKind::Human,
                        PrincipalKind::Agent => TargetKind::Agent,
                    },
                    id: principal.id.clone(),
                    name: principal.name.clone(),
                }
            } else {
                let team = self.state.team(recipient)?;
                if !team.active {
                    return Err(MambaError::Validation(format!(
                        "recipient team {} is inactive",
                        team.name
                    )));
                }
                AssignmentTarget {
                    kind: TargetKind::Team,
                    id: team.id.clone(),
                    name: team.name.clone(),
                }
            };
            if !sender_is_requester && !self.message_target_is_flow_participant(&flow, &target) {
                return Err(MambaError::PermissionDenied(format!(
                    "only demand requester {} can bring {} into flow {}",
                    requester.name, target.name, flow.id
                )));
            }
            if recipient_ids.insert(target.id.clone()) {
                resolved.push(target);
            }
        }
        let message = FlowMessage {
            id: new_id("MSG"),
            flow_id: flow.id,
            task_id,
            kind,
            sender_id: sender.id,
            sender_name: sender.name.clone(),
            recipients: resolved,
            body: body.to_string(),
            requires_ack,
            acknowledgements: Vec::new(),
            created_at: Utc::now(),
        };
        self.commit(
            &sender.name,
            vec![DomainEvent::FlowMessagePosted {
                message: message.clone(),
            }],
        )?;
        Ok(message)
    }

    pub fn message_inbox(
        &self,
        target: &str,
        include_acknowledged: bool,
    ) -> Result<Vec<MessageInboxItem>> {
        let principal = self.state.principal(target)?;
        let mut items = self
            .state
            .messages
            .values()
            .filter_map(|message| {
                let represented = self.message_recipient_ids(message, principal);
                if represented.is_empty() {
                    return None;
                }
                let pending_recipient_ids = represented
                    .into_iter()
                    .filter(|recipient_id| !message.recipient_is_acknowledged(recipient_id))
                    .collect::<Vec<_>>();
                if message.requires_ack && pending_recipient_ids.is_empty() && !include_acknowledged
                {
                    return None;
                }
                Some(MessageInboxItem {
                    message: message.clone(),
                    pending_recipient_ids,
                })
            })
            .collect::<Vec<_>>();
        items.sort_by_key(|item| std::cmp::Reverse(item.message.created_at));
        Ok(items)
    }

    pub fn flow_messages(&self, flow_id: &str, actor: &str) -> Result<Vec<FlowMessage>> {
        let flow = self.state.flow(flow_id)?;
        let principal = self.state.principal(actor)?;
        if !self.principal_has_flow_access(flow, principal) {
            return Err(MambaError::PermissionDenied(format!(
                "{} cannot access flow {}",
                principal.name, flow.id
            )));
        }
        let requester = self.state.principal(&flow.demand.requester)?;
        let mut messages = self
            .state
            .messages
            .values()
            .filter(|message| message.flow_id == flow.id)
            .filter(|message| {
                principal.id == requester.id
                    || message.sender_id == principal.id
                    || !self.message_recipient_ids(message, principal).is_empty()
                    || message.task_id.as_deref().is_some_and(|task_id| {
                        flow.task(task_id)
                            .is_some_and(|task| self.principal_is_task_actor(task, principal))
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        messages.sort_by_key(|message| message.created_at);
        Ok(messages)
    }
}
