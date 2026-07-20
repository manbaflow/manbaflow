use super::MambaApp;
use crate::domain::{
    AssignmentTarget, Flow, FlowMessage, Principal, PrincipalKind, TargetKind, Task, TaskStatus,
};
use crate::error::{MambaError, Result};

impl MambaApp {
    pub(super) fn principal_has_flow_access(&self, flow: &Flow, principal: &Principal) -> bool {
        self.principal_is_flow_participant(flow, principal)
            || self.state.messages.values().any(|message| {
                message.flow_id == flow.id
                    && (message.sender_id == principal.id
                        || !self.message_recipient_ids(message, principal).is_empty())
            })
    }

    pub(super) fn principal_is_flow_participant(&self, flow: &Flow, principal: &Principal) -> bool {
        flow.demand.requester == principal.id
            || flow.demand.requester == principal.name
            || flow
                .tasks
                .iter()
                .any(|task| self.principal_is_task_actor(task, principal))
    }

    pub(super) fn principal_is_task_actor(&self, task: &Task, principal: &Principal) -> bool {
        task.assignment.as_ref().is_some_and(|assignment| {
            assignment.owner.id == principal.id
                || (assignment.owner.kind == TargetKind::Team
                    && principal.team_id.as_deref() == Some(assignment.owner.id.as_str()))
                || assignment
                    .copilots
                    .iter()
                    .any(|copilot| copilot.id == principal.id)
                || principal.owner_id.as_deref() == Some(assignment.owner.id.as_str())
                || self
                    .state
                    .principals
                    .get(&assignment.owner.id)
                    .and_then(|owner| owner.owner_id.as_deref())
                    == Some(principal.id.as_str())
        })
    }

    pub(super) fn message_target_is_flow_participant(
        &self,
        flow: &Flow,
        target: &AssignmentTarget,
    ) -> bool {
        match target.kind {
            TargetKind::Human | TargetKind::Agent => self
                .state
                .principals
                .get(&target.id)
                .is_some_and(|principal| self.principal_is_flow_participant(flow, principal)),
            TargetKind::Team => {
                flow.tasks.iter().any(|task| {
                    task.assignment.as_ref().is_some_and(|assignment| {
                        assignment.owner.id == target.id
                            || assignment
                                .copilots
                                .iter()
                                .any(|copilot| copilot.id == target.id)
                    })
                }) || self
                    .state
                    .principal(&flow.demand.requester)
                    .ok()
                    .and_then(|requester| requester.team_id.as_deref())
                    == Some(target.id.as_str())
            }
        }
    }

    pub(super) fn message_recipient_ids(
        &self,
        message: &FlowMessage,
        principal: &Principal,
    ) -> Vec<String> {
        message
            .recipients
            .iter()
            .filter(|recipient| match recipient.kind {
                TargetKind::Human => recipient.id == principal.id,
                TargetKind::Agent => {
                    recipient.id == principal.id
                        || (principal.kind == PrincipalKind::Human
                            && self
                                .state
                                .principals
                                .get(&recipient.id)
                                .and_then(|agent| agent.owner_id.as_deref())
                                == Some(principal.id.as_str()))
                }
                TargetKind::Team => principal.team_id.as_deref() == Some(recipient.id.as_str()),
            })
            .map(|recipient| recipient.id.clone())
            .collect()
    }

    pub(super) fn ensure_task_actor(&self, task: &Task, actor: &str) -> Result<()> {
        if task.assignment.is_none() {
            return Err(MambaError::NoEligibleAssignee(task.title.clone()));
        }
        let principal = self.state.principal(actor)?;
        if self.principal_is_task_actor(task, principal) {
            Ok(())
        } else {
            Err(MambaError::PermissionDenied(format!(
                "{} is not assigned to task {}",
                principal.name, task.id
            )))
        }
    }

    pub(super) fn ensure_dependencies_complete(&self, flow: &Flow, task: &Task) -> Result<()> {
        let incomplete = task
            .depends_on
            .iter()
            .filter_map(|id| flow.task(id))
            .filter(|dependency| dependency.status != TaskStatus::Completed)
            .map(|dependency| dependency.key.clone())
            .collect::<Vec<_>>();
        if incomplete.is_empty() {
            Ok(())
        } else {
            Err(MambaError::InvalidTransition(format!(
                "task {} is waiting for: {}",
                task.key,
                incomplete.join(", ")
            )))
        }
    }
}

pub(super) fn ensure_status(task: &Task, expected: &[TaskStatus]) -> Result<()> {
    if expected.contains(&task.status) {
        Ok(())
    } else {
        Err(MambaError::InvalidTransition(format!(
            "task {} is {:?}, expected one of {:?}",
            task.id, task.status, expected
        )))
    }
}
