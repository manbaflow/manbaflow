use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};

use super::MambaApp;
use crate::application::tracker;
use crate::domain::{
    AttentionSeverity, FlowStatus, Principal, PrincipalKind, TrackingAttention, TrackingEscalation,
    TrackingScan,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl MambaApp {
    pub fn scan_tracking(&mut self, stale_after_hours: u64, actor: &str) -> Result<TrackingScan> {
        self.scan_tracking_with_policy(stale_after_hours, 4, actor)
    }

    pub fn scan_tracking_with_policy(
        &mut self,
        stale_after_hours: u64,
        escalate_after_hours: u64,
        actor: &str,
    ) -> Result<TrackingScan> {
        self.scan_tracking_with_policy_at(
            Utc::now(),
            stale_after_hours,
            escalate_after_hours,
            actor,
        )
    }

    #[cfg(test)]
    pub(super) fn scan_tracking_at(
        &mut self,
        now: DateTime<Utc>,
        stale_after_hours: u64,
        actor: &str,
    ) -> Result<TrackingScan> {
        self.scan_tracking_with_policy_at(now, stale_after_hours, 4, actor)
    }

    pub(super) fn scan_tracking_with_policy_at(
        &mut self,
        now: DateTime<Utc>,
        stale_after_hours: u64,
        escalate_after_hours: u64,
        actor: &str,
    ) -> Result<TrackingScan> {
        self.state.organization()?;
        if stale_after_hours == 0 {
            return Err(MambaError::Validation(
                "stale-after hours must be greater than zero".into(),
            ));
        }
        let stale_after = i64::try_from(stale_after_hours)
            .ok()
            .and_then(Duration::try_hours)
            .ok_or_else(|| MambaError::Validation("stale-after hours is too large".into()))?;
        let escalate_after = i64::try_from(escalate_after_hours)
            .ok()
            .and_then(Duration::try_hours)
            .ok_or_else(|| MambaError::Validation("escalate-after hours is too large".into()))?;
        let findings = tracker::evaluate(&self.state, now, stale_after);
        let desired = findings
            .into_iter()
            .map(|finding| {
                (
                    (
                        finding.flow_id.clone(),
                        finding.task_id.clone(),
                        finding.kind,
                    ),
                    finding,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let current = self
            .state
            .active_attentions()
            .map(|attention| {
                (
                    (
                        attention.flow_id.clone(),
                        attention.task_id.clone(),
                        attention.kind,
                    ),
                    attention.id.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let mut events = Vec::new();
        let mut resolved_ids = Vec::new();
        for (key, attention_id) in &current {
            if !desired.contains_key(key) {
                resolved_ids.push(attention_id.clone());
                events.push(DomainEvent::TrackingAttentionResolved {
                    flow_id: key.0.clone(),
                    task_id: key.1.clone(),
                    attention_id: attention_id.clone(),
                    kind: key.2,
                    resolved_at: now,
                    reason: "condition cleared by tracker scan".into(),
                });
            }
        }

        let mut raised_ids = Vec::new();
        let mut projected_attentions = Vec::new();
        for (key, finding) in &desired {
            if let Some(attention_id) = current.get(key) {
                projected_attentions.push(
                    self.state
                        .attentions
                        .get(attention_id)
                        .ok_or_else(|| MambaError::NotFound {
                            entity: "tracking attention",
                            id: attention_id.clone(),
                        })?
                        .clone(),
                );
            } else {
                let attention = TrackingAttention {
                    id: new_id("ATTN"),
                    flow_id: finding.flow_id.clone(),
                    task_id: finding.task_id.clone(),
                    kind: finding.kind,
                    severity: finding.severity,
                    summary: finding.summary.clone(),
                    raised_at: now,
                    resolved_at: None,
                };
                raised_ids.push(attention.id.clone());
                projected_attentions.push(attention.clone());
                events.push(DomainEvent::TrackingAttentionRaised { attention });
            }
        }

        let current_escalations = self
            .state
            .active_escalations()
            .map(|escalation| (escalation.attention_id.clone(), escalation.id.clone()))
            .collect::<BTreeMap<_, _>>();
        let projected_ids = projected_attentions
            .iter()
            .map(|attention| attention.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut resolved_escalation_ids = Vec::new();
        for (attention_id, escalation_id) in &current_escalations {
            if !projected_ids.contains(attention_id.as_str()) {
                let escalation = self.state.escalations.get(escalation_id).ok_or_else(|| {
                    MambaError::NotFound {
                        entity: "tracking escalation",
                        id: escalation_id.clone(),
                    }
                })?;
                resolved_escalation_ids.push(escalation_id.clone());
                events.push(DomainEvent::TrackingEscalationResolved {
                    flow_id: escalation.flow_id.clone(),
                    task_id: escalation.task_id.clone(),
                    escalation_id: escalation_id.clone(),
                    resolved_at: now,
                    reason: "source attention resolved".into(),
                });
            }
        }

        let mut escalated_ids = Vec::new();
        for attention in &projected_attentions {
            let should_escalate = attention.severity == AttentionSeverity::Critical
                || now - attention.raised_at >= escalate_after;
            if !should_escalate || current_escalations.contains_key(&attention.id) {
                continue;
            }
            let Some(recipient) = self.escalation_recipient(attention) else {
                continue;
            };
            let escalation = TrackingEscalation {
                id: new_id("ESC"),
                attention_id: attention.id.clone(),
                flow_id: attention.flow_id.clone(),
                task_id: attention.task_id.clone(),
                recipient_id: recipient.id.clone(),
                recipient_name: recipient.name.clone(),
                reason: attention.summary.clone(),
                raised_at: now,
                acknowledged_at: None,
                acknowledged_by: None,
                resolved_at: None,
            };
            escalated_ids.push(escalation.id.clone());
            events.push(DomainEvent::TrackingEscalationRaised { escalation });
        }

        if !events.is_empty() {
            self.commit(actor, events)?;
        }

        let collect_attentions = |ids: &[String]| {
            ids.iter()
                .filter_map(|id| self.state.attentions.get(id).cloned())
                .collect::<Vec<_>>()
        };
        let collect_escalations = |ids: &[String]| {
            ids.iter()
                .filter_map(|id| self.state.escalations.get(id).cloned())
                .collect::<Vec<_>>()
        };
        let mut active = self.state.active_attentions().cloned().collect::<Vec<_>>();
        active.sort_by(|left, right| {
            right
                .severity
                .cmp(&left.severity)
                .then_with(|| left.raised_at.cmp(&right.raised_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        let scanned_tasks = self
            .state
            .flows
            .values()
            .filter(|flow| matches!(flow.status, FlowStatus::Approved | FlowStatus::Active))
            .flat_map(|flow| &flow.tasks)
            .filter(|task| !task.status.is_terminal())
            .count();
        Ok(TrackingScan {
            scanned_at: now,
            scanned_tasks,
            raised: collect_attentions(&raised_ids),
            resolved: collect_attentions(&resolved_ids),
            active,
            escalated: collect_escalations(&escalated_ids),
            resolved_escalations: collect_escalations(&resolved_escalation_ids),
        })
    }

    pub fn escalation_inbox(
        &self,
        target: &str,
        include_resolved: bool,
    ) -> Result<Vec<&TrackingEscalation>> {
        let principal = self.state.principal(target)?;
        let mut escalations = self
            .state
            .escalations
            .values()
            .filter(|escalation| escalation.recipient_id == principal.id)
            .filter(|escalation| include_resolved || escalation.is_active())
            .collect::<Vec<_>>();
        escalations.sort_by(|left, right| {
            right
                .needs_acknowledgement()
                .cmp(&left.needs_acknowledgement())
                .then_with(|| right.raised_at.cmp(&left.raised_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(escalations)
    }

    fn escalation_recipient(&self, attention: &TrackingAttention) -> Option<&Principal> {
        let flow = self.state.flows.get(&attention.flow_id)?;
        self.state
            .principal(&flow.demand.requester)
            .ok()
            .filter(|principal| principal.kind == PrincipalKind::Human)
    }
}
