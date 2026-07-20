use chrono::{DateTime, Duration, Utc};

use super::MambaApp;
use crate::domain::{AvailabilityBlock, FlowScheduleRevision, FlowStatus, WorkCalendar, Workday};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;
use crate::scheduler::reschedule;
use crate::state::OrganizationState;

impl MambaApp {
    #[allow(clippy::too_many_arguments)]
    pub fn configure_work_calendar(
        &mut self,
        target: &str,
        utc_offset_minutes: i32,
        working_days: Vec<Workday>,
        day_start_minute: u16,
        day_end_minute: u16,
        actor: &str,
    ) -> Result<WorkCalendar> {
        let principal = self.state.principal(target)?.clone();
        let now = Utc::now();
        let mut working_days = working_days;
        working_days.sort();
        working_days.dedup();
        let calendar = WorkCalendar {
            principal_id: principal.id.clone(),
            utc_offset_minutes,
            working_days,
            day_start_minute,
            day_end_minute,
            time_off: self.state.work_calendar(&principal.id)?.time_off.clone(),
            updated_by: actor.to_string(),
            updated_at: now,
        };
        crate::calendar::validate(&calendar)?;
        let mut projected = self.state.clone();
        projected
            .calendars
            .insert(principal.id.clone(), calendar.clone());
        let mut events = vec![DomainEvent::WorkCalendarConfigured {
            calendar: calendar.clone(),
        }];
        events.extend(reschedule_for_principal(
            &projected,
            &principal.id,
            actor,
            now,
            "work calendar updated",
        )?);
        self.commit(actor, events)?;
        Ok(self.state.work_calendar(&principal.id)?.clone())
    }

    pub fn add_time_off(
        &mut self,
        target: &str,
        starts_at: DateTime<Utc>,
        ends_at: DateTime<Utc>,
        reason: &str,
        actor: &str,
    ) -> Result<AvailabilityBlock> {
        let principal = self.state.principal(target)?.clone();
        let reason = reason.trim();
        if reason.is_empty() || reason.chars().count() > 500 {
            return Err(MambaError::Validation(
                "time off reason must contain 1 to 500 characters".into(),
            ));
        }
        if starts_at >= ends_at {
            return Err(MambaError::Validation(
                "time off must end after it starts".into(),
            ));
        }
        if ends_at.signed_duration_since(starts_at) > Duration::days(366) {
            return Err(MambaError::Validation(
                "one time off block cannot exceed 366 days".into(),
            ));
        }
        let now = Utc::now();
        let block = AvailabilityBlock {
            id: new_id("OFF"),
            principal_id: principal.id.clone(),
            starts_at,
            ends_at,
            reason: reason.to_string(),
            created_by: actor.to_string(),
            created_at: now,
            cancelled_by: None,
            cancelled_at: None,
        };
        let mut projected = self.state.clone();
        projected
            .calendars
            .get_mut(&principal.id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "work calendar",
                id: principal.id.clone(),
            })?
            .time_off
            .push(block.clone());
        let mut events = vec![DomainEvent::TimeOffAdded {
            block: block.clone(),
        }];
        events.extend(reschedule_for_principal(
            &projected,
            &principal.id,
            actor,
            now,
            "time off added",
        )?);
        self.commit(actor, events)?;
        Ok(block)
    }

    pub fn cancel_time_off(
        &mut self,
        target: &str,
        block_id: &str,
        actor: &str,
    ) -> Result<AvailabilityBlock> {
        let principal = self.state.principal(target)?.clone();
        let calendar = self.state.work_calendar(&principal.id)?;
        let block = calendar
            .time_off
            .iter()
            .find(|block| block.id == block_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "time off block",
                id: block_id.to_string(),
            })?;
        if !block.is_active() {
            return Err(MambaError::InvalidTransition(format!(
                "time off block {block_id} is already cancelled"
            )));
        }
        let now = Utc::now();
        let mut projected = self.state.clone();
        let projected_block = projected
            .calendars
            .get_mut(&principal.id)
            .and_then(|calendar| {
                calendar
                    .time_off
                    .iter_mut()
                    .find(|block| block.id == block_id)
            })
            .ok_or_else(|| MambaError::NotFound {
                entity: "time off block",
                id: block_id.to_string(),
            })?;
        projected_block.cancelled_by = Some(actor.to_string());
        projected_block.cancelled_at = Some(now);
        let mut events = vec![DomainEvent::TimeOffCancelled {
            principal_id: principal.id.clone(),
            block_id: block_id.to_string(),
            cancelled_by: actor.to_string(),
            cancelled_at: now,
        }];
        events.extend(reschedule_for_principal(
            &projected,
            &principal.id,
            actor,
            now,
            "time off cancelled",
        )?);
        self.commit(actor, events)?;
        Ok(self
            .state
            .work_calendar(&principal.id)?
            .time_off
            .iter()
            .find(|block| block.id == block_id)
            .expect("cancelled block remains in calendar")
            .clone())
    }
}

fn reschedule_for_principal(
    state: &OrganizationState,
    principal_id: &str,
    actor: &str,
    now: DateTime<Utc>,
    reason: &str,
) -> Result<Vec<DomainEvent>> {
    state
        .flows
        .values()
        .filter(|flow| matches!(flow.status, FlowStatus::Approved | FlowStatus::Active))
        .filter(|flow| {
            flow.tasks.iter().any(|task| {
                !task.status.is_terminal()
                    && task
                        .assignment
                        .as_ref()
                        .is_some_and(|assignment| assignment.owner.id == principal_id)
            })
        })
        .map(|flow| {
            let scheduled = reschedule(flow, state, now)?;
            Ok(DomainEvent::FlowRescheduled {
                flow_id: flow.id.clone(),
                revision: FlowScheduleRevision {
                    task_estimates: scheduled.task_estimates,
                    p50_finish: scheduled.p50_finish,
                    p80_finish: scheduled.p80_finish,
                    critical_path: scheduled.critical_path,
                    reason: format!("{reason} for {principal_id}"),
                    revised_by: actor.to_string(),
                    revised_at: now,
                },
            })
        })
        .collect()
}
