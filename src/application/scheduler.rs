use std::collections::{BTreeMap, VecDeque};

use chrono::{DateTime, Duration, Utc};

use crate::domain::{Assignment, Estimate, Flow, TargetKind, Task, TaskDraft, TaskStatus};
use crate::error::{MambaError, Result};
use crate::ids::new_id;
use crate::state::OrganizationState;

pub struct Schedule {
    pub tasks: Vec<Task>,
    pub p50_finish: chrono::DateTime<Utc>,
    pub p80_finish: chrono::DateTime<Utc>,
    pub critical_path: Vec<String>,
}

pub struct Reschedule {
    pub task_estimates: BTreeMap<String, Estimate>,
    pub p50_finish: DateTime<Utc>,
    pub p80_finish: DateTime<Utc>,
    pub critical_path: Vec<String>,
}

pub fn schedule(
    drafts: &[TaskDraft],
    assignments: &BTreeMap<String, Assignment>,
    state: &OrganizationState,
) -> Result<Schedule> {
    if drafts.is_empty() {
        return Err(MambaError::Validation(
            "a plan must contain at least one task".to_string(),
        ));
    }

    let by_key = drafts
        .iter()
        .enumerate()
        .map(|(index, task)| (task.key.clone(), index))
        .collect::<BTreeMap<_, _>>();
    if by_key.len() != drafts.len() {
        return Err(MambaError::Validation(
            "task keys must be unique".to_string(),
        ));
    }

    let mut indegree = vec![0usize; drafts.len()];
    let mut dependents = vec![Vec::new(); drafts.len()];
    for (index, task) in drafts.iter().enumerate() {
        for dependency in &task.depends_on {
            let dependency_index = *by_key.get(dependency).ok_or_else(|| {
                MambaError::Validation(format!(
                    "task `{}` depends on unknown task `{dependency}`",
                    task.key
                ))
            })?;
            if dependency_index == index {
                return Err(MambaError::Validation(format!(
                    "task `{}` cannot depend on itself",
                    task.key
                )));
            }
            indegree[index] += 1;
            dependents[dependency_index].push(index);
        }
    }

    let mut queue = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, degree)| (*degree == 0).then_some(index))
        .collect::<VecDeque<_>>();
    let mut order = Vec::with_capacity(drafts.len());
    while let Some(index) = queue.pop_front() {
        order.push(index);
        for dependent in &dependents[index] {
            indegree[*dependent] -= 1;
            if indegree[*dependent] == 0 {
                queue.push_back(*dependent);
            }
        }
    }
    if order.len() != drafts.len() {
        return Err(MambaError::Validation(
            "task dependency graph contains a cycle".to_string(),
        ));
    }

    let now = Utc::now();
    let ids = drafts
        .iter()
        .map(|task| (task.key.clone(), new_id("TSK")))
        .collect::<BTreeMap<_, _>>();
    let mut estimates = BTreeMap::<String, Estimate>::new();
    let mut paths = BTreeMap::<String, Vec<String>>::new();

    for index in order {
        let draft = &drafts[index];
        let assignment = assignments
            .get(&draft.key)
            .ok_or_else(|| MambaError::NoEligibleAssignee(draft.title.clone()))?;
        let capacity = assignment_capacity(assignment, state).max(0.1);
        let coordination_factor = 1.0
            + if draft.depends_on.len() > 1 { 0.1 } else { 0.0 }
            + if draft.requires_human { 0.08 } else { 0.0 };
        let p50_hours = round_hours(draft.effort_hours / capacity * coordination_factor);
        let p80_hours = round_hours(p50_hours * 1.4);
        let dependency_start = draft
            .depends_on
            .iter()
            .filter_map(|dependency| estimates.get(dependency))
            .map(|estimate| estimate.p80_finish)
            .max()
            .unwrap_or(now);
        let earliest_start = assignment_start(assignment, state, dependency_start)?;
        let p50_finish = assignment_finish(assignment, state, earliest_start, p50_hours)?;
        let p80_finish = assignment_finish(assignment, state, earliest_start, p80_hours)?;

        let mut path = draft
            .depends_on
            .iter()
            .filter_map(|dependency| {
                Some((
                    estimates.get(dependency)?.p80_finish,
                    paths.get(dependency)?,
                ))
            })
            .max_by_key(|(finish, _)| *finish)
            .map(|(_, path)| path.clone())
            .unwrap_or_default();
        path.push(draft.key.clone());
        paths.insert(draft.key.clone(), path);

        estimates.insert(
            draft.key.clone(),
            Estimate {
                effort_hours: draft.effort_hours,
                p50_hours,
                p80_hours,
                confidence: "medium".to_string(),
                rationale: vec![
                    format!("base effort: {:.1}h", draft.effort_hours),
                    format!("owner capacity factor: {:.2}", capacity),
                    format!("coordination factor: {:.2}", coordination_factor),
                    assignment_calendar_rationale(assignment, state),
                ],
                earliest_start,
                p50_finish,
                p80_finish,
            },
        );
    }

    let tasks = drafts
        .iter()
        .map(|draft| Task {
            id: ids[&draft.key].clone(),
            key: draft.key.clone(),
            title: draft.title.clone(),
            description: draft.description.clone(),
            required_capabilities: draft.required_capabilities.clone(),
            depends_on: draft
                .depends_on
                .iter()
                .map(|key| ids[key].clone())
                .collect(),
            requires_human: draft.requires_human,
            acceptance_criteria: draft.acceptance_criteria.clone(),
            assignment: assignments.get(&draft.key).cloned(),
            estimate: estimates[&draft.key].clone(),
            status: TaskStatus::Proposed,
            blocker: None,
            last_heartbeat: None,
            evidence: Vec::new(),
            external_artifacts: Vec::new(),
        })
        .collect::<Vec<_>>();
    let p50_finish = tasks
        .iter()
        .map(|task| task.estimate.p50_finish)
        .max()
        .unwrap_or(now);
    let p80_finish = tasks
        .iter()
        .map(|task| task.estimate.p80_finish)
        .max()
        .unwrap_or(now);
    let critical_path = tasks
        .iter()
        .max_by_key(|task| task.estimate.p80_finish)
        .and_then(|task| paths.remove(&task.key))
        .unwrap_or_default();

    Ok(Schedule {
        tasks,
        p50_finish,
        p80_finish,
        critical_path,
    })
}

pub fn reschedule(
    flow: &Flow,
    state: &OrganizationState,
    now: DateTime<Utc>,
) -> Result<Reschedule> {
    if flow.tasks.is_empty() {
        return Err(MambaError::Validation(
            "a flow must contain at least one task".into(),
        ));
    }
    let by_id = flow
        .tasks
        .iter()
        .enumerate()
        .map(|(index, task)| (task.id.clone(), index))
        .collect::<BTreeMap<_, _>>();
    let mut indegree = vec![0usize; flow.tasks.len()];
    let mut dependents = vec![Vec::new(); flow.tasks.len()];
    for (index, task) in flow.tasks.iter().enumerate() {
        for dependency in &task.depends_on {
            let dependency_index = *by_id.get(dependency).ok_or_else(|| {
                MambaError::Validation(format!(
                    "task `{}` depends on unknown task `{dependency}`",
                    task.key
                ))
            })?;
            if dependency_index == index {
                return Err(MambaError::Validation(format!(
                    "task `{}` cannot depend on itself",
                    task.key
                )));
            }
            indegree[index] += 1;
            dependents[dependency_index].push(index);
        }
    }
    let mut queue = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, degree)| (*degree == 0).then_some(index))
        .collect::<VecDeque<_>>();
    let mut order = Vec::with_capacity(flow.tasks.len());
    while let Some(index) = queue.pop_front() {
        order.push(index);
        for dependent in &dependents[index] {
            indegree[*dependent] -= 1;
            if indegree[*dependent] == 0 {
                queue.push_back(*dependent);
            }
        }
    }
    if order.len() != flow.tasks.len() {
        return Err(MambaError::Validation(
            "task dependency graph contains a cycle".into(),
        ));
    }

    let mut estimates = BTreeMap::<String, Estimate>::new();
    let mut paths = BTreeMap::<String, Vec<String>>::new();
    for index in order {
        let task = &flow.tasks[index];
        let terminal = task.status.is_terminal();
        let dependency_start = task
            .depends_on
            .iter()
            .filter_map(|dependency| estimates.get(dependency))
            .map(|estimate| estimate.p80_finish)
            .max()
            .unwrap_or(now)
            .max(now);
        let mut estimate = task.estimate.clone();
        if terminal {
            let finished_at = task.last_heartbeat.unwrap_or(now).min(now);
            estimate.earliest_start = finished_at;
            estimate.p50_finish = finished_at;
            estimate.p80_finish = finished_at;
            estimate.confidence = "actual".into();
            if !estimate
                .rationale
                .iter()
                .any(|reason| reason == "terminal task preserved as an actual waypoint")
            {
                estimate
                    .rationale
                    .push("terminal task preserved as an actual waypoint".into());
            }
        } else {
            let assignment = task
                .assignment
                .as_ref()
                .ok_or_else(|| MambaError::NoEligibleAssignee(task.title.clone()))?;
            let capacity = assignment_capacity(assignment, state).max(0.1);
            let coordination_factor = 1.0
                + if task.depends_on.len() > 1 { 0.1 } else { 0.0 }
                + if task.requires_human { 0.08 } else { 0.0 };
            estimate.p50_hours =
                round_hours(estimate.effort_hours / capacity * coordination_factor);
            estimate.p80_hours = round_hours(estimate.p50_hours * 1.4);
            let earliest_start = assignment_start(assignment, state, dependency_start)?;
            estimate.earliest_start = earliest_start;
            estimate.p50_finish =
                assignment_finish(assignment, state, earliest_start, estimate.p50_hours)?;
            estimate.p80_finish =
                assignment_finish(assignment, state, earliest_start, estimate.p80_hours)?;
            estimate.confidence = "rescheduled".into();
            estimate.rationale = vec![
                format!("base effort: {:.1}h", estimate.effort_hours),
                format!("owner capacity factor: {:.2}", capacity),
                format!("coordination factor: {:.2}", coordination_factor),
                assignment_calendar_rationale(assignment, state),
                format!("dynamic schedule anchor: {}", now.to_rfc3339()),
            ];
        }

        let mut path = task
            .depends_on
            .iter()
            .filter_map(|dependency| {
                Some((
                    estimates.get(dependency)?.p80_finish,
                    paths.get(dependency)?,
                ))
            })
            .max_by_key(|(finish, _)| *finish)
            .map(|(_, path)| path.clone())
            .unwrap_or_default();
        path.push(task.key.clone());
        paths.insert(task.id.clone(), path);
        estimates.insert(task.id.clone(), estimate);
    }

    let non_terminal = flow.tasks.iter().filter(|task| !task.status.is_terminal());
    let p50_finish = non_terminal
        .clone()
        .filter_map(|task| estimates.get(&task.id))
        .map(|estimate| estimate.p50_finish)
        .max()
        .unwrap_or(now);
    let p80_finish = non_terminal
        .filter_map(|task| estimates.get(&task.id))
        .map(|estimate| estimate.p80_finish)
        .max()
        .unwrap_or(now);
    let critical_path = flow
        .tasks
        .iter()
        .filter(|task| !task.status.is_terminal())
        .filter_map(|task| Some((estimates.get(&task.id)?.p80_finish, paths.get(&task.id)?)))
        .max_by_key(|(finish, _)| *finish)
        .map(|(_, path)| path.clone())
        .unwrap_or_default();
    Ok(Reschedule {
        task_estimates: estimates,
        p50_finish,
        p80_finish,
        critical_path,
    })
}

fn assignment_capacity(assignment: &Assignment, state: &OrganizationState) -> f64 {
    if assignment.owner.kind == TargetKind::Team {
        return 1.0;
    }
    state
        .principals
        .get(&assignment.owner.id)
        .map(|principal| f64::from(principal.capacity_percent) / 100.0)
        .unwrap_or(1.0)
}

fn assignment_start(
    assignment: &Assignment,
    state: &OrganizationState,
    start: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    match assignment_calendar(assignment, state) {
        Some(calendar) => crate::calendar::next_available(calendar, start),
        None => Ok(start),
    }
}

fn assignment_finish(
    assignment: &Assignment,
    state: &OrganizationState,
    start: DateTime<Utc>,
    working_hours: f64,
) -> Result<DateTime<Utc>> {
    match assignment_calendar(assignment, state) {
        Some(calendar) => crate::calendar::add_working_hours(calendar, start, working_hours),
        None => Ok(start + hours(working_hours)),
    }
}

fn assignment_calendar<'a>(
    assignment: &Assignment,
    state: &'a OrganizationState,
) -> Option<&'a crate::domain::WorkCalendar> {
    (assignment.owner.kind != TargetKind::Team)
        .then(|| state.calendars.get(&assignment.owner.id))
        .flatten()
}

fn assignment_calendar_rationale(assignment: &Assignment, state: &OrganizationState) -> String {
    assignment_calendar(assignment, state).map_or_else(
        || "work calendar: continuous team availability".into(),
        |calendar| format!("work calendar: {}", crate::calendar::summary(calendar)),
    )
}

fn hours(value: f64) -> Duration {
    Duration::milliseconds((value.max(0.0) * 3_600_000.0).round() as i64)
}

fn round_hours(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::calendar::parse_workdays;
    use crate::domain::{AssignmentTarget, TargetKind, WorkCalendar};

    #[test]
    fn dependency_moves_downstream_start_after_upstream_p80() {
        let drafts = vec![
            TaskDraft {
                key: "a".into(),
                title: "A".into(),
                description: "A".into(),
                required_capabilities: vec![],
                depends_on: vec![],
                effort_hours: 2.0,
                requires_human: false,
                acceptance_criteria: vec!["done".into()],
            },
            TaskDraft {
                key: "b".into(),
                title: "B".into(),
                description: "B".into(),
                required_capabilities: vec![],
                depends_on: vec!["a".into()],
                effort_hours: 1.0,
                requires_human: false,
                acceptance_criteria: vec!["done".into()],
            },
        ];
        let target = AssignmentTarget {
            kind: TargetKind::Team,
            id: "T-1".into(),
            name: "Team".into(),
        };
        let assignments = drafts
            .iter()
            .map(|task| {
                (
                    task.key.clone(),
                    Assignment {
                        owner: target.clone(),
                        copilots: vec![],
                        score: 1.0,
                        rationale: vec![],
                    },
                )
            })
            .collect();
        let schedule = schedule(&drafts, &assignments, &OrganizationState::default()).unwrap();
        assert!(schedule.tasks[1].estimate.earliest_start >= schedule.tasks[0].estimate.p80_finish);
        assert_eq!(schedule.critical_path, vec!["a", "b"]);
    }

    #[test]
    fn assignment_finish_obeys_the_owners_work_calendar() {
        let mut state = OrganizationState::default();
        state.calendars.insert(
            "H-1".into(),
            WorkCalendar {
                principal_id: "H-1".into(),
                utc_offset_minutes: 8 * 60,
                working_days: parse_workdays("mon,tue,wed,thu,fri").unwrap(),
                day_start_minute: 9 * 60,
                day_end_minute: 18 * 60,
                time_off: Vec::new(),
                updated_by: "admin".into(),
                updated_at: Utc::now(),
            },
        );
        let assignment = Assignment {
            owner: AssignmentTarget {
                kind: TargetKind::Human,
                id: "H-1".into(),
                name: "Engineer".into(),
            },
            copilots: Vec::new(),
            score: 1.0,
            rationale: Vec::new(),
        };
        let friday = Utc.with_ymd_and_hms(2026, 7, 17, 8, 0, 0).unwrap();

        assert_eq!(
            assignment_finish(&assignment, &state, friday, 4.0).unwrap(),
            Utc.with_ymd_and_hms(2026, 7, 20, 3, 0, 0).unwrap()
        );
        assert!(assignment_calendar_rationale(&assignment, &state).contains("Mon,Tue"));
    }
}
