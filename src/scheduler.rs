use std::collections::{BTreeMap, VecDeque};

use chrono::{Duration, Utc};

use crate::domain::{Assignment, Estimate, TargetKind, Task, TaskDraft, TaskStatus};
use crate::error::{MambaError, Result};
use crate::ids::new_id;
use crate::state::OrganizationState;

pub struct Schedule {
    pub tasks: Vec<Task>,
    pub p50_finish: chrono::DateTime<Utc>,
    pub p80_finish: chrono::DateTime<Utc>,
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
    let mut longest = BTreeMap::<String, (f64, Vec<String>)>::new();

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
        let earliest_start = draft
            .depends_on
            .iter()
            .filter_map(|dependency| estimates.get(dependency))
            .map(|estimate| estimate.p80_finish)
            .max()
            .unwrap_or(now);
        let p50_finish = earliest_start + hours(p50_hours);
        let p80_finish = earliest_start + hours(p80_hours);

        let (previous_hours, mut path) = draft
            .depends_on
            .iter()
            .filter_map(|dependency| longest.get(dependency))
            .max_by(|left, right| left.0.total_cmp(&right.0))
            .cloned()
            .unwrap_or((0.0, Vec::new()));
        path.push(draft.key.clone());
        longest.insert(draft.key.clone(), (previous_hours + p80_hours, path));

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
    let critical_path = longest
        .into_values()
        .max_by(|left, right| left.0.total_cmp(&right.0))
        .map(|(_, path)| path)
        .unwrap_or_default();

    Ok(Schedule {
        tasks,
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

fn hours(value: f64) -> Duration {
    Duration::milliseconds((value.max(0.0) * 3_600_000.0).round() as i64)
}

fn round_hours(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{AssignmentTarget, TargetKind};

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
}
