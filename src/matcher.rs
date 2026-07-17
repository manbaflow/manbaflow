use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;

use crate::domain::{
    Assignment, AssignmentTarget, Principal, PrincipalKind, TargetKind, TaskDraft,
};
use crate::error::{MambaError, Result};
use crate::ids::normalize_capability;
use crate::state::OrganizationState;

pub struct Matcher<'a> {
    state: &'a OrganizationState,
    active_load: BTreeMap<String, usize>,
}

impl<'a> Matcher<'a> {
    pub fn new(state: &'a OrganizationState) -> Self {
        let mut active_load = BTreeMap::new();
        for flow in state.flows.values() {
            for task in &flow.tasks {
                if task.status.is_terminal() {
                    continue;
                }
                if let Some(assignment) = &task.assignment {
                    *active_load.entry(assignment.owner.id.clone()).or_insert(0) += 1;
                }
            }
        }
        Self { state, active_load }
    }

    pub fn match_task(&mut self, task: &TaskDraft) -> Result<Assignment> {
        let required = normalized_set(&task.required_capabilities);
        let mut candidates = self
            .state
            .principals
            .values()
            .filter(|principal| principal.active)
            .filter(|principal| !task.requires_human || principal.kind == PrincipalKind::Human)
            .filter_map(|principal| self.score_principal(principal, &required))
            .collect::<Vec<_>>();

        candidates.sort_by(|left, right| {
            right
                .0
                .total_cmp(&left.0)
                .then_with(|| left.1.name.cmp(&right.1.name))
        });

        if let Some((score, principal, rationale)) = candidates.into_iter().next() {
            let assignment = Assignment {
                owner: target_for_principal(principal),
                copilots: self.copilots_for(principal, &required),
                score,
                rationale,
            };
            *self.active_load.entry(principal.id.clone()).or_insert(0) += 1;
            return Ok(assignment);
        }

        let mut teams = self
            .state
            .teams
            .values()
            .filter(|team| team.active)
            .filter(|team| {
                !task.requires_human
                    || self.state.principals.values().any(|principal| {
                        principal.active
                            && principal.kind == PrincipalKind::Human
                            && principal.team_id.as_deref() == Some(team.id.as_str())
                    })
            })
            .filter_map(|team| {
                let capabilities = normalized_set(&team.capabilities);
                let coverage = coverage(&required, &capabilities);
                (required.is_empty() || coverage >= 1.0).then_some((coverage, team))
            })
            .collect::<Vec<_>>();
        teams.sort_by(|left, right| {
            right
                .0
                .total_cmp(&left.0)
                .then_with(|| left.1.name.cmp(&right.1.name))
        });

        if let Some((coverage, team)) = teams.into_iter().next() {
            return Ok(Assignment {
                owner: AssignmentTarget {
                    kind: TargetKind::Team,
                    id: team.id.clone(),
                    name: team.name.clone(),
                },
                copilots: Vec::new(),
                score: coverage * 70.0,
                rationale: vec![
                    "no individual had a complete capability match".to_string(),
                    format!("team capability coverage: {:.0}%", coverage * 100.0),
                ],
            });
        }

        Err(MambaError::NoEligibleAssignee(task.title.clone()))
    }

    fn score_principal(
        &self,
        principal: &'a Principal,
        required: &BTreeSet<String>,
    ) -> Option<(f64, &'a Principal, Vec<String>)> {
        let capabilities = normalized_set(&principal.capabilities);
        let coverage = coverage(required, &capabilities);
        if !required.is_empty() && coverage < 1.0 {
            return None;
        }

        let capacity = f64::from(principal.capacity_percent) / 100.0;
        if capacity <= 0.0 {
            return None;
        }
        let load = *self.active_load.get(&principal.id).unwrap_or(&0) as f64;
        let load_penalty = (load * 7.5).min(30.0);
        let executor_bonus = if principal.executor.is_some() {
            3.0
        } else {
            0.0
        };
        let calendar = self.state.calendars.get(&principal.id);
        let now = Utc::now();
        let next_available =
            calendar.and_then(|calendar| crate::calendar::next_available(calendar, now).ok());
        let availability_delay_hours = next_available
            .map(|next| next.signed_duration_since(now).num_minutes().max(0) as f64 / 60.0)
            .unwrap_or(0.0);
        let availability_penalty = (availability_delay_hours / 24.0 * 5.0).min(25.0);
        let score = coverage * 70.0 + capacity * 30.0 + executor_bonus
            - load_penalty
            - availability_penalty;

        let mut rationale = vec![
            format!("capability coverage: {:.0}%", coverage * 100.0),
            format!("declared capacity: {}%", principal.capacity_percent),
            format!("current active assignments: {load:.0}"),
        ];
        if let Some(calendar) = calendar {
            rationale.push(format!(
                "work calendar: {}",
                crate::calendar::summary(calendar)
            ));
        }
        if availability_delay_hours >= 0.1 {
            rationale.push(format!(
                "next availability: {} ({availability_delay_hours:.1}h delay)",
                next_available.unwrap().to_rfc3339()
            ));
        }

        Some((score, principal, rationale))
    }

    fn copilots_for(
        &self,
        owner: &Principal,
        required: &BTreeSet<String>,
    ) -> Vec<AssignmentTarget> {
        let mut copilots = self
            .state
            .principals
            .values()
            .filter(|principal| principal.active && principal.id != owner.id)
            .filter(|principal| match owner.kind {
                PrincipalKind::Human => {
                    principal.kind == PrincipalKind::Agent
                        && principal.owner_id.as_deref() == Some(owner.id.as_str())
                }
                PrincipalKind::Agent => owner.owner_id.as_deref() == Some(principal.id.as_str()),
            })
            .filter(|principal| match owner.kind {
                PrincipalKind::Human => {
                    required.is_empty()
                        || coverage(required, &normalized_set(&principal.capabilities)) > 0.0
                }
                PrincipalKind::Agent => true,
            })
            .map(target_for_principal)
            .collect::<Vec<_>>();
        copilots.truncate(2);
        copilots
    }
}

fn target_for_principal(principal: &Principal) -> AssignmentTarget {
    AssignmentTarget {
        kind: match principal.kind {
            PrincipalKind::Human => TargetKind::Human,
            PrincipalKind::Agent => TargetKind::Agent,
        },
        id: principal.id.clone(),
        name: principal.name.clone(),
    }
}

fn normalized_set(values: &[String]) -> BTreeSet<String> {
    values
        .iter()
        .map(|value| normalize_capability(value))
        .filter(|value| !value.is_empty())
        .collect()
}

fn coverage(required: &BTreeSet<String>, actual: &BTreeSet<String>) -> f64 {
    if required.is_empty() {
        return 1.0;
    }
    let matches = required.intersection(actual).count();
    matches as f64 / required.len() as f64
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;
    use crate::domain::{AvailabilityBlock, Organization, Principal, WorkCalendar};

    #[test]
    fn human_work_is_assigned_to_human_with_owned_agent_as_copilot() {
        let mut state = OrganizationState {
            organization: Some(Organization {
                id: "ORG-1".into(),
                name: "Mamba".into(),
                created_at: Utc::now(),
            }),
            ..OrganizationState::default()
        };
        state.principals.insert(
            "P-1".into(),
            Principal {
                id: "P-1".into(),
                name: "Engineer".into(),
                kind: PrincipalKind::Human,
                team_id: None,
                owner_id: None,
                capabilities: vec!["rust".into()],
                capacity_percent: 100,
                executor: None,
                active: true,
                created_at: Utc::now(),
            },
        );
        state.principals.insert(
            "A-1".into(),
            Principal {
                id: "A-1".into(),
                name: "Codex".into(),
                kind: PrincipalKind::Agent,
                team_id: None,
                owner_id: Some("P-1".into()),
                capabilities: vec!["rust".into()],
                capacity_percent: 100,
                executor: None,
                active: true,
                created_at: Utc::now(),
            },
        );
        let task = TaskDraft {
            key: "implementation".into(),
            title: "Implement".into(),
            description: "Implement it".into(),
            required_capabilities: vec!["Rust".into()],
            depends_on: vec![],
            effort_hours: 8.0,
            requires_human: true,
            acceptance_criteria: vec!["tests pass".into()],
        };

        let assignment = Matcher::new(&state).match_task(&task).unwrap();
        assert_eq!(assignment.owner.id, "P-1");
        assert_eq!(assignment.copilots[0].id, "A-1");
    }

    #[test]
    fn current_time_off_lowers_a_candidates_priority() {
        let now = Utc::now();
        let mut state = OrganizationState::default();
        for (id, name) in [("P-1", "Alice"), ("P-2", "Bob")] {
            state.principals.insert(
                id.into(),
                Principal {
                    id: id.into(),
                    name: name.into(),
                    kind: PrincipalKind::Human,
                    team_id: None,
                    owner_id: None,
                    capabilities: vec!["delivery".into()],
                    capacity_percent: 100,
                    executor: None,
                    active: true,
                    created_at: now,
                },
            );
            state
                .calendars
                .insert(id.into(), WorkCalendar::always_available(id.into(), now));
        }
        state
            .calendars
            .get_mut("P-1")
            .unwrap()
            .time_off
            .push(AvailabilityBlock {
                id: "OFF-1".into(),
                principal_id: "P-1".into(),
                starts_at: now - Duration::hours(1),
                ends_at: now + Duration::days(10),
                reason: "leave".into(),
                created_by: "Alice".into(),
                created_at: now,
                cancelled_by: None,
                cancelled_at: None,
            });
        let task = TaskDraft {
            key: "launch".into(),
            title: "Launch".into(),
            description: "Launch".into(),
            required_capabilities: vec!["delivery".into()],
            depends_on: Vec::new(),
            effort_hours: 4.0,
            requires_human: true,
            acceptance_criteria: vec!["done".into()],
        };

        let assignment = Matcher::new(&state).match_task(&task).unwrap();
        assert_eq!(assignment.owner.name, "Bob");
        assert!(
            assignment
                .rationale
                .iter()
                .any(|reason| reason.contains("work calendar"))
        );
    }
}
