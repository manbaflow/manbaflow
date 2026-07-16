use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::domain::{
    ApiCredential, AttentionSeverity, Demand, Evidence, ExecutionRecord, ExecutorConfig,
    ExecutorMode, Flow, FlowStatus, IssuedCredential, Organization, Principal, PrincipalKind,
    TargetKind, Task, TaskStatus, Team, TrackingAttention, TrackingEscalation, TrackingScan,
};
use crate::error::{MambaError, Result};
use crate::event::{DomainEvent, EventEnvelope};
use crate::executor::{ExecutionRequest, TerminalExecutor};
use crate::ids::{new_id, parse_capabilities};
use crate::matcher::Matcher;
use crate::planner::{PlannerKind, generate_plan};
use crate::scheduler::schedule;
use crate::state::OrganizationState;
use crate::store::EventStore;
use crate::tracker;

pub struct MambaApp {
    data_dir: PathBuf,
    store: EventStore,
    state: OrganizationState,
}

impl MambaApp {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;
        let store = EventStore::open(data_dir.join("flow.db"))?;
        let state = OrganizationState::replay(&store.load_all()?)?;
        Ok(Self {
            data_dir,
            store,
            state,
        })
    }

    pub fn state(&self) -> &OrganizationState {
        &self.state
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn reload(&mut self) -> Result<()> {
        self.state = OrganizationState::replay(&self.store.load_all()?)?;
        Ok(())
    }

    pub fn init_organization(&mut self, name: &str, actor: &str) -> Result<Organization> {
        if self.state.organization.is_some() {
            return Err(MambaError::OrganizationAlreadyInitialized);
        }
        if name.trim().is_empty() {
            return Err(MambaError::Validation(
                "organization name cannot be empty".into(),
            ));
        }
        let organization = Organization {
            id: new_id("ORG"),
            name: name.trim().to_string(),
            created_at: Utc::now(),
        };
        self.commit_as(
            &organization.id,
            actor,
            vec![DomainEvent::OrganizationInitialized {
                organization: organization.clone(),
            }],
        )?;
        Ok(organization)
    }

    pub fn create_team(&mut self, name: &str, capabilities: &str, actor: &str) -> Result<Team> {
        self.state.organization()?;
        if name.trim().is_empty() {
            return Err(MambaError::Validation("team name cannot be empty".into()));
        }
        if self
            .state
            .teams
            .values()
            .any(|team| team.name.eq_ignore_ascii_case(name.trim()))
        {
            return Err(MambaError::Validation(format!(
                "team already exists: {}",
                name.trim()
            )));
        }
        let team = Team {
            id: new_id("TEAM"),
            name: name.trim().to_string(),
            capabilities: parse_capabilities([capabilities.to_string()]),
            active: true,
            created_at: Utc::now(),
        };
        self.commit(actor, vec![DomainEvent::TeamCreated { team: team.clone() }])?;
        Ok(team)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_principal(
        &mut self,
        name: &str,
        kind: PrincipalKind,
        team: Option<&str>,
        owner: Option<&str>,
        capabilities: &str,
        capacity_percent: u8,
        executor: Option<ExecutorConfig>,
        actor: &str,
    ) -> Result<Principal> {
        self.state.organization()?;
        if name.trim().is_empty() {
            return Err(MambaError::Validation(
                "principal name cannot be empty".into(),
            ));
        }
        if !(1..=100).contains(&capacity_percent) {
            return Err(MambaError::Validation(
                "capacity must be between 1 and 100".into(),
            ));
        }
        if self
            .state
            .principals
            .values()
            .any(|principal| principal.name.eq_ignore_ascii_case(name.trim()))
        {
            return Err(MambaError::Validation(format!(
                "principal already exists: {}",
                name.trim()
            )));
        }
        if kind == PrincipalKind::Human && (owner.is_some() || executor.is_some()) {
            return Err(MambaError::Validation(
                "a human cannot have an owner or terminal executor".into(),
            ));
        }
        if kind == PrincipalKind::Agent && executor.is_none() {
            return Err(MambaError::Validation(
                "an agent requires a Claude Code or Codex executor".into(),
            ));
        }
        let team_id = team
            .map(|value| self.state.team(value).map(|team| team.id.clone()))
            .transpose()?;
        let owner_id = owner
            .map(|value| {
                let principal = self.state.principal(value)?;
                if principal.kind != PrincipalKind::Human {
                    return Err(MambaError::Validation(
                        "an agent owner must be a human".into(),
                    ));
                }
                Ok(principal.id.clone())
            })
            .transpose()?;
        if let Some(config) = &executor
            && !config.workspace.is_dir()
        {
            return Err(MambaError::InvalidWorkspace(config.workspace.clone()));
        }
        let principal = Principal {
            id: new_id(match kind {
                PrincipalKind::Human => "HUM",
                PrincipalKind::Agent => "AGT",
            }),
            name: name.trim().to_string(),
            kind,
            team_id,
            owner_id,
            capabilities: parse_capabilities([capabilities.to_string()]),
            capacity_percent,
            executor,
            active: true,
            created_at: Utc::now(),
        };
        self.commit(
            actor,
            vec![DomainEvent::PrincipalRegistered {
                principal: principal.clone(),
            }],
        )?;
        Ok(principal)
    }

    pub fn issue_api_credential(
        &mut self,
        target: &str,
        label: &str,
        actor: &str,
    ) -> Result<IssuedCredential> {
        self.state.organization()?;
        let principal = self.state.principal(target)?.clone();
        let label = label.trim();
        if label.is_empty() || label.chars().count() > 80 {
            return Err(MambaError::Validation(
                "credential label must contain 1 to 80 characters".into(),
            ));
        }
        let token = format!("mmb_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let token_hash = credential_hash(&token);
        let credential = ApiCredential {
            id: new_id("CRED"),
            principal_id: principal.id,
            label: label.to_string(),
            created_at: Utc::now(),
            revoked_at: None,
        };
        self.store.insert_credential(
            &credential.id,
            &credential.principal_id,
            &token_hash,
            credential.created_at,
        )?;
        if let Err(error) = self.commit(
            actor,
            vec![DomainEvent::ApiCredentialIssued {
                credential: credential.clone(),
            }],
        ) {
            let _ = self.store.delete_credential(&credential.id);
            return Err(error);
        }
        Ok(IssuedCredential { credential, token })
    }

    pub fn revoke_api_credential(
        &mut self,
        credential_id: &str,
        actor: &str,
    ) -> Result<ApiCredential> {
        let credential = self
            .state
            .credentials
            .get(credential_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "API credential",
                id: credential_id.to_string(),
            })?
            .clone();
        if !credential.is_active() {
            return Err(MambaError::InvalidTransition(format!(
                "API credential {} is already revoked",
                credential.id
            )));
        }
        let revoked_at = Utc::now();
        self.commit(
            actor,
            vec![DomainEvent::ApiCredentialRevoked {
                credential_id: credential.id.clone(),
                principal_id: credential.principal_id.clone(),
                revoked_at,
            }],
        )?;
        self.store.revoke_credential(&credential.id, revoked_at)?;
        Ok(self
            .state
            .credentials
            .get(credential_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "API credential",
                id: credential_id.to_string(),
            })?
            .clone())
    }

    pub fn authenticate_api_token(&self, token: &str) -> Result<Option<Principal>> {
        if token.len() != 68
            || !token.starts_with("mmb_")
            || !token[4..].bytes().all(|value| value.is_ascii_hexdigit())
        {
            return Ok(None);
        }
        let token_hash = credential_hash(token);
        let Some((credential_id, principal_id)) =
            self.store.authenticate_credential(&token_hash)?
        else {
            return Ok(None);
        };
        let Some(credential) = self.state.credentials.get(&credential_id) else {
            return Ok(None);
        };
        if !credential.is_active() || credential.principal_id != principal_id {
            return Ok(None);
        }
        Ok(self
            .state
            .principals
            .get(&principal_id)
            .filter(|principal| principal.active)
            .cloned())
    }

    pub async fn create_demand(
        &mut self,
        summary: &str,
        requester: &str,
        planner: PlannerKind,
        workspace: &Path,
        timeout_seconds: u64,
    ) -> Result<Flow> {
        self.state.organization()?;
        if summary.trim().is_empty() {
            return Err(MambaError::Validation("demand cannot be empty".into()));
        }
        if self.state.principals.is_empty() {
            return Err(MambaError::Validation(
                "register at least one human or agent before creating a demand".into(),
            ));
        }
        let requester = self.state.principal(requester)?;
        if requester.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "a demand requester must be a registered human".into(),
            ));
        }
        let requester = requester.name.clone();
        if !workspace.is_dir() {
            return Err(MambaError::InvalidWorkspace(workspace.to_path_buf()));
        }

        let flow_id = new_id("FLOW");
        let demand_id = new_id("DEM");
        let planner_log = self
            .data_dir
            .join("runs")
            .join(&flow_id)
            .join("planner.json");
        let plan = generate_plan(
            planner,
            summary,
            &self.state,
            workspace,
            planner_log,
            timeout_seconds,
        )
        .await?;

        let mut matcher = Matcher::new(&self.state);
        let mut assignments = BTreeMap::new();
        for task in &plan.tasks {
            assignments.insert(task.key.clone(), matcher.match_task(task)?);
        }
        let scheduled = schedule(&plan.tasks, &assignments, &self.state)?;
        let now = Utc::now();
        let demand = Demand {
            id: demand_id,
            flow_id: flow_id.clone(),
            requester: requester.clone(),
            summary: summary.trim().to_string(),
            created_at: now,
        };
        let flow = Flow {
            id: flow_id,
            demand: demand.clone(),
            prd: plan.prd,
            tasks: scheduled.tasks,
            status: FlowStatus::Draft,
            planner: planner.to_string(),
            p50_finish: scheduled.p50_finish,
            p80_finish: scheduled.p80_finish,
            critical_path: scheduled.critical_path,
            created_at: now,
            approved_at: None,
            completed_at: None,
        };
        self.commit(
            &requester,
            vec![
                DomainEvent::DemandCreated { demand },
                DomainEvent::PlanGenerated { flow: flow.clone() },
            ],
        )?;
        Ok(flow)
    }

    pub fn approve_flow(&mut self, flow_id: &str, approved_by: &str) -> Result<Flow> {
        let approver = self.state.principal(approved_by)?;
        if approver.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "flow approval requires a registered human".into(),
            ));
        }
        let flow = self.state.flow(flow_id)?.clone();
        if flow.demand.requester != approver.name && flow.demand.requester != approver.id {
            return Err(MambaError::PermissionDenied(format!(
                "only demand requester {} can approve flow {}",
                flow.demand.requester, flow.id
            )));
        }
        if flow.status != FlowStatus::Draft {
            return Err(MambaError::InvalidTransition(format!(
                "flow {} is {:?}, expected draft",
                flow.id, flow.status
            )));
        }
        let mut events = vec![DomainEvent::FlowApproved {
            flow_id: flow.id.clone(),
            approved_by: approved_by.to_string(),
            approved_at: Utc::now(),
        }];
        for task in &flow.tasks {
            let assignment = task
                .assignment
                .as_ref()
                .ok_or_else(|| MambaError::NoEligibleAssignee(task.title.clone()))?;
            events.push(DomainEvent::WorkRequestSent {
                flow_id: flow.id.clone(),
                task_id: task.id.clone(),
                target_id: assignment.owner.id.clone(),
            });
        }
        self.commit(approved_by, events)?;
        Ok(self.state.flow(flow_id)?.clone())
    }

    pub fn inbox(&self, target: &str) -> Result<Vec<(&Flow, &Task)>> {
        let principal = self.state.principal(target)?;
        let items = self
            .state
            .flows
            .values()
            .flat_map(|flow| flow.tasks.iter().map(move |task| (flow, task)))
            .filter(|(_, task)| {
                matches!(
                    task.status,
                    TaskStatus::Assigned
                        | TaskStatus::Accepted
                        | TaskStatus::InProgress
                        | TaskStatus::Blocked
                        | TaskStatus::Submitted
                )
            })
            .filter(|(_, task)| {
                task.assignment.as_ref().is_some_and(|assignment| {
                    assignment.owner.id == principal.id
                        || (assignment.owner.kind == TargetKind::Team
                            && principal.team_id.as_deref() == Some(assignment.owner.id.as_str()))
                        || assignment
                            .copilots
                            .iter()
                            .any(|copilot| copilot.id == principal.id)
                })
            })
            .collect();
        Ok(items)
    }

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
    fn scan_tracking_at(
        &mut self,
        now: DateTime<Utc>,
        stale_after_hours: u64,
        actor: &str,
    ) -> Result<TrackingScan> {
        self.scan_tracking_with_policy_at(now, stale_after_hours, 4, actor)
    }

    fn scan_tracking_with_policy_at(
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

    pub fn acknowledge_escalation(
        &mut self,
        escalation_id: &str,
        actor: &str,
    ) -> Result<TrackingEscalation> {
        let principal = self.state.principal(actor)?;
        if principal.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "tracking escalation acknowledgement requires a human".into(),
            ));
        }
        let escalation = self
            .state
            .escalations
            .get(escalation_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "tracking escalation",
                id: escalation_id.to_string(),
            })?
            .clone();
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
        let actor_name = principal.name.clone();
        self.commit(
            &actor_name,
            vec![DomainEvent::TrackingEscalationAcknowledged {
                flow_id: escalation.flow_id.clone(),
                task_id: escalation.task_id.clone(),
                escalation_id: escalation.id.clone(),
                acknowledged_by: actor_name.clone(),
                acknowledged_at: Utc::now(),
            }],
        )?;
        self.state
            .escalations
            .get(escalation_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "tracking escalation",
                id: escalation_id.to_string(),
            })
    }

    fn escalation_recipient(&self, attention: &TrackingAttention) -> Option<&Principal> {
        let flow = self.state.flows.get(&attention.flow_id)?;
        self.state
            .principal(&flow.demand.requester)
            .ok()
            .filter(|principal| principal.kind == PrincipalKind::Human)
    }

    pub fn accept_task(&mut self, task_id: &str, actor: &str) -> Result<Task> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::Assigned])?;
        self.ensure_task_actor(&task, actor)?;
        self.commit(
            actor,
            vec![DomainEvent::TaskAccepted {
                flow_id: flow.id,
                task_id: task.id.clone(),
                accepted_by: actor.to_string(),
                accepted_at: Utc::now(),
            }],
        )?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub fn reject_task(&mut self, task_id: &str, actor: &str, reason: &str) -> Result<Task> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::Assigned])?;
        self.ensure_task_actor(&task, actor)?;
        self.commit(
            actor,
            vec![DomainEvent::TaskRejected {
                flow_id: flow.id,
                task_id: task.id.clone(),
                rejected_by: actor.to_string(),
                reason: reason.to_string(),
            }],
        )?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub fn negotiate_task(
        &mut self,
        task_id: &str,
        actor: &str,
        effort_hours: f64,
    ) -> Result<Task> {
        if !effort_hours.is_finite() || effort_hours <= 0.0 {
            return Err(MambaError::Validation(
                "estimate must be greater than zero".into(),
            ));
        }
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(
            &task,
            &[
                TaskStatus::Assigned,
                TaskStatus::Accepted,
                TaskStatus::Blocked,
            ],
        )?;
        self.ensure_task_actor(&task, actor)?;
        let mut estimate = task.estimate.clone();
        let ratio_p50 = estimate.p50_hours / estimate.effort_hours.max(0.1);
        let ratio_p80 = estimate.p80_hours / estimate.effort_hours.max(0.1);
        estimate.effort_hours = effort_hours;
        estimate.p50_hours = round_hours(effort_hours * ratio_p50);
        estimate.p80_hours = round_hours(effort_hours * ratio_p80);
        estimate.p50_finish = estimate.earliest_start + hours(estimate.p50_hours);
        estimate.p80_finish = estimate.earliest_start + hours(estimate.p80_hours);
        estimate.confidence = "negotiated".into();
        estimate.rationale.push(format!(
            "{actor} negotiated base effort to {effort_hours:.1}h"
        ));
        self.commit(
            actor,
            vec![DomainEvent::TaskEstimateNegotiated {
                flow_id: flow.id,
                task_id: task.id.clone(),
                negotiated_by: actor.to_string(),
                estimate,
            }],
        )?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub fn start_task(&mut self, task_id: &str, actor: &str) -> Result<Task> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::Accepted, TaskStatus::Blocked])?;
        self.ensure_dependencies_complete(&flow, &task)?;
        self.ensure_task_actor(&task, actor)?;
        self.commit(
            actor,
            vec![DomainEvent::TaskStarted {
                flow_id: flow.id,
                task_id: task.id.clone(),
                started_by: actor.to_string(),
                started_at: Utc::now(),
            }],
        )?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub fn heartbeat_task(
        &mut self,
        task_id: &str,
        actor: &str,
        note: Option<String>,
    ) -> Result<Task> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::InProgress, TaskStatus::Blocked])?;
        self.ensure_task_actor(&task, actor)?;
        self.commit(
            actor,
            vec![DomainEvent::TaskHeartbeat {
                flow_id: flow.id,
                task_id: task.id.clone(),
                actor: actor.to_string(),
                note,
                at: Utc::now(),
            }],
        )?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub fn block_task(&mut self, task_id: &str, actor: &str, reason: &str) -> Result<Task> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::InProgress])?;
        self.ensure_task_actor(&task, actor)?;
        self.commit(
            actor,
            vec![DomainEvent::TaskBlocked {
                flow_id: flow.id,
                task_id: task.id.clone(),
                actor: actor.to_string(),
                reason: reason.to_string(),
                at: Utc::now(),
            }],
        )?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub fn add_evidence(
        &mut self,
        task_id: &str,
        actor: &str,
        kind: &str,
        uri: &str,
        summary: &str,
    ) -> Result<Evidence> {
        let (flow, task) = self.task_snapshot(task_id)?;
        if task.status.is_terminal() {
            return Err(MambaError::InvalidTransition(
                "cannot add evidence to a terminal task".into(),
            ));
        }
        self.ensure_task_actor(&task, actor)?;
        let evidence = Evidence {
            id: new_id("EVD"),
            kind: kind.to_string(),
            uri: uri.to_string(),
            summary: summary.to_string(),
            created_by: actor.to_string(),
            created_at: Utc::now(),
        };
        self.commit(
            actor,
            vec![DomainEvent::EvidenceAdded {
                flow_id: flow.id,
                task_id: task.id,
                evidence: evidence.clone(),
            }],
        )?;
        Ok(evidence)
    }

    pub fn submit_task(&mut self, task_id: &str, actor: &str) -> Result<Task> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::InProgress])?;
        self.ensure_task_actor(&task, actor)?;
        if task.evidence.is_empty() {
            return Err(MambaError::Validation(
                "attach at least one piece of evidence before submission".into(),
            ));
        }
        self.commit(
            actor,
            vec![DomainEvent::TaskSubmitted {
                flow_id: flow.id,
                task_id: task.id.clone(),
                submitted_by: actor.to_string(),
                at: Utc::now(),
            }],
        )?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub fn complete_task(&mut self, task_id: &str, actor: &str) -> Result<Task> {
        let principal = self.state.principal(actor)?;
        if principal.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "task completion requires a registered human".into(),
            ));
        }
        let (flow, task) = self.task_snapshot(task_id)?;
        if flow.demand.requester != principal.name && flow.demand.requester != principal.id {
            return Err(MambaError::PermissionDenied(format!(
                "only demand requester {} can complete task {}",
                flow.demand.requester, task.id
            )));
        }
        ensure_status(&task, &[TaskStatus::Submitted])?;
        let at = Utc::now();
        let mut events = vec![DomainEvent::TaskCompleted {
            flow_id: flow.id.clone(),
            task_id: task.id.clone(),
            completed_by: actor.to_string(),
            at,
        }];
        if flow
            .tasks
            .iter()
            .filter(|candidate| candidate.id != task.id)
            .all(|candidate| candidate.status == TaskStatus::Completed)
        {
            events.push(DomainEvent::FlowCompleted {
                flow_id: flow.id,
                completed_by: actor.to_string(),
                at,
            });
        }
        self.commit(actor, events)?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub async fn run_task(
        &mut self,
        task_id: &str,
        requested_by: &str,
        executor_principal: Option<&str>,
        mode: ExecutorMode,
        timeout_seconds: u64,
    ) -> Result<ExecutionRecord> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(
            &task,
            &[
                TaskStatus::Accepted,
                TaskStatus::InProgress,
                TaskStatus::Blocked,
            ],
        )?;
        self.ensure_dependencies_complete(&flow, &task)?;
        self.ensure_task_actor(&task, requested_by)?;
        if mode == ExecutorMode::Execute
            && self.state.principal(requested_by)?.kind != PrincipalKind::Human
        {
            return Err(MambaError::Validation(
                "execute mode requires an assigned human to authorize takeoff".into(),
            ));
        }
        let executor = self.resolve_executor(&task, executor_principal)?.clone();
        let config = executor.executor.clone().ok_or_else(|| {
            MambaError::Validation(format!("{} has no terminal executor", executor.name))
        })?;
        let execution_id = new_id("RUN");
        let log_path = self
            .data_dir
            .join("runs")
            .join(&flow.id)
            .join(format!("{execution_id}.json"));
        let started_at = Utc::now();
        let mut takeoff_events = Vec::new();
        if mode == ExecutorMode::Execute && task.status != TaskStatus::InProgress {
            takeoff_events.push(DomainEvent::TaskStarted {
                flow_id: flow.id.clone(),
                task_id: task.id.clone(),
                started_by: requested_by.to_string(),
                started_at,
            });
        }
        takeoff_events.push(DomainEvent::ExecutorStarted {
            flow_id: flow.id.clone(),
            task_id: task.id.clone(),
            execution_id: execution_id.clone(),
            principal_id: executor.id.clone(),
            executor: config.kind.to_string(),
            mode: format!("{:?}", mode).to_lowercase(),
            at: started_at,
        });
        self.commit(requested_by, takeoff_events)?;

        let prompt = task_prompt(&flow, &task, &mode, requested_by);
        let result = TerminalExecutor::run(ExecutionRequest {
            kind: config.kind.clone(),
            command: config.command,
            workspace: config.workspace.clone(),
            model: config.model,
            mode: mode.clone(),
            prompt,
            output_schema: None,
            timeout_seconds,
            log_path: log_path.clone(),
        })
        .await;
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                let mut events = vec![DomainEvent::ExecutorFailed {
                    flow_id: flow.id.clone(),
                    task_id: task.id.clone(),
                    execution_id,
                    reason: error.to_string(),
                    log_path: Some(log_path.display().to_string()),
                    at: Utc::now(),
                }];
                if mode == ExecutorMode::Execute {
                    events.push(DomainEvent::TaskBlocked {
                        flow_id: flow.id,
                        task_id: task.id,
                        actor: requested_by.to_string(),
                        reason: format!("executor crashed: {error}"),
                        at: Utc::now(),
                    });
                }
                self.commit(requested_by, events)?;
                return Err(error);
            }
        };

        let record = ExecutionRecord {
            id: execution_id,
            flow_id: flow.id.clone(),
            task_id: task.id.clone(),
            executor: config.kind,
            mode: mode.clone(),
            principal_id: executor.id,
            workspace: config.workspace,
            log_path: log_path.clone(),
            session_id: output.session_id,
            cost_usd: output.cost_usd,
            summary: output.summary.clone(),
            started_at,
            finished_at: Utc::now(),
        };
        let evidence = Evidence {
            id: new_id("EVD"),
            kind: match mode {
                ExecutorMode::Plan => "executor-plan",
                ExecutorMode::Execute => "executor-run",
            }
            .into(),
            uri: log_path.display().to_string(),
            summary: output.summary,
            created_by: executor.name,
            created_at: record.finished_at,
        };
        self.commit(
            requested_by,
            vec![
                DomainEvent::ExecutorFinished {
                    record: record.clone(),
                },
                DomainEvent::EvidenceAdded {
                    flow_id: flow.id,
                    task_id: task.id,
                    evidence,
                },
            ],
        )?;
        Ok(record)
    }

    pub fn timeline(&self, flow_id: &str) -> Result<Vec<EventEnvelope>> {
        self.state.flow(flow_id)?;
        self.store.load_flow(flow_id)
    }

    fn resolve_executor(&self, task: &Task, requested: Option<&str>) -> Result<&Principal> {
        let assignment = task
            .assignment
            .as_ref()
            .ok_or_else(|| MambaError::NoEligibleAssignee(task.title.clone()))?;
        let allowed = |principal: &&Principal| {
            principal.executor.is_some()
                && (assignment.owner.id == principal.id
                    || assignment
                        .copilots
                        .iter()
                        .any(|copilot| copilot.id == principal.id)
                    || principal.owner_id.as_deref() == Some(assignment.owner.id.as_str()))
        };
        if let Some(value) = requested {
            let principal = self.state.principal(value)?;
            if allowed(&principal) {
                return Ok(principal);
            }
            return Err(MambaError::Validation(format!(
                "executor {} is not assigned to task {}",
                principal.name, task.id
            )));
        }
        self.state
            .principals
            .values()
            .filter(allowed)
            .min_by(|left, right| left.name.cmp(&right.name))
            .ok_or_else(|| {
                MambaError::Validation(format!(
                    "task {} has no assigned Claude Code or Codex terminal",
                    task.id
                ))
            })
    }

    fn ensure_task_actor(&self, task: &Task, actor: &str) -> Result<()> {
        let principal = self.state.principal(actor)?;
        let assignment = task
            .assignment
            .as_ref()
            .ok_or_else(|| MambaError::NoEligibleAssignee(task.title.clone()))?;
        let allowed = assignment.owner.id == principal.id
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
                == Some(principal.id.as_str());
        if allowed {
            Ok(())
        } else {
            Err(MambaError::PermissionDenied(format!(
                "{} is not assigned to task {}",
                principal.name, task.id
            )))
        }
    }

    fn ensure_dependencies_complete(&self, flow: &Flow, task: &Task) -> Result<()> {
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

    fn task_snapshot(&self, task_id: &str) -> Result<(Flow, Task)> {
        let (flow, task) = self.state.find_task(task_id)?;
        Ok((flow.clone(), task.clone()))
    }

    fn commit(&mut self, actor: &str, events: Vec<DomainEvent>) -> Result<Vec<EventEnvelope>> {
        let organization_id = self.state.organization()?.id.clone();
        self.commit_as(&organization_id, actor, events)
    }

    fn commit_as(
        &mut self,
        organization_id: &str,
        actor: &str,
        events: Vec<DomainEvent>,
    ) -> Result<Vec<EventEnvelope>> {
        let envelopes = self.store.append_batch(organization_id, actor, &events)?;
        for envelope in &envelopes {
            self.state.apply(envelope)?;
        }
        Ok(envelopes)
    }
}

fn ensure_status(task: &Task, expected: &[TaskStatus]) -> Result<()> {
    if expected.contains(&task.status) {
        Ok(())
    } else {
        Err(MambaError::InvalidTransition(format!(
            "task {} is {:?}, expected one of {:?}",
            task.id, task.status, expected
        )))
    }
}

fn hours(value: f64) -> Duration {
    Duration::milliseconds((value.max(0.0) * 3_600_000.0).round() as i64)
}

fn round_hours(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn credential_hash(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

fn task_prompt(flow: &Flow, task: &Task, mode: &ExecutorMode, requested_by: &str) -> String {
    let action = match mode {
        ExecutorMode::Plan => {
            "Inspect the workspace read-only. Return a concrete implementation plan, risks, checks and questions. Do not modify files."
        }
        ExecutorMode::Execute => {
            "Implement this task in the workspace. Keep changes scoped, run relevant checks, and report changed files, verification evidence and remaining risks."
        }
    };
    format!(
        "MambaFlow work request\n\
         Flow: {} - {}\n\
         Requested by: {}\n\
         Task: {} - {}\n\
         Description: {}\n\
         Acceptance criteria:\n- {}\n\
         Required capabilities: {}\n\n\
         {}",
        flow.id,
        flow.prd.title,
        requested_by,
        task.id,
        task.title,
        task.description,
        task.acceptance_criteria.join("\n- "),
        task.required_capabilities.join(", "),
        action
    )
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn api_credentials_authenticate_replay_and_revoke() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app.create_team("Ops", "operations", "admin").unwrap();
        let human = app
            .register_principal(
                "Leader",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        let issued = app
            .issue_api_credential(&human.id, "laptop", "admin")
            .unwrap();
        assert_eq!(
            app.authenticate_api_token(&issued.token)
                .unwrap()
                .unwrap()
                .id,
            human.id
        );
        assert!(
            !serde_json::to_string(&app.state().credentials)
                .unwrap()
                .contains(&issued.token)
        );
        drop(app);

        let mut replayed = MambaApp::open(&data_dir).unwrap();
        assert_eq!(
            replayed
                .authenticate_api_token(&issued.token)
                .unwrap()
                .unwrap()
                .id,
            human.id
        );
        replayed
            .revoke_api_credential(&issued.credential.id, "admin")
            .unwrap();
        assert!(
            replayed
                .authenticate_api_token(&issued.token)
                .unwrap()
                .is_none()
        );
        drop(replayed);

        let replayed = MambaApp::open(&data_dir).unwrap();
        assert!(
            replayed
                .authenticate_api_token(&issued.token)
                .unwrap()
                .is_none()
        );
        assert!(
            !replayed
                .state()
                .credentials
                .get(&issued.credential.id)
                .unwrap()
                .is_active()
        );
    }

    #[tokio::test]
    async fn organization_flow_replays_after_human_acceptance() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Product", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "Leader",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        let agent = app
            .register_principal(
                "Leader's Codex",
                PrincipalKind::Agent,
                Some(&team.id),
                Some(&human.id),
                "delivery",
                100,
                Some(ExecutorConfig {
                    kind: crate::domain::ExecutorKind::Codex,
                    workspace: directory.path().to_path_buf(),
                    model: None,
                    command: Some(directory.path().join("missing-codex")),
                }),
                "admin",
            )
            .unwrap();
        let error = app
            .create_demand(
                "Agent cannot own requester accountability",
                &agent.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, MambaError::PermissionDenied(_)));
        let flow = app
            .create_demand(
                "Prepare a launch brief",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        let first_task = flow.tasks[0].id.clone();
        let agent_task = flow
            .tasks
            .iter()
            .find(|task| {
                task.assignment
                    .as_ref()
                    .is_some_and(|assignment| assignment.owner.id == agent.id)
            })
            .unwrap()
            .id
            .clone();
        app.approve_flow(&flow.id, &human.name).unwrap();
        app.accept_task(&first_task, &human.name).unwrap();
        app.start_task(&first_task, &human.name).unwrap();
        app.add_evidence(
            &first_task,
            &human.name,
            "document",
            "docs/brief.md",
            "scope is documented",
        )
        .unwrap();
        app.submit_task(&first_task, &human.name).unwrap();
        app.complete_task(&first_task, &human.name).unwrap();
        app.accept_task(&agent_task, &human.name).unwrap();
        let error = app
            .run_task(&agent_task, &human.name, None, ExecutorMode::Execute, 1)
            .await
            .unwrap_err();
        assert!(matches!(error, MambaError::ExecutorUnavailable(_)));
        assert_eq!(
            app.state().find_task(&agent_task).unwrap().1.status,
            TaskStatus::Blocked
        );
        drop(app);

        let replayed = MambaApp::open(&data_dir).unwrap();
        assert_eq!(
            replayed.state().find_task(&first_task).unwrap().1.status,
            TaskStatus::Completed
        );
        assert!(replayed.timeline(&flow.id).unwrap().len() >= 10);
    }

    #[tokio::test]
    async fn tracking_scan_is_idempotent_resolves_and_replays() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Delivery", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "Leader",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare a launch brief",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        let observer = app
            .register_principal(
                "Observer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        let first_task = flow.tasks[0].id.clone();
        app.approve_flow(&flow.id, &human.name).unwrap();
        app.accept_task(&first_task, &human.name).unwrap();
        app.start_task(&first_task, &human.name).unwrap();
        app.block_task(&first_task, &human.name, "waiting for access")
            .unwrap();

        let now = Utc::now();
        let blocked = app.scan_tracking_at(now, 24, "tower").unwrap();
        assert_eq!(blocked.raised.len(), 1);
        assert_eq!(
            blocked.raised[0].kind,
            crate::domain::AttentionKind::Blocked
        );
        assert_eq!(blocked.active.len(), 1);
        assert_eq!(blocked.escalated.len(), 1);
        assert_eq!(blocked.escalated[0].recipient_id, human.id);
        let escalation_id = blocked.escalated[0].id.clone();
        let error = app
            .acknowledge_escalation(&escalation_id, &observer.name)
            .unwrap_err();
        assert!(matches!(error, MambaError::PermissionDenied(_)));
        let acknowledged = app
            .acknowledge_escalation(&escalation_id, &human.name)
            .unwrap();
        assert!(acknowledged.acknowledged_at.is_some());
        assert_eq!(app.escalation_inbox(&human.name, false).unwrap().len(), 1);

        let duplicate = app.scan_tracking_at(now, 24, "tower").unwrap();
        assert!(duplicate.raised.is_empty());
        assert!(duplicate.resolved.is_empty());
        assert_eq!(duplicate.active[0].id, blocked.active[0].id);
        assert!(duplicate.escalated.is_empty());

        app.start_task(&first_task, &human.name).unwrap();
        let cleared = app.scan_tracking_at(Utc::now(), 24, "tower").unwrap();
        assert!(cleared.raised.is_empty());
        assert_eq!(cleared.resolved.len(), 1);
        assert_eq!(cleared.resolved_escalations.len(), 1);
        assert!(cleared.active.is_empty());
        assert!(app.escalation_inbox(&human.name, false).unwrap().is_empty());

        let warning_time = Utc::now() + Duration::hours(2);
        let warnings = app
            .scan_tracking_with_policy_at(warning_time, 1, 1, "tower")
            .unwrap();
        assert!(!warnings.raised.is_empty());
        assert!(
            warnings
                .active
                .iter()
                .all(|attention| attention.severity == AttentionSeverity::Warning)
        );
        assert!(warnings.escalated.is_empty());
        let delayed = app
            .scan_tracking_with_policy_at(warning_time + Duration::hours(1), 1, 1, "tower")
            .unwrap();
        assert_eq!(delayed.escalated.len(), warnings.active.len());
        let warnings_cleared = app.scan_tracking_at(Utc::now(), 24, "tower").unwrap();
        assert_eq!(
            warnings_cleared.resolved_escalations.len(),
            delayed.escalated.len()
        );
        assert!(warnings_cleared.active.is_empty());

        let future = flow.p80_finish + Duration::hours(48);
        let at_risk = app.scan_tracking_at(future, 24, "tower").unwrap();
        assert!(
            at_risk
                .active
                .iter()
                .any(|attention| attention.kind == crate::domain::AttentionKind::StaleHeartbeat)
        );
        assert!(at_risk.active.iter().any(|attention| {
            attention.kind == crate::domain::AttentionKind::AcceptanceWaiting
        }));
        assert!(
            at_risk
                .active
                .iter()
                .any(|attention| attention.kind == crate::domain::AttentionKind::Overdue)
        );
        let mut active_ids = at_risk
            .active
            .iter()
            .map(|attention| attention.id.clone())
            .collect::<Vec<_>>();
        active_ids.sort();

        drop(app);
        let mut replayed = MambaApp::open(&data_dir).unwrap();
        let mut replayed_ids = replayed
            .state()
            .active_attentions()
            .map(|attention| attention.id.clone())
            .collect::<Vec<_>>();
        replayed_ids.sort();
        assert_eq!(replayed_ids, active_ids);
        assert!(
            replayed
                .state()
                .attentions
                .values()
                .any(
                    |attention| attention.kind == crate::domain::AttentionKind::Blocked
                        && !attention.is_active()
                )
        );

        let recovered = replayed.scan_tracking_at(Utc::now(), 24, "tower").unwrap();
        assert_eq!(recovered.resolved.len(), active_ids.len());
        assert!(recovered.active.is_empty());

        replayed
            .add_evidence(
                &first_task,
                &human.name,
                "document",
                "docs/release.md",
                "release evidence",
            )
            .unwrap();
        replayed.submit_task(&first_task, &human.name).unwrap();
        let awaiting_review = replayed
            .scan_tracking_at(Utc::now() + Duration::hours(25), 24, "tower")
            .unwrap();
        assert!(
            awaiting_review
                .active
                .iter()
                .any(|attention| { attention.kind == crate::domain::AttentionKind::ReviewWaiting })
        );
        replayed.complete_task(&first_task, &human.name).unwrap();
        let reviewed = replayed.scan_tracking_at(Utc::now(), 24, "tower").unwrap();
        assert!(reviewed.active.is_empty());
        drop(replayed);

        let replayed = MambaApp::open(&data_dir).unwrap();
        assert_eq!(replayed.state().active_attentions().count(), 0);
        assert_eq!(replayed.state().active_escalations().count(), 0);
        assert!(
            replayed
                .timeline(&flow.id)
                .unwrap()
                .iter()
                .any(|event| event.kind == "tracking.attention_resolved")
        );
        assert!(
            replayed
                .timeline(&flow.id)
                .unwrap()
                .iter()
                .any(|event| event.kind == "tracking.escalation_acknowledged")
        );
    }
}
