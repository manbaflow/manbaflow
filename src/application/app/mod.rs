use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};

use crate::dashboard::{DashboardSnapshot, build_dashboard};
use crate::domain::{
    Assignment, AssignmentTarget, DeliverableKind, Demand, Estimate, Evidence, ExecutionRecord,
    ExecutorConfig, ExecutorKind, ExecutorMode, ExternalArtifact, FailureClass, FlightLease,
    FlightLeaseStatus, FlightManifestDraft, Flow, FlowChangeImpact, FlowChangeRequest,
    FlowChangeStatus, FlowScheduleRevision, FlowStatus, Organization, OrganizationRole, Principal,
    PrincipalKind, RemoteFlightReport, RoleBinding, TargetKind, Task, TaskDraft, TaskStatus, Team,
    Tenant,
};
use crate::error::{MambaError, Result};
use crate::event::{DomainEvent, EventEnvelope};
use crate::executor::{ExecutionRequest, TerminalExecutor};
use crate::ids::{new_id, normalize_capability, parse_capabilities};
use crate::matcher::Matcher;
use crate::planner::{PlannerKind, generate_plan, generate_revision_plan};
use crate::scheduler::{reschedule, schedule};
use crate::state::OrganizationState;
use crate::store::{FlowStore, StorageHealth};

mod actions;
mod authority;
mod calendars;
mod commit;
mod credentials;
mod flights;
mod interactions;
mod messages;
mod notifications;
mod policy;
mod tracking;

use self::authority::Permission;
use self::policy::ensure_status;

pub use self::credentials::tenant_token_hint;

pub struct MambaApp {
    data_dir: PathBuf,
    store: FlowStore,
    state: OrganizationState,
}

pub(crate) struct ExternalDeliverySync {
    pub duplicate: bool,
    pub stale: bool,
    pub matched_tasks: usize,
    pub changed_tasks: usize,
}

#[cfg(unix)]
fn restrict_data_dir_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_data_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

impl MambaApp {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;
        restrict_data_dir_permissions(&data_dir)?;
        let store = FlowStore::sqlite(data_dir.join("flow.db"))?;
        Self::open_with_store(data_dir, store)
    }

    pub fn open_postgres(
        data_dir: impl AsRef<Path>,
        database_url: &str,
        tenant_id: &str,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;
        restrict_data_dir_permissions(&data_dir)?;
        let store = FlowStore::postgres(database_url, tenant_id)?;
        Self::open_with_store(data_dir, store)
    }

    fn open_with_store(data_dir: PathBuf, store: FlowStore) -> Result<Self> {
        let state = OrganizationState::replay(&store.load_all()?)?;
        if let (Some(expected), Some(actual)) = (store.tenant_id(), state.tenant.as_ref())
            && expected != actual.id
        {
            return Err(MambaError::Validation(format!(
                "PostgreSQL stream {expected} contains events for tenant {}",
                actual.id
            )));
        }
        let mut app = Self {
            data_dir,
            store,
            state,
        };
        if let Err(MambaError::ConcurrentModification { .. }) = app.migrate_legacy_authority() {
            app.migrate_legacy_authority()?;
        }
        Ok(app)
    }

    pub fn state(&self) -> &OrganizationState {
        &self.state
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn storage_health(&self) -> Result<StorageHealth> {
        self.store.health()
    }

    pub fn uses_shared_storage(&self) -> bool {
        self.store.is_shared()
    }

    pub fn backup_storage(&mut self, destination: impl AsRef<Path>) -> Result<PathBuf> {
        self.store.backup(destination)
    }

    pub fn reload(&mut self) -> Result<()> {
        self.state = OrganizationState::replay(&self.store.load_all()?)?;
        Ok(())
    }

    pub fn refresh_shared_state(&mut self) -> Result<()> {
        if self.store.is_shared() {
            let events = self.store.load_after(self.state.last_sequence)?;
            if !events.is_empty() {
                let mut projected = self.state.clone();
                for event in &events {
                    projected.apply(event)?;
                }
                self.state = projected;
            }
        }
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
        if self.state.tenant.is_some() {
            return Err(MambaError::TenantAlreadyInitialized);
        }
        let now = Utc::now();
        let tenant = Tenant {
            id: self
                .store
                .tenant_id()
                .map(str::to_string)
                .unwrap_or_else(|| new_id("TEN")),
            name: name.trim().to_string(),
            created_at: now,
        };
        let organization = Organization {
            id: new_id("ORG"),
            name: name.trim().to_string(),
            created_at: now,
        };
        self.commit_as(
            &organization.id,
            actor,
            vec![
                DomainEvent::TenantInitialized { tenant },
                DomainEvent::OrganizationInitialized {
                    organization: organization.clone(),
                },
            ],
        )?;
        Ok(organization)
    }

    pub fn create_team(&mut self, name: &str, capabilities: &str, actor: &str) -> Result<Team> {
        self.state.organization()?;
        self.ensure_permission(actor, Permission::OrganizationManage)?;
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
        self.ensure_permission(actor, Permission::PrincipalManage)?;
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
        let default_role = match &kind {
            PrincipalKind::Human
                if !self
                    .state
                    .principals
                    .values()
                    .any(|principal| principal.kind == PrincipalKind::Human) =>
            {
                OrganizationRole::TenantAdmin
            }
            PrincipalKind::Human => OrganizationRole::Member,
            PrincipalKind::Agent => OrganizationRole::Agent,
        };
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
        let binding = RoleBinding {
            id: new_id("ROLE"),
            tenant_id: self.state.tenant()?.id.clone(),
            organization_id: self.state.organization()?.id.clone(),
            principal_id: principal.id.clone(),
            role: default_role,
            granted_by: actor.to_string(),
            granted_at: principal.created_at,
            revoked_by: None,
            revoked_at: None,
        };
        self.commit(
            actor,
            vec![
                DomainEvent::PrincipalRegistered {
                    principal: principal.clone(),
                },
                DomainEvent::RoleGranted { binding },
            ],
        )?;
        Ok(principal)
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
        self.ensure_permission(&requester.id, Permission::DemandCreate)?;
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

    pub fn negotiate_task(
        &mut self,
        task_id: &str,
        actor: &str,
        effort_hours: f64,
    ) -> Result<Task> {
        if !effort_hours.is_finite() || effort_hours <= 0.0 || effort_hours > 100_000.0 {
            return Err(MambaError::Validation(
                "estimate must be greater than zero and at most 100000 hours".into(),
            ));
        }
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(
            &task,
            &[
                TaskStatus::Assigned,
                TaskStatus::Accepted,
                TaskStatus::InProgress,
                TaskStatus::Blocked,
            ],
        )?;
        self.ensure_task_actor(&task, actor)?;
        let now = Utc::now();
        let mut updated_flow = flow.clone();
        updated_flow
            .task_mut(&task.id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "task",
                id: task.id.clone(),
            })?
            .estimate
            .effort_hours = effort_hours;
        let scheduled = reschedule(&updated_flow, &self.state, now)?;
        let estimate = scheduled.task_estimates[&task.id].clone();
        let revision = FlowScheduleRevision {
            task_estimates: scheduled.task_estimates,
            p50_finish: scheduled.p50_finish,
            p80_finish: scheduled.p80_finish,
            critical_path: scheduled.critical_path,
            reason: format!("{actor} negotiated {} to {effort_hours:.1}h", task.id),
            revised_by: actor.to_string(),
            revised_at: now,
        };
        self.commit(
            actor,
            vec![
                DomainEvent::TaskEstimateNegotiated {
                    flow_id: flow.id.clone(),
                    task_id: task.id.clone(),
                    negotiated_by: actor.to_string(),
                    estimate,
                },
                DomainEvent::FlowRescheduled {
                    flow_id: flow.id,
                    revision,
                },
            ],
        )?;
        Ok(self.state.find_task(&task.id)?.1.clone())
    }

    pub fn reassignment_candidates(
        &self,
        task_id: &str,
        actor: &str,
    ) -> Result<Vec<AssignmentTarget>> {
        let principal = self.state.principal(actor)?;
        if principal.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "task reassignment requires a human requester".into(),
            ));
        }
        let (flow, task) = self.state.find_task(task_id)?;
        if !matches!(flow.status, FlowStatus::Approved | FlowStatus::Active) {
            return Err(MambaError::InvalidTransition(format!(
                "flow {} is {:?}; only an approved or active flow can be reassigned",
                flow.id, flow.status
            )));
        }
        if flow.demand.requester != principal.id && flow.demand.requester != principal.name {
            return Err(MambaError::PermissionDenied(format!(
                "only demand requester {} can reassign task {}",
                flow.demand.requester, task.id
            )));
        }
        ensure_status(
            task,
            &[
                TaskStatus::Assigned,
                TaskStatus::Accepted,
                TaskStatus::InProgress,
                TaskStatus::Blocked,
                TaskStatus::Rejected,
            ],
        )?;
        let current_owner = task
            .assignment
            .as_ref()
            .map(|assignment| &assignment.owner.id);
        let mut candidates = self
            .state
            .principals
            .values()
            .filter(|candidate| candidate.active)
            .filter(|candidate| !task.requires_human || candidate.kind == PrincipalKind::Human)
            .filter(|candidate| {
                capabilities_cover(&task.required_capabilities, &candidate.capabilities)
            })
            .map(target_for_principal)
            .chain(
                self.state
                    .teams
                    .values()
                    .filter(|team| team.active)
                    .filter(|team| {
                        capabilities_cover(&task.required_capabilities, &team.capabilities)
                    })
                    .filter(|team| {
                        !task.requires_human
                            || self.state.principals.values().any(|candidate| {
                                candidate.active
                                    && candidate.kind == PrincipalKind::Human
                                    && candidate.team_id.as_deref() == Some(team.id.as_str())
                            })
                    })
                    .map(|team| AssignmentTarget {
                        kind: TargetKind::Team,
                        id: team.id.clone(),
                        name: team.name.clone(),
                    }),
            )
            .filter(|target| current_owner != Some(&target.id))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            target_kind_rank(&left.kind)
                .cmp(&target_kind_rank(&right.kind))
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(candidates)
    }

    pub fn reassign_task(
        &mut self,
        task_id: &str,
        actor: &str,
        new_owner: &str,
        copilots: &[String],
        reason: &str,
    ) -> Result<Flow> {
        let reason = reason.trim();
        if reason.is_empty() || reason.chars().count() > 1_000 {
            return Err(MambaError::Validation(
                "reassignment reason must contain 1 to 1000 characters".into(),
            ));
        }
        let candidates = self.reassignment_candidates(task_id, actor)?;
        let owner = candidates
            .into_iter()
            .find(|candidate| {
                candidate.id == new_owner || candidate.name.eq_ignore_ascii_case(new_owner)
            })
            .ok_or_else(|| {
                MambaError::Validation(format!(
                    "{new_owner} is not an eligible reassignment target for task {task_id}"
                ))
            })?;
        let (flow, task) = self.task_snapshot(task_id)?;
        let now = Utc::now();
        if self.state.flight_leases.values().any(|lease| {
            lease.task_id == task.id
                && (lease.status == FlightLeaseStatus::Active || lease.is_claimable_at(now))
        }) {
            return Err(MambaError::InvalidTransition(format!(
                "task {} has an open flight lease; revoke or finish it before reassignment",
                task.id
            )));
        }
        let mut resolved_copilots = Vec::new();
        let mut seen = BTreeSet::new();
        for value in copilots {
            let target = self.resolve_active_target(value)?;
            if target.id == owner.id {
                return Err(MambaError::Validation(
                    "task owner cannot also be a copilot".into(),
                ));
            }
            if seen.insert(target.id.clone()) {
                resolved_copilots.push(target);
            }
        }
        if resolved_copilots.is_empty() {
            resolved_copilots = self.default_copilots_for(&owner);
        }
        let assignment = Assignment {
            owner: owner.clone(),
            copilots: resolved_copilots,
            score: 100.0,
            rationale: vec![
                format!("manual reassignment by {actor}"),
                format!("reason: {reason}"),
            ],
        };
        let mut updated_flow = flow.clone();
        let updated_task = updated_flow
            .task_mut(&task.id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "task",
                id: task.id.clone(),
            })?;
        updated_task.assignment = Some(assignment.clone());
        updated_task.status = TaskStatus::Assigned;
        updated_task.blocker = None;
        updated_task.last_heartbeat = None;
        let scheduled = reschedule(&updated_flow, &self.state, now)?;
        let revision = FlowScheduleRevision {
            task_estimates: scheduled.task_estimates,
            p50_finish: scheduled.p50_finish,
            p80_finish: scheduled.p80_finish,
            critical_path: scheduled.critical_path,
            reason: format!("{} reassigned to {}: {reason}", task.id, owner.name),
            revised_by: actor.to_string(),
            revised_at: now,
        };
        self.commit(
            actor,
            vec![
                DomainEvent::TaskReassigned {
                    flow_id: flow.id.clone(),
                    task_id: task.id.clone(),
                    previous_assignment: task.assignment,
                    assignment,
                    reassigned_by: actor.to_string(),
                    reason: reason.to_string(),
                    at: now,
                },
                DomainEvent::WorkRequestSent {
                    flow_id: flow.id.clone(),
                    task_id: task.id,
                    target_id: owner.id,
                },
                DomainEvent::FlowRescheduled {
                    flow_id: flow.id.clone(),
                    revision,
                },
            ],
        )?;
        Ok(self.state.flow(&flow.id)?.clone())
    }

    pub async fn propose_flow_change(
        &mut self,
        flow_id: &str,
        actor: &str,
        summary: &str,
        planner: PlannerKind,
        workspace: &Path,
        timeout_seconds: u64,
    ) -> Result<FlowChangeRequest> {
        let summary = summary.trim();
        if summary.is_empty() || summary.chars().count() > 4_000 {
            return Err(MambaError::Validation(
                "flow change summary must contain 1 to 4000 characters".into(),
            ));
        }
        if !workspace.is_dir() {
            return Err(MambaError::InvalidWorkspace(workspace.to_path_buf()));
        }
        if self.state.flow_changes.values().any(|request| {
            request.flow_id == flow_id && request.status == FlowChangeStatus::Proposed
        }) {
            return Err(MambaError::InvalidTransition(format!(
                "flow {flow_id} already has a proposed change awaiting a decision"
            )));
        }
        let requester = self.state.principal(actor)?.clone();
        if requester.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "flow change requests require a human requester".into(),
            ));
        }
        let flow = self.state.flow(flow_id)?.clone();
        if flow.demand.requester != requester.id && flow.demand.requester != requester.name {
            return Err(MambaError::PermissionDenied(format!(
                "only demand requester {} can revise flow {}",
                flow.demand.requester, flow.id
            )));
        }
        if !matches!(flow.status, FlowStatus::Approved | FlowStatus::Active) {
            return Err(MambaError::InvalidTransition(format!(
                "flow {} is {:?}; only approved or active flows can be revised",
                flow.id, flow.status
            )));
        }

        let request_id = new_id("CHG");
        let planner_log = self
            .data_dir
            .join("runs")
            .join(&flow.id)
            .join(format!("{request_id}-planner.json"));
        let plan = generate_revision_plan(
            planner,
            &flow,
            summary,
            &self.state,
            workspace,
            planner_log,
            timeout_seconds,
        )
        .await?;
        let additions = validate_append_only_revision(&flow, &plan.tasks)?;
        if additions.len() > 20 {
            return Err(MambaError::Validation(
                "one flow change can append at most 20 tasks".into(),
            ));
        }

        let mut matcher = Matcher::new(&self.state);
        let mut assignments = BTreeMap::new();
        for draft in &additions {
            assignments.insert(draft.key.clone(), matcher.match_task(draft)?);
        }
        let now = Utc::now();
        let mut ids = flow
            .tasks
            .iter()
            .map(|task| (task.key.clone(), task.id.clone()))
            .collect::<BTreeMap<_, _>>();
        for draft in &additions {
            ids.insert(draft.key.clone(), new_id("TSK"));
        }
        let mut new_tasks = additions
            .iter()
            .map(|draft| {
                let assignment = assignments
                    .get(&draft.key)
                    .cloned()
                    .ok_or_else(|| MambaError::NoEligibleAssignee(draft.title.clone()))?;
                let depends_on = draft
                    .depends_on
                    .iter()
                    .map(|dependency| {
                        ids.get(dependency).cloned().ok_or_else(|| {
                            MambaError::Validation(format!(
                                "new task {} depends on unknown task {dependency}",
                                draft.key
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Task {
                    id: ids[&draft.key].clone(),
                    key: draft.key.clone(),
                    title: draft.title.clone(),
                    description: draft.description.clone(),
                    required_capabilities: draft.required_capabilities.clone(),
                    depends_on,
                    requires_human: draft.requires_human,
                    acceptance_criteria: draft.acceptance_criteria.clone(),
                    assignment: Some(assignment),
                    estimate: Estimate {
                        effort_hours: draft.effort_hours,
                        p50_hours: draft.effort_hours,
                        p80_hours: draft.effort_hours * 1.4,
                        confidence: "preview".into(),
                        rationale: vec!["flow change preview".into()],
                        earliest_start: now,
                        p50_finish: now,
                        p80_finish: now,
                    },
                    status: TaskStatus::Proposed,
                    blocker: None,
                    last_heartbeat: None,
                    evidence: Vec::new(),
                    external_artifacts: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut preview_flow = flow.clone();
        preview_flow.tasks.extend(new_tasks.clone());
        let baseline = reschedule(&flow, &self.state, now)?;
        let scheduled = reschedule(&preview_flow, &self.state, now)?;
        for task in &mut new_tasks {
            task.estimate = scheduled.task_estimates[&task.id].clone();
        }
        let delta_millis = scheduled
            .p80_finish
            .signed_duration_since(baseline.p80_finish)
            .num_milliseconds();
        let delta_hours = delta_millis as f64 / 3_600_000.0;
        let net_delta_millis = scheduled
            .p80_finish
            .signed_duration_since(flow.p80_finish)
            .num_milliseconds();
        let net_delta_hours = net_delta_millis as f64 / 3_600_000.0;
        let rebase_delta_hours = baseline
            .p80_finish
            .signed_duration_since(flow.p80_finish)
            .num_milliseconds() as f64
            / 3_600_000.0;
        let mut risks = Vec::new();
        if flow
            .tasks
            .iter()
            .any(|task| matches!(task.status, TaskStatus::InProgress | TaskStatus::Blocked))
        {
            risks.push("当前 Flow 已有执行中或阻塞任务，追加范围可能增加协调成本".into());
        }
        if new_tasks.iter().any(|task| task.requires_human) {
            risks.push("新增工作引入了 Human 审批或责任 Gate".into());
        }
        if delta_hours > 0.0 {
            risks.push(format!("新增范围使重算基线 P80 延后 {delta_hours:.1} 小时"));
        }
        if rebase_delta_hours.abs() >= 0.1 {
            risks.push(format!(
                "当前进度先使正式 P80 重基线 {rebase_delta_hours:+.1} 小时，再叠加新增范围"
            ));
        }
        let revision = FlowScheduleRevision {
            task_estimates: scheduled.task_estimates,
            p50_finish: scheduled.p50_finish,
            p80_finish: scheduled.p80_finish,
            critical_path: scheduled.critical_path,
            reason: format!("preview change {request_id}: {summary}"),
            revised_by: requester.name.clone(),
            revised_at: now,
        };
        let impact = FlowChangeImpact {
            added_task_ids: new_tasks.iter().map(|task| task.id.clone()).collect(),
            added_task_titles: new_tasks.iter().map(|task| task.title.clone()).collect(),
            affected_task_ids: new_tasks.iter().map(|task| task.id.clone()).collect(),
            official_p80_finish: flow.p80_finish,
            baseline_p80_finish: baseline.p80_finish,
            proposed_p80_finish: revision.p80_finish,
            baseline_p80_delta_hours: (rebase_delta_hours * 10.0).round() / 10.0,
            scope_p80_delta_hours: (delta_hours * 10.0).round() / 10.0,
            net_p80_delta_hours: (net_delta_hours * 10.0).round() / 10.0,
            risks,
        };
        let request = FlowChangeRequest {
            id: request_id,
            flow_id: flow.id,
            summary: summary.to_string(),
            requested_by_id: requester.id,
            requested_by_name: requester.name.clone(),
            planner: planner.to_string(),
            proposed_prd: plan.prd,
            new_tasks,
            preview_schedule: revision,
            base_task_statuses: flow
                .tasks
                .iter()
                .map(|task| (task.id.clone(), task.status.clone()))
                .collect(),
            base_p80_finish: flow.p80_finish,
            impact,
            status: FlowChangeStatus::Proposed,
            created_at: now,
            resolved_at: None,
            resolved_by: None,
            rejection_reason: None,
        };
        self.commit(
            &requester.name,
            vec![DomainEvent::FlowChangeProposed {
                request: Box::new(request.clone()),
            }],
        )?;
        Ok(request)
    }

    pub fn flow_changes(&self, flow_id: &str, actor: &str) -> Result<Vec<FlowChangeRequest>> {
        let flow = self.state.flow(flow_id)?;
        let principal = self.state.principal(actor)?;
        if !self.principal_has_flow_access(flow, principal) {
            return Err(MambaError::PermissionDenied(format!(
                "{} cannot access flow {}",
                principal.name, flow.id
            )));
        }
        let mut changes = self
            .state
            .flow_changes
            .values()
            .filter(|request| request.flow_id == flow.id)
            .cloned()
            .collect::<Vec<_>>();
        changes.sort_by_key(|request| std::cmp::Reverse(request.created_at));
        Ok(changes)
    }

    pub fn pending_flow_change(&self, flow_id: &str) -> Option<&FlowChangeRequest> {
        self.state.flow_changes.values().find(|request| {
            request.flow_id == flow_id && request.status == FlowChangeStatus::Proposed
        })
    }

    pub fn approve_flow_change(
        &mut self,
        request_id: &str,
        actor: &str,
    ) -> Result<FlowChangeRequest> {
        let principal = self.state.principal(actor)?.clone();
        let request = self
            .state
            .flow_changes
            .get(request_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "flow change request",
                id: request_id.to_string(),
            })?;
        let flow = self.state.flow(&request.flow_id)?.clone();
        if principal.kind != PrincipalKind::Human
            || (flow.demand.requester != principal.id && flow.demand.requester != principal.name)
        {
            return Err(MambaError::PermissionDenied(format!(
                "only demand requester {} can approve flow change {}",
                flow.demand.requester, request.id
            )));
        }
        if request.status != FlowChangeStatus::Proposed {
            return Err(MambaError::InvalidTransition(format!(
                "flow change {} is {:?}",
                request.id, request.status
            )));
        }
        let current_statuses = flow
            .tasks
            .iter()
            .map(|task| (task.id.clone(), task.status.clone()))
            .collect::<BTreeMap<_, _>>();
        if current_statuses != request.base_task_statuses
            || flow.p80_finish != request.base_p80_finish
        {
            return Err(MambaError::InvalidTransition(
                "flow changed after this preview; generate a fresh change request".into(),
            ));
        }
        let now = Utc::now();
        let mut new_tasks = request.new_tasks.clone();
        for task in &mut new_tasks {
            task.status = TaskStatus::Assigned;
        }
        let mut updated_flow = flow.clone();
        updated_flow.prd = request.proposed_prd.clone();
        updated_flow.tasks.extend(new_tasks.clone());
        let scheduled = reschedule(&updated_flow, &self.state, now)?;
        let revision = FlowScheduleRevision {
            task_estimates: scheduled.task_estimates,
            p50_finish: scheduled.p50_finish,
            p80_finish: scheduled.p80_finish,
            critical_path: scheduled.critical_path,
            reason: format!("approved flow change {}: {}", request.id, request.summary),
            revised_by: principal.name.clone(),
            revised_at: now,
        };
        let mut events = vec![DomainEvent::FlowChangeApplied {
            flow_id: flow.id.clone(),
            request_id: request.id.clone(),
            prd: request.proposed_prd,
            new_tasks: new_tasks.clone(),
            revision,
            applied_by: principal.name.clone(),
            applied_at: now,
        }];
        events.extend(new_tasks.iter().map(|task| DomainEvent::WorkRequestSent {
            flow_id: flow.id.clone(),
            task_id: task.id.clone(),
            target_id: task.assignment.as_ref().unwrap().owner.id.clone(),
        }));
        self.commit(&principal.name, events)?;
        Ok(self.state.flow_changes[request_id].clone())
    }

    pub fn reject_flow_change(
        &mut self,
        request_id: &str,
        actor: &str,
        reason: &str,
    ) -> Result<FlowChangeRequest> {
        let reason = reason.trim();
        if reason.is_empty() || reason.chars().count() > 1_000 {
            return Err(MambaError::Validation(
                "flow change rejection reason must contain 1 to 1000 characters".into(),
            ));
        }
        let principal = self.state.principal(actor)?.clone();
        let request = self
            .state
            .flow_changes
            .get(request_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "flow change request",
                id: request_id.to_string(),
            })?;
        let flow = self.state.flow(&request.flow_id)?;
        if principal.kind != PrincipalKind::Human
            || (flow.demand.requester != principal.id && flow.demand.requester != principal.name)
        {
            return Err(MambaError::PermissionDenied(format!(
                "only demand requester {} can reject flow change {}",
                flow.demand.requester, request.id
            )));
        }
        if request.status != FlowChangeStatus::Proposed {
            return Err(MambaError::InvalidTransition(format!(
                "flow change {} is {:?}",
                request.id, request.status
            )));
        }
        self.commit(
            &principal.name,
            vec![DomainEvent::FlowChangeRejected {
                flow_id: flow.id.clone(),
                request_id: request.id.clone(),
                rejected_by: principal.name.clone(),
                reason: reason.to_string(),
                rejected_at: Utc::now(),
            }],
        )?;
        Ok(self.state.flow_changes[request_id].clone())
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

    pub fn sync_external_artifacts(
        &mut self,
        task_id: &str,
        actor: &str,
        artifacts: Vec<ExternalArtifact>,
    ) -> Result<Vec<ExternalArtifact>> {
        let (flow, task) = self.task_snapshot(task_id)?;
        self.ensure_task_actor(&task, actor)?;
        let incoming = artifacts
            .into_iter()
            .map(|artifact| (artifact.id.clone(), artifact))
            .collect::<BTreeMap<_, _>>();
        let mut changed = Vec::new();
        for artifact in incoming.into_values() {
            validate_external_artifact(&artifact)?;
            if task
                .external_artifacts
                .iter()
                .find(|existing| existing.id == artifact.id)
                .is_some_and(|existing| {
                    existing.same_snapshot(&artifact) || existing.synced_at > artifact.synced_at
                })
            {
                continue;
            }
            changed.push(artifact);
        }
        if changed.is_empty() {
            return Ok(changed);
        }
        let events = changed
            .iter()
            .cloned()
            .map(|artifact| DomainEvent::ExternalArtifactSynced {
                flow_id: flow.id.clone(),
                task_id: task.id.clone(),
                artifact,
            })
            .collect();
        self.commit(actor, events)?;
        Ok(changed)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sync_bound_external_artifact(
        &mut self,
        provider: &str,
        delivery_id: &str,
        binding_kind: &str,
        binding_project: &str,
        binding_external_id: &str,
        occurred_at: DateTime<Utc>,
        artifact: ExternalArtifact,
        actor: &str,
    ) -> Result<ExternalDeliverySync> {
        validate_external_artifact(&artifact)?;
        let delivery_key = format!("{provider}:{delivery_id}");
        if self.state.external_deliveries.contains_key(&delivery_key) {
            return Ok(ExternalDeliverySync {
                duplicate: true,
                stale: false,
                matched_tasks: 0,
                changed_tasks: 0,
            });
        }
        let binding_key =
            format!("{provider}:{binding_kind}:{binding_project}:{binding_external_id}");
        let matches = self
            .state
            .flows
            .values()
            .flat_map(|flow| {
                flow.tasks.iter().filter_map(|task| {
                    task.external_artifacts
                        .iter()
                        .find(|candidate| {
                            candidate.provider == provider
                                && candidate.kind == binding_kind
                                && candidate.project == binding_project
                                && candidate.external_id == binding_external_id
                        })
                        .map(|binding| (flow.id.clone(), task.id.clone(), binding.synced_at))
                })
            })
            .collect::<Vec<_>>();
        let mut stale = self
            .state
            .external_binding_clocks
            .get(&binding_key)
            .is_some_and(|current| *current > occurred_at);
        let mut artifact_events = Vec::new();
        if !stale {
            for (flow_id, task_id, binding_synced_at) in &matches {
                if *binding_synced_at > occurred_at {
                    stale = true;
                    continue;
                }
                let task = self.state.flow(flow_id)?.task(task_id).ok_or_else(|| {
                    MambaError::NotFound {
                        entity: "task",
                        id: task_id.clone(),
                    }
                })?;
                let mut task_artifact = artifact.clone();
                if let Some(existing) = task
                    .external_artifacts
                    .iter()
                    .find(|existing| existing.id == task_artifact.id)
                {
                    if task_artifact.revision.is_none() {
                        task_artifact.revision = existing.revision.clone();
                    }
                    if existing.same_snapshot(&task_artifact)
                        || existing.synced_at > task_artifact.synced_at
                    {
                        continue;
                    }
                }
                artifact_events.push(DomainEvent::ExternalArtifactSynced {
                    flow_id: flow_id.clone(),
                    task_id: task_id.clone(),
                    artifact: task_artifact,
                });
            }
        }
        let changed_tasks = artifact_events.len();
        artifact_events.push(DomainEvent::ExternalDeliveryProcessed {
            provider: provider.to_string(),
            delivery_id: delivery_id.to_string(),
            binding_key,
            occurred_at,
            processed_at: Utc::now(),
        });
        self.commit(actor, artifact_events)?;
        Ok(ExternalDeliverySync {
            duplicate: false,
            stale,
            matched_tasks: matches.len(),
            changed_tasks,
        })
    }

    pub fn authorize_task_actor(&self, task_id: &str, actor: &str) -> Result<()> {
        let (_, task) = self.state.find_task(task_id)?;
        self.ensure_task_actor(task, actor)
    }

    pub fn admin_dashboard(&self, actor: &str) -> Result<DashboardSnapshot> {
        let principal = self.state.principal(actor)?;
        if principal.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "organization dashboard requires a human identity".into(),
            ));
        }
        self.ensure_permission(&principal.id, Permission::DashboardRead)?;
        Ok(build_dashboard(&self.state))
    }

    pub fn authorize_remote_flight(
        &mut self,
        task_id: &str,
        authorized_by: &str,
        worker: &str,
        executor: ExecutorKind,
        ttl_seconds: u64,
    ) -> Result<FlightLease> {
        self.authorize_remote_flight_with_manifest(
            task_id,
            authorized_by,
            worker,
            executor,
            ttl_seconds,
            FlightManifestDraft::default(),
        )
    }

    pub fn authorize_remote_flight_with_manifest(
        &mut self,
        task_id: &str,
        authorized_by: &str,
        worker: &str,
        executor: ExecutorKind,
        ttl_seconds: u64,
        manifest: FlightManifestDraft,
    ) -> Result<FlightLease> {
        self.expire_remote_flights("tower://lease-reaper")?;
        if !(60..=86_400).contains(&ttl_seconds) {
            return Err(MambaError::Validation(
                "flight lease TTL must be between 60 and 86400 seconds".into(),
            ));
        }
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
        self.ensure_task_actor(&task, authorized_by)?;
        let human = self.state.principal(authorized_by)?.clone();
        if human.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "remote write authorization requires a human".into(),
            ));
        }
        let worker = self.state.principal(worker)?.clone();
        if worker.kind != PrincipalKind::Agent {
            return Err(MambaError::Validation(
                "a remote flight lease can only target an agent".into(),
            ));
        }
        if worker.owner_id.as_deref() != Some(human.id.as_str()) {
            return Err(MambaError::PermissionDenied(format!(
                "{} can only authorize a personal agent they own",
                human.name
            )));
        }
        self.ensure_task_actor(&task, &worker.name)?;
        let now = Utc::now();
        if self.state.flight_leases.values().any(|lease| {
            lease.task_id == task.id
                && lease.principal_id == worker.id
                && (lease.status == FlightLeaseStatus::Active || lease.is_claimable_at(now))
        }) {
            return Err(MambaError::InvalidTransition(format!(
                "task {} already has an open flight lease for {}",
                task.id, worker.name
            )));
        }
        let manifest = self.build_flight_manifest(&flow, &task, &human, ttl_seconds, manifest)?;
        self.ensure_resource_claims_available(&manifest.resources, now)?;
        let lease_id = new_id("LEASE");
        let lease = FlightLease {
            id: lease_id.clone(),
            flow_id: flow.id,
            task_id: task.id,
            principal_id: worker.id,
            principal_name: worker.name,
            authorized_by: human.name.clone(),
            executor,
            status: FlightLeaseStatus::Authorized,
            issued_at: now,
            expires_at: now + Duration::seconds(ttl_seconds as i64),
            claimed_at: None,
            finished_at: None,
            run_id: None,
            report: None,
            manifest: Some(manifest.clone()),
            parent_lease_id: None,
            root_lease_id: Some(lease_id),
            attempt: 1,
        };
        let mut events = vec![DomainEvent::RemoteFlightAuthorized {
            lease: Box::new(lease.clone()),
        }];
        events.extend(self.resource_acquisition_events(&lease, &manifest.resources));
        self.commit(&human.name, events)?;
        Ok(lease)
    }

    pub fn remote_flight_leases(
        &self,
        principal: &str,
        include_terminal: bool,
    ) -> Result<Vec<FlightLease>> {
        let principal = self.state.principal(principal)?;
        let now = Utc::now();
        let mut leases = self
            .state
            .flight_leases
            .values()
            .filter(|lease| match principal.kind {
                PrincipalKind::Agent => lease.principal_id == principal.id,
                PrincipalKind::Human => {
                    lease.authorized_by == principal.name
                        || self
                            .state
                            .principals
                            .get(&lease.principal_id)
                            .and_then(|worker| worker.owner_id.as_deref())
                            == Some(principal.id.as_str())
                        || self.state.flows.get(&lease.flow_id).is_some_and(|flow| {
                            flow.demand.requester == principal.name
                                || flow.demand.requester == principal.id
                        })
                }
            })
            .filter(|lease| {
                include_terminal
                    || lease.status == FlightLeaseStatus::Active
                    || lease.is_claimable_at(now)
            })
            .cloned()
            .collect::<Vec<_>>();
        leases.sort_by_key(|lease| std::cmp::Reverse(lease.issued_at));
        Ok(leases)
    }

    pub fn revoke_remote_flight(&mut self, lease_id: &str, actor: &str) -> Result<FlightLease> {
        let principal = self.state.principal(actor)?.clone();
        if principal.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "flight lease revocation requires a human".into(),
            ));
        }
        let lease = self
            .state
            .flight_leases
            .get(lease_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "flight lease",
                id: lease_id.to_string(),
            })?;
        if lease.authorized_by != principal.name {
            return Err(MambaError::PermissionDenied(format!(
                "only {} can revoke flight lease {}",
                lease.authorized_by, lease.id
            )));
        }
        if lease.status != FlightLeaseStatus::Authorized {
            return Err(MambaError::InvalidTransition(format!(
                "flight lease {} is {:?}; only an unclaimed lease can be revoked",
                lease.id, lease.status
            )));
        }
        let revoked_at = Utc::now();
        let mut events = vec![DomainEvent::RemoteFlightRevoked {
            flow_id: lease.flow_id.clone(),
            task_id: lease.task_id.clone(),
            lease_id: lease.id.clone(),
            revoked_by: principal.name.clone(),
            revoked_at,
        }];
        events.extend(self.resource_release_events(&lease, revoked_at, "flight revoked"));
        self.commit(&principal.name, events)?;
        Ok(self.state.flight_leases[lease_id].clone())
    }

    pub fn claim_remote_flight(
        &mut self,
        lease_id: &str,
        actor: &str,
        run_id: &str,
    ) -> Result<FlightLease> {
        validate_run_id(run_id)?;
        let principal = self.state.principal(actor)?.clone();
        if principal.kind != PrincipalKind::Agent {
            return Err(MambaError::PermissionDenied(
                "only the authorized remote agent can claim a flight lease".into(),
            ));
        }
        let lease = self
            .state
            .flight_leases
            .get(lease_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "flight lease",
                id: lease_id.to_string(),
            })?;
        if lease.principal_id != principal.id {
            return Err(MambaError::PermissionDenied(format!(
                "flight lease {} belongs to another agent",
                lease.id
            )));
        }
        let now = Utc::now();
        if !lease.is_claimable_at(now) {
            return Err(MambaError::InvalidTransition(if lease.expires_at <= now {
                format!("flight lease {} has expired", lease.id)
            } else {
                format!("flight lease {} is {:?}", lease.id, lease.status)
            }));
        }
        let (flow, task) = self.task_snapshot(&lease.task_id)?;
        ensure_status(
            &task,
            &[
                TaskStatus::Accepted,
                TaskStatus::InProgress,
                TaskStatus::Blocked,
            ],
        )?;
        self.ensure_dependencies_complete(&flow, &task)?;
        self.ensure_task_actor(&task, &principal.name)?;
        let mut events = Vec::new();
        if task.status != TaskStatus::InProgress {
            events.push(DomainEvent::TaskStarted {
                flow_id: flow.id.clone(),
                task_id: task.id.clone(),
                started_by: principal.name.clone(),
                started_at: now,
            });
        }
        events.push(DomainEvent::RemoteFlightClaimed {
            flow_id: lease.flow_id.clone(),
            task_id: lease.task_id.clone(),
            lease_id: lease.id.clone(),
            run_id: run_id.to_string(),
            claimed_at: now,
        });
        self.commit(&principal.name, events)?;
        Ok(self.state.flight_leases[lease_id].clone())
    }

    pub fn finish_remote_flight(
        &mut self,
        lease_id: &str,
        actor: &str,
        landed: bool,
        mut report: RemoteFlightReport,
    ) -> Result<FlightLease> {
        let principal = self.state.principal(actor)?.clone();
        let lease = self
            .state
            .flight_leases
            .get(lease_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "flight lease",
                id: lease_id.to_string(),
            })?;
        if lease.principal_id != principal.id {
            return Err(MambaError::PermissionDenied(format!(
                "flight lease {} belongs to another agent",
                lease.id
            )));
        }
        validate_remote_flight_report(&lease, &report)?;
        report.deliverables = self.normalize_deliverables(&lease, &report.changed_files);
        report.contract_violations = self.output_contract_violations(&lease, &report);
        report.budget_exhaustions = self.fuel_exhaustions(&lease, &report);
        let effective_landed =
            landed && report.budget_exhaustions.is_empty() && report.contract_violations.is_empty();
        if !effective_landed && report.failure_class.is_none() {
            report.failure_class = Some(if !report.budget_exhaustions.is_empty() {
                FailureClass::Budget
            } else if !report.contract_violations.is_empty() {
                FailureClass::Validation
            } else {
                FailureClass::Unknown
            });
        }
        if matches!(
            lease.status,
            FlightLeaseStatus::Landed | FlightLeaseStatus::Crashed
        ) && lease.report.as_ref() == Some(&report)
            && effective_landed == (lease.status == FlightLeaseStatus::Landed)
        {
            return Ok(lease);
        }
        if lease.status != FlightLeaseStatus::Active {
            return Err(MambaError::InvalidTransition(format!(
                "flight lease {} is {:?}, expected active",
                lease.id, lease.status
            )));
        }
        let finished_at = Utc::now();
        let evidence = Evidence {
            id: new_id("EVD"),
            kind: if effective_landed && report.patch_sha256.is_some() {
                "remote_patch"
            } else if effective_landed {
                "remote_flight"
            } else {
                "worker_blackbox"
            }
            .into(),
            uri: format!("flight://{}", lease.id),
            summary: report.summary.clone(),
            created_by: principal.name.clone(),
            created_at: finished_at,
        };
        let mut events = vec![
            DomainEvent::RemoteFlightFinished {
                flow_id: lease.flow_id.clone(),
                task_id: lease.task_id.clone(),
                lease_id: lease.id.clone(),
                landed: effective_landed,
                report: report.clone(),
                finished_at,
            },
            DomainEvent::EvidenceAdded {
                flow_id: lease.flow_id.clone(),
                task_id: lease.task_id.clone(),
                evidence,
            },
        ];
        if effective_landed {
            events.extend(report.deliverables.iter().map(|deliverable| {
                DomainEvent::EvidenceAdded {
                    flow_id: lease.flow_id.clone(),
                    task_id: lease.task_id.clone(),
                    evidence: Evidence {
                        id: new_id("EVD"),
                        kind: match deliverable.kind {
                            DeliverableKind::Code => "code",
                            DeliverableKind::Document => "document",
                            DeliverableKind::Spreadsheet => "spreadsheet",
                            DeliverableKind::Presentation => "presentation",
                            DeliverableKind::EmailDraft => "email_draft",
                            DeliverableKind::CalendarProposal => "calendar_proposal",
                            DeliverableKind::Other => "artifact",
                        }
                        .into(),
                        uri: format!("flight://{}/artifact/{}", lease.id, deliverable.path),
                        summary: format!(
                            "Flight deliverable awaiting Human release: {}",
                            deliverable.path
                        ),
                        created_by: principal.name.clone(),
                        created_at: finished_at,
                    },
                }
            }));
        }
        if effective_landed {
            events.push(DomainEvent::TaskHeartbeat {
                flow_id: lease.flow_id.clone(),
                task_id: lease.task_id.clone(),
                actor: principal.name.clone(),
                note: Some(format!(
                    "remote flight {} landed for Human review",
                    lease.id
                )),
                at: finished_at,
            });
        } else {
            let budget_reason = (!report.budget_exhaustions.is_empty())
                .then(|| format!("; fuel exhausted: {}", report.budget_exhaustions.join(", ")));
            let contract_reason = (!report.contract_violations.is_empty()).then(|| {
                format!(
                    "; landing contract: {}",
                    report.contract_violations.join(", ")
                )
            });
            events.push(DomainEvent::TaskBlocked {
                flow_id: lease.flow_id.clone(),
                task_id: lease.task_id.clone(),
                actor: principal.name.clone(),
                reason: format!(
                    "remote execution flight crashed: {}{}{}",
                    report.summary,
                    budget_reason.as_deref().unwrap_or_default(),
                    contract_reason.as_deref().unwrap_or_default()
                ),
                at: finished_at,
            });
        }
        events.extend(self.resource_release_events(
            &lease,
            finished_at,
            if effective_landed {
                "flight landed"
            } else {
                "flight crashed"
            },
        ));
        self.commit(&principal.name, events)?;
        Ok(self.state.flight_leases[lease_id].clone())
    }

    pub fn submit_task(&mut self, task_id: &str, actor: &str) -> Result<Task> {
        let (flow, task) = self.task_snapshot(task_id)?;
        ensure_status(&task, &[TaskStatus::InProgress])?;
        self.ensure_task_actor(&task, actor)?;
        if task.evidence.is_empty()
            && !task
                .external_artifacts
                .iter()
                .any(|artifact| artifact.verified)
        {
            return Err(MambaError::Validation(
                "attach evidence or sync a verified external artifact before submission".into(),
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

    fn resolve_active_target(&self, value: &str) -> Result<AssignmentTarget> {
        if let Ok(principal) = self.state.principal(value) {
            if !principal.active {
                return Err(MambaError::Validation(format!(
                    "principal {} is inactive",
                    principal.name
                )));
            }
            return Ok(target_for_principal(principal));
        }
        let team = self.state.team(value)?;
        if !team.active {
            return Err(MambaError::Validation(format!(
                "team {} is inactive",
                team.name
            )));
        }
        Ok(AssignmentTarget {
            kind: TargetKind::Team,
            id: team.id.clone(),
            name: team.name.clone(),
        })
    }

    fn default_copilots_for(&self, owner: &AssignmentTarget) -> Vec<AssignmentTarget> {
        let mut copilots = match owner.kind {
            TargetKind::Human => self
                .state
                .principals
                .values()
                .filter(|principal| {
                    principal.active
                        && principal.kind == PrincipalKind::Agent
                        && principal.owner_id.as_deref() == Some(owner.id.as_str())
                })
                .map(target_for_principal)
                .collect::<Vec<_>>(),
            TargetKind::Agent => self
                .state
                .principals
                .get(&owner.id)
                .and_then(|agent| agent.owner_id.as_deref())
                .and_then(|owner_id| self.state.principals.get(owner_id))
                .filter(|principal| principal.active)
                .map(target_for_principal)
                .into_iter()
                .collect(),
            TargetKind::Team => Vec::new(),
        };
        copilots.sort_by(|left, right| left.name.cmp(&right.name));
        copilots.truncate(2);
        copilots
    }

    fn task_snapshot(&self, task_id: &str) -> Result<(Flow, Task)> {
        let (flow, task) = self.state.find_task(task_id)?;
        Ok((flow.clone(), task.clone()))
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

fn capabilities_cover(required: &[String], actual: &[String]) -> bool {
    let actual = actual
        .iter()
        .map(|capability| normalize_capability(capability))
        .collect::<BTreeSet<_>>();
    required
        .iter()
        .map(|capability| normalize_capability(capability))
        .all(|capability| actual.contains(&capability))
}

fn target_kind_rank(kind: &TargetKind) -> u8 {
    match kind {
        TargetKind::Human => 0,
        TargetKind::Agent => 1,
        TargetKind::Team => 2,
    }
}

fn validate_append_only_revision(flow: &Flow, drafts: &[TaskDraft]) -> Result<Vec<TaskDraft>> {
    let keys = drafts
        .iter()
        .map(|draft| draft.key.as_str())
        .collect::<BTreeSet<_>>();
    if keys.len() != drafts.len() {
        return Err(MambaError::Validation(
            "flow change task keys must be unique".into(),
        ));
    }
    let existing_keys = flow
        .tasks
        .iter()
        .map(|task| task.key.as_str())
        .collect::<BTreeSet<_>>();
    for task in &flow.tasks {
        let expected = TaskDraft {
            key: task.key.clone(),
            title: task.title.clone(),
            description: task.description.clone(),
            required_capabilities: task.required_capabilities.clone(),
            depends_on: task
                .depends_on
                .iter()
                .filter_map(|dependency| flow.task(dependency))
                .map(|dependency| dependency.key.clone())
                .collect(),
            effort_hours: task.estimate.effort_hours,
            requires_human: task.requires_human,
            acceptance_criteria: task.acceptance_criteria.clone(),
        };
        let proposed = drafts
            .iter()
            .find(|draft| draft.key == task.key)
            .ok_or_else(|| {
                MambaError::Validation(format!(
                    "flow change cannot remove existing task {}",
                    task.key
                ))
            })?;
        if proposed != &expected {
            return Err(MambaError::Validation(format!(
                "flow change cannot modify existing task {}; append a new task instead",
                task.key
            )));
        }
    }
    let additions = drafts
        .iter()
        .filter(|draft| !existing_keys.contains(draft.key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let all_keys = drafts
        .iter()
        .map(|draft| draft.key.as_str())
        .collect::<BTreeSet<_>>();
    for task in &additions {
        if let Some(dependency) = task
            .depends_on
            .iter()
            .find(|dependency| !all_keys.contains(dependency.as_str()))
        {
            return Err(MambaError::Validation(format!(
                "new task {} depends on unknown task {}",
                task.key, dependency
            )));
        }
    }
    Ok(additions)
}

fn validate_run_id(run_id: &str) -> Result<()> {
    if run_id.is_empty()
        || run_id.len() > 100
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(MambaError::Validation(
            "invalid remote flight run ID".into(),
        ));
    }
    Ok(())
}

fn validate_remote_flight_report(lease: &FlightLease, report: &RemoteFlightReport) -> Result<()> {
    validate_run_id(&report.run_id)?;
    if lease.run_id.as_deref() != Some(report.run_id.as_str()) {
        return Err(MambaError::Validation(
            "remote flight report run ID does not match its lease".into(),
        ));
    }
    if lease.executor != report.executor {
        return Err(MambaError::Validation(
            "remote flight report executor does not match its lease".into(),
        ));
    }
    if report.summary.trim().is_empty() || report.summary.chars().count() > 4_000 {
        return Err(MambaError::Validation(
            "remote flight report summary must contain 1 to 4000 characters".into(),
        ));
    }
    if report.base_revision.trim().is_empty() || report.base_revision.len() > 128 {
        return Err(MambaError::Validation(
            "remote flight report has an invalid base revision".into(),
        ));
    }
    if report.started_at > report.finished_at {
        return Err(MambaError::Validation(
            "remote flight report finishes before it starts".into(),
        ));
    }
    if report
        .fuel
        .cost_usd
        .is_some_and(|cost| !cost.is_finite() || cost < 0.0)
    {
        return Err(MambaError::Validation(
            "remote flight report contains invalid fuel cost".into(),
        ));
    }
    if report.fuel.duration_seconds > 604_800
        || report.fuel.context_bytes > 67_108_864
        || report
            .fuel
            .tokens
            .is_some_and(|tokens| tokens > 1_000_000_000)
        || report
            .fuel
            .tool_calls
            .is_some_and(|tool_calls| tool_calls > 1_000_000)
    {
        return Err(MambaError::Validation(
            "remote flight report contains implausible fuel usage".into(),
        ));
    }
    if !is_sha256(&report.log_sha256)
        || report
            .patch_sha256
            .as_deref()
            .is_some_and(|hash| !is_sha256(hash))
    {
        return Err(MambaError::Validation(
            "remote flight report contains an invalid SHA-256 digest".into(),
        ));
    }
    if report.patch_sha256.is_some() == report.changed_files.is_empty() {
        return Err(MambaError::Validation(
            "remote flight patch digest and changed-file list do not agree".into(),
        ));
    }
    if report.changed_files.len() > 1_000
        || report.changed_files.iter().any(|path| {
            path.is_empty()
                || path.len() > 1_024
                || Path::new(path).is_absolute()
                || Path::new(path)
                    .components()
                    .any(|component| matches!(component, std::path::Component::ParentDir))
        })
    {
        return Err(MambaError::Validation(
            "remote flight report contains an unsafe changed-file path".into(),
        ));
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_external_artifact(artifact: &ExternalArtifact) -> Result<()> {
    if artifact.id.trim().is_empty()
        || artifact.provider.trim().is_empty()
        || artifact.kind.trim().is_empty()
        || artifact.project.trim().is_empty()
        || artifact.external_id.trim().is_empty()
        || artifact.title.trim().is_empty()
        || artifact.url.trim().is_empty()
        || artifact.status.trim().is_empty()
    {
        Err(MambaError::Validation(
            "external artifact fields cannot be empty".into(),
        ))
    } else {
        Ok(())
    }
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
    use crate::domain::{
        AttentionSeverity, CapabilityPack, ExternalInteractionAction, FlowMessageKind, FuelBudget,
        FuelUsage, NotificationConnector, NotificationStatus, RecoveryAction, ResourceClaim,
        ResourceKind, ResourceLeaseStatus,
    };
    use crate::notification::NotificationAttempt;

    #[tokio::test]
    async fn external_human_interactions_are_bound_atomic_idempotent_and_replayable() {
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
        let binding = app
            .bind_external_identity("Slack", "U-LEADER", &human.id, "admin")
            .unwrap();
        assert_eq!(binding.provider, "slack");
        assert!(
            app.bind_external_identity("slack", "U-OTHER", &human.id, "admin")
                .is_err()
        );
        let flow = app
            .create_demand(
                "Prepare a launch plan",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        app.approve_flow(&flow.id, &human.name).unwrap();
        let task = flow.tasks[0].clone();

        let accepted = app
            .process_external_interaction(
                "slack",
                "delivery-1",
                "U-LEADER",
                ExternalInteractionAction::TaskAccept,
                &task.id,
                None,
            )
            .unwrap();
        assert!(!accepted.duplicate);
        assert_eq!(accepted.receipt.principal_id, human.id);
        assert_eq!(
            app.state().find_task(&task.id).unwrap().1.status,
            TaskStatus::Accepted
        );
        let duplicate = app
            .process_external_interaction(
                "slack",
                "delivery-1",
                "U-LEADER",
                ExternalInteractionAction::TaskAccept,
                &task.id,
                None,
            )
            .unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.receipt.id, accepted.receipt.id);
        assert!(
            app.process_external_interaction(
                "slack",
                "delivery-1",
                "U-LEADER",
                ExternalInteractionAction::TaskAccept,
                "TSK-DIFFERENT",
                None,
            )
            .is_err()
        );

        let message = app
            .post_flow_message(
                &flow.id,
                Some(&task.id),
                &human.name,
                FlowMessageKind::Command,
                std::slice::from_ref(&human.name),
                "Confirm the release window",
                true,
            )
            .unwrap();
        app.process_external_interaction(
            "slack",
            "delivery-2",
            "U-LEADER",
            ExternalInteractionAction::MessageAck,
            &message.id,
            None,
        )
        .unwrap();
        assert!(app.state().messages[&message.id].recipient_is_acknowledged(&human.id));
        assert_eq!(app.state().external_interactions.len(), 2);

        app.unbind_external_identity(&binding.id, "admin").unwrap();
        assert!(
            app.process_external_interaction(
                "slack",
                "delivery-3",
                "U-LEADER",
                ExternalInteractionAction::MessageAck,
                &message.id,
                None,
            )
            .is_err()
        );
        assert_eq!(app.state().external_interactions.len(), 2);

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        assert_eq!(replayed.state().external_interactions.len(), 2);
        assert!(!replayed.state().external_identities[&binding.id].is_active());
        assert_eq!(
            replayed.state().find_task(&task.id).unwrap().1.status,
            TaskStatus::Accepted
        );
    }

    #[tokio::test]
    async fn notification_outbox_is_atomic_retryable_and_replayable() {
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
        let endpoint = app
            .register_notification_endpoint(
                "operations",
                "https://example.invalid/hooks/mamba",
                &["task.blocked".into(), "flow_message.posted".into()],
                "MAMBA_TEST_WEBHOOK_SECRET",
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare a launch plan",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        app.approve_flow(&flow.id, &human.name).unwrap();
        let task = &flow.tasks[0];
        app.accept_task(&task.id, &human.name).unwrap();
        app.start_task(&task.id, &human.name).unwrap();
        app.block_task(&task.id, &human.name, "waiting for access")
            .unwrap();
        assert_eq!(app.state().notification_deliveries.len(), 1);
        let delivery = app
            .state()
            .notification_deliveries
            .values()
            .next()
            .unwrap()
            .clone();
        assert_eq!(delivery.source_event_kind, "task.blocked");
        assert_eq!(delivery.flow_id.as_deref(), Some(flow.id.as_str()));

        app.record_notification_attempt(
            &delivery.id,
            NotificationAttempt {
                delivered: false,
                response_status: Some(503),
                error: Some("endpoint returned HTTP 503".into()),
                attempted_at: Utc::now(),
            },
            "tower://test",
        )
        .unwrap();
        assert_eq!(
            app.state().notification_deliveries[&delivery.id].status,
            NotificationStatus::Failed
        );
        assert!(app.notification_attempts(10, false).is_empty());
        assert_eq!(app.notification_attempts(10, true).len(), 1);
        app.record_notification_attempt(
            &delivery.id,
            NotificationAttempt {
                delivered: true,
                response_status: Some(204),
                error: None,
                attempted_at: Utc::now(),
            },
            "tower://test",
        )
        .unwrap();
        assert_eq!(
            app.state().notification_deliveries[&delivery.id].attempts,
            2
        );

        app.post_flow_message(
            &flow.id,
            Some(&task.id),
            &human.name,
            FlowMessageKind::Update,
            std::slice::from_ref(&human.name),
            "still waiting",
            false,
        )
        .unwrap();
        assert_eq!(app.state().notification_deliveries.len(), 2);
        let queued_message = app
            .state()
            .notification_deliveries
            .values()
            .find(|candidate| candidate.id != delivery.id)
            .unwrap()
            .id
            .clone();
        app.disable_notification_endpoint(&endpoint.id, "admin")
            .unwrap();
        assert_eq!(
            app.state().notification_deliveries[&queued_message].status,
            NotificationStatus::Cancelled
        );
        app.post_flow_message(
            &flow.id,
            Some(&task.id),
            &human.name,
            FlowMessageKind::Update,
            std::slice::from_ref(&human.name),
            "endpoint disabled",
            false,
        )
        .unwrap();
        assert_eq!(app.state().notification_deliveries.len(), 2);

        let teams = app
            .register_notification_connector(
                "leadership teams",
                NotificationConnector::Teams,
                "MAMBA_TEAMS_WEBHOOK_URL",
                &["tracking.escalation_raised".into()],
                None,
                "admin",
            )
            .unwrap();
        assert_eq!(teams.url_env.as_deref(), Some("MAMBA_TEAMS_WEBHOOK_URL"));
        assert!(teams.url.is_empty());

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        let replayed_delivery = &replayed.state().notification_deliveries[&delivery.id];
        assert_eq!(replayed_delivery.status, NotificationStatus::Delivered);
        assert_eq!(replayed_delivery.attempts, 2);
        assert_eq!(
            replayed.state().notification_deliveries[&queued_message].status,
            NotificationStatus::Cancelled
        );
        assert!(!replayed.state().notification_endpoints[&endpoint.id].active);
        assert_eq!(
            replayed.state().notification_endpoints[&teams.id].connector,
            NotificationConnector::Teams
        );
    }

    #[tokio::test]
    async fn work_calendar_and_time_off_reschedule_active_flows_and_replay() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Product", "product,delivery", "admin")
            .unwrap();
        let manager = app
            .register_principal(
                "Manager",
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
                "Prepare a customer launch plan",
                &manager.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        app.approve_flow(&flow.id, &manager.name).unwrap();

        let calendar = app
            .configure_work_calendar(
                &manager.id,
                8 * 60,
                crate::calendar::parse_workdays("mon,tue,wed,thu,fri").unwrap(),
                9 * 60,
                18 * 60,
                "admin",
            )
            .unwrap();
        assert_eq!(calendar.utc_offset_minutes, 8 * 60);
        let manager_task_id = app
            .state()
            .flow(&flow.id)
            .unwrap()
            .tasks
            .iter()
            .find(|task| task.assignment.as_ref().unwrap().owner.id == manager.id)
            .unwrap()
            .id
            .clone();
        let before_leave = app
            .state()
            .find_task(&manager_task_id)
            .unwrap()
            .1
            .estimate
            .p80_finish;
        let block = app
            .add_time_off(
                &manager.id,
                Utc::now() - Duration::hours(1),
                Utc::now() + Duration::days(10),
                "planned leave",
                &manager.name,
            )
            .unwrap();
        let during_leave = app
            .state()
            .find_task(&manager_task_id)
            .unwrap()
            .1
            .estimate
            .p80_finish;
        assert!(during_leave > before_leave);
        let cancelled = app
            .cancel_time_off(&manager.id, &block.id, &manager.name)
            .unwrap();
        assert!(!cancelled.is_active());
        let after_cancel = app
            .state()
            .find_task(&manager_task_id)
            .unwrap()
            .1
            .estimate
            .p80_finish;
        assert!(after_cancel < during_leave);

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        let replayed_calendar = replayed.state().work_calendar(&manager.id).unwrap();
        assert_eq!(replayed_calendar.utc_offset_minutes, 8 * 60);
        assert!(!replayed_calendar.time_off[0].is_active());
        assert_eq!(
            replayed
                .state()
                .find_task(&manager_task_id)
                .unwrap()
                .1
                .estimate
                .p80_finish,
            after_cancel
        );
    }

    #[test]
    fn remote_agent_does_not_require_a_server_local_executor() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app.create_team("Platform", "backend", "admin").unwrap();
        let human = app
            .register_principal(
                "Engineer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "backend",
                100,
                None,
                "admin",
            )
            .unwrap();
        let agent = app
            .register_principal(
                "Engineer Personal Agent",
                PrincipalKind::Agent,
                Some(&team.id),
                Some(&human.id),
                "backend",
                100,
                None,
                "admin",
            )
            .unwrap();
        assert!(agent.executor.is_none());
        assert_eq!(agent.owner_id.as_deref(), Some(human.id.as_str()));
    }

    #[tokio::test]
    async fn human_authorized_remote_flight_is_single_use_and_replays() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Delivery", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "Engineer",
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
                "Engineer Personal Agent",
                PrincipalKind::Agent,
                Some(&team.id),
                Some(&human.id),
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
        let task_id = flow.tasks[0].id.clone();
        app.approve_flow(&flow.id, &human.name).unwrap();
        app.accept_task(&task_id, &human.name).unwrap();

        let agent_authorization = app.authorize_remote_flight(
            &task_id,
            &agent.name,
            &agent.name,
            ExecutorKind::Codex,
            3_600,
        );
        assert!(matches!(
            agent_authorization,
            Err(MambaError::PermissionDenied(_))
        ));
        let lease = app
            .authorize_remote_flight(
                &task_id,
                &human.name,
                &agent.name,
                ExecutorKind::Codex,
                3_600,
            )
            .unwrap();
        assert_eq!(lease.status, FlightLeaseStatus::Authorized);
        assert!(lease.manifest.is_some());
        assert!(app.state().resource_leases.values().any(|resource| {
            resource.flight_lease_id == lease.id && resource.status == ResourceLeaseStatus::Active
        }));
        let backup = app
            .register_principal(
                "Backup Engineer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        let unsafe_reassignment = app.reassign_task(
            &task_id,
            &human.name,
            &backup.name,
            &[],
            "move work while a lease is open",
        );
        assert!(matches!(
            unsafe_reassignment,
            Err(MambaError::InvalidTransition(_))
        ));
        assert!(
            app.authorize_remote_flight(
                &task_id,
                &human.name,
                &agent.name,
                ExecutorKind::Codex,
                3_600,
            )
            .is_err()
        );

        let active = app
            .claim_remote_flight(&lease.id, &agent.name, "WRUN-test")
            .unwrap();
        assert_eq!(active.status, FlightLeaseStatus::Active);
        assert!(
            app.claim_remote_flight(&lease.id, &agent.name, "WRUN-second")
                .is_err()
        );
        let now = Utc::now();
        let report = RemoteFlightReport {
            run_id: "WRUN-test".into(),
            executor: ExecutorKind::Codex,
            summary: "implementation patch is ready for Human review".into(),
            base_revision: "abc123".into(),
            changed_files: vec!["src/gateway.rs".into()],
            patch_sha256: Some("a".repeat(64)),
            log_sha256: "b".repeat(64),
            started_at: now,
            finished_at: now,
            fuel: Default::default(),
            failure_class: None,
            budget_exhaustions: Vec::new(),
            deliverables: Vec::new(),
            contract_violations: Vec::new(),
        };
        let landed = app
            .finish_remote_flight(&lease.id, &agent.name, true, report.clone())
            .unwrap();
        assert_eq!(landed.status, FlightLeaseStatus::Landed);
        assert_eq!(
            landed.report.as_ref().unwrap().deliverables[0].kind,
            DeliverableKind::Code
        );
        assert!(app.state().resource_leases.values().any(|resource| {
            resource.flight_lease_id == lease.id && resource.status == ResourceLeaseStatus::Released
        }));
        assert!(
            app.state()
                .find_task(&task_id)
                .unwrap()
                .1
                .evidence
                .iter()
                .any(|evidence| evidence.kind == "remote_patch")
        );
        assert_eq!(
            app.finish_remote_flight(&lease.id, &agent.name, true, landed.report.clone().unwrap(),)
                .unwrap(),
            landed
        );
        let revoked = app
            .authorize_remote_flight(
                &task_id,
                &human.name,
                &agent.name,
                ExecutorKind::ClaudeCode,
                3_600,
            )
            .and_then(|lease| app.revoke_remote_flight(&lease.id, &human.name))
            .unwrap();
        assert_eq!(revoked.status, FlightLeaseStatus::Revoked);
        assert!(app.state().resource_leases.values().any(|resource| {
            resource.flight_lease_id == revoked.id
                && resource.status == ResourceLeaseStatus::Released
        }));
        assert!(
            app.claim_remote_flight(&revoked.id, &agent.name, "WRUN-revoked")
                .is_err()
        );

        let office = app
            .authorize_remote_flight_with_manifest(
                &task_id,
                &human.name,
                &agent.name,
                ExecutorKind::Codex,
                3_600,
                FlightManifestDraft {
                    capability_pack: Some(CapabilityPack::Office),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            office
                .manifest
                .as_ref()
                .unwrap()
                .output_contract
                .requires_human_release
        );
        app.claim_remote_flight(&office.id, &agent.name, "WRUN-office")
            .unwrap();
        let now = Utc::now();
        let office_landed = app
            .finish_remote_flight(
                &office.id,
                &agent.name,
                true,
                RemoteFlightReport {
                    run_id: "WRUN-office".into(),
                    executor: ExecutorKind::Codex,
                    summary: "release notes draft is ready".into(),
                    base_revision: "abc123".into(),
                    changed_files: vec!["docs/release-notes.docx".into()],
                    patch_sha256: Some("c".repeat(64)),
                    log_sha256: "d".repeat(64),
                    started_at: now,
                    finished_at: now,
                    fuel: Default::default(),
                    failure_class: None,
                    budget_exhaustions: Vec::new(),
                    deliverables: Vec::new(),
                    contract_violations: Vec::new(),
                },
            )
            .unwrap();
        assert_eq!(office_landed.status, FlightLeaseStatus::Landed);
        assert_eq!(
            office_landed.report.as_ref().unwrap().deliverables[0].kind,
            DeliverableKind::Document
        );
        assert!(
            app.state()
                .find_task(&task_id)
                .unwrap()
                .1
                .evidence
                .iter()
                .any(|evidence| evidence.kind == "document"
                    && evidence.uri.contains("release-notes.docx"))
        );

        let invalid_office = app
            .authorize_remote_flight_with_manifest(
                &task_id,
                &human.name,
                &agent.name,
                ExecutorKind::Codex,
                3_600,
                FlightManifestDraft {
                    capability_pack: Some(CapabilityPack::Office),
                    ..Default::default()
                },
            )
            .unwrap();
        app.claim_remote_flight(&invalid_office.id, &agent.name, "WRUN-office-invalid")
            .unwrap();
        let invalid_office = app
            .finish_remote_flight(
                &invalid_office.id,
                &agent.name,
                true,
                RemoteFlightReport {
                    run_id: "WRUN-office-invalid".into(),
                    executor: ExecutorKind::Codex,
                    summary: "unexpected source file".into(),
                    base_revision: "abc123".into(),
                    changed_files: vec!["src/unapproved.rs".into()],
                    patch_sha256: Some("e".repeat(64)),
                    log_sha256: "f".repeat(64),
                    started_at: now,
                    finished_at: now,
                    fuel: Default::default(),
                    failure_class: None,
                    budget_exhaustions: Vec::new(),
                    deliverables: Vec::new(),
                    contract_violations: Vec::new(),
                },
            )
            .unwrap();
        assert_eq!(invalid_office.status, FlightLeaseStatus::Crashed);
        let invalid_report = invalid_office.report.as_ref().unwrap();
        assert_eq!(invalid_report.failure_class, Some(FailureClass::Validation));
        assert_eq!(invalid_report.contract_violations.len(), 1);
        drop(app);

        let replayed = MambaApp::open(&data_dir).unwrap();
        let replayed_lease = &replayed.state().flight_leases[&lease.id];
        assert_eq!(replayed_lease.status, FlightLeaseStatus::Landed);
        assert_eq!(replayed_lease.run_id.as_deref(), Some("WRUN-test"));
        assert_eq!(
            replayed.state().flight_leases[&revoked.id].status,
            FlightLeaseStatus::Revoked
        );
        assert!(
            replayed
                .timeline(&flow.id)
                .unwrap()
                .iter()
                .any(|event| event.kind == "remote_flight.landed")
        );
    }

    #[tokio::test]
    async fn flight_fuel_resources_and_supervision_tree_are_enforced_and_replayed() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Platform", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "Engineer",
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
                "Engineer Agent",
                PrincipalKind::Agent,
                Some(&team.id),
                Some(&human.id),
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        let first_flow = app
            .create_demand(
                "Prepare the gateway release",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        let first_task = first_flow.tasks[0].id.clone();
        app.approve_flow(&first_flow.id, &human.name).unwrap();
        app.accept_task(&first_task, &human.name).unwrap();

        let shared_file = ResourceClaim {
            kind: ResourceKind::File,
            key: "src/gateway.rs".into(),
            exclusive: true,
        };
        let lease = app
            .authorize_remote_flight_with_manifest(
                &first_task,
                &human.name,
                &agent.name,
                ExecutorKind::Codex,
                3_600,
                FlightManifestDraft {
                    fuel: Some(FuelBudget {
                        max_duration_seconds: 60,
                        max_context_bytes: 8,
                        max_tokens: Some(100),
                        max_tool_calls: Some(10),
                        max_cost_usd: Some(1.0),
                    }),
                    resources: vec![shared_file.clone()],
                    ..Default::default()
                },
            )
            .unwrap();

        let second_flow = app
            .create_demand(
                "Audit the gateway release",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        let second_task = second_flow.tasks[0].id.clone();
        app.approve_flow(&second_flow.id, &human.name).unwrap();
        app.accept_task(&second_task, &human.name).unwrap();
        let conflict = app.authorize_remote_flight_with_manifest(
            &second_task,
            &human.name,
            &agent.name,
            ExecutorKind::ClaudeCode,
            3_600,
            FlightManifestDraft {
                resources: vec![shared_file],
                ..Default::default()
            },
        );
        assert!(matches!(conflict, Err(MambaError::InvalidTransition(_))));

        app.claim_remote_flight(&lease.id, &agent.name, "WRUN-fuel")
            .unwrap();
        let finished_at = Utc::now();
        let crashed = app
            .finish_remote_flight(
                &lease.id,
                &agent.name,
                true,
                RemoteFlightReport {
                    run_id: "WRUN-fuel".into(),
                    executor: ExecutorKind::Codex,
                    summary: "executor reported success".into(),
                    base_revision: "abc123".into(),
                    changed_files: Vec::new(),
                    patch_sha256: None,
                    log_sha256: "a".repeat(64),
                    started_at: finished_at - Duration::seconds(1),
                    finished_at,
                    fuel: FuelUsage {
                        duration_seconds: 1,
                        context_bytes: 9,
                        tokens: Some(90),
                        tool_calls: Some(3),
                        cost_usd: Some(0.5),
                    },
                    failure_class: None,
                    budget_exhaustions: Vec::new(),
                    deliverables: Vec::new(),
                    contract_violations: Vec::new(),
                },
            )
            .unwrap();
        assert_eq!(crashed.status, FlightLeaseStatus::Crashed);
        let report = crashed.report.as_ref().unwrap();
        assert_eq!(report.failure_class, Some(FailureClass::Budget));
        assert_eq!(report.budget_exhaustions.len(), 1);
        assert!(report.budget_exhaustions[0].contains("context"));
        assert!(app.state().resource_leases.values().any(|resource| {
            resource.flight_lease_id == lease.id && resource.status == ResourceLeaseStatus::Released
        }));
        assert_eq!(
            app.recovery_options(&lease.id, &human.name).unwrap(),
            vec![RecoveryAction::ReduceScope, RecoveryAction::HumanHandoff]
        );

        let child = app
            .recover_remote_flight(
                &lease.id,
                &human.name,
                RecoveryAction::ReduceScope,
                "retry with a smaller context payload",
                None,
                Some("implement only the gateway routing change".into()),
                3_600,
            )
            .unwrap()
            .unwrap();
        assert_eq!(child.parent_lease_id.as_deref(), Some(lease.id.as_str()));
        assert_eq!(child.root_lease_id.as_deref(), Some(lease.id.as_str()));
        assert_eq!(child.attempt, 2);
        assert_eq!(child.status, FlightLeaseStatus::Authorized);
        assert!(app.state().resource_leases.values().any(|resource| {
            resource.flight_lease_id == child.id
                && resource.status == ResourceLeaseStatus::Active
                && resource.expires_at > child.expires_at
        }));
        let decision = app.state().flight_recoveries.values().next().unwrap();
        assert_eq!(decision.parent_lease_id, lease.id);
        assert_eq!(decision.child_lease_id.as_deref(), Some(child.id.as_str()));
        assert_eq!(
            app.expire_remote_flights_at(
                "tower://test-reaper",
                child.expires_at + Duration::seconds(1),
            )
            .unwrap(),
            1
        );
        assert_eq!(
            app.state().flight_leases[&child.id].status,
            FlightLeaseStatus::Expired
        );
        assert!(app.state().resource_leases.values().any(|resource| {
            resource.flight_lease_id == child.id && resource.status == ResourceLeaseStatus::Released
        }));

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        assert_eq!(
            replayed.state().flight_leases[&lease.id].status,
            FlightLeaseStatus::Crashed
        );
        assert_eq!(replayed.state().flight_leases[&child.id].attempt, 2);
        assert_eq!(
            replayed.state().flight_leases[&child.id].status,
            FlightLeaseStatus::Expired
        );
        assert!(replayed.state().flight_recoveries.values().any(|decision| {
            decision.parent_lease_id == lease.id
                && decision.child_lease_id.as_deref() == Some(child.id.as_str())
        }));
    }

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
        let expires_at = issued.credential.expires_at.unwrap();
        assert!(expires_at > Utc::now() + Duration::days(29));
        assert!(expires_at < Utc::now() + Duration::days(31));
        assert!(
            app.issue_api_credential_with_ttl(&human.id, "invalid", "admin", 0)
                .is_err()
        );
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
    async fn flow_messages_route_to_humans_agents_and_teams_with_replayable_receipts() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let product = app
            .create_team("Product", "product,delivery", "admin")
            .unwrap();
        let security = app
            .create_team("Security", "security,operations", "admin")
            .unwrap();
        let legal = app.create_team("Legal", "legal", "admin").unwrap();
        let manager = app
            .register_principal(
                "Manager",
                PrincipalKind::Human,
                Some(&product.id),
                None,
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        let engineer = app
            .register_principal(
                "Security Engineer",
                PrincipalKind::Human,
                Some(&security.id),
                None,
                "security,operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        let agent = app
            .register_principal(
                "Security Copilot",
                PrincipalKind::Agent,
                Some(&security.id),
                Some(&engineer.id),
                "security,operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare a launch brief",
                &manager.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        let outsider = app
            .register_principal(
                "Outsider",
                PrincipalKind::Human,
                Some(&legal.id),
                None,
                "legal",
                100,
                None,
                "admin",
            )
            .unwrap();
        let task_id = flow.tasks[0].id.clone();
        let message = app
            .post_flow_message(
                &flow.id,
                Some(&task_id),
                &manager.name,
                FlowMessageKind::Command,
                &[agent.name.clone(), security.name.clone()],
                "Confirm the production secret rotation boundary",
                true,
            )
            .unwrap();

        let engineer_inbox = app.message_inbox(&engineer.name, false).unwrap();
        assert_eq!(engineer_inbox.len(), 1);
        assert_eq!(engineer_inbox[0].pending_recipient_ids.len(), 2);
        assert!(engineer_inbox[0].needs_acknowledgement());
        assert_eq!(app.message_inbox(&agent.name, false).unwrap().len(), 1);
        assert!(app.flow_messages(&flow.id, &outsider.name).is_err());

        let reply = app
            .post_flow_message(
                &flow.id,
                Some(&task_id),
                &engineer.name,
                FlowMessageKind::Update,
                std::slice::from_ref(&manager.name),
                "Boundary confirmed; rollout can continue",
                false,
            )
            .unwrap();
        assert_eq!(app.message_inbox(&manager.name, false).unwrap().len(), 1);
        assert_eq!(app.flow_messages(&flow.id, &manager.name).unwrap().len(), 2);
        assert_eq!(reply.sender_id, engineer.id);

        let acknowledged = app
            .acknowledge_flow_message(&message.id, &engineer.name)
            .unwrap();
        assert_eq!(acknowledged.acknowledgements.len(), 2);
        assert!(app.message_inbox(&engineer.name, false).unwrap().is_empty());
        assert!(app.message_inbox(&agent.name, false).unwrap().is_empty());
        assert_eq!(app.message_inbox(&engineer.name, true).unwrap().len(), 1);

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        assert_eq!(replayed.state().messages.len(), 2);
        assert_eq!(
            replayed.state().messages[&message.id]
                .acknowledgements
                .len(),
            2
        );
        let kinds = replayed
            .timeline(&flow.id)
            .unwrap()
            .into_iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>();
        assert!(kinds.contains(&"flow_message.posted".to_string()));
        assert!(kinds.contains(&"flow_message.acknowledged".to_string()));
    }

    #[tokio::test]
    async fn reassignment_and_negotiation_reschedule_the_full_flow_and_replay() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team(
                "Platform",
                "product,backend,llm-platform,security,quality,observability,operations",
                "admin",
            )
            .unwrap();
        let manager = app
            .register_principal(
                "Manager",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,llm-platform,operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        let fast = app
            .register_principal(
                "Fast Engineer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "backend,llm-platform,security,quality,observability",
                100,
                None,
                "admin",
            )
            .unwrap();
        let slow = app
            .register_principal(
                "Slow Engineer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "backend,llm-platform",
                25,
                None,
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Build an LLM Gateway this week",
                &manager.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        app.approve_flow(&flow.id, &manager.name).unwrap();
        let gateway = flow.task("gateway-core").unwrap().clone();
        assert_eq!(gateway.assignment.as_ref().unwrap().owner.id, fast.id);
        let old_gateway_p80 = gateway.estimate.p80_hours;
        let old_observability_start = flow.task("observability").unwrap().estimate.earliest_start;

        let denied = app
            .reassign_task(
                &gateway.id,
                &fast.name,
                &slow.name,
                &[],
                "capacity rebalance",
            )
            .unwrap_err();
        assert!(matches!(denied, MambaError::PermissionDenied(_)));
        let reassigned = app
            .reassign_task(
                &gateway.id,
                &manager.name,
                &slow.name,
                &[],
                "Fast Engineer is handling the incident response",
            )
            .unwrap();
        let gateway = reassigned.task(&gateway.id).unwrap();
        assert_eq!(gateway.assignment.as_ref().unwrap().owner.id, slow.id);
        assert_eq!(gateway.status, TaskStatus::Assigned);
        assert!(gateway.estimate.p80_hours > old_gateway_p80);
        assert!(
            reassigned
                .task("observability")
                .unwrap()
                .estimate
                .earliest_start
                > old_observability_start
        );

        app.accept_task(&gateway.id, &slow.name).unwrap();
        let negotiated = app.negotiate_task(&gateway.id, &slow.name, 40.0).unwrap();
        assert_eq!(negotiated.estimate.effort_hours, 40.0);
        let updated = app.state().flow(&flow.id).unwrap();
        assert!(
            updated
                .task("observability")
                .unwrap()
                .estimate
                .earliest_start
                >= negotiated.estimate.p80_finish
        );
        assert!(updated.critical_path.contains(&"gateway-core".to_string()));
        let expected_p80 = updated.p80_finish;

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        let replayed_flow = replayed.state().flow(&flow.id).unwrap();
        assert_eq!(replayed_flow.p80_finish, expected_p80);
        assert_eq!(
            replayed_flow
                .task(&gateway.id)
                .unwrap()
                .assignment
                .as_ref()
                .unwrap()
                .owner
                .id,
            slow.id
        );
        let kinds = replayed
            .timeline(&flow.id)
            .unwrap()
            .into_iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>();
        assert!(kinds.contains(&"task.reassigned".to_string()));
        assert!(kinds.contains(&"flow.rescheduled".to_string()));
    }

    #[tokio::test]
    async fn flow_change_preview_requires_fresh_human_approval_and_replays() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Delivery", "product,delivery,security", "admin")
            .unwrap();
        let manager = app
            .register_principal(
                "Manager",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery,security",
                100,
                None,
                "admin",
            )
            .unwrap();
        let engineer = app
            .register_principal(
                "Engineer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "delivery,security",
                100,
                None,
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare a launch brief",
                &manager.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        app.approve_flow(&flow.id, &manager.name).unwrap();
        let original_tasks = flow.tasks.len();
        let change = app
            .propose_flow_change(
                &flow.id,
                &manager.name,
                "Add a security sign-off checklist",
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        assert_eq!(change.status, FlowChangeStatus::Proposed);
        assert_eq!(change.new_tasks.len(), 1);
        assert_eq!(
            app.state().flow(&flow.id).unwrap().tasks.len(),
            original_tasks
        );
        assert!(change.impact.scope_p80_delta_hours > 0.0);
        assert!(app.approve_flow_change(&change.id, &engineer.name).is_err());
        let applied = app.approve_flow_change(&change.id, &manager.name).unwrap();
        assert_eq!(applied.status, FlowChangeStatus::Applied);
        let updated = app.state().flow(&flow.id).unwrap();
        assert_eq!(updated.tasks.len(), original_tasks + 1);
        assert_eq!(
            updated.task("change-1").unwrap().status,
            TaskStatus::Assigned
        );
        assert!(
            updated
                .prd
                .acceptance_criteria
                .iter()
                .any(|criterion| criterion.contains("security sign-off"))
        );

        let stale = app
            .propose_flow_change(
                &flow.id,
                &manager.name,
                "Add a customer communication step",
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        let manager_task = app
            .state()
            .flow(&flow.id)
            .unwrap()
            .tasks
            .iter()
            .find(|task| {
                task.status == TaskStatus::Assigned
                    && task.assignment.as_ref().is_some_and(|assignment| {
                        assignment.owner.id == manager.id
                            || assignment.owner.kind == TargetKind::Team
                    })
            })
            .unwrap()
            .id
            .clone();
        app.accept_task(&manager_task, &manager.name).unwrap();
        let stale_error = app
            .approve_flow_change(&stale.id, &manager.name)
            .unwrap_err();
        assert!(matches!(stale_error, MambaError::InvalidTransition(_)));
        let rejected = app
            .reject_flow_change(&stale.id, &manager.name, "Regenerate against current work")
            .unwrap();
        assert_eq!(rejected.status, FlowChangeStatus::Rejected);

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        assert_eq!(
            replayed.state().flow(&flow.id).unwrap().tasks.len(),
            original_tasks + 1
        );
        assert_eq!(
            replayed.state().flow_changes[&change.id].status,
            FlowChangeStatus::Applied
        );
        assert_eq!(
            replayed.state().flow_changes[&stale.id].status,
            FlowChangeStatus::Rejected
        );
    }

    #[tokio::test]
    async fn external_artifact_sync_is_idempotent_and_can_gate_submission() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team(
                "Platform",
                "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
                "admin",
            )
            .unwrap();
        let human = app
            .register_principal(
                "Leader",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
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
        let task_id = flow.tasks[0].id.clone();
        app.approve_flow(&flow.id, &human.name).unwrap();
        app.accept_task(&task_id, &human.name).unwrap();
        app.start_task(&task_id, &human.name).unwrap();
        let artifact = ExternalArtifact {
            id: "EXT-stable".into(),
            provider: "gitlab".into(),
            kind: "pipeline".into(),
            project: "platform/gateway".into(),
            external_id: "99".into(),
            parent_id: Some("EXT-merge-request".into()),
            title: "Pipeline #99".into(),
            url: "https://gitlab.example/platform/gateway/-/pipelines/99".into(),
            status: "success".into(),
            revision: Some("abc123".into()),
            verified: true,
            synced_at: Utc::now(),
        };
        assert_eq!(
            app.sync_external_artifacts(&task_id, &human.name, vec![artifact.clone()])
                .unwrap()
                .len(),
            1
        );
        let event_count = app.timeline(&flow.id).unwrap().len();
        let mut later_snapshot = artifact;
        later_snapshot.synced_at = Utc::now() + Duration::minutes(1);
        assert!(
            app.sync_external_artifacts(&task_id, &human.name, vec![later_snapshot.clone()])
                .unwrap()
                .is_empty()
        );
        assert_eq!(app.timeline(&flow.id).unwrap().len(), event_count);

        let mut failed_pipeline = later_snapshot.clone();
        failed_pipeline.id = "EXT-failed".into();
        failed_pipeline.external_id = "100".into();
        failed_pipeline.title = "Pipeline #100".into();
        failed_pipeline.status = "failed".into();
        failed_pipeline.verified = false;
        failed_pipeline.synced_at += Duration::minutes(1);
        app.sync_external_artifacts(&task_id, &human.name, vec![failed_pipeline.clone()])
            .unwrap();
        assert!(app.submit_task(&task_id, &human.name).is_err());
        assert_eq!(
            app.state()
                .find_task(&task_id)
                .unwrap()
                .1
                .external_artifacts
                .len(),
            1
        );

        let mut recovered_pipeline = failed_pipeline;
        recovered_pipeline.id = "EXT-recovered".into();
        recovered_pipeline.external_id = "101".into();
        recovered_pipeline.title = "Pipeline #101".into();
        recovered_pipeline.status = "success".into();
        recovered_pipeline.verified = true;
        recovered_pipeline.synced_at += Duration::minutes(1);
        app.sync_external_artifacts(&task_id, &human.name, vec![recovered_pipeline])
            .unwrap();
        app.submit_task(&task_id, &human.name).unwrap();
        drop(app);

        let replayed = MambaApp::open(&data_dir).unwrap();
        let task = replayed.state().find_task(&task_id).unwrap().1;
        assert_eq!(task.external_artifacts.len(), 1);
        assert_eq!(task.status, TaskStatus::Submitted);
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
