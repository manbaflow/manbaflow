use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dashboard::{DashboardSnapshot, build_dashboard};
use crate::domain::{
    ApiCredential, Assignment, AssignmentTarget, AttentionSeverity, AvailabilityBlock, Demand,
    Estimate, Evidence, ExecutionRecord, ExecutorConfig, ExecutorKind, ExecutorMode,
    ExternalArtifact, FlightLease, FlightLeaseStatus, Flow, FlowChangeImpact, FlowChangeRequest,
    FlowChangeStatus, FlowMessage, FlowMessageKind, FlowScheduleRevision, FlowStatus,
    IssuedCredential, MessageAcknowledgement, MessageInboxItem, NotificationDelivery,
    NotificationEndpoint, NotificationStatus, Organization, Principal, PrincipalKind,
    RemoteFlightReport, TargetKind, Task, TaskDraft, TaskStatus, Team, TrackingAttention,
    TrackingEscalation, TrackingScan, WorkCalendar, Workday,
};
use crate::error::{MambaError, Result};
use crate::event::{DomainEvent, EventEnvelope};
use crate::executor::{ExecutionRequest, TerminalExecutor};
use crate::ids::{new_id, normalize_capability, parse_capabilities};
use crate::matcher::Matcher;
use crate::notification::{NotificationAttempt, NotificationDispatchSummary};
use crate::planner::{PlannerKind, generate_plan, generate_revision_plan};
use crate::scheduler::{reschedule, schedule};
use crate::state::OrganizationState;
use crate::store::EventStore;
use crate::tracker;

pub struct MambaApp {
    data_dir: PathBuf,
    store: EventStore,
    state: OrganizationState,
}

pub(crate) struct ExternalDeliverySync {
    pub duplicate: bool,
    pub stale: bool,
    pub matched_tasks: usize,
    pub changed_tasks: usize,
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

    pub fn register_notification_endpoint(
        &mut self,
        name: &str,
        url: &str,
        event_kinds: &[String],
        secret_env: &str,
        actor: &str,
    ) -> Result<NotificationEndpoint> {
        let mut event_kinds = event_kinds
            .iter()
            .map(|kind| kind.trim().to_ascii_lowercase())
            .filter(|kind| !kind.is_empty())
            .collect::<Vec<_>>();
        event_kinds.sort();
        event_kinds.dedup();
        if self
            .state
            .notification_endpoints
            .values()
            .any(|endpoint| endpoint.name.eq_ignore_ascii_case(name.trim()) && endpoint.active)
        {
            return Err(MambaError::Validation(format!(
                "active notification endpoint already exists: {}",
                name.trim()
            )));
        }
        let endpoint = NotificationEndpoint {
            id: new_id("NEND"),
            name: name.trim().to_string(),
            url: url.trim().to_string(),
            event_kinds,
            secret_env: secret_env.trim().to_string(),
            active: true,
            created_by: actor.to_string(),
            created_at: Utc::now(),
            disabled_by: None,
            disabled_at: None,
        };
        crate::notification::validate_endpoint(&endpoint)?;
        self.commit(
            actor,
            vec![DomainEvent::NotificationEndpointRegistered {
                endpoint: endpoint.clone(),
            }],
        )?;
        Ok(endpoint)
    }

    pub fn disable_notification_endpoint(
        &mut self,
        endpoint_id: &str,
        actor: &str,
    ) -> Result<NotificationEndpoint> {
        let endpoint = self
            .state
            .notification_endpoints
            .get(endpoint_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "notification endpoint",
                id: endpoint_id.to_string(),
            })?;
        if !endpoint.active {
            return Err(MambaError::InvalidTransition(format!(
                "notification endpoint {endpoint_id} is already disabled"
            )));
        }
        self.commit(
            actor,
            vec![DomainEvent::NotificationEndpointDisabled {
                endpoint_id: endpoint_id.to_string(),
                disabled_by: actor.to_string(),
                disabled_at: Utc::now(),
            }],
        )?;
        Ok(self.state.notification_endpoints[endpoint_id].clone())
    }

    pub fn notification_attempts(
        &self,
        limit: usize,
        force_failed: bool,
    ) -> Vec<(NotificationEndpoint, NotificationDelivery)> {
        let now = Utc::now();
        let mut deliveries = self
            .state
            .notification_deliveries
            .values()
            .filter(|delivery| {
                matches!(
                    delivery.status,
                    NotificationStatus::Pending | NotificationStatus::Failed
                )
            })
            .filter(|delivery| {
                force_failed
                    || delivery.status == NotificationStatus::Pending
                    || delivery.last_attempt_at.is_none_or(|attempted_at| {
                        let exponent = delivery.attempts.min(8);
                        let delay = Duration::seconds(15 * (1_i64 << exponent));
                        attempted_at + delay <= now
                    })
            })
            .filter_map(|delivery| {
                let endpoint = self
                    .state
                    .notification_endpoints
                    .get(&delivery.endpoint_id)?;
                endpoint
                    .active
                    .then_some((endpoint.clone(), delivery.clone()))
            })
            .collect::<Vec<_>>();
        deliveries.sort_by(|left, right| {
            left.1
                .queued_at
                .cmp(&right.1.queued_at)
                .then_with(|| left.1.id.cmp(&right.1.id))
        });
        deliveries.truncate(limit);
        deliveries
    }

    pub fn record_notification_attempt(
        &mut self,
        delivery_id: &str,
        attempt: NotificationAttempt,
        actor: &str,
    ) -> Result<NotificationDelivery> {
        let delivery = self
            .state
            .notification_deliveries
            .get(delivery_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "notification delivery",
                id: delivery_id.to_string(),
            })?;
        if matches!(
            delivery.status,
            NotificationStatus::Delivered | NotificationStatus::Cancelled
        ) {
            return Err(MambaError::InvalidTransition(format!(
                "notification delivery {delivery_id} is already delivered"
            )));
        }
        let event = if attempt.delivered {
            DomainEvent::NotificationDelivered {
                delivery_id: delivery_id.to_string(),
                flow_id: delivery.flow_id.clone(),
                response_status: attempt.response_status.unwrap_or(200),
                delivered_at: attempt.attempted_at,
            }
        } else {
            DomainEvent::NotificationFailed {
                delivery_id: delivery_id.to_string(),
                flow_id: delivery.flow_id.clone(),
                response_status: attempt.response_status,
                error: attempt
                    .error
                    .unwrap_or_else(|| "notification delivery failed".into()),
                attempted_at: attempt.attempted_at,
            }
        };
        self.commit(actor, vec![event])?;
        Ok(self.state.notification_deliveries[delivery_id].clone())
    }

    pub async fn dispatch_notifications(
        &mut self,
        limit: usize,
        force_failed: bool,
        actor: &str,
    ) -> Result<NotificationDispatchSummary> {
        if limit == 0 || limit > 1_000 {
            return Err(MambaError::Validation(
                "notification dispatch limit must be between 1 and 1000".into(),
            ));
        }
        let attempts = self.notification_attempts(limit, force_failed);
        let mut summary = NotificationDispatchSummary::default();
        for (endpoint, delivery) in attempts {
            let attempt = crate::notification::deliver(&endpoint, &delivery).await;
            summary.attempted += 1;
            if attempt.delivered {
                summary.delivered += 1;
            } else {
                summary.failed += 1;
            }
            self.record_notification_attempt(&delivery.id, attempt, actor)?;
        }
        Ok(summary)
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
                crate::domain::AssignmentTarget {
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
                crate::domain::AssignmentTarget {
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

    pub fn acknowledge_flow_message(
        &mut self,
        message_id: &str,
        actor: &str,
    ) -> Result<FlowMessage> {
        let principal = self.state.principal(actor)?.clone();
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
        let represented = self.message_recipient_ids(&message, &principal);
        if represented.is_empty() {
            return Err(MambaError::PermissionDenied(format!(
                "{} is not a recipient of flow message {}",
                principal.name, message.id
            )));
        }
        let at = Utc::now();
        let acknowledgements = represented
            .into_iter()
            .filter(|recipient_id| !message.recipient_is_acknowledged(recipient_id))
            .map(|recipient_id| MessageAcknowledgement {
                recipient_id,
                acknowledged_by_id: principal.id.clone(),
                acknowledged_by_name: principal.name.clone(),
                acknowledged_at: at,
            })
            .collect::<Vec<_>>();
        if acknowledgements.is_empty() {
            return Ok(message);
        }
        self.commit(
            &principal.name,
            vec![DomainEvent::FlowMessageAcknowledged {
                flow_id: message.flow_id,
                message_id: message.id.clone(),
                acknowledgements,
            }],
        )?;
        Ok(self.state.messages[message_id].clone())
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
        let lease = FlightLease {
            id: new_id("LEASE"),
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
        };
        self.commit(
            &human.name,
            vec![DomainEvent::RemoteFlightAuthorized {
                lease: lease.clone(),
            }],
        )?;
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
        self.commit(
            &principal.name,
            vec![DomainEvent::RemoteFlightRevoked {
                flow_id: lease.flow_id,
                task_id: lease.task_id,
                lease_id: lease.id.clone(),
                revoked_by: principal.name.clone(),
                revoked_at,
            }],
        )?;
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
        report: RemoteFlightReport,
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
        if matches!(
            lease.status,
            FlightLeaseStatus::Landed | FlightLeaseStatus::Crashed
        ) && lease.report.as_ref() == Some(&report)
            && landed == (lease.status == FlightLeaseStatus::Landed)
        {
            return Ok(lease);
        }
        if lease.status != FlightLeaseStatus::Active {
            return Err(MambaError::InvalidTransition(format!(
                "flight lease {} is {:?}, expected active",
                lease.id, lease.status
            )));
        }
        validate_remote_flight_report(&lease, &report)?;
        let finished_at = Utc::now();
        let evidence = Evidence {
            id: new_id("EVD"),
            kind: if landed && report.patch_sha256.is_some() {
                "remote_patch"
            } else if landed {
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
                landed,
                report: report.clone(),
                finished_at,
            },
            DomainEvent::EvidenceAdded {
                flow_id: lease.flow_id.clone(),
                task_id: lease.task_id.clone(),
                evidence,
            },
        ];
        if landed {
            events.push(DomainEvent::TaskHeartbeat {
                flow_id: lease.flow_id,
                task_id: lease.task_id,
                actor: principal.name.clone(),
                note: Some(format!(
                    "remote flight {} landed for Human review",
                    lease.id
                )),
                at: finished_at,
            });
        } else {
            events.push(DomainEvent::TaskBlocked {
                flow_id: lease.flow_id,
                task_id: lease.task_id,
                actor: principal.name.clone(),
                reason: format!("remote execution flight crashed: {}", report.summary),
                at: finished_at,
            });
        }
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

    fn principal_has_flow_access(&self, flow: &Flow, principal: &Principal) -> bool {
        self.principal_is_flow_participant(flow, principal)
            || self.state.messages.values().any(|message| {
                message.flow_id == flow.id
                    && (message.sender_id == principal.id
                        || !self.message_recipient_ids(message, principal).is_empty())
            })
    }

    fn principal_is_flow_participant(&self, flow: &Flow, principal: &Principal) -> bool {
        flow.demand.requester == principal.id
            || flow.demand.requester == principal.name
            || flow
                .tasks
                .iter()
                .any(|task| self.principal_is_task_actor(task, principal))
    }

    fn principal_is_task_actor(&self, task: &Task, principal: &Principal) -> bool {
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

    fn message_target_is_flow_participant(
        &self,
        flow: &Flow,
        target: &crate::domain::AssignmentTarget,
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

    fn message_recipient_ids(&self, message: &FlowMessage, principal: &Principal) -> Vec<String> {
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

    fn ensure_task_actor(&self, task: &Task, actor: &str) -> Result<()> {
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
        mut events: Vec<DomainEvent>,
    ) -> Result<Vec<EventEnvelope>> {
        let queued =
            crate::notification::queue_events(&self.state, organization_id, actor, &events)?;
        events.extend(queued);
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
        };
        let landed = app
            .finish_remote_flight(&lease.id, &agent.name, true, report.clone())
            .unwrap();
        assert_eq!(landed.status, FlightLeaseStatus::Landed);
        assert_eq!(landed.report, Some(report));
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
        assert!(
            app.claim_remote_flight(&revoked.id, &agent.name, "WRUN-revoked")
                .is_err()
        );
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
