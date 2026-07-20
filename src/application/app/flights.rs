use std::collections::BTreeSet;

use chrono::{Duration, Utc};

use super::MambaApp;
use crate::domain::{
    ExecutorKind, FlightLease, FlightLeaseStatus, FlightManifest, FlightManifestDraft,
    FlightRecoveryDecision, Flow, FuelBudget, Principal, PrincipalKind, RecoveryAction,
    RecoveryPolicy, ResourceClaim, ResourceKind, ResourceLease, ResourceLeaseStatus, Task,
    ToolAccess, ToolPermission,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl MambaApp {
    pub(super) fn build_flight_manifest(
        &self,
        flow: &Flow,
        task: &Task,
        human: &Principal,
        ttl_seconds: u64,
        draft: FlightManifestDraft,
    ) -> Result<FlightManifest> {
        let objective = draft
            .objective
            .unwrap_or_else(|| format!("{}: {}", task.title, task.description));
        validate_text(&objective, "flight objective", 4_000)?;

        let landing_conditions = if draft.landing_conditions.is_empty() {
            task.acceptance_criteria.clone()
        } else {
            draft.landing_conditions
        };
        validate_list(&landing_conditions, "landing conditions", 32, 1_000)?;

        let mut context_refs = if draft.context_refs.is_empty() {
            let mut refs = vec![
                format!("flow://{}/prd", flow.id),
                format!("task://{}", task.id),
            ];
            refs.extend(
                self.state
                    .messages
                    .values()
                    .filter(|message| {
                        message.flow_id == flow.id
                            && message
                                .task_id
                                .as_deref()
                                .is_none_or(|task_id| task_id == task.id)
                    })
                    .map(|message| format!("message://{}", message.id)),
            );
            refs
        } else {
            draft.context_refs
        };
        context_refs.sort();
        context_refs.dedup();
        validate_list(&context_refs, "context references", 128, 2_000)?;

        let tool_permissions = if draft.tool_permissions.is_empty() {
            vec![
                ToolPermission {
                    tool: "filesystem".into(),
                    access: ToolAccess::Write,
                },
                ToolPermission {
                    tool: "git".into(),
                    access: ToolAccess::Execute,
                },
            ]
        } else {
            draft.tool_permissions
        };
        if tool_permissions.len() > 64 {
            return Err(MambaError::Validation(
                "a flight can declare at most 64 tool permissions".into(),
            ));
        }
        for permission in &tool_permissions {
            validate_text(&permission.tool, "tool permission", 100)?;
        }

        let fuel = match draft.fuel {
            Some(fuel) => fuel,
            None => FuelBudget {
                max_duration_seconds: ttl_seconds,
                ..FuelBudget::default()
            },
        };
        validate_fuel(&fuel, ttl_seconds)?;

        let recovery = draft.recovery.unwrap_or_default();
        if !(1..=10).contains(&recovery.max_attempts) || recovery.allowed_actions.is_empty() {
            return Err(MambaError::Validation(
                "recovery policy requires 1 to 10 attempts and at least one action".into(),
            ));
        }
        let mut seen_actions = BTreeSet::new();
        if recovery
            .allowed_actions
            .iter()
            .any(|action| !seen_actions.insert(*action as u8))
        {
            return Err(MambaError::Validation(
                "recovery policy contains duplicate actions".into(),
            ));
        }

        let resources = if draft.resources.is_empty() {
            vec![ResourceClaim {
                kind: ResourceKind::Workspace,
                key: task.id.clone(),
                exclusive: true,
            }]
        } else {
            draft.resources
        };
        validate_resources(&resources)?;

        Ok(FlightManifest {
            id: new_id("MANIFEST"),
            objective,
            landing_conditions,
            context_refs,
            tool_permissions,
            fuel,
            recovery,
            resources,
            declared_by: human.name.clone(),
            declared_at: Utc::now(),
        })
    }

    pub(super) fn ensure_resource_claims_available(
        &self,
        claims: &[ResourceClaim],
        now: chrono::DateTime<Utc>,
    ) -> Result<()> {
        for claim in claims {
            if let Some(existing) = self
                .state
                .resource_leases
                .values()
                .find(|lease| lease.conflicts_with(claim, now))
            {
                return Err(MambaError::InvalidTransition(format!(
                    "resource {:?}:{} is leased by flight {} until {}",
                    claim.kind, claim.key, existing.flight_lease_id, existing.expires_at
                )));
            }
        }
        Ok(())
    }

    pub(super) fn resource_acquisition_events(
        &self,
        flight: &FlightLease,
        claims: &[ResourceClaim],
    ) -> Vec<DomainEvent> {
        claims
            .iter()
            .map(|claim| DomainEvent::ResourceLeaseAcquired {
                lease: ResourceLease {
                    id: new_id("RESOURCE"),
                    flight_lease_id: flight.id.clone(),
                    flow_id: flight.flow_id.clone(),
                    task_id: flight.task_id.clone(),
                    principal_id: flight.principal_id.clone(),
                    claim: claim.clone(),
                    status: ResourceLeaseStatus::Active,
                    issued_at: flight.issued_at,
                    expires_at: flight.expires_at,
                    released_at: None,
                    release_reason: None,
                },
            })
            .collect()
    }

    pub(super) fn resource_release_events(
        &self,
        flight: &FlightLease,
        at: chrono::DateTime<Utc>,
        reason: &str,
    ) -> Vec<DomainEvent> {
        self.state
            .resource_leases
            .values()
            .filter(|lease| {
                lease.flight_lease_id == flight.id && lease.status == ResourceLeaseStatus::Active
            })
            .map(|lease| DomainEvent::ResourceLeaseReleased {
                flow_id: flight.flow_id.clone(),
                task_id: flight.task_id.clone(),
                resource_lease_id: lease.id.clone(),
                released_at: at,
                reason: reason.to_string(),
            })
            .collect()
    }

    pub(super) fn fuel_exhaustions(
        &self,
        lease: &FlightLease,
        report: &crate::domain::RemoteFlightReport,
    ) -> Vec<String> {
        let Some(manifest) = &lease.manifest else {
            return Vec::new();
        };
        let budget = &manifest.fuel;
        let measured_duration = report
            .finished_at
            .signed_duration_since(report.started_at)
            .num_seconds()
            .max(0) as u64;
        let duration = report.fuel.duration_seconds.max(measured_duration);
        let mut exhausted = Vec::new();
        if duration > budget.max_duration_seconds {
            exhausted.push(format!(
                "duration {duration}s exceeded {}s",
                budget.max_duration_seconds
            ));
        }
        if report.fuel.context_bytes > budget.max_context_bytes {
            exhausted.push(format!(
                "context {} bytes exceeded {} bytes",
                report.fuel.context_bytes, budget.max_context_bytes
            ));
        }
        if let (Some(used), Some(limit)) = (report.fuel.tokens, budget.max_tokens)
            && used > limit
        {
            exhausted.push(format!("tokens {used} exceeded {limit}"));
        }
        if let (Some(used), Some(limit)) = (report.fuel.tool_calls, budget.max_tool_calls)
            && used > limit
        {
            exhausted.push(format!("tool calls {used} exceeded {limit}"));
        }
        if let (Some(used), Some(limit)) = (report.fuel.cost_usd, budget.max_cost_usd)
            && used > limit
        {
            exhausted.push(format!("cost ${used:.4} exceeded ${limit:.4}"));
        }
        exhausted
    }

    pub fn recovery_options(&self, lease_id: &str, actor: &str) -> Result<Vec<RecoveryAction>> {
        let principal = self.state.principal(actor)?;
        let lease = self
            .state
            .flight_leases
            .get(lease_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "flight lease",
                id: lease_id.to_string(),
            })?;
        if principal.id != lease.principal_id
            && principal.name != lease.authorized_by
            && !self.state.flows.get(&lease.flow_id).is_some_and(|flow| {
                flow.demand.requester == principal.id || flow.demand.requester == principal.name
            })
        {
            return Err(MambaError::PermissionDenied(
                "principal cannot inspect recovery options for this flight".into(),
            ));
        }
        let allowed = lease
            .manifest
            .as_ref()
            .map(|manifest| manifest.recovery.allowed_actions.clone())
            .unwrap_or_else(|| RecoveryPolicy::default().allowed_actions);
        let recommended = recommendations_for(
            lease
                .report
                .as_ref()
                .and_then(|report| report.failure_class),
        );
        Ok(recommended
            .into_iter()
            .filter(|action| allowed.contains(action))
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn recover_remote_flight(
        &mut self,
        lease_id: &str,
        actor: &str,
        action: RecoveryAction,
        reason: &str,
        executor: Option<ExecutorKind>,
        objective: Option<String>,
        ttl_seconds: u64,
    ) -> Result<Option<FlightLease>> {
        let reason = reason.trim();
        validate_text(reason, "recovery reason", 1_000)?;
        if !(60..=86_400).contains(&ttl_seconds) {
            return Err(MambaError::Validation(
                "recovery lease TTL must be between 60 and 86400 seconds".into(),
            ));
        }
        let human = self.state.principal(actor)?.clone();
        if human.kind != PrincipalKind::Human {
            return Err(MambaError::PermissionDenied(
                "flight recovery requires a Human decision".into(),
            ));
        }
        let parent = self
            .state
            .flight_leases
            .get(lease_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "flight lease",
                id: lease_id.to_string(),
            })?;
        let is_requester = self.state.flows.get(&parent.flow_id).is_some_and(|flow| {
            flow.demand.requester == human.id || flow.demand.requester == human.name
        });
        if parent.authorized_by != human.name && !is_requester {
            return Err(MambaError::PermissionDenied(
                "only the authorizing Human or Demand Requester can recover a flight".into(),
            ));
        }
        if action == RecoveryAction::Fork {
            if !parent.status.is_terminal() {
                return Err(MambaError::InvalidTransition(
                    "a fork requires a terminal parent flight".into(),
                ));
            }
        } else if parent.status != FlightLeaseStatus::Crashed {
            return Err(MambaError::InvalidTransition(
                "only a crashed flight can enter recovery".into(),
            ));
        }
        let parent_manifest = parent.manifest.clone().unwrap_or_else(|| FlightManifest {
            id: new_id("MANIFEST"),
            objective: format!("recover task {}", parent.task_id),
            landing_conditions: vec!["return verifiable evidence".into()],
            context_refs: vec![format!("flight://{}", parent.id)],
            tool_permissions: Vec::new(),
            fuel: FuelBudget::default(),
            recovery: Default::default(),
            resources: vec![ResourceClaim {
                kind: ResourceKind::Workspace,
                key: parent.task_id.clone(),
                exclusive: true,
            }],
            declared_by: human.name.clone(),
            declared_at: Utc::now(),
        });
        if !parent_manifest.recovery.allowed_actions.contains(&action) {
            return Err(MambaError::PermissionDenied(format!(
                "recovery action {action:?} is not allowed by the manifest"
            )));
        }
        if parent.attempt >= parent_manifest.recovery.max_attempts
            && !matches!(
                action,
                RecoveryAction::Ground | RecoveryAction::HumanHandoff
            )
        {
            return Err(MambaError::InvalidTransition(format!(
                "flight recovery exhausted {} attempts",
                parent_manifest.recovery.max_attempts
            )));
        }

        let creates_child = !matches!(
            action,
            RecoveryAction::Ground | RecoveryAction::HumanHandoff
        );
        let child_id = creates_child.then(|| new_id("LEASE"));
        let decision = FlightRecoveryDecision {
            id: new_id("RECOVERY"),
            parent_lease_id: parent.id.clone(),
            child_lease_id: child_id.clone(),
            action,
            reason: reason.to_string(),
            decided_by: human.name.clone(),
            decided_at: Utc::now(),
        };
        let mut events = vec![DomainEvent::FlightRecoveryDecided {
            flow_id: parent.flow_id.clone(),
            task_id: parent.task_id.clone(),
            decision,
        }];
        let Some(child_id) = child_id else {
            self.commit(&human.name, events)?;
            return Ok(None);
        };

        let (flow, task) = self.task_snapshot(&parent.task_id)?;
        let worker = self.state.principal(&parent.principal_id)?.clone();
        let draft = FlightManifestDraft {
            objective: objective.or(Some(parent_manifest.objective)),
            landing_conditions: parent_manifest.landing_conditions,
            context_refs: {
                let mut refs = parent_manifest.context_refs;
                refs.push(format!("flight://{}", parent.id));
                refs
            },
            tool_permissions: parent_manifest.tool_permissions,
            fuel: Some(parent_manifest.fuel),
            recovery: Some(parent_manifest.recovery),
            resources: parent_manifest.resources,
        };
        let manifest = self.build_flight_manifest(&flow, &task, &human, ttl_seconds, draft)?;
        let now = Utc::now();
        self.ensure_resource_claims_available(&manifest.resources, now)?;
        let selected_executor = match action {
            RecoveryAction::SwitchExecutor => {
                let selected = executor.ok_or_else(|| {
                    MambaError::Validation("switch_executor requires an executor".into())
                })?;
                if selected == parent.executor {
                    return Err(MambaError::Validation(
                        "switch_executor must select a different executor".into(),
                    ));
                }
                selected
            }
            _ => executor.unwrap_or(parent.executor),
        };
        let child = FlightLease {
            id: child_id,
            flow_id: parent.flow_id,
            task_id: parent.task_id,
            principal_id: worker.id,
            principal_name: worker.name,
            authorized_by: human.name.clone(),
            executor: selected_executor,
            status: FlightLeaseStatus::Authorized,
            issued_at: now,
            expires_at: now + Duration::seconds(ttl_seconds as i64),
            claimed_at: None,
            finished_at: None,
            run_id: None,
            report: None,
            manifest: Some(manifest.clone()),
            parent_lease_id: Some(parent.id.clone()),
            root_lease_id: Some(parent.root_lease_id.unwrap_or(parent.id)),
            attempt: parent.attempt + 1,
        };
        events.push(DomainEvent::RemoteFlightAuthorized {
            lease: Box::new(child.clone()),
        });
        events.extend(self.resource_acquisition_events(&child, &manifest.resources));
        self.commit(&human.name, events)?;
        Ok(Some(child))
    }
}

fn validate_text(value: &str, label: &str, max_chars: usize) -> Result<()> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > max_chars || value.chars().any(char::is_control)
    {
        return Err(MambaError::Validation(format!(
            "{label} must contain 1 to {max_chars} printable characters"
        )));
    }
    Ok(())
}

fn validate_list(values: &[String], label: &str, max_items: usize, max_chars: usize) -> Result<()> {
    if values.is_empty() || values.len() > max_items {
        return Err(MambaError::Validation(format!(
            "{label} must contain 1 to {max_items} entries"
        )));
    }
    for value in values {
        validate_text(value, label, max_chars)?;
    }
    Ok(())
}

fn validate_fuel(fuel: &FuelBudget, ttl_seconds: u64) -> Result<()> {
    if fuel.max_duration_seconds == 0 || fuel.max_duration_seconds > ttl_seconds {
        return Err(MambaError::Validation(
            "fuel duration must be positive and no greater than the flight TTL".into(),
        ));
    }
    if fuel.max_context_bytes == 0 || fuel.max_context_bytes > 67_108_864 {
        return Err(MambaError::Validation(
            "fuel context budget must be between 1 byte and 64 MiB".into(),
        ));
    }
    if fuel.max_tokens == Some(0) || fuel.max_tool_calls == Some(0) {
        return Err(MambaError::Validation(
            "optional token and tool-call budgets must be greater than zero".into(),
        ));
    }
    if fuel
        .max_cost_usd
        .is_some_and(|value| !value.is_finite() || value <= 0.0 || value > 100_000.0)
    {
        return Err(MambaError::Validation(
            "fuel cost budget must be positive and at most 100000 USD".into(),
        ));
    }
    Ok(())
}

fn validate_resources(resources: &[ResourceClaim]) -> Result<()> {
    if resources.is_empty() || resources.len() > 32 {
        return Err(MambaError::Validation(
            "a flight must claim between 1 and 32 resources".into(),
        ));
    }
    let mut seen = BTreeSet::new();
    for resource in resources {
        validate_text(&resource.key, "resource key", 512)?;
        if !seen.insert((resource.kind, resource.key.clone())) {
            return Err(MambaError::Validation(format!(
                "duplicate resource claim {:?}:{}",
                resource.kind, resource.key
            )));
        }
    }
    Ok(())
}

fn recommendations_for(failure: Option<crate::domain::FailureClass>) -> Vec<RecoveryAction> {
    use crate::domain::FailureClass;
    match failure.unwrap_or_default() {
        FailureClass::Timeout => vec![RecoveryAction::Retry, RecoveryAction::SwitchExecutor],
        FailureClass::Permission => vec![RecoveryAction::HumanHandoff, RecoveryAction::Ground],
        FailureClass::Resource => vec![RecoveryAction::Retry, RecoveryAction::ReduceScope],
        FailureClass::Budget => vec![RecoveryAction::ReduceScope, RecoveryAction::HumanHandoff],
        FailureClass::Validation => vec![RecoveryAction::ReduceScope, RecoveryAction::Ground],
        FailureClass::Tool => vec![RecoveryAction::SwitchExecutor, RecoveryAction::Retry],
        FailureClass::Unknown => vec![
            RecoveryAction::Retry,
            RecoveryAction::SwitchExecutor,
            RecoveryAction::HumanHandoff,
        ],
    }
}
