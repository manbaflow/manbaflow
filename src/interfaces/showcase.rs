use std::path::Path;

use chrono::Utc;
use serde::Serialize;

use crate::MambaApp;
use crate::domain::{
    CapabilityPack, ExecutorConfig, ExecutorKind, ExternalInteractionAction, FailureClass,
    FlightManifestDraft, Flow, FlowMessageKind, GitLabWritePayload, OfficeBodyType, OfficeProvider,
    OfficeReleasePayload, PrincipalKind, RemoteFlightReport, TargetKind, Task, TaskStatus,
};
use crate::error::{MambaError, Result};
use crate::planner::PlannerKind;

#[derive(Clone, Debug, Serialize)]
pub struct ShowcaseSummary {
    pub highlighted_flow_id: String,
    pub blocked_task_id: String,
    pub in_progress_task_id: String,
    pub waiting_review_task_id: String,
    pub completed_flow_id: String,
    pub command_message_id: Option<String>,
    pub office_release_id: String,
    pub gitlab_write_id: String,
    pub flows: Vec<Flow>,
}

pub async fn bootstrap_showcase(app: &mut MambaApp, workspace: &Path) -> Result<ShowcaseSummary> {
    if app.state().organization.is_some() {
        return Err(MambaError::Validation(
            "Showcase 只能装载到空塔台，请换一个独立的 --data-dir".to_string(),
        ));
    }

    app.init_organization("Mamba Labs", "admin")?;
    let team = app.create_team(
        "洛杉矶研发队",
        "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
        "admin",
    )?;
    let leader = app.register_principal(
        "牢大",
        PrincipalKind::Human,
        Some(&team.id),
        None,
        "product,delivery,llm-platform,operations",
        80,
        None,
        "admin",
    )?;
    let engineer = app.register_principal(
        "佐巴扬",
        PrincipalKind::Human,
        Some(&team.id),
        None,
        "backend,rust,llm-platform,security,quality,observability",
        100,
        None,
        "admin",
    )?;
    let workdays = crate::calendar::parse_workdays("mon,tue,wed,thu,fri")?;
    app.configure_work_calendar(
        &leader.id,
        8 * 60,
        workdays.clone(),
        9 * 60,
        18 * 60,
        "admin",
    )?;
    app.configure_work_calendar(&engineer.id, 8 * 60, workdays, 9 * 60, 18 * 60, "admin")?;
    app.register_principal(
        "Codex 副驾",
        PrincipalKind::Agent,
        Some(&team.id),
        Some(&engineer.id),
        "backend,rust,llm-platform,security,quality,observability",
        100,
        Some(ExecutorConfig {
            kind: ExecutorKind::Codex,
            workspace: workspace.to_path_buf(),
            model: None,
            command: None,
        }),
        "admin",
    )?;
    app.register_principal(
        "Claude Code 副驾",
        PrincipalKind::Agent,
        Some(&team.id),
        Some(&leader.id),
        "product,delivery,llm-platform,operations,backend",
        100,
        Some(ExecutorConfig {
            kind: ExecutorKind::ClaudeCode,
            workspace: workspace.to_path_buf(),
            model: None,
            command: None,
        }),
        "admin",
    )?;

    seed_showcase(app, workspace, &leader.name).await
}

pub async fn seed_showcase(
    app: &mut MambaApp,
    workspace: &Path,
    requester: &str,
) -> Result<ShowcaseSummary> {
    let gateway = app
        .create_demand(
            "本周交付 LLM Gateway v0，统一模型入口、鉴权、可观测性和灰度发布",
            requester,
            PlannerKind::Local,
            workspace,
            30,
        )
        .await?;
    app.approve_flow(&gateway.id, requester)?;
    complete_task(app, &gateway.id, "scope-contract", requester)?;

    let gateway_core_assignment = app
        .state()
        .flow(&gateway.id)?
        .task("gateway-core")
        .cloned()
        .ok_or_else(|| MambaError::NotFound {
            entity: "showcase task",
            id: "gateway-core".into(),
        })?;
    let gateway_actor = task_actor(app, &gateway_core_assignment)?;
    let gateway_principal = app.state().principal(&gateway_actor)?.clone();
    let showcase_external_user = format!("SHOWCASE_{}", gateway_principal.id);
    if app
        .state()
        .external_identity("slack", &showcase_external_user)
        .is_err()
    {
        app.bind_external_identity(
            "slack",
            &showcase_external_user,
            &gateway_principal.id,
            "tower://showcase",
        )?;
    }
    app.process_external_interaction(
        "slack",
        "showcase-gateway-core-accept",
        &showcase_external_user,
        ExternalInteractionAction::TaskAccept,
        &gateway_core_assignment.id,
        None,
    )?;
    let gateway_core = start_task(app, &gateway.id, "gateway-core")?;
    let gateway_actor = task_actor(app, &gateway_core)?;
    app.heartbeat_task(
        &gateway_core.id,
        &gateway_actor,
        Some("Codex 已完成 provider adapter 骨架，正在补 contract tests".into()),
    )?;
    app.add_evidence(
        &gateway_core.id,
        &gateway_actor,
        "agent_plan",
        "worker://showcase/gateway-core/plan",
        "已定位路由、错误模型与 provider adapter 的改动范围",
    )?;
    let (worker, owner, executor) = assigned_agent_and_owner(app, &gateway_core)?;
    app.authorize_remote_flight_with_manifest(
        &gateway_core.id,
        &owner,
        &worker,
        executor,
        3_600,
        FlightManifestDraft {
            capability_pack: Some(CapabilityPack::Coding),
            ..Default::default()
        },
    )?;
    let gitlab_write = app.request_gitlab_write(
        &gateway_core.id,
        GitLabWritePayload::CreateIssue {
            project: "platform/llm-gateway".into(),
            title: "LLM Gateway v0 rollout checklist".into(),
            description:
                "Track contract tests, observability, staged rollout and rollback evidence.".into(),
            labels: vec!["delivery".into(), "mambaflow".into()],
        },
        &gateway_actor,
    )?;

    let auth_policy = start_task(app, &gateway.id, "auth-policy")?;
    let (worker, owner, executor) = assigned_agent_and_owner(app, &auth_policy)?;
    let crashed_lease = app.authorize_remote_flight_with_manifest(
        &auth_policy.id,
        &owner,
        &worker,
        executor.clone(),
        3_600,
        FlightManifestDraft {
            capability_pack: Some(CapabilityPack::Coding),
            ..Default::default()
        },
    )?;
    app.claim_remote_flight(&crashed_lease.id, &worker, "WRUN-showcase-crash")?;
    let now = Utc::now();
    app.finish_remote_flight(
        &crashed_lease.id,
        &worker,
        false,
        RemoteFlightReport {
            run_id: "WRUN-showcase-crash".into(),
            executor,
            summary: "生产 Provider Secret 权限不足，需要 Human 确认轮换边界".into(),
            base_revision: "showcase".into(),
            changed_files: Vec::new(),
            patch_sha256: None,
            log_sha256: "8".repeat(64),
            started_at: now,
            finished_at: now,
            fuel: Default::default(),
            failure_class: Some(FailureClass::Permission),
            budget_exhaustions: Vec::new(),
            deliverables: Vec::new(),
            contract_violations: Vec::new(),
        },
    )?;
    let command_message_id = if app.state().principal("佐巴扬").is_ok() {
        Some(
            app.post_flow_message(
                &gateway.id,
                Some(&auth_policy.id),
                requester,
                FlowMessageKind::Command,
                &["佐巴扬".to_string(), "Codex 副驾".to_string()],
                "确认 Provider Secret 轮换边界，给出生产放行结论；收到后回传塔台",
                true,
            )?
            .id,
        )
    } else {
        None
    };

    let review = app
        .create_demand(
            "准备 Q3 客户发布说明、迁移指南与内部 FAQ",
            requester,
            PlannerKind::Local,
            workspace,
            30,
        )
        .await?;
    app.approve_flow(&review.id, requester)?;
    complete_task(app, &review.id, "clarify", requester)?;
    let deliver = start_task(app, &review.id, "deliver")?;
    let deliver_actor = task_actor(app, &deliver)?;
    app.add_evidence(
        &deliver.id,
        &deliver_actor,
        "document",
        "docs://showcase/q3-release-draft",
        "发布说明、迁移指南和 FAQ 草案已经完成",
    )?;
    app.submit_task(&deliver.id, &deliver_actor)?;
    let office_release = app.request_office_release(
        &deliver.id,
        OfficeProvider::Microsoft365,
        OfficeReleasePayload::SendEmail {
            account_id: "release-owner@mamba.example".into(),
            to: vec!["customers@mamba.example".into()],
            cc: vec!["support@mamba.example".into()],
            bcc: Vec::new(),
            subject: "Q3 客户发布说明".into(),
            body: "发布说明、迁移指南和 FAQ 已完成 Human Review，详见发布包。".into(),
            body_type: OfficeBodyType::Text,
        },
        &deliver_actor,
    )?;

    let completed = app
        .create_demand(
            "完成生产值班手册和故障升级路径",
            requester,
            PlannerKind::Local,
            workspace,
            30,
        )
        .await?;
    app.approve_flow(&completed.id, requester)?;
    for key in ["clarify", "deliver", "review"] {
        complete_task(app, &completed.id, key, requester)?;
    }

    app.scan_tracking_with_policy(24, 4, "tower://showcase")?;
    Ok(ShowcaseSummary {
        highlighted_flow_id: gateway.id.clone(),
        blocked_task_id: auth_policy.id,
        in_progress_task_id: gateway_core.id,
        waiting_review_task_id: deliver.id,
        completed_flow_id: completed.id.clone(),
        command_message_id,
        office_release_id: office_release.id,
        gitlab_write_id: gitlab_write.id,
        flows: [gateway.id, review.id, completed.id]
            .iter()
            .map(|id| app.state().flow(id).cloned())
            .collect::<Result<Vec<_>>>()?,
    })
}

fn start_task(app: &mut MambaApp, flow_id: &str, task_key: &str) -> Result<Task> {
    let task = app
        .state()
        .flow(flow_id)?
        .task(task_key)
        .cloned()
        .ok_or_else(|| MambaError::NotFound {
            entity: "showcase task",
            id: task_key.to_string(),
        })?;
    let actor = task_actor(app, &task)?;
    let task = match task.status {
        TaskStatus::Assigned => app.accept_task(&task.id, &actor)?,
        _ => task,
    };
    match task.status {
        TaskStatus::Accepted | TaskStatus::Blocked => app.start_task(&task.id, &actor),
        TaskStatus::InProgress => Ok(task),
        status => Err(MambaError::InvalidTransition(format!(
            "showcase task {} cannot start from {:?}",
            task.id, status
        ))),
    }
}

fn complete_task(
    app: &mut MambaApp,
    flow_id: &str,
    task_key: &str,
    requester: &str,
) -> Result<Task> {
    let task = start_task(app, flow_id, task_key)?;
    let actor = task_actor(app, &task)?;
    app.add_evidence(
        &task.id,
        &actor,
        "showcase_evidence",
        &format!("demo://{flow_id}/{task_key}"),
        "演示数据：交付条件和验证记录已经归档",
    )?;
    app.submit_task(&task.id, &actor)?;
    app.complete_task(&task.id, requester)
}

fn task_actor(app: &MambaApp, task: &Task) -> Result<String> {
    let assignment = task
        .assignment
        .as_ref()
        .ok_or_else(|| MambaError::NoEligibleAssignee(task.title.clone()))?;
    match assignment.owner.kind {
        TargetKind::Human => Ok(assignment.owner.name.clone()),
        TargetKind::Agent => {
            let agent = app.state().principal(&assignment.owner.id)?;
            let owner_id = agent.owner_id.as_deref().ok_or_else(|| {
                MambaError::Validation(format!("agent {} has no Human owner", agent.name))
            })?;
            Ok(app.state().principal(owner_id)?.name.clone())
        }
        TargetKind::Team => app
            .state()
            .principals
            .values()
            .find(|principal| {
                principal.active
                    && principal.kind == PrincipalKind::Human
                    && principal.team_id.as_deref() == Some(assignment.owner.id.as_str())
            })
            .map(|principal| principal.name.clone())
            .ok_or_else(|| MambaError::NoEligibleAssignee(task.title.clone())),
    }
}

fn assigned_agent_and_owner(app: &MambaApp, task: &Task) -> Result<(String, String, ExecutorKind)> {
    let assignment = task
        .assignment
        .as_ref()
        .ok_or_else(|| MambaError::NoEligibleAssignee(task.title.clone()))?;
    let principal = std::iter::once(&assignment.owner)
        .chain(&assignment.copilots)
        .filter_map(|target| app.state().principals.get(&target.id))
        .find(|principal| principal.kind == PrincipalKind::Agent && principal.owner_id.is_some())
        .ok_or_else(|| {
            MambaError::Validation(format!(
                "showcase task {} has no assigned personal agent",
                task.id
            ))
        })?;
    let owner = app
        .state()
        .principal(principal.owner_id.as_deref().unwrap())?;
    let executor = principal
        .executor
        .as_ref()
        .map(|executor| executor.kind.clone())
        .unwrap_or(ExecutorKind::Codex);
    Ok((principal.name.clone(), owner.name.clone(), executor))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::domain::{ExecutorConfig, PrincipalKind};

    #[tokio::test]
    async fn showcase_contains_progress_risk_review_flight_and_completion() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Mamba Labs", "admin").unwrap();
        let team = app
            .create_team(
                "Platform",
                "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
                "admin",
            )
            .unwrap();
        let human = app
            .register_principal(
                "牢大",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        app.register_principal(
            "Codex 副驾",
            PrincipalKind::Agent,
            Some(&team.id),
            Some(&human.id),
            "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
            100,
            Some(ExecutorConfig {
                kind: ExecutorKind::Codex,
                workspace: directory.path().to_path_buf(),
                model: None,
                command: None,
            }),
            "admin",
        )
        .unwrap();

        let showcase = seed_showcase(&mut app, directory.path(), &human.name)
            .await
            .unwrap();
        let dashboard = app.admin_dashboard(&human.name).unwrap();
        assert_eq!(showcase.flows.len(), 3);
        assert_eq!(dashboard.metrics.total_flows, 3);
        assert_eq!(dashboard.metrics.blocked_tasks, 1);
        assert_eq!(dashboard.metrics.awaiting_human, 3);
        assert_eq!(dashboard.metrics.pending_office_releases, 1);
        assert_eq!(dashboard.office_releases.len(), 1);
        assert_eq!(dashboard.metrics.pending_gitlab_writes, 1);
        assert_eq!(dashboard.gitlab_writes.len(), 1);
        assert_eq!(dashboard.metrics.open_flights, 1);
        assert_eq!(app.state().external_interactions.len(), 1);
        assert_eq!(
            app.state()
                .external_identities
                .values()
                .filter(|binding| binding.is_active())
                .count(),
            1
        );
        assert!(
            dashboard
                .flows
                .iter()
                .any(|flow| flow.health == crate::dashboard::FlowHealth::Completed)
        );
        assert!(
            dashboard
                .action_items
                .first()
                .is_some_and(|action| action.priority == crate::dashboard::ActionPriority::Critical)
        );
    }
}
