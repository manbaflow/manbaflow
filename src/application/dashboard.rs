use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::{
    CapabilityPack, FailureClass, FlightLeaseStatus, FlowStatus, GitLabWritePayload,
    GitLabWriteStatus, NotificationStatus, OfficeReleasePayload, OfficeReleaseStatus,
    ResourceLeaseStatus, TaskStatus,
};
use crate::state::OrganizationState;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DashboardSnapshot {
    pub generated_at: DateTime<Utc>,
    pub metrics: DashboardMetrics,
    pub flows: Vec<DashboardFlow>,
    pub action_items: Vec<DashboardAction>,
    pub flights: Vec<DashboardFlight>,
    pub office_releases: Vec<DashboardOfficeRelease>,
    pub gitlab_writes: Vec<DashboardGitLabWrite>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardMetrics {
    pub total_flows: usize,
    pub active_flows: usize,
    pub total_tasks: usize,
    pub completed_tasks: usize,
    pub at_risk_tasks: usize,
    pub blocked_tasks: usize,
    pub awaiting_human: usize,
    pub active_attentions: usize,
    pub open_flights: usize,
    pub pending_notifications: usize,
    pub failed_notifications: usize,
    pub pending_office_releases: usize,
    pub indeterminate_office_releases: usize,
    pub pending_gitlab_writes: usize,
    pub indeterminate_gitlab_writes: usize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum FlowHealth {
    Blocked,
    AtRisk,
    WaitingHuman,
    OnTrack,
    Completed,
    Draft,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardFlow {
    pub id: String,
    pub title: String,
    pub requester: String,
    pub status: FlowStatus,
    pub health: FlowHealth,
    pub completed_tasks: usize,
    pub total_tasks: usize,
    pub progress_percent: u8,
    pub active_attentions: usize,
    pub p50_finish: DateTime<Utc>,
    pub p80_finish: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ActionPriority {
    Critical,
    High,
    Normal,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardAction {
    pub flow_id: String,
    pub flow_title: String,
    pub task_id: String,
    pub task_title: String,
    pub owner: String,
    pub status: TaskStatus,
    pub priority: ActionPriority,
    pub reason: String,
    pub p80_finish: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DashboardFlight {
    pub id: String,
    pub flow_id: String,
    pub task_id: String,
    pub principal: String,
    pub executor: String,
    pub status: String,
    pub summary: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub attempt: Option<u32>,
    pub parent_flight_id: Option<String>,
    pub root_flight_id: Option<String>,
    pub manifest_id: Option<String>,
    pub objective: Option<String>,
    pub fuel: Option<DashboardFuel>,
    pub active_resource_leases: usize,
    pub total_resource_claims: usize,
    pub failure_class: Option<FailureClass>,
    pub budget_exhaustions: Vec<String>,
    pub capability_pack: Option<CapabilityPack>,
    pub deliverable_count: usize,
    pub requires_human_release: bool,
    pub contract_violations: Vec<String>,
    pub sandbox_backend: Option<String>,
    pub sandbox_image_id: Option<String>,
    pub sandbox_network: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardOfficeRelease {
    pub id: String,
    pub flow_id: String,
    pub task_id: String,
    pub provider: String,
    pub kind: String,
    pub status: OfficeReleaseStatus,
    pub summary: String,
    pub payload_sha256: String,
    pub requested_by: String,
    pub requested_at: DateTime<Utc>,
    pub reviewed_by: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardGitLabWrite {
    pub id: String,
    pub flow_id: String,
    pub task_id: String,
    pub project: String,
    pub kind: String,
    pub status: GitLabWriteStatus,
    pub summary: String,
    pub payload_sha256: String,
    pub requested_by: String,
    pub requested_at: DateTime<Utc>,
    pub reviewed_by: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DashboardFuel {
    pub duration_used_seconds: u64,
    pub duration_budget_seconds: u64,
    pub context_used_bytes: u64,
    pub context_budget_bytes: u64,
    pub tokens_used: Option<u64>,
    pub tokens_budget: Option<u64>,
    pub tool_calls_used: Option<u64>,
    pub tool_calls_budget: Option<u64>,
    pub cost_used_usd: Option<f64>,
    pub cost_budget_usd: Option<f64>,
}

pub fn build_dashboard(state: &OrganizationState) -> DashboardSnapshot {
    let now = Utc::now();
    let mut active_attention_tasks = state
        .active_attentions()
        .map(|attention| (attention.flow_id.as_str(), attention.task_id.as_str()))
        .collect::<BTreeSet<_>>();
    active_attention_tasks.extend(state.flows.values().flat_map(|flow| {
        flow.tasks
            .iter()
            .filter(|task| task.status == TaskStatus::Blocked)
            .map(|task| (flow.id.as_str(), task.id.as_str()))
    }));
    let mut flows = state
        .flows
        .values()
        .map(|flow| {
            let completed_tasks = flow
                .tasks
                .iter()
                .filter(|task| task.status == TaskStatus::Completed)
                .count();
            let blocked = flow
                .tasks
                .iter()
                .any(|task| task.status == TaskStatus::Blocked);
            let waiting_human = flow
                .tasks
                .iter()
                .any(|task| task.status == TaskStatus::Submitted);
            let active_attentions = state
                .active_attentions()
                .filter(|attention| attention.flow_id == flow.id)
                .count();
            let health = match flow.status {
                FlowStatus::Completed => FlowHealth::Completed,
                FlowStatus::Draft => FlowHealth::Draft,
                FlowStatus::Cancelled => FlowHealth::Cancelled,
                FlowStatus::Approved | FlowStatus::Active if blocked => FlowHealth::Blocked,
                FlowStatus::Approved | FlowStatus::Active if active_attentions > 0 => {
                    FlowHealth::AtRisk
                }
                FlowStatus::Approved | FlowStatus::Active if waiting_human => {
                    FlowHealth::WaitingHuman
                }
                FlowStatus::Approved | FlowStatus::Active => FlowHealth::OnTrack,
            };
            let progress_percent = if flow.tasks.is_empty() {
                0
            } else {
                ((completed_tasks * 100) / flow.tasks.len()) as u8
            };
            DashboardFlow {
                id: flow.id.clone(),
                title: flow.prd.title.clone(),
                requester: flow.demand.requester.clone(),
                status: flow.status.clone(),
                health,
                completed_tasks,
                total_tasks: flow.tasks.len(),
                progress_percent,
                active_attentions,
                p50_finish: flow.p50_finish,
                p80_finish: flow.p80_finish,
            }
        })
        .collect::<Vec<_>>();
    flows.sort_by(|left, right| {
        flow_health_rank(left.health)
            .cmp(&flow_health_rank(right.health))
            .then_with(|| left.p80_finish.cmp(&right.p80_finish))
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut action_items = state
        .flows
        .values()
        .flat_map(|flow| {
            flow.tasks.iter().filter_map(|task| {
                let attention =
                    active_attention_tasks.contains(&(flow.id.as_str(), task.id.as_str()));
                let (priority, reason) = match task.status {
                    TaskStatus::Blocked => (
                        ActionPriority::Critical,
                        format!(
                            "请求塔台协防：{}",
                            task.blocker.as_deref().unwrap_or("未提供阻塞原因")
                        ),
                    ),
                    TaskStatus::Submitted => (
                        ActionPriority::High,
                        "交付已提交，等待需求发起人验收".into(),
                    ),
                    TaskStatus::Assigned => {
                        (ActionPriority::High, "WorkRequest 已发出，等待接单".into())
                    }
                    TaskStatus::Accepted => {
                        (ActionPriority::Normal, "已接单，等待依赖完成或开工".into())
                    }
                    TaskStatus::InProgress if attention => {
                        (ActionPriority::High, "执行任务需要管理者关注".into())
                    }
                    TaskStatus::InProgress => (
                        ActionPriority::Normal,
                        "执行中，等待下一次 Heartbeat".into(),
                    ),
                    TaskStatus::Proposed
                    | TaskStatus::Completed
                    | TaskStatus::Rejected
                    | TaskStatus::Cancelled => return None,
                };
                let owner = task
                    .assignment
                    .as_ref()
                    .map(|assignment| assignment.owner.name.clone())
                    .unwrap_or_else(|| "未分配".into());
                Some(DashboardAction {
                    flow_id: flow.id.clone(),
                    flow_title: flow.prd.title.clone(),
                    task_id: task.id.clone(),
                    task_title: task.title.clone(),
                    owner,
                    status: task.status.clone(),
                    priority,
                    reason,
                    p80_finish: task.estimate.p80_finish,
                })
            })
        })
        .collect::<Vec<_>>();
    action_items.sort_by(|left, right| {
        action_rank(left.priority)
            .cmp(&action_rank(right.priority))
            .then_with(|| left.p80_finish.cmp(&right.p80_finish))
            .then_with(|| left.task_id.cmp(&right.task_id))
    });

    let mut flights = state
        .flight_leases
        .values()
        .map(|lease| {
            let usage = lease.report.as_ref().map(|report| &report.fuel);
            let fuel = lease.manifest.as_ref().map(|manifest| DashboardFuel {
                duration_used_seconds: usage.map_or(0, |fuel| fuel.duration_seconds),
                duration_budget_seconds: manifest.fuel.max_duration_seconds,
                context_used_bytes: usage.map_or(0, |fuel| fuel.context_bytes),
                context_budget_bytes: manifest.fuel.max_context_bytes,
                tokens_used: usage.and_then(|fuel| fuel.tokens),
                tokens_budget: manifest.fuel.max_tokens,
                tool_calls_used: usage.and_then(|fuel| fuel.tool_calls),
                tool_calls_budget: manifest.fuel.max_tool_calls,
                cost_used_usd: usage.and_then(|fuel| fuel.cost_usd),
                cost_budget_usd: manifest.fuel.max_cost_usd,
            });
            DashboardFlight {
                id: lease.id.clone(),
                flow_id: lease.flow_id.clone(),
                task_id: lease.task_id.clone(),
                principal: lease.principal_name.clone(),
                executor: lease.executor.to_string(),
                status: format!("{:?}", lease.status).to_lowercase(),
                summary: lease.report.as_ref().map(|report| report.summary.clone()),
                updated_at: lease
                    .finished_at
                    .or(lease.claimed_at)
                    .unwrap_or(lease.issued_at),
                attempt: Some(lease.attempt),
                parent_flight_id: lease.parent_lease_id.clone(),
                root_flight_id: lease.root_lease_id.clone(),
                manifest_id: lease.manifest.as_ref().map(|manifest| manifest.id.clone()),
                objective: lease
                    .manifest
                    .as_ref()
                    .map(|manifest| manifest.objective.clone()),
                fuel,
                active_resource_leases: state
                    .resource_leases
                    .values()
                    .filter(|resource| {
                        resource.flight_lease_id == lease.id
                            && resource.status == ResourceLeaseStatus::Active
                    })
                    .count(),
                total_resource_claims: lease
                    .manifest
                    .as_ref()
                    .map_or(0, |manifest| manifest.resources.len()),
                failure_class: lease
                    .report
                    .as_ref()
                    .and_then(|report| report.failure_class),
                budget_exhaustions: lease
                    .report
                    .as_ref()
                    .map(|report| report.budget_exhaustions.clone())
                    .unwrap_or_default(),
                capability_pack: lease
                    .manifest
                    .as_ref()
                    .map(|manifest| manifest.capability_pack),
                deliverable_count: lease
                    .report
                    .as_ref()
                    .map_or(0, |report| report.deliverables.len()),
                requires_human_release: lease
                    .manifest
                    .as_ref()
                    .is_some_and(|manifest| manifest.output_contract.requires_human_release),
                contract_violations: lease
                    .report
                    .as_ref()
                    .map(|report| report.contract_violations.clone())
                    .unwrap_or_default(),
                sandbox_backend: lease
                    .report
                    .as_ref()
                    .and_then(|report| report.sandbox.as_ref())
                    .map(|sandbox| sandbox.backend.clone()),
                sandbox_image_id: lease
                    .report
                    .as_ref()
                    .and_then(|report| report.sandbox.as_ref())
                    .and_then(|sandbox| sandbox.image_id.clone()),
                sandbox_network: lease
                    .report
                    .as_ref()
                    .and_then(|report| report.sandbox.as_ref())
                    .map(|sandbox| sandbox.network.clone()),
            }
        })
        .chain(state.executions.values().map(|record| {
            DashboardFlight {
                id: record.id.clone(),
                flow_id: record.flow_id.clone(),
                task_id: record.task_id.clone(),
                principal: state
                    .principals
                    .get(&record.principal_id)
                    .map(|principal| principal.name.clone())
                    .unwrap_or_else(|| record.principal_id.clone()),
                executor: record.executor.to_string(),
                status: "landed".into(),
                summary: Some(record.summary.clone()),
                updated_at: record.finished_at,
                attempt: None,
                parent_flight_id: None,
                root_flight_id: None,
                manifest_id: None,
                objective: None,
                fuel: None,
                active_resource_leases: 0,
                total_resource_claims: 0,
                failure_class: None,
                budget_exhaustions: Vec::new(),
                capability_pack: None,
                deliverable_count: 0,
                requires_human_release: false,
                contract_violations: Vec::new(),
                sandbox_backend: Some("process".into()),
                sandbox_image_id: None,
                sandbox_network: Some("host".into()),
            }
        }))
        .collect::<Vec<_>>();
    flights.sort_by_key(|flight| std::cmp::Reverse(flight.updated_at));

    let mut office_releases = state
        .office_releases
        .values()
        .map(|release| {
            let (kind, summary) = match &release.payload {
                OfficeReleasePayload::DriveUpload { file_name, .. } => {
                    ("drive_upload", format!("发布文件 {file_name}"))
                }
                OfficeReleasePayload::SendEmail { subject, to, .. } => (
                    "send_email",
                    format!("发送邮件《{subject}》给 {} 人", to.len()),
                ),
                OfficeReleasePayload::CreateCalendarEvent {
                    subject, attendees, ..
                } => (
                    "create_calendar_event",
                    format!("创建日程《{subject}》，{} 位参与者", attendees.len()),
                ),
            };
            DashboardOfficeRelease {
                id: release.id.clone(),
                flow_id: release.flow_id.clone(),
                task_id: release.task_id.clone(),
                provider: format!("{:?}", release.provider).to_lowercase(),
                kind: kind.into(),
                status: release.status,
                summary,
                payload_sha256: release.payload_sha256.clone(),
                requested_by: state
                    .principals
                    .get(&release.requested_by)
                    .map(|principal| principal.name.clone())
                    .unwrap_or_else(|| release.requested_by.clone()),
                requested_at: release.requested_at,
                reviewed_by: release.reviewed_by.as_ref().map(|reviewer| {
                    state
                        .principals
                        .get(reviewer)
                        .map(|principal| principal.name.clone())
                        .unwrap_or_else(|| reviewer.clone())
                }),
                last_error: release.last_error.clone(),
            }
        })
        .collect::<Vec<_>>();
    office_releases.sort_by_key(|release| std::cmp::Reverse(release.requested_at));

    let mut gitlab_writes = state
        .gitlab_writes
        .values()
        .map(|request| {
            let summary = match &request.payload {
                GitLabWritePayload::CreateIssue { title, .. } => {
                    format!("创建 Issue《{title}》")
                }
                GitLabWritePayload::CommentIssue {
                    issue_iid, body, ..
                } => format!("评论 Issue #{issue_iid}: {body}"),
                GitLabWritePayload::CreateMergeRequest {
                    source_branch,
                    target_branch,
                    title,
                    ..
                } => format!("创建 MR《{title}》 {source_branch} -> {target_branch}"),
                GitLabWritePayload::CommentMergeRequest {
                    merge_request_iid,
                    body,
                    ..
                } => format!("评论 MR !{merge_request_iid}: {body}"),
            };
            DashboardGitLabWrite {
                id: request.id.clone(),
                flow_id: request.flow_id.clone(),
                task_id: request.task_id.clone(),
                project: request.payload.project().to_string(),
                kind: request.payload.action_name().into(),
                status: request.status,
                summary,
                payload_sha256: request.payload_sha256.clone(),
                requested_by: state
                    .principals
                    .get(&request.requested_by)
                    .map(|principal| principal.name.clone())
                    .unwrap_or_else(|| request.requested_by.clone()),
                requested_at: request.requested_at,
                reviewed_by: request.reviewed_by.as_ref().map(|reviewer| {
                    state
                        .principals
                        .get(reviewer)
                        .map(|principal| principal.name.clone())
                        .unwrap_or_else(|| reviewer.clone())
                }),
                last_error: request.last_error.clone(),
            }
        })
        .collect::<Vec<_>>();
    gitlab_writes.sort_by_key(|request| std::cmp::Reverse(request.requested_at));

    let total_tasks = state.flows.values().map(|flow| flow.tasks.len()).sum();
    let completed_tasks = state
        .flows
        .values()
        .flat_map(|flow| &flow.tasks)
        .filter(|task| task.status == TaskStatus::Completed)
        .count();
    let blocked_tasks = state
        .flows
        .values()
        .flat_map(|flow| &flow.tasks)
        .filter(|task| task.status == TaskStatus::Blocked)
        .count();
    let awaiting_human_tasks = state
        .flows
        .values()
        .flat_map(|flow| &flow.tasks)
        .filter(|task| task.status == TaskStatus::Submitted)
        .count();
    let awaiting_human = awaiting_human_tasks
        + state
            .office_releases
            .values()
            .filter(|release| release.status == OfficeReleaseStatus::Requested)
            .count()
        + state
            .gitlab_writes
            .values()
            .filter(|request| request.status == GitLabWriteStatus::Requested)
            .count();
    let open_flights = state
        .flight_leases
        .values()
        .filter(|lease| lease.status == FlightLeaseStatus::Active || lease.is_claimable_at(now))
        .count();
    DashboardSnapshot {
        generated_at: now,
        metrics: DashboardMetrics {
            total_flows: state.flows.len(),
            active_flows: state
                .flows
                .values()
                .filter(|flow| matches!(flow.status, FlowStatus::Approved | FlowStatus::Active))
                .count(),
            total_tasks,
            completed_tasks,
            at_risk_tasks: active_attention_tasks.len(),
            blocked_tasks,
            awaiting_human,
            active_attentions: state.active_attentions().count(),
            open_flights,
            pending_notifications: state
                .notification_deliveries
                .values()
                .filter(|delivery| delivery.status == NotificationStatus::Pending)
                .count(),
            failed_notifications: state
                .notification_deliveries
                .values()
                .filter(|delivery| delivery.status == NotificationStatus::Failed)
                .count(),
            pending_office_releases: state
                .office_releases
                .values()
                .filter(|release| release.status == OfficeReleaseStatus::Requested)
                .count(),
            indeterminate_office_releases: state
                .office_releases
                .values()
                .filter(|release| release.status == OfficeReleaseStatus::Indeterminate)
                .count(),
            pending_gitlab_writes: state
                .gitlab_writes
                .values()
                .filter(|request| request.status == GitLabWriteStatus::Requested)
                .count(),
            indeterminate_gitlab_writes: state
                .gitlab_writes
                .values()
                .filter(|request| request.status == GitLabWriteStatus::Indeterminate)
                .count(),
        },
        flows,
        action_items,
        flights,
        office_releases,
        gitlab_writes,
    }
}

fn flow_health_rank(health: FlowHealth) -> u8 {
    match health {
        FlowHealth::Blocked => 0,
        FlowHealth::AtRisk => 1,
        FlowHealth::WaitingHuman => 2,
        FlowHealth::OnTrack => 3,
        FlowHealth::Draft => 4,
        FlowHealth::Completed => 5,
        FlowHealth::Cancelled => 6,
    }
}

fn action_rank(priority: ActionPriority) -> u8 {
    match priority {
        ActionPriority::Critical => 0,
        ActionPriority::High => 1,
        ActionPriority::Normal => 2,
    }
}
