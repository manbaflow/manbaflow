use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::{FlightLeaseStatus, FlowStatus, NotificationStatus, TaskStatus};
use crate::state::OrganizationState;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardSnapshot {
    pub generated_at: DateTime<Utc>,
    pub metrics: DashboardMetrics,
    pub flows: Vec<DashboardFlow>,
    pub action_items: Vec<DashboardAction>,
    pub flights: Vec<DashboardFlight>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardFlight {
    pub id: String,
    pub flow_id: String,
    pub task_id: String,
    pub principal: String,
    pub executor: String,
    pub status: String,
    pub summary: Option<String>,
    pub updated_at: DateTime<Utc>,
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
        .map(|lease| DashboardFlight {
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
            }
        }))
        .collect::<Vec<_>>();
    flights.sort_by_key(|flight| std::cmp::Reverse(flight.updated_at));

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
    let awaiting_human = state
        .flows
        .values()
        .flat_map(|flow| &flow.tasks)
        .filter(|task| task.status == TaskStatus::Submitted)
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
        },
        flows,
        action_items,
        flights,
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
