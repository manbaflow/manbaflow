use chrono::{DateTime, Duration, Utc};

use crate::domain::{AttentionKind, AttentionSeverity, Flow, FlowStatus, Task, TaskStatus};
use crate::state::OrganizationState;

#[derive(Clone, Debug)]
pub(crate) struct TrackingFinding {
    pub flow_id: String,
    pub task_id: String,
    pub kind: AttentionKind,
    pub severity: AttentionSeverity,
    pub summary: String,
}

pub(crate) fn evaluate(
    state: &OrganizationState,
    now: DateTime<Utc>,
    stale_after: Duration,
) -> Vec<TrackingFinding> {
    let mut findings = Vec::new();
    for flow in state
        .flows
        .values()
        .filter(|flow| matches!(flow.status, FlowStatus::Approved | FlowStatus::Active))
    {
        for task in &flow.tasks {
            if task.status.is_terminal() {
                continue;
            }

            if now > task.estimate.p80_finish {
                findings.push(finding(
                    flow,
                    task,
                    AttentionKind::Overdue,
                    AttentionSeverity::Critical,
                    format!(
                        "{} 已超过 P80 落地时间 {}",
                        task.title,
                        task.estimate.p80_finish.format("%Y-%m-%d %H:%M UTC")
                    ),
                ));
            }

            match task.status {
                TaskStatus::Assigned => {
                    if flow
                        .approved_at
                        .is_some_and(|approved_at| now - approved_at >= stale_after)
                    {
                        findings.push(finding(
                            flow,
                            task,
                            AttentionKind::AcceptanceWaiting,
                            AttentionSeverity::Warning,
                            format!("{} 派发后仍未接单", task.title),
                        ));
                    }
                }
                TaskStatus::Accepted if dependencies_complete(flow, task) => {
                    if heartbeat_stale(task, now, stale_after) {
                        findings.push(finding(
                            flow,
                            task,
                            AttentionKind::StaleHeartbeat,
                            AttentionSeverity::Warning,
                            format!("{} 已可开工但长时间没有新航点", task.title),
                        ));
                    }
                }
                TaskStatus::InProgress => {
                    if heartbeat_stale(task, now, stale_after) {
                        findings.push(finding(
                            flow,
                            task,
                            AttentionKind::StaleHeartbeat,
                            AttentionSeverity::Warning,
                            format!("{} 执行中但长时间没有新航点", task.title),
                        ));
                    }
                }
                TaskStatus::Blocked => findings.push(finding(
                    flow,
                    task,
                    AttentionKind::Blocked,
                    AttentionSeverity::Critical,
                    format!(
                        "{} 请求塔台协防：{}",
                        task.title,
                        task.blocker.as_deref().unwrap_or("未提供原因")
                    ),
                )),
                TaskStatus::Submitted => {
                    if heartbeat_stale(task, now, stale_after) {
                        findings.push(finding(
                            flow,
                            task,
                            AttentionKind::ReviewWaiting,
                            AttentionSeverity::Warning,
                            format!("{} 提交后仍在等待 Human 验收", task.title),
                        ));
                    }
                }
                TaskStatus::Proposed
                | TaskStatus::Accepted
                | TaskStatus::Completed
                | TaskStatus::Rejected
                | TaskStatus::Cancelled => {}
            }
        }
    }
    findings
}

fn finding(
    flow: &Flow,
    task: &Task,
    kind: AttentionKind,
    severity: AttentionSeverity,
    summary: String,
) -> TrackingFinding {
    TrackingFinding {
        flow_id: flow.id.clone(),
        task_id: task.id.clone(),
        kind,
        severity,
        summary,
    }
}

fn heartbeat_stale(task: &Task, now: DateTime<Utc>, stale_after: Duration) -> bool {
    task.last_heartbeat
        .is_some_and(|heartbeat| now - heartbeat >= stale_after)
}

fn dependencies_complete(flow: &Flow, task: &Task) -> bool {
    task.depends_on.iter().all(|task_id| {
        flow.task(task_id)
            .is_some_and(|dependency| dependency.status == TaskStatus::Completed)
    })
}
