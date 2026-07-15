use std::path::{Path, PathBuf};

use schemars::schema_for;
use serde::{Deserialize, Serialize};

use crate::domain::{ExecutorKind, ExecutorMode, PlanDraft, PrdDraft, TaskDraft};
use crate::error::{MambaError, Result};
use crate::executor::{ExecutionRequest, TerminalExecutor};
use crate::ids::normalize_capability;
use crate::state::OrganizationState;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlannerKind {
    Local,
    ClaudeCode,
    Codex,
}

impl std::fmt::Display for PlannerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => write!(f, "local"),
            Self::ClaudeCode => write!(f, "claude-code"),
            Self::Codex => write!(f, "codex"),
        }
    }
}

pub async fn generate_plan(
    planner: PlannerKind,
    demand: &str,
    state: &OrganizationState,
    workspace: &Path,
    log_path: PathBuf,
    timeout_seconds: u64,
) -> Result<PlanDraft> {
    let plan = match planner {
        PlannerKind::Local => local_plan(demand),
        PlannerKind::ClaudeCode | PlannerKind::Codex => {
            let kind = match planner {
                PlannerKind::ClaudeCode => ExecutorKind::ClaudeCode,
                PlannerKind::Codex => ExecutorKind::Codex,
                PlannerKind::Local => unreachable!(),
            };
            let prompt = planner_prompt(demand, state);
            let schema = serde_json::to_value(schema_for!(PlanDraft))?;
            let output = TerminalExecutor::run(ExecutionRequest {
                kind,
                command: None,
                workspace: workspace.to_path_buf(),
                model: None,
                mode: ExecutorMode::Plan,
                prompt,
                output_schema: Some(schema),
                timeout_seconds,
                log_path,
            })
            .await?;
            serde_json::from_value(output.structured_output.ok_or_else(|| {
                MambaError::InvalidExecutorOutput("planner returned no structured output".into())
            })?)?
        }
    };
    validate_plan(plan)
}

fn planner_prompt(demand: &str, state: &OrganizationState) -> String {
    let teams = state
        .teams
        .values()
        .map(|team| format!("- {}: {}", team.name, team.capabilities.join(", ")))
        .collect::<Vec<_>>()
        .join("\n");
    let people = state
        .principals
        .values()
        .map(|principal| {
            format!(
                "- {} ({:?}, capacity {}%): {}",
                principal.name,
                principal.kind,
                principal.capacity_percent,
                principal.capabilities.join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "You are the planning terminal for an enterprise Human-Agent Flow system.\n\
         Turn the manager demand into a concise PRD and a dependency-aware task graph.\n\
         Return only data matching the supplied JSON schema. Task keys must be unique kebab-case.\n\
         Capabilities must be short reusable labels. Estimate hands-on effort in hours, not elapsed duration.\n\
         Mark tasks requiring business judgment, approval, stakeholder contact, or final accountability with requires_human=true.\n\
         Do not assign people: the control plane will match owners from organization data.\n\n\
         Manager demand:\n{demand}\n\n\
         Teams:\n{teams}\n\n\
         People and agents:\n{people}"
    )
}

fn local_plan(demand: &str) -> PlanDraft {
    let lowered = demand.to_lowercase();
    if lowered.contains("llm gateway") || lowered.contains("llm-gateway") {
        return PlanDraft {
            prd: PrdDraft {
                title: "LLM Gateway v0".into(),
                summary: demand.to_string(),
                goals: vec![
                    "provide one authenticated API across configured model providers".into(),
                    "make routing, usage, latency and failures observable".into(),
                    "ship a tested rollout path with an accountable owner".into(),
                ],
                non_goals: vec![
                    "provider-specific feature parity in v0".into(),
                    "a full billing platform".into(),
                ],
                acceptance_criteria: vec![
                    "a client can call at least two configured providers through one API".into(),
                    "requests are authenticated, metered and traceable".into(),
                    "failover behavior and operational ownership are documented".into(),
                ],
            },
            tasks: vec![
                task(
                    "scope-contract",
                    "Confirm product scope and API contract",
                    "Resolve stakeholders, supported providers, API semantics and rollout constraints.",
                    &["product", "llm-platform"],
                    &[],
                    6.0,
                    true,
                    &["API contract and v0 scope are approved"],
                ),
                task(
                    "gateway-core",
                    "Implement gateway core",
                    "Implement the normalized request path, provider adapters and error model.",
                    &["backend", "llm-platform"],
                    &["scope-contract"],
                    20.0,
                    false,
                    &["two provider adapters pass contract tests"],
                ),
                task(
                    "auth-policy",
                    "Add authentication and policy controls",
                    "Add caller authentication, provider credential boundaries and request policy checks.",
                    &["backend", "security"],
                    &["scope-contract"],
                    12.0,
                    false,
                    &["unauthorized calls fail closed and secrets are not logged"],
                ),
                task(
                    "observability",
                    "Add usage and reliability telemetry",
                    "Record request identity, model, latency, token usage, outcome and retry/failover path.",
                    &["observability", "backend"],
                    &["gateway-core"],
                    10.0,
                    false,
                    &["operators can trace a request and aggregate usage"],
                ),
                task(
                    "verification",
                    "Verify reliability and security",
                    "Run contract, load, failure-path and security checks against the integrated gateway.",
                    &["quality", "security"],
                    &["gateway-core", "auth-policy", "observability"],
                    10.0,
                    true,
                    &["release checks pass with evidence attached"],
                ),
                task(
                    "rollout",
                    "Approve and execute rollout",
                    "Review evidence, assign operational ownership and approve the staged release.",
                    &["product", "operations"],
                    &["verification"],
                    4.0,
                    true,
                    &["release owner and rollback plan are confirmed"],
                ),
            ],
        };
    }

    PlanDraft {
        prd: PrdDraft {
            title: demand
                .lines()
                .next()
                .unwrap_or("New initiative")
                .trim()
                .to_string(),
            summary: demand.to_string(),
            goals: vec!["deliver the requested outcome with visible ownership".into()],
            non_goals: vec!["unrequested follow-up work".into()],
            acceptance_criteria: vec![
                "the requester reviews and accepts the delivered outcome".into(),
            ],
        },
        tasks: vec![
            task(
                "clarify",
                "Clarify scope and success criteria",
                "Confirm boundaries, stakeholders, risks and measurable completion conditions.",
                &["product"],
                &[],
                4.0,
                true,
                &["scope and acceptance criteria are approved"],
            ),
            task(
                "deliver",
                "Produce the requested deliverable",
                demand,
                &[],
                &["clarify"],
                16.0,
                false,
                &["the planned deliverable is complete"],
            ),
            task(
                "review",
                "Review evidence and close the flow",
                "Review the result and evidence against the approved success criteria.",
                &["product"],
                &["deliver"],
                3.0,
                true,
                &["an accountable human accepts the result"],
            ),
        ],
    }
}

#[allow(clippy::too_many_arguments)]
fn task(
    key: &str,
    title: &str,
    description: &str,
    capabilities: &[&str],
    depends_on: &[&str],
    effort_hours: f64,
    requires_human: bool,
    acceptance_criteria: &[&str],
) -> TaskDraft {
    TaskDraft {
        key: key.into(),
        title: title.into(),
        description: description.into(),
        required_capabilities: capabilities.iter().map(|value| value.to_string()).collect(),
        depends_on: depends_on.iter().map(|value| value.to_string()).collect(),
        effort_hours,
        requires_human,
        acceptance_criteria: acceptance_criteria
            .iter()
            .map(|value| value.to_string())
            .collect(),
    }
}

fn validate_plan(mut plan: PlanDraft) -> Result<PlanDraft> {
    if plan.prd.title.trim().is_empty() || plan.prd.acceptance_criteria.is_empty() {
        return Err(MambaError::Validation(
            "PRD title and acceptance criteria are required".into(),
        ));
    }
    if plan.tasks.is_empty() {
        return Err(MambaError::Validation(
            "plan must contain at least one task".into(),
        ));
    }
    for task in &mut plan.tasks {
        task.key = task.key.trim().to_lowercase().replace(' ', "-");
        if task.key.is_empty()
            || task.title.trim().is_empty()
            || task.description.trim().is_empty()
            || !task.effort_hours.is_finite()
            || task.effort_hours <= 0.0
            || task.acceptance_criteria.is_empty()
        {
            return Err(MambaError::Validation(format!(
                "task `{}` is missing a key, description, positive effort, or acceptance criteria",
                task.title
            )));
        }
        task.required_capabilities = task
            .required_capabilities
            .iter()
            .map(|value| normalize_capability(value))
            .filter(|value| !value.is_empty())
            .collect();
    }
    Ok(plan)
}
