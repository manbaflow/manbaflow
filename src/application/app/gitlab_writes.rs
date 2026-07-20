use std::fmt::Write as _;

use chrono::Utc;
use sha2::{Digest, Sha256};

use super::MambaApp;
use crate::domain::{
    ExternalArtifact, GitLabWritePayload, GitLabWriteRequest, GitLabWriteResult, GitLabWriteStatus,
    PrincipalKind, TaskStatus,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl MambaApp {
    pub fn request_gitlab_write(
        &mut self,
        task_id: &str,
        payload: GitLabWritePayload,
        actor: &str,
    ) -> Result<GitLabWriteRequest> {
        let principal = self.state.principal(actor)?.clone();
        let (flow, task) = self.task_snapshot(task_id)?;
        if !matches!(task.status, TaskStatus::InProgress | TaskStatus::Submitted) {
            return Err(MambaError::InvalidTransition(format!(
                "task {} is {:?}, expected in_progress or submitted",
                task.id, task.status
            )));
        }
        self.ensure_task_actor(&task, &principal.id)?;
        validate_payload(&payload)?;
        let payload_sha256 = payload_sha256(&payload)?;
        if let Some(existing) = self.state.gitlab_writes.values().find(|request| {
            request.task_id == task.id
                && request.requested_by == principal.id
                && request.payload_sha256 == payload_sha256
                && request.status != GitLabWriteStatus::Rejected
        }) {
            return Ok(existing.clone());
        }
        let request = GitLabWriteRequest {
            id: new_id("GLW"),
            flow_id: flow.id,
            task_id: task.id,
            payload,
            payload_sha256,
            requested_by: principal.id,
            requested_at: Utc::now(),
            status: GitLabWriteStatus::Requested,
            reviewed_by: None,
            reviewed_at: None,
            review_reason: None,
            dispatch_id: None,
            dispatch_started_at: None,
            result: None,
            last_error: None,
        };
        self.commit(
            &principal.name,
            vec![DomainEvent::GitLabWriteRequested {
                request: request.clone(),
            }],
        )?;
        Ok(request)
    }

    pub fn gitlab_writes(
        &self,
        actor: &str,
        flow_id: Option<&str>,
    ) -> Result<Vec<GitLabWriteRequest>> {
        let principal = self.state.principal(actor)?;
        if let Some(flow_id) = flow_id {
            let flow = self.state.flow(flow_id)?;
            if !self.principal_has_flow_access(flow, principal) {
                return Err(MambaError::PermissionDenied(format!(
                    "{} cannot access flow {}",
                    principal.name, flow.id
                )));
            }
        }
        let mut requests = self
            .state
            .gitlab_writes
            .values()
            .filter(|request| {
                flow_id.is_none_or(|flow_id| request.flow_id == flow_id)
                    && self
                        .state
                        .flows
                        .get(&request.flow_id)
                        .is_some_and(|flow| self.principal_has_flow_access(flow, principal))
            })
            .cloned()
            .collect::<Vec<_>>();
        requests.sort_by_key(|request| request.requested_at);
        Ok(requests)
    }

    pub fn approve_gitlab_write(
        &mut self,
        write_id: &str,
        actor: &str,
    ) -> Result<GitLabWriteRequest> {
        let (request, reviewer) = self.gitlab_review_context(write_id, actor)?;
        if request.status != GitLabWriteStatus::Requested {
            return Err(MambaError::InvalidTransition(format!(
                "GitLab write {} is {:?}, expected requested",
                request.id, request.status
            )));
        }
        self.commit(
            &reviewer.name,
            vec![DomainEvent::GitLabWriteApproved {
                flow_id: request.flow_id,
                task_id: request.task_id,
                write_id: request.id.clone(),
                approved_by: reviewer.id,
                approved_at: Utc::now(),
            }],
        )?;
        Ok(self.state.gitlab_writes[write_id].clone())
    }

    pub fn reject_gitlab_write(
        &mut self,
        write_id: &str,
        reason: &str,
        actor: &str,
    ) -> Result<GitLabWriteRequest> {
        let reason = validate_required_text(reason, "GitLab rejection reason", 500)?;
        let (request, reviewer) = self.gitlab_review_context(write_id, actor)?;
        if request.status != GitLabWriteStatus::Requested {
            return Err(MambaError::InvalidTransition(format!(
                "GitLab write {} is {:?}, expected requested",
                request.id, request.status
            )));
        }
        self.commit(
            &reviewer.name,
            vec![DomainEvent::GitLabWriteRejected {
                flow_id: request.flow_id,
                task_id: request.task_id,
                write_id: request.id.clone(),
                rejected_by: reviewer.id,
                reason,
                rejected_at: Utc::now(),
            }],
        )?;
        Ok(self.state.gitlab_writes[write_id].clone())
    }

    pub fn retry_gitlab_write(
        &mut self,
        write_id: &str,
        actor: &str,
    ) -> Result<GitLabWriteRequest> {
        let (request, reviewer) = self.gitlab_review_context(write_id, actor)?;
        if !matches!(
            request.status,
            GitLabWriteStatus::Failed | GitLabWriteStatus::Indeterminate
        ) {
            return Err(MambaError::InvalidTransition(format!(
                "GitLab write {} is {:?}, expected failed or indeterminate",
                request.id, request.status
            )));
        }
        self.commit(
            &reviewer.name,
            vec![DomainEvent::GitLabWriteRetryApproved {
                flow_id: request.flow_id,
                task_id: request.task_id,
                write_id: request.id.clone(),
                approved_by: reviewer.id,
                approved_at: Utc::now(),
            }],
        )?;
        Ok(self.state.gitlab_writes[write_id].clone())
    }

    pub fn claim_gitlab_write(&mut self) -> Result<Option<GitLabWriteRequest>> {
        self.refresh_shared_state()?;
        self.recover_stale_gitlab_writes(Utc::now(), chrono::Duration::minutes(5))?;
        let Some(request) = self
            .state
            .gitlab_writes
            .values()
            .filter(|request| request.status == GitLabWriteStatus::Approved)
            .min_by_key(|request| request.reviewed_at.unwrap_or(request.requested_at))
            .cloned()
        else {
            return Ok(None);
        };
        let dispatch_id = new_id("DSP");
        self.commit(
            "tower://gitlab",
            vec![DomainEvent::GitLabWriteDispatchClaimed {
                flow_id: request.flow_id,
                task_id: request.task_id,
                write_id: request.id.clone(),
                dispatch_id,
                claimed_at: Utc::now(),
            }],
        )?;
        Ok(Some(self.state.gitlab_writes[&request.id].clone()))
    }

    pub fn recover_stale_gitlab_writes(
        &mut self,
        now: chrono::DateTime<Utc>,
        stale_after: chrono::Duration,
    ) -> Result<usize> {
        let stale = self
            .state
            .gitlab_writes
            .values()
            .filter(|request| request.status == GitLabWriteStatus::Dispatching)
            .filter(|request| {
                request
                    .dispatch_started_at
                    .is_some_and(|started| started + stale_after <= now)
            })
            .cloned()
            .collect::<Vec<_>>();
        if stale.is_empty() {
            return Ok(0);
        }
        let events = stale
            .iter()
            .map(|request| DomainEvent::GitLabWriteDispatchExpired {
                flow_id: request.flow_id.clone(),
                task_id: request.task_id.clone(),
                write_id: request.id.clone(),
                dispatch_id: request
                    .dispatch_id
                    .clone()
                    .expect("dispatching GitLab write has a dispatch ID"),
                expired_at: now,
            })
            .collect();
        self.commit("tower://gitlab-recovery", events)?;
        Ok(stale.len())
    }

    pub fn finish_gitlab_write(
        &mut self,
        write_id: &str,
        dispatch_id: &str,
        outcome: std::result::Result<GitLabWriteResult, (String, bool)>,
    ) -> Result<GitLabWriteRequest> {
        let request = self
            .state
            .gitlab_writes
            .get(write_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "GitLab write",
                id: write_id.to_string(),
            })?;
        let events = match outcome {
            Ok(result) => {
                validate_result(&request, &result)?;
                let artifact = result_artifact(&request, &result);
                vec![
                    DomainEvent::GitLabWriteSucceeded {
                        flow_id: request.flow_id.clone(),
                        task_id: request.task_id.clone(),
                        write_id: request.id.clone(),
                        dispatch_id: dispatch_id.to_string(),
                        result,
                    },
                    DomainEvent::ExternalArtifactSynced {
                        flow_id: request.flow_id,
                        task_id: request.task_id,
                        artifact,
                    },
                ]
            }
            Err((error, indeterminate)) => vec![DomainEvent::GitLabWriteFailed {
                flow_id: request.flow_id,
                task_id: request.task_id,
                write_id: request.id.clone(),
                dispatch_id: dispatch_id.to_string(),
                error: validate_required_text(&error, "GitLab dispatch error", 2_000)?,
                indeterminate,
                failed_at: Utc::now(),
            }],
        };
        self.commit("tower://gitlab", events)?;
        Ok(self.state.gitlab_writes[write_id].clone())
    }

    fn gitlab_review_context(
        &self,
        write_id: &str,
        actor: &str,
    ) -> Result<(GitLabWriteRequest, crate::domain::Principal)> {
        let request = self
            .state
            .gitlab_writes
            .get(write_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "GitLab write",
                id: write_id.to_string(),
            })?;
        let reviewer = self.state.principal(actor)?.clone();
        let flow = self.state.flow(&request.flow_id)?;
        if reviewer.kind != PrincipalKind::Human
            || !reviewer.active
            || (flow.demand.requester != reviewer.id && flow.demand.requester != reviewer.name)
        {
            return Err(MambaError::PermissionDenied(
                "only the Human Demand Requester can release GitLab writes".into(),
            ));
        }
        Ok((request, reviewer))
    }
}

fn validate_payload(payload: &GitLabWritePayload) -> Result<()> {
    validate_project(payload.project())?;
    match payload {
        GitLabWritePayload::CreateIssue {
            title,
            description,
            labels,
            ..
        } => {
            validate_required_text(title, "GitLab issue title", 255)?;
            validate_body(description, "GitLab issue description", 1_000_000, true)?;
            validate_labels(labels)?;
        }
        GitLabWritePayload::CommentIssue {
            issue_iid, body, ..
        } => {
            validate_iid(*issue_iid, "issue")?;
            validate_body(body, "GitLab issue comment", 1_000_000, false)?;
        }
        GitLabWritePayload::CreateMergeRequest {
            source_branch,
            target_branch,
            title,
            description,
            labels,
            ..
        } => {
            validate_branch(source_branch, "source branch")?;
            validate_branch(target_branch, "target branch")?;
            if source_branch == target_branch {
                return Err(MambaError::Validation(
                    "GitLab source and target branches must differ".into(),
                ));
            }
            validate_required_text(title, "GitLab merge request title", 255)?;
            validate_body(
                description,
                "GitLab merge request description",
                1_000_000,
                true,
            )?;
            validate_labels(labels)?;
        }
        GitLabWritePayload::CommentMergeRequest {
            merge_request_iid,
            body,
            ..
        } => {
            validate_iid(*merge_request_iid, "merge request")?;
            validate_body(body, "GitLab merge request comment", 1_000_000, false)?;
        }
    }
    Ok(())
}

fn validate_result(request: &GitLabWriteRequest, result: &GitLabWriteResult) -> Result<()> {
    let expected_kind = match request.payload {
        GitLabWritePayload::CreateIssue { .. } => "issue",
        GitLabWritePayload::CommentIssue { .. } => "issue_note",
        GitLabWritePayload::CreateMergeRequest { .. } => "merge_request",
        GitLabWritePayload::CommentMergeRequest { .. } => "merge_request_note",
    };
    if result.kind != expected_kind {
        return Err(MambaError::Validation(format!(
            "GitLab write result kind {} does not match {expected_kind}",
            result.kind
        )));
    }
    validate_required_text(&result.external_id, "GitLab result external ID", 500)?;
    validate_required_text(&result.title, "GitLab result title", 1_000)?;
    validate_required_text(&result.status, "GitLab result status", 100)?;
    if !(200..300).contains(&result.response_status) {
        return Err(MambaError::Validation(
            "GitLab successful result must contain a 2xx response status".into(),
        ));
    }
    let url = reqwest::Url::parse(&result.url)
        .map_err(|_| MambaError::Validation("GitLab result URL is invalid".into()))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(MambaError::Validation(
            "GitLab result URL must be an absolute HTTP(S) URL".into(),
        ));
    }
    Ok(())
}

fn validate_project(value: &str) -> Result<()> {
    let value = value.trim();
    let valid_numeric = value.parse::<u64>().is_ok_and(|id| id > 0);
    let valid_path = value.contains('/')
        && value.split('/').all(|segment| {
            !segment.is_empty()
                && !matches!(segment, "." | "..")
                && segment.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
                })
        });
    if value.len() > 500 || value != value.trim() || (!valid_numeric && !valid_path) {
        return Err(MambaError::Validation(
            "GitLab project must be a numeric ID or namespace/project path".into(),
        ));
    }
    Ok(())
}

fn validate_iid(value: u64, label: &str) -> Result<()> {
    if value == 0 {
        return Err(MambaError::Validation(format!(
            "GitLab {label} IID must be greater than zero"
        )));
    }
    Ok(())
}

fn validate_branch(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || value.starts_with('-')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.ends_with(".lock")
        || value.contains("..")
        || value.contains("@{")
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(MambaError::Validation(format!("invalid GitLab {label}")));
    }
    Ok(())
}

fn validate_labels(labels: &[String]) -> Result<()> {
    if labels.len() > 50 {
        return Err(MambaError::Validation(
            "GitLab write cannot contain more than 50 labels".into(),
        ));
    }
    for label in labels {
        validate_required_text(label, "GitLab label", 255)?;
        if label.contains(',') {
            return Err(MambaError::Validation(
                "GitLab labels must not contain commas".into(),
            ));
        }
    }
    Ok(())
}

fn validate_required_text(value: &str, label: &str, max: usize) -> Result<String> {
    let value = value.trim();
    if value.is_empty()
        || value.chars().count() > max
        || value.chars().any(|character| character.is_control())
    {
        return Err(MambaError::Validation(format!(
            "{label} must contain 1 to {max} printable characters"
        )));
    }
    Ok(value.to_string())
}

fn validate_body(value: &str, label: &str, max: usize, allow_empty: bool) -> Result<()> {
    if (!allow_empty && value.trim().is_empty())
        || value.chars().count() > max
        || value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\r' | '\n' | '\t'))
    {
        return Err(MambaError::Validation(format!(
            "{label} must contain at most {max} safe text characters"
        )));
    }
    Ok(())
}

fn payload_sha256(payload: &GitLabWritePayload) -> Result<String> {
    let bytes = serde_json::to_vec(payload)?;
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn gitlab_artifact_id(kind: &str, project: &str, external_id: &str) -> String {
    let digest = Sha256::digest(format!("gitlab:{kind}:{project}:{external_id}").as_bytes());
    let mut id = String::from("EXT-");
    for byte in digest.iter().take(8) {
        write!(&mut id, "{byte:02x}").expect("writing to a string cannot fail");
    }
    id
}

fn result_artifact(request: &GitLabWriteRequest, result: &GitLabWriteResult) -> ExternalArtifact {
    ExternalArtifact {
        id: gitlab_artifact_id(&result.kind, request.payload.project(), &result.external_id),
        provider: "gitlab".into(),
        kind: result.kind.clone(),
        project: request.payload.project().to_string(),
        external_id: result.external_id.clone(),
        parent_id: None,
        title: result.title.clone(),
        url: result.url.clone(),
        status: result.status.clone(),
        revision: None,
        verified: false,
        synced_at: result.written_at,
    }
}
