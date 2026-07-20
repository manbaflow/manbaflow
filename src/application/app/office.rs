use chrono::Utc;
use sha2::{Digest, Sha256};

use super::MambaApp;
use crate::domain::{
    FlightLeaseStatus, OfficeProvider, OfficeReleasePayload, OfficeReleaseRequest,
    OfficeReleaseResult, OfficeReleaseStatus, PrincipalKind, TaskStatus,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl MambaApp {
    pub fn request_office_release(
        &mut self,
        task_id: &str,
        provider: OfficeProvider,
        payload: OfficeReleasePayload,
        actor: &str,
    ) -> Result<OfficeReleaseRequest> {
        let principal = self.state.principal(actor)?.clone();
        let (flow, task) = self.task_snapshot(task_id)?;
        if !matches!(task.status, TaskStatus::InProgress | TaskStatus::Submitted) {
            return Err(MambaError::InvalidTransition(format!(
                "task {} is {:?}, expected in_progress or submitted",
                task.id, task.status
            )));
        }
        self.ensure_task_actor(&task, &principal.id)?;
        validate_office_payload(&self.state, &task.id, provider, &payload)?;
        let payload_sha256 = payload_sha256(provider, &payload)?;
        if let Some(existing) = self.state.office_releases.values().find(|release| {
            release.task_id == task.id
                && release.requested_by == principal.id
                && release.payload_sha256 == payload_sha256
                && release.status != OfficeReleaseStatus::Rejected
        }) {
            return Ok(existing.clone());
        }
        let requested_at = Utc::now();
        let request = OfficeReleaseRequest {
            id: new_id("REL"),
            flow_id: flow.id,
            task_id: task.id,
            provider,
            payload,
            payload_sha256,
            requested_by: principal.id,
            requested_at,
            status: OfficeReleaseStatus::Requested,
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
            vec![DomainEvent::OfficeReleaseRequested {
                request: request.clone(),
            }],
        )?;
        Ok(request)
    }

    pub fn office_releases(
        &self,
        actor: &str,
        flow_id: Option<&str>,
    ) -> Result<Vec<OfficeReleaseRequest>> {
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
        let mut releases = self
            .state
            .office_releases
            .values()
            .filter(|release| {
                flow_id.is_none_or(|flow_id| release.flow_id == flow_id)
                    && self
                        .state
                        .flows
                        .get(&release.flow_id)
                        .is_some_and(|flow| self.principal_has_flow_access(flow, principal))
            })
            .cloned()
            .collect::<Vec<_>>();
        releases.sort_by_key(|release| release.requested_at);
        Ok(releases)
    }

    pub fn approve_office_release(
        &mut self,
        release_id: &str,
        actor: &str,
    ) -> Result<OfficeReleaseRequest> {
        let (release, reviewer) = self.release_review_context(release_id, actor)?;
        self.commit(
            &reviewer.name,
            vec![DomainEvent::OfficeReleaseApproved {
                flow_id: release.flow_id,
                task_id: release.task_id,
                release_id: release.id.clone(),
                approved_by: reviewer.id,
                approved_at: Utc::now(),
            }],
        )?;
        Ok(self.state.office_releases[release_id].clone())
    }

    pub fn reject_office_release(
        &mut self,
        release_id: &str,
        reason: &str,
        actor: &str,
    ) -> Result<OfficeReleaseRequest> {
        let reason = validate_text(reason, "release rejection reason", 500)?;
        let (release, reviewer) = self.release_review_context(release_id, actor)?;
        self.commit(
            &reviewer.name,
            vec![DomainEvent::OfficeReleaseRejected {
                flow_id: release.flow_id,
                task_id: release.task_id,
                release_id: release.id.clone(),
                rejected_by: reviewer.id,
                reason,
                rejected_at: Utc::now(),
            }],
        )?;
        Ok(self.state.office_releases[release_id].clone())
    }

    pub fn retry_office_release(
        &mut self,
        release_id: &str,
        actor: &str,
    ) -> Result<OfficeReleaseRequest> {
        let (release, reviewer) = self.release_review_context(release_id, actor)?;
        self.commit(
            &reviewer.name,
            vec![DomainEvent::OfficeReleaseRetryApproved {
                flow_id: release.flow_id,
                task_id: release.task_id,
                release_id: release.id.clone(),
                approved_by: reviewer.id,
                approved_at: Utc::now(),
            }],
        )?;
        Ok(self.state.office_releases[release_id].clone())
    }

    pub fn claim_office_release(&mut self) -> Result<Option<OfficeReleaseRequest>> {
        self.refresh_shared_state()?;
        self.recover_stale_office_dispatches(Utc::now(), chrono::Duration::minutes(5))?;
        let Some(release) = self
            .state
            .office_releases
            .values()
            .filter(|release| release.status == OfficeReleaseStatus::Approved)
            .min_by_key(|release| release.reviewed_at.unwrap_or(release.requested_at))
            .cloned()
        else {
            return Ok(None);
        };
        let dispatch_id = new_id("DSP");
        self.commit(
            "tower://office",
            vec![DomainEvent::OfficeReleaseDispatchClaimed {
                flow_id: release.flow_id,
                task_id: release.task_id,
                release_id: release.id.clone(),
                dispatch_id,
                claimed_at: Utc::now(),
            }],
        )?;
        Ok(Some(self.state.office_releases[&release.id].clone()))
    }

    pub fn recover_stale_office_dispatches(
        &mut self,
        now: chrono::DateTime<Utc>,
        stale_after: chrono::Duration,
    ) -> Result<usize> {
        let stale = self
            .state
            .office_releases
            .values()
            .filter(|release| release.status == OfficeReleaseStatus::Dispatching)
            .filter(|release| {
                release
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
            .map(|release| DomainEvent::OfficeReleaseDispatchExpired {
                flow_id: release.flow_id.clone(),
                task_id: release.task_id.clone(),
                release_id: release.id.clone(),
                dispatch_id: release
                    .dispatch_id
                    .clone()
                    .expect("dispatching release has a dispatch ID"),
                retry_safe: release.payload.retry_safe(release.provider),
                expired_at: now,
            })
            .collect();
        self.commit("tower://office-recovery", events)?;
        Ok(stale.len())
    }

    pub fn finish_office_release(
        &mut self,
        release_id: &str,
        dispatch_id: &str,
        outcome: std::result::Result<OfficeReleaseResult, (String, bool)>,
    ) -> Result<OfficeReleaseRequest> {
        let release = self
            .state
            .office_releases
            .get(release_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "Office release",
                id: release_id.to_string(),
            })?;
        let event = match outcome {
            Ok(result) => DomainEvent::OfficeReleaseSucceeded {
                flow_id: release.flow_id,
                task_id: release.task_id,
                release_id: release.id.clone(),
                dispatch_id: dispatch_id.to_string(),
                result,
            },
            Err((error, indeterminate)) => DomainEvent::OfficeReleaseFailed {
                flow_id: release.flow_id,
                task_id: release.task_id,
                release_id: release.id.clone(),
                dispatch_id: dispatch_id.to_string(),
                error: validate_text(&error, "Office dispatch error", 2_000)?,
                indeterminate,
                failed_at: Utc::now(),
            },
        };
        self.commit("tower://office", vec![event])?;
        Ok(self.state.office_releases[release_id].clone())
    }

    fn release_review_context(
        &self,
        release_id: &str,
        actor: &str,
    ) -> Result<(OfficeReleaseRequest, crate::domain::Principal)> {
        let release = self
            .state
            .office_releases
            .get(release_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "Office release",
                id: release_id.to_string(),
            })?;
        let reviewer = self.state.principal(actor)?.clone();
        let flow = self.state.flow(&release.flow_id)?;
        if reviewer.kind != PrincipalKind::Human
            || !reviewer.active
            || (flow.demand.requester != reviewer.id && flow.demand.requester != reviewer.name)
        {
            return Err(MambaError::PermissionDenied(
                "only the Human Demand Requester can release Office side effects".into(),
            ));
        }
        Ok((release, reviewer))
    }
}

fn validate_office_payload(
    state: &crate::state::OrganizationState,
    task_id: &str,
    provider: OfficeProvider,
    payload: &OfficeReleasePayload,
) -> Result<()> {
    match payload {
        OfficeReleasePayload::DriveUpload {
            artifact_id,
            account_id,
            parent_id,
            file_name,
            file_id,
        } => {
            validate_text(account_id, "Office account", 200)?;
            validate_text(parent_id, "drive parent", 500)?;
            let file_name = validate_text(file_name, "drive file name", 200)?;
            if file_name.contains(['/', '\\']) || matches!(file_name.as_str(), "." | "..") {
                return Err(MambaError::Validation(
                    "drive file name must not contain path separators".into(),
                ));
            }
            if let Some(file_id) = file_id {
                validate_text(file_id, "drive file ID", 500)?;
            }
            if provider == OfficeProvider::Microsoft365 && file_id.is_some() {
                return Err(MambaError::Validation(
                    "Microsoft 365 drive uploads target the approved parent and file name".into(),
                ));
            }
            let artifact =
                state
                    .staged_artifacts
                    .get(artifact_id)
                    .ok_or_else(|| MambaError::NotFound {
                        entity: "staged artifact",
                        id: artifact_id.clone(),
                    })?;
            if artifact.task_id != task_id {
                return Err(MambaError::Validation(
                    "Office release artifact belongs to another task".into(),
                ));
            }
            let lease = state
                .flight_leases
                .get(&artifact.flight_lease_id)
                .ok_or_else(|| MambaError::NotFound {
                    entity: "flight lease",
                    id: artifact.flight_lease_id.clone(),
                })?;
            if lease.status != FlightLeaseStatus::Landed {
                return Err(MambaError::InvalidTransition(
                    "Office artifact can only be released after its flight landed".into(),
                ));
            }
        }
        OfficeReleasePayload::SendEmail {
            account_id,
            to,
            cc,
            bcc,
            subject,
            body,
            body_type: _,
        } => {
            validate_text(account_id, "Office account", 200)?;
            validate_addresses(to, "email recipient", true)?;
            validate_addresses(cc, "email CC", false)?;
            validate_addresses(bcc, "email BCC", false)?;
            validate_text(subject, "email subject", 500)?;
            validate_body(body, "email body", 100_000)?;
            if to.len() + cc.len() + bcc.len() > 100 {
                return Err(MambaError::Validation(
                    "email release cannot contain more than 100 recipients".into(),
                ));
            }
        }
        OfficeReleasePayload::CreateCalendarEvent {
            account_id,
            calendar_id,
            subject,
            body,
            body_type: _,
            start,
            end,
            time_zone,
            attendees,
            location,
            send_updates: _,
        } => {
            validate_text(account_id, "Office account", 200)?;
            validate_text(calendar_id, "calendar ID", 500)?;
            validate_text(subject, "calendar subject", 500)?;
            validate_body(body, "calendar body", 100_000)?;
            validate_text(time_zone, "calendar time zone", 100)?;
            if end <= start || end.signed_duration_since(*start) > chrono::Duration::days(31) {
                return Err(MambaError::Validation(
                    "calendar event must end after it starts and last at most 31 days".into(),
                ));
            }
            validate_addresses(attendees, "calendar attendee", false)?;
            if attendees.len() > 100 {
                return Err(MambaError::Validation(
                    "calendar release cannot contain more than 100 attendees".into(),
                ));
            }
            if let Some(location) = location {
                validate_text(location, "calendar location", 500)?;
            }
        }
    }
    Ok(())
}

fn validate_addresses(values: &[String], label: &str, required: bool) -> Result<()> {
    if required && values.is_empty() {
        return Err(MambaError::Validation(format!(
            "{label} requires at least one address"
        )));
    }
    for value in values {
        let value = validate_text(value, label, 320)?;
        let mut parts = value.split('@');
        if parts.next().is_none_or(str::is_empty)
            || parts.next().is_none_or(str::is_empty)
            || parts.next().is_some()
            || value.chars().any(char::is_whitespace)
        {
            return Err(MambaError::Validation(format!("invalid {label}: {value}")));
        }
    }
    Ok(())
}

fn validate_text(value: &str, label: &str, max: usize) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > max || value.chars().any(char::is_control) {
        return Err(MambaError::Validation(format!(
            "{label} must contain 1 to {max} printable characters"
        )));
    }
    Ok(value.to_string())
}

fn validate_body(value: &str, label: &str, max: usize) -> Result<()> {
    if value.trim().is_empty()
        || value.chars().count() > max
        || value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\r' | '\n' | '\t'))
    {
        return Err(MambaError::Validation(format!(
            "{label} must contain 1 to {max} safe text characters"
        )));
    }
    Ok(())
}

fn payload_sha256(provider: OfficeProvider, payload: &OfficeReleasePayload) -> Result<String> {
    let bytes = serde_json::to_vec(&(provider, payload))?;
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
