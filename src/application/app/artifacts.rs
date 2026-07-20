use std::path::{Component, Path};

use chrono::Utc;
use sha2::{Digest, Sha256};

use super::MambaApp;
use crate::domain::{CapabilityPack, FlightDeliverable, FlightLeaseStatus, StagedArtifact};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;
use crate::store::ArtifactBlob;

pub const MAX_ARTIFACT_BYTES: usize = 25 * 1024 * 1024;

impl MambaApp {
    pub fn stage_flight_artifact(
        &mut self,
        lease_id: &str,
        path: &str,
        media_type: &str,
        content: Vec<u8>,
        actor: &str,
    ) -> Result<StagedArtifact> {
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
        if lease.status != FlightLeaseStatus::Active {
            return Err(MambaError::InvalidTransition(format!(
                "flight lease {} is {:?}, expected active",
                lease.id, lease.status
            )));
        }
        let manifest = lease.manifest.as_ref().ok_or_else(|| {
            MambaError::Validation("artifact staging requires a FlightManifest".into())
        })?;
        if manifest.capability_pack != CapabilityPack::Office {
            return Err(MambaError::Validation(
                "artifact staging is only available to Office flights".into(),
            ));
        }
        let path = validate_artifact_path(path)?;
        let media_type = validate_media_type(media_type)?;
        if content.is_empty() || content.len() > MAX_ARTIFACT_BYTES {
            return Err(MambaError::Validation(format!(
                "artifact content must contain 1 to {MAX_ARTIFACT_BYTES} bytes"
            )));
        }
        let deliverable = FlightDeliverable::from_path(path.clone(), true);
        let extension = Path::new(&path)
            .extension()
            .and_then(|value| value.to_str())
            .map(str::to_ascii_lowercase)
            .ok_or_else(|| MambaError::Validation("artifact path requires an extension".into()))?;
        if !manifest.output_contract.allowed_extensions.is_empty()
            && !manifest
                .output_contract
                .allowed_extensions
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(&extension))
        {
            return Err(MambaError::Validation(format!(
                "artifact extension .{extension} is outside the FlightManifest output contract"
            )));
        }
        let sha256 = sha256_hex(&content);
        if let Some(existing) = self
            .state
            .staged_artifacts
            .values()
            .find(|artifact| artifact.flight_lease_id == lease.id && artifact.path == path)
        {
            if existing.sha256 == sha256 && existing.media_type == media_type {
                return Ok(existing.clone());
            }
            return Err(MambaError::InvalidTransition(format!(
                "artifact path {path} is already staged with immutable content"
            )));
        }

        let staged_bytes = self
            .state
            .staged_artifacts
            .values()
            .filter(|artifact| artifact.flight_lease_id == lease.id)
            .try_fold(0usize, |total, artifact| {
                usize::try_from(artifact.size_bytes)
                    .ok()
                    .and_then(|size| total.checked_add(size))
            })
            .ok_or_else(|| MambaError::Validation("artifact staging budget overflow".into()))?;
        if staged_bytes
            .checked_add(content.len())
            .is_none_or(|total| total > MAX_ARTIFACT_BYTES)
        {
            return Err(MambaError::Validation(format!(
                "Office flight artifacts exceed the {MAX_ARTIFACT_BYTES} byte staging budget"
            )));
        }

        let staged_at = Utc::now();
        let size_bytes = u64::try_from(content.len())
            .map_err(|_| MambaError::Validation("artifact is too large".into()))?;
        self.store.put_artifact(&ArtifactBlob {
            sha256: sha256.clone(),
            media_type: media_type.clone(),
            size_bytes: i64::try_from(size_bytes)
                .map_err(|_| MambaError::Validation("artifact is too large".into()))?,
            content,
            created_at: staged_at.to_rfc3339(),
        })?;
        let artifact = StagedArtifact {
            id: new_id("ART"),
            flight_lease_id: lease.id,
            flow_id: lease.flow_id,
            task_id: lease.task_id,
            path,
            kind: deliverable.kind,
            media_type,
            sha256,
            size_bytes,
            staged_by: principal.name.clone(),
            staged_at,
        };
        self.commit(
            &principal.name,
            vec![DomainEvent::FlightArtifactStaged {
                artifact: artifact.clone(),
            }],
        )?;
        Ok(artifact)
    }

    pub fn flight_artifacts(&self, lease_id: &str, actor: &str) -> Result<Vec<StagedArtifact>> {
        let lease = self
            .state
            .flight_leases
            .get(lease_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "flight lease",
                id: lease_id.to_string(),
            })?;
        self.ensure_flow_access(&lease.flow_id, actor)?;
        let mut artifacts = self
            .state
            .staged_artifacts
            .values()
            .filter(|artifact| artifact.flight_lease_id == lease_id)
            .cloned()
            .collect::<Vec<_>>();
        artifacts.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(artifacts)
    }

    pub fn artifact_content(
        &self,
        artifact_id: &str,
        actor: &str,
    ) -> Result<(StagedArtifact, Vec<u8>)> {
        let artifact = self
            .state
            .staged_artifacts
            .get(artifact_id)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "staged artifact",
                id: artifact_id.to_string(),
            })?;
        self.ensure_flow_access(&artifact.flow_id, actor)?;
        let blob = self.store.load_artifact(&artifact.sha256)?.ok_or_else(|| {
            MambaError::Validation(format!(
                "artifact {} content is missing from storage",
                artifact.id
            ))
        })?;
        if blob.content.len() as u64 != artifact.size_bytes
            || sha256_hex(&blob.content) != artifact.sha256
        {
            return Err(MambaError::Validation(format!(
                "artifact {} failed content integrity verification",
                artifact.id
            )));
        }
        Ok((artifact, blob.content))
    }

    fn ensure_flow_access(&self, flow_id: &str, actor: &str) -> Result<()> {
        let flow = self.state.flow(flow_id)?;
        let principal = self.state.principal(actor)?;
        if self.principal_has_flow_access(flow, principal) {
            Ok(())
        } else {
            Err(MambaError::PermissionDenied(format!(
                "{} cannot access flow {}",
                principal.name, flow.id
            )))
        }
    }
}

fn validate_artifact_path(value: &str) -> Result<String> {
    let value = value.trim();
    let path = Path::new(value);
    if value.is_empty()
        || value.chars().count() > 240
        || value.chars().any(char::is_control)
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(MambaError::Validation(
            "artifact path must be a safe relative path of at most 240 characters".into(),
        ));
    }
    Ok(value.replace('\\', "/"))
}

fn validate_media_type(value: &str) -> Result<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty()
        || value.chars().count() > 120
        || !value.contains('/')
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "!#$&^_.+-/".contains(character))
    {
        return Err(MambaError::Validation("invalid artifact media type".into()));
    }
    Ok(value)
}

fn sha256_hex(content: &[u8]) -> String {
    Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
