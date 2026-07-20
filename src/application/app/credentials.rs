use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::MambaApp;
use super::authority::Permission;
use crate::domain::{ApiCredential, IssuedCredential, Principal};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl MambaApp {
    pub fn issue_api_credential(
        &mut self,
        target: &str,
        label: &str,
        actor: &str,
    ) -> Result<IssuedCredential> {
        self.issue_api_credential_with_ttl(target, label, actor, 30)
    }

    pub fn issue_api_credential_with_ttl(
        &mut self,
        target: &str,
        label: &str,
        actor: &str,
        ttl_days: u32,
    ) -> Result<IssuedCredential> {
        self.state.organization()?;
        self.ensure_permission(actor, Permission::CredentialManage)?;
        if !(1..=365).contains(&ttl_days) {
            return Err(MambaError::Validation(
                "credential TTL must be between 1 and 365 days".into(),
            ));
        }
        let principal = self.state.principal(target)?.clone();
        let label = label.trim();
        if label.is_empty() || label.chars().count() > 80 {
            return Err(MambaError::Validation(
                "credential label must contain 1 to 80 characters".into(),
            ));
        }
        let token = format!("mmb_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let token_hash = credential_hash(&token);
        let created_at = Utc::now();
        let credential = ApiCredential {
            id: new_id("CRED"),
            principal_id: principal.id,
            label: label.to_string(),
            created_at,
            expires_at: Some(created_at + Duration::days(i64::from(ttl_days))),
            revoked_at: None,
        };
        self.store.insert_credential(
            &credential.id,
            &credential.principal_id,
            &token_hash,
            credential.created_at,
            credential.expires_at,
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
        self.ensure_permission(actor, Permission::CredentialManage)?;
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
                "API credential {} is no longer active",
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
}

fn credential_hash(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}
