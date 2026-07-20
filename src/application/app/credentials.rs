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
        if !(1..=365).contains(&ttl_days) {
            return Err(MambaError::Validation(
                "credential TTL must be between 1 and 365 days".into(),
            ));
        }
        self.issue_credential_until(
            target,
            label,
            actor,
            Utc::now() + Duration::days(i64::from(ttl_days)),
        )
    }

    pub fn issue_oidc_session(&mut self, target: &str) -> Result<IssuedCredential> {
        self.issue_credential_until(
            target,
            "OIDC browser session",
            "tower://oidc",
            Utc::now() + Duration::hours(8),
        )
    }

    fn issue_credential_until(
        &mut self,
        target: &str,
        label: &str,
        actor: &str,
        expires_at: chrono::DateTime<Utc>,
    ) -> Result<IssuedCredential> {
        self.state.organization()?;
        self.ensure_permission(actor, Permission::CredentialManage)?;
        let principal = self.state.principal(target)?.clone();
        let label = label.trim();
        if label.is_empty() || label.chars().count() > 80 {
            return Err(MambaError::Validation(
                "credential label must contain 1 to 80 characters".into(),
            ));
        }
        let tenant_id = &self.state.tenant()?.id;
        let token = format!(
            "mmb_{tenant_id}_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        let token_hash = credential_hash(&token);
        let created_at = Utc::now();
        let credential = ApiCredential {
            id: new_id("CRED"),
            principal_id: principal.id,
            label: label.to_string(),
            created_at,
            expires_at: Some(expires_at),
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
        if !valid_api_token(token) {
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

    pub fn revoke_oidc_session(&mut self, token: &str) -> Result<()> {
        if !valid_api_token(token) {
            return Err(MambaError::PermissionDenied("invalid OIDC session".into()));
        }
        let token_hash = credential_hash(token);
        let Some((credential_id, principal_id)) =
            self.store.authenticate_credential(&token_hash)?
        else {
            return Ok(());
        };
        let Some(credential) = self.state.credentials.get(&credential_id) else {
            return Ok(());
        };
        if credential.principal_id != principal_id || !credential.label.starts_with("OIDC ") {
            return Err(MambaError::PermissionDenied(
                "credential is not an OIDC browser session".into(),
            ));
        }
        let revoked_at = Utc::now();
        self.commit(
            "tower://oidc",
            vec![DomainEvent::ApiCredentialRevoked {
                credential_id: credential_id.clone(),
                principal_id,
                revoked_at,
            }],
        )?;
        self.store.revoke_credential(&credential_id, revoked_at)
    }
}

fn credential_hash(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

pub fn tenant_token_hint(token: &str) -> Option<&str> {
    let value = token.strip_prefix("mmb_")?;
    let (tenant_id, secret) = value.rsplit_once('_')?;
    (tenant_id.starts_with("TEN-")
        && tenant_id.len() > 4
        && tenant_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        && secret.len() == 64
        && secret.bytes().all(|byte| byte.is_ascii_hexdigit()))
    .then_some(tenant_id)
}

fn valid_api_token(token: &str) -> bool {
    let legacy = token.len() == 68
        && token.starts_with("mmb_")
        && token[4..].bytes().all(|value| value.is_ascii_hexdigit());
    legacy || tenant_token_hint(token).is_some()
}

#[cfg(test)]
mod token_tests {
    use super::*;

    #[test]
    fn tenant_hint_is_strict_and_legacy_tokens_remain_valid() {
        let secret = "a".repeat(64);
        let routed = format!("mmb_TEN-ab12cd34_{secret}");
        assert_eq!(tenant_token_hint(&routed), Some("TEN-ab12cd34"));
        assert!(valid_api_token(&format!("mmb_{secret}")));
        assert!(!valid_api_token(&format!(
            "mmb_TEN-ab12cd34_{}",
            "z".repeat(64)
        )));
        assert_eq!(tenant_token_hint(&format!("mmb_other_{secret}")), None);
    }
}
