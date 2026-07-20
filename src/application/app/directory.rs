use chrono::Utc;

use super::MambaApp;
use super::authority::Permission;
use crate::domain::{
    ExternalIdentityBinding, OrganizationRole, Principal, PrincipalKind, RoleBinding, Team,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl MambaApp {
    pub fn provision_directory_human(
        &mut self,
        name: &str,
        user_name: &str,
        external_user_id: &str,
        team: Option<&str>,
        active: bool,
        actor: &str,
    ) -> Result<(Principal, ExternalIdentityBinding)> {
        self.ensure_permission(actor, Permission::PrincipalManage)?;
        let name = validate_directory_name(name, "Human")?;
        let user_name = validate_directory_name(user_name, "user")?;
        let external_user_id = validate_external_id(external_user_id)?;
        if self
            .state
            .principals
            .values()
            .any(|principal| principal.name.eq_ignore_ascii_case(&name))
        {
            return Err(MambaError::Validation(format!(
                "principal already exists: {name}"
            )));
        }
        if self.state.principals.values().any(|principal| {
            principal
                .directory_username
                .as_deref()
                .is_some_and(|current| current.eq_ignore_ascii_case(&user_name))
        }) {
            return Err(MambaError::Validation(format!(
                "directory userName already exists: {user_name}"
            )));
        }
        if self.state.external_identities.values().any(|binding| {
            binding.is_active()
                && binding.provider == "oidc"
                && binding.external_user_id == external_user_id
        }) {
            return Err(MambaError::Validation(
                "OIDC subject is already provisioned".into(),
            ));
        }
        let team_id = team
            .map(|value| self.state.team(value).map(|team| team.id.clone()))
            .transpose()?;
        let now = Utc::now();
        let principal = Principal {
            id: new_id("HUM"),
            name,
            directory_username: Some(user_name),
            kind: PrincipalKind::Human,
            team_id,
            owner_id: None,
            capabilities: Vec::new(),
            capacity_percent: 100,
            executor: None,
            active,
            created_at: now,
        };
        let role = RoleBinding {
            id: new_id("ROLE"),
            tenant_id: self.state.tenant()?.id.clone(),
            organization_id: self.state.organization()?.id.clone(),
            principal_id: principal.id.clone(),
            role: OrganizationRole::Member,
            granted_by: actor.to_string(),
            granted_at: now,
            revoked_by: None,
            revoked_at: None,
        };
        let binding = ExternalIdentityBinding {
            id: new_id("XID"),
            provider: "oidc".into(),
            external_user_id,
            principal_id: principal.id.clone(),
            bound_by: actor.to_string(),
            bound_at: now,
            unbound_by: None,
            unbound_at: None,
        };
        self.commit(
            actor,
            vec![
                DomainEvent::PrincipalRegistered {
                    principal: principal.clone(),
                },
                DomainEvent::RoleGranted { binding: role },
                DomainEvent::ExternalIdentityBound {
                    binding: binding.clone(),
                },
            ],
        )?;
        Ok((principal, binding))
    }

    pub fn update_directory_human(
        &mut self,
        principal: &str,
        name: &str,
        user_name: &str,
        team: Option<&str>,
        active: bool,
        actor: &str,
    ) -> Result<Principal> {
        self.ensure_permission(actor, Permission::PrincipalManage)?;
        let principal = self.state.principal(principal)?.clone();
        if principal.kind != PrincipalKind::Human {
            return Err(MambaError::Validation(
                "directory provisioning can only update Human principals".into(),
            ));
        }
        let name = validate_directory_name(name, "Human")?;
        let user_name = validate_directory_name(user_name, "user")?;
        if self.state.principals.values().any(|candidate| {
            candidate.id != principal.id
                && candidate
                    .directory_username
                    .as_deref()
                    .is_some_and(|current| current.eq_ignore_ascii_case(&user_name))
        }) {
            return Err(MambaError::Validation(format!(
                "directory userName already exists: {user_name}"
            )));
        }
        let team_id = team
            .map(|value| self.state.team(value).map(|team| team.id.clone()))
            .transpose()?;
        self.commit(
            actor,
            vec![DomainEvent::PrincipalDirectoryUpdated {
                principal_id: principal.id.clone(),
                name,
                user_name,
                team_id,
                active,
                updated_by: actor.to_string(),
                updated_at: Utc::now(),
            }],
        )?;
        Ok(self.state.principal(&principal.id)?.clone())
    }

    pub fn update_directory_team(
        &mut self,
        team: &str,
        name: &str,
        external_id: Option<&str>,
        active: bool,
        actor: &str,
    ) -> Result<Team> {
        self.ensure_permission(actor, Permission::PrincipalManage)?;
        let team = self.state.team(team)?.clone();
        let name = validate_directory_name(name, "team")?;
        let external_id = external_id.map(validate_external_id).transpose()?;
        if external_id.as_ref().is_some_and(|external_id| {
            self.state.teams.values().any(|candidate| {
                candidate.id != team.id
                    && candidate.directory_external_id.as_ref() == Some(external_id)
            })
        }) {
            return Err(MambaError::Validation(
                "directory Group externalId already exists".into(),
            ));
        }
        self.commit(
            actor,
            vec![DomainEvent::TeamDirectoryUpdated {
                team_id: team.id.clone(),
                name,
                external_id,
                active,
                updated_by: actor.to_string(),
                updated_at: Utc::now(),
            }],
        )?;
        Ok(self.state.team(&team.id)?.clone())
    }

    pub fn oidc_principal(&self, subject: &str) -> Result<Principal> {
        let binding = self.state.external_identity("oidc", subject)?;
        let principal = self.state.principal(&binding.principal_id)?;
        if principal.kind != PrincipalKind::Human || !principal.active {
            return Err(MambaError::PermissionDenied(
                "OIDC identity is not an active Human".into(),
            ));
        }
        Ok(principal.clone())
    }
}

fn validate_directory_name(value: &str, label: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 120 || value.chars().any(char::is_control) {
        return Err(MambaError::Validation(format!(
            "{label} name must contain 1 to 120 printable characters"
        )));
    }
    Ok(value.to_string())
}

fn validate_external_id(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 200 || value.chars().any(char::is_control) {
        return Err(MambaError::Validation(
            "directory external ID must contain 1 to 200 printable characters".into(),
        ));
    }
    Ok(value.to_string())
}
