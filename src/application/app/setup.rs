use serde::Serialize;

use super::MambaApp;
use crate::domain::{
    ApiCredential, IssuedCredential, Organization, OrganizationRole, Principal, PrincipalKind,
    Team, Tenant,
};
use crate::error::{MambaError, Result};

const BOOTSTRAP_CREDENTIAL_LABEL: &str = "bootstrap-admin";

#[derive(Clone, Debug)]
pub struct InstallationSetupOptions {
    pub organization_name: String,
    pub team_name: String,
    pub administrator_name: String,
    pub capabilities: String,
    pub token_ttl_days: u32,
    pub rotate_token: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct SetupCreated {
    pub organization: bool,
    pub team: bool,
    pub administrator: bool,
    pub credential: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct InstallationSetup {
    pub tenant: Tenant,
    pub organization: Organization,
    pub team: Team,
    pub administrator: Principal,
    pub credential: ApiCredential,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub created: SetupCreated,
}

impl MambaApp {
    pub fn setup_installation(
        &mut self,
        options: InstallationSetupOptions,
    ) -> Result<InstallationSetup> {
        let organization_name = required_name(&options.organization_name, "organization")?;
        let team_name = required_name(&options.team_name, "team")?;
        let administrator_name = required_name(&options.administrator_name, "administrator")?;
        if !(1..=365).contains(&options.token_ttl_days) {
            return Err(MambaError::Validation(
                "credential TTL must be between 1 and 365 days".into(),
            ));
        }

        let (organization, organization_created) =
            if let Some(organization) = self.state.organization.clone() {
                if organization.name != organization_name {
                    return Err(MambaError::Validation(format!(
                        "installation already belongs to organization {}; requested {}",
                        organization.name, organization_name
                    )));
                }
                (organization, false)
            } else {
                (self.init_organization(&organization_name, "admin")?, true)
            };

        let (team, team_created) = if let Some(team) = self
            .state
            .teams
            .values()
            .find(|team| team.name.eq_ignore_ascii_case(&team_name))
            .cloned()
        {
            if !team.active {
                return Err(MambaError::Validation(format!(
                    "installation team is inactive: {}",
                    team.name
                )));
            }
            (team, false)
        } else {
            (
                self.create_team(&team_name, &options.capabilities, "admin")?,
                true,
            )
        };

        let (administrator, administrator_created) = if let Some(administrator) = self
            .state
            .principals
            .values()
            .find(|principal| principal.name.eq_ignore_ascii_case(&administrator_name))
            .cloned()
        {
            if administrator.kind != PrincipalKind::Human || !administrator.active {
                return Err(MambaError::Validation(format!(
                    "installation administrator must be an active Human: {}",
                    administrator.name
                )));
            }
            if administrator.team_id.as_deref() != Some(team.id.as_str()) {
                return Err(MambaError::Validation(format!(
                    "installation administrator {} is not assigned to team {}",
                    administrator.name, team.name
                )));
            }
            (administrator, false)
        } else {
            (
                self.register_principal(
                    &administrator_name,
                    PrincipalKind::Human,
                    Some(&team.id),
                    None,
                    &options.capabilities,
                    100,
                    None,
                    "admin",
                )?,
                true,
            )
        };

        if !self
            .state
            .has_role(&administrator.id, OrganizationRole::TenantAdmin)
        {
            self.grant_role(&administrator.id, OrganizationRole::TenantAdmin, "admin")?;
        }

        let active_bootstrap_credentials = self
            .state
            .credentials
            .values()
            .filter(|credential| {
                credential.principal_id == administrator.id
                    && credential.label == BOOTSTRAP_CREDENTIAL_LABEL
                    && credential.is_active()
            })
            .cloned()
            .collect::<Vec<_>>();

        if options.rotate_token {
            for credential in &active_bootstrap_credentials {
                self.revoke_api_credential(&credential.id, &administrator.id)?;
            }
        }

        let issued = if options.rotate_token || active_bootstrap_credentials.is_empty() {
            Some(self.issue_api_credential_with_ttl(
                &administrator.id,
                BOOTSTRAP_CREDENTIAL_LABEL,
                &administrator.id,
                options.token_ttl_days,
            )?)
        } else {
            None
        };
        let (credential, token, credential_created) = match issued {
            Some(IssuedCredential { credential, token }) => (credential, Some(token), true),
            None => {
                let credential = active_bootstrap_credentials
                    .into_iter()
                    .max_by_key(|credential| credential.created_at)
                    .ok_or_else(|| {
                        MambaError::Validation(
                            "installation has no active bootstrap credential".into(),
                        )
                    })?;
                (credential, None, false)
            }
        };

        Ok(InstallationSetup {
            tenant: self.state.tenant()?.clone(),
            organization,
            team,
            administrator,
            credential,
            token,
            created: SetupCreated {
                organization: organization_created,
                team: team_created,
                administrator: administrator_created,
                credential: credential_created,
            },
        })
    }
}

fn required_name(value: &str, label: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 120 {
        return Err(MambaError::Validation(format!(
            "{label} name must contain 1 to 120 characters"
        )));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn installation_setup_is_idempotent_and_rotates_only_on_request() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        let setup_options = options(false);
        let first = app.setup_installation(setup_options.clone()).unwrap();
        let first_token = first.token.clone().unwrap();
        assert!(first.created.organization);
        assert!(first.created.team);
        assert!(first.created.administrator);
        assert!(first.created.credential);
        assert!(app.state().flows.is_empty());
        assert_eq!(
            app.authenticate_api_token(&first_token)
                .unwrap()
                .unwrap()
                .id,
            first.administrator.id
        );
        let sequence = app.state().last_sequence;

        let repeated = app.setup_installation(setup_options).unwrap();
        assert_eq!(repeated.token, None);
        assert_eq!(app.state().last_sequence, sequence);
        assert_eq!(repeated.credential.id, first.credential.id);

        let rotated = app.setup_installation(options(true)).unwrap();
        let rotated_token = rotated.token.unwrap();
        assert!(rotated.created.credential);
        assert!(app.authenticate_api_token(&first_token).unwrap().is_none());
        assert!(
            app.authenticate_api_token(&rotated_token)
                .unwrap()
                .is_some()
        );

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        assert_eq!(replayed.state().flows.len(), 0);
        assert_eq!(replayed.state().principals.len(), 1);
        assert!(
            replayed
                .state()
                .has_role(&first.administrator.id, OrganizationRole::TenantAdmin)
        );
    }

    #[test]
    fn installation_setup_rejects_a_different_existing_organization() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path()).unwrap();
        app.setup_installation(options(false)).unwrap();
        let mut changed = options(false);
        changed.organization_name = "Another Company".into();
        assert!(matches!(
            app.setup_installation(changed),
            Err(MambaError::Validation(_))
        ));
    }

    #[test]
    fn installation_setup_validates_credentials_before_writing_state() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path()).unwrap();
        let mut invalid = options(false);
        invalid.token_ttl_days = 0;
        assert!(matches!(
            app.setup_installation(invalid),
            Err(MambaError::Validation(_))
        ));
        assert!(app.state().organization.is_none());
        assert_eq!(app.state().last_sequence, 0);
    }

    fn options(rotate_token: bool) -> InstallationSetupOptions {
        InstallationSetupOptions {
            organization_name: "Acme".into(),
            team_name: "Core Team".into(),
            administrator_name: "Admin".into(),
            capabilities: "product,delivery,operations".into(),
            token_ttl_days: 30,
            rotate_token,
        }
    }
}
