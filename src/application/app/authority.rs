use chrono::Utc;

use super::MambaApp;
use crate::domain::{OrganizationRole, PrincipalKind, RoleBinding, Tenant};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Permission {
    OrganizationRead,
    OrganizationManage,
    PrincipalManage,
    AuthorityRead,
    AuthorityManage,
    CredentialManage,
    DemandCreate,
    DashboardRead,
    NotificationManage,
    AuditRead,
}

impl MambaApp {
    pub fn authorize_organization_read(&self, actor: &str) -> Result<()> {
        self.ensure_permission(actor, Permission::OrganizationRead)
    }

    pub fn authorize_principal_admin(&self, actor: &str) -> Result<()> {
        self.ensure_permission(actor, Permission::PrincipalManage)
    }

    pub fn authorize_credential_admin(&self, actor: &str) -> Result<()> {
        self.ensure_permission(actor, Permission::CredentialManage)
    }

    pub fn authorize_notification_admin(&self, actor: &str) -> Result<()> {
        self.ensure_permission(actor, Permission::NotificationManage)
    }

    pub fn authorize_audit(&self, actor: &str) -> Result<()> {
        self.ensure_permission(actor, Permission::AuditRead)
    }

    pub fn role_bindings(
        &self,
        target: &str,
        actor: &str,
        include_revoked: bool,
    ) -> Result<Vec<RoleBinding>> {
        let target = self.state.principal(target)?;
        let actor_is_target = self
            .state
            .principal(actor)
            .is_ok_and(|principal| principal.id == target.id);
        if !actor_is_target {
            self.ensure_permission(actor, Permission::AuthorityRead)?;
        }
        let mut bindings = self
            .state
            .role_bindings
            .values()
            .filter(|binding| binding.principal_id == target.id)
            .filter(|binding| include_revoked || binding.is_active())
            .cloned()
            .collect::<Vec<_>>();
        bindings.sort_by_key(|binding| binding.granted_at);
        Ok(bindings)
    }

    pub fn grant_role(
        &mut self,
        target: &str,
        role: OrganizationRole,
        actor: &str,
    ) -> Result<RoleBinding> {
        self.ensure_permission(actor, Permission::AuthorityManage)?;
        let actor_principal = self.state.principal(actor).ok().cloned();
        let actor_name = actor_principal
            .as_ref()
            .map(|principal| principal.name.clone())
            .unwrap_or_else(|| actor.to_string());
        let target = self.state.principal(target)?.clone();
        validate_role_kind(target.kind.clone(), role)?;
        if role == OrganizationRole::TenantAdmin
            && actor_principal.as_ref().is_some_and(|principal| {
                !self
                    .state
                    .has_role(&principal.id, OrganizationRole::TenantAdmin)
            })
        {
            return Err(MambaError::PermissionDenied(
                "only a tenant admin can grant tenant_admin".into(),
            ));
        }
        if self.state.has_role(&target.id, role) {
            return Err(MambaError::Validation(format!(
                "{} already has role {role}",
                target.name
            )));
        }
        let binding = RoleBinding {
            id: new_id("ROLE"),
            tenant_id: self.state.tenant()?.id.clone(),
            organization_id: self.state.organization()?.id.clone(),
            principal_id: target.id,
            role,
            granted_by: actor_name.clone(),
            granted_at: Utc::now(),
            revoked_by: None,
            revoked_at: None,
        };
        self.commit(
            &actor_name,
            vec![DomainEvent::RoleGranted {
                binding: binding.clone(),
            }],
        )?;
        Ok(binding)
    }

    pub fn revoke_role(&mut self, binding_id: &str, actor: &str) -> Result<RoleBinding> {
        self.ensure_permission(actor, Permission::AuthorityManage)?;
        let actor_name = self
            .state
            .principal(actor)
            .map(|principal| principal.name.clone())
            .unwrap_or_else(|_| actor.to_string());
        let binding = self
            .state
            .role_bindings
            .get(binding_id)
            .filter(|binding| binding.is_active())
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "active role binding",
                id: binding_id.to_string(),
            })?;
        if binding.role == OrganizationRole::TenantAdmin {
            let active_admins = self
                .state
                .role_bindings
                .values()
                .filter(|candidate| {
                    candidate.is_active() && candidate.role == OrganizationRole::TenantAdmin
                })
                .count();
            if active_admins == 1 {
                return Err(MambaError::InvalidTransition(
                    "cannot revoke the last tenant admin".into(),
                ));
            }
        }
        self.commit(
            &actor_name,
            vec![DomainEvent::RoleRevoked {
                binding_id: binding.id.clone(),
                revoked_by: actor_name.clone(),
                revoked_at: Utc::now(),
            }],
        )?;
        Ok(self.state.role_bindings[binding_id].clone())
    }

    pub(crate) fn ensure_permission(&self, actor: &str, permission: Permission) -> Result<()> {
        let principal = match self.state.principal(actor) {
            Ok(principal) => principal,
            Err(MambaError::NotFound { .. }) if is_system_actor(actor) => return Ok(()),
            Err(error) => return Err(error),
        };
        if self
            .state
            .roles_for(&principal.id)
            .into_iter()
            .any(|role| role_allows(role, permission))
        {
            Ok(())
        } else {
            Err(MambaError::PermissionDenied(format!(
                "{} lacks permission {:?}",
                principal.name, permission
            )))
        }
    }

    pub(super) fn migrate_legacy_authority(&mut self) -> Result<()> {
        let Some(organization) = self.state.organization.clone() else {
            return Ok(());
        };
        let tenant = self.state.tenant.clone().unwrap_or_else(|| Tenant {
            id: new_id("TEN"),
            name: organization.name.clone(),
            created_at: organization.created_at,
        });
        let mut events = Vec::new();
        if self.state.tenant.is_none() {
            events.push(DomainEvent::TenantInitialized {
                tenant: tenant.clone(),
            });
        }

        let mut principals = self.state.principals.values().cloned().collect::<Vec<_>>();
        principals.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        let mut tenant_admin_assigned =
            self.state.role_bindings.values().any(|binding| {
                binding.is_active() && binding.role == OrganizationRole::TenantAdmin
            });
        for principal in principals {
            if self
                .state
                .role_bindings
                .values()
                .any(|binding| binding.is_active() && binding.principal_id == principal.id)
            {
                continue;
            }
            let role = match principal.kind {
                PrincipalKind::Human if !tenant_admin_assigned => {
                    tenant_admin_assigned = true;
                    OrganizationRole::TenantAdmin
                }
                PrincipalKind::Human => OrganizationRole::Member,
                PrincipalKind::Agent => OrganizationRole::Agent,
            };
            events.push(DomainEvent::RoleGranted {
                binding: RoleBinding {
                    id: new_id("ROLE"),
                    tenant_id: tenant.id.clone(),
                    organization_id: organization.id.clone(),
                    principal_id: principal.id,
                    role,
                    granted_by: "tower://authority-migration".into(),
                    granted_at: Utc::now(),
                    revoked_by: None,
                    revoked_at: None,
                },
            });
        }
        if !events.is_empty() {
            self.commit_as(&organization.id, "tower://authority-migration", events)?;
        }
        Ok(())
    }
}

fn validate_role_kind(kind: PrincipalKind, role: OrganizationRole) -> Result<()> {
    match (kind, role) {
        (PrincipalKind::Agent, OrganizationRole::Agent) => Ok(()),
        (PrincipalKind::Agent, _) => Err(MambaError::Validation(
            "an Agent principal can only hold the agent role".into(),
        )),
        (PrincipalKind::Human, OrganizationRole::Agent) => Err(MambaError::Validation(
            "a Human principal cannot hold the agent role".into(),
        )),
        (PrincipalKind::Human, _) => Ok(()),
    }
}

fn is_system_actor(actor: &str) -> bool {
    actor == "admin" || actor.starts_with("tower://")
}

fn role_allows(role: OrganizationRole, permission: Permission) -> bool {
    match role {
        OrganizationRole::TenantAdmin => true,
        OrganizationRole::OrganizationAdmin => true,
        OrganizationRole::Manager => matches!(
            permission,
            Permission::OrganizationRead
                | Permission::DemandCreate
                | Permission::DashboardRead
                | Permission::AuditRead
                | Permission::AuthorityRead
        ),
        OrganizationRole::Auditor => matches!(
            permission,
            Permission::OrganizationRead
                | Permission::DashboardRead
                | Permission::AuditRead
                | Permission::AuthorityRead
        ),
        OrganizationRole::Member | OrganizationRole::Agent => {
            permission == Permission::OrganizationRead
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn default_roles_are_minimal_grantable_revocable_and_replayable() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = MambaApp::open(&data_dir).unwrap();
        app.init_organization("Mamba", "admin").unwrap();
        let team = app
            .create_team("Platform", "product,rust", "admin")
            .unwrap();
        let admin = app
            .register_principal(
                "Admin",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product",
                100,
                None,
                "admin",
            )
            .unwrap();
        let member = app
            .register_principal(
                "Member",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "rust",
                100,
                None,
                &admin.name,
            )
            .unwrap();
        let agent = app
            .register_principal(
                "Member Agent",
                PrincipalKind::Agent,
                Some(&team.id),
                Some(&member.id),
                "rust",
                100,
                None,
                &admin.name,
            )
            .unwrap();

        assert!(app.state.has_role(&admin.id, OrganizationRole::TenantAdmin));
        assert!(app.state.has_role(&member.id, OrganizationRole::Member));
        assert!(app.state.has_role(&agent.id, OrganizationRole::Agent));
        assert!(matches!(
            app.admin_dashboard(&member.id),
            Err(MambaError::PermissionDenied(_))
        ));

        let manager = app
            .grant_role(&member.id, OrganizationRole::Manager, &admin.id)
            .unwrap();
        app.admin_dashboard(&member.id).unwrap();
        assert!(matches!(
            app.grant_role(&agent.id, OrganizationRole::Manager, &admin.id),
            Err(MambaError::Validation(_))
        ));
        let admin_binding = app
            .role_bindings(&admin.id, &admin.id, false)
            .unwrap()
            .into_iter()
            .find(|binding| binding.role == OrganizationRole::TenantAdmin)
            .unwrap();
        assert!(matches!(
            app.revoke_role(&admin_binding.id, &admin.id),
            Err(MambaError::InvalidTransition(_))
        ));
        app.revoke_role(&manager.id, &admin.id).unwrap();
        assert!(matches!(
            app.admin_dashboard(&member.id),
            Err(MambaError::PermissionDenied(_))
        ));

        drop(app);
        let replayed = MambaApp::open(&data_dir).unwrap();
        assert!(
            replayed
                .state
                .has_role(&admin.id, OrganizationRole::TenantAdmin)
        );
        assert!(replayed.state.tenant.is_some());
        assert!(!replayed.state.role_bindings[&manager.id].is_active());
    }
}
