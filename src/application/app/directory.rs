use chrono::Utc;

use super::RelayApp;
use super::authority::Permission;
use crate::domain::{
    ExternalIdentityBinding, OrganizationRole, Principal, PrincipalKind, RoleBinding, Team,
};
use crate::error::{RelayError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl RelayApp {
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
            return Err(RelayError::Validation(format!(
                "principal already exists: {name}"
            )));
        }
        if self.state.principals.values().any(|principal| {
            principal
                .directory_username
                .as_deref()
                .is_some_and(|current| current.eq_ignore_ascii_case(&user_name))
        }) {
            return Err(RelayError::Validation(format!(
                "directory userName already exists: {user_name}"
            )));
        }
        if self.state.external_identities.values().any(|binding| {
            binding.is_active()
                && binding.provider == "oidc"
                && binding.external_user_id == external_user_id
        }) {
            return Err(RelayError::Validation(
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
            return Err(RelayError::Validation(
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
            return Err(RelayError::Validation(format!(
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
            return Err(RelayError::Validation(
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
            return Err(RelayError::PermissionDenied(
                "OIDC identity is not an active Human".into(),
            ));
        }
        Ok(principal.clone())
    }

    /// 用飞书 `open_id` 找到对应 Human；没有就自动开户。
    ///
    /// 这里**有意**放宽了 OIDC 那条链路「登录不自动扩张组织边界」的约束：飞书侧
    /// 已经按 `tenant_key` 确认过是本企业员工（见 `feishu_auth.rs`），再要求管理员
    /// 逐个预建 Principal 就谈不上零侵入了。自动开户只给最小的 `Member` 角色，
    /// 建 Demand、看组织看板等仍需管理员另行授予。
    ///
    /// 绑定用的 provider 是 `feishu`，与交互网关（docs/INTERACTIONS.md）共用同一条
    /// 记录——本来就是同一个人的同一个飞书身份，登录之后在飞书里点按钮也能认出来。
    ///
    /// 调用方必须先做完 `tenant_key` 校验，本方法不重复判断。
    pub fn feishu_principal(&mut self, open_id: &str, display_name: &str) -> Result<Principal> {
        let open_id = validate_external_id(open_id)?;
        if let Ok(binding) = self.state.external_identity("feishu", &open_id) {
            let principal = self.state.principal(&binding.principal_id)?;
            if principal.kind != PrincipalKind::Human || !principal.active {
                return Err(RelayError::PermissionDenied(
                    "Feishu identity is not an active Human".into(),
                ));
            }
            return Ok(principal.clone());
        }

        const ACTOR: &str = "tower://feishu";
        let name = self.available_principal_name(display_name, &open_id)?;
        let now = Utc::now();
        let principal = Principal {
            id: new_id("HUM"),
            name,
            directory_username: None,
            kind: PrincipalKind::Human,
            team_id: None,
            owner_id: None,
            capabilities: Vec::new(),
            capacity_percent: 100,
            executor: None,
            active: true,
            created_at: now,
        };
        let role = RoleBinding {
            id: new_id("ROLE"),
            tenant_id: self.state.tenant()?.id.clone(),
            organization_id: self.state.organization()?.id.clone(),
            principal_id: principal.id.clone(),
            role: OrganizationRole::Member,
            granted_by: ACTOR.to_string(),
            granted_at: now,
            revoked_by: None,
            revoked_at: None,
        };
        let binding = ExternalIdentityBinding {
            id: new_id("XID"),
            provider: "feishu".into(),
            external_user_id: open_id,
            principal_id: principal.id.clone(),
            bound_by: ACTOR.to_string(),
            bound_at: now,
            unbound_by: None,
            unbound_at: None,
        };
        self.commit(
            ACTOR,
            vec![
                DomainEvent::PrincipalRegistered {
                    principal: principal.clone(),
                },
                DomainEvent::RoleGranted { binding: role },
                DomainEvent::ExternalIdentityBound { binding },
            ],
        )?;
        Ok(principal)
    }

    /// 同名同事不能因此登录失败，所以重名时用 open_id 尾段消歧，而不是报错。
    fn available_principal_name(&self, display_name: &str, open_id: &str) -> Result<String> {
        let base = validate_directory_name(display_name, "Human")?;
        if !self.principal_name_taken(&base) {
            return Ok(base);
        }
        let suffix: String = open_id
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .rev()
            .take(6)
            .collect();
        let candidate = format!("{base} ({suffix})");
        if !self.principal_name_taken(&candidate) {
            return Ok(candidate);
        }
        // 兜底：open_id 本身在租户内唯一。
        Ok(open_id.to_string())
    }

    fn principal_name_taken(&self, name: &str) -> bool {
        self.state
            .principals
            .values()
            .any(|principal| principal.name.eq_ignore_ascii_case(name))
    }
}

fn validate_directory_name(value: &str, label: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 120 || value.chars().any(char::is_control) {
        return Err(RelayError::Validation(format!(
            "{label} name must contain 1 to 120 printable characters"
        )));
    }
    Ok(value.to_string())
}

fn validate_external_id(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 200 || value.chars().any(char::is_control) {
        return Err(RelayError::Validation(
            "directory external ID must contain 1 to 200 printable characters".into(),
        ));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// 返回值里必须保留 `TempDir`，drop 掉它目录就没了。
    fn app() -> (RelayApp, tempfile::TempDir) {
        let directory = tempdir().unwrap();
        let mut app = RelayApp::open(directory.path()).unwrap();
        app.init_organization("EduMind", "admin").unwrap();
        (app, directory)
    }

    #[test]
    fn first_feishu_login_provisions_a_member_and_binds_the_open_id() {
        let (mut app, _directory) = app();
        let principal = app.feishu_principal("ou_alice", "李伟").unwrap();

        assert_eq!(principal.name, "李伟");
        assert_eq!(principal.kind, PrincipalKind::Human);
        assert!(principal.active);
        // 自动开户只给最小角色，建 Demand、看组织看板仍需管理员另行授予。
        let roles = app.state.roles_for(&principal.id);
        assert_eq!(roles, vec![OrganizationRole::Member]);
        // 绑定的 provider 必须是 feishu，这样交互网关能复用同一条记录。
        let binding = app.state.external_identity("feishu", "ou_alice").unwrap();
        assert_eq!(binding.principal_id, principal.id);
    }

    #[test]
    fn repeated_logins_reuse_the_same_principal() {
        let (mut app, _directory) = app();
        let first = app.feishu_principal("ou_alice", "李伟").unwrap();
        // 改了飞书昵称也不能变成另一个人。
        let second = app.feishu_principal("ou_alice", "李伟(已改名)").unwrap();

        assert_eq!(first.id, second.id);
        assert_eq!(second.name, "李伟");
        assert_eq!(app.state.principals.len(), 1);
    }

    #[test]
    fn a_second_person_with_the_same_display_name_still_gets_an_account() {
        let (mut app, _directory) = app();
        let first = app.feishu_principal("ou_alice", "李伟").unwrap();
        let second = app.feishu_principal("ou_bob", "李伟").unwrap();

        assert_ne!(first.id, second.id);
        assert_ne!(first.name, second.name);
        assert!(second.name.starts_with("李伟"));
        assert_eq!(app.state.principals.len(), 2);
    }

    #[test]
    fn a_deactivated_person_cannot_sign_back_in() {
        let (mut app, _directory) = app();
        let principal = app.feishu_principal("ou_alice", "李伟").unwrap();
        app.update_directory_human(&principal.id, "李伟", "liwei", None, false, "admin")
            .unwrap();

        // 停用之后不能因为「找不到活动账号」就重新开一个。
        let error = app.feishu_principal("ou_alice", "李伟").unwrap_err();
        assert!(matches!(error, RelayError::PermissionDenied(_)));
        assert_eq!(app.state.principals.len(), 1);
    }
}
