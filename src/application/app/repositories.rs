use chrono::Utc;

use super::RelayApp;
use super::authority::Permission;
use crate::domain::Repository;
use crate::error::{RelayError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl RelayApp {
    /// 登记一个可以在上面干活的代码仓库。
    ///
    /// 这里**不做** GitLab 连通性校验：网络调用留给调用方（CLI 或 HTTP 处理器），
    /// 它先用 `GitLabClient::check_project` 拿到真实的默认分支再传进来。这样应用层
    /// 保持同步且无 I/O，测试不需要 mock——和 `sync_external_artifacts` 的分工一致。
    pub fn register_repository(
        &mut self,
        name: &str,
        gitlab_project_path: &str,
        default_branch: &str,
        actor: &str,
    ) -> Result<Repository> {
        self.ensure_permission(actor, Permission::PrincipalManage)?;
        let name = validate_repository_name(name)?;
        let gitlab_project_path = validate_project_path(gitlab_project_path)?;
        let default_branch = validate_branch(default_branch)?;

        if self
            .state
            .repositories
            .values()
            .any(|repository| repository.active && repository.name.eq_ignore_ascii_case(&name))
        {
            return Err(RelayError::Validation(format!("仓库名已被占用：{name}")));
        }
        // 同一个 GitLab 项目登记两次会让「在哪个仓库干活」出现歧义。
        if self.state.repositories.values().any(|repository| {
            repository.active && repository.gitlab_project_path == gitlab_project_path
        }) {
            return Err(RelayError::Validation(format!(
                "GitLab 项目已经登记过了：{gitlab_project_path}"
            )));
        }

        let repository = Repository {
            id: new_id("REPO"),
            name,
            gitlab_project_path,
            default_branch,
            active: true,
            registered_by: actor.to_string(),
            created_at: Utc::now(),
        };
        self.commit(
            actor,
            vec![DomainEvent::RepositoryRegistered {
                repository: repository.clone(),
            }],
        )?;
        Ok(repository)
    }

    /// 归档一个仓库：不再能往上面派新活，但历史航班记录保持可读。
    pub fn archive_repository(&mut self, selector: &str, actor: &str) -> Result<Repository> {
        self.ensure_permission(actor, Permission::PrincipalManage)?;
        let repository = self.state.repository(selector)?.clone();
        self.commit(
            actor,
            vec![DomainEvent::RepositoryArchived {
                repository_id: repository.id.clone(),
                archived_by: actor.to_string(),
                archived_at: Utc::now(),
            }],
        )?;
        Ok(self
            .state
            .repositories
            .get(&repository.id)
            .cloned()
            .unwrap_or(repository))
    }

    pub fn repositories(&self) -> Vec<Repository> {
        let mut repositories: Vec<_> = self.state.repositories.values().cloned().collect();
        repositories.sort_by(|left, right| left.name.cmp(&right.name));
        repositories
    }
}

fn validate_repository_name(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 80 || value.chars().any(char::is_control) {
        return Err(RelayError::Validation(
            "仓库名需要 1 到 80 个可打印字符".into(),
        ));
    }
    Ok(value.to_string())
}

/// GitLab 的 `group/project` 路径。故意收紧字符集：这个值会被拼进 API URL，
/// 也会被 Worker 用来做本地仓库映射的键。
fn validate_project_path(value: &str) -> Result<String> {
    let value = value.trim();
    let shape_ok = !value.is_empty()
        && value.len() <= 200
        && value.contains('/')
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains("..")
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.'));
    if !shape_ok {
        return Err(RelayError::Validation(
            "GitLab 项目路径应形如 group/project，只允许字母、数字、- _ . 和 /".into(),
        ));
    }
    Ok(value.to_string())
}

fn validate_branch(value: &str) -> Result<String> {
    let value = value.trim();
    let shape_ok = !value.is_empty()
        && value.len() <= 120
        && !value.starts_with('-')
        && !value.contains("..")
        && !value.contains(' ')
        && value.chars().all(|c| !c.is_control());
    if !shape_ok {
        return Err(RelayError::Validation("分支名不合法".into()));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{TempDir, tempdir};

    fn app() -> (RelayApp, TempDir) {
        let directory = tempdir().unwrap();
        let mut app = RelayApp::open(directory.path()).unwrap();
        app.init_organization("EduMind", "admin").unwrap();
        (app, directory)
    }

    #[test]
    fn registering_a_repository_makes_it_selectable() {
        let (mut app, _directory) = app();
        let repository = app
            .register_repository("web-app", "acme/web-app", "main", "admin")
            .unwrap();

        assert!(repository.active);
        assert_eq!(repository.gitlab_project_path, "acme/web-app");
        // 按 ID 和按名称都要能查到。
        assert_eq!(
            app.state().repository(&repository.id).unwrap().id,
            repository.id
        );
        assert_eq!(app.state().repository("web-app").unwrap().id, repository.id);
    }

    #[test]
    fn the_same_gitlab_project_cannot_be_registered_twice() {
        let (mut app, _directory) = app();
        app.register_repository("web-app", "acme/web-app", "main", "admin")
            .unwrap();
        // 换个名字但指向同一个项目，会让「在哪个仓库干活」出现歧义。
        let error = app
            .register_repository("web-x", "acme/web-app", "main", "admin")
            .unwrap_err();
        assert!(matches!(error, RelayError::Validation(_)));
    }

    #[test]
    fn archived_repositories_can_no_longer_be_selected() {
        let (mut app, _directory) = app();
        let repository = app
            .register_repository("web-app", "acme/web-app", "main", "admin")
            .unwrap();
        app.archive_repository(&repository.id, "admin").unwrap();

        assert!(app.state().repository("web-app").is_err());
        // 归档只是停用，记录还在——历史航班要能读出当时用的是哪个仓库。
        assert_eq!(app.repositories().len(), 1);
        assert!(!app.repositories()[0].active);
    }

    #[test]
    fn archiving_frees_the_name_for_reuse() {
        let (mut app, _directory) = app();
        let first = app
            .register_repository("web-app", "acme/web-app", "main", "admin")
            .unwrap();
        app.archive_repository(&first.id, "admin").unwrap();
        let second = app
            .register_repository("web-app", "acme/web-app", "main", "admin")
            .unwrap();
        assert_ne!(first.id, second.id);
    }

    #[test]
    fn project_path_must_look_like_a_gitlab_path() {
        let (mut app, _directory) = app();
        for bad in ["web-app", "/acme/web-app", "acme/../etc", "a b/c"] {
            assert!(
                app.register_repository("x", bad, "main", "admin").is_err(),
                "应当拒绝: {bad}"
            );
        }
    }
}
