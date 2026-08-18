use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use sha2::{Digest, Sha256};

use crate::error::{RelayError, Result};

#[derive(Clone, Debug)]
pub struct WorktreeArtifact {
    pub base_revision: String,
    pub changed_files: Vec<String>,
    pub patch_path: Option<PathBuf>,
    pub patch_sha256: Option<String>,
}

pub struct IsolatedWorktree {
    repository: PathBuf,
    root: PathBuf,
    workspace: PathBuf,
    base_revision: String,
    attached: bool,
}

impl IsolatedWorktree {
    pub fn create(source_workspace: &Path, root: PathBuf) -> Result<Self> {
        let source_workspace = source_workspace.canonicalize()?;
        let repository = git_text(&source_workspace, &["rev-parse", "--show-toplevel"])?;
        let repository = PathBuf::from(repository).canonicalize()?;
        let relative_workspace = source_workspace.strip_prefix(&repository).map_err(|_| {
            RelayError::Validation("workspace is not inside its Git repository".into())
        })?;
        let status = git_text(&repository, &["status", "--porcelain"])?;
        if !status.is_empty() {
            return Err(RelayError::Validation(
                "remote execute requires a clean source worktree".into(),
            ));
        }
        let base_revision = git_text(&repository, &["rev-parse", "HEAD"])?;
        if root.exists() {
            return Err(RelayError::Validation(format!(
                "isolated worktree already exists: {}",
                root.display()
            )));
        }
        if let Some(parent) = root.parent() {
            fs::create_dir_all(parent)?;
        }
        git_ok(
            &repository,
            &[
                "worktree",
                "add",
                "--detach",
                path_arg(&root)?,
                &base_revision,
            ],
        )?;
        let workspace = root.join(relative_workspace);
        Ok(Self {
            repository,
            root,
            workspace,
            base_revision,
            attached: true,
        })
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    pub fn resume(source_workspace: &Path, root: PathBuf) -> Result<Self> {
        let source_workspace = source_workspace.canonicalize()?;
        let repository = PathBuf::from(git_text(
            &source_workspace,
            &["rev-parse", "--show-toplevel"],
        )?)
        .canonicalize()?;
        let relative_workspace = source_workspace.strip_prefix(&repository).map_err(|_| {
            RelayError::Validation("workspace is not inside its Git repository".into())
        })?;
        let root = root.canonicalize().map_err(|_| {
            RelayError::Validation("active flight has no resumable isolated worktree".into())
        })?;
        let worktree_repository =
            PathBuf::from(git_text(&root, &["rev-parse", "--show-toplevel"])?);
        if worktree_repository.canonicalize()? != root {
            return Err(RelayError::Validation(
                "active flight worktree root does not match its Git repository".into(),
            ));
        }
        let source_common = git_path(&repository, &["rev-parse", "--git-common-dir"])?;
        let worktree_common = git_path(&root, &["rev-parse", "--git-common-dir"])?;
        if source_common != worktree_common {
            return Err(RelayError::Validation(
                "active flight worktree belongs to another Git repository".into(),
            ));
        }
        let base_revision = git_text(&root, &["rev-parse", "HEAD"])?;
        let workspace = root.join(relative_workspace);
        if !workspace.is_dir() {
            return Err(RelayError::InvalidWorkspace(workspace));
        }
        Ok(Self {
            repository,
            root,
            workspace,
            base_revision,
            attached: true,
        })
    }

    pub fn collect(&self, patch_path: &Path) -> Result<WorktreeArtifact> {
        git_ok(&self.root, &["add", "-A"])?;
        let patch = git_output(
            &self.root,
            &["diff", "--cached", "--binary", "--no-ext-diff"],
        )?
        .stdout;
        let names = git_output(&self.root, &["diff", "--cached", "--name-only", "-z"])?.stdout;
        let changed_files = names
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .map(|path| String::from_utf8_lossy(path).into_owned())
            .collect::<Vec<_>>();
        let (patch_path, patch_sha256) = if patch.is_empty() {
            (None, None)
        } else {
            if let Some(parent) = patch_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(patch_path, &patch)?;
            (Some(patch_path.to_path_buf()), Some(hex_digest(&patch)))
        };
        Ok(WorktreeArtifact {
            base_revision: self.base_revision.clone(),
            changed_files,
            patch_path,
            patch_sha256,
        })
    }

    pub fn cleanup(&mut self) -> Result<()> {
        if !self.attached {
            return Ok(());
        }
        git_ok(
            &self.repository,
            &["worktree", "remove", "--force", path_arg(&self.root)?],
        )?;
        self.attached = false;
        Ok(())
    }
}

/// 推送成功后的分支信息。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedBranch {
    pub branch: String,
    pub commit: String,
}

impl IsolatedWorktree {
    /// 把工作副本里的改动提交并推到远端分支。
    ///
    /// 认证走临时的 `http.extraHeader`，不写进 remote URL——URL 会进 reflog、
    /// 进 `git remote -v`、也可能被别的进程看到，令牌放那里等于泄露。
    ///
    /// 分支名由调用方给（约定 `relay/<任务ID>`）。用 `--force-with-lease` 之外
    /// 的普通推送：同名分支已存在说明上一次执行的结果还在，不该悄悄覆盖。
    pub fn publish(
        &self,
        branch: &str,
        remote_url: &str,
        token: &str,
        message: &str,
        push_options: &[String],
    ) -> Result<Option<PublishedBranch>> {
        git_ok(&self.root, &["add", "-A"])?;
        let staged = git_output(&self.root, &["diff", "--cached", "--name-only"])?.stdout;
        if staged.is_empty() {
            // 没有改动就不要造一个空分支出来。
            return Ok(None);
        }
        git_ok(
            &self.root,
            &[
                "-c",
                "user.email=relay@localhost",
                "-c",
                "user.name=Relay",
                "commit",
                "-m",
                message,
            ],
        )?;
        let commit = git_text(&self.root, &["rev-parse", "HEAD"])?;
        let auth_header = format!("http.extraHeader=PRIVATE-TOKEN: {token}");
        let refspec = format!("HEAD:refs/heads/{branch}");
        let mut args: Vec<&str> = vec!["-c", &auth_header, "push"];
        // push option 建 MR 只需要 write_repository；走 API 建则要 api scope。
        // 用前者可以让 Worker 的令牌保持在"只能推这个仓库"的最小权限上。
        let option_args: Vec<String> = push_options
            .iter()
            .flat_map(|option| ["-o".to_string(), option.clone()])
            .collect();
        args.extend(option_args.iter().map(String::as_str));
        args.push(remote_url);
        args.push(&refspec);
        git_ok(&self.root, &args)?;
        Ok(Some(PublishedBranch {
            branch: branch.to_string(),
            commit,
        }))
    }
}

impl Drop for IsolatedWorktree {
    fn drop(&mut self) {
        if self.attached {
            let _ = Command::new("git")
                .arg("-C")
                .arg(&self.repository)
                .args(["worktree", "remove", "--force"])
                .arg(&self.root)
                .output();
        }
    }
}

pub fn sha256_file(path: &Path) -> Result<String> {
    Ok(hex_digest(&fs::read(path)?))
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn git_text(workspace: &Path, args: &[&str]) -> Result<String> {
    let output = git_output(workspace, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_path(workspace: &Path, args: &[&str]) -> Result<PathBuf> {
    let path = PathBuf::from(git_text(workspace, args)?);
    if path.is_absolute() {
        path.canonicalize().map_err(Into::into)
    } else {
        workspace.join(path).canonicalize().map_err(Into::into)
    }
}

fn git_ok(workspace: &Path, args: &[&str]) -> Result<()> {
    git_output(workspace, args).map(|_| ())
}

fn git_output(workspace: &Path, args: &[&str]) -> Result<Output> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(args)
        .output()
        .map_err(|error| {
            RelayError::Validation(format!(
                "could not start Git for isolated worktree: {error}"
            ))
        })?;
    if !output.status.success() {
        let message = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(RelayError::Validation(format!(
            "Git worktree operation failed: {message}"
        )));
    }
    Ok(output)
}

fn path_arg(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| RelayError::Validation("worktree path is not valid UTF-8".into()))
}

#[cfg(test)]
mod tests {
    #[test]
    fn publish_commits_and_reports_the_branch() {
        // 用一个本地裸仓库当远端：不需要真的 GitLab 就能验证提交与推送。
        let source = tempdir().unwrap();
        let repository = source.path().join("repo");
        fs::create_dir_all(&repository).unwrap();
        git_ok(&repository, &["init", "-q"]).unwrap();
        git_ok(&repository, &["config", "user.email", "t@example.com"]).unwrap();
        git_ok(&repository, &["config", "user.name", "T"]).unwrap();
        fs::write(repository.join("README.md"), "base").unwrap();
        git_ok(&repository, &["add", "README.md"]).unwrap();
        git_ok(&repository, &["commit", "-qm", "base"]).unwrap();

        let remote = source.path().join("remote.git");
        git_ok(
            source.path(),
            &["init", "-q", "--bare", remote.to_str().unwrap()],
        )
        .unwrap();

        let worktree = IsolatedWorktree::create(&repository, source.path().join("work")).unwrap();
        fs::write(worktree.workspace().join("added.txt"), "changed").unwrap();

        let published = worktree
            .publish(
                "relay/TSK-1",
                remote.to_str().unwrap(),
                "unused-for-local-remote",
                "relay: TSK-1",
                &[],
            )
            .unwrap()
            .expect("有改动就应该产出分支");
        assert_eq!(published.branch, "relay/TSK-1");
        assert_eq!(published.commit.len(), 40);

        // 远端确实收到了这个分支。
        let refs = git_text(&remote, &["for-each-ref", "--format=%(refname)"]).unwrap();
        assert!(
            refs.contains("refs/heads/relay/TSK-1"),
            "远端分支未创建: {refs}"
        );
    }

    #[test]
    fn publish_without_changes_creates_no_branch() {
        let source = tempdir().unwrap();
        let repository = source.path().join("repo");
        fs::create_dir_all(&repository).unwrap();
        git_ok(&repository, &["init", "-q"]).unwrap();
        git_ok(&repository, &["config", "user.email", "t@example.com"]).unwrap();
        git_ok(&repository, &["config", "user.name", "T"]).unwrap();
        fs::write(repository.join("README.md"), "base").unwrap();
        git_ok(&repository, &["add", "README.md"]).unwrap();
        git_ok(&repository, &["commit", "-qm", "base"]).unwrap();
        let remote = source.path().join("remote.git");
        git_ok(
            source.path(),
            &["init", "-q", "--bare", remote.to_str().unwrap()],
        )
        .unwrap();

        let worktree = IsolatedWorktree::create(&repository, source.path().join("work")).unwrap();
        // 一行没改：不该推一个空分支出去。
        assert!(
            worktree
                .publish(
                    "relay/TSK-2",
                    remote.to_str().unwrap(),
                    "t",
                    "relay: TSK-2",
                    &[]
                )
                .unwrap()
                .is_none()
        );
    }

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn isolated_worktree_collects_patch_without_touching_source() {
        let directory = tempdir().unwrap();
        let repository = directory.path().join("repo");
        fs::create_dir_all(&repository).unwrap();
        git_ok(&repository, &["init", "-q"]).unwrap();
        git_ok(&repository, &["config", "user.email", "test@example.com"]).unwrap();
        git_ok(&repository, &["config", "user.name", "Test"]).unwrap();
        fs::write(repository.join("README.md"), "base\n").unwrap();
        git_ok(&repository, &["add", "README.md"]).unwrap();
        git_ok(&repository, &["commit", "-qm", "base"]).unwrap();

        let root = directory.path().join("worktree");
        let mut worktree = IsolatedWorktree::create(&repository, root.clone()).unwrap();
        fs::write(worktree.workspace().join("README.md"), "changed\n").unwrap();
        fs::write(worktree.workspace().join("new.txt"), "new\n").unwrap();
        let patch = directory.path().join("result.patch");
        let artifact = worktree.collect(&patch).unwrap();

        assert_eq!(
            fs::read_to_string(repository.join("README.md")).unwrap(),
            "base\n"
        );
        assert_eq!(artifact.changed_files, ["README.md", "new.txt"]);
        assert!(artifact.patch_sha256.is_some());
        assert!(artifact.patch_path.unwrap().is_file());
        worktree.cleanup().unwrap();
        assert!(!root.exists());
    }

    #[test]
    fn active_worktree_can_resume_after_worker_exit() {
        let directory = tempdir().unwrap();
        let repository = directory.path().join("repo");
        fs::create_dir_all(&repository).unwrap();
        git_ok(&repository, &["init", "-q"]).unwrap();
        git_ok(&repository, &["config", "user.email", "test@example.com"]).unwrap();
        git_ok(&repository, &["config", "user.name", "Test"]).unwrap();
        fs::write(repository.join("README.md"), "base\n").unwrap();
        git_ok(&repository, &["add", "README.md"]).unwrap();
        git_ok(&repository, &["commit", "-qm", "base"]).unwrap();

        let root = directory.path().join("worktree");
        let worktree = IsolatedWorktree::create(&repository, root.clone()).unwrap();
        fs::write(worktree.workspace().join("partial.txt"), "partial\n").unwrap();
        std::mem::forget(worktree);

        let mut resumed = IsolatedWorktree::resume(&repository, root.clone()).unwrap();
        let artifact = resumed
            .collect(&directory.path().join("resumed.patch"))
            .unwrap();
        assert_eq!(artifact.changed_files, ["partial.txt"]);
        resumed.cleanup().unwrap();
        assert!(!root.exists());
    }
}
