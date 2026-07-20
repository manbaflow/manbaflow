use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use sha2::{Digest, Sha256};

use crate::error::{MambaError, Result};

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
            MambaError::Validation("workspace is not inside its Git repository".into())
        })?;
        let status = git_text(&repository, &["status", "--porcelain"])?;
        if !status.is_empty() {
            return Err(MambaError::Validation(
                "remote execute requires a clean source worktree".into(),
            ));
        }
        let base_revision = git_text(&repository, &["rev-parse", "HEAD"])?;
        if root.exists() {
            return Err(MambaError::Validation(format!(
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
            MambaError::Validation("workspace is not inside its Git repository".into())
        })?;
        let root = root.canonicalize().map_err(|_| {
            MambaError::Validation("active flight has no resumable isolated worktree".into())
        })?;
        let worktree_repository =
            PathBuf::from(git_text(&root, &["rev-parse", "--show-toplevel"])?);
        if worktree_repository.canonicalize()? != root {
            return Err(MambaError::Validation(
                "active flight worktree root does not match its Git repository".into(),
            ));
        }
        let source_common = git_path(&repository, &["rev-parse", "--git-common-dir"])?;
        let worktree_common = git_path(&root, &["rev-parse", "--git-common-dir"])?;
        if source_common != worktree_common {
            return Err(MambaError::Validation(
                "active flight worktree belongs to another Git repository".into(),
            ));
        }
        let base_revision = git_text(&root, &["rev-parse", "HEAD"])?;
        let workspace = root.join(relative_workspace);
        if !workspace.is_dir() {
            return Err(MambaError::InvalidWorkspace(workspace));
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
            MambaError::Validation(format!(
                "could not start Git for isolated worktree: {error}"
            ))
        })?;
    if !output.status.success() {
        let message = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(MambaError::Validation(format!(
            "Git worktree operation failed: {message}"
        )));
    }
    Ok(output)
}

fn path_arg(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| MambaError::Validation("worktree path is not valid UTF-8".into()))
}

#[cfg(test)]
mod tests {
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
