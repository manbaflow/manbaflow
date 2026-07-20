use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use tokio::process::Command;

use crate::domain::ExecutionSandboxReport;
use crate::error::{MambaError, Result};

pub const CONTAINER_WORKSPACE: &str = "/workspace";
pub const CONTAINER_OUTPUT: &str = "/mamba-output";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxBackend {
    Process,
    Docker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxNetwork {
    None,
    Bridge,
}

impl SandboxNetwork {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Bridge => "bridge",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DockerSandboxConfig {
    pub runtime: PathBuf,
    pub image: String,
    pub network: SandboxNetwork,
    pub cpus_millis: u32,
    pub memory_mb: u64,
    pub pids_limit: u32,
    pub tmpfs_mb: u64,
    pub user: Option<String>,
    pub environment: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedDockerSandbox {
    config: DockerSandboxConfig,
    image_id: String,
    user: String,
}

pub struct DockerRunSpec<'a> {
    pub name: &'a str,
    pub workspace: &'a Path,
    pub workspace_writable: bool,
    pub output_dir: &'a Path,
    pub program: &'a OsStr,
    pub args: &'a [OsString],
}

pub struct DockerContainerGuard {
    runtime: PathBuf,
    name: String,
}

impl DockerSandboxConfig {
    pub fn resolve(self) -> Result<ResolvedDockerSandbox> {
        self.validate()?;
        let output = StdCommand::new(&self.runtime)
            .args(["image", "inspect", "--format", "{{.Id}}"])
            .arg(&self.image)
            .output()
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    MambaError::ExecutorUnavailable(self.runtime.display().to_string())
                } else {
                    MambaError::ExternalConnector(format!(
                        "could not inspect Docker sandbox image: {error}"
                    ))
                }
            })?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr);
            return Err(MambaError::Validation(format!(
                "Docker sandbox image {} is unavailable locally (--pull=never): {}",
                self.image,
                message.trim()
            )));
        }
        let image_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !valid_sha256_id(&image_id) {
            return Err(MambaError::Validation(
                "Docker image inspect did not return a sha256 image ID".into(),
            ));
        }
        let user = self.user.clone().unwrap_or(current_user()?);
        validate_user(&user)?;
        Ok(ResolvedDockerSandbox {
            config: self,
            image_id,
            user,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.runtime.as_os_str().is_empty() {
            return Err(MambaError::Validation(
                "Docker runtime command cannot be empty".into(),
            ));
        }
        if self.image.is_empty()
            || self.image.len() > 512
            || self.image.starts_with('-')
            || self.image.chars().any(char::is_whitespace)
            || self.image.chars().any(char::is_control)
        {
            return Err(MambaError::Validation(
                "Docker sandbox image reference is invalid".into(),
            ));
        }
        if !(100..=64_000).contains(&self.cpus_millis) {
            return Err(MambaError::Validation(
                "Docker CPU limit must be between 100 and 64000 millicores".into(),
            ));
        }
        if !(128..=262_144).contains(&self.memory_mb) {
            return Err(MambaError::Validation(
                "Docker memory limit must be between 128 and 262144 MiB".into(),
            ));
        }
        if !(16..=32_768).contains(&self.pids_limit) {
            return Err(MambaError::Validation(
                "Docker PID limit must be between 16 and 32768".into(),
            ));
        }
        if !(16..=16_384).contains(&self.tmpfs_mb) {
            return Err(MambaError::Validation(
                "Docker tmpfs limit must be between 16 and 16384 MiB".into(),
            ));
        }
        if self.environment.len() > 64 {
            return Err(MambaError::Validation(
                "Docker sandbox can forward at most 64 environment variables".into(),
            ));
        }
        for name in &self.environment {
            validate_environment_name(name)?;
            if std::env::var_os(name).is_none() {
                return Err(MambaError::Validation(format!(
                    "sandbox environment variable is not set: {name}"
                )));
            }
        }
        Ok(())
    }
}

impl ResolvedDockerSandbox {
    pub fn image_id(&self) -> &str {
        &self.image_id
    }

    pub fn command(&self, spec: DockerRunSpec<'_>) -> Result<Command> {
        validate_container_name(spec.name)?;
        let workspace = canonical_mount(spec.workspace, "sandbox workspace")?;
        let output_dir = canonical_mount(spec.output_dir, "sandbox output directory")?;
        let workspace_mount = format!(
            "type=bind,src={},dst={CONTAINER_WORKSPACE}{}",
            workspace.display(),
            if spec.workspace_writable {
                ""
            } else {
                ",readonly"
            }
        );
        let output_mount = format!(
            "type=bind,src={},dst={CONTAINER_OUTPUT}",
            output_dir.display()
        );
        let cpus = format!(
            "{}.{:03}",
            self.config.cpus_millis / 1_000,
            self.config.cpus_millis % 1_000
        );
        let memory = format!("{}m", self.config.memory_mb);
        let tmpfs = format!(
            "/tmp:rw,noexec,nosuid,nodev,size={}m,mode=1777",
            self.config.tmpfs_mb
        );
        let mut command = Command::new(&self.config.runtime);
        command
            .arg("run")
            .arg("--rm")
            .arg("--pull=never")
            .arg("--init")
            .arg("--name")
            .arg(spec.name)
            .arg("--label")
            .arg(format!("io.manbaflow.flight={}", spec.name))
            .arg("--read-only")
            .arg("--cap-drop=ALL")
            .arg("--security-opt=no-new-privileges=true")
            .arg("--pids-limit")
            .arg(self.config.pids_limit.to_string())
            .arg("--memory")
            .arg(&memory)
            .arg("--memory-swap")
            .arg(memory)
            .arg("--cpus")
            .arg(cpus)
            .arg("--network")
            .arg(self.config.network.as_str())
            .arg("--user")
            .arg(&self.user)
            .arg("--workdir")
            .arg(CONTAINER_WORKSPACE)
            .arg("--tmpfs")
            .arg(tmpfs)
            .arg("--mount")
            .arg(workspace_mount)
            .arg("--mount")
            .arg(output_mount)
            .arg("--env")
            .arg("HOME=/tmp/mamba-home")
            .arg("--env")
            .arg("XDG_CONFIG_HOME=/tmp/mamba-home/.config")
            .arg("--env")
            .arg("CODEX_HOME=/tmp/mamba-home/.codex")
            .arg("--env")
            .arg("DISABLE_AUTOUPDATER=1")
            .arg("--env")
            .arg("DISABLE_UPDATES=1");
        for name in &self.config.environment {
            command.arg("--env").arg(name);
        }
        command
            .arg(&self.image_id)
            .arg(spec.program)
            .args(spec.args);
        Ok(command)
    }

    pub fn report(&self) -> ExecutionSandboxReport {
        ExecutionSandboxReport {
            backend: "docker".into(),
            image: Some(self.config.image.clone()),
            image_id: Some(self.image_id.clone()),
            network: self.config.network.as_str().into(),
            root_read_only: true,
            user: Some(self.user.clone()),
            cpus_millis: Some(self.config.cpus_millis),
            memory_bytes: self.config.memory_mb.checked_mul(1024 * 1024),
            pids_limit: Some(self.config.pids_limit),
            forwarded_environment: self.config.environment.clone(),
        }
    }

    pub fn cleanup_guard(&self, name: &str) -> Result<DockerContainerGuard> {
        validate_container_name(name)?;
        Ok(DockerContainerGuard {
            runtime: self.config.runtime.clone(),
            name: name.to_string(),
        })
    }
}

impl Drop for DockerContainerGuard {
    fn drop(&mut self) {
        let runtime = self.runtime.clone();
        let name = self.name.clone();
        let _ = std::thread::Builder::new()
            .name("mamba-container-reaper".into())
            .spawn(move || {
                let _ = StdCommand::new(runtime)
                    .args(["container", "rm", "--force"])
                    .arg(name)
                    .output();
            });
    }
}

fn canonical_mount(path: &Path, label: &str) -> Result<PathBuf> {
    let path = path.canonicalize().map_err(|_| {
        MambaError::Validation(format!("{label} does not exist: {}", path.display()))
    })?;
    let display = path.to_string_lossy();
    if display.contains(',') || display.contains(['\n', '\r']) {
        return Err(MambaError::Validation(format!(
            "{label} contains characters unsupported by Docker --mount"
        )));
    }
    Ok(path)
}

fn validate_environment_name(name: &str) -> Result<()> {
    const DENIED: &[&str] = &[
        "MAMBA_TOKEN",
        "DOCKER_HOST",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "HOME",
        "PATH",
    ];
    if name.is_empty()
        || name.len() > 128
        || name.as_bytes()[0].is_ascii_digit()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || DENIED.contains(&name)
    {
        return Err(MambaError::Validation(format!(
            "sandbox environment name is invalid or denied: {name}"
        )));
    }
    Ok(())
}

fn validate_user(user: &str) -> Result<()> {
    let Some((uid, gid)) = user.split_once(':') else {
        return Err(MambaError::Validation(
            "Docker sandbox user must use numeric UID:GID".into(),
        ));
    };
    let valid = uid.parse::<u32>().is_ok_and(|value| value > 0)
        && gid.parse::<u32>().is_ok_and(|value| value > 0);
    if !valid {
        return Err(MambaError::Validation(
            "Docker sandbox user must use non-root numeric UID:GID".into(),
        ));
    }
    Ok(())
}

fn current_user() -> Result<String> {
    let uid = id_value("-u")?;
    let gid = id_value("-g")?;
    let user = format!("{uid}:{gid}");
    validate_user(&user)?;
    Ok(user)
}

fn id_value(flag: &str) -> Result<String> {
    let output = StdCommand::new("id").arg(flag).output().map_err(|_| {
        MambaError::Validation(
            "could not determine host UID/GID; pass --sandbox-user UID:GID".into(),
        )
    })?;
    if !output.status.success() {
        return Err(MambaError::Validation(
            "could not determine host UID/GID; pass --sandbox-user UID:GID".into(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn validate_container_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(MambaError::Validation(
            "invalid Docker sandbox container name".into(),
        ));
    }
    Ok(())
}

fn valid_sha256_id(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn docker_command_has_closed_defaults_and_pinned_image_id() {
        let directory = tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        let output = directory.path().join("output");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let sandbox = ResolvedDockerSandbox {
            config: config(),
            image_id: format!("sha256:{}", "a".repeat(64)),
            user: "1000:1000".into(),
        };
        let command = sandbox
            .command(DockerRunSpec {
                name: "mamba-WRUN-1",
                workspace: &workspace,
                workspace_writable: false,
                output_dir: &output,
                program: OsStr::new("codex"),
                args: &[OsString::from("exec")],
            })
            .unwrap();
        let args = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(args.contains(&"--read-only".into()));
        assert!(args.contains(&"--cap-drop=ALL".into()));
        assert!(args.contains(&"--security-opt=no-new-privileges=true".into()));
        assert!(args.windows(2).any(|pair| pair == ["--network", "none"]));
        assert!(args.iter().any(|arg| arg.ends_with(",readonly")));
        assert!(args.contains(&format!("sha256:{}", "a".repeat(64))));
        assert!(!args.iter().any(|arg| arg.contains("MAMBA_TOKEN")));
        assert!(!args.iter().any(|arg| arg.contains("docker.sock")));
    }

    #[test]
    fn sensitive_or_implicit_environment_is_rejected() {
        let mut config = config();
        config.environment = vec!["MAMBA_TOKEN".into()];
        assert!(config.validate().is_err());
        config.environment = vec!["NOT_SET_FOR_MAMBA_SANDBOX_TEST".into()];
        assert!(config.validate().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn dropping_a_container_guard_forces_cleanup() {
        let directory = tempdir().unwrap();
        let runtime = directory.path().join("fake-docker");
        let marker = directory.path().join("cleanup.txt");
        std::fs::write(
            &runtime,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n",
                marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o755)).unwrap();

        drop(DockerContainerGuard {
            runtime,
            name: "mamba-WRUN-cancelled".into(),
        });

        for _ in 0..100 {
            if marker.is_file() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let invocation = std::fs::read_to_string(marker).unwrap();
        assert_eq!(
            invocation.lines().collect::<Vec<_>>(),
            ["container", "rm", "--force", "mamba-WRUN-cancelled"]
        );
    }

    fn config() -> DockerSandboxConfig {
        DockerSandboxConfig {
            runtime: "docker".into(),
            image: "manbaflow-agent-runtime:0.1.0".into(),
            network: SandboxNetwork::None,
            cpus_millis: 2_000,
            memory_mb: 4_096,
            pids_limit: 256,
            tmpfs_mb: 512,
            user: Some("1000:1000".into()),
            environment: Vec::new(),
        }
    }
}
