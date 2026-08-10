use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use tokio::process::Command;

use crate::domain::ExecutionSandboxReport;
use crate::error::{RelayError, Result};

pub const CONTAINER_WORKSPACE: &str = "/workspace";
pub const CONTAINER_OUTPUT: &str = "/relay-output";

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
                    RelayError::ExecutorUnavailable(self.runtime.display().to_string())
                } else {
                    RelayError::ExternalConnector(format!(
                        "could not inspect Docker sandbox image: {error}"
                    ))
                }
            })?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr);
            return Err(RelayError::Validation(format!(
                "Docker sandbox image {} is unavailable locally (--pull=never): {}",
                self.image,
                message.trim()
            )));
        }
        let image_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !valid_sha256_id(&image_id) {
            return Err(RelayError::Validation(
                "Docker image inspect did not return a sha256 image ID".into(),
            ));
        }
        let user = resolve_sandbox_user(self.user.clone(), current_user)?;
        Ok(ResolvedDockerSandbox {
            config: self,
            image_id,
            user,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.runtime.as_os_str().is_empty() {
            return Err(RelayError::Validation(
                "Docker runtime command cannot be empty".into(),
            ));
        }
        if self.image.is_empty()
            || self.image.len() > 512
            || self.image.starts_with('-')
            || self.image.chars().any(char::is_whitespace)
            || self.image.chars().any(char::is_control)
        {
            return Err(RelayError::Validation(
                "Docker sandbox image reference is invalid".into(),
            ));
        }
        if !(100..=64_000).contains(&self.cpus_millis) {
            return Err(RelayError::Validation(
                "Docker CPU limit must be between 100 and 64000 millicores".into(),
            ));
        }
        if !(128..=262_144).contains(&self.memory_mb) {
            return Err(RelayError::Validation(
                "Docker memory limit must be between 128 and 262144 MiB".into(),
            ));
        }
        if !(16..=32_768).contains(&self.pids_limit) {
            return Err(RelayError::Validation(
                "Docker PID limit must be between 16 and 32768".into(),
            ));
        }
        if !(16..=16_384).contains(&self.tmpfs_mb) {
            return Err(RelayError::Validation(
                "Docker tmpfs limit must be between 16 and 16384 MiB".into(),
            ));
        }
        if self.environment.len() > 64 {
            return Err(RelayError::Validation(
                "Docker sandbox can forward at most 64 environment variables".into(),
            ));
        }
        for name in &self.environment {
            validate_environment_name(name)?;
            if std::env::var_os(name).is_none() {
                return Err(RelayError::Validation(format!(
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
            .arg(format!("io.relay.flight={}", spec.name))
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
            .arg("HOME=/tmp/relay-home")
            .arg("--env")
            .arg("XDG_CONFIG_HOME=/tmp/relay-home/.config")
            .arg("--env")
            .arg("CODEX_HOME=/tmp/relay-home/.codex")
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
            .name("relay-container-reaper".into())
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
        RelayError::Validation(format!("{label} does not exist: {}", path.display()))
    })?;
    let display = path.to_string_lossy();
    if display.contains(',') || display.contains(['\n', '\r']) {
        return Err(RelayError::Validation(format!(
            "{label} contains characters unsupported by Docker --mount"
        )));
    }
    Ok(path)
}

fn validate_environment_name(name: &str) -> Result<()> {
    const DENIED: &[&str] = &[
        "RELAY_TOKEN",
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
        return Err(RelayError::Validation(format!(
            "sandbox environment name is invalid or denied: {name}"
        )));
    }
    Ok(())
}

/// 决定容器以哪个用户运行：显式指定优先，没指定才回退到宿主机当前用户。
///
/// `fallback` 必须惰性调用。之前这里写的是 `self.user.clone().unwrap_or(current_user()?)`，
/// 而 `unwrap_or` 的参数是立即求值的——宿主机是 root 时 `current_user()` 会先返回
/// 「must use non-root numeric UID:GID」，于是即使显式传了 `--sandbox-user`
/// 也起不来，正好堵死「以 root 跑 Worker、显式指定容器用户」这个最该支持的场景。
fn resolve_sandbox_user(
    explicit: Option<String>,
    fallback: impl FnOnce() -> Result<String>,
) -> Result<String> {
    let user = match explicit {
        Some(user) => user,
        None => fallback()?,
    };
    validate_user(&user)?;
    Ok(user)
}

fn validate_user(user: &str) -> Result<()> {
    let Some((uid, gid)) = user.split_once(':') else {
        return Err(RelayError::Validation(
            "Docker sandbox user must use numeric UID:GID".into(),
        ));
    };
    let valid = uid.parse::<u32>().is_ok_and(|value| value > 0)
        && gid.parse::<u32>().is_ok_and(|value| value > 0);
    if !valid {
        return Err(RelayError::Validation(
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
        RelayError::Validation(
            "could not determine host UID/GID; pass --sandbox-user UID:GID".into(),
        )
    })?;
    if !output.status.success() {
        return Err(RelayError::Validation(
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
        return Err(RelayError::Validation(
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
                name: "relay-WRUN-1",
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
        assert!(!args.iter().any(|arg| arg.contains("RELAY_TOKEN")));
        assert!(!args.iter().any(|arg| arg.contains("docker.sock")));
    }

    #[test]
    fn sensitive_or_implicit_environment_is_rejected() {
        let mut config = config();
        config.environment = vec!["RELAY_TOKEN".into()];
        assert!(config.validate().is_err());
        config.environment = vec!["NOT_SET_FOR_RELAY_SANDBOX_TEST".into()];
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
            name: "relay-WRUN-cancelled".into(),
        });

        // Drop 里是异步起清理进程，这里只能轮询。窗口给到 5 秒：全量测试并行跑满 CPU 时，
        // 原来的 1 秒会偶发超时，在 CI 上尤其明显。成功路径仍然立刻返回，不影响耗时。
        for _ in 0..500 {
            if marker.is_file() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let invocation = std::fs::read_to_string(&marker).unwrap_or_else(|error| {
            panic!("清理进程没有在 5 秒内写出 {}：{error}", marker.display())
        });
        assert_eq!(
            invocation.lines().collect::<Vec<_>>(),
            ["container", "rm", "--force", "relay-WRUN-cancelled"]
        );
    }

    #[test]
    fn explicit_sandbox_user_wins_even_when_the_host_user_is_unusable() {
        // 宿主机是 root（或 `id` 取不到）时，fallback 一定失败。显式指定的用户
        // 必须照常生效，否则以 root 跑 Worker 就没法指定容器用户了。
        let resolved = resolve_sandbox_user(Some("1000:1000".into()), || {
            Err(RelayError::Validation(
                "Docker sandbox user must use non-root numeric UID:GID".into(),
            ))
        })
        .unwrap();
        assert_eq!(resolved, "1000:1000");
    }

    #[test]
    fn sandbox_user_falls_back_to_the_host_user_when_unspecified() {
        let resolved = resolve_sandbox_user(None, || Ok("501:20".into())).unwrap();
        assert_eq!(resolved, "501:20");
    }

    #[test]
    fn explicit_root_sandbox_user_is_still_rejected() {
        // 放宽的只是求值时机，不是校验本身。
        let error = resolve_sandbox_user(Some("0:0".into()), || Ok("1000:1000".into()));
        assert!(error.is_err());
    }

    fn config() -> DockerSandboxConfig {
        DockerSandboxConfig {
            runtime: "docker".into(),
            image: "relay-agent-runtime:0.1.0".into(),
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
