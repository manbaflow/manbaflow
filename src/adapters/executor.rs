use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::process::Command;

use crate::domain::{ExecutorKind, ExecutorMode};
use crate::error::{MambaError, Result};
use crate::sandbox::{
    CONTAINER_OUTPUT, CONTAINER_WORKSPACE, DockerContainerGuard, DockerRunSpec,
    ResolvedDockerSandbox,
};

#[derive(Clone, Debug)]
pub struct ExecutionRequest {
    pub kind: ExecutorKind,
    pub command: Option<PathBuf>,
    pub workspace: PathBuf,
    pub model: Option<String>,
    pub mode: ExecutorMode,
    pub prompt: String,
    pub output_schema: Option<Value>,
    pub timeout_seconds: u64,
    pub log_path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct ExecutionOutput {
    pub structured_output: Option<Value>,
    pub summary: String,
    pub session_id: Option<String>,
    pub cost_usd: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExecutorLog {
    executor: ExecutorKind,
    mode: ExecutorMode,
    command: String,
    workspace: PathBuf,
    model: Option<String>,
    prompt: String,
    output_schema: Option<Value>,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

pub struct TerminalExecutor;

impl TerminalExecutor {
    pub async fn run(request: ExecutionRequest) -> Result<ExecutionOutput> {
        Self::run_inner(request, None).await
    }

    pub async fn run_in_docker(
        request: ExecutionRequest,
        sandbox: &ResolvedDockerSandbox,
        container_name: &str,
    ) -> Result<ExecutionOutput> {
        Self::run_inner(request, Some((sandbox, container_name))).await
    }

    async fn run_inner(
        request: ExecutionRequest,
        sandbox: Option<(&ResolvedDockerSandbox, &str)>,
    ) -> Result<ExecutionOutput> {
        if let Some(parent) = request.log_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let started_at = Utc::now();
        let requested_command = request
            .command
            .as_deref()
            .map(|value| value.display().to_string())
            .unwrap_or_else(|| default_command(&request.kind).to_string());
        if !request.workspace.is_dir() {
            let error = MambaError::InvalidWorkspace(request.workspace.clone());
            write_failure_log(&request, &requested_command, started_at, error.to_string())?;
            return Err(error);
        }
        let BuiltCommand {
            mut command,
            result_file,
            command_label,
            _container_guard,
        } = match build_command(&request, sandbox) {
            Ok(command) => command,
            Err(error) => {
                write_failure_log(&request, &requested_command, started_at, error.to_string())?;
                return Err(error);
            }
        };
        command
            .current_dir(&request.workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let output = match tokio::time::timeout(
            Duration::from_secs(request.timeout_seconds),
            command.output(),
        )
        .await
        {
            Ok(Ok(output)) => output,
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                write_failure_log(
                    &request,
                    &command_label,
                    started_at,
                    format!("executor not found: {error}"),
                )?;
                return Err(MambaError::ExecutorUnavailable(command_label));
            }
            Ok(Err(error)) => {
                write_failure_log(
                    &request,
                    &command_label,
                    started_at,
                    format!("failed to start executor: {error}"),
                )?;
                return Err(error.into());
            }
            Err(_) => {
                write_failure_log(
                    &request,
                    &command_label,
                    started_at,
                    format!(
                        "executor timed out after {} seconds",
                        request.timeout_seconds
                    ),
                )?;
                return Err(MambaError::ExecutorTimeout(request.timeout_seconds));
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let log = ExecutorLog {
            executor: request.kind.clone(),
            mode: request.mode.clone(),
            command: command_label,
            workspace: request.workspace.clone(),
            model: request.model.clone(),
            prompt: request.prompt.clone(),
            output_schema: request.output_schema.clone(),
            started_at,
            finished_at: Utc::now(),
            exit_code: output.status.code(),
            stdout: stdout.clone(),
            stderr: stderr.clone(),
        };
        fs::write(&request.log_path, serde_json::to_vec_pretty(&log)?)?;

        if !output.status.success() {
            let message = if stderr.trim().is_empty() {
                stdout.trim().to_string()
            } else {
                stderr.trim().to_string()
            };
            return Err(MambaError::ExecutorFailed {
                code: output.status.code(),
                message: truncate(&message, 500),
            });
        }

        match request.kind {
            ExecutorKind::ClaudeCode => parse_claude(&stdout, request.output_schema.is_some()),
            ExecutorKind::Codex => parse_codex(
                &stdout,
                result_file.as_deref(),
                request.output_schema.is_some(),
            ),
        }
    }
}

struct BuiltCommand {
    command: Command,
    result_file: Option<PathBuf>,
    command_label: String,
    _container_guard: Option<DockerContainerGuard>,
}

fn build_command(
    request: &ExecutionRequest,
    sandbox: Option<(&ResolvedDockerSandbox, &str)>,
) -> Result<BuiltCommand> {
    if let Some((sandbox, container_name)) = sandbox {
        return build_docker_command(request, sandbox, container_name);
    }
    let executable = request
        .command
        .clone()
        .unwrap_or_else(|| PathBuf::from(default_command(&request.kind)));
    let command_label = executable.display().to_string();
    let mut command = Command::new(&executable);

    match request.kind {
        ExecutorKind::ClaudeCode => {
            command
                .arg("-p")
                .arg(&request.prompt)
                .arg("--output-format")
                .arg("json")
                .arg("--no-session-persistence")
                .arg("--permission-mode")
                .arg(match request.mode {
                    ExecutorMode::Plan => "plan",
                    ExecutorMode::Execute => "acceptEdits",
                });
            if request.mode == ExecutorMode::Plan {
                command.arg("--tools").arg("");
            }
            if let Some(model) = &request.model {
                command.arg("--model").arg(model);
            }
            if let Some(schema) = &request.output_schema {
                command
                    .arg("--json-schema")
                    .arg(serde_json::to_string(schema)?);
            }
            Ok(BuiltCommand {
                command,
                result_file: None,
                command_label,
                _container_guard: None,
            })
        }
        ExecutorKind::Codex => {
            let artifact_dir = request.log_path.parent().unwrap_or_else(|| Path::new("."));
            fs::create_dir_all(artifact_dir)?;
            let result_path = request.log_path.with_extension("result.json");
            command
                .arg("exec")
                .arg("--json")
                .arg("--ephemeral")
                .arg("--skip-git-repo-check")
                .arg("--sandbox")
                .arg(match request.mode {
                    ExecutorMode::Plan => "read-only",
                    ExecutorMode::Execute => "workspace-write",
                })
                .arg("--cd")
                .arg(&request.workspace)
                .arg("--output-last-message")
                .arg(&result_path);
            if let Some(model) = &request.model {
                command.arg("--model").arg(model);
            }
            if let Some(schema) = &request.output_schema {
                let schema_path = request.log_path.with_extension("schema.json");
                fs::write(&schema_path, serde_json::to_vec_pretty(schema)?)?;
                command.arg("--output-schema").arg(schema_path);
            }
            command.arg(&request.prompt);
            Ok(BuiltCommand {
                command,
                result_file: Some(result_path),
                command_label,
                _container_guard: None,
            })
        }
    }
}

fn build_docker_command(
    request: &ExecutionRequest,
    sandbox: &ResolvedDockerSandbox,
    container_name: &str,
) -> Result<BuiltCommand> {
    let executable = request
        .command
        .clone()
        .unwrap_or_else(|| PathBuf::from(default_command(&request.kind)));
    let artifact_dir = request.log_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(artifact_dir)?;
    let result_path = request.log_path.with_extension("result.json");
    let container_result = container_artifact_path(&result_path)?;
    let mut args = Vec::<OsString>::new();
    let result_file = match request.kind {
        ExecutorKind::ClaudeCode => {
            args.extend([
                "-p".into(),
                request.prompt.clone().into(),
                "--output-format".into(),
                "json".into(),
                "--no-session-persistence".into(),
                "--permission-mode".into(),
                match request.mode {
                    ExecutorMode::Plan => "plan",
                    ExecutorMode::Execute => "acceptEdits",
                }
                .into(),
            ]);
            if request.mode == ExecutorMode::Plan {
                args.extend(["--tools".into(), "".into()]);
            }
            if let Some(model) = &request.model {
                args.extend(["--model".into(), model.into()]);
            }
            if let Some(schema) = &request.output_schema {
                args.extend([
                    "--json-schema".into(),
                    serde_json::to_string(schema)?.into(),
                ]);
            }
            None
        }
        ExecutorKind::Codex => {
            args.extend([
                "exec".into(),
                "--json".into(),
                "--ephemeral".into(),
                "--skip-git-repo-check".into(),
                "--sandbox".into(),
                match request.mode {
                    ExecutorMode::Plan => "read-only",
                    ExecutorMode::Execute => "workspace-write",
                }
                .into(),
                "--cd".into(),
                CONTAINER_WORKSPACE.into(),
                "--output-last-message".into(),
                container_result.as_os_str().into(),
            ]);
            if let Some(model) = &request.model {
                args.extend(["--model".into(), model.into()]);
            }
            if let Some(schema) = &request.output_schema {
                let schema_path = request.log_path.with_extension("schema.json");
                fs::write(&schema_path, serde_json::to_vec_pretty(schema)?)?;
                let container_schema = container_artifact_path(&schema_path)?;
                args.extend([
                    "--output-schema".into(),
                    container_schema.as_os_str().into(),
                ]);
            }
            args.push(request.prompt.clone().into());
            Some(result_path)
        }
    };
    let command = sandbox.command(DockerRunSpec {
        name: container_name,
        workspace: &request.workspace,
        workspace_writable: request.mode == ExecutorMode::Execute,
        output_dir: artifact_dir,
        program: executable.as_os_str(),
        args: &args,
    })?;
    let command_label = format!("docker:{}:{}", sandbox.image_id(), executable.display());
    Ok(BuiltCommand {
        command,
        result_file,
        command_label,
        _container_guard: Some(sandbox.cleanup_guard(container_name)?),
    })
}

fn container_artifact_path(host_path: &Path) -> Result<PathBuf> {
    let file_name = host_path.file_name().ok_or_else(|| {
        MambaError::Validation("executor artifact path requires a file name".into())
    })?;
    Ok(Path::new(CONTAINER_OUTPUT).join(file_name))
}

fn parse_claude(stdout: &str, expects_structured: bool) -> Result<ExecutionOutput> {
    let value: Value = serde_json::from_str(stdout.trim())
        .map_err(|error| MambaError::InvalidExecutorOutput(format!("Claude Code JSON: {error}")))?;
    let structured_output = value.get("structured_output").cloned().or_else(|| {
        expects_structured
            .then(|| value.get("result").and_then(parse_embedded_json))
            .flatten()
    });
    if expects_structured && structured_output.is_none() {
        return Err(MambaError::InvalidExecutorOutput(
            "Claude Code response did not contain structured_output".to_string(),
        ));
    }
    let summary = value
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| "Claude Code completed the terminal run".to_string());
    Ok(ExecutionOutput {
        structured_output,
        summary,
        session_id: value
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        cost_usd: value.get("total_cost_usd").and_then(Value::as_f64),
    })
}

fn parse_codex(
    stdout: &str,
    result_file: Option<&Path>,
    expects_structured: bool,
) -> Result<ExecutionOutput> {
    let result = result_file
        .filter(|path| path.exists())
        .map(fs::read_to_string)
        .transpose()?
        .unwrap_or_default();
    let structured_output = expects_structured
        .then(|| serde_json::from_str::<Value>(result.trim()).ok())
        .flatten();
    if expects_structured && structured_output.is_none() {
        return Err(MambaError::InvalidExecutorOutput(
            "Codex last message did not match the requested JSON schema".to_string(),
        ));
    }

    let mut session_id = None;
    for line in stdout.lines() {
        if let Ok(event) = serde_json::from_str::<Value>(line)
            && session_id.is_none()
        {
            session_id = event
                .get("thread_id")
                .or_else(|| event.get("session_id"))
                .and_then(Value::as_str)
                .map(str::to_string);
        }
    }
    let summary = if result.trim().is_empty() {
        "Codex completed the terminal run".to_string()
    } else {
        result.trim().to_string()
    };
    Ok(ExecutionOutput {
        structured_output,
        summary,
        session_id,
        cost_usd: None,
    })
}

fn parse_embedded_json(value: &Value) -> Option<Value> {
    value
        .as_str()
        .and_then(|text| serde_json::from_str(text).ok())
}

fn default_command(kind: &ExecutorKind) -> &'static str {
    match kind {
        ExecutorKind::ClaudeCode => "claude",
        ExecutorKind::Codex => "codex",
    }
}

fn write_failure_log(
    request: &ExecutionRequest,
    command: &str,
    started_at: chrono::DateTime<Utc>,
    message: String,
) -> Result<()> {
    let log = ExecutorLog {
        executor: request.kind.clone(),
        mode: request.mode.clone(),
        command: command.to_string(),
        workspace: request.workspace.clone(),
        model: request.model.clone(),
        prompt: request.prompt.clone(),
        output_schema: request.output_schema.clone(),
        started_at,
        finished_at: Utc::now(),
        exit_code: None,
        stdout: String::new(),
        stderr: message,
    };
    fs::write(&request.log_path, serde_json::to_vec_pretty(&log)?)?;
    Ok(())
}

fn truncate(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::sandbox::{DockerSandboxConfig, SandboxNetwork};

    #[tokio::test]
    async fn unavailable_executor_still_writes_a_blackbox() {
        let directory = tempdir().unwrap();
        let log_path = directory.path().join("run.json");
        let error = TerminalExecutor::run(ExecutionRequest {
            kind: ExecutorKind::ClaudeCode,
            command: Some(directory.path().join("missing-executor")),
            workspace: directory.path().to_path_buf(),
            model: None,
            mode: ExecutorMode::Plan,
            prompt: "plan".into(),
            output_schema: None,
            timeout_seconds: 1,
            log_path: log_path.clone(),
        })
        .await
        .unwrap_err();

        assert!(matches!(error, MambaError::ExecutorUnavailable(_)));
        let log: ExecutorLog = serde_json::from_slice(&fs::read(log_path).unwrap()).unwrap();
        assert!(log.stderr.contains("executor not found"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn docker_executor_maps_artifacts_and_keeps_closed_runtime_flags() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        let output = directory.path().join("output");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&output).unwrap();
        let runtime = directory.path().join("fake-docker");
        let arguments = directory.path().join("docker-args.txt");
        fs::write(
            &runtime,
            format!(
                r#"#!/bin/sh
if [ "$1" = "image" ] && [ "$2" = "inspect" ]; then
  printf '%s\n' 'sha256:{digest}'
  exit 0
fi
if [ "$1" = "container" ]; then
  exit 0
fi
printf '%s\n' "$@" > '{arguments}'
output=''
result=''
previous=''
for value in "$@"; do
  case "$value" in
    type=bind,src=*,dst=/mamba-output)
      output="${{value#type=bind,src=}}"
      output="${{output%,dst=/mamba-output}}"
      ;;
  esac
  if [ "$previous" = "--output-last-message" ]; then
    result="${{value##*/}}"
  fi
  previous="$value"
done
printf '%s' 'Container flight landed.' > "$output/$result"
printf '%s\n' '{{"thread_id":"docker-thread"}}'
"#,
                digest = "a".repeat(64),
                arguments = arguments.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&runtime, fs::Permissions::from_mode(0o755)).unwrap();
        let sandbox = DockerSandboxConfig {
            runtime,
            image: "manbaflow-agent-runtime:0.1.0".into(),
            network: SandboxNetwork::None,
            cpus_millis: 1_000,
            memory_mb: 512,
            pids_limit: 64,
            tmpfs_mb: 64,
            user: Some("1000:1000".into()),
            environment: Vec::new(),
        }
        .resolve()
        .unwrap();
        let log_path = output.join("blackbox.json");
        let result = TerminalExecutor::run_in_docker(
            ExecutionRequest {
                kind: ExecutorKind::Codex,
                command: Some("codex".into()),
                workspace,
                model: None,
                mode: ExecutorMode::Plan,
                prompt: "inspect only".into(),
                output_schema: None,
                timeout_seconds: 5,
                log_path,
            },
            &sandbox,
            "mamba-WRUN-test",
        )
        .await
        .unwrap();
        assert_eq!(result.summary, "Container flight landed.");
        let args = fs::read_to_string(arguments).unwrap();
        assert!(args.contains("--read-only"));
        assert!(args.contains("--cap-drop=ALL"));
        assert!(args.contains("--network\nnone"));
        assert!(args.contains("readonly"));
        assert!(args.contains(&format!("sha256:{}", "a".repeat(64))));
    }
}
