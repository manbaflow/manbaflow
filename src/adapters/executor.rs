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
        let (mut command, result_file, command_label) = match build_command(&request) {
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

fn build_command(request: &ExecutionRequest) -> Result<(Command, Option<PathBuf>, String)> {
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
            Ok((command, None, command_label))
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
            Ok((command, Some(result_path), command_label))
        }
    }
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
}
