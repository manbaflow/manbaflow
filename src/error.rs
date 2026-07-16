use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MambaError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("organization has not been initialized; run `mamba org init` first")]
    OrganizationNotInitialized,

    #[error("organization is already initialized")]
    OrganizationAlreadyInitialized,

    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },

    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    #[error("validation failed: {0}")]
    Validation(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("no eligible assignee for task `{0}`")]
    NoEligibleAssignee(String),

    #[error("executor `{0}` is not installed or not on PATH")]
    ExecutorUnavailable(String),

    #[error("executor failed with exit code {code:?}: {message}")]
    ExecutorFailed { code: Option<i32>, message: String },

    #[error("executor timed out after {0} seconds")]
    ExecutorTimeout(u64),

    #[error("executor returned invalid structured output: {0}")]
    InvalidExecutorOutput(String),

    #[error("workspace does not exist or is not a directory: {0}")]
    InvalidWorkspace(PathBuf),

    #[error("external connector error: {0}")]
    ExternalConnector(String),
}

pub type Result<T> = std::result::Result<T, MambaError>;
