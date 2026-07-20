use std::collections::BTreeMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::{Client, Response, StatusCode, Url, redirect};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::domain::{GitLabWritePayload, GitLabWriteRequest, GitLabWriteResult};
use crate::error::{MambaError, Result};
use crate::gitlab::DEFAULT_GITLAB_URL;

#[derive(Clone)]
pub struct GitLabWriteBridge {
    client: Client,
    api_base: Url,
    fallback_token: Option<String>,
    tenant_tokens: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitLabDispatchError {
    pub message: String,
    pub indeterminate: bool,
}

impl GitLabWriteBridge {
    pub fn from_env() -> Result<Self> {
        let base_url = std::env::var("MAMBA_GITLAB_WRITE_URL")
            .ok()
            .or_else(|| std::env::var("GITLAB_URL").ok())
            .unwrap_or_else(|| DEFAULT_GITLAB_URL.to_string());
        let fallback_token = std::env::var("MAMBA_GITLAB_WRITE_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let tenant_tokens = std::env::var("MAMBA_GITLAB_WRITE_TOKENS_JSON")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| parse_tenant_tokens(&value))
            .transpose()?
            .unwrap_or_default();
        Self::new(&base_url, fallback_token.as_deref(), tenant_tokens)
    }

    pub fn disabled() -> Self {
        Self::new(DEFAULT_GITLAB_URL, None, BTreeMap::new()).expect("default GitLab URL is valid")
    }

    pub fn new(
        base_url: &str,
        fallback_token: Option<&str>,
        tenant_tokens: BTreeMap<String, String>,
    ) -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let api_base = normalize_api_base(base_url)?;
        let fallback_token = fallback_token
            .map(|token| validate_token(token, "fallback GitLab write token"))
            .transpose()?;
        let tenant_tokens = tenant_tokens
            .into_iter()
            .map(|(tenant, token)| {
                let tenant = tenant.trim().to_string();
                if tenant.is_empty() || tenant.chars().any(char::is_whitespace) {
                    return Err(MambaError::Validation(
                        "GitLab write token tenant IDs cannot be empty or contain whitespace"
                            .into(),
                    ));
                }
                Ok((tenant, validate_token(&token, "tenant GitLab write token")?))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let client = Client::builder()
            .redirect(redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .user_agent(concat!("MambaFlow/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| {
                MambaError::ExternalConnector("could not initialize GitLab writer".into())
            })?;
        Ok(Self {
            client,
            api_base,
            fallback_token,
            tenant_tokens,
        })
    }

    pub fn enabled_for(&self, tenant_id: &str) -> bool {
        if self.tenant_tokens.is_empty() {
            self.fallback_token.is_some()
        } else {
            self.tenant_tokens.contains_key(tenant_id)
        }
    }

    pub async fn dispatch(
        &self,
        tenant_id: &str,
        request: &GitLabWriteRequest,
    ) -> std::result::Result<GitLabWriteResult, GitLabDispatchError> {
        let token = self.token_for(tenant_id)?;
        match &request.payload {
            GitLabWritePayload::CreateIssue {
                project,
                title,
                description,
                labels,
            } => {
                let url = self.endpoint(&["projects", project, "issues"])?;
                let response: GitLabIssue = self
                    .post_json(
                        url,
                        token,
                        &CreateIssueBody {
                            title,
                            description,
                            labels: labels.join(","),
                        },
                        "create issue",
                    )
                    .await?;
                Ok(GitLabWriteResult {
                    kind: "issue".into(),
                    external_id: response.iid.to_string(),
                    title: response.title,
                    url: response.web_url,
                    status: response.state,
                    response_status: StatusCode::CREATED.as_u16(),
                    written_at: Utc::now(),
                })
            }
            GitLabWritePayload::CommentIssue {
                project,
                issue_iid,
                body,
            } => {
                let iid = issue_iid.to_string();
                let url = self.endpoint(&["projects", project, "issues", &iid, "notes"])?;
                let result_url = url.to_string();
                let response: GitLabNote = self
                    .post_json(url, token, &CreateNoteBody { body }, "comment on issue")
                    .await?;
                Ok(GitLabWriteResult {
                    kind: "issue_note".into(),
                    external_id: response.id.to_string(),
                    title: format!("Issue #{issue_iid} comment"),
                    url: result_url,
                    status: "created".into(),
                    response_status: StatusCode::CREATED.as_u16(),
                    written_at: Utc::now(),
                })
            }
            GitLabWritePayload::CreateMergeRequest {
                project,
                source_branch,
                target_branch,
                title,
                description,
                labels,
                remove_source_branch,
                draft,
            } => {
                let url = self.endpoint(&["projects", project, "merge_requests"])?;
                let title = if *draft && !title.starts_with("Draft:") {
                    format!("Draft: {title}")
                } else {
                    title.clone()
                };
                let response: GitLabMergeRequest = self
                    .post_json(
                        url,
                        token,
                        &CreateMergeRequestBody {
                            source_branch,
                            target_branch,
                            title: &title,
                            description,
                            labels: labels.join(","),
                            remove_source_branch: *remove_source_branch,
                        },
                        "create merge request",
                    )
                    .await?;
                Ok(GitLabWriteResult {
                    kind: "merge_request".into(),
                    external_id: response.iid.to_string(),
                    title: response.title,
                    url: response.web_url,
                    status: response.state,
                    response_status: StatusCode::CREATED.as_u16(),
                    written_at: Utc::now(),
                })
            }
            GitLabWritePayload::CommentMergeRequest {
                project,
                merge_request_iid,
                body,
            } => {
                let iid = merge_request_iid.to_string();
                let url = self.endpoint(&["projects", project, "merge_requests", &iid, "notes"])?;
                let result_url = url.to_string();
                let response: GitLabNote = self
                    .post_json(
                        url,
                        token,
                        &CreateNoteBody { body },
                        "comment on merge request",
                    )
                    .await?;
                Ok(GitLabWriteResult {
                    kind: "merge_request_note".into(),
                    external_id: response.id.to_string(),
                    title: format!("Merge request !{merge_request_iid} comment"),
                    url: result_url,
                    status: "created".into(),
                    response_status: StatusCode::CREATED.as_u16(),
                    written_at: Utc::now(),
                })
            }
        }
    }

    fn token_for(&self, tenant_id: &str) -> std::result::Result<&str, GitLabDispatchError> {
        let token = if self.tenant_tokens.is_empty() {
            self.fallback_token.as_deref()
        } else {
            self.tenant_tokens.get(tenant_id).map(String::as_str)
        };
        token.ok_or_else(|| GitLabDispatchError {
            message: format!("GitLab writer is not configured for tenant {tenant_id}"),
            indeterminate: false,
        })
    }

    fn endpoint(&self, segments: &[&str]) -> std::result::Result<Url, GitLabDispatchError> {
        let mut url = self.api_base.clone();
        let mut path = url.path_segments_mut().map_err(|_| GitLabDispatchError {
            message: "GitLab URL cannot be used as an API endpoint".into(),
            indeterminate: false,
        })?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }

    async fn post_json<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        url: Url,
        token: &str,
        body: &B,
        operation: &str,
    ) -> std::result::Result<T, GitLabDispatchError> {
        let response = self
            .client
            .post(url)
            .header("PRIVATE-TOKEN", token)
            .json(body)
            .send()
            .await
            .map_err(|error| GitLabDispatchError {
                message: format!("GitLab {operation} request outcome is unknown: {error}"),
                indeterminate: true,
            })?;
        parse_response(response, operation).await
    }
}

#[derive(Serialize)]
struct CreateIssueBody<'a> {
    title: &'a str,
    description: &'a str,
    labels: String,
}

#[derive(Serialize)]
struct CreateNoteBody<'a> {
    body: &'a str,
}

#[derive(Serialize)]
struct CreateMergeRequestBody<'a> {
    source_branch: &'a str,
    target_branch: &'a str,
    title: &'a str,
    description: &'a str,
    labels: String,
    remove_source_branch: bool,
}

#[derive(Deserialize)]
struct GitLabIssue {
    iid: u64,
    title: String,
    state: String,
    web_url: String,
}

#[derive(Deserialize)]
struct GitLabMergeRequest {
    iid: u64,
    title: String,
    state: String,
    web_url: String,
}

#[derive(Deserialize)]
struct GitLabNote {
    id: u64,
}

async fn parse_response<T: DeserializeOwned>(
    response: Response,
    operation: &str,
) -> std::result::Result<T, GitLabDispatchError> {
    let status = response.status();
    if !status.is_success() {
        let indeterminate = status.is_server_error()
            || status.is_redirection()
            || matches!(
                status,
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS
            );
        return Err(GitLabDispatchError {
            message: format!(
                "GitLab {operation} returned HTTP {}; {}",
                status.as_u16(),
                if indeterminate {
                    "reconcile with GitLab before retrying"
                } else {
                    "the write was rejected"
                }
            ),
            indeterminate,
        });
    }
    response.json().await.map_err(|_| GitLabDispatchError {
        message: format!(
            "GitLab {operation} succeeded but returned invalid JSON; reconcile before retrying"
        ),
        indeterminate: true,
    })
}

fn normalize_api_base(base_url: &str) -> Result<Url> {
    let mut url = Url::parse(base_url.trim())
        .map_err(|_| MambaError::Validation("invalid GitLab write URL".into()))?;
    let host = url
        .host_str()
        .ok_or_else(|| MambaError::Validation("GitLab write URL must include a hostname".into()))?;
    let loopback = matches!(host, "localhost" | "127.0.0.1" | "::1");
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(MambaError::Validation(
            "GitLab write URL must use HTTPS, except for a loopback test server".into(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(MambaError::Validation(
            "GitLab write URL must not contain credentials".into(),
        ));
    }
    url.set_query(None);
    url.set_fragment(None);
    let already_api = url.path().trim_end_matches('/').ends_with("/api/v4");
    if !already_api {
        let mut segments = url.path_segments_mut().map_err(|_| {
            MambaError::Validation("GitLab write URL cannot be used as an API base".into())
        })?;
        segments.pop_if_empty().push("api").push("v4");
    }
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn validate_token(token: &str, label: &str) -> Result<String> {
    let token = token.trim();
    if token.len() < 16 || token.chars().any(char::is_whitespace) {
        return Err(MambaError::Validation(format!(
            "{label} must contain at least 16 non-whitespace characters"
        )));
    }
    Ok(token.to_string())
}

fn parse_tenant_tokens(value: &str) -> Result<BTreeMap<String, String>> {
    serde_json::from_str(value).map_err(|_| {
        MambaError::Validation(
            "MAMBA_GITLAB_WRITE_TOKENS_JSON must be a JSON object of tenant IDs to tokens".into(),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::Request;
    use axum::http::StatusCode as AxumStatus;
    use axum::response::IntoResponse;
    use axum::routing::any;
    use chrono::Utc;
    use tokio::net::TcpListener;

    use super::*;
    use crate::domain::GitLabWriteStatus;

    #[tokio::test]
    async fn dispatches_all_supported_writes_with_separate_token() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_handler = seen.clone();
        let app = Router::new().fallback(any(move |request: Request| {
            let seen = seen_handler.clone();
            async move {
                let token = request
                    .headers()
                    .get("private-token")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                let path = request.uri().path().to_string();
                let body = axum::body::to_bytes(request.into_body(), 1_100_000)
                    .await
                    .unwrap();
                seen.lock().unwrap().push((path.clone(), token, body));
                if path.ends_with("/issues") {
                    return (AxumStatus::CREATED, r#"{"iid":7,"title":"Issue","state":"opened","web_url":"https://gitlab.test/issues/7"}"#).into_response();
                }
                if path.ends_with("/merge_requests") {
                    return (AxumStatus::CREATED, r#"{"iid":9,"title":"MR","state":"opened","web_url":"https://gitlab.test/merge_requests/9"}"#).into_response();
                }
                (AxumStatus::CREATED, r#"{"id":11}"#).into_response()
            }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let bridge = GitLabWriteBridge::new(
            &format!("http://{address}"),
            Some("write-token-123456789"),
            BTreeMap::new(),
        )
        .unwrap();
        for payload in [
            GitLabWritePayload::CreateIssue {
                project: "platform/gateway".into(),
                title: "Issue".into(),
                description: "Body".into(),
                labels: vec!["flow".into()],
            },
            GitLabWritePayload::CommentIssue {
                project: "platform/gateway".into(),
                issue_iid: 7,
                body: "Comment".into(),
            },
            GitLabWritePayload::CreateMergeRequest {
                project: "platform/gateway".into(),
                source_branch: "feature/gateway".into(),
                target_branch: "main".into(),
                title: "MR".into(),
                description: "Body".into(),
                labels: vec![],
                remove_source_branch: true,
                draft: false,
            },
            GitLabWritePayload::CommentMergeRequest {
                project: "platform/gateway".into(),
                merge_request_iid: 9,
                body: "Review".into(),
            },
        ] {
            bridge.dispatch("TEN-1", &request(payload)).await.unwrap();
        }
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 4);
        assert!(
            seen.iter()
                .all(|(_, token, _)| token == "write-token-123456789")
        );
        assert!(seen[0].0.contains("platform%2Fgateway/issues"));
        assert!(seen[3].0.ends_with("/merge_requests/9/notes"));
    }

    #[tokio::test]
    async fn treats_unknown_post_outcomes_as_indeterminate() {
        let app = Router::new().fallback(any(|| async {
            (AxumStatus::INTERNAL_SERVER_ERROR, "try later")
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let bridge = GitLabWriteBridge::new(
            &format!("http://{address}"),
            Some("write-token-123456789"),
            BTreeMap::new(),
        )
        .unwrap();
        let error = bridge
            .dispatch(
                "TEN-1",
                &request(GitLabWritePayload::CreateIssue {
                    project: "platform/gateway".into(),
                    title: "Issue".into(),
                    description: String::new(),
                    labels: vec![],
                }),
            )
            .await
            .unwrap_err();
        assert!(error.indeterminate);
    }

    #[test]
    fn tenant_token_map_never_falls_back_to_another_tenant() {
        let bridge = GitLabWriteBridge::new(
            DEFAULT_GITLAB_URL,
            Some("fallback-token-123456789"),
            BTreeMap::from([("TEN-A".into(), "tenant-a-token-123456789".into())]),
        )
        .unwrap();
        assert!(bridge.enabled_for("TEN-A"));
        assert!(!bridge.enabled_for("TEN-B"));
        assert_eq!(
            bridge.token_for("TEN-A").unwrap(),
            "tenant-a-token-123456789"
        );
        assert!(!bridge.token_for("TEN-B").unwrap_err().indeterminate);
    }

    fn request(payload: GitLabWritePayload) -> GitLabWriteRequest {
        GitLabWriteRequest {
            id: "GLW-1".into(),
            flow_id: "FLOW-1".into(),
            task_id: "TASK-1".into(),
            payload,
            payload_sha256: "0".repeat(64),
            requested_by: "AGENT-1".into(),
            requested_at: Utc::now(),
            status: GitLabWriteStatus::Dispatching,
            reviewed_by: Some("HUMAN-1".into()),
            reviewed_at: Some(Utc::now()),
            review_reason: None,
            dispatch_id: Some("DSP-1".into()),
            dispatch_started_at: Some(Utc::now()),
            result: None,
            last_error: None,
        }
    }
}
