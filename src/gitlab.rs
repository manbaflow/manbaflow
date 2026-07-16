use std::fmt::Write as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::domain::ExternalArtifact;
use crate::error::{MambaError, Result};

pub const DEFAULT_GITLAB_URL: &str = "https://gitlab.com";
const WEBHOOK_TOLERANCE_SECONDS: u64 = 300;
type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitLabProject {
    pub id: u64,
    pub path_with_namespace: String,
    pub web_url: String,
    pub default_branch: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct GitLabSnapshot {
    pub project: GitLabProject,
    pub merge_request_iid: u64,
    pub artifacts: Vec<ExternalArtifact>,
}

#[derive(Clone)]
pub struct GitLabWebhookAuth {
    signing_key: Option<Vec<u8>>,
    legacy_token_hash: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitLabWebhookVerification {
    pub delivery_id: String,
    pub occurred_at: DateTime<Utc>,
    pub signed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitLabWebhookUpdate {
    pub event_kind: &'static str,
    pub project: String,
    pub merge_request_iid: String,
    pub artifact: ExternalArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GitLabWebhookEvent {
    Update(Box<GitLabWebhookUpdate>),
    Ignored { object_kind: String },
}

impl GitLabWebhookAuth {
    pub fn from_env() -> Result<Option<Self>> {
        let signing_token = std::env::var("GITLAB_WEBHOOK_SIGNING_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let legacy_token = std::env::var("GITLAB_WEBHOOK_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty());
        Self::new(signing_token.as_deref(), legacy_token.as_deref())
    }

    pub fn new(signing_token: Option<&str>, legacy_token: Option<&str>) -> Result<Option<Self>> {
        let signing_key = signing_token.map(parse_signing_token).transpose()?;
        let legacy_token_hash = legacy_token
            .map(|token| {
                if token.trim().is_empty() {
                    return Err(MambaError::Validation(
                        "GitLab legacy webhook token cannot be empty".into(),
                    ));
                }
                Ok(Sha256::digest(token.as_bytes()).to_vec())
            })
            .transpose()?;
        if signing_key.is_none() && legacy_token_hash.is_none() {
            Ok(None)
        } else {
            Ok(Some(Self {
                signing_key,
                legacy_token_hash,
            }))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn verify(
        &self,
        signature: Option<&str>,
        message_id: Option<&str>,
        timestamp: Option<&str>,
        legacy_token: Option<&str>,
        fallback_delivery_id: Option<&str>,
        body: &[u8],
        now: DateTime<Utc>,
    ) -> Result<GitLabWebhookVerification> {
        if let Some(received_signatures) = signature {
            let signing_key = self.signing_key.as_deref().ok_or_else(webhook_denied)?;
            let message_id = valid_delivery_id(message_id.ok_or_else(webhook_denied)?)?;
            let timestamp = timestamp.ok_or_else(webhook_denied)?;
            let timestamp_value = timestamp.parse::<i64>().map_err(|_| webhook_denied())?;
            if now.timestamp().abs_diff(timestamp_value) > WEBHOOK_TOLERANCE_SECONDS {
                return Err(webhook_denied());
            }
            let occurred_at =
                DateTime::from_timestamp(timestamp_value, 0).ok_or_else(webhook_denied)?;
            let mut message =
                Vec::with_capacity(message_id.len() + timestamp.len() + body.len() + 2);
            message.extend_from_slice(message_id.as_bytes());
            message.push(b'.');
            message.extend_from_slice(timestamp.as_bytes());
            message.push(b'.');
            message.extend_from_slice(body);
            let verified = received_signatures
                .split_ascii_whitespace()
                .filter_map(|value| value.strip_prefix("v1,"))
                .filter_map(|value| BASE64_STANDARD.decode(value).ok())
                .any(|received| verify_hmac(signing_key, &message, &received));
            if !verified {
                return Err(webhook_denied());
            }
            return Ok(GitLabWebhookVerification {
                delivery_id: message_id.to_string(),
                occurred_at,
                signed: true,
            });
        }

        let expected_hash = self
            .legacy_token_hash
            .as_deref()
            .ok_or_else(webhook_denied)?;
        let received_hash = Sha256::digest(
            legacy_token
                .filter(|token| !token.is_empty())
                .ok_or_else(webhook_denied)?
                .as_bytes(),
        );
        if !bool::from(expected_hash.ct_eq(received_hash.as_slice())) {
            return Err(webhook_denied());
        }
        let delivery_id = fallback_delivery_id
            .map(valid_delivery_id)
            .transpose()?
            .map(str::to_string)
            .unwrap_or_else(|| body_delivery_id(body));
        Ok(GitLabWebhookVerification {
            delivery_id,
            occurred_at: now,
            signed: false,
        })
    }
}

pub fn parse_webhook_event(body: &[u8], occurred_at: DateTime<Utc>) -> Result<GitLabWebhookEvent> {
    let envelope: WebhookEnvelope = serde_json::from_slice(body).map_err(|_| {
        MambaError::Validation("GitLab webhook body is not a recognized JSON event".into())
    })?;
    match envelope.object_kind.as_str() {
        "merge_request" => {
            let payload: MergeRequestWebhook = serde_json::from_slice(body).map_err(|_| {
                MambaError::Validation("invalid GitLab merge request webhook payload".into())
            })?;
            let iid = payload.object_attributes.iid.to_string();
            let project_id = payload.project.id.to_string();
            let status = if payload.object_attributes.state == "merged" {
                "merged".to_string()
            } else if payload.object_attributes.draft {
                "draft".to_string()
            } else {
                payload.object_attributes.state
            };
            Ok(GitLabWebhookEvent::Update(Box::new(GitLabWebhookUpdate {
                event_kind: "merge_request",
                project: payload.project.path_with_namespace.clone(),
                merge_request_iid: iid.clone(),
                artifact: ExternalArtifact {
                    id: artifact_id("gitlab", "merge_request", &project_id, &iid),
                    provider: "gitlab".into(),
                    kind: "merge_request".into(),
                    project: payload.project.path_with_namespace,
                    external_id: iid,
                    parent_id: None,
                    title: payload.object_attributes.title,
                    url: payload.object_attributes.url,
                    status: status.clone(),
                    revision: payload
                        .object_attributes
                        .last_commit
                        .map(|commit| commit.id),
                    verified: status == "merged",
                    synced_at: occurred_at,
                },
            })))
        }
        "pipeline" => {
            let payload: PipelineWebhook = serde_json::from_slice(body).map_err(|_| {
                MambaError::Validation("invalid GitLab pipeline webhook payload".into())
            })?;
            let Some(merge_request) = payload.merge_request else {
                return Ok(GitLabWebhookEvent::Ignored {
                    object_kind: "pipeline_without_merge_request".into(),
                });
            };
            let project_id = payload.project.id.to_string();
            let pipeline_id = payload.object_attributes.id.to_string();
            let mr_iid = merge_request.iid.to_string();
            let merge_request_artifact_id =
                artifact_id("gitlab", "merge_request", &project_id, &mr_iid);
            let title = payload.object_attributes.name.unwrap_or_else(|| {
                format!(
                    "Pipeline #{} ({})",
                    payload.object_attributes.id, payload.object_attributes.git_ref
                )
            });
            Ok(GitLabWebhookEvent::Update(Box::new(GitLabWebhookUpdate {
                event_kind: "pipeline",
                project: payload.project.path_with_namespace.clone(),
                merge_request_iid: mr_iid,
                artifact: ExternalArtifact {
                    id: artifact_id("gitlab", "pipeline", &project_id, &pipeline_id),
                    provider: "gitlab".into(),
                    kind: "pipeline".into(),
                    project: payload.project.path_with_namespace,
                    external_id: pipeline_id,
                    parent_id: Some(merge_request_artifact_id),
                    title,
                    url: payload.object_attributes.url,
                    status: payload.object_attributes.status.clone(),
                    revision: Some(payload.object_attributes.sha),
                    verified: payload.object_attributes.status == "success",
                    synced_at: occurred_at,
                },
            })))
        }
        _ => Ok(GitLabWebhookEvent::Ignored {
            object_kind: envelope.object_kind,
        }),
    }
}

#[derive(Clone)]
pub struct GitLabClient {
    client: Client,
    api_base: Url,
    token: Option<String>,
}

impl GitLabClient {
    pub fn from_env(base_url: Option<&str>) -> Result<Self> {
        let token = std::env::var("GITLAB_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let configured_url = base_url
            .map(str::to_string)
            .or_else(|| std::env::var("GITLAB_URL").ok())
            .unwrap_or_else(|| DEFAULT_GITLAB_URL.to_string());
        Self::new(&configured_url, token.as_deref())
    }

    pub fn new(base_url: &str, token: Option<&str>) -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut api_base = Url::parse(base_url.trim())
            .map_err(|_| MambaError::Validation("invalid GitLab URL".into()))?;
        if !matches!(api_base.scheme(), "http" | "https") {
            return Err(MambaError::Validation(
                "GitLab URL must use http or https".into(),
            ));
        }
        if !api_base.username().is_empty() || api_base.password().is_some() {
            return Err(MambaError::Validation(
                "GitLab URL must not contain credentials; use GITLAB_TOKEN".into(),
            ));
        }
        api_base.set_query(None);
        api_base.set_fragment(None);
        let already_api = api_base.path().trim_end_matches('/').ends_with("/api/v4");
        if !already_api {
            let mut segments = api_base.path_segments_mut().map_err(|_| {
                MambaError::Validation("GitLab URL cannot be used as an API base".into())
            })?;
            segments.pop_if_empty().push("api").push("v4");
        }
        if !api_base.path().ends_with('/') {
            api_base.set_path(&format!("{}/", api_base.path()));
        }
        let client = Client::builder()
            .user_agent(concat!("MambaFlow/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| {
                MambaError::ExternalConnector("could not initialize GitLab client".into())
            })?;
        Ok(Self {
            client,
            api_base,
            token: token.map(str::to_string),
        })
    }

    pub async fn check_project(&self, project: &str) -> Result<GitLabProject> {
        validate_project(project)?;
        let url = self.endpoint(&["projects", project])?;
        self.get_json(url, "read project").await
    }

    pub async fn merge_request_snapshot(
        &self,
        project: &str,
        merge_request_iid: u64,
    ) -> Result<GitLabSnapshot> {
        validate_project(project)?;
        if merge_request_iid == 0 {
            return Err(MambaError::Validation(
                "merge request IID must be greater than zero".into(),
            ));
        }
        let project_info = self.check_project(project).await?;
        let iid = merge_request_iid.to_string();
        let merge_request: GitLabMergeRequest = self
            .get_json(
                self.endpoint(&["projects", project, "merge_requests", &iid])?,
                "read merge request",
            )
            .await?;
        let pipelines: Vec<GitLabPipeline> = self
            .get_json(
                self.endpoint(&["projects", project, "merge_requests", &iid, "pipelines"])?,
                "read merge request pipelines",
            )
            .await?;

        let synced_at = Utc::now();
        let project_key = project_info.id.to_string();
        let mr_external_id = merge_request.iid.to_string();
        let mr_status = if merge_request.merged_at.is_some() || merge_request.state == "merged" {
            "merged".to_string()
        } else if merge_request.draft {
            "draft".to_string()
        } else {
            merge_request.state.clone()
        };
        let merge_request_artifact_id =
            artifact_id("gitlab", "merge_request", &project_key, &mr_external_id);
        let mut artifacts = vec![ExternalArtifact {
            id: merge_request_artifact_id.clone(),
            provider: "gitlab".into(),
            kind: "merge_request".into(),
            project: project_info.path_with_namespace.clone(),
            external_id: mr_external_id,
            parent_id: None,
            title: merge_request.title,
            url: merge_request.web_url,
            status: mr_status.clone(),
            revision: Some(merge_request.sha),
            verified: mr_status == "merged",
            synced_at,
        }];
        if let Some(pipeline) = pipelines.into_iter().max_by_key(|pipeline| pipeline.id) {
            let external_id = pipeline.id.to_string();
            artifacts.push(ExternalArtifact {
                id: artifact_id("gitlab", "pipeline", &project_key, &external_id),
                provider: "gitlab".into(),
                kind: "pipeline".into(),
                project: project_info.path_with_namespace.clone(),
                external_id,
                parent_id: Some(merge_request_artifact_id),
                title: format!("Pipeline #{} ({})", pipeline.id, pipeline.git_ref),
                url: pipeline.web_url,
                status: pipeline.status.clone(),
                revision: Some(pipeline.sha),
                verified: pipeline.status == "success",
                synced_at,
            });
        }
        Ok(GitLabSnapshot {
            project: project_info,
            merge_request_iid,
            artifacts,
        })
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.api_base.clone();
        let mut path = url.path_segments_mut().map_err(|_| {
            MambaError::Validation("GitLab URL cannot be used as an API endpoint".into())
        })?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }

    async fn get_json<T: DeserializeOwned>(&self, url: Url, operation: &str) -> Result<T> {
        let mut request = self.client.get(url);
        if let Some(token) = &self.token {
            request = request.header("PRIVATE-TOKEN", token);
        }
        let response = request.send().await.map_err(|error| {
            MambaError::ExternalConnector(format!("GitLab {operation} request failed: {error}"))
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(gitlab_status_error(operation, status));
        }
        response.json().await.map_err(|_| {
            MambaError::ExternalConnector(format!(
                "GitLab {operation} returned an invalid JSON response"
            ))
        })
    }
}

#[derive(Debug, Deserialize)]
struct GitLabMergeRequest {
    iid: u64,
    title: String,
    state: String,
    web_url: String,
    sha: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    merged_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WebhookEnvelope {
    object_kind: String,
}

#[derive(Debug, Deserialize)]
struct WebhookProject {
    id: u64,
    path_with_namespace: String,
}

#[derive(Debug, Deserialize)]
struct MergeRequestWebhook {
    project: WebhookProject,
    object_attributes: MergeRequestWebhookAttributes,
}

#[derive(Debug, Deserialize)]
struct MergeRequestWebhookAttributes {
    iid: u64,
    title: String,
    state: String,
    #[serde(default)]
    draft: bool,
    url: String,
    #[serde(default)]
    last_commit: Option<WebhookCommit>,
}

#[derive(Debug, Deserialize)]
struct WebhookCommit {
    id: String,
}

#[derive(Debug, Deserialize)]
struct PipelineWebhook {
    project: WebhookProject,
    object_attributes: PipelineWebhookAttributes,
    #[serde(default)]
    merge_request: Option<WebhookMergeRequest>,
}

#[derive(Debug, Deserialize)]
struct PipelineWebhookAttributes {
    id: u64,
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "ref")]
    git_ref: String,
    sha: String,
    status: String,
    url: String,
}

#[derive(Debug, Deserialize)]
struct WebhookMergeRequest {
    iid: u64,
}

#[derive(Debug, Deserialize)]
struct GitLabPipeline {
    id: u64,
    sha: String,
    #[serde(rename = "ref")]
    git_ref: String,
    status: String,
    web_url: String,
}

fn validate_project(project: &str) -> Result<()> {
    if project.trim().is_empty() || project.trim() != project {
        return Err(MambaError::Validation(
            "GitLab project must be a numeric ID or namespace/project path".into(),
        ));
    }
    Ok(())
}

fn parse_signing_token(token: &str) -> Result<Vec<u8>> {
    let encoded = token.strip_prefix("whsec_").ok_or_else(|| {
        MambaError::Validation("GitLab webhook signing token must use the whsec_ format".into())
    })?;
    let key = BASE64_STANDARD.decode(encoded).map_err(|_| {
        MambaError::Validation("GitLab webhook signing token is not valid base64".into())
    })?;
    if key.is_empty() {
        return Err(MambaError::Validation(
            "GitLab webhook signing token cannot be empty".into(),
        ));
    }
    Ok(key)
}

fn verify_hmac(key: &[u8], message: &[u8], received: &[u8]) -> bool {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any size");
    mac.update(message);
    mac.verify_slice(received).is_ok()
}

fn valid_delivery_id(value: &str) -> Result<&str> {
    if value.is_empty()
        || value.len() > 200
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(webhook_denied());
    }
    Ok(value)
}

fn body_delivery_id(body: &[u8]) -> String {
    let digest = Sha256::digest(body);
    let mut id = String::from("legacy-");
    for byte in digest.iter().take(8) {
        write!(&mut id, "{byte:02x}").expect("writing to a string cannot fail");
    }
    id
}

fn webhook_denied() -> MambaError {
    MambaError::PermissionDenied("invalid GitLab webhook authentication".into())
}

fn artifact_id(provider: &str, kind: &str, project: &str, external_id: &str) -> String {
    let digest = Sha256::digest(format!("{provider}:{kind}:{project}:{external_id}").as_bytes());
    let mut id = String::from("EXT-");
    for byte in digest.iter().take(8) {
        write!(&mut id, "{byte:02x}").expect("writing to a string cannot fail");
    }
    id
}

fn gitlab_status_error(operation: &str, status: StatusCode) -> MambaError {
    let hint = match status {
        StatusCode::UNAUTHORIZED => "check GITLAB_TOKEN",
        StatusCode::FORBIDDEN => "the token lacks project access",
        StatusCode::NOT_FOUND => "check the project path, MR IID, and token access",
        _ => "check the GitLab server and project",
    };
    MambaError::ExternalConnector(format!(
        "GitLab {operation} returned HTTP {} ({hint})",
        status.as_u16()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{Router, extract::Request, http::HeaderValue, response::IntoResponse, routing::any};
    use serde_json::json;

    use super::*;

    #[test]
    fn webhook_auth_verifies_signed_and_legacy_deliveries() {
        let key = b"webhook signing key";
        let signing_token = format!("whsec_{}", BASE64_STANDARD.encode(key));
        let auth = GitLabWebhookAuth::new(Some(&signing_token), Some("legacy-secret"))
            .unwrap()
            .unwrap();
        let body = br#"{"object_kind":"merge_request"}"#;
        let now = Utc::now();
        let timestamp = now.timestamp().to_string();
        let delivery_id = "delivery-42";
        let mut message = format!("{delivery_id}.{timestamp}.").into_bytes();
        message.extend_from_slice(body);
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(&message);
        let signature = format!("v1,{}", BASE64_STANDARD.encode(mac.finalize().into_bytes()));

        let verified = auth
            .verify(
                Some(&signature),
                Some(delivery_id),
                Some(&timestamp),
                None,
                None,
                body,
                now,
            )
            .unwrap();
        assert!(verified.signed);
        assert_eq!(verified.delivery_id, delivery_id);
        assert!(
            auth.verify(
                Some(&signature),
                Some(delivery_id),
                Some(&timestamp),
                None,
                None,
                b"tampered",
                now,
            )
            .is_err()
        );
        assert!(
            auth.verify(
                Some(&signature),
                Some(delivery_id),
                Some(&timestamp),
                None,
                None,
                body,
                now + chrono::Duration::minutes(6),
            )
            .is_err()
        );

        let legacy = auth
            .verify(
                None,
                None,
                None,
                Some("legacy-secret"),
                Some("legacy-delivery"),
                body,
                now,
            )
            .unwrap();
        assert!(!legacy.signed);
        assert_eq!(legacy.delivery_id, "legacy-delivery");
    }

    #[tokio::test]
    async fn reads_namespaced_merge_request_and_latest_pipeline() {
        let requests = Arc::new(Mutex::new(Vec::<(String, Option<HeaderValue>)>::new()));
        let captured = Arc::clone(&requests);
        let router = Router::new().fallback(any(move |request: Request| {
            let captured = Arc::clone(&captured);
            async move {
                let path = request.uri().path().to_string();
                let token = request.headers().get("PRIVATE-TOKEN").cloned();
                captured.lock().unwrap().push((path.clone(), token));
                if path.ends_with("/pipelines") {
                    axum::Json(json!([
                        {"id": 8, "sha": "old", "ref": "feature", "status": "failed", "web_url": "https://gitlab.test/p/8"},
                        {"id": 9, "sha": "abc", "ref": "feature", "status": "success", "web_url": "https://gitlab.test/p/9"}
                    ])).into_response()
                } else if path.ends_with("/merge_requests/42") {
                    axum::Json(json!({
                        "iid": 42, "title": "Ship gateway", "state": "merged",
                        "web_url": "https://gitlab.test/mr/42", "sha": "abc",
                        "draft": false, "merged_at": "2026-07-16T00:00:00Z"
                    })).into_response()
                } else {
                    axum::Json(json!({
                        "id": 7, "path_with_namespace": "platform/gateway",
                        "web_url": "https://gitlab.test/platform/gateway",
                        "default_branch": "main"
                    })).into_response()
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        let client = GitLabClient::new(&format!("http://{address}"), Some("secret-token")).unwrap();
        let snapshot = client
            .merge_request_snapshot("platform/gateway", 42)
            .await
            .unwrap();
        assert_eq!(snapshot.project.id, 7);
        assert_eq!(snapshot.artifacts.len(), 2);
        assert!(snapshot.artifacts.iter().all(|artifact| artifact.verified));
        assert_eq!(snapshot.artifacts[1].external_id, "9");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].0.contains("platform%2Fgateway"));
        assert!(requests.iter().all(|(_, token)| {
            token.as_ref().and_then(|value| value.to_str().ok()) == Some("secret-token")
        }));
        server.abort();
    }
}
