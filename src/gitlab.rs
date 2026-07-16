use std::fmt::Write as _;

use chrono::Utc;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::domain::ExternalArtifact;
use crate::error::{MambaError, Result};

pub const DEFAULT_GITLAB_URL: &str = "https://gitlab.com";

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
        let mut artifacts = vec![ExternalArtifact {
            id: artifact_id("gitlab", "merge_request", &project_key, &mr_external_id),
            provider: "gitlab".into(),
            kind: "merge_request".into(),
            project: project_info.path_with_namespace.clone(),
            external_id: mr_external_id,
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
