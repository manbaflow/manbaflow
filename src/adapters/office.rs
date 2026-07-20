use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use reqwest::{Client, Response, Url};
use serde_json::{Value, json};

use crate::domain::{
    OfficeBodyType, OfficeProvider, OfficeReleasePayload, OfficeReleaseRequest, OfficeReleaseResult,
};
use crate::error::{MambaError, Result};

#[derive(Clone)]
pub struct OfficeBridge {
    client: Client,
    microsoft: ProviderConfig,
    google: ProviderConfig,
}

#[derive(Clone)]
struct ProviderConfig {
    base_url: Url,
    fallback_token: Option<String>,
    tenant_tokens: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OfficeDispatchError {
    pub message: String,
    pub indeterminate: bool,
}

impl std::fmt::Display for OfficeDispatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl OfficeBridge {
    pub fn from_env() -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|error| office_error("could not build HTTP client", error))?;
        Ok(Self {
            client,
            microsoft: ProviderConfig::from_env(
                "MAMBA_MICROSOFT_GRAPH_BASE_URL",
                "https://graph.microsoft.com/v1.0/",
                "MAMBA_MICROSOFT_GRAPH_TOKEN",
                "MAMBA_MICROSOFT_GRAPH_TOKENS_JSON",
            )?,
            google: ProviderConfig::from_env(
                "MAMBA_GOOGLE_WORKSPACE_BASE_URL",
                "https://www.googleapis.com/",
                "MAMBA_GOOGLE_WORKSPACE_TOKEN",
                "MAMBA_GOOGLE_WORKSPACE_TOKENS_JSON",
            )?,
        })
    }

    pub fn disabled() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self {
            client: Client::new(),
            microsoft: ProviderConfig {
                base_url: Url::parse("https://graph.microsoft.com/v1.0/")
                    .expect("static Microsoft Graph URL is valid"),
                fallback_token: None,
                tenant_tokens: BTreeMap::new(),
            },
            google: ProviderConfig {
                base_url: Url::parse("https://www.googleapis.com/")
                    .expect("static Google Workspace URL is valid"),
                fallback_token: None,
                tenant_tokens: BTreeMap::new(),
            },
        }
    }

    pub fn configured(&self, tenant_id: &str, provider: OfficeProvider) -> bool {
        self.provider(provider).token(tenant_id).is_some()
    }

    pub async fn dispatch(
        &self,
        tenant_id: &str,
        release: &OfficeReleaseRequest,
        artifact: Option<&[u8]>,
    ) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
        let provider = self.provider(release.provider);
        let token = provider
            .token(tenant_id)
            .ok_or_else(|| OfficeDispatchError {
                message: format!("{:?} Office Bridge is not configured", release.provider),
                indeterminate: false,
            })?;
        match (&release.provider, &release.payload) {
            (OfficeProvider::Microsoft365, OfficeReleasePayload::DriveUpload { .. }) => {
                self.microsoft_drive(provider, token, release, required_artifact(artifact)?)
                    .await
            }
            (OfficeProvider::Microsoft365, OfficeReleasePayload::SendEmail { .. }) => {
                self.microsoft_mail(provider, token, release).await
            }
            (OfficeProvider::Microsoft365, OfficeReleasePayload::CreateCalendarEvent { .. }) => {
                self.microsoft_calendar(provider, token, release).await
            }
            (OfficeProvider::GoogleWorkspace, OfficeReleasePayload::DriveUpload { .. }) => {
                self.google_drive(provider, token, release, required_artifact(artifact)?)
                    .await
            }
            (OfficeProvider::GoogleWorkspace, OfficeReleasePayload::SendEmail { .. }) => {
                self.google_mail(provider, token, release).await
            }
            (OfficeProvider::GoogleWorkspace, OfficeReleasePayload::CreateCalendarEvent { .. }) => {
                self.google_calendar(provider, token, release).await
            }
        }
    }

    fn provider(&self, provider: OfficeProvider) -> &ProviderConfig {
        match provider {
            OfficeProvider::Microsoft365 => &self.microsoft,
            OfficeProvider::GoogleWorkspace => &self.google,
        }
    }

    async fn microsoft_drive(
        &self,
        provider: &ProviderConfig,
        token: &str,
        release: &OfficeReleaseRequest,
        artifact: &[u8],
    ) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
        let OfficeReleasePayload::DriveUpload {
            account_id,
            parent_id,
            file_name,
            ..
        } = &release.payload
        else {
            unreachable!()
        };
        let url = provider.url(&[
            "users",
            account_id,
            "drive",
            "items",
            &format!("{parent_id}:"),
            &format!("{file_name}:"),
            "content",
        ])?;
        let response = self
            .send(
                self.client
                    .put(url)
                    .bearer_auth(token)
                    .header(reqwest::header::CONTENT_TYPE, artifact_media_type(release))
                    .body(artifact.to_vec()),
                release,
            )
            .await?;
        result_from_json(response, release, false).await
    }

    async fn microsoft_mail(
        &self,
        provider: &ProviderConfig,
        token: &str,
        release: &OfficeReleaseRequest,
    ) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
        let OfficeReleasePayload::SendEmail {
            account_id,
            to,
            cc,
            bcc,
            subject,
            body,
            body_type,
        } = &release.payload
        else {
            unreachable!()
        };
        let url = provider.url(&["users", account_id, "sendMail"])?;
        let response = self
            .send(
                self.client.post(url).bearer_auth(token).json(&json!({
                    "message": {
                        "subject": subject,
                        "body": {"contentType": microsoft_body_type(*body_type), "content": body},
                        "toRecipients": microsoft_recipients(to),
                        "ccRecipients": microsoft_recipients(cc),
                        "bccRecipients": microsoft_recipients(bcc)
                    },
                    "saveToSentItems": true
                })),
                release,
            )
            .await?;
        success_without_body(response, release).await
    }

    async fn microsoft_calendar(
        &self,
        provider: &ProviderConfig,
        token: &str,
        release: &OfficeReleaseRequest,
    ) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
        let OfficeReleasePayload::CreateCalendarEvent {
            account_id,
            calendar_id,
            subject,
            body,
            body_type,
            start,
            end,
            time_zone,
            attendees,
            location,
            ..
        } = &release.payload
        else {
            unreachable!()
        };
        let mut segments = vec!["users", account_id.as_str()];
        if calendar_id == "default" {
            segments.push("events");
        } else {
            segments.extend(["calendars", calendar_id.as_str(), "events"]);
        }
        let url = provider.url(&segments)?;
        let response = self
            .send(
                self.client.post(url).bearer_auth(token).json(&json!({
                    "subject": subject,
                    "body": {"contentType": microsoft_body_type(*body_type), "content": body},
                    "start": {"dateTime": start.to_rfc3339(), "timeZone": time_zone},
                    "end": {"dateTime": end.to_rfc3339(), "timeZone": time_zone},
                    "attendees": attendees.iter().map(|address| json!({
                        "emailAddress": {"address": address}, "type": "required"
                    })).collect::<Vec<_>>(),
                    "location": location.as_ref().map(|value| json!({"displayName": value})),
                    "transactionId": release.id
                })),
                release,
            )
            .await?;
        result_from_json(response, release, false).await
    }

    async fn google_drive(
        &self,
        provider: &ProviderConfig,
        token: &str,
        release: &OfficeReleaseRequest,
        artifact: &[u8],
    ) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
        let OfficeReleasePayload::DriveUpload {
            parent_id,
            file_name,
            file_id,
            ..
        } = &release.payload
        else {
            unreachable!()
        };
        let response = if let Some(file_id) = file_id {
            let mut url = provider.url(&["upload", "drive", "v3", "files", file_id])?;
            url.query_pairs_mut()
                .append_pair("uploadType", "media")
                .append_pair("supportsAllDrives", "true")
                .append_pair("fields", "id,webViewLink");
            self.send(
                self.client
                    .patch(url)
                    .bearer_auth(token)
                    .header(reqwest::header::CONTENT_TYPE, artifact_media_type(release))
                    .body(artifact.to_vec()),
                release,
            )
            .await?
        } else {
            let mut url = provider.url(&["upload", "drive", "v3", "files"])?;
            url.query_pairs_mut()
                .append_pair("uploadType", "multipart")
                .append_pair("supportsAllDrives", "true")
                .append_pair("fields", "id,webViewLink");
            let boundary = format!("mambaflow-{}", release.id);
            let metadata = serde_json::to_vec(&json!({
                "name": file_name,
                "parents": [parent_id],
                "appProperties": {
                    "mambaflowReleaseId": release.id,
                    "mambaflowPayloadSha256": release.payload_sha256
                }
            }))
            .map_err(|error| dispatch_error(error, false))?;
            let body = multipart_body(&boundary, &metadata, artifact_media_type(release), artifact);
            self.send(
                self.client
                    .post(url)
                    .bearer_auth(token)
                    .header(
                        reqwest::header::CONTENT_TYPE,
                        format!("multipart/related; boundary={boundary}"),
                    )
                    .body(body),
                release,
            )
            .await?
        };
        result_from_json(response, release, false).await
    }

    async fn google_mail(
        &self,
        provider: &ProviderConfig,
        token: &str,
        release: &OfficeReleaseRequest,
    ) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
        let OfficeReleasePayload::SendEmail { account_id, .. } = &release.payload else {
            unreachable!()
        };
        let url = provider.url(&["gmail", "v1", "users", account_id, "messages", "send"])?;
        let raw = URL_SAFE_NO_PAD.encode(rfc822_message(&release.payload));
        let response = self
            .send(
                self.client
                    .post(url)
                    .bearer_auth(token)
                    .json(&json!({"raw": raw})),
                release,
            )
            .await?;
        result_from_json(response, release, false).await
    }

    async fn google_calendar(
        &self,
        provider: &ProviderConfig,
        token: &str,
        release: &OfficeReleaseRequest,
    ) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
        let OfficeReleasePayload::CreateCalendarEvent {
            calendar_id,
            subject,
            body,
            start,
            end,
            time_zone,
            attendees,
            location,
            send_updates,
            ..
        } = &release.payload
        else {
            unreachable!()
        };
        let mut url = provider.url(&["calendar", "v3", "calendars", calendar_id, "events"])?;
        url.query_pairs_mut()
            .append_pair("sendUpdates", if *send_updates { "all" } else { "none" });
        let response = self
            .send(
                self.client.post(url).bearer_auth(token).json(&json!({
                    "id": google_event_id(&release.id),
                    "summary": subject,
                    "description": body,
                    "start": {"dateTime": start.to_rfc3339(), "timeZone": time_zone},
                    "end": {"dateTime": end.to_rfc3339(), "timeZone": time_zone},
                    "attendees": attendees.iter().map(|email| json!({"email": email})).collect::<Vec<_>>(),
                    "location": location,
                    "extendedProperties": {"private": {
                        "mambaflowReleaseId": release.id,
                        "mambaflowPayloadSha256": release.payload_sha256
                    }}
                })),
                release,
            )
            .await?;
        if response.status() == reqwest::StatusCode::CONFLICT {
            return Ok(OfficeReleaseResult {
                external_id: Some(google_event_id(&release.id)),
                url: None,
                response_status: response.status().as_u16(),
                released_at: Utc::now(),
            });
        }
        result_from_json(response, release, false).await
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        release: &OfficeReleaseRequest,
    ) -> std::result::Result<Response, OfficeDispatchError> {
        request
            .send()
            .await
            .map_err(|error| dispatch_error(error, !release.payload.retry_safe(release.provider)))
    }
}

impl ProviderConfig {
    fn from_env(
        base_name: &str,
        default_base: &str,
        token_name: &str,
        tokens_name: &str,
    ) -> Result<Self> {
        let base = std::env::var(base_name).unwrap_or_else(|_| default_base.into());
        let base_url = validate_base_url(base_name, &base)?;
        let fallback_token = optional_env(token_name)?;
        let tenant_tokens = optional_env(tokens_name)?
            .map(|value| {
                serde_json::from_str::<BTreeMap<String, String>>(&value).map_err(|_| {
                    MambaError::Validation(format!(
                        "{tokens_name} must be a JSON object of Tenant IDs to OAuth access tokens"
                    ))
                })
            })
            .transpose()?
            .unwrap_or_default();
        for (tenant_id, token) in &tenant_tokens {
            if !tenant_id.starts_with("TEN-") || token.len() < 20 {
                return Err(MambaError::Validation(format!(
                    "invalid Tenant ID or token in {tokens_name}: {tenant_id}"
                )));
            }
        }
        if fallback_token
            .as_ref()
            .is_some_and(|token| token.len() < 20)
        {
            return Err(MambaError::Validation(format!(
                "{token_name} must contain at least 20 characters"
            )));
        }
        Ok(Self {
            base_url,
            fallback_token,
            tenant_tokens,
        })
    }

    fn token(&self, tenant_id: &str) -> Option<&str> {
        if self.tenant_tokens.is_empty() {
            self.fallback_token.as_deref()
        } else {
            self.tenant_tokens.get(tenant_id).map(String::as_str)
        }
    }

    fn url(&self, segments: &[&str]) -> std::result::Result<Url, OfficeDispatchError> {
        let mut url = self.base_url.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|_| dispatch_error("Office provider URL cannot form a path", false))?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }
}

fn required_artifact(artifact: Option<&[u8]>) -> std::result::Result<&[u8], OfficeDispatchError> {
    artifact.ok_or_else(|| dispatch_error("approved drive artifact content is missing", false))
}

fn artifact_media_type(release: &OfficeReleaseRequest) -> &str {
    match &release.payload {
        OfficeReleasePayload::DriveUpload { file_name, .. } => match file_name
            .rsplit_once('.')
            .map(|(_, extension)| extension.to_ascii_lowercase())
            .as_deref()
        {
            Some("pdf") => "application/pdf",
            Some("docx") => {
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            }
            Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Some("pptx") => {
                "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            }
            Some("html") => "text/html",
            Some("csv") => "text/csv",
            Some("txt" | "md") => "text/plain",
            _ => "application/octet-stream",
        },
        _ => "application/octet-stream",
    }
}

fn microsoft_body_type(body_type: OfficeBodyType) -> &'static str {
    match body_type {
        OfficeBodyType::Text => "Text",
        OfficeBodyType::Html => "HTML",
    }
}

fn microsoft_recipients(addresses: &[String]) -> Vec<Value> {
    addresses
        .iter()
        .map(|address| json!({"emailAddress": {"address": address}}))
        .collect()
}

fn rfc822_message(payload: &OfficeReleasePayload) -> Vec<u8> {
    let OfficeReleasePayload::SendEmail {
        to,
        cc,
        bcc,
        subject,
        body,
        body_type,
        ..
    } = payload
    else {
        unreachable!()
    };
    let subtype = match body_type {
        OfficeBodyType::Text => "plain",
        OfficeBodyType::Html => "html",
    };
    let mut message = format!(
        "To: {}\r\nSubject: {}\r\nMIME-Version: 1.0\r\nContent-Type: text/{subtype}; charset=UTF-8\r\nContent-Transfer-Encoding: 8bit\r\n",
        to.join(", "),
        subject
    );
    if !cc.is_empty() {
        message.push_str(&format!("Cc: {}\r\n", cc.join(", ")));
    }
    if !bcc.is_empty() {
        message.push_str(&format!("Bcc: {}\r\n", bcc.join(", ")));
    }
    message.push_str("\r\n");
    message.push_str(body);
    message.into_bytes()
}

fn multipart_body(boundary: &str, metadata: &[u8], media_type: &str, content: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(metadata);
    body.extend_from_slice(
        format!("\r\n--{boundary}\r\nContent-Type: {media_type}\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(content);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

fn google_event_id(release_id: &str) -> String {
    let suffix = release_id
        .chars()
        .filter(|character| character.is_ascii_hexdigit())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    format!("mmb{suffix}")
}

async fn result_from_json(
    response: Response,
    release: &OfficeReleaseRequest,
    accept_empty: bool,
) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
    let status = response.status();
    if !status.is_success() {
        return Err(response_error(response, release).await);
    }
    let value = if accept_empty {
        Value::Null
    } else {
        response.json::<Value>().await.unwrap_or(Value::Null)
    };
    Ok(OfficeReleaseResult {
        external_id: value.get("id").and_then(Value::as_str).map(str::to_string),
        url: value
            .get("webUrl")
            .or_else(|| value.get("webViewLink"))
            .or_else(|| value.get("htmlLink"))
            .and_then(Value::as_str)
            .map(str::to_string),
        response_status: status.as_u16(),
        released_at: Utc::now(),
    })
}

async fn success_without_body(
    response: Response,
    release: &OfficeReleaseRequest,
) -> std::result::Result<OfficeReleaseResult, OfficeDispatchError> {
    let status = response.status();
    if !status.is_success() {
        return Err(response_error(response, release).await);
    }
    Ok(OfficeReleaseResult {
        external_id: None,
        url: None,
        response_status: status.as_u16(),
        released_at: Utc::now(),
    })
}

async fn response_error(response: Response, release: &OfficeReleaseRequest) -> OfficeDispatchError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let may_have_applied = status.is_server_error()
        || matches!(
            status,
            reqwest::StatusCode::REQUEST_TIMEOUT
                | reqwest::StatusCode::TOO_EARLY
                | reqwest::StatusCode::TOO_MANY_REQUESTS
        );
    OfficeDispatchError {
        message: format!(
            "Office provider returned {}: {}",
            status.as_u16(),
            truncate(&body, 1_000)
        ),
        indeterminate: may_have_applied && !release.payload.retry_safe(release.provider),
    }
}

fn dispatch_error(error: impl std::fmt::Display, indeterminate: bool) -> OfficeDispatchError {
    OfficeDispatchError {
        message: error.to_string(),
        indeterminate,
    }
}

fn office_error(context: &str, error: impl std::fmt::Display) -> MambaError {
    MambaError::ExternalConnector(format!("Office Bridge {context}: {error}"))
}

fn optional_env(name: &str) -> Result<Option<String>> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| MambaError::Validation(format!("{name} must be valid UTF-8")))?;
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(MambaError::Validation(format!("{name} cannot be empty")));
    }
    Ok(Some(value))
}

fn validate_base_url(name: &str, value: &str) -> Result<Url> {
    let mut url =
        Url::parse(value).map_err(|_| MambaError::Validation(format!("invalid {name}")))?;
    let secure = url.scheme() == "https";
    let loopback = url.scheme() == "http"
        && url
            .host_str()
            .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    if (!secure && !loopback)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(MambaError::Validation(format!(
            "{name} must be HTTPS or loopback HTTP without credentials, query or fragment"
        )));
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url)
}

fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::{Request, State};
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    use axum::routing::any;
    use chrono::Duration;
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;
    use crate::domain::{OfficeReleasePayload, OfficeReleaseStatus};

    #[derive(Clone, Debug)]
    struct CapturedRequest {
        method: String,
        uri: String,
        authorization: String,
        content_type: String,
        body: Vec<u8>,
    }

    #[tokio::test]
    async fn graph_and_google_requests_keep_release_payloads_and_artifacts_exact() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let service = Router::new()
            .fallback(any(capture_request))
            .with_state(requests.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
        let base = Url::parse(&format!("http://{address}/")).unwrap();
        let token = "office-test-access-token-at-least-twenty".to_string();
        let config = ProviderConfig {
            base_url: base,
            fallback_token: Some(token.clone()),
            tenant_tokens: BTreeMap::new(),
        };
        let bridge = OfficeBridge {
            client: Client::new(),
            microsoft: config.clone(),
            google: config,
        };

        let graph_drive = release(
            "REL-graph-drive",
            OfficeProvider::Microsoft365,
            OfficeReleasePayload::DriveUpload {
                artifact_id: "ART-1".into(),
                account_id: "pilot@example.com".into(),
                parent_id: "folder-1".into(),
                file_name: "weekly.docx".into(),
                file_id: None,
            },
        );
        let drive_result = bridge
            .dispatch("TEN-test", &graph_drive, Some(b"docx-bytes"))
            .await
            .unwrap();
        assert_eq!(drive_result.external_id.as_deref(), Some("external-42"));

        let graph_email = release(
            "REL-graph-mail",
            OfficeProvider::Microsoft365,
            email_payload("pilot@example.com"),
        );
        assert_eq!(
            bridge
                .dispatch("TEN-test", &graph_email, None)
                .await
                .unwrap()
                .response_status,
            202
        );

        let graph_calendar = release(
            "REL-graph-calendar",
            OfficeProvider::Microsoft365,
            calendar_payload("default", "UTC"),
        );
        bridge
            .dispatch("TEN-test", &graph_calendar, None)
            .await
            .unwrap();

        let google_drive = release(
            "REL-google-drive",
            OfficeProvider::GoogleWorkspace,
            OfficeReleasePayload::DriveUpload {
                artifact_id: "ART-2".into(),
                account_id: "me".into(),
                parent_id: "shared-folder".into(),
                file_name: "metrics.xlsx".into(),
                file_id: None,
            },
        );
        bridge
            .dispatch("TEN-test", &google_drive, Some(b"xlsx-bytes"))
            .await
            .unwrap();

        let google_email = release(
            "REL-google-mail",
            OfficeProvider::GoogleWorkspace,
            email_payload("me"),
        );
        bridge
            .dispatch("TEN-test", &google_email, None)
            .await
            .unwrap();

        let google_calendar = release(
            "REL-google-calendar-42",
            OfficeProvider::GoogleWorkspace,
            OfficeReleasePayload::CreateCalendarEvent {
                account_id: "me".into(),
                calendar_id: "primary".into(),
                subject: "Launch review".into(),
                body: "Review evidence".into(),
                body_type: OfficeBodyType::Text,
                start: Utc::now() + Duration::hours(1),
                end: Utc::now() + Duration::hours(2),
                time_zone: "Asia/Shanghai".into(),
                attendees: vec!["reviewer@example.com".into()],
                location: Some("Tower".into()),
                send_updates: true,
            },
        );
        bridge
            .dispatch("TEN-test", &google_calendar, None)
            .await
            .unwrap();

        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 6);
        assert!(
            captured
                .iter()
                .all(|request| { request.authorization == format!("Bearer {token}") })
        );
        let drive = &captured[0];
        assert_eq!(drive.method, "PUT");
        assert_eq!(
            drive.uri,
            "/users/pilot@example.com/drive/items/folder-1:/weekly.docx:/content"
        );
        assert_eq!(drive.body, b"docx-bytes");
        assert_eq!(
            drive.content_type,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
        let graph_mail_body = serde_json::from_slice::<Value>(&captured[1].body).unwrap();
        assert_eq!(
            graph_mail_body["message"]["toRecipients"][0]["emailAddress"]["address"],
            "team@example.com"
        );
        assert_eq!(captured[2].uri, "/users/pilot@example.com/events");
        let graph_calendar_body = serde_json::from_slice::<Value>(&captured[2].body).unwrap();
        assert_eq!(graph_calendar_body["transactionId"], "REL-graph-calendar");
        assert!(captured[3].uri.contains("uploadType=multipart"));
        assert!(captured[3].content_type.starts_with("multipart/related"));
        assert!(
            captured[3]
                .body
                .windows(b"xlsx-bytes".len())
                .any(|window| window == b"xlsx-bytes")
        );
        let gmail_body = serde_json::from_slice::<Value>(&captured[4].body).unwrap();
        let raw = URL_SAFE_NO_PAD
            .decode(gmail_body["raw"].as_str().unwrap())
            .unwrap();
        assert!(
            String::from_utf8(raw)
                .unwrap()
                .contains("Subject: Weekly update")
        );
        assert!(captured[5].uri.contains("sendUpdates=all"));
        let calendar = serde_json::from_slice::<Value>(&captured[5].body).unwrap();
        assert_eq!(calendar["summary"], "Launch review");
        server.abort();
    }

    async fn capture_request(
        State(requests): State<Arc<Mutex<Vec<CapturedRequest>>>>,
        request: Request,
    ) -> impl IntoResponse {
        let method = request.method().to_string();
        let uri = request
            .uri()
            .path_and_query()
            .map(ToString::to_string)
            .unwrap();
        let authorization = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let content_type = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = axum::body::to_bytes(request.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        requests.lock().unwrap().push(CapturedRequest {
            method,
            uri: uri.clone(),
            authorization,
            content_type,
            body,
        });
        if uri.ends_with("sendMail") {
            StatusCode::ACCEPTED.into_response()
        } else {
            (
                StatusCode::CREATED,
                axum::Json(json!({
                    "id": "external-42",
                    "webUrl": "https://office.example/external-42"
                })),
            )
                .into_response()
        }
    }

    fn email_payload(account_id: &str) -> OfficeReleasePayload {
        OfficeReleasePayload::SendEmail {
            account_id: account_id.into(),
            to: vec!["team@example.com".into()],
            cc: vec!["lead@example.com".into()],
            bcc: Vec::new(),
            subject: "Weekly update".into(),
            body: "All flights landed.".into(),
            body_type: OfficeBodyType::Text,
        }
    }

    fn calendar_payload(calendar_id: &str, time_zone: &str) -> OfficeReleasePayload {
        OfficeReleasePayload::CreateCalendarEvent {
            account_id: "pilot@example.com".into(),
            calendar_id: calendar_id.into(),
            subject: "Launch review".into(),
            body: "Review evidence".into(),
            body_type: OfficeBodyType::Text,
            start: Utc::now() + Duration::hours(1),
            end: Utc::now() + Duration::hours(2),
            time_zone: time_zone.into(),
            attendees: vec!["reviewer@example.com".into()],
            location: Some("Tower".into()),
            send_updates: true,
        }
    }

    fn release(
        id: &str,
        provider: OfficeProvider,
        payload: OfficeReleasePayload,
    ) -> OfficeReleaseRequest {
        OfficeReleaseRequest {
            id: id.into(),
            flow_id: "FLOW-1".into(),
            task_id: "TSK-1".into(),
            provider,
            payload,
            payload_sha256: "a".repeat(64),
            requested_by: "HUM-1".into(),
            requested_at: Utc::now(),
            status: OfficeReleaseStatus::Dispatching,
            reviewed_by: Some("HUM-1".into()),
            reviewed_at: Some(Utc::now()),
            review_reason: None,
            dispatch_id: Some("DSP-1".into()),
            dispatch_started_at: Some(Utc::now()),
            result: None,
            last_error: None,
        }
    }
}
