use std::time::Duration as StdDuration;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::domain::{NotificationDelivery, NotificationEndpoint, NotificationStatus};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;
use crate::state::OrganizationState;

type HmacSha256 = Hmac<Sha256>;

pub const DEFAULT_EVENT_KINDS: &[&str] = &[
    "work_request.sent",
    "flow_message.posted",
    "task.blocked",
    "task.submitted",
    "tracking.escalation_raised",
    "flow_change.proposed",
    "flow_change.applied",
    "flow_change.rejected",
    "remote_flight.crashed",
    "flow.completed",
];

#[derive(Clone, Debug, Serialize)]
pub struct NotificationWebhook {
    pub specversion: &'static str,
    pub id: String,
    pub source: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub subject: Option<String>,
    pub time: DateTime<Utc>,
    pub actor: String,
    pub data: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationAttempt {
    pub delivered: bool,
    pub response_status: Option<u16>,
    pub error: Option<String>,
    pub attempted_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NotificationDispatchSummary {
    pub attempted: usize,
    pub delivered: usize,
    pub failed: usize,
}

pub fn validate_endpoint(endpoint: &NotificationEndpoint) -> Result<()> {
    if endpoint.name.trim().is_empty() || endpoint.name.chars().count() > 100 {
        return Err(MambaError::Validation(
            "notification endpoint name must contain 1 to 100 characters".into(),
        ));
    }
    let url = Url::parse(&endpoint.url)
        .map_err(|_| MambaError::Validation("notification URL is invalid".into()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(MambaError::Validation(
            "notification URL must use http or https".into(),
        ));
    }
    if endpoint.event_kinds.is_empty() {
        return Err(MambaError::Validation(
            "notification endpoint must subscribe to at least one event kind".into(),
        ));
    }
    if endpoint
        .event_kinds
        .iter()
        .any(|kind| kind.trim().is_empty() || kind.starts_with("notification."))
    {
        return Err(MambaError::Validation(
            "notification event filters cannot be empty or subscribe to notification.*".into(),
        ));
    }
    if endpoint.secret_env.trim().is_empty()
        || !endpoint
            .secret_env
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(MambaError::Validation(
            "notification secret environment name must contain only letters, digits and _".into(),
        ));
    }
    Ok(())
}

pub fn queue_events(
    state: &OrganizationState,
    organization_id: &str,
    actor: &str,
    events: &[DomainEvent],
) -> Result<Vec<DomainEvent>> {
    let mut queued = Vec::new();
    for event in events {
        let event_kind = event.kind();
        if event_kind.starts_with("notification.") {
            continue;
        }
        let payload = serde_json::to_value(event)?;
        for endpoint in state.notification_endpoints.values().filter(|endpoint| {
            endpoint.active
                && endpoint
                    .event_kinds
                    .iter()
                    .any(|filter| filter == "*" || filter == event_kind)
        }) {
            queued.push(DomainEvent::NotificationQueued {
                delivery: Box::new(NotificationDelivery {
                    id: new_id("NTF"),
                    organization_id: organization_id.to_string(),
                    endpoint_id: endpoint.id.clone(),
                    source_event_kind: event_kind.to_string(),
                    flow_id: event.flow_id().map(str::to_string),
                    actor: actor.to_string(),
                    payload: payload.clone(),
                    status: NotificationStatus::Pending,
                    attempts: 0,
                    queued_at: Utc::now(),
                    last_attempt_at: None,
                    delivered_at: None,
                    response_status: None,
                    last_error: None,
                }),
            });
        }
    }
    Ok(queued)
}

pub async fn deliver(
    endpoint: &NotificationEndpoint,
    delivery: &NotificationDelivery,
) -> NotificationAttempt {
    let attempted_at = Utc::now();
    let secret = match std::env::var(&endpoint.secret_env) {
        Ok(secret) if !secret.is_empty() => secret,
        _ => {
            return NotificationAttempt {
                delivered: false,
                response_status: None,
                error: Some(format!(
                    "secret environment variable {} is missing",
                    endpoint.secret_env
                )),
                attempted_at,
            };
        }
    };
    deliver_with_secret(endpoint, delivery, secret.as_bytes(), attempted_at).await
}

pub async fn deliver_with_secret(
    endpoint: &NotificationEndpoint,
    delivery: &NotificationDelivery,
    secret: &[u8],
    attempted_at: DateTime<Utc>,
) -> NotificationAttempt {
    let webhook = NotificationWebhook {
        specversion: "1.0",
        id: delivery.id.clone(),
        source: format!("mambaflow://organizations/{}", delivery.organization_id),
        event_type: delivery.source_event_kind.clone(),
        subject: delivery
            .flow_id
            .as_ref()
            .map(|flow_id| format!("mambaflow://flows/{flow_id}")),
        time: delivery.queued_at,
        actor: delivery.actor.clone(),
        data: delivery.payload.clone(),
    };
    let body = match serde_json::to_vec(&webhook) {
        Ok(body) => body,
        Err(error) => {
            return NotificationAttempt {
                delivered: false,
                response_status: None,
                error: Some(format!("webhook serialization failed: {error}")),
                attempted_at,
            };
        }
    };
    let timestamp = attempted_at.timestamp().to_string();
    let signature = sign(secret, &delivery.id, &timestamp, &body);
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = match Client::builder()
        .timeout(StdDuration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return failed_attempt(attempted_at, None, format!("HTTP client failed: {error}"));
        }
    };
    match client
        .post(&endpoint.url)
        .header("content-type", "application/json")
        .header("user-agent", "MambaFlow-Notification/1.0")
        .header("webhook-id", &delivery.id)
        .header("webhook-timestamp", &timestamp)
        .header("webhook-signature", format!("v1,{signature}"))
        .body(body)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => NotificationAttempt {
            delivered: true,
            response_status: Some(response.status().as_u16()),
            error: None,
            attempted_at,
        },
        Ok(response) => failed_attempt(
            attempted_at,
            Some(response.status().as_u16()),
            format!("endpoint returned HTTP {}", response.status().as_u16()),
        ),
        Err(error) => failed_attempt(attempted_at, None, format!("delivery failed: {error}")),
    }
}

fn sign(secret: &[u8], delivery_id: &str, timestamp: &str, body: &[u8]) -> String {
    let mut message = Vec::with_capacity(delivery_id.len() + timestamp.len() + body.len() + 2);
    message.extend_from_slice(delivery_id.as_bytes());
    message.push(b'.');
    message.extend_from_slice(timestamp.as_bytes());
    message.push(b'.');
    message.extend_from_slice(body);
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts keys of any size");
    mac.update(&message);
    BASE64_STANDARD.encode(mac.finalize().into_bytes())
}

fn failed_attempt(
    attempted_at: DateTime<Utc>,
    response_status: Option<u16>,
    error: String,
) -> NotificationAttempt {
    NotificationAttempt {
        delivered: false,
        response_status,
        error: Some(error.chars().take(500).collect()),
        attempted_at,
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Bytes;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn webhook_delivery_is_signed_and_uses_a_stable_id() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel::<(HeaderMap, Bytes)>();
        let service = Router::new().route(
            "/hook",
            post(move |headers: HeaderMap, body: Bytes| {
                let tx = tx.clone();
                async move {
                    tx.send((headers, body)).unwrap();
                    StatusCode::NO_CONTENT
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
        let now = Utc::now();
        let endpoint = NotificationEndpoint {
            id: "NEND-1".into(),
            name: "test".into(),
            url: format!("http://{address}/hook"),
            event_kinds: vec!["task.blocked".into()],
            secret_env: "NOT_USED_IN_TEST".into(),
            active: true,
            created_by: "admin".into(),
            created_at: now,
            disabled_by: None,
            disabled_at: None,
        };
        let delivery = NotificationDelivery {
            id: "NTF-1".into(),
            organization_id: "ORG-1".into(),
            endpoint_id: endpoint.id.clone(),
            source_event_kind: "task.blocked".into(),
            flow_id: Some("FLOW-1".into()),
            actor: "Engineer".into(),
            payload: serde_json::json!({"reason": "waiting for access"}),
            status: NotificationStatus::Pending,
            attempts: 0,
            queued_at: now,
            last_attempt_at: None,
            delivered_at: None,
            response_status: None,
            last_error: None,
        };
        let secret = b"test webhook secret";

        let attempt = deliver_with_secret(&endpoint, &delivery, secret, now).await;
        assert!(attempt.delivered);
        assert_eq!(attempt.response_status, Some(204));
        let (headers, body) = rx.recv().await.unwrap();
        assert_eq!(headers["webhook-id"], "NTF-1");
        assert_eq!(headers["webhook-timestamp"], now.timestamp().to_string());
        let expected = format!(
            "v1,{}",
            sign(secret, "NTF-1", &now.timestamp().to_string(), body.as_ref())
        );
        assert_eq!(headers["webhook-signature"], expected);
        let webhook: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(webhook["id"], "NTF-1");
        assert_eq!(webhook["type"], "task.blocked");
        assert_eq!(webhook["subject"], "mambaflow://flows/FLOW-1");
        server.abort();
    }
}
