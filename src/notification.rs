use std::time::Duration as StdDuration;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;

use crate::domain::{
    NotificationConnector, NotificationDelivery, NotificationEndpoint, NotificationStatus,
};
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
    match endpoint.url_env.as_deref() {
        Some(url_env) => validate_env_name(url_env, "notification URL")?,
        None => {
            let url = Url::parse(&endpoint.url)
                .map_err(|_| MambaError::Validation("notification URL is invalid".into()))?;
            if !matches!(url.scheme(), "http" | "https") {
                return Err(MambaError::Validation(
                    "notification URL must use http or https".into(),
                ));
            }
        }
    }
    if endpoint.connector != NotificationConnector::Generic
        && (endpoint.url_env.is_none() || !endpoint.url.is_empty())
    {
        return Err(MambaError::Validation(
            "provider connector URLs must be referenced through an environment variable".into(),
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
    match endpoint.connector {
        NotificationConnector::Generic => {
            validate_env_name(&endpoint.secret_env, "notification secret")?;
        }
        NotificationConnector::Feishu if !endpoint.secret_env.is_empty() => {
            validate_env_name(&endpoint.secret_env, "Feishu signing secret")?;
        }
        NotificationConnector::Slack | NotificationConnector::Teams
            if !endpoint.secret_env.is_empty() =>
        {
            return Err(MambaError::Validation(format!(
                "{} connector stores its credential in the webhook URL environment variable",
                endpoint.connector.as_str()
            )));
        }
        _ => {}
    }
    Ok(())
}

fn validate_env_name(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(MambaError::Validation(format!(
            "{label} environment name must contain only letters, digits and _"
        )));
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
    let url = match resolve_url(endpoint) {
        Ok(url) => url,
        Err(error) => return failed_attempt(attempted_at, None, error.to_string()),
    };
    let secret = match endpoint.connector {
        NotificationConnector::Generic => match resolve_secret(&endpoint.secret_env) {
            Ok(secret) => Some(secret),
            Err(error) => return failed_attempt(attempted_at, None, error.to_string()),
        },
        NotificationConnector::Feishu if !endpoint.secret_env.is_empty() => {
            match resolve_secret(&endpoint.secret_env) {
                Ok(secret) => Some(secret),
                Err(error) => return failed_attempt(attempted_at, None, error.to_string()),
            }
        }
        _ => None,
    };
    deliver_configured(endpoint, delivery, &url, secret.as_deref(), attempted_at).await
}

pub async fn deliver_with_secret(
    endpoint: &NotificationEndpoint,
    delivery: &NotificationDelivery,
    secret: &[u8],
    attempted_at: DateTime<Utc>,
) -> NotificationAttempt {
    deliver_configured(
        endpoint,
        delivery,
        &endpoint.url,
        Some(secret),
        attempted_at,
    )
    .await
}

fn resolve_url(endpoint: &NotificationEndpoint) -> Result<String> {
    let value = if let Some(url_env) = &endpoint.url_env {
        std::env::var(url_env).map_err(|_| {
            MambaError::ExternalConnector(format!(
                "webhook URL environment variable {url_env} is missing"
            ))
        })?
    } else {
        endpoint.url.clone()
    };
    let url = Url::parse(&value)
        .map_err(|_| MambaError::ExternalConnector("resolved webhook URL is invalid".into()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(MambaError::ExternalConnector(
            "resolved webhook URL must use http or https".into(),
        ));
    }
    Ok(value)
}

fn resolve_secret(secret_env: &str) -> Result<Vec<u8>> {
    match std::env::var(secret_env) {
        Ok(secret) if !secret.is_empty() => Ok(secret.into_bytes()),
        _ => Err(MambaError::ExternalConnector(format!(
            "secret environment variable {secret_env} is missing"
        ))),
    }
}

async fn deliver_configured(
    endpoint: &NotificationEndpoint,
    delivery: &NotificationDelivery,
    url: &str,
    secret: Option<&[u8]>,
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
    let body = match render_body(endpoint.connector, &webhook, delivery, secret, attempted_at) {
        Ok(body) => body,
        Err(error) => {
            return NotificationAttempt {
                delivered: false,
                response_status: None,
                error: Some(format!("connector payload failed: {error}")),
                attempted_at,
            };
        }
    };
    let timestamp = attempted_at.timestamp().to_string();
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
    let mut request = client
        .post(url)
        .header("content-type", "application/json")
        .header("user-agent", "MambaFlow-Notification/1.0")
        .header("webhook-id", &delivery.id)
        .header("webhook-timestamp", &timestamp);
    if endpoint.connector == NotificationConnector::Generic {
        let Some(secret) = secret else {
            return failed_attempt(
                attempted_at,
                None,
                "generic connector secret is missing".into(),
            );
        };
        request = request.header(
            "webhook-signature",
            format!("v1,{}", sign(secret, &delivery.id, &timestamp, &body)),
        );
    }
    match request.body(body).send().await {
        Ok(response) => validate_response(endpoint.connector, response, attempted_at).await,
        Err(error) => failed_attempt(attempted_at, None, format!("delivery failed: {error}")),
    }
}

fn render_body(
    connector: NotificationConnector,
    webhook: &NotificationWebhook,
    delivery: &NotificationDelivery,
    secret: Option<&[u8]>,
    attempted_at: DateTime<Utc>,
) -> std::result::Result<Vec<u8>, serde_json::Error> {
    let value = match connector {
        NotificationConnector::Generic => serde_json::to_value(webhook)?,
        NotificationConnector::Feishu => {
            render_feishu(&notification_card(webhook, delivery), secret, attempted_at)
        }
        NotificationConnector::Slack => render_slack(&notification_card(webhook, delivery)),
        NotificationConnector::Teams => render_teams(&notification_card(webhook, delivery)),
    };
    serde_json::to_vec(&value)
}

async fn validate_response(
    connector: NotificationConnector,
    response: reqwest::Response,
    attempted_at: DateTime<Utc>,
) -> NotificationAttempt {
    let status = response.status();
    let response_status = Some(status.as_u16());
    let body = response.bytes().await.unwrap_or_default();
    if !status.is_success() {
        let detail = String::from_utf8_lossy(&body);
        return failed_attempt(
            attempted_at,
            response_status,
            format!(
                "endpoint returned HTTP {}{}",
                status.as_u16(),
                response_suffix(&detail)
            ),
        );
    }
    if connector == NotificationConnector::Feishu
        && let Ok(value) = serde_json::from_slice::<Value>(&body)
        && let Some(code) = value
            .get("code")
            .or_else(|| value.get("StatusCode"))
            .and_then(Value::as_i64)
        && code != 0
    {
        let message = value
            .get("msg")
            .or_else(|| value.get("StatusMessage"))
            .and_then(Value::as_str)
            .unwrap_or("Feishu rejected the message");
        return failed_attempt(
            attempted_at,
            response_status,
            format!("Feishu returned code {code}: {message}"),
        );
    }
    NotificationAttempt {
        delivered: true,
        response_status,
        error: None,
        attempted_at,
    }
}

fn response_suffix(value: &str) -> String {
    let detail = value.trim().chars().take(200).collect::<String>();
    if detail.is_empty() {
        String::new()
    } else {
        format!(": {detail}")
    }
}

#[derive(Clone, Debug)]
struct NotificationCard {
    title: String,
    summary: String,
    facts: Vec<(String, String)>,
    severity: CardSeverity,
    footer: String,
    actions: Vec<CardAction>,
}

#[derive(Clone, Debug)]
struct CardAction {
    label: &'static str,
    action_id: &'static str,
    target_id: String,
}

#[derive(Clone, Copy, Debug)]
enum CardSeverity {
    Info,
    Success,
    Warning,
    Critical,
}

fn notification_card(
    webhook: &NotificationWebhook,
    delivery: &NotificationDelivery,
) -> NotificationCard {
    let data = webhook.data.get("data").unwrap_or(&webhook.data);
    let (title, summary, severity) = match webhook.event_type.as_str() {
        "work_request.sent" => (
            "新任务已传球",
            "任务已经派往对应队员与个人 Agent，请确认接球。".into(),
            CardSeverity::Info,
        ),
        "flow_message.posted" => (
            "Flow 收到新传球",
            value_text(data, "body").unwrap_or_else(|| "协作线程有一条新消息。".into()),
            CardSeverity::Info,
        ),
        "task.blocked" => (
            "航线受阻，等待塔台支援",
            value_text(data, "reason").unwrap_or_else(|| "任务已标记为阻塞。".into()),
            CardSeverity::Warning,
        ),
        "task.submitted" => (
            "任务已交球，等待验收",
            "执行人已经提交结果，需要 Human 完成最终验收。".into(),
            CardSeverity::Info,
        ),
        "tracking.escalation_raised" => (
            "塔台升级：需要管理者处理",
            nested_text(data, &["escalation", "reason"])
                .or_else(|| value_text(data, "reason"))
                .unwrap_or_else(|| "Todo 风险已超过升级阈值。".into()),
            CardSeverity::Critical,
        ),
        "flow_change.proposed" => (
            "航线变更等待批准",
            "运行中的 Flow 收到变更提案，请评估影响后放行。".into(),
            CardSeverity::Warning,
        ),
        "flow_change.applied" => (
            "新航线已生效",
            "Human 已批准变更，任务与排期已经更新。".into(),
            CardSeverity::Info,
        ),
        "flow_change.rejected" => (
            "航线变更未获放行",
            value_text(data, "reason").unwrap_or_else(|| "Human 已驳回变更。".into()),
            CardSeverity::Warning,
        ),
        "remote_flight.crashed" => (
            "航班坠机，黑匣子已记录",
            value_text(data, "reason")
                .unwrap_or_else(|| "远程执行失败，请检查证据并选择恢复策略。".into()),
            CardSeverity::Critical,
        ),
        "flow.completed" => (
            "Mamba Out",
            "所有任务已经通过验收，Flow 安全落地。".into(),
            CardSeverity::Success,
        ),
        "connector.test" => (
            "塔台信号测试",
            "Connector 已收到 MambaFlow 测试传球。".into(),
            CardSeverity::Success,
        ),
        event_type => (
            "MambaFlow 状态更新",
            format!("组织事件 {event_type} 已写入 Flow Ledger。"),
            CardSeverity::Info,
        ),
    };
    let mut facts = Vec::new();
    if let Some(flow_id) = &delivery.flow_id {
        facts.push(("Flow".into(), flow_id.clone()));
    }
    for (label, keys) in [
        ("任务", &["task_id", "task", "task_key"][..]),
        ("接球人", &["target_id", "owner_id", "principal_id"][..]),
    ] {
        if let Some(value) = keys.iter().find_map(|key| value_text(data, key)) {
            facts.push((label.into(), value));
        }
    }
    facts.push(("传球人".into(), webhook.actor.clone()));
    let actions = match webhook.event_type.as_str() {
        "work_request.sent" => value_text(data, "task_id")
            .map(|target_id| CardAction {
                label: "接球",
                action_id: "mambaflow.task.accept",
                target_id,
            })
            .into_iter()
            .collect(),
        "flow_message.posted" if nested_bool(data, &["message", "requires_ack"]) == Some(true) => {
            nested_text(data, &["message", "id"])
                .map(|target_id| CardAction {
                    label: "确认收到",
                    action_id: "mambaflow.message.ack",
                    target_id,
                })
                .into_iter()
                .collect()
        }
        "tracking.escalation_raised" => nested_text(data, &["escalation", "id"])
            .map(|target_id| CardAction {
                label: "接手处理",
                action_id: "mambaflow.escalation.ack",
                target_id,
            })
            .into_iter()
            .collect(),
        _ => Vec::new(),
    };
    NotificationCard {
        title: limit_text(title, 120),
        summary: limit_text(&summary, 1_500),
        facts,
        severity,
        footer: format!(
            "{} · {} · {}",
            delivery.id,
            delivery.source_event_kind,
            delivery.queued_at.format("%Y-%m-%d %H:%M UTC")
        ),
        actions,
    }
}

fn value_text(value: &Value, key: &str) -> Option<String> {
    scalar_text(value.get(key)?)
}

fn nested_text(value: &Value, keys: &[&str]) -> Option<String> {
    let value = keys
        .iter()
        .try_fold(value, |current, key| current.get(key))?;
    scalar_text(value)
}

fn nested_bool(value: &Value, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .try_fold(value, |current, key| current.get(key))?
        .as_bool()
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn limit_text(value: &str, limit: usize) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
        .take(limit)
        .collect()
}

fn render_slack(card: &NotificationCard) -> Value {
    let fields = card
        .facts
        .iter()
        .map(|(label, value)| {
            json!({
                "type": "mrkdwn",
                "text": format!("*{}*\n{}", slack_escape(label), slack_escape(value))
            })
        })
        .collect::<Vec<_>>();
    let mut blocks = vec![
        json!({
            "type": "header",
            "text": {"type": "plain_text", "text": card.title, "emoji": true}
        }),
        json!({
            "type": "section",
            "text": {"type": "mrkdwn", "text": slack_escape(&card.summary)}
        }),
        json!({"type": "section", "fields": fields}),
    ];
    if !card.actions.is_empty() {
        blocks.push(json!({
            "type": "actions",
            "elements": card.actions.iter().map(|action| json!({
                "type": "button",
                "text": {"type": "plain_text", "text": action.label, "emoji": true},
                "action_id": action.action_id,
                "value": action.target_id,
                "style": "primary"
            })).collect::<Vec<_>>()
        }));
    }
    blocks.push(json!({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": slack_escape(&card.footer)}]
    }));
    json!({
        "text": format!("{}: {}", card.title, card.summary),
        "blocks": blocks
    })
}

fn slack_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn render_teams(card: &NotificationCard) -> Value {
    let color = match card.severity {
        CardSeverity::Info => "Accent",
        CardSeverity::Success => "Good",
        CardSeverity::Warning => "Warning",
        CardSeverity::Critical => "Attention",
    };
    let facts = card
        .facts
        .iter()
        .map(|(title, value)| json!({"title": title, "value": value}))
        .collect::<Vec<_>>();
    json!({
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": null,
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": card.title,
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": color,
                        "wrap": true
                    },
                    {"type": "TextBlock", "text": card.summary, "wrap": true},
                    {"type": "FactSet", "facts": facts},
                    {
                        "type": "TextBlock",
                        "text": card.footer,
                        "isSubtle": true,
                        "size": "Small",
                        "spacing": "Small",
                        "wrap": true
                    }
                ]
            }
        }]
    })
}

fn render_feishu(
    card: &NotificationCard,
    secret: Option<&[u8]>,
    attempted_at: DateTime<Utc>,
) -> Value {
    let template = match card.severity {
        CardSeverity::Info => "blue",
        CardSeverity::Success => "green",
        CardSeverity::Warning => "orange",
        CardSeverity::Critical => "red",
    };
    let fields = card
        .facts
        .iter()
        .map(|(label, value)| {
            json!({
                "is_short": true,
                "text": {
                    "tag": "lark_md",
                    "content": format!("**{}**\n{}", label, value)
                }
            })
        })
        .collect::<Vec<_>>();
    let mut payload = json!({
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": true},
            "header": {
                "template": template,
                "title": {"tag": "plain_text", "content": card.title}
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": card.summary}
                },
                {"tag": "hr"},
                {"tag": "div", "fields": fields},
                {
                    "tag": "note",
                    "elements": [{"tag": "plain_text", "content": card.footer}]
                }
            ]
        }
    });
    if let Some(secret) = secret {
        let timestamp = attempted_at.timestamp().to_string();
        payload["timestamp"] = Value::String(timestamp.clone());
        payload["sign"] = Value::String(sign_feishu(secret, &timestamp));
    }
    payload
}

fn sign_feishu(secret: &[u8], timestamp: &str) -> String {
    let mut key = Vec::with_capacity(timestamp.len() + secret.len() + 1);
    key.extend_from_slice(timestamp.as_bytes());
    key.push(b'\n');
    key.extend_from_slice(secret);
    let mut mac = HmacSha256::new_from_slice(&key).expect("HMAC accepts keys of any size");
    mac.update(&[]);
    BASE64_STANDARD.encode(mac.finalize().into_bytes())
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

    #[test]
    fn legacy_endpoint_replays_as_generic_connector() {
        let endpoint: NotificationEndpoint = serde_json::from_value(json!({
            "id": "NEND-LEGACY",
            "name": "legacy",
            "url": "https://bridge.example.com/mamba",
            "event_kinds": ["task.blocked"],
            "secret_env": "MAMBA_SECRET",
            "active": true,
            "created_by": "admin",
            "created_at": "2026-07-17T00:00:00Z",
            "disabled_by": null,
            "disabled_at": null
        }))
        .unwrap();
        assert_eq!(endpoint.connector, NotificationConnector::Generic);
        assert!(endpoint.url_env.is_none());
        validate_endpoint(&endpoint).unwrap();
    }

    #[test]
    fn slack_work_request_card_contains_a_scoped_accept_action() {
        let now = Utc::now();
        let delivery = NotificationDelivery {
            id: "NTF-ACTION".into(),
            organization_id: "ORG-1".into(),
            endpoint_id: "NEND-1".into(),
            source_event_kind: "work_request.sent".into(),
            flow_id: Some("FLOW-1".into()),
            actor: "牢大".into(),
            payload: json!({
                "type": "work_request_sent",
                "data": {"flow_id": "FLOW-1", "task_id": "TSK-1", "target_id": "HUM-1"}
            }),
            status: NotificationStatus::Pending,
            attempts: 0,
            queued_at: now,
            last_attempt_at: None,
            delivered_at: None,
            response_status: None,
            last_error: None,
        };
        let webhook = NotificationWebhook {
            specversion: "1.0",
            id: delivery.id.clone(),
            source: "mambaflow://organizations/ORG-1".into(),
            event_type: delivery.source_event_kind.clone(),
            subject: Some("mambaflow://flows/FLOW-1".into()),
            time: now,
            actor: delivery.actor.clone(),
            data: delivery.payload.clone(),
        };
        let slack = render_slack(&notification_card(&webhook, &delivery));
        let action = slack["blocks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|block| block["type"] == "actions")
            .unwrap();
        assert_eq!(action["elements"][0]["action_id"], "mambaflow.task.accept");
        assert_eq!(action["elements"][0]["value"], "TSK-1");
    }

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
            connector: NotificationConnector::Generic,
            url_env: None,
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

    #[tokio::test]
    async fn provider_connectors_render_native_cards_and_validate_feishu_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
        let service = Router::new()
            .route(
                "/hook",
                post(move |body: Bytes| {
                    let tx = tx.clone();
                    async move {
                        tx.send(body).unwrap();
                        (StatusCode::OK, r#"{"code":0,"msg":"success"}"#)
                    }
                }),
            )
            .route(
                "/reject",
                post(|| async {
                    (
                        StatusCode::OK,
                        r#"{"code":19001,"msg":"signature rejected"}"#,
                    )
                }),
            );
        let server = tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
        let now = Utc::now();
        let delivery = NotificationDelivery {
            id: "NTF-CARD".into(),
            organization_id: "ORG-1".into(),
            endpoint_id: "NEND-CARD".into(),
            source_event_kind: "task.blocked".into(),
            flow_id: Some("FLOW-1".into()),
            actor: "佐巴扬".into(),
            payload: json!({
                "type": "task_blocked",
                "data": {"task_id": "TSK-1", "reason": "等待生产权限"}
            }),
            status: NotificationStatus::Pending,
            attempts: 0,
            queued_at: now,
            last_attempt_at: None,
            delivered_at: None,
            response_status: None,
            last_error: None,
        };

        for connector in [
            NotificationConnector::Feishu,
            NotificationConnector::Slack,
            NotificationConnector::Teams,
        ] {
            let endpoint = NotificationEndpoint {
                id: delivery.endpoint_id.clone(),
                name: connector.as_str().into(),
                connector,
                url_env: Some("TEST_WEBHOOK_URL".into()),
                url: String::new(),
                event_kinds: vec!["task.blocked".into()],
                secret_env: if connector == NotificationConnector::Feishu {
                    "TEST_FEISHU_SECRET".into()
                } else {
                    String::new()
                },
                active: true,
                created_by: "admin".into(),
                created_at: now,
                disabled_by: None,
                disabled_at: None,
            };
            let secret =
                (connector == NotificationConnector::Feishu).then_some(b"feishu-secret".as_slice());
            let attempt = deliver_configured(
                &endpoint,
                &delivery,
                &format!("http://{address}/hook"),
                secret,
                now,
            )
            .await;
            assert!(attempt.delivered, "{connector:?}: {:?}", attempt.error);
            let body: Value = serde_json::from_slice(&rx.recv().await.unwrap()).unwrap();
            match connector {
                NotificationConnector::Feishu => {
                    assert_eq!(body["msg_type"], "interactive");
                    assert_eq!(body["card"]["header"]["template"], "orange");
                    assert_eq!(body["timestamp"], now.timestamp().to_string());
                    assert_eq!(
                        body["sign"],
                        sign_feishu(b"feishu-secret", &now.timestamp().to_string())
                    );
                }
                NotificationConnector::Slack => {
                    assert_eq!(body["blocks"][0]["type"], "header");
                    assert!(body["text"].as_str().unwrap().contains("航线受阻"));
                }
                NotificationConnector::Teams => {
                    assert_eq!(body["type"], "message");
                    assert_eq!(body["attachments"][0]["content"]["type"], "AdaptiveCard");
                }
                NotificationConnector::Generic => unreachable!(),
            }
        }

        let endpoint = NotificationEndpoint {
            id: delivery.endpoint_id.clone(),
            name: "feishu".into(),
            connector: NotificationConnector::Feishu,
            url_env: Some("TEST_WEBHOOK_URL".into()),
            url: String::new(),
            event_kinds: vec!["task.blocked".into()],
            secret_env: String::new(),
            active: true,
            created_by: "admin".into(),
            created_at: now,
            disabled_by: None,
            disabled_at: None,
        };
        let rejected = deliver_configured(
            &endpoint,
            &delivery,
            &format!("http://{address}/reject"),
            None,
            now,
        )
        .await;
        assert!(!rejected.delivered);
        assert!(rejected.error.unwrap().contains("code 19001"));
        server.abort();
    }
}
