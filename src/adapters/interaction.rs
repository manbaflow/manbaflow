use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::ExternalInteractionAction;
use crate::error::{MambaError, Result};

type HmacSha256 = Hmac<Sha256>;

pub const BRIDGE_SECRET_ENV: &str = "MAMBA_INTERACTION_WEBHOOK_SECRET";
pub const SLACK_SECRET_ENV: &str = "MAMBA_SLACK_SIGNING_SECRET";
const MAX_REQUEST_AGE_SECONDS: i64 = 300;

#[derive(Clone, Default)]
pub struct InteractionWebhookAuth {
    bridge_secret: Option<Vec<u8>>,
    slack_secret: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalInteractionInput {
    pub external_user_id: String,
    pub action: ExternalInteractionAction,
    pub target_id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SlackForm {
    payload: String,
}

#[derive(Debug, Deserialize)]
struct SlackPayload {
    #[serde(rename = "type")]
    payload_type: String,
    user: SlackUser,
    actions: Vec<SlackAction>,
}

#[derive(Debug, Deserialize)]
struct SlackUser {
    id: String,
}

#[derive(Debug, Deserialize)]
struct SlackAction {
    action_id: String,
    value: String,
}

impl InteractionWebhookAuth {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            bridge_secret: optional_secret(BRIDGE_SECRET_ENV)?,
            slack_secret: optional_secret(SLACK_SECRET_ENV)?,
        })
    }

    pub fn bridge_enabled(&self) -> bool {
        self.bridge_secret.is_some()
    }

    pub fn slack_enabled(&self) -> bool {
        self.slack_secret.is_some()
    }

    #[cfg(test)]
    pub(crate) fn for_test(bridge_secret: Option<&[u8]>, slack_secret: Option<&[u8]>) -> Self {
        Self {
            bridge_secret: bridge_secret.map(<[u8]>::to_vec),
            slack_secret: slack_secret.map(<[u8]>::to_vec),
        }
    }

    pub fn verify_bridge(
        &self,
        provider: &str,
        delivery_id: &str,
        timestamp: &str,
        signature: &str,
        body: &[u8],
        now: DateTime<Utc>,
    ) -> Result<()> {
        let secret = self.bridge_secret.as_deref().ok_or_else(|| {
            MambaError::PermissionDenied("interaction Bridge is not enabled".into())
        })?;
        verify_fresh_timestamp(timestamp, now)?;
        let signature = signature.strip_prefix("v1,").ok_or_else(|| {
            MambaError::PermissionDenied("invalid interaction Bridge signature".into())
        })?;
        let signature = BASE64_STANDARD.decode(signature).map_err(|_| {
            MambaError::PermissionDenied("invalid interaction Bridge signature".into())
        })?;
        let message = bridge_message(provider, delivery_id, timestamp, body);
        let mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts keys of any size");
        mac.chain_update(&message)
            .verify_slice(&signature)
            .map_err(|_| {
                MambaError::PermissionDenied("invalid interaction Bridge signature".into())
            })
    }

    pub fn verify_slack(
        &self,
        timestamp: &str,
        signature: &str,
        body: &[u8],
        now: DateTime<Utc>,
    ) -> Result<()> {
        let secret = self.slack_secret.as_deref().ok_or_else(|| {
            MambaError::PermissionDenied("Slack interactions are not enabled".into())
        })?;
        verify_fresh_timestamp(timestamp, now)?;
        let signature = signature.strip_prefix("v0=").ok_or_else(|| {
            MambaError::PermissionDenied("invalid Slack request signature".into())
        })?;
        let signature = decode_hex(signature).ok_or_else(|| {
            MambaError::PermissionDenied("invalid Slack request signature".into())
        })?;
        let mut message = Vec::with_capacity(timestamp.len() + body.len() + 4);
        message.extend_from_slice(b"v0:");
        message.extend_from_slice(timestamp.as_bytes());
        message.push(b':');
        message.extend_from_slice(body);
        let mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts keys of any size");
        mac.chain_update(&message)
            .verify_slice(&signature)
            .map_err(|_| MambaError::PermissionDenied("invalid Slack request signature".into()))
    }
}

pub fn parse_slack_interaction(body: &[u8]) -> Result<ExternalInteractionInput> {
    let form: SlackForm = serde_urlencoded::from_bytes(body)
        .map_err(|_| MambaError::Validation("invalid Slack interaction form".into()))?;
    let payload: SlackPayload = serde_json::from_str(&form.payload)
        .map_err(|_| MambaError::Validation("invalid Slack interaction payload".into()))?;
    if payload.payload_type != "block_actions" || payload.actions.len() != 1 {
        return Err(MambaError::Validation(
            "Slack interaction must contain exactly one block action".into(),
        ));
    }
    let action = &payload.actions[0];
    let action = match action.action_id.as_str() {
        "mambaflow.task.accept" => ExternalInteractionAction::TaskAccept,
        "mambaflow.message.ack" => ExternalInteractionAction::MessageAck,
        "mambaflow.escalation.ack" => ExternalInteractionAction::EscalationAck,
        _ => {
            return Err(MambaError::Validation(format!(
                "unsupported Slack action {}",
                action.action_id
            )));
        }
    };
    Ok(ExternalInteractionInput {
        external_user_id: payload.user.id,
        action,
        target_id: payload.actions[0].value.clone(),
        reason: None,
    })
}

pub fn slack_delivery_id(timestamp: &str, body: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(timestamp.as_bytes());
    digest.update(b":");
    digest.update(body);
    format!("slack-{}", hex(&digest.finalize()))
}

fn optional_secret(name: &str) -> Result<Option<Vec<u8>>> {
    match std::env::var(name) {
        Ok(value) if value.is_empty() => Err(MambaError::Validation(format!(
            "{name} cannot be empty when configured"
        ))),
        Ok(value) => Ok(Some(value.into_bytes())),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(MambaError::Validation(format!(
            "could not read {name}: {error}"
        ))),
    }
}

fn verify_fresh_timestamp(value: &str, now: DateTime<Utc>) -> Result<()> {
    let timestamp = value
        .parse::<i64>()
        .map_err(|_| MambaError::PermissionDenied("invalid interaction timestamp".into()))?;
    let age = (now.timestamp() - timestamp).abs();
    if age > Duration::seconds(MAX_REQUEST_AGE_SECONDS).num_seconds() {
        return Err(MambaError::PermissionDenied(
            "interaction timestamp is outside the five minute replay window".into(),
        ));
    }
    Ok(())
}

fn bridge_message(provider: &str, delivery_id: &str, timestamp: &str, body: &[u8]) -> Vec<u8> {
    let mut message =
        Vec::with_capacity(provider.len() + delivery_id.len() + timestamp.len() + body.len() + 3);
    message.extend_from_slice(provider.as_bytes());
    message.push(b'.');
    message.extend_from_slice(delivery_id.as_bytes());
    message.push(b'.');
    message.extend_from_slice(timestamp.as_bytes());
    message.push(b'.');
    message.extend_from_slice(body);
    message
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|chunk| {
            let high = (chunk[0] as char).to_digit(16)?;
            let low = (chunk[1] as char).to_digit(16)?;
            Some(((high << 4) | low) as u8)
        })
        .collect()
}

fn hex(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_slacks_published_signature_vector() {
        let secret = b"8f742231b10e8888abcd99yyyzzz85a5".to_vec();
        let body = b"token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c";
        let auth = InteractionWebhookAuth {
            bridge_secret: None,
            slack_secret: Some(secret),
        };
        auth.verify_slack(
            "1531420618",
            "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503",
            body,
            DateTime::from_timestamp(1_531_420_618, 0).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn parses_supported_slack_block_action() {
        let payload = serde_json::json!({
            "type": "block_actions",
            "user": {"id": "U123"},
            "actions": [{"action_id": "mambaflow.task.accept", "value": "TSK-1"}]
        });
        let body = serde_urlencoded::to_string([("payload", payload.to_string())]).unwrap();
        let input = parse_slack_interaction(body.as_bytes()).unwrap();
        assert_eq!(input.external_user_id, "U123");
        assert_eq!(input.action, ExternalInteractionAction::TaskAccept);
        assert_eq!(input.target_id, "TSK-1");
    }
}
