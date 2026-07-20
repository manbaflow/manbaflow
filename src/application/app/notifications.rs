use chrono::{Duration, Utc};

use super::MambaApp;
use super::authority::Permission;
use crate::domain::{
    NotificationConnector, NotificationDelivery, NotificationEndpoint, NotificationStatus,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;
use crate::notification::{NotificationAttempt, NotificationDispatchSummary};

impl MambaApp {
    pub fn register_notification_endpoint(
        &mut self,
        name: &str,
        url: &str,
        event_kinds: &[String],
        secret_env: &str,
        actor: &str,
    ) -> Result<NotificationEndpoint> {
        self.ensure_permission(actor, Permission::NotificationManage)?;
        let mut event_kinds = event_kinds
            .iter()
            .map(|kind| kind.trim().to_ascii_lowercase())
            .filter(|kind| !kind.is_empty())
            .collect::<Vec<_>>();
        event_kinds.sort();
        event_kinds.dedup();
        if self
            .state
            .notification_endpoints
            .values()
            .any(|endpoint| endpoint.name.eq_ignore_ascii_case(name.trim()) && endpoint.active)
        {
            return Err(MambaError::Validation(format!(
                "active notification endpoint already exists: {}",
                name.trim()
            )));
        }
        let endpoint = NotificationEndpoint {
            id: new_id("NEND"),
            name: name.trim().to_string(),
            connector: NotificationConnector::Generic,
            url_env: None,
            url: url.trim().to_string(),
            event_kinds,
            secret_env: secret_env.trim().to_string(),
            active: true,
            created_by: actor.to_string(),
            created_at: Utc::now(),
            disabled_by: None,
            disabled_at: None,
        };
        crate::notification::validate_endpoint(&endpoint)?;
        self.commit(
            actor,
            vec![DomainEvent::NotificationEndpointRegistered {
                endpoint: endpoint.clone(),
            }],
        )?;
        Ok(endpoint)
    }

    pub fn register_notification_connector(
        &mut self,
        name: &str,
        connector: NotificationConnector,
        url_env: &str,
        event_kinds: &[String],
        secret_env: Option<&str>,
        actor: &str,
    ) -> Result<NotificationEndpoint> {
        self.ensure_permission(actor, Permission::NotificationManage)?;
        if connector == NotificationConnector::Generic {
            return Err(MambaError::Validation(
                "use register_notification_endpoint for a generic signed webhook".into(),
            ));
        }
        let mut event_kinds = event_kinds
            .iter()
            .map(|kind| kind.trim().to_ascii_lowercase())
            .filter(|kind| !kind.is_empty())
            .collect::<Vec<_>>();
        event_kinds.sort();
        event_kinds.dedup();
        if self
            .state
            .notification_endpoints
            .values()
            .any(|endpoint| endpoint.name.eq_ignore_ascii_case(name.trim()) && endpoint.active)
        {
            return Err(MambaError::Validation(format!(
                "active notification endpoint already exists: {}",
                name.trim()
            )));
        }
        let endpoint = NotificationEndpoint {
            id: new_id("NEND"),
            name: name.trim().to_string(),
            connector,
            url_env: Some(url_env.trim().to_string()),
            url: String::new(),
            event_kinds,
            secret_env: secret_env.unwrap_or_default().trim().to_string(),
            active: true,
            created_by: actor.to_string(),
            created_at: Utc::now(),
            disabled_by: None,
            disabled_at: None,
        };
        crate::notification::validate_endpoint(&endpoint)?;
        self.commit(
            actor,
            vec![DomainEvent::NotificationEndpointRegistered {
                endpoint: endpoint.clone(),
            }],
        )?;
        Ok(endpoint)
    }

    pub fn disable_notification_endpoint(
        &mut self,
        endpoint_id: &str,
        actor: &str,
    ) -> Result<NotificationEndpoint> {
        self.ensure_permission(actor, Permission::NotificationManage)?;
        let endpoint = self
            .state
            .notification_endpoints
            .get(endpoint_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "notification endpoint",
                id: endpoint_id.to_string(),
            })?;
        if !endpoint.active {
            return Err(MambaError::InvalidTransition(format!(
                "notification endpoint {endpoint_id} is already disabled"
            )));
        }
        self.commit(
            actor,
            vec![DomainEvent::NotificationEndpointDisabled {
                endpoint_id: endpoint_id.to_string(),
                disabled_by: actor.to_string(),
                disabled_at: Utc::now(),
            }],
        )?;
        Ok(self.state.notification_endpoints[endpoint_id].clone())
    }

    pub fn notification_attempts(
        &self,
        limit: usize,
        force_failed: bool,
    ) -> Vec<(NotificationEndpoint, NotificationDelivery)> {
        let now = Utc::now();
        let mut deliveries = self
            .state
            .notification_deliveries
            .values()
            .filter(|delivery| {
                matches!(
                    delivery.status,
                    NotificationStatus::Pending | NotificationStatus::Failed
                )
            })
            .filter(|delivery| {
                force_failed
                    || delivery.status == NotificationStatus::Pending
                    || delivery.last_attempt_at.is_none_or(|attempted_at| {
                        let exponent = delivery.attempts.min(8);
                        let delay = Duration::seconds(15 * (1_i64 << exponent));
                        attempted_at + delay <= now
                    })
            })
            .filter_map(|delivery| {
                let endpoint = self
                    .state
                    .notification_endpoints
                    .get(&delivery.endpoint_id)?;
                endpoint
                    .active
                    .then_some((endpoint.clone(), delivery.clone()))
            })
            .collect::<Vec<_>>();
        deliveries.sort_by(|left, right| {
            left.1
                .queued_at
                .cmp(&right.1.queued_at)
                .then_with(|| left.1.id.cmp(&right.1.id))
        });
        deliveries.truncate(limit);
        deliveries
    }

    pub fn record_notification_attempt(
        &mut self,
        delivery_id: &str,
        attempt: NotificationAttempt,
        actor: &str,
    ) -> Result<NotificationDelivery> {
        let delivery = self
            .state
            .notification_deliveries
            .get(delivery_id)
            .ok_or_else(|| MambaError::NotFound {
                entity: "notification delivery",
                id: delivery_id.to_string(),
            })?;
        if matches!(
            delivery.status,
            NotificationStatus::Delivered | NotificationStatus::Cancelled
        ) {
            return Err(MambaError::InvalidTransition(format!(
                "notification delivery {delivery_id} is already delivered"
            )));
        }
        let event = if attempt.delivered {
            DomainEvent::NotificationDelivered {
                delivery_id: delivery_id.to_string(),
                flow_id: delivery.flow_id.clone(),
                response_status: attempt.response_status.unwrap_or(200),
                delivered_at: attempt.attempted_at,
            }
        } else {
            DomainEvent::NotificationFailed {
                delivery_id: delivery_id.to_string(),
                flow_id: delivery.flow_id.clone(),
                response_status: attempt.response_status,
                error: attempt
                    .error
                    .unwrap_or_else(|| "notification delivery failed".into()),
                attempted_at: attempt.attempted_at,
            }
        };
        self.commit(actor, vec![event])?;
        Ok(self.state.notification_deliveries[delivery_id].clone())
    }

    pub async fn dispatch_notifications(
        &mut self,
        limit: usize,
        force_failed: bool,
        actor: &str,
    ) -> Result<NotificationDispatchSummary> {
        self.ensure_permission(actor, Permission::NotificationManage)?;
        if limit == 0 || limit > 1_000 {
            return Err(MambaError::Validation(
                "notification dispatch limit must be between 1 and 1000".into(),
            ));
        }
        let attempts = self.notification_attempts(limit, force_failed);
        let mut summary = NotificationDispatchSummary::default();
        for (endpoint, delivery) in attempts {
            let attempt = crate::notification::deliver(&endpoint, &delivery).await;
            summary.attempted += 1;
            if attempt.delivered {
                summary.delivered += 1;
            } else {
                summary.failed += 1;
            }
            self.record_notification_attempt(&delivery.id, attempt, actor)?;
        }
        Ok(summary)
    }

    pub async fn test_notification_endpoint(
        &mut self,
        endpoint_id: &str,
        actor: &str,
    ) -> Result<NotificationDelivery> {
        self.ensure_permission(actor, Permission::NotificationManage)?;
        let endpoint = self
            .state
            .notification_endpoints
            .get(endpoint_id)
            .filter(|endpoint| endpoint.active)
            .cloned()
            .ok_or_else(|| MambaError::NotFound {
                entity: "active notification endpoint",
                id: endpoint_id.to_string(),
            })?;
        let delivery = NotificationDelivery {
            id: new_id("NTF"),
            organization_id: self.state.organization()?.id.clone(),
            endpoint_id: endpoint.id.clone(),
            source_event_kind: "connector.test".into(),
            flow_id: None,
            actor: actor.to_string(),
            payload: serde_json::json!({
                "type": "connector_test",
                "data": {"endpoint_id": endpoint.id, "connector": endpoint.connector.as_str()}
            }),
            status: NotificationStatus::Pending,
            attempts: 0,
            queued_at: Utc::now(),
            last_attempt_at: None,
            delivered_at: None,
            response_status: None,
            last_error: None,
        };
        self.commit(
            actor,
            vec![DomainEvent::NotificationQueued {
                delivery: Box::new(delivery.clone()),
            }],
        )?;
        let attempt = crate::notification::deliver(&endpoint, &delivery).await;
        self.record_notification_attempt(&delivery.id, attempt, actor)
    }
}
