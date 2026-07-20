use chrono::Utc;

use super::MambaApp;
use super::authority::Permission;
use crate::domain::{
    ExternalIdentityBinding, ExternalInteractionAction, ExternalInteractionReceipt,
    ExternalInteractionResult, PrincipalKind,
};
use crate::error::{MambaError, Result};
use crate::event::DomainEvent;
use crate::ids::new_id;

impl MambaApp {
    pub fn bind_external_identity(
        &mut self,
        provider: &str,
        external_user_id: &str,
        principal: &str,
        actor: &str,
    ) -> Result<ExternalIdentityBinding> {
        let provider = normalize_external_provider(provider)?;
        let external_user_id = validate_external_value(external_user_id, "external user ID", 200)?;
        let principal = self.state.principal(principal)?.clone();
        let actor_is_principal = self
            .state
            .principal(actor)
            .is_ok_and(|candidate| candidate.id == principal.id);
        if !actor_is_principal {
            self.ensure_permission(actor, Permission::PrincipalManage)?;
        }
        if principal.kind != PrincipalKind::Human || !principal.active {
            return Err(MambaError::PermissionDenied(
                "external identities can only bind to an active Human principal".into(),
            ));
        }
        if self.state.external_identities.values().any(|binding| {
            binding.is_active()
                && binding.provider == provider
                && (binding.external_user_id == external_user_id
                    || binding.principal_id == principal.id)
        }) {
            return Err(MambaError::Validation(format!(
                "active {provider} identity is already bound"
            )));
        }
        let binding = ExternalIdentityBinding {
            id: new_id("XID"),
            provider,
            external_user_id,
            principal_id: principal.id,
            bound_by: actor.to_string(),
            bound_at: Utc::now(),
            unbound_by: None,
            unbound_at: None,
        };
        self.commit(
            actor,
            vec![DomainEvent::ExternalIdentityBound {
                binding: binding.clone(),
            }],
        )?;
        Ok(binding)
    }

    pub fn unbind_external_identity(
        &mut self,
        binding_id: &str,
        actor: &str,
    ) -> Result<ExternalIdentityBinding> {
        let binding = self
            .state
            .external_identities
            .get(binding_id)
            .filter(|binding| binding.is_active())
            .ok_or_else(|| MambaError::NotFound {
                entity: "active external identity binding",
                id: binding_id.to_string(),
            })?
            .clone();
        let actor_is_principal = self
            .state
            .principal(actor)
            .is_ok_and(|candidate| candidate.id == binding.principal_id);
        if !actor_is_principal {
            self.ensure_permission(actor, Permission::PrincipalManage)?;
        }
        self.commit(
            actor,
            vec![DomainEvent::ExternalIdentityUnbound {
                binding_id: binding.id.clone(),
                unbound_by: actor.to_string(),
                unbound_at: Utc::now(),
            }],
        )?;
        Ok(self.state.external_identities[binding_id].clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_external_interaction(
        &mut self,
        provider: &str,
        delivery_id: &str,
        external_user_id: &str,
        action: ExternalInteractionAction,
        target_id: &str,
        reason: Option<&str>,
    ) -> Result<ExternalInteractionResult> {
        let provider = normalize_external_provider(provider)?;
        let delivery_id = validate_external_value(delivery_id, "delivery ID", 200)?;
        let external_user_id = validate_external_value(external_user_id, "external user ID", 200)?;
        let target_id = validate_external_value(target_id, "interaction target", 200)?;
        let reason = reason
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| validate_external_value(value, "interaction reason", 500))
            .transpose()?;
        let key = format!("{provider}:{delivery_id}");
        if let Some(receipt) = self.state.external_interactions.get(&key) {
            if receipt.external_user_id != external_user_id
                || receipt.action != action
                || receipt.target_id != target_id
                || receipt.reason != reason
            {
                return Err(MambaError::Validation(format!(
                    "external interaction delivery ID collision: {key}"
                )));
            }
            return Ok(ExternalInteractionResult {
                duplicate: true,
                receipt: receipt.clone(),
            });
        }
        let binding = self
            .state
            .external_identity(&provider, &external_user_id)?
            .clone();
        let principal = self.state.principal(&binding.principal_id)?.clone();
        if principal.kind != PrincipalKind::Human || !principal.active {
            return Err(MambaError::PermissionDenied(
                "external interaction requires an active Human binding".into(),
            ));
        }
        let now = Utc::now();
        let mut events = Vec::new();
        let flow_id = match action {
            ExternalInteractionAction::TaskAccept => {
                let (flow_id, _, event) =
                    self.prepare_task_accept(&target_id, &principal.name, now)?;
                events.push(event);
                Some(flow_id)
            }
            ExternalInteractionAction::TaskReject => {
                let reason = reason.as_deref().ok_or_else(|| {
                    MambaError::Validation("task.reject requires a reason".into())
                })?;
                let (flow_id, _, event) =
                    self.prepare_task_reject(&target_id, &principal.name, reason)?;
                events.push(event);
                Some(flow_id)
            }
            ExternalInteractionAction::MessageAck => {
                let (flow_id, event) = self.prepare_message_ack(&target_id, &principal, now)?;
                if let Some(event) = event {
                    events.push(event);
                }
                Some(flow_id)
            }
            ExternalInteractionAction::EscalationAck => {
                let (flow_id, event) = self.prepare_escalation_ack(&target_id, &principal, now)?;
                events.push(event);
                Some(flow_id)
            }
        };
        let receipt = ExternalInteractionReceipt {
            id: new_id("XACT"),
            provider,
            delivery_id,
            external_user_id,
            principal_id: principal.id,
            action,
            target_id,
            reason,
            flow_id,
            processed_at: now,
        };
        events.push(DomainEvent::ExternalInteractionProcessed {
            receipt: receipt.clone(),
        });
        self.commit(&principal.name, events)?;
        Ok(ExternalInteractionResult {
            duplicate: false,
            receipt,
        })
    }
}

fn normalize_external_provider(value: &str) -> Result<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty()
        || value.chars().count() > 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(MambaError::Validation(
            "external provider must contain only letters, digits, _ or -".into(),
        ));
    }
    Ok(value)
}

fn validate_external_value(value: &str, label: &str, max_chars: usize) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > max_chars || value.chars().any(char::is_control)
    {
        return Err(MambaError::Validation(format!(
            "{label} must contain 1 to {max_chars} printable characters"
        )));
    }
    Ok(value.to_string())
}
