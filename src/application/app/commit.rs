use super::MambaApp;
use crate::error::{MambaError, Result};
use crate::event::{DomainEvent, EventEnvelope};
use crate::store::EventStore;

impl MambaApp {
    pub(super) fn commit(
        &mut self,
        actor: &str,
        events: Vec<DomainEvent>,
    ) -> Result<Vec<EventEnvelope>> {
        let organization_id = self.state.organization()?.id.clone();
        self.commit_as(&organization_id, actor, events)
    }

    pub(super) fn commit_as(
        &mut self,
        organization_id: &str,
        actor: &str,
        mut events: Vec<DomainEvent>,
    ) -> Result<Vec<EventEnvelope>> {
        let queued =
            crate::notification::queue_events(&self.state, organization_id, actor, &events)?;
        events.extend(queued);

        let expected_sequence = self.state.last_sequence;
        let envelopes =
            EventStore::prepare_batch(expected_sequence, organization_id, actor, &events)?;
        let mut projected_state = self.state.clone();
        for envelope in &envelopes {
            projected_state.apply(envelope)?;
        }

        if let Err(error) = self.store.append_prepared(expected_sequence, &envelopes) {
            if matches!(error, MambaError::ConcurrentModification { .. }) {
                self.reload()?;
            }
            return Err(error);
        }
        self.state = projected_state;
        Ok(envelopes)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;
    use crate::domain::Organization;

    #[test]
    fn invalid_projection_never_reaches_the_event_store() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path()).unwrap();
        app.init_organization("Mamba", "admin").unwrap();
        let sequence_before = app.state.last_sequence;

        let error = app
            .commit(
                "admin",
                vec![DomainEvent::OrganizationInitialized {
                    organization: Organization {
                        id: "ORG-INVALID".into(),
                        name: "Duplicate".into(),
                        created_at: Utc::now(),
                    },
                }],
            )
            .unwrap_err();

        assert!(matches!(error, MambaError::OrganizationAlreadyInitialized));
        assert_eq!(app.state.last_sequence, sequence_before);
        assert_eq!(app.store.current_sequence().unwrap(), sequence_before);
    }

    #[test]
    fn stale_application_instance_cannot_skip_concurrent_events() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path();
        let mut first = MambaApp::open(data_dir).unwrap();
        first.init_organization("Mamba", "admin").unwrap();
        let mut stale = MambaApp::open(data_dir).unwrap();

        first
            .create_team("Platform", "rust,infra", "admin")
            .unwrap();
        let error = stale
            .create_team("People", "operations", "admin")
            .unwrap_err();

        assert!(matches!(
            error,
            MambaError::ConcurrentModification {
                expected: 1,
                actual: 2
            }
        ));
        assert_eq!(stale.state.last_sequence, 2);
        assert_eq!(stale.store.current_sequence().unwrap(), 2);
        assert!(stale.state.team("Platform").is_ok());
        assert!(stale.state.team("People").is_err());
    }
}
