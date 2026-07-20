use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, TransactionBehavior, params};
use serde::Serialize;
use serde_json::Value;

use crate::error::{MambaError, Result};
use crate::event::{CURRENT_EVENT_VERSION, DomainEvent, EventEnvelope};
use crate::ids::new_id;

#[derive(Serialize)]
struct StoredEvent<'a> {
    version: u16,
    event: &'a DomainEvent,
}

pub struct EventStore {
    connection: Connection,
    path: PathBuf,
}

impl EventStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS events (
                sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
                id              TEXT NOT NULL UNIQUE,
                organization_id TEXT NOT NULL,
                flow_id         TEXT,
                actor           TEXT NOT NULL,
                kind            TEXT NOT NULL,
                payload         TEXT NOT NULL,
                occurred_at     TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_flow ON events(flow_id, sequence);
            CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind, sequence);
            CREATE TABLE IF NOT EXISTS api_credentials (
                id           TEXT PRIMARY KEY,
                principal_id TEXT NOT NULL,
                token_hash   BLOB NOT NULL UNIQUE,
                created_at   TEXT NOT NULL,
                revoked_at   TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_api_credentials_principal
                ON api_credentials(principal_id, revoked_at);
            CREATE TABLE IF NOT EXISTS metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO metadata(key, value) VALUES ('schema_version', '2')
                ON CONFLICT(key) DO UPDATE SET value = excluded.value;
            ",
        )?;
        Ok(Self { connection, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(test)]
    fn append_batch(
        &mut self,
        organization_id: &str,
        actor: &str,
        events: &[DomainEvent],
    ) -> Result<Vec<EventEnvelope>> {
        let expected_sequence = self.current_sequence()?;
        let envelopes = Self::prepare_batch(expected_sequence, organization_id, actor, events)?;
        self.append_prepared(expected_sequence, &envelopes)?;
        Ok(envelopes)
    }

    #[cfg(test)]
    pub(crate) fn current_sequence(&self) -> Result<i64> {
        Ok(self.connection.query_row(
            "SELECT COALESCE(MAX(sequence), 0) FROM events",
            [],
            |row| row.get(0),
        )?)
    }

    pub(crate) fn prepare_batch(
        expected_sequence: i64,
        organization_id: &str,
        actor: &str,
        events: &[DomainEvent],
    ) -> Result<Vec<EventEnvelope>> {
        let mut envelopes = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            let offset = i64::try_from(index)
                .map_err(|_| MambaError::Validation("event batch is too large".into()))?;
            let sequence = expected_sequence
                .checked_add(offset)
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| {
                    MambaError::Validation("event sequence exceeded the supported range".into())
                })?;
            let id = new_id("EVT");
            let occurred_at = Utc::now();
            let flow_id = event.flow_id().map(str::to_string);
            envelopes.push(EventEnvelope {
                event_version: CURRENT_EVENT_VERSION,
                sequence,
                id,
                organization_id: organization_id.to_string(),
                flow_id,
                actor: actor.to_string(),
                kind: event.kind().to_string(),
                event: event.clone(),
                occurred_at,
            });
        }

        Ok(envelopes)
    }

    pub(crate) fn append_prepared(
        &mut self,
        expected_sequence: i64,
        envelopes: &[EventEnvelope],
    ) -> Result<()> {
        if envelopes.is_empty() {
            return Ok(());
        }

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual_sequence =
            transaction.query_row("SELECT COALESCE(MAX(sequence), 0) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })?;
        if actual_sequence != expected_sequence {
            return Err(MambaError::ConcurrentModification {
                expected: expected_sequence,
                actual: actual_sequence,
            });
        }

        for (index, envelope) in envelopes.iter().enumerate() {
            let offset = i64::try_from(index)
                .map_err(|_| MambaError::Validation("event batch is too large".into()))?;
            let expected_envelope_sequence = expected_sequence
                .checked_add(offset)
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| {
                    MambaError::Validation("event sequence exceeded the supported range".into())
                })?;
            if envelope.sequence != expected_envelope_sequence {
                return Err(MambaError::Validation(format!(
                    "prepared event sequence {} is not the expected sequence {}",
                    envelope.sequence, expected_envelope_sequence
                )));
            }
            if envelope.event_version != CURRENT_EVENT_VERSION {
                return Err(MambaError::Validation(format!(
                    "cannot append event payload version {}",
                    envelope.event_version
                )));
            }
            if envelope.kind != envelope.event.kind() {
                return Err(MambaError::Validation(format!(
                    "event kind `{}` does not match payload kind `{}`",
                    envelope.kind,
                    envelope.event.kind()
                )));
            }
            let payload = serde_json::to_string(&StoredEvent {
                version: envelope.event_version,
                event: &envelope.event,
            })?;
            transaction.execute(
                "INSERT INTO events(
                    sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    envelope.sequence,
                    envelope.id,
                    envelope.organization_id,
                    envelope.flow_id,
                    envelope.actor,
                    envelope.kind,
                    payload,
                    envelope.occurred_at.to_rfc3339()
                ],
            )?;
        }

        transaction.commit()?;
        Ok(())
    }

    pub fn load_all(&self) -> Result<Vec<EventEnvelope>> {
        self.load_where("SELECT sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at FROM events ORDER BY sequence", [])
    }

    pub fn load_flow(&self, flow_id: &str) -> Result<Vec<EventEnvelope>> {
        self.load_where(
            "SELECT sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at FROM events WHERE flow_id = ?1 ORDER BY sequence",
            [flow_id],
        )
    }

    pub fn insert_credential(
        &mut self,
        id: &str,
        principal_id: &str,
        token_hash: &[u8],
        created_at: DateTime<Utc>,
    ) -> Result<()> {
        self.connection.execute(
            "INSERT INTO api_credentials(id, principal_id, token_hash, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![id, principal_id, token_hash, created_at.to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn delete_credential(&mut self, id: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM api_credentials WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn revoke_credential(&mut self, id: &str, revoked_at: DateTime<Utc>) -> Result<()> {
        let updated = self.connection.execute(
            "UPDATE api_credentials SET revoked_at = ?2
             WHERE id = ?1 AND revoked_at IS NULL",
            params![id, revoked_at.to_rfc3339()],
        )?;
        if updated == 0 {
            return Err(MambaError::NotFound {
                entity: "active API credential",
                id: id.to_string(),
            });
        }
        Ok(())
    }

    pub fn authenticate_credential(&self, token_hash: &[u8]) -> Result<Option<(String, String)>> {
        let mut statement = self.connection.prepare(
            "SELECT id, principal_id FROM api_credentials
             WHERE token_hash = ?1 AND revoked_at IS NULL",
        )?;
        let mut rows = statement.query(params![token_hash])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?)))
        } else {
            Ok(None)
        }
    }

    fn load_where<const N: usize>(
        &self,
        sql: &str,
        values: [&str; N],
    ) -> Result<Vec<EventEnvelope>> {
        let mut statement = self.connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(values), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;

        let mut events = Vec::new();
        for row in rows {
            let (sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at) = row?;
            let (event_version, event) = decode_event_payload(&payload)?;
            if kind != event.kind() {
                return Err(MambaError::Validation(format!(
                    "stored event kind `{kind}` does not match payload kind `{}` at sequence {sequence}",
                    event.kind()
                )));
            }
            let occurred_at = DateTime::parse_from_rfc3339(&occurred_at)
                .map_err(|error| MambaError::Validation(error.to_string()))?
                .with_timezone(&Utc);
            events.push(EventEnvelope {
                event_version,
                sequence,
                id,
                organization_id,
                flow_id,
                actor,
                kind,
                event,
                occurred_at,
            });
        }
        Ok(events)
    }
}

fn decode_event_payload(payload: &str) -> Result<(u16, DomainEvent)> {
    let value = serde_json::from_str::<Value>(payload)?;
    let Some(object) = value.as_object() else {
        return Err(MambaError::Validation(
            "stored event payload must be a JSON object".into(),
        ));
    };

    if object.contains_key("version") && object.contains_key("event") {
        let version_value = object
            .get("version")
            .cloned()
            .ok_or_else(|| MambaError::Validation("stored event version is missing".into()))?;
        let version = serde_json::from_value::<u16>(version_value)?;
        if version != CURRENT_EVENT_VERSION {
            return Err(MambaError::Validation(format!(
                "unsupported event payload version {version}"
            )));
        }
        let event_value = object
            .get("event")
            .cloned()
            .ok_or_else(|| MambaError::Validation("stored event body is missing".into()))?;
        return Ok((version, serde_json::from_value(event_value)?));
    }

    // Databases written before payload versioning stored the DomainEvent directly.
    Ok((0, serde_json::from_value(value)?))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::domain::Organization;

    #[test]
    fn events_round_trip_in_sequence() {
        let directory = tempdir().unwrap();
        let mut store = EventStore::open(directory.path().join("mamba.db")).unwrap();
        let event = DomainEvent::OrganizationInitialized {
            organization: Organization {
                id: "ORG-1".to_string(),
                name: "Mamba".to_string(),
                created_at: Utc::now(),
            },
        };
        let appended = store
            .append_batch("ORG-1", "test", std::slice::from_ref(&event))
            .unwrap();
        let loaded = store.load_all().unwrap();

        assert_eq!(appended.len(), 1);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].sequence, 1);
        assert_eq!(loaded[0].event_version, CURRENT_EVENT_VERSION);
        assert_eq!(loaded[0].event, event);

        let payload = store
            .connection
            .query_row("SELECT payload FROM events WHERE sequence = 1", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap();
        let payload = serde_json::from_str::<Value>(&payload).unwrap();
        assert_eq!(payload["version"], CURRENT_EVENT_VERSION);
        assert!(payload.get("event").is_some());
    }

    #[test]
    fn legacy_unversioned_events_still_replay() {
        let directory = tempdir().unwrap();
        let store = EventStore::open(directory.path().join("mamba.db")).unwrap();
        let event = DomainEvent::OrganizationInitialized {
            organization: Organization {
                id: "ORG-LEGACY".to_string(),
                name: "Legacy Mamba".to_string(),
                created_at: Utc::now(),
            },
        };
        store
            .connection
            .execute(
                "INSERT INTO events(
                    sequence, id, organization_id, actor, kind, payload, occurred_at
                 ) VALUES (1, 'EVT-LEGACY', 'ORG-LEGACY', 'test', ?1, ?2, ?3)",
                params![
                    event.kind(),
                    serde_json::to_string(&event).unwrap(),
                    Utc::now().to_rfc3339()
                ],
            )
            .unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].event_version, 0);
        assert_eq!(loaded[0].event, event);
    }

    #[test]
    fn prepared_append_rejects_a_stale_writer() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("mamba.db");
        let mut first = EventStore::open(&path).unwrap();
        let mut stale = EventStore::open(&path).unwrap();
        let event = DomainEvent::OrganizationInitialized {
            organization: Organization {
                id: "ORG-1".to_string(),
                name: "Mamba".to_string(),
                created_at: Utc::now(),
            },
        };
        first
            .append_batch("ORG-1", "first", std::slice::from_ref(&event))
            .unwrap();
        let prepared = EventStore::prepare_batch(0, "ORG-1", "stale", &[event]).unwrap();

        let error = stale.append_prepared(0, &prepared).unwrap_err();
        assert!(matches!(
            error,
            MambaError::ConcurrentModification {
                expected: 0,
                actual: 1
            }
        ));
    }
}
