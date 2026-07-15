use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};

use crate::error::{MambaError, Result};
use crate::event::{DomainEvent, EventEnvelope};
use crate::ids::new_id;

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
            CREATE TABLE IF NOT EXISTS metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO metadata(key, value) VALUES ('schema_version', '1');
            ",
        )?;
        Ok(Self { connection, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append_batch(
        &mut self,
        organization_id: &str,
        actor: &str,
        events: &[DomainEvent],
    ) -> Result<Vec<EventEnvelope>> {
        let transaction = self.connection.transaction()?;
        let mut envelopes = Vec::with_capacity(events.len());

        for event in events {
            let id = new_id("EVT");
            let occurred_at = Utc::now();
            let payload = serde_json::to_string(event)?;
            let flow_id = event.flow_id().map(str::to_string);
            transaction.execute(
                "INSERT INTO events(id, organization_id, flow_id, actor, kind, payload, occurred_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    id,
                    organization_id,
                    flow_id,
                    actor,
                    event.kind(),
                    payload,
                    occurred_at.to_rfc3339()
                ],
            )?;
            let sequence = transaction.last_insert_rowid();
            envelopes.push(EventEnvelope {
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

        transaction.commit()?;
        Ok(envelopes)
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
            let event = serde_json::from_str::<DomainEvent>(&payload)?;
            let occurred_at = DateTime::parse_from_rfc3339(&occurred_at)
                .map_err(|error| MambaError::Validation(error.to_string()))?
                .with_timezone(&Utc);
            events.push(EventEnvelope {
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
        assert_eq!(loaded[0].event, event);
    }
}
