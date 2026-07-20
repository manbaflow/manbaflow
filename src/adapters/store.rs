use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, TransactionBehavior, params};
use serde::Serialize;
use serde_json::Value;

use super::postgres_store::PostgresEventStore;
use crate::error::{MambaError, Result};
use crate::event::{CURRENT_EVENT_VERSION, DomainEvent, EventEnvelope};
use crate::ids::new_id;

const SCHEMA_VERSION: i64 = 4;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct StorageHealth {
    pub path: PathBuf,
    pub backend: String,
    pub schema_version: i64,
    pub journal_mode: String,
    pub integrity: String,
    pub event_count: i64,
    pub active_credentials: i64,
}

pub(crate) enum FlowStore {
    Sqlite(EventStore),
    Postgres(PostgresEventStore),
}

impl FlowStore {
    pub fn sqlite(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::Sqlite(EventStore::open(path)?))
    }

    pub fn postgres(database_url: &str, tenant_id: &str) -> Result<Self> {
        Ok(Self::Postgres(PostgresEventStore::connect(
            database_url,
            tenant_id,
        )?))
    }

    pub fn health(&self) -> Result<StorageHealth> {
        match self {
            Self::Sqlite(store) => store.health(),
            Self::Postgres(store) => store.health(),
        }
    }

    pub fn backup(&mut self, destination: impl AsRef<Path>) -> Result<PathBuf> {
        match self {
            Self::Sqlite(store) => store.backup(destination),
            Self::Postgres(_) => Err(MambaError::Validation(
                "PostgreSQL storage must be backed up with database snapshots or PITR".into(),
            )),
        }
    }

    pub(crate) fn append_prepared(
        &mut self,
        expected_sequence: i64,
        envelopes: &[EventEnvelope],
    ) -> Result<()> {
        match self {
            Self::Sqlite(store) => store.append_prepared(expected_sequence, envelopes),
            Self::Postgres(store) => store.append_prepared(expected_sequence, envelopes),
        }
    }

    pub fn load_all(&self) -> Result<Vec<EventEnvelope>> {
        match self {
            Self::Sqlite(store) => store.load_all(),
            Self::Postgres(store) => store.load_all(),
        }
    }

    pub fn load_flow(&self, flow_id: &str) -> Result<Vec<EventEnvelope>> {
        match self {
            Self::Sqlite(store) => store.load_flow(flow_id),
            Self::Postgres(store) => store.load_flow(flow_id),
        }
    }

    pub fn load_after(&self, sequence: i64) -> Result<Vec<EventEnvelope>> {
        match self {
            Self::Sqlite(store) => store.load_after(sequence),
            Self::Postgres(store) => store.load_after(sequence),
        }
    }

    pub fn insert_credential(
        &mut self,
        id: &str,
        principal_id: &str,
        token_hash: &[u8],
        created_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        match self {
            Self::Sqlite(store) => {
                store.insert_credential(id, principal_id, token_hash, created_at, expires_at)
            }
            Self::Postgres(store) => {
                store.insert_credential(id, principal_id, token_hash, created_at, expires_at)
            }
        }
    }

    pub fn delete_credential(&mut self, id: &str) -> Result<()> {
        match self {
            Self::Sqlite(store) => store.delete_credential(id),
            Self::Postgres(store) => store.delete_credential(id),
        }
    }

    pub fn revoke_credential(&mut self, id: &str, revoked_at: DateTime<Utc>) -> Result<()> {
        match self {
            Self::Sqlite(store) => store.revoke_credential(id, revoked_at),
            Self::Postgres(store) => store.revoke_credential(id, revoked_at),
        }
    }

    pub fn authenticate_credential(&self, token_hash: &[u8]) -> Result<Option<(String, String)>> {
        match self {
            Self::Sqlite(store) => store.authenticate_credential(token_hash),
            Self::Postgres(store) => store.authenticate_credential(token_hash),
        }
    }

    pub(crate) fn put_artifact(&mut self, artifact: &ArtifactBlob) -> Result<()> {
        match self {
            Self::Sqlite(store) => store.put_artifact(artifact),
            Self::Postgres(store) => store.put_artifact(artifact),
        }
    }

    pub(crate) fn load_artifact(&self, sha256: &str) -> Result<Option<ArtifactBlob>> {
        match self {
            Self::Sqlite(store) => store.load_artifact(sha256),
            Self::Postgres(store) => store.load_artifact(sha256),
        }
    }

    pub fn tenant_id(&self) -> Option<&str> {
        match self {
            Self::Sqlite(_) => None,
            Self::Postgres(store) => Some(store.tenant_id()),
        }
    }

    pub fn is_shared(&self) -> bool {
        matches!(self, Self::Postgres(_))
    }

    #[cfg(test)]
    pub(crate) fn current_sequence(&self) -> Result<i64> {
        match self {
            Self::Sqlite(store) => store.current_sequence(),
            Self::Postgres(store) => store.current_sequence(),
        }
    }
}

#[derive(Serialize)]
struct StoredEvent<'a> {
    version: u16,
    event: &'a DomainEvent,
}

#[derive(Clone, Debug)]
pub(crate) struct CredentialSnapshot {
    pub id: String,
    pub principal_id: String,
    pub token_hash: Vec<u8>,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArtifactBlob {
    pub sha256: String,
    pub media_type: String,
    pub size_bytes: i64,
    pub content: Vec<u8>,
    pub created_at: String,
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
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let has_metadata = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'metadata'
             )",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if has_metadata {
            let existing_version = connection.query_row(
                "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            if !matches!(existing_version, 2 | 3 | SCHEMA_VERSION) {
                return Err(MambaError::Validation(format!(
                    "unsupported storage schema version {existing_version}; this binary requires {SCHEMA_VERSION}"
                )));
            }
        }
        connection.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            PRAGMA synchronous = FULL;
            PRAGMA wal_autocheckpoint = 1000;
            PRAGMA trusted_schema = OFF;
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
                expires_at   TEXT,
                revoked_at   TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_api_credentials_principal
                ON api_credentials(principal_id, revoked_at);
            CREATE TABLE IF NOT EXISTS artifacts (
                sha256     TEXT PRIMARY KEY,
                media_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                content    BLOB NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO metadata(key, value) VALUES ('schema_version', '4')
                ON CONFLICT(key) DO NOTHING;
            ",
        )?;
        let mut schema_version = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if schema_version == 2 {
            let transaction = connection.unchecked_transaction()?;
            transaction.execute_batch(
                "ALTER TABLE api_credentials ADD COLUMN expires_at TEXT;
                 UPDATE metadata SET value = '3' WHERE key = 'schema_version';",
            )?;
            transaction.commit()?;
            schema_version = 3;
        }
        if schema_version == 3 {
            connection.execute(
                "UPDATE metadata SET value = '4' WHERE key = 'schema_version'",
                [],
            )?;
            schema_version = 4;
        }
        if schema_version != SCHEMA_VERSION {
            return Err(MambaError::Validation(format!(
                "unsupported storage schema version {schema_version}; this binary requires {SCHEMA_VERSION}"
            )));
        }
        restrict_file_permissions(&path)?;
        restrict_file_permissions(&sidecar_path(&path, "-wal"))?;
        restrict_file_permissions(&sidecar_path(&path, "-shm"))?;
        Ok(Self { connection, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn health(&self) -> Result<StorageHealth> {
        let schema_version = self.connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )?;
        let journal_mode = self
            .connection
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?;
        let integrity = self
            .connection
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))?;
        let event_count = self
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?;
        let active_credentials = self.connection.query_row(
            "SELECT COUNT(*) FROM api_credentials
             WHERE revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ?1)",
            [Utc::now().to_rfc3339()],
            |row| row.get(0),
        )?;
        Ok(StorageHealth {
            path: self.path.clone(),
            backend: "sqlite".into(),
            schema_version,
            journal_mode,
            integrity,
            event_count,
            active_credentials,
        })
    }

    pub fn backup(&mut self, destination: impl AsRef<Path>) -> Result<PathBuf> {
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(MambaError::Validation(format!(
                "backup destination already exists: {}",
                destination.display()
            )));
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(FULL);")?;
        let destination_text = destination.to_str().ok_or_else(|| {
            MambaError::Validation("backup destination is not valid UTF-8".into())
        })?;
        self.connection
            .execute("VACUUM INTO ?1", [destination_text])?;
        restrict_file_permissions(&destination)?;
        let backup = EventStore::open(&destination)?;
        let health = backup.health()?;
        if health.integrity != "ok" {
            return Err(MambaError::Validation(format!(
                "backup integrity check failed: {}",
                health.integrity
            )));
        }
        Ok(destination)
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

    pub fn load_after(&self, sequence: i64) -> Result<Vec<EventEnvelope>> {
        self.load_where(
            "SELECT sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at
             FROM events WHERE sequence > ?1 ORDER BY sequence",
            [&sequence.to_string()],
        )
    }

    pub fn insert_credential(
        &mut self,
        id: &str,
        principal_id: &str,
        token_hash: &[u8],
        created_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        self.connection.execute(
            "INSERT INTO api_credentials(id, principal_id, token_hash, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id,
                principal_id,
                token_hash,
                created_at.to_rfc3339(),
                expires_at.map(|value| value.to_rfc3339())
            ],
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
             WHERE token_hash = ?1 AND revoked_at IS NULL
               AND (expires_at IS NULL OR expires_at > ?2)",
        )?;
        let mut rows = statement.query(params![token_hash, Utc::now().to_rfc3339()])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?)))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn put_artifact(&mut self, artifact: &ArtifactBlob) -> Result<()> {
        let inserted = self.connection.execute(
            "INSERT INTO artifacts(sha256, media_type, size_bytes, content, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(sha256) DO NOTHING",
            params![
                artifact.sha256,
                artifact.media_type,
                artifact.size_bytes,
                artifact.content,
                artifact.created_at
            ],
        )?;
        if inserted == 0 {
            let existing = self.load_artifact(&artifact.sha256)?.ok_or_else(|| {
                MambaError::Validation("artifact disappeared after a hash conflict".into())
            })?;
            if existing.content != artifact.content
                || existing.media_type != artifact.media_type
                || existing.size_bytes != artifact.size_bytes
            {
                return Err(MambaError::Validation(
                    "artifact hash already exists with different content or metadata".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn load_artifact(&self, sha256: &str) -> Result<Option<ArtifactBlob>> {
        let mut statement = self.connection.prepare(
            "SELECT sha256, media_type, size_bytes, content, created_at
             FROM artifacts WHERE sha256 = ?1",
        )?;
        let mut rows = statement.query([sha256])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        Ok(Some(ArtifactBlob {
            sha256: row.get(0)?,
            media_type: row.get(1)?,
            size_bytes: row.get(2)?,
            content: row.get(3)?,
            created_at: row.get(4)?,
        }))
    }

    pub(crate) fn export_credentials(&self) -> Result<Vec<CredentialSnapshot>> {
        let mut statement = self.connection.prepare(
            "SELECT id, principal_id, token_hash, created_at, expires_at, revoked_at
             FROM api_credentials ORDER BY created_at, id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(CredentialSnapshot {
                id: row.get(0)?,
                principal_id: row.get(1)?,
                token_hash: row.get(2)?,
                created_at: row.get(3)?,
                expires_at: row.get(4)?,
                revoked_at: row.get(5)?,
            })
        })?;
        let mut credentials = Vec::new();
        for row in rows {
            credentials.push(row?);
        }
        Ok(credentials)
    }

    pub(crate) fn export_artifacts(&self) -> Result<Vec<ArtifactBlob>> {
        let mut statement = self.connection.prepare(
            "SELECT sha256, media_type, size_bytes, content, created_at
             FROM artifacts ORDER BY created_at, sha256",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(ArtifactBlob {
                sha256: row.get(0)?,
                media_type: row.get(1)?,
                size_bytes: row.get(2)?,
                content: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
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

#[cfg(unix)]
fn restrict_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if path.exists() {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn restrict_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

pub(crate) fn decode_event_payload(payload: &str) -> Result<(u16, DomainEvent)> {
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
        if !matches!(version, 1 | CURRENT_EVENT_VERSION) {
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
    fn online_backup_is_consistent_and_future_schemas_are_rejected() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("source.sqlite");
        let backup_path = directory.path().join("backups").join("snapshot.sqlite");
        let mut store = EventStore::open(&path).unwrap();
        store
            .append_batch(
                "ORG-1",
                "admin",
                &[DomainEvent::OrganizationInitialized {
                    organization: Organization {
                        id: "ORG-1".into(),
                        name: "Mamba".into(),
                        created_at: Utc::now(),
                    },
                }],
            )
            .unwrap();
        let health = store.health().unwrap();
        assert_eq!(health.integrity, "ok");
        assert_eq!(health.journal_mode, "wal");
        assert_eq!(health.event_count, 1);
        let artifact = ArtifactBlob {
            sha256: "a".repeat(64),
            media_type: "text/plain".into(),
            size_bytes: 12,
            content: b"office draft".to_vec(),
            created_at: Utc::now().to_rfc3339(),
        };
        store.put_artifact(&artifact).unwrap();
        store.put_artifact(&artifact).unwrap();
        assert_eq!(
            store.load_artifact(&artifact.sha256).unwrap(),
            Some(artifact.clone())
        );
        store.backup(&backup_path).unwrap();
        let backup = EventStore::open(&backup_path).unwrap();
        assert_eq!(backup.load_all().unwrap().len(), 1);
        assert_eq!(
            backup.load_artifact(&artifact.sha256).unwrap(),
            Some(artifact)
        );
        assert!(store.backup(&backup_path).is_err());
        store
            .insert_credential(
                "CRED-expired",
                "HUM-1",
                &[7; 32],
                Utc::now() - chrono::Duration::days(2),
                Some(Utc::now() - chrono::Duration::days(1)),
            )
            .unwrap();
        assert!(store.authenticate_credential(&[7; 32]).unwrap().is_none());

        let v2_path = directory.path().join("v2.sqlite");
        let v2 = Connection::open(&v2_path).unwrap();
        v2.execute_batch(
            "CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO metadata(key, value) VALUES ('schema_version', '2');
             CREATE TABLE api_credentials(
                id TEXT PRIMARY KEY,
                principal_id TEXT NOT NULL,
                token_hash BLOB NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                revoked_at TEXT
             );",
        )
        .unwrap();
        drop(v2);
        let migrated = EventStore::open(v2_path).unwrap();
        assert_eq!(migrated.health().unwrap().schema_version, 4);

        let future_path = directory.path().join("future.sqlite");
        let future = Connection::open(&future_path).unwrap();
        future
            .execute_batch(
                "CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO metadata(key, value) VALUES ('schema_version', '99');",
            )
            .unwrap();
        drop(future);
        assert!(matches!(
            EventStore::open(future_path),
            Err(MambaError::Validation(message)) if message.contains("schema version 99")
        ));
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
    fn version_one_events_still_replay_after_authority_upgrade() {
        let directory = tempdir().unwrap();
        let store = EventStore::open(directory.path().join("mamba.db")).unwrap();
        let event = DomainEvent::OrganizationInitialized {
            organization: Organization {
                id: "ORG-V1".to_string(),
                name: "Version One".to_string(),
                created_at: Utc::now(),
            },
        };
        let payload = serde_json::json!({"version": 1, "event": event});
        store
            .connection
            .execute(
                "INSERT INTO events(
                    sequence, id, organization_id, actor, kind, payload, occurred_at
                 ) VALUES (1, 'EVT-V1', 'ORG-V1', 'test', ?1, ?2, ?3)",
                params![
                    event.kind(),
                    serde_json::to_string(&payload).unwrap(),
                    Utc::now().to_rfc3339()
                ],
            )
            .unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded[0].event_version, 1);
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
