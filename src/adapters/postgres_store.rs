use std::path::PathBuf;
use std::sync::mpsc;

use chrono::{DateTime, Utc};
use native_tls::TlsConnector;
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;
use serde::Serialize;

use crate::error::{MambaError, Result};
use crate::event::{CURRENT_EVENT_VERSION, EventEnvelope};
use crate::store::{CredentialSnapshot, StorageHealth, decode_event_payload};

const POSTGRES_SCHEMA_VERSION: i64 = 1;

type DatabaseOperation = Box<dyn FnOnce(&mut Client) + Send + 'static>;

pub(crate) struct PostgresDatabase {
    sender: mpsc::Sender<DatabaseOperation>,
}

impl PostgresDatabase {
    pub(crate) fn connect(database_url: &str, worker_name: &str) -> Result<Self> {
        if database_url.trim().is_empty() {
            return Err(MambaError::Validation(
                "PostgreSQL database URL cannot be empty".into(),
            ));
        }
        let database_url = database_url.to_string();
        let (sender, receiver) = mpsc::channel::<DatabaseOperation>();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(worker_name.to_string())
            .spawn(move || {
                let connection = (|| -> Result<Client> {
                    let tls = MakeTlsConnector::new(TlsConnector::new()?);
                    Ok(Client::connect(&database_url, tls)?)
                })();
                match connection {
                    Ok(mut client) => {
                        let _ = ready_sender.send(Ok(()));
                        while let Ok(operation) = receiver.recv() {
                            operation(&mut client);
                        }
                    }
                    Err(error) => {
                        let _ = ready_sender.send(Err(error));
                    }
                }
            })?;
        ready_receiver
            .recv()
            .map_err(|_| database_worker_stopped())??;
        Ok(Self { sender })
    }

    pub(crate) fn call<T>(
        &self,
        operation: impl FnOnce(&mut Client) -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        self.sender
            .send(Box::new(move |client| {
                let _ = result_sender.send(operation(client));
            }))
            .map_err(|_| database_worker_stopped())?;
        result_receiver
            .recv()
            .map_err(|_| database_worker_stopped())?
    }
}

fn database_worker_stopped() -> MambaError {
    MambaError::ExternalConnector("PostgreSQL connection worker stopped".into())
}

#[derive(Serialize)]
struct StoredEvent<'a> {
    version: u16,
    event: &'a crate::event::DomainEvent,
}

pub(crate) struct PostgresEventStore {
    database: PostgresDatabase,
    tenant_id: String,
}

impl PostgresEventStore {
    pub(crate) fn connect(database_url: &str, tenant_id: &str) -> Result<Self> {
        validate_tenant_id(tenant_id)?;
        let database =
            PostgresDatabase::connect(database_url, &format!("mamba-pg-events-{tenant_id}"))?;
        database.call(|client| {
            client.batch_execute(
                "CREATE TABLE IF NOT EXISTS mamba_metadata (
                     key TEXT PRIMARY KEY,
                     value TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS mamba_streams (
                     tenant_id TEXT PRIMARY KEY,
                     current_sequence BIGINT NOT NULL DEFAULT 0
                 );
                 CREATE TABLE IF NOT EXISTS mamba_events (
                     tenant_id       TEXT NOT NULL,
                     sequence        BIGINT NOT NULL,
                     id              TEXT NOT NULL,
                     organization_id TEXT NOT NULL,
                     flow_id         TEXT,
                     actor           TEXT NOT NULL,
                     kind            TEXT NOT NULL,
                     payload         TEXT NOT NULL,
                     occurred_at     TEXT NOT NULL,
                     PRIMARY KEY (tenant_id, sequence),
                     UNIQUE (tenant_id, id)
                 );
                 CREATE INDEX IF NOT EXISTS idx_mamba_events_flow
                     ON mamba_events(tenant_id, flow_id, sequence);
                 CREATE INDEX IF NOT EXISTS idx_mamba_events_kind
                     ON mamba_events(tenant_id, kind, sequence);
                 CREATE TABLE IF NOT EXISTS mamba_api_credentials (
                     tenant_id    TEXT NOT NULL,
                     id           TEXT NOT NULL,
                     principal_id TEXT NOT NULL,
                     token_hash   BYTEA NOT NULL UNIQUE,
                     created_at   TEXT NOT NULL,
                     expires_at   TEXT,
                     revoked_at   TEXT,
                     PRIMARY KEY (tenant_id, id)
                 );
                 CREATE INDEX IF NOT EXISTS idx_mamba_credentials_principal
                     ON mamba_api_credentials(tenant_id, principal_id, revoked_at);",
            )?;
            client.execute(
                "INSERT INTO mamba_metadata(key, value) VALUES ('schema_version', $1)
                 ON CONFLICT(key) DO NOTHING",
                &[&POSTGRES_SCHEMA_VERSION.to_string()],
            )?;
            let schema_version = client
                .query_one(
                    "SELECT CAST(value AS BIGINT) FROM mamba_metadata WHERE key = 'schema_version'",
                    &[],
                )?
                .get::<_, i64>(0);
            if schema_version != POSTGRES_SCHEMA_VERSION {
                return Err(MambaError::Validation(format!(
                    "unsupported PostgreSQL schema version {schema_version}; this binary requires {POSTGRES_SCHEMA_VERSION}"
                )));
            }
            Ok(())
        })?;
        let stream_tenant = tenant_id.to_string();
        database.call(move |client| {
            client.execute(
                "INSERT INTO mamba_streams(tenant_id, current_sequence) VALUES ($1, 0)
                 ON CONFLICT(tenant_id) DO NOTHING",
                &[&stream_tenant],
            )?;
            Ok(())
        })?;
        Ok(Self {
            database,
            tenant_id: tenant_id.to_string(),
        })
    }

    pub(crate) fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub(crate) fn import_sqlite_snapshot(
        &mut self,
        events: &[EventEnvelope],
        credentials: &[CredentialSnapshot],
    ) -> Result<bool> {
        let tenant_id = self.tenant_id.clone();
        let events = events.to_vec();
        let credentials = credentials.to_vec();
        self.database.call(move |client| {
            let mut transaction = client.transaction()?;
            let current_sequence = transaction
                .query_one(
                    "SELECT current_sequence FROM mamba_streams
                     WHERE tenant_id = $1 FOR UPDATE",
                    &[&tenant_id],
                )?
                .get::<_, i64>(0);
            let credential_count = transaction
                .query_one(
                    "SELECT COUNT(*) FROM mamba_api_credentials WHERE tenant_id = $1",
                    &[&tenant_id],
                )?
                .get::<_, i64>(0);
            if current_sequence != 0 || credential_count != 0 {
                let expected_sequence = events.last().map_or(0, |event| event.sequence);
                let actual_events = transaction
                    .query_one(
                        "SELECT COUNT(*) FROM mamba_events WHERE tenant_id = $1",
                        &[&tenant_id],
                    )?
                    .get::<_, i64>(0);
                let expected_events = i64::try_from(events.len()).map_err(|_| {
                    MambaError::Validation("SQLite event stream is too large to migrate".into())
                })?;
                let actual_credentials = transaction
                    .query_one(
                        "SELECT COUNT(*) FROM mamba_api_credentials WHERE tenant_id = $1",
                        &[&tenant_id],
                    )?
                    .get::<_, i64>(0);
                let expected_credentials = i64::try_from(credentials.len()).map_err(|_| {
                    MambaError::Validation("SQLite credential set is too large to migrate".into())
                })?;
                if current_sequence == expected_sequence
                    && actual_events == expected_events
                    && actual_credentials == expected_credentials
                {
                    transaction.commit()?;
                    return Ok(false);
                }
                return Err(MambaError::Validation(format!(
                    "PostgreSQL tenant {tenant_id} already contains a different event or credential snapshot"
                )));
            }

            for event in &events {
                let payload = serde_json::to_string(&StoredEvent {
                    version: CURRENT_EVENT_VERSION,
                    event: &event.event,
                })?;
                let occurred_at = event.occurred_at.to_rfc3339();
                transaction.execute(
                    "INSERT INTO mamba_events(
                        tenant_id, sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    &[
                        &tenant_id,
                        &event.sequence,
                        &event.id,
                        &event.organization_id,
                        &event.flow_id,
                        &event.actor,
                        &event.kind,
                        &payload,
                        &occurred_at,
                    ],
                )?;
            }
            for credential in &credentials {
                transaction.execute(
                    "INSERT INTO mamba_api_credentials(
                        tenant_id, id, principal_id, token_hash, created_at, expires_at, revoked_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &tenant_id,
                        &credential.id,
                        &credential.principal_id,
                        &credential.token_hash,
                        &credential.created_at,
                        &credential.expires_at,
                        &credential.revoked_at,
                    ],
                )?;
            }
            let final_sequence = events.last().map_or(0, |event| event.sequence);
            transaction.execute(
                "UPDATE mamba_streams SET current_sequence = $2 WHERE tenant_id = $1",
                &[&tenant_id, &final_sequence],
            )?;
            transaction.commit()?;
            Ok(true)
        })
    }

    #[cfg(test)]
    pub(crate) fn current_sequence(&self) -> Result<i64> {
        let tenant_id = self.tenant_id.clone();
        self.database.call(move |client| {
            Ok(client
                .query_one(
                    "SELECT current_sequence FROM mamba_streams WHERE tenant_id = $1",
                    &[&tenant_id],
                )?
                .get(0))
        })
    }

    pub(crate) fn health(&self) -> Result<StorageHealth> {
        let tenant_id = self.tenant_id.clone();
        self.database.call(move |client| {
            let schema_version = client
                .query_one(
                    "SELECT CAST(value AS BIGINT) FROM mamba_metadata WHERE key = 'schema_version'",
                    &[],
                )?
                .get(0);
            let event_count = client
                .query_one(
                    "SELECT COUNT(*) FROM mamba_events WHERE tenant_id = $1",
                    &[&tenant_id],
                )?
                .get(0);
            let now = Utc::now().to_rfc3339();
            let active_credentials = client
                .query_one(
                    "SELECT COUNT(*) FROM mamba_api_credentials
                     WHERE tenant_id = $1 AND revoked_at IS NULL
                       AND (expires_at IS NULL OR expires_at > $2)",
                    &[&tenant_id, &now],
                )?
                .get(0);
            client.query_one("SELECT 1", &[])?;
            Ok(StorageHealth {
                path: PathBuf::from("postgresql://shared"),
                backend: "postgresql".into(),
                schema_version,
                journal_mode: "server_managed".into(),
                integrity: "ok".into(),
                event_count,
                active_credentials,
            })
        })
    }

    pub(crate) fn append_prepared(
        &mut self,
        expected_sequence: i64,
        envelopes: &[EventEnvelope],
    ) -> Result<()> {
        if envelopes.is_empty() {
            return Ok(());
        }
        let tenant_id = self.tenant_id.clone();
        let envelopes = envelopes.to_vec();
        self.database.call(move |client| {
            let mut transaction = client.transaction()?;
            let actual_sequence = transaction
                .query_one(
                    "SELECT current_sequence FROM mamba_streams
                     WHERE tenant_id = $1 FOR UPDATE",
                    &[&tenant_id],
                )?
                .get::<_, i64>(0);
            if actual_sequence != expected_sequence {
                return Err(MambaError::ConcurrentModification {
                    expected: expected_sequence,
                    actual: actual_sequence,
                });
            }
            for (index, envelope) in envelopes.iter().enumerate() {
                let offset = i64::try_from(index)
                    .map_err(|_| MambaError::Validation("event batch is too large".into()))?;
                let required_sequence = expected_sequence
                    .checked_add(offset)
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| {
                        MambaError::Validation("event sequence exceeded the supported range".into())
                    })?;
                if envelope.sequence != required_sequence
                    || envelope.event_version != CURRENT_EVENT_VERSION
                    || envelope.kind != envelope.event.kind()
                {
                    return Err(MambaError::Validation(
                        "prepared event does not match its sequence, version or kind".into(),
                    ));
                }
                let payload = serde_json::to_string(&StoredEvent {
                    version: envelope.event_version,
                    event: &envelope.event,
                })?;
                let occurred_at = envelope.occurred_at.to_rfc3339();
                transaction.execute(
                    "INSERT INTO mamba_events(
                        tenant_id, sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    &[
                        &tenant_id,
                        &envelope.sequence,
                        &envelope.id,
                        &envelope.organization_id,
                        &envelope.flow_id,
                        &envelope.actor,
                        &envelope.kind,
                        &payload,
                        &occurred_at,
                    ],
                )?;
            }
            let final_sequence = envelopes.last().expect("non-empty checked above").sequence;
            transaction.execute(
                "UPDATE mamba_streams SET current_sequence = $2 WHERE tenant_id = $1",
                &[&tenant_id, &final_sequence],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    pub(crate) fn load_all(&self) -> Result<Vec<EventEnvelope>> {
        self.load(None)
    }

    pub(crate) fn load_flow(&self, flow_id: &str) -> Result<Vec<EventEnvelope>> {
        self.load(Some(flow_id))
    }

    pub(crate) fn load_after(&self, sequence: i64) -> Result<Vec<EventEnvelope>> {
        let tenant_id = self.tenant_id.clone();
        self.database.call(move |client| {
            client
                .query(
                    "SELECT sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at
                     FROM mamba_events
                     WHERE tenant_id = $1 AND sequence > $2 ORDER BY sequence",
                    &[&tenant_id, &sequence],
                )?
                .into_iter()
                .map(decode_row)
                .collect()
        })
    }

    fn load(&self, flow_id: Option<&str>) -> Result<Vec<EventEnvelope>> {
        let tenant_id = self.tenant_id.clone();
        let flow_id = flow_id.map(str::to_string);
        self.database.call(move |client| {
            let rows = if let Some(flow_id) = flow_id {
                client.query(
                    "SELECT sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at
                     FROM mamba_events WHERE tenant_id = $1 AND flow_id = $2 ORDER BY sequence",
                    &[&tenant_id, &flow_id],
                )?
            } else {
                client.query(
                    "SELECT sequence, id, organization_id, flow_id, actor, kind, payload, occurred_at
                     FROM mamba_events WHERE tenant_id = $1 ORDER BY sequence",
                    &[&tenant_id],
                )?
            };
            rows.into_iter().map(decode_row).collect()
        })
    }

    pub(crate) fn insert_credential(
        &mut self,
        id: &str,
        principal_id: &str,
        token_hash: &[u8],
        created_at: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let tenant_id = self.tenant_id.clone();
        let id = id.to_string();
        let principal_id = principal_id.to_string();
        let token_hash = token_hash.to_vec();
        let created_at = created_at.to_rfc3339();
        let expires_at = expires_at.map(|value| value.to_rfc3339());
        self.database.call(move |client| {
            client.execute(
                "INSERT INTO mamba_api_credentials(
                    tenant_id, id, principal_id, token_hash, created_at, expires_at
                 ) VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &tenant_id,
                    &id,
                    &principal_id,
                    &token_hash,
                    &created_at,
                    &expires_at,
                ],
            )?;
            Ok(())
        })
    }

    pub(crate) fn delete_credential(&mut self, id: &str) -> Result<()> {
        let tenant_id = self.tenant_id.clone();
        let id = id.to_string();
        self.database.call(move |client| {
            client.execute(
                "DELETE FROM mamba_api_credentials WHERE tenant_id = $1 AND id = $2",
                &[&tenant_id, &id],
            )?;
            Ok(())
        })
    }

    pub(crate) fn revoke_credential(&mut self, id: &str, revoked_at: DateTime<Utc>) -> Result<()> {
        let tenant_id = self.tenant_id.clone();
        let id = id.to_string();
        let revoked_at = revoked_at.to_rfc3339();
        self.database.call(move |client| {
            let updated = client.execute(
                "UPDATE mamba_api_credentials SET revoked_at = $3
                 WHERE tenant_id = $1 AND id = $2 AND revoked_at IS NULL",
                &[&tenant_id, &id, &revoked_at],
            )?;
            if updated == 0 {
                return Err(MambaError::NotFound {
                    entity: "active API credential",
                    id,
                });
            }
            Ok(())
        })
    }

    pub(crate) fn authenticate_credential(
        &self,
        token_hash: &[u8],
    ) -> Result<Option<(String, String)>> {
        let tenant_id = self.tenant_id.clone();
        let token_hash = token_hash.to_vec();
        self.database.call(move |client| {
            let now = Utc::now().to_rfc3339();
            let row = client.query_opt(
                "SELECT id, principal_id FROM mamba_api_credentials
                 WHERE tenant_id = $1 AND token_hash = $2 AND revoked_at IS NULL
                   AND (expires_at IS NULL OR expires_at > $3)",
                &[&tenant_id, &token_hash, &now],
            )?;
            Ok(row.map(|row| (row.get(0), row.get(1))))
        })
    }
}

fn decode_row(row: postgres::Row) -> Result<EventEnvelope> {
    let sequence = row.get(0);
    let kind: String = row.get(5);
    let payload: String = row.get(6);
    let (event_version, event) = decode_event_payload(&payload)?;
    if kind != event.kind() {
        return Err(MambaError::Validation(format!(
            "stored event kind `{kind}` does not match payload kind `{}` at sequence {sequence}",
            event.kind()
        )));
    }
    let occurred_at = DateTime::parse_from_rfc3339(row.get::<_, String>(7).as_str())
        .map_err(|error| MambaError::Validation(error.to_string()))?
        .with_timezone(&Utc);
    Ok(EventEnvelope {
        event_version,
        sequence,
        id: row.get(1),
        organization_id: row.get(2),
        flow_id: row.get(3),
        actor: row.get(4),
        kind,
        event,
        occurred_at,
    })
}

fn validate_tenant_id(tenant_id: &str) -> Result<()> {
    if !tenant_id.starts_with("TEN-")
        || tenant_id.len() < 5
        || !tenant_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(MambaError::Validation(
            "invalid PostgreSQL tenant ID".into(),
        ));
    }
    Ok(())
}
