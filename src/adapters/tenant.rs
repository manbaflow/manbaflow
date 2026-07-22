use std::fs;
use std::path::{Component, Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use super::postgres_store::PostgresDatabase;
use crate::domain::Tenant;
use crate::error::{MambaError, Result};

const CATALOG_SCHEMA_VERSION: i64 = 1;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct TenantRecord {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub storage_path: PathBuf,
    pub is_default: bool,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

pub struct TenantCatalog {
    backend: CatalogBackend,
}

enum CatalogBackend {
    Sqlite(SqliteTenantCatalog),
    Postgres(PostgresTenantCatalog),
}

impl TenantCatalog {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            backend: CatalogBackend::Sqlite(SqliteTenantCatalog::open(root)?),
        })
    }

    pub fn configured(root: impl AsRef<Path>) -> Result<Self> {
        if let Some(database_url) = database_url_from_env()? {
            Self::postgres(root, &database_url)
        } else {
            Self::open(root)
        }
    }

    pub fn postgres(root: impl AsRef<Path>, database_url: &str) -> Result<Self> {
        Ok(Self {
            backend: CatalogBackend::Postgres(PostgresTenantCatalog::connect(root, database_url)?),
        })
    }

    pub fn adopt_default(&mut self, tenant: &Tenant) -> Result<TenantRecord> {
        match &mut self.backend {
            CatalogBackend::Sqlite(catalog) => catalog.adopt_default(tenant),
            CatalogBackend::Postgres(catalog) => catalog.adopt_default(tenant),
        }
    }

    pub fn register(
        &mut self,
        tenant: &Tenant,
        slug: &str,
        storage_path: impl AsRef<Path>,
    ) -> Result<TenantRecord> {
        match &mut self.backend {
            CatalogBackend::Sqlite(catalog) => catalog.register(tenant, slug, storage_path),
            CatalogBackend::Postgres(catalog) => catalog.register(tenant, slug, storage_path),
        }
    }

    pub fn list(&self) -> Result<Vec<TenantRecord>> {
        match &self.backend {
            CatalogBackend::Sqlite(catalog) => catalog.list(),
            CatalogBackend::Postgres(catalog) => catalog.list(),
        }
    }

    pub fn active(&self) -> Result<Vec<TenantRecord>> {
        Ok(self
            .list()?
            .into_iter()
            .filter(|record| record.active)
            .collect())
    }

    pub fn find(&self, id_or_slug: &str) -> Result<Option<TenantRecord>> {
        match &self.backend {
            CatalogBackend::Sqlite(catalog) => catalog.find(id_or_slug),
            CatalogBackend::Postgres(catalog) => catalog.find(id_or_slug),
        }
    }

    pub fn default_tenant(&self) -> Result<Option<TenantRecord>> {
        match &self.backend {
            CatalogBackend::Sqlite(catalog) => catalog.default_tenant(),
            CatalogBackend::Postgres(catalog) => catalog.default_tenant(),
        }
    }

    pub fn data_dir(&self, record: &TenantRecord) -> Result<PathBuf> {
        match &self.backend {
            CatalogBackend::Sqlite(catalog) => catalog.data_dir(record),
            CatalogBackend::Postgres(catalog) => catalog.data_dir(record),
        }
    }

    pub fn uses_shared_storage(&self) -> bool {
        matches!(self.backend, CatalogBackend::Postgres(_))
    }
}

struct SqliteTenantCatalog {
    root: PathBuf,
    connection: Connection,
}

impl SqliteTenantCatalog {
    fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let path = root.join("control.db");
        let connection = Connection::open(&path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA synchronous = FULL;
             PRAGMA trusted_schema = OFF;
             CREATE TABLE IF NOT EXISTS metadata (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS tenants (
                 id           TEXT PRIMARY KEY,
                 slug         TEXT NOT NULL UNIQUE,
                 name         TEXT NOT NULL,
                 storage_path TEXT NOT NULL UNIQUE,
                 is_default   INTEGER NOT NULL DEFAULT 0,
                 active       INTEGER NOT NULL DEFAULT 1,
                 created_at   TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS idx_tenants_single_default
                 ON tenants(is_default) WHERE is_default = 1;
             INSERT INTO metadata(key, value) VALUES ('schema_version', '1')
                 ON CONFLICT(key) DO NOTHING;",
        )?;
        let schema_version = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if schema_version != CATALOG_SCHEMA_VERSION {
            return Err(MambaError::Validation(format!(
                "unsupported tenant catalog schema version {schema_version}; this binary requires {CATALOG_SCHEMA_VERSION}"
            )));
        }
        restrict_file_permissions(&path)?;
        restrict_file_permissions(&sidecar_path(&path, "-wal"))?;
        restrict_file_permissions(&sidecar_path(&path, "-shm"))?;
        Ok(Self { root, connection })
    }

    fn adopt_default(&mut self, tenant: &Tenant) -> Result<TenantRecord> {
        if let Some(record) = self.default_tenant()? {
            if record.id != tenant.id {
                return Err(MambaError::Validation(format!(
                    "default tenant catalog points to {} but the root Ledger belongs to {}",
                    record.id, tenant.id
                )));
            }
            return Ok(record);
        }
        self.insert(tenant, "default", Path::new("."), true)
    }

    fn register(
        &mut self,
        tenant: &Tenant,
        slug: &str,
        storage_path: impl AsRef<Path>,
    ) -> Result<TenantRecord> {
        self.insert(tenant, slug, storage_path.as_ref(), false)
    }

    fn list(&self) -> Result<Vec<TenantRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT id, slug, name, storage_path, is_default, active, created_at
             FROM tenants ORDER BY is_default DESC, slug",
        )?;
        let rows = statement.query_map([], decode_record)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }
        Ok(records)
    }

    fn find(&self, id_or_slug: &str) -> Result<Option<TenantRecord>> {
        self.connection
            .query_row(
                "SELECT id, slug, name, storage_path, is_default, active, created_at
                 FROM tenants WHERE id = ?1 OR slug = ?1",
                [id_or_slug],
                decode_record,
            )
            .optional()
            .map_err(Into::into)
    }

    fn default_tenant(&self) -> Result<Option<TenantRecord>> {
        self.connection
            .query_row(
                "SELECT id, slug, name, storage_path, is_default, active, created_at
                 FROM tenants WHERE is_default = 1",
                [],
                decode_record,
            )
            .optional()
            .map_err(Into::into)
    }

    fn data_dir(&self, record: &TenantRecord) -> Result<PathBuf> {
        validate_relative_storage_path(&record.storage_path)?;
        Ok(self.root.join(&record.storage_path))
    }

    fn insert(
        &mut self,
        tenant: &Tenant,
        slug: &str,
        storage_path: &Path,
        is_default: bool,
    ) -> Result<TenantRecord> {
        let slug = validate_slug(slug)?;
        validate_relative_storage_path(storage_path)?;
        let storage_path = storage_path.to_path_buf();
        let storage_text = storage_path.to_str().ok_or_else(|| {
            MambaError::Validation("tenant storage path must be valid UTF-8".into())
        })?;
        self.connection.execute(
            "INSERT INTO tenants(id, slug, name, storage_path, is_default, active, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            params![
                tenant.id,
                slug,
                tenant.name,
                storage_text,
                is_default,
                tenant.created_at.to_rfc3339()
            ],
        )?;
        Ok(TenantRecord {
            id: tenant.id.clone(),
            slug,
            name: tenant.name.clone(),
            storage_path,
            is_default,
            active: true,
            created_at: tenant.created_at,
        })
    }
}

struct PostgresTenantCatalog {
    root: PathBuf,
    database: PostgresDatabase,
}

impl PostgresTenantCatalog {
    fn connect(root: impl AsRef<Path>, database_url: &str) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let database = PostgresDatabase::connect(database_url, "mamba-pg-tenant-catalog")?;
        database.call(|client| {
            client.batch_execute(
                "CREATE TABLE IF NOT EXISTS mamba_tenants (
                     id           TEXT PRIMARY KEY,
                     slug         TEXT NOT NULL UNIQUE,
                     name         TEXT NOT NULL,
                     storage_path TEXT NOT NULL UNIQUE,
                     is_default   BOOLEAN NOT NULL DEFAULT FALSE,
                     active       BOOLEAN NOT NULL DEFAULT TRUE,
                     created_at   TEXT NOT NULL
                 );
                 CREATE UNIQUE INDEX IF NOT EXISTS idx_mamba_tenants_single_default
                     ON mamba_tenants(is_default) WHERE is_default = TRUE;",
            )?;
            Ok(())
        })?;
        Ok(Self { root, database })
    }

    fn adopt_default(&mut self, tenant: &Tenant) -> Result<TenantRecord> {
        if let Some(record) = self.default_tenant()? {
            if record.id != tenant.id {
                return Err(MambaError::Validation(format!(
                    "default PostgreSQL tenant is {} but bootstrap opened {}",
                    record.id, tenant.id
                )));
            }
            return Ok(record);
        }
        self.insert(tenant, "default", Path::new("."), true)
    }

    fn register(
        &mut self,
        tenant: &Tenant,
        slug: &str,
        storage_path: impl AsRef<Path>,
    ) -> Result<TenantRecord> {
        self.insert(tenant, slug, storage_path.as_ref(), false)
    }

    fn list(&self) -> Result<Vec<TenantRecord>> {
        self.database.call(|client| {
            client
                .query(
                    "SELECT id, slug, name, storage_path, is_default, active, created_at
                 FROM mamba_tenants ORDER BY is_default DESC, slug",
                    &[],
                )?
                .into_iter()
                .map(decode_postgres_record)
                .collect()
        })
    }

    fn find(&self, id_or_slug: &str) -> Result<Option<TenantRecord>> {
        let id_or_slug = id_or_slug.to_string();
        self.database.call(move |client| {
            client
                .query_opt(
                    "SELECT id, slug, name, storage_path, is_default, active, created_at
                 FROM mamba_tenants WHERE id = $1 OR slug = $1",
                    &[&id_or_slug],
                )?
                .map(decode_postgres_record)
                .transpose()
        })
    }

    fn default_tenant(&self) -> Result<Option<TenantRecord>> {
        self.database.call(|client| {
            client
                .query_opt(
                    "SELECT id, slug, name, storage_path, is_default, active, created_at
                 FROM mamba_tenants WHERE is_default = TRUE",
                    &[],
                )?
                .map(decode_postgres_record)
                .transpose()
        })
    }

    fn data_dir(&self, record: &TenantRecord) -> Result<PathBuf> {
        validate_relative_storage_path(&record.storage_path)?;
        Ok(self.root.join(&record.storage_path))
    }

    fn insert(
        &mut self,
        tenant: &Tenant,
        slug: &str,
        storage_path: &Path,
        is_default: bool,
    ) -> Result<TenantRecord> {
        let slug = validate_slug(slug)?;
        validate_relative_storage_path(storage_path)?;
        let storage_path = storage_path.to_path_buf();
        let storage_text = storage_path
            .to_str()
            .ok_or_else(|| {
                MambaError::Validation("tenant storage path must be valid UTF-8".into())
            })?
            .to_string();
        let tenant_id = tenant.id.clone();
        let tenant_name = tenant.name.clone();
        let created_at = tenant.created_at.to_rfc3339();
        let stored_slug = slug.clone();
        self.database.call(move |client| {
            client.execute(
                "INSERT INTO mamba_tenants(
                    id, slug, name, storage_path, is_default, active, created_at
                 ) VALUES ($1, $2, $3, $4, $5, TRUE, $6)",
                &[
                    &tenant_id,
                    &stored_slug,
                    &tenant_name,
                    &storage_text,
                    &is_default,
                    &created_at,
                ],
            )?;
            Ok(())
        })?;
        Ok(TenantRecord {
            id: tenant.id.clone(),
            slug,
            name: tenant.name.clone(),
            storage_path,
            is_default,
            active: true,
            created_at: tenant.created_at,
        })
    }
}

fn decode_postgres_record(row: postgres::Row) -> Result<TenantRecord> {
    let created_at = DateTime::parse_from_rfc3339(row.get::<_, String>(6).as_str())
        .map_err(|error| MambaError::Validation(error.to_string()))?
        .with_timezone(&Utc);
    Ok(TenantRecord {
        id: row.get(0),
        slug: row.get(1),
        name: row.get(2),
        storage_path: PathBuf::from(row.get::<_, String>(3)),
        is_default: row.get(4),
        active: row.get(5),
        created_at,
    })
}

pub fn database_url_from_env() -> Result<Option<String>> {
    let direct = unicode_environment("MAMBA_DATABASE_URL")?;
    let file = unicode_environment("MAMBA_DATABASE_URL_FILE")?.map(PathBuf::from);
    database_url_from_sources(direct, file)
}

fn database_url_from_sources(
    direct: Option<String>,
    file: Option<PathBuf>,
) -> Result<Option<String>> {
    if direct.is_some() && file.is_some() {
        return Err(MambaError::Validation(
            "configure only one of MAMBA_DATABASE_URL or MAMBA_DATABASE_URL_FILE".into(),
        ));
    }
    let (value, source) = match (direct, file) {
        (Some(value), None) => (value, "MAMBA_DATABASE_URL"),
        (None, Some(path)) => {
            if path.as_os_str().is_empty() || !path.is_file() {
                return Err(MambaError::Validation(
                    "MAMBA_DATABASE_URL_FILE must point to a readable regular file".into(),
                ));
            }
            (
                fs::read_to_string(&path).map_err(|_| {
                    MambaError::Validation(
                        "MAMBA_DATABASE_URL_FILE could not be read as UTF-8".into(),
                    )
                })?,
                "MAMBA_DATABASE_URL_FILE",
            )
        }
        (None, None) => return Ok(None),
        (Some(_), Some(_)) => unreachable!("conflicting database sources were rejected"),
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(MambaError::Validation(format!("{source} cannot be empty")));
    }
    Ok(Some(value.to_string()))
}

fn unicode_environment(name: &str) -> Result<Option<String>> {
    std::env::var_os(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| MambaError::Validation(format!("{name} must be valid UTF-8")))
        })
        .transpose()
}

fn decode_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<TenantRecord> {
    let created_at = row.get::<_, String>(6)?;
    let created_at = DateTime::parse_from_rfc3339(&created_at)
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?
        .with_timezone(&Utc);
    Ok(TenantRecord {
        id: row.get(0)?,
        slug: row.get(1)?,
        name: row.get(2)?,
        storage_path: PathBuf::from(row.get::<_, String>(3)?),
        is_default: row.get(4)?,
        active: row.get(5)?,
        created_at,
    })
}

pub fn validate_slug(value: &str) -> Result<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.len() < 2
        || value.len() > 48
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || value.starts_with('-')
        || value.ends_with('-')
        || value.contains("--")
    {
        return Err(MambaError::Validation(
            "tenant slug must contain 2 to 48 lowercase letters, digits or single hyphens".into(),
        ));
    }
    Ok(value)
}

fn validate_relative_storage_path(path: &Path) -> Result<()> {
    if path.as_os_str() == "." {
        return Ok(());
    }
    if path.is_absolute()
        || path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(MambaError::Validation(
            "tenant storage path must stay inside the MambaFlow data directory".into(),
        ));
    }
    Ok(())
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", path.display()))
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

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn tenant(id: &str, name: &str) -> Tenant {
        Tenant {
            id: id.into(),
            name: name.into(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn catalog_keeps_default_and_sharded_tenants_separate() {
        let directory = tempdir().unwrap();
        let mut catalog = TenantCatalog::open(directory.path()).unwrap();
        let default = catalog
            .adopt_default(&tenant("TEN-default", "Mamba"))
            .unwrap();
        let second = catalog
            .register(
                &tenant("TEN-second", "Second"),
                "second-team",
                "tenants/second-team",
            )
            .unwrap();

        assert!(default.is_default);
        assert!(!second.is_default);
        assert_eq!(catalog.active().unwrap().len(), 2);
        assert_eq!(
            catalog.find("second-team").unwrap().unwrap().id,
            "TEN-second"
        );
        assert_eq!(
            catalog.data_dir(&second).unwrap(),
            directory.path().join("tenants/second-team")
        );
    }

    #[test]
    fn catalog_rejects_aliases_and_paths_that_escape_the_root() {
        let directory = tempdir().unwrap();
        let mut catalog = TenantCatalog::open(directory.path()).unwrap();
        assert!(
            catalog
                .register(&tenant("TEN-bad", "Bad"), "Bad Name", "tenants/bad")
                .is_err()
        );
        assert!(
            catalog
                .register(&tenant("TEN-bad", "Bad"), "bad", "../bad")
                .is_err()
        );
    }

    #[test]
    fn database_url_file_is_trimmed_and_conflicts_are_rejected() {
        let directory = tempdir().unwrap();
        let secret = directory.path().join("database-url");
        fs::write(
            &secret,
            "  postgresql://mamba:secret@database.example/mamba?sslmode=require\n",
        )
        .unwrap();
        assert_eq!(
            database_url_from_sources(None, Some(secret.clone())).unwrap(),
            Some("postgresql://mamba:secret@database.example/mamba?sslmode=require".into())
        );
        assert!(
            database_url_from_sources(Some("postgresql://direct/db".into()), Some(secret)).is_err()
        );
    }

    #[test]
    fn database_url_file_must_be_nonempty_and_regular() {
        let directory = tempdir().unwrap();
        let empty = directory.path().join("empty");
        fs::write(&empty, " \n").unwrap();
        assert!(database_url_from_sources(None, Some(empty)).is_err());
        assert!(database_url_from_sources(None, Some(directory.path().into())).is_err());
    }
}
