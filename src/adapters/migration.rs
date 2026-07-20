use std::path::{Path, PathBuf};

use serde::Serialize;

use super::postgres_store::PostgresEventStore;
use crate::error::{MambaError, Result};
use crate::state::OrganizationState;
use crate::store::EventStore;
use crate::tenant::TenantCatalog;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PostgresMigrationReport {
    pub tenants: usize,
    pub migrated_tenants: usize,
    pub replayed_tenants: usize,
    pub events: usize,
    pub credentials: usize,
}

pub fn sqlite_fleet_to_postgres(
    data_dir: impl AsRef<Path>,
    database_url: &str,
) -> Result<PostgresMigrationReport> {
    let data_dir = data_dir.as_ref();
    let root_store = EventStore::open(data_dir.join("flow.db"))?;
    let root_state = OrganizationState::replay(&root_store.load_all()?)?;
    let mut source_catalog = TenantCatalog::open(data_dir)?;
    source_catalog.adopt_default(root_state.tenant()?)?;
    let source_tenants = source_catalog.list()?;
    let mut target_catalog = TenantCatalog::postgres(data_dir, database_url)?;
    let mut report = PostgresMigrationReport {
        tenants: source_tenants.len(),
        migrated_tenants: 0,
        replayed_tenants: 0,
        events: 0,
        credentials: 0,
    };

    for record in source_tenants {
        let source_dir = source_catalog.data_dir(&record)?;
        let source_store = EventStore::open(source_dir.join("flow.db"))?;
        let events = source_store.load_all()?;
        let state = OrganizationState::replay(&events)?;
        let tenant = state.tenant()?;
        if tenant.id != record.id {
            return Err(MambaError::Validation(format!(
                "SQLite catalog tenant {} points to Ledger {}",
                record.id, tenant.id
            )));
        }
        let credentials = source_store.export_credentials()?;
        if let Some(existing) = target_catalog.find(&record.id)? {
            if existing.slug != record.slug
                || existing.name != record.name
                || existing.storage_path != record.storage_path
                || existing.is_default != record.is_default
            {
                return Err(MambaError::Validation(format!(
                    "PostgreSQL catalog has a different definition for tenant {}",
                    record.id
                )));
            }
        } else if record.is_default {
            target_catalog.adopt_default(tenant)?;
        } else {
            target_catalog.register(tenant, &record.slug, &record.storage_path)?;
        }

        let runtime_dir = postgres_runtime_dir(data_dir, &record.slug);
        let mut target = PostgresEventStore::connect(database_url, &record.id)?;
        if target.import_sqlite_snapshot(&events, &credentials)? {
            report.migrated_tenants += 1;
        } else {
            report.replayed_tenants += 1;
        }
        std::fs::create_dir_all(runtime_dir)?;
        report.events += events.len();
        report.credentials += credentials.len();
    }
    Ok(report)
}

fn postgres_runtime_dir(data_dir: &Path, slug: &str) -> PathBuf {
    if slug == "default" {
        data_dir.to_path_buf()
    } else {
        data_dir.join("tenants").join(slug)
    }
}
