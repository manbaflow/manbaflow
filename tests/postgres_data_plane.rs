use manbaflow::domain::PrincipalKind;
use manbaflow::ids::new_id;
use manbaflow::migration::sqlite_fleet_to_postgres;
use manbaflow::tenant::TenantCatalog;
use manbaflow::{MambaApp, MambaError};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn postgres_replicas_share_tenant_events_and_credentials() {
    let Some(database_url) = std::env::var("MAMBA_TEST_DATABASE_URL").ok() else {
        eprintln!("skipping PostgreSQL integration test; MAMBA_TEST_DATABASE_URL is not set");
        return;
    };
    let directory = tempdir().unwrap();
    let tenant_id = new_id("TEN");
    let mut first = MambaApp::open_postgres(
        directory.path().join("first-runtime"),
        &database_url,
        &tenant_id,
    )
    .unwrap();
    first.init_organization("Shared Mamba", "admin").unwrap();
    let mut stale = MambaApp::open_postgres(
        directory.path().join("second-runtime"),
        &database_url,
        &tenant_id,
    )
    .unwrap();

    first
        .create_team("Platform", "rust,infra", "admin")
        .unwrap();
    let error = stale
        .create_team("People", "operations", "admin")
        .unwrap_err();
    assert!(matches!(error, MambaError::ConcurrentModification { .. }));
    stale.create_team("People", "operations", "admin").unwrap();

    let admin = stale
        .register_principal(
            "Admin",
            PrincipalKind::Human,
            Some("Platform"),
            None,
            "rust",
            100,
            None,
            "admin",
        )
        .unwrap();
    let credential = stale
        .issue_api_credential(&admin.id, "replica test", "admin")
        .unwrap();

    first.refresh_shared_state().unwrap();
    assert!(first.state().team("Platform").is_ok());
    assert!(first.state().team("People").is_ok());
    assert_eq!(
        first
            .authenticate_api_token(&credential.token)
            .unwrap()
            .unwrap()
            .id,
        admin.id
    );

    let slug = format!(
        "test-{}",
        tenant_id.trim_start_matches("TEN-").to_ascii_lowercase()
    );
    let mut catalog = TenantCatalog::postgres(directory.path(), &database_url).unwrap();
    catalog
        .register(
            first.state().tenant().unwrap(),
            &slug,
            format!("tenants/{slug}"),
        )
        .unwrap();
    assert_eq!(catalog.find(&slug).unwrap().unwrap().id, tenant_id);

    let source_dir = directory.path().join("sqlite-source");
    let mut source = MambaApp::open(&source_dir).unwrap();
    source.init_organization("Migrated Mamba", "admin").unwrap();
    let source_team = source
        .create_team("Migration", "operations", "admin")
        .unwrap();
    let source_admin = source
        .register_principal(
            "Migration Admin",
            PrincipalKind::Human,
            Some(&source_team.id),
            None,
            "operations",
            100,
            None,
            "admin",
        )
        .unwrap();
    let source_token = source
        .issue_api_credential(&source_admin.id, "migration", "admin")
        .unwrap()
        .token;
    let source_tenant_id = source.state().tenant().unwrap().id.clone();
    let migration = sqlite_fleet_to_postgres(&source_dir, &database_url).unwrap();
    assert_eq!(migration.migrated_tenants, 1);
    assert_eq!(
        sqlite_fleet_to_postgres(&source_dir, &database_url)
            .unwrap()
            .replayed_tenants,
        1
    );
    let migrated = MambaApp::open_postgres(
        directory.path().join("migrated-runtime"),
        &database_url,
        &source_tenant_id,
    )
    .unwrap();
    assert_eq!(
        migrated
            .authenticate_api_token(&source_token)
            .unwrap()
            .unwrap()
            .id,
        source_admin.id
    );
}
