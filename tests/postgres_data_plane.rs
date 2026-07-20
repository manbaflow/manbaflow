use chrono::Utc;
use manbaflow::domain::{
    CapabilityPack, ExecutorKind, FlightManifestDraft, OrganizationRole, PrincipalKind,
    RemoteFlightReport,
};
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

    let office_human = stale
        .register_principal(
            "Office Owner",
            PrincipalKind::Human,
            Some("Platform"),
            None,
            "product,documentation,delivery",
            100,
            None,
            &admin.id,
        )
        .unwrap();
    let agent = stale
        .register_principal(
            "Office Agent",
            PrincipalKind::Agent,
            Some("Platform"),
            Some(&office_human.id),
            "product,documentation,delivery",
            100,
            None,
            &admin.id,
        )
        .unwrap();
    stale
        .grant_role(&office_human.id, OrganizationRole::Manager, &admin.id)
        .unwrap();
    let flow = stale
        .create_demand(
            "Prepare a reviewable Office release",
            &office_human.name,
            manbaflow::planner::PlannerKind::Local,
            directory.path(),
            10,
        )
        .await
        .unwrap();
    stale.approve_flow(&flow.id, &office_human.name).unwrap();
    let task = flow.tasks[0].clone();
    stale.accept_task(&task.id, &office_human.name).unwrap();
    let lease = stale
        .authorize_remote_flight_with_manifest(
            &task.id,
            &office_human.name,
            &agent.name,
            ExecutorKind::Codex,
            3_600,
            FlightManifestDraft {
                capability_pack: Some(CapabilityPack::Office),
                ..Default::default()
            },
        )
        .unwrap();
    stale
        .claim_remote_flight(&lease.id, &agent.name, "WRUN-pg-office")
        .unwrap();
    let artifact = stale
        .stage_flight_artifact(
            &lease.id,
            "reports/weekly.txt",
            "text/plain",
            b"shared office artifact".to_vec(),
            &agent.name,
        )
        .unwrap();
    let now = Utc::now();
    stale
        .finish_remote_flight(
            &lease.id,
            &agent.name,
            true,
            RemoteFlightReport {
                run_id: "WRUN-pg-office".into(),
                executor: ExecutorKind::Codex,
                summary: "Office draft staged".into(),
                base_revision: "pg-test".into(),
                changed_files: vec!["reports/weekly.txt".into()],
                patch_sha256: Some("a".repeat(64)),
                log_sha256: "b".repeat(64),
                started_at: now,
                finished_at: now,
                fuel: Default::default(),
                failure_class: None,
                budget_exhaustions: Vec::new(),
                deliverables: Vec::new(),
                contract_violations: Vec::new(),
                sandbox: None,
            },
        )
        .unwrap();
    first.refresh_shared_state().unwrap();
    assert_eq!(
        first
            .artifact_content(&artifact.id, &office_human.id)
            .unwrap()
            .1,
        b"shared office artifact"
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
