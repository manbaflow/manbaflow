use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router, middleware};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::{MissedTickBehavior, interval};
use tower::ServiceExt;

use crate::RelayApp;
use crate::dashboard::DashboardSnapshot;
use crate::domain::{
    AssignmentTarget, AvailabilityBlock, Evidence, ExecutorConfig, ExecutorKind, FlightLease,
    FlightManifestDraft, Flow, FlowChangeRequest, FlowMessage, FlowMessageKind, GitLabWritePayload,
    GitLabWriteRequest, IssuedCredential, MessageInboxItem, NotificationConnector,
    NotificationDelivery, NotificationEndpoint, OfficeProvider, OfficeReleasePayload,
    OfficeReleaseRequest, Organization, OrganizationRole, Principal, PrincipalKind, RecoveryAction,
    RemoteFlightReport, Repository, RoleBinding, Task, Team, Tenant, TrackingEscalation,
    WorkCalendar, Workday,
};
use crate::error::{RelayError, Result};
use crate::feishu_auth::FeishuProvider;
use crate::gitlab::{GitLabClient, GitLabWebhookAuth, GitLabWebhookEvent, parse_webhook_event};
use crate::gitlab_write::GitLabWriteBridge;
use crate::identity::{OidcProvider, ScimAuthenticator};
use crate::interaction::{
    ExternalInteractionInput, InteractionWebhookAuth, parse_slack_interaction, slack_delivery_id,
};
use crate::notification::NotificationDispatchSummary;
use crate::office::OfficeBridge;
use crate::planner::PlannerKind;
use crate::tenant::{TenantCatalog, database_url_from_env};

#[derive(Clone, Debug)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub allow_insecure_public_http: bool,
    pub tracker_interval_seconds: u64,
    pub stale_after_hours: u64,
    pub escalate_after_hours: u64,
    pub notification_interval_seconds: u64,
}

#[derive(Clone)]
struct ApiState {
    app: Arc<Mutex<RelayApp>>,
    tenant_id: String,
    gitlab_webhook_auth: Option<GitLabWebhookAuth>,
    interaction_auth: InteractionWebhookAuth,
    oidc: Option<OidcProvider>,
    feishu: Option<FeishuProvider>,
    scim_auth: ScimAuthenticator,
    office_bridge: OfficeBridge,
    gitlab_write_bridge: GitLabWriteBridge,
}

#[derive(Clone)]
struct FleetState {
    routers: Arc<BTreeMap<String, Router>>,
    default_tenant_id: String,
}

#[derive(Clone, Default)]
struct RateLimitState {
    buckets: Arc<StdMutex<BTreeMap<[u8; 32], RateBucket>>>,
}

#[derive(Clone, Copy)]
struct RateBucket {
    window_started: Instant,
    requests: u32,
}

impl RateLimitState {
    const LIMIT_PER_MINUTE: u32 = 300;
    const MAX_BUCKETS: usize = 10_000;

    fn allow(&self, key: [u8; 32], now: Instant) -> bool {
        let mut buckets = self.buckets.lock().expect("rate-limit mutex poisoned");
        if buckets.len() >= Self::MAX_BUCKETS && !buckets.contains_key(&key) {
            buckets.retain(|_, bucket| {
                now.duration_since(bucket.window_started) < Duration::from_secs(60)
            });
            if buckets.len() >= Self::MAX_BUCKETS {
                return false;
            }
        }
        let bucket = buckets.entry(key).or_insert(RateBucket {
            window_started: now,
            requests: 0,
        });
        if now.duration_since(bucket.window_started) >= Duration::from_secs(60) {
            *bucket = RateBucket {
                window_started: now,
                requests: 0,
            };
        }
        if bucket.requests >= Self::LIMIT_PER_MINUTE {
            return false;
        }
        bucket.requests += 1;
        true
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "missing or invalid bearer token".into(),
        }
    }
}

impl From<RelayError> for ApiError {
    fn from(error: RelayError) -> Self {
        let status = match &error {
            RelayError::NotFound { .. } => StatusCode::NOT_FOUND,
            RelayError::InvalidTransition(_) | RelayError::ConcurrentModification { .. } => {
                StatusCode::CONFLICT
            }
            RelayError::PermissionDenied(_) => StatusCode::FORBIDDEN,
            RelayError::Validation(_) | RelayError::InvalidWorkspace(_) => StatusCode::BAD_REQUEST,
            RelayError::OrganizationNotInitialized | RelayError::TenantNotInitialized => {
                StatusCode::PRECONDITION_REQUIRED
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            "internal control plane error".to_string()
        } else {
            error.to_string()
        };
        Self { status, message }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

#[derive(Debug)]
struct ScimApiError {
    status: StatusCode,
    detail: String,
    scim_type: Option<&'static str>,
}

impl From<RelayError> for ScimApiError {
    fn from(error: RelayError) -> Self {
        let (status, scim_type) = match &error {
            RelayError::NotFound { .. } => (StatusCode::NOT_FOUND, None),
            RelayError::PermissionDenied(_) => (StatusCode::FORBIDDEN, None),
            RelayError::Validation(message)
                if message.contains("already") || message.contains("provisioned") =>
            {
                (StatusCode::CONFLICT, Some("uniqueness"))
            }
            RelayError::Validation(_) => (StatusCode::BAD_REQUEST, Some("invalidValue")),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, None),
        };
        let detail = if status == StatusCode::INTERNAL_SERVER_ERROR {
            "internal directory provisioning error".into()
        } else {
            error.to_string()
        };
        Self {
            status,
            detail,
            scim_type,
        }
    }
}

impl IntoResponse for ScimApiError {
    fn into_response(self) -> Response {
        let body = crate::scim::error(self.status.as_u16(), self.detail, self.scim_type);
        let mut response = (
            self.status,
            [(header::CONTENT_TYPE, "application/scim+json")],
            Json(body),
        )
            .into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                "Bearer realm=\"Relay SCIM\"".parse().unwrap(),
            );
        }
        response
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Clone, Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct ReadinessResponse {
    status: &'static str,
    schema_version: Option<i64>,
    event_count: Option<i64>,
    message: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct InboxItem {
    flow_id: String,
    flow_title: String,
    task: Task,
    blocked_by: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct OrganizationView {
    tenant: Tenant,
    organization: Organization,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRepositoryInput {
    /// GitLab 上的完整路径，例如 acme/web-app
    gitlab_project_path: String,
    /// 界面上显示的短名字；不填就取项目路径的最后一段
    #[serde(default)]
    name: Option<String>,
    /// 不填就用 GitLab 上的默认分支
    #[serde(default)]
    default_branch: Option<String>,
    /// 覆盖 GitLab 地址，一般不用填
    #[serde(default)]
    gitlab_url: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTeamInput {
    name: String,
    #[serde(default)]
    capabilities: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreatePrincipalInput {
    name: String,
    kind: PrincipalKind,
    #[serde(default)]
    team: Option<String>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    capabilities: String,
    #[serde(default = "default_capacity_percent")]
    capacity_percent: u8,
    /// Agent 的执行器配置。只有 kind=agent 时有意义。
    ///
    /// 可执行文件路径和工作区留空即可——Worker 跑在别的机器上时，这两个值由
    /// Worker 自己的启动参数决定，控制面这边只需要知道它是哪种执行器。
    #[serde(default)]
    executor: Option<CreateExecutorInput>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateExecutorInput {
    kind: ExecutorKind,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    command: Option<std::path::PathBuf>,
    #[serde(default)]
    workspace: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantRoleInput {
    role: OrganizationRole,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IssueCredentialInput {
    label: String,
    #[serde(default = "default_credential_ttl_days")]
    ttl_days: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDemandInput {
    summary: String,
    /// 这条需求在哪个代码仓库上做（ID 或名称）
    #[serde(default)]
    repository: Option<String>,
    #[serde(default = "default_planner_kind")]
    planner: PlannerKind,
    #[serde(default = "default_planner_timeout_seconds")]
    timeout_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetProviderInput {
    provider: String,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    model: Option<String>,
    api_key: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimPlanningInput {
    /// 领取后多久没回传就允许别人重领。
    #[serde(default = "default_planning_lease_seconds")]
    lease_seconds: u64,
}

const fn default_planning_lease_seconds() -> u64 {
    600
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitPlanningInput {
    plan: crate::domain::PlanDraft,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailPlanningInput {
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatInput {
    #[serde(default)]
    note: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockInput {
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceInput {
    kind: String,
    uri: String,
    summary: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NegotiateInput {
    effort_hours: f64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReassignInput {
    owner: String,
    #[serde(default)]
    copilots: Vec<String>,
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProposeFlowChangeInput {
    summary: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectFlowChangeInput {
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigureCalendarInput {
    utc_offset_minutes: i32,
    working_days: Vec<Workday>,
    day_start_minute: u16,
    day_end_minute: u16,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimeOffInput {
    starts_at: DateTime<Utc>,
    ends_at: DateTime<Utc>,
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchNotificationsInput {
    #[serde(default = "default_notification_dispatch_limit")]
    limit: usize,
    #[serde(default)]
    force: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct MessageInboxQuery {
    #[serde(default)]
    all: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct NotificationListQuery {
    #[serde(default)]
    all: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NotificationEndpointView {
    id: String,
    name: String,
    connector: NotificationConnector,
    destination_env: Option<String>,
    uses_legacy_url: bool,
    signing_secret_env: Option<String>,
    event_kinds: Vec<String>,
    active: bool,
    created_by: String,
    created_at: DateTime<Utc>,
    disabled_by: Option<String>,
    disabled_at: Option<DateTime<Utc>>,
}

impl From<&NotificationEndpoint> for NotificationEndpointView {
    fn from(endpoint: &NotificationEndpoint) -> Self {
        Self {
            id: endpoint.id.clone(),
            name: endpoint.name.clone(),
            connector: endpoint.connector,
            destination_env: endpoint.url_env.clone(),
            uses_legacy_url: endpoint.url_env.is_none(),
            signing_secret_env: (!endpoint.secret_env.is_empty())
                .then(|| endpoint.secret_env.clone()),
            event_kinds: endpoint.event_kinds.clone(),
            active: endpoint.active,
            created_by: endpoint.created_by.clone(),
            created_at: endpoint.created_at,
            disabled_by: endpoint.disabled_by.clone(),
            disabled_at: endpoint.disabled_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostMessageInput {
    #[serde(default)]
    task_id: Option<String>,
    kind: FlowMessageKind,
    recipients: Vec<String>,
    body: String,
    #[serde(default = "default_requires_ack")]
    requires_ack: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorizeFlightInput {
    agent: String,
    executor: ExecutorKind,
    #[serde(default = "default_lease_ttl_seconds")]
    ttl_seconds: u64,
    #[serde(default)]
    manifest: FlightManifestDraft,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimFlightInput {
    run_id: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FinishFlightInput {
    landed: bool,
    report: RemoteFlightReport,
}

#[derive(Clone, Debug, Deserialize)]
struct StageArtifactQuery {
    path: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoverFlightInput {
    action: RecoveryAction,
    reason: String,
    #[serde(default)]
    executor: Option<ExecutorKind>,
    #[serde(default)]
    objective: Option<String>,
    #[serde(default = "default_lease_ttl_seconds")]
    ttl_seconds: u64,
}

#[derive(Clone, Debug, Serialize)]
struct GitLabWebhookResponse {
    status: &'static str,
    event: String,
    matched_tasks: usize,
    changed_tasks: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct OidcLoginQuery {
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default = "default_oidc_return_to")]
    return_to: String,
}

#[derive(Clone, Debug, Deserialize)]
struct OidcCallbackQuery {
    code: String,
    state: String,
}

#[derive(Clone, Debug, Deserialize)]
struct OfficeReleasesQuery {
    #[serde(default)]
    flow_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestOfficeReleaseInput {
    task_id: String,
    provider: OfficeProvider,
    payload: OfficeReleasePayload,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectOfficeReleaseInput {
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
struct GitLabWritesQuery {
    #[serde(default)]
    flow_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestGitLabWriteInput {
    task_id: String,
    payload: GitLabWritePayload,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectGitLabWriteInput {
    reason: String,
}

pub async fn run(app: RelayApp, options: ServerOptions) -> Result<()> {
    validate_server_options(&options)?;
    let gitlab_webhook_auth = GitLabWebhookAuth::from_env()?;
    let interaction_auth = InteractionWebhookAuth::from_env()?;
    let oidc = OidcProvider::from_env().await?;
    let feishu = FeishuProvider::from_env()?;
    let scim_auth = ScimAuthenticator::from_env()?;
    let office_bridge = OfficeBridge::from_env()?;
    let gitlab_write_bridge = GitLabWriteBridge::from_env()?;
    let listener = TcpListener::bind(options.bind).await?;
    announce_server(&options, &gitlab_webhook_auth, &interaction_auth, 1);
    let tenant_id = app.state().tenant()?.id.clone();
    let app = Arc::new(Mutex::new(app));
    spawn_tracker(app.clone(), &options);
    spawn_notification_dispatcher(app.clone(), &options);
    spawn_office_dispatcher(
        app.clone(),
        office_bridge.clone(),
        tenant_id.clone(),
        &options,
    );
    spawn_gitlab_write_dispatcher(
        app.clone(),
        gitlab_write_bridge.clone(),
        tenant_id,
        &options,
    );
    axum::serve(
        listener,
        router_with_identity(
            app,
            ServerIntegrations {
                gitlab_webhook_auth,
                interaction_auth,
                oidc,
                feishu,
                scim_auth,
                office_bridge,
                gitlab_write_bridge,
            },
        ),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

pub async fn run_fleet(
    data_dir: impl AsRef<std::path::Path>,
    options: ServerOptions,
) -> Result<()> {
    validate_server_options(&options)?;
    let data_dir = data_dir.as_ref();
    let database_url = database_url_from_env()?;
    let mut catalog = TenantCatalog::configured(data_dir)?;
    let (default_record, mut default_app) = if database_url.is_some() {
        let record = catalog.default_tenant()?.ok_or_else(|| {
            RelayError::Validation(
                "PostgreSQL tenant catalog is empty; run `relay org init` before serve".into(),
            )
        })?;
        (record, None)
    } else {
        let app = RelayApp::open(data_dir)?;
        let record = catalog.adopt_default(app.state().tenant()?)?;
        (record, Some(app))
    };
    let gitlab_webhook_auth = GitLabWebhookAuth::from_env()?;
    let interaction_auth = InteractionWebhookAuth::from_env()?;
    let oidc = OidcProvider::from_env().await?;
    let feishu = FeishuProvider::from_env()?;
    let scim_auth = ScimAuthenticator::from_env()?;
    let office_bridge = OfficeBridge::from_env()?;
    let gitlab_write_bridge = GitLabWriteBridge::from_env()?;
    let mut routers = BTreeMap::new();

    for record in catalog.active()? {
        let app = if let Some(database_url) = database_url.as_deref() {
            RelayApp::open_postgres(catalog.data_dir(&record)?, database_url, &record.id)?
        } else if record.id == default_record.id {
            if default_record.storage_path != record.storage_path {
                return Err(RelayError::Validation(
                    "default tenant storage path changed while loading the fleet".into(),
                ));
            }
            default_app.take().ok_or_else(|| {
                RelayError::Validation("default tenant appears more than once in catalog".into())
            })?
        } else {
            RelayApp::open(catalog.data_dir(&record)?)?
        };
        let actual_tenant_id = &app.state().tenant()?.id;
        if actual_tenant_id != &record.id {
            return Err(RelayError::Validation(format!(
                "tenant catalog entry {} points to a Ledger owned by {actual_tenant_id}",
                record.id
            )));
        }
        let app = Arc::new(Mutex::new(app));
        spawn_tracker(app.clone(), &options);
        spawn_notification_dispatcher(app.clone(), &options);
        spawn_office_dispatcher(
            app.clone(),
            office_bridge.clone(),
            record.id.clone(),
            &options,
        );
        spawn_gitlab_write_dispatcher(
            app.clone(),
            gitlab_write_bridge.clone(),
            record.id.clone(),
            &options,
        );
        routers.insert(
            record.id,
            router_with_identity(
                app,
                ServerIntegrations {
                    gitlab_webhook_auth: gitlab_webhook_auth.clone(),
                    interaction_auth: interaction_auth.clone(),
                    oidc: oidc.clone(),
                    feishu: feishu.clone(),
                    scim_auth: scim_auth.clone(),
                    office_bridge: office_bridge.clone(),
                    gitlab_write_bridge: gitlab_write_bridge.clone(),
                },
            ),
        );
    }

    let tenant_count = routers.len();
    let listener = TcpListener::bind(options.bind).await?;
    announce_server(
        &options,
        &gitlab_webhook_auth,
        &interaction_auth,
        tenant_count,
    );
    let fleet = Router::new()
        .fallback(fleet_dispatch)
        .with_state(FleetState {
            routers: Arc::new(routers),
            default_tenant_id: default_record.id,
        });
    axum::serve(listener, fleet)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn fleet_dispatch(State(state): State<FleetState>, request: Request) -> Response {
    let header_tenant = request
        .headers()
        .get("x-relay-tenant")
        .and_then(|value| value.to_str().ok());
    let token_tenant = bearer_token(request.headers())
        .or_else(|| cookie_value(request.headers(), "relay_session"))
        .and_then(crate::app::tenant_token_hint);
    let path = request.uri().path();
    let login_tenant = if path == "/auth/oidc/login" || path == "/auth/feishu/login" {
        request.uri().query().and_then(|query| {
            serde_urlencoded::from_str::<BTreeMap<String, String>>(query)
                .ok()
                .and_then(|values| values.get("tenant").cloned())
        })
    } else if path == "/auth/oidc/callback" || path == "/auth/feishu/callback" {
        cookie_value(request.headers(), "relay_login_tenant").map(str::to_string)
    } else {
        None
    };
    if token_tenant.is_some() && header_tenant.is_some() && token_tenant != header_tenant {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "bearer token and x-relay-tenant select different tenants"})),
        )
            .into_response();
    }
    let tenant_id = token_tenant
        .or(header_tenant)
        .or(login_tenant.as_deref())
        .unwrap_or(&state.default_tenant_id);
    let Some(router) = state.routers.get(tenant_id) else {
        return ApiError::unauthorized().into_response();
    };
    match router.clone().oneshot(request).await {
        Ok(response) => response,
        Err(error) => match error {},
    }
}

fn validate_server_options(options: &ServerOptions) -> Result<()> {
    if options.tracker_interval_seconds == 0 {
        return Err(RelayError::Validation(
            "tracker interval must be greater than zero".into(),
        ));
    }
    if options.notification_interval_seconds == 0 {
        return Err(RelayError::Validation(
            "notification interval must be greater than zero".into(),
        ));
    }
    if !options.bind.ip().is_loopback() && !options.allow_insecure_public_http {
        return Err(RelayError::Validation(
            "refusing non-loopback plain HTTP; terminate TLS at a trusted proxy and pass --allow-insecure-public-http to acknowledge the hop".into(),
        ));
    }
    Ok(())
}

fn announce_server(
    options: &ServerOptions,
    gitlab_webhook_auth: &Option<GitLabWebhookAuth>,
    interaction_auth: &InteractionWebhookAuth,
    tenant_count: usize,
) {
    println!(
        "Relay control plane listening on http://{} ({tenant_count} tenant{})",
        options.bind,
        if tenant_count == 1 { "" } else { "s" }
    );
    if gitlab_webhook_auth.is_some() {
        println!("GitLab webhook receiver enabled");
    }
    if interaction_auth.bridge_enabled() {
        println!("Human interaction Bridge receiver enabled");
    }
    if interaction_auth.slack_enabled() {
        println!("Slack interaction receiver enabled");
    }
}

/// 一次进程启动解析出来的全部外部集成。
///
/// 收成结构体而不是继续加参数：`run` 和 `run_fleet` 都要原样传给每个 Tenant 的
/// Router，散成一长串位置参数很容易在调用点串位——尤其是相邻的几个
/// `Option<..Provider>`，串了编译器也未必报错。
#[derive(Clone)]
struct ServerIntegrations {
    gitlab_webhook_auth: Option<GitLabWebhookAuth>,
    interaction_auth: InteractionWebhookAuth,
    oidc: Option<OidcProvider>,
    feishu: Option<FeishuProvider>,
    scim_auth: ScimAuthenticator,
    office_bridge: OfficeBridge,
    gitlab_write_bridge: GitLabWriteBridge,
}

#[cfg(test)]
fn router(
    app: Arc<Mutex<RelayApp>>,
    gitlab_webhook_auth: Option<GitLabWebhookAuth>,
    interaction_auth: InteractionWebhookAuth,
) -> Router {
    router_with_identity(
        app,
        ServerIntegrations {
            gitlab_webhook_auth,
            interaction_auth,
            oidc: None,
            feishu: None,
            scim_auth: ScimAuthenticator::default(),
            office_bridge: OfficeBridge::disabled(),
            gitlab_write_bridge: GitLabWriteBridge::disabled(),
        },
    )
}

fn router_with_identity(app: Arc<Mutex<RelayApp>>, integrations: ServerIntegrations) -> Router {
    let ServerIntegrations {
        gitlab_webhook_auth,
        interaction_auth,
        oidc,
        feishu,
        scim_auth,
        office_bridge,
        gitlab_write_bridge,
    } = integrations;
    let rate_limit = RateLimitState::default();
    let tenant_id = app
        .try_lock()
        .expect("new API router must not hold the application lock")
        .state()
        .tenant
        .as_ref()
        .map(|tenant| tenant.id.clone())
        .unwrap_or_default();
    let state = ApiState {
        app,
        tenant_id,
        gitlab_webhook_auth,
        interaction_auth,
        oidc,
        feishu,
        scim_auth,
        office_bridge,
        gitlab_write_bridge,
    };
    Router::new()
        .route("/", get(root))
        .route("/console", get(crate::console::index))
        .route("/console/assets/app.js", get(crate::console::script))
        // Console 是单页应用，路由在浏览器里：/console/repositories 这类地址
        // 直接刷新时服务端必须回同一个 index.html，否则就是 404。
        .route("/console/{*path}", get(crate::console::index))
        .route("/health", get(health))
        .route("/health/live", get(health))
        .route("/health/ready", get(readiness))
        .route("/auth/oidc/login", get(oidc_login))
        .route("/auth/oidc/callback", get(oidc_callback))
        .route("/auth/feishu/login", get(feishu_login))
        .route("/auth/feishu/callback", get(feishu_callback))
        .route("/auth/logout", post(oidc_logout))
        .route("/scim/v2/Users", get(scim_users).post(scim_create_user))
        .route(
            "/scim/v2/Users/{id}",
            get(scim_user)
                .put(scim_replace_user)
                .patch(scim_patch_user)
                .delete(scim_delete_user),
        )
        .route("/scim/v2/Groups", get(scim_groups).post(scim_create_group))
        .route(
            "/scim/v2/Groups/{id}",
            get(scim_group)
                .put(scim_replace_group)
                .patch(scim_patch_group)
                .delete(scim_delete_group),
        )
        .route(
            "/scim/v2/ServiceProviderConfig",
            get(scim_service_provider_config),
        )
        .route("/scim/v2/ResourceTypes", get(scim_resource_types))
        .route("/scim/v2/Schemas", get(scim_schemas))
        .route("/metrics", get(metrics))
        .route("/api/v1/auth/methods", get(auth_methods))
        .route("/api/v1/me", get(me))
        .route("/api/v1/organization", get(organization))
        .route("/api/v1/teams", get(teams).post(create_team))
        .route(
            "/api/v1/repositories",
            get(repositories).post(create_repository),
        )
        .route(
            "/api/v1/repositories/{id}/archive",
            post(archive_repository),
        )
        .route("/api/v1/principals", get(principals).post(create_principal))
        .route(
            "/api/v1/principals/{id}/roles",
            get(principal_roles).post(grant_principal_role),
        )
        .route("/api/v1/roles/{id}/revoke", post(revoke_principal_role))
        .route(
            "/api/v1/principals/{id}/credentials",
            post(issue_principal_credential),
        )
        .route(
            "/api/v1/credentials/{id}/revoke",
            post(revoke_principal_credential),
        )
        .route("/api/v1/demands", post(create_demand))
        .route("/api/v1/planning-requests", get(planning_requests))
        .route(
            "/api/v1/planning-requests/{id}/claim",
            post(claim_planning_request),
        )
        .route(
            "/api/v1/planning-requests/{id}/submit",
            post(submit_planning_result),
        )
        .route(
            "/api/v1/planning-requests/{id}/fail",
            post(fail_planning_request),
        )
        .route(
            "/api/v1/me/model-provider",
            get(my_provider)
                .put(set_my_provider)
                .delete(delete_my_provider),
        )
        .route("/api/v1/me/calendar", get(my_calendar).put(set_my_calendar))
        .route("/api/v1/me/time-off", post(add_my_time_off))
        .route("/api/v1/me/time-off/{id}/cancel", post(cancel_my_time_off))
        .route("/api/v1/dashboard", get(dashboard))
        .route("/api/v1/inbox", get(inbox))
        .route("/api/v1/messages", get(message_inbox))
        .route(
            "/api/v1/notifications/endpoints",
            get(notification_endpoints),
        )
        .route(
            "/api/v1/notifications/deliveries",
            get(notification_deliveries),
        )
        .route(
            "/api/v1/notifications/dispatch",
            post(dispatch_notifications),
        )
        .route("/api/v1/messages/{id}/ack", post(acknowledge_message))
        .route("/api/v1/escalations", get(escalations))
        .route("/api/v1/escalations/{id}/ack", post(ack_escalation))
        .route("/api/v1/flows/{id}/approve", post(approve_flow))
        .route(
            "/api/v1/flows/{id}/messages",
            get(flow_messages).post(post_message),
        )
        .route(
            "/api/v1/flows/{id}/changes",
            get(flow_changes).post(propose_flow_change),
        )
        .route(
            "/api/v1/flow-changes/{id}/approve",
            post(approve_flow_change),
        )
        .route("/api/v1/flow-changes/{id}/reject", post(reject_flow_change))
        .route("/api/v1/tasks/{id}/accept", post(accept_task))
        .route("/api/v1/tasks/{id}/start", post(start_task))
        .route("/api/v1/tasks/{id}/heartbeat", post(heartbeat_task))
        .route("/api/v1/tasks/{id}/negotiate", post(negotiate_task))
        .route("/api/v1/tasks/{id}/reassign", post(reassign_task))
        .route(
            "/api/v1/tasks/{id}/reassignment-candidates",
            get(reassignment_candidates),
        )
        .route("/api/v1/tasks/{id}/block", post(block_task))
        .route("/api/v1/tasks/{id}/evidence", post(add_evidence))
        .route("/api/v1/tasks/{id}/flight-leases", post(authorize_flight))
        .route("/api/v1/flight-leases", get(flight_leases))
        .route("/api/v1/flight-leases/{id}/claim", post(claim_flight))
        .route("/api/v1/flight-leases/{id}/revoke", post(revoke_flight))
        .route("/api/v1/flight-leases/{id}/finish", post(finish_flight))
        .route(
            "/api/v1/flight-leases/{id}/artifacts",
            get(flight_artifacts)
                .put(stage_flight_artifact)
                .layer(DefaultBodyLimit::max(
                    crate::application::app::artifacts::MAX_ARTIFACT_BYTES,
                )),
        )
        .route("/api/v1/artifacts/{id}/content", get(artifact_content))
        .route(
            "/api/v1/office/releases",
            get(office_releases).post(request_office_release),
        )
        .route(
            "/api/v1/office/releases/{id}/approve",
            post(approve_office_release),
        )
        .route(
            "/api/v1/office/releases/{id}/reject",
            post(reject_office_release),
        )
        .route(
            "/api/v1/office/releases/{id}/retry",
            post(retry_office_release),
        )
        .route(
            "/api/v1/office/releases/dispatch",
            post(dispatch_office_release),
        )
        .route(
            "/api/v1/gitlab/writes",
            get(gitlab_writes).post(request_gitlab_write),
        )
        .route(
            "/api/v1/gitlab/writes/{id}/approve",
            post(approve_gitlab_write),
        )
        .route(
            "/api/v1/gitlab/writes/{id}/reject",
            post(reject_gitlab_write),
        )
        .route("/api/v1/gitlab/writes/{id}/retry", post(retry_gitlab_write))
        .route(
            "/api/v1/gitlab/writes/dispatch",
            post(dispatch_gitlab_write),
        )
        .route(
            "/api/v1/flight-leases/{id}/recovery-options",
            get(flight_recovery_options),
        )
        .route("/api/v1/flight-leases/{id}/recover", post(recover_flight))
        .route("/api/v1/tasks/{id}/submit", post(submit_task))
        .route("/api/v1/tasks/{id}/complete", post(complete_task))
        .route("/api/v1/connectors/gitlab/webhook", post(gitlab_webhook))
        .route("/api/v1/connectors/interactions", post(bridge_interaction))
        .route("/api/v1/connectors/slack/actions", post(slack_interaction))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(middleware::from_fn_with_state(rate_limit, request_guard))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            refresh_shared_ledger,
        ))
        .with_state(state)
}

async fn refresh_shared_ledger(
    State(state): State<ApiState>,
    request: Request,
    next: middleware::Next,
) -> Response {
    let needs_state = request.uri().path().starts_with("/api/")
        || request.uri().path().starts_with("/scim/")
        || matches!(request.uri().path(), "/metrics" | "/health/ready");
    if needs_state {
        let mut app = state.app.lock().await;
        if let Err(error) = app.refresh_shared_state() {
            eprintln!("shared Flow Ledger refresh failed: {error}");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "shared Flow Ledger is unavailable"})),
            )
                .into_response();
        }
    }
    next.run(request).await
}

async fn request_guard(
    State(rate_limit): State<RateLimitState>,
    request: Request,
    next: middleware::Next,
) -> Response {
    let api_response = request.uri().path().starts_with("/api/")
        || matches!(request.uri().path(), "/metrics" | "/health/ready");
    let request_id = format!("REQ-{}", uuid::Uuid::new_v4().simple());
    let key = request
        .headers()
        .get(header::AUTHORIZATION)
        .map(|value| Sha256::digest(value.as_bytes()).into())
        .or_else(|| {
            cookie_value(request.headers(), "relay_session")
                .map(|value| Sha256::digest(value.as_bytes()).into())
        })
        .unwrap_or_else(|| Sha256::digest(b"anonymous").into());
    if !rate_limit.allow(key, Instant::now()) {
        let mut response = (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "request rate limit exceeded"})),
        )
            .into_response();
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, "60".parse().unwrap());
        return harden_response(response, &request_id, api_response);
    }
    harden_response(next.run(request).await, &request_id, api_response)
}

fn harden_response(mut response: Response, request_id: &str, no_store: bool) -> Response {
    response.headers_mut().insert(
        header::HeaderName::from_static("x-request-id"),
        request_id.parse().unwrap(),
    );
    response
        .headers_mut()
        .insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    if no_store {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    }
    response
}

fn spawn_tracker(app: Arc<Mutex<RelayApp>>, options: &ServerOptions) {
    let tracker_interval_seconds = options.tracker_interval_seconds;
    let stale_after_hours = options.stale_after_hours;
    let escalate_after_hours = options.escalate_after_hours;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(tracker_interval_seconds));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let mut app = app.lock().await;
            if app.state().organization.is_some() {
                if let Err(error) = app.expire_remote_flights("tower://lease-reaper") {
                    eprintln!("flight lease reaper failed: {error}");
                }
                if let Err(error) = app.scan_tracking_with_policy(
                    stale_after_hours,
                    escalate_after_hours,
                    "tower://server",
                ) {
                    eprintln!("tracker scan failed: {error}");
                }
            }
        }
    });
}

fn spawn_notification_dispatcher(app: Arc<Mutex<RelayApp>>, options: &ServerOptions) {
    let interval_seconds = options.notification_interval_seconds;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_seconds));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(error) = deliver_notification_batch(
                &app,
                50,
                false,
                "tower://server-notification-dispatcher",
            )
            .await
            {
                eprintln!("notification dispatch failed: {error}");
            }
        }
    });
}

fn spawn_office_dispatcher(
    app: Arc<Mutex<RelayApp>>,
    bridge: OfficeBridge,
    tenant_id: String,
    options: &ServerOptions,
) {
    let interval_seconds = options.notification_interval_seconds;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_seconds));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            loop {
                match dispatch_office_once(&app, &bridge, &tenant_id).await {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(error) => {
                        eprintln!("Office release dispatch failed: {error}");
                        break;
                    }
                }
            }
        }
    });
}

fn spawn_gitlab_write_dispatcher(
    app: Arc<Mutex<RelayApp>>,
    bridge: GitLabWriteBridge,
    tenant_id: String,
    options: &ServerOptions,
) {
    let interval_seconds = options.notification_interval_seconds;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_seconds));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            loop {
                match dispatch_gitlab_write_once(&app, &bridge, &tenant_id).await {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(error) => {
                        eprintln!("GitLab write dispatch failed: {error}");
                        break;
                    }
                }
            }
        }
    });
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn root() -> Redirect {
    Redirect::temporary("/console")
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "relay-control-plane",
    })
}

async fn oidc_login(
    State(state): State<ApiState>,
    Query(query): Query<OidcLoginQuery>,
) -> ApiResult<Response> {
    let provider = state.oidc.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::NOT_FOUND,
        message: "OIDC login is not configured".into(),
    })?;
    let app = state.app.lock().await;
    let tenant_id = &app.state().tenant()?.id;
    if query
        .tenant
        .as_deref()
        .is_some_and(|value| value != tenant_id)
    {
        return Err(ApiError::unauthorized());
    }
    let login = provider.begin_login(tenant_id, &query.return_to).await?;
    let mut response = Redirect::temporary(&login.authorization_url).into_response();
    let secure = if provider.secure_cookie() {
        "; Secure"
    } else {
        ""
    };
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_login_tenant={}; Path=/auth/oidc/callback; HttpOnly; SameSite=Lax; Max-Age=600{secure}",
            login.tenant_id
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not establish OIDC login state".into(),
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_oidc_state={}; Path=/auth/oidc/callback; HttpOnly; SameSite=Lax; Max-Age=600{secure}",
            login.state_cookie
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not establish OIDC state cookie".into(),
        })?,
    );
    Ok(response)
}

async fn oidc_callback(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<OidcCallbackQuery>,
) -> ApiResult<Response> {
    let provider = state.oidc.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::NOT_FOUND,
        message: "OIDC login is not configured".into(),
    })?;
    let state_cookie =
        cookie_value(&headers, "relay_oidc_state").ok_or_else(ApiError::unauthorized)?;
    let identity = provider
        .complete_login(&query.code, &query.state, state_cookie)
        .await?;
    let mut app = state.app.lock().await;
    if app.state().tenant()?.id != identity.tenant_id {
        return Err(ApiError::unauthorized());
    }
    let principal = app.oidc_principal(&identity.subject)?;
    let session = app.issue_oidc_session(&principal.id)?;
    let mut response = Redirect::to(&identity.return_to).into_response();
    let secure = if provider.secure_cookie() {
        "; Secure"
    } else {
        ""
    };
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_session={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=28800{secure}",
            session.token
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not establish OIDC session".into(),
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_oidc_state=; Path=/auth/oidc/callback; HttpOnly; SameSite=Lax; Max-Age=0{secure}"
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not clear OIDC state cookie".into(),
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_login_tenant=; Path=/auth/oidc/callback; HttpOnly; SameSite=Lax; Max-Age=0{secure}"
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not clear OIDC login state".into(),
        })?,
    );
    Ok(response)
}

async fn feishu_login(
    State(state): State<ApiState>,
    Query(query): Query<OidcLoginQuery>,
) -> ApiResult<Response> {
    let provider = state.feishu.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::NOT_FOUND,
        message: "Feishu login is not configured".into(),
    })?;
    let app = state.app.lock().await;
    let tenant_id = &app.state().tenant()?.id;
    if query
        .tenant
        .as_deref()
        .is_some_and(|value| value != tenant_id)
    {
        return Err(ApiError::unauthorized());
    }
    let login = provider.begin_login(tenant_id, &query.return_to)?;
    let mut response = Redirect::temporary(&login.authorization_url).into_response();
    let secure = if provider.secure_cookie() {
        "; Secure"
    } else {
        ""
    };
    // Cookie 名字和 OIDC 那条链路相同，但 Path 不同，两者不会互相覆盖。
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_login_tenant={}; Path=/auth/feishu/callback; HttpOnly; SameSite=Lax; Max-Age=600{secure}",
            login.tenant_id
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not establish Feishu login state".into(),
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_feishu_state={}; Path=/auth/feishu/callback; HttpOnly; SameSite=Lax; Max-Age=600{secure}",
            login.state_cookie
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not establish Feishu state cookie".into(),
        })?,
    );
    Ok(response)
}

async fn feishu_callback(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<OidcCallbackQuery>,
) -> ApiResult<Response> {
    let provider = state.feishu.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::NOT_FOUND,
        message: "Feishu login is not configured".into(),
    })?;
    let state_cookie =
        cookie_value(&headers, "relay_feishu_state").ok_or_else(ApiError::unauthorized)?;
    // complete_login 内部已经按 tenant_key 白名单挡过外部企业的用户。
    let identity = provider
        .complete_login(&query.code, &query.state, state_cookie)
        .await?;
    let mut app = state.app.lock().await;
    if app.state().tenant()?.id != identity.tenant_id {
        return Err(ApiError::unauthorized());
    }
    let principal = app.feishu_principal(&identity.open_id, &identity.name)?;
    let session = app.issue_oidc_session(&principal.id)?;
    let mut response = Redirect::to(&identity.return_to).into_response();
    let secure = if provider.secure_cookie() {
        "; Secure"
    } else {
        ""
    };
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_session={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=28800{secure}",
            session.token
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not establish Feishu session".into(),
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_feishu_state=; Path=/auth/feishu/callback; HttpOnly; SameSite=Lax; Max-Age=0{secure}"
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not clear Feishu state cookie".into(),
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        format!(
            "relay_login_tenant=; Path=/auth/feishu/callback; HttpOnly; SameSite=Lax; Max-Age=0{secure}"
        )
        .parse()
        .map_err(|_| ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "could not clear Feishu login state".into(),
        })?,
    );
    Ok(response)
}

async fn oidc_logout(State(state): State<ApiState>, headers: HeaderMap) -> ApiResult<Response> {
    if let Some(token) = cookie_value(&headers, "relay_session") {
        state.app.lock().await.revoke_oidc_session(token)?;
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        "relay_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"
            .parse()
            .unwrap(),
    );
    Ok(response)
}

async fn scim_users(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<crate::scim::ListQuery>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let app = state.app.lock().await;
    scim_json(StatusCode::OK, crate::scim::list_users(&app, &query)?)
}

async fn scim_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let app = state.app.lock().await;
    scim_json(StatusCode::OK, crate::scim::get_user(&app, &id)?)
}

async fn scim_create_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<crate::scim::UserInput>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let mut app = state.app.lock().await;
    scim_json(
        StatusCode::CREATED,
        crate::scim::create_user(&mut app, input)?,
    )
}

async fn scim_replace_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<crate::scim::UserInput>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let mut app = state.app.lock().await;
    scim_json(
        StatusCode::OK,
        crate::scim::replace_user(&mut app, &id, input)?,
    )
}

async fn scim_patch_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<crate::scim::PatchRequest>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let mut app = state.app.lock().await;
    scim_json(
        StatusCode::OK,
        crate::scim::patch_user(&mut app, &id, input)?,
    )
}

async fn scim_delete_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let mut app = state.app.lock().await;
    crate::scim::delete_user(&mut app, &id)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn scim_groups(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<crate::scim::ListQuery>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let app = state.app.lock().await;
    scim_json(StatusCode::OK, crate::scim::list_groups(&app, &query)?)
}

async fn scim_group(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let app = state.app.lock().await;
    scim_json(StatusCode::OK, crate::scim::get_group(&app, &id)?)
}

async fn scim_create_group(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<crate::scim::GroupInput>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let mut app = state.app.lock().await;
    scim_json(
        StatusCode::CREATED,
        crate::scim::create_group(&mut app, input)?,
    )
}

async fn scim_replace_group(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<crate::scim::GroupInput>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let mut app = state.app.lock().await;
    scim_json(
        StatusCode::OK,
        crate::scim::replace_group(&mut app, &id, input)?,
    )
}

async fn scim_patch_group(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<crate::scim::PatchRequest>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let mut app = state.app.lock().await;
    scim_json(
        StatusCode::OK,
        crate::scim::patch_group(&mut app, &id, input)?,
    )
}

async fn scim_delete_group(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    let mut app = state.app.lock().await;
    crate::scim::delete_group(&mut app, &id)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn scim_service_provider_config(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    scim_json(
        StatusCode::OK,
        json!({
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
            "patch": {"supported": true},
            "bulk": {"supported": false, "maxOperations": 0, "maxPayloadSize": 0},
            "filter": {"supported": true, "maxResults": 200},
            "changePassword": {"supported": false},
            "sort": {"supported": false},
            "etag": {"supported": false},
            "authenticationSchemes": [{
                "type": "oauthbearertoken",
                "name": "Bearer Token",
                "description": "Tenant-scoped SCIM provisioning token",
                "primary": true
            }]
        }),
    )
}

async fn scim_resource_types(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    scim_json(
        StatusCode::OK,
        json!({
            "schemas": [crate::scim::LIST_SCHEMA],
            "totalResults": 2,
            "startIndex": 1,
            "itemsPerPage": 2,
            "Resources": [
                {
                    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
                    "id": "User",
                    "name": "User",
                    "endpoint": "/Users",
                    "schema": crate::scim::USER_SCHEMA
                },
                {
                    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
                    "id": "Group",
                    "name": "Group",
                    "endpoint": "/Groups",
                    "schema": crate::scim::GROUP_SCHEMA
                }
            ]
        }),
    )
}

async fn scim_schemas(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> std::result::Result<Response, ScimApiError> {
    authorize_scim(&state, &headers)?;
    scim_json(
        StatusCode::OK,
        json!({
            "schemas": [crate::scim::LIST_SCHEMA],
            "totalResults": 2,
            "startIndex": 1,
            "itemsPerPage": 2,
            "Resources": [
                {
                    "id": crate::scim::USER_SCHEMA,
                    "name": "User",
                    "description": "Relay Human Principal",
                    "attributes": [
                        {"name": "userName", "type": "string", "multiValued": false, "required": true, "uniqueness": "server"},
                        {"name": "displayName", "type": "string", "multiValued": false, "required": false},
                        {"name": "active", "type": "boolean", "multiValued": false, "required": false}
                    ]
                },
                {
                    "id": crate::scim::GROUP_SCHEMA,
                    "name": "Group",
                    "description": "Relay Team",
                    "attributes": [
                        {"name": "displayName", "type": "string", "multiValued": false, "required": true},
                        {"name": "members", "type": "complex", "multiValued": true, "required": false}
                    ]
                }
            ]
        }),
    )
}

fn authorize_scim(state: &ApiState, headers: &HeaderMap) -> std::result::Result<(), ScimApiError> {
    let token = bearer_token(headers).ok_or_else(scim_unauthorized)?;
    if !state.scim_auth.enabled() || !state.scim_auth.verify(&state.tenant_id, token) {
        return Err(scim_unauthorized());
    }
    Ok(())
}

fn scim_unauthorized() -> ScimApiError {
    ScimApiError {
        status: StatusCode::UNAUTHORIZED,
        detail: "missing or invalid SCIM bearer token".into(),
        scim_type: None,
    }
}

fn scim_json<T: Serialize>(
    status: StatusCode,
    value: T,
) -> std::result::Result<Response, ScimApiError> {
    Ok((
        status,
        [(header::CONTENT_TYPE, "application/scim+json")],
        Json(value),
    )
        .into_response())
}

async fn readiness(State(state): State<ApiState>) -> Response {
    let app = state.app.lock().await;
    if app.state().organization.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadinessResponse {
                status: "not_ready",
                schema_version: None,
                event_count: None,
                message: Some("organization is not initialized".into()),
            }),
        )
            .into_response();
    }
    match app.storage_health() {
        Ok(health) if health.integrity == "ok" => (
            StatusCode::OK,
            Json(ReadinessResponse {
                status: "ready",
                schema_version: Some(health.schema_version),
                event_count: Some(health.event_count),
                message: None,
            }),
        )
            .into_response(),
        Ok(health) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadinessResponse {
                status: "not_ready",
                schema_version: Some(health.schema_version),
                event_count: Some(health.event_count),
                message: Some(format!("storage integrity: {}", health.integrity)),
            }),
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadinessResponse {
                status: "not_ready",
                schema_version: None,
                event_count: None,
                message: Some(error.to_string()),
            }),
        )
            .into_response(),
    }
}

async fn metrics(State(state): State<ApiState>, headers: HeaderMap) -> ApiResult<Response> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    let dashboard = app.admin_dashboard(&principal.id)?;
    let storage = app.storage_health()?;
    let metrics = dashboard.metrics;
    let body = format!(
        "# TYPE relay_flows gauge\nrelay_flows {}\n\
         # TYPE relay_active_flows gauge\nrelay_active_flows {}\n\
         # TYPE relay_tasks gauge\nrelay_tasks {}\n\
         # TYPE relay_blocked_tasks gauge\nrelay_blocked_tasks {}\n\
         # TYPE relay_open_flights gauge\nrelay_open_flights {}\n\
         # TYPE relay_pending_notifications gauge\nrelay_pending_notifications {}\n\
         # TYPE relay_pending_office_releases gauge\nrelay_pending_office_releases {}\n\
         # TYPE relay_indeterminate_office_releases gauge\nrelay_indeterminate_office_releases {}\n\
         # TYPE relay_pending_gitlab_writes gauge\nrelay_pending_gitlab_writes {}\n\
         # TYPE relay_indeterminate_gitlab_writes gauge\nrelay_indeterminate_gitlab_writes {}\n\
         # TYPE relay_ledger_events counter\nrelay_ledger_events {}\n",
        metrics.total_flows,
        metrics.active_flows,
        metrics.total_tasks,
        metrics.blocked_tasks,
        metrics.open_flights,
        metrics.pending_notifications,
        metrics.pending_office_releases,
        metrics.indeterminate_office_releases,
        metrics.pending_gitlab_writes,
        metrics.indeterminate_gitlab_writes,
        storage.event_count,
    );
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response())
}

/// 本部署配置了哪些登录方式。故意不鉴权：登录页在拿到身份之前就要用它决定
/// 显示哪些按钮。只暴露布尔值，不泄露 App ID、Issuer 这类配置细节。
async fn auth_methods(State(state): State<ApiState>) -> Json<serde_json::Value> {
    Json(json!({
        "feishu": state.feishu.is_some(),
        "oidc": state.oidc.is_some(),
    }))
}

async fn me(State(state): State<ApiState>, headers: HeaderMap) -> ApiResult<Json<Principal>> {
    let app = state.app.lock().await;
    Ok(Json(authenticate(&app, &headers)?))
}

async fn organization(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<OrganizationView>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    app.authorize_organization_read(&principal.id)?;
    Ok(Json(OrganizationView {
        tenant: app.state().tenant()?.clone(),
        organization: app.state().organization()?.clone(),
    }))
}

async fn teams(State(state): State<ApiState>, headers: HeaderMap) -> ApiResult<Json<Vec<Team>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    app.authorize_organization_read(&principal.id)?;
    let mut teams = app.state().teams.values().cloned().collect::<Vec<_>>();
    teams.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(Json(teams))
}

async fn repositories(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<Repository>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    app.authorize_organization_read(&principal.id)?;
    Ok(Json(app.repositories()))
}

async fn create_repository(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<CreateRepositoryInput>,
) -> ApiResult<Json<Repository>> {
    // 先确认 GitLab 上项目真实存在、Token 也有权限，再落库。否则会攒下一堆
    // 连不上的仓库，等 Agent 开工时才发现——那时排查成本高得多。
    let client = GitLabClient::from_env(input.gitlab_url.as_deref())?;
    let project = client.check_project(&input.gitlab_project_path).await?;
    let branch = input
        .default_branch
        .clone()
        .or_else(|| project.default_branch.clone())
        .unwrap_or_else(|| "main".to_string());
    let name = input.name.clone().unwrap_or_else(|| {
        project
            .path_with_namespace
            .rsplit('/')
            .next()
            .unwrap_or(&project.path_with_namespace)
            .to_string()
    });
    mutate(&state, &headers, |app, actor| {
        app.register_repository(&name, &project.path_with_namespace, &branch, actor)
    })
    .await
}

async fn archive_repository(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Repository>> {
    mutate(&state, &headers, |app, actor| {
        app.archive_repository(&id, actor)
    })
    .await
}

async fn create_team(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<CreateTeamInput>,
) -> ApiResult<Json<Team>> {
    mutate(&state, &headers, |app, actor| {
        app.create_team(&input.name, &input.capabilities, actor)
    })
    .await
}

async fn principals(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<Principal>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    app.authorize_organization_read(&principal.id)?;
    let mut principals = app.state().principals.values().cloned().collect::<Vec<_>>();
    principals.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(Json(principals))
}

async fn create_principal(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<CreatePrincipalInput>,
) -> ApiResult<Json<Principal>> {
    let executor = input.executor.as_ref().map(|config| ExecutorConfig {
        kind: config.kind.clone(),
        // 远程 Worker 自带工作区，控制面这边给个占位即可。
        workspace: config
            .workspace
            .clone()
            .unwrap_or_else(|| std::path::PathBuf::from(".")),
        model: config.model.clone(),
        command: config.command.clone(),
    });
    mutate(&state, &headers, |app, actor| {
        app.register_principal(
            &input.name,
            input.kind,
            input.team.as_deref(),
            input.owner.as_deref(),
            &input.capabilities,
            input.capacity_percent,
            executor.clone(),
            actor,
        )
    })
    .await
}

async fn principal_roles(
    State(state): State<ApiState>,
    Path(principal_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<RoleBinding>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.role_bindings(
        &principal_id,
        &principal.id,
        false,
    )?))
}

async fn grant_principal_role(
    State(state): State<ApiState>,
    Path(principal_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<GrantRoleInput>,
) -> ApiResult<Json<RoleBinding>> {
    mutate(&state, &headers, |app, actor| {
        app.grant_role(&principal_id, input.role, actor)
    })
    .await
}

async fn revoke_principal_role(
    State(state): State<ApiState>,
    Path(binding_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<RoleBinding>> {
    mutate(&state, &headers, |app, actor| {
        app.revoke_role(&binding_id, actor)
    })
    .await
}

async fn issue_principal_credential(
    State(state): State<ApiState>,
    Path(principal_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<IssueCredentialInput>,
) -> ApiResult<Json<IssuedCredential>> {
    mutate(&state, &headers, |app, actor| {
        app.issue_api_credential_with_ttl(&principal_id, &input.label, actor, input.ttl_days)
    })
    .await
}

async fn revoke_principal_credential(
    State(state): State<ApiState>,
    Path(credential_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<crate::domain::ApiCredential>> {
    mutate(&state, &headers, |app, actor| {
        app.revoke_api_credential(&credential_id, actor)
    })
    .await
}

async fn create_demand(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<CreateDemandInput>,
) -> ApiResult<Json<serde_json::Value>> {
    if input.timeout_seconds == 0 || input.timeout_seconds > 3_600 {
        return Err(RelayError::Validation(
            "planner timeout must be between 1 and 3600 seconds".into(),
        )
        .into());
    }
    let workspace = std::env::current_dir().map_err(RelayError::from)?;
    let (data_dir, principal_id, tenant_id) = {
        let app = state.app.lock().await;
        let principal = authenticate(&app, &headers)?;
        (
            app.data_dir().to_path_buf(),
            principal.id,
            app.state().tenant()?.id.clone(),
        )
    };
    // 需要模型的拆解一律排队交给 Worker：控制面容器里没有 claude / codex，
    // 也不该有——那等于把凭据和任意代码执行塞进管状态的进程里。
    if input.planner != PlannerKind::Local {
        let request = {
            let mut app = state.app.lock().await;
            app.request_planning(
                &input.summary,
                &principal_id,
                input.planner,
                input.repository.as_deref(),
            )?
        };
        // 信封固定两个字段：排队时 flow 为 null，本地拆解时 planning_request 为 null。
        // 形状不随 planner 变化，客户端不用猜。
        return Ok(Json(
            json!({ "flow": serde_json::Value::Null, "planning_request": request }),
        ));
    }

    // 必须跟主实例连同一个库；写死 SQLite 会在 PostgreSQL 部署下开出空库。
    let mut planning_app = RelayApp::open_for_side_task(data_dir, &tenant_id)?;
    let flow = planning_app
        .create_demand(
            &input.summary,
            &principal_id,
            input.planner,
            &workspace,
            input.timeout_seconds,
            input.repository.as_deref(),
        )
        .await?;
    state.app.lock().await.reload()?;
    Ok(Json(
        json!({ "flow": flow, "planning_request": serde_json::Value::Null }),
    ))
}

async fn planning_requests(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<crate::domain::PlanningRequest>>> {
    let app = state.app.lock().await;
    authenticate(&app, &headers)?;
    Ok(Json(app.planning_requests()))
}

/// Worker 领取一条拆解请求。
///
/// 响应里**带上提出人的模型凭据**：Worker 要用提出人自己的 Key 去调模型，
/// 这样费用落到本人头上，也不需要在集群里放一把公用密钥。
/// 代价是密钥会经过集群网络（内网是明文 HTTP），这是部署时接受的取舍。
async fn claim_planning_request(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<ClaimPlanningInput>,
) -> ApiResult<Json<serde_json::Value>> {
    let request = mutate(&state, &headers, |app, actor| {
        app.claim_planning_request(&id, actor, input.lease_seconds)
    })
    .await?
    .0;
    let app = state.app.lock().await;
    // 提出人没配凭据时不报错：Worker 可能自带环境变量，让它自己决定能不能跑。
    let provider = app
        .state()
        .principals
        .values()
        .find(|principal| principal.name == request.requester || principal.id == request.requester)
        .and_then(|principal| app.provider_credential(&principal.id).ok().flatten());
    Ok(Json(json!({ "request": request, "provider": provider })))
}

async fn submit_planning_result(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<SubmitPlanningInput>,
) -> ApiResult<Json<Flow>> {
    mutate(&state, &headers, |app, actor| {
        app.submit_planning_result(&id, input.plan.clone(), actor)
    })
    .await
}

async fn fail_planning_request(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<FailPlanningInput>,
) -> ApiResult<Json<crate::domain::PlanningRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.fail_planning_request(&id, &input.reason, actor)
    })
    .await
}

/// 看自己的模型配置。**永远不返回明文 Key**——只回一个掩码，够确认"配没配、
/// 配的是不是这把"就行。
async fn my_provider(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    let Some(credential) = app.provider_credential(&principal.id)? else {
        return Ok(Json(json!({ "configured": false })));
    };
    Ok(Json(json!({
        "configured": true,
        "provider": credential.provider,
        "base_url": credential.base_url,
        "model": credential.model,
        "api_key_hint": mask_secret(&credential.api_key),
        "updated_at": credential.updated_at,
    })))
}

async fn set_my_provider(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<SetProviderInput>,
) -> ApiResult<Json<serde_json::Value>> {
    let mut app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    app.set_provider_credential(
        &principal.id,
        &input.provider,
        input.base_url.as_deref(),
        input.model.as_deref(),
        &input.api_key,
    )?;
    Ok(Json(json!({ "configured": true })))
}

async fn delete_my_provider(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    let mut app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    app.delete_provider_credential(&principal.id)?;
    Ok(Json(json!({ "configured": false })))
}

/// 只露头尾，中间一律星号。太短的直接全星号——否则等于把 Key 说了一半。
fn mask_secret(value: &str) -> String {
    let characters: Vec<char> = value.chars().collect();
    if characters.len() <= 8 {
        return "*".repeat(characters.len().max(4));
    }
    let head: String = characters.iter().take(4).collect();
    let tail: String = characters
        .iter()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head}…{tail}")
}

async fn my_calendar(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<WorkCalendar>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.state().work_calendar(&principal.id)?.clone()))
}

async fn set_my_calendar(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<ConfigureCalendarInput>,
) -> ApiResult<Json<WorkCalendar>> {
    mutate(&state, &headers, |app, actor| {
        app.configure_work_calendar(
            actor,
            input.utc_offset_minutes,
            input.working_days,
            input.day_start_minute,
            input.day_end_minute,
            actor,
        )
    })
    .await
}

async fn add_my_time_off(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<TimeOffInput>,
) -> ApiResult<Json<AvailabilityBlock>> {
    mutate(&state, &headers, |app, actor| {
        app.add_time_off(actor, input.starts_at, input.ends_at, &input.reason, actor)
    })
    .await
}

async fn cancel_my_time_off(
    State(state): State<ApiState>,
    Path(block_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<AvailabilityBlock>> {
    mutate(&state, &headers, |app, actor| {
        app.cancel_time_off(actor, &block_id, actor)
    })
    .await
}

async fn dashboard(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<DashboardSnapshot>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.admin_dashboard(&principal.id)?))
}

async fn inbox(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<InboxItem>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    let items = app
        .inbox(&principal.id)?
        .into_iter()
        .map(|(flow, task)| {
            let blocked_by = task
                .depends_on
                .iter()
                .filter_map(|id| flow.task(id))
                .filter(|dependency| dependency.status != crate::domain::TaskStatus::Completed)
                .map(|dependency| dependency.id.clone())
                .collect();
            InboxItem {
                flow_id: flow.id.clone(),
                flow_title: flow.prd.title.clone(),
                task: task.clone(),
                blocked_by,
            }
        })
        .collect();
    Ok(Json(items))
}

async fn message_inbox(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<MessageInboxQuery>,
) -> ApiResult<Json<Vec<MessageInboxItem>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.message_inbox(&principal.id, query.all)?))
}

async fn notification_endpoints(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<NotificationListQuery>,
) -> ApiResult<Json<Vec<NotificationEndpointView>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    app.authorize_notification_admin(&principal.id)?;
    let mut endpoints = app
        .state()
        .notification_endpoints
        .values()
        .filter(|endpoint| query.all || endpoint.active)
        .map(NotificationEndpointView::from)
        .collect::<Vec<_>>();
    endpoints.sort_by_key(|endpoint| endpoint.created_at);
    Ok(Json(endpoints))
}

async fn notification_deliveries(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<NotificationListQuery>,
) -> ApiResult<Json<Vec<NotificationDelivery>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    app.authorize_notification_admin(&principal.id)?;
    let mut deliveries = app
        .state()
        .notification_deliveries
        .values()
        .filter(|delivery| {
            query.all
                || matches!(
                    delivery.status,
                    crate::domain::NotificationStatus::Pending
                        | crate::domain::NotificationStatus::Failed
                )
        })
        .cloned()
        .collect::<Vec<_>>();
    deliveries.sort_by_key(|delivery| std::cmp::Reverse(delivery.queued_at));
    Ok(Json(deliveries))
}

async fn dispatch_notifications(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<DispatchNotificationsInput>,
) -> ApiResult<Json<NotificationDispatchSummary>> {
    let actor = {
        let app = state.app.lock().await;
        let principal = authenticate(&app, &headers)?;
        app.authorize_notification_admin(&principal.id)?;
        principal.name
    };
    Ok(Json(
        deliver_notification_batch(&state.app, input.limit, input.force, &actor).await?,
    ))
}

async fn flow_messages(
    State(state): State<ApiState>,
    Path(flow_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<FlowMessage>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.flow_messages(&flow_id, &principal.id)?))
}

async fn post_message(
    State(state): State<ApiState>,
    Path(flow_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<PostMessageInput>,
) -> ApiResult<Json<FlowMessage>> {
    mutate(&state, &headers, |app, actor| {
        app.post_flow_message(
            &flow_id,
            input.task_id.as_deref(),
            actor,
            input.kind,
            &input.recipients,
            &input.body,
            input.requires_ack,
        )
    })
    .await
}

async fn flow_changes(
    State(state): State<ApiState>,
    Path(flow_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<FlowChangeRequest>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.flow_changes(&flow_id, &principal.id)?))
}

async fn propose_flow_change(
    State(state): State<ApiState>,
    Path(flow_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<ProposeFlowChangeInput>,
) -> ApiResult<Json<FlowChangeRequest>> {
    let workspace = std::env::current_dir().map_err(RelayError::from)?;
    let (data_dir, principal_id, tenant_id) = {
        let app = state.app.lock().await;
        let principal = authenticate(&app, &headers)?;
        (
            app.data_dir().to_path_buf(),
            principal.id,
            app.state().tenant()?.id.clone(),
        )
    };
    let mut planning_app = RelayApp::open_for_side_task(data_dir, &tenant_id)?;
    let change = planning_app
        .propose_flow_change(
            &flow_id,
            &principal_id,
            &input.summary,
            PlannerKind::Local,
            &workspace,
            30,
        )
        .await?;
    state.app.lock().await.reload()?;
    Ok(Json(change))
}

async fn approve_flow_change(
    State(state): State<ApiState>,
    Path(request_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<FlowChangeRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.approve_flow_change(&request_id, actor)
    })
    .await
}

async fn reject_flow_change(
    State(state): State<ApiState>,
    Path(request_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<RejectFlowChangeInput>,
) -> ApiResult<Json<FlowChangeRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.reject_flow_change(&request_id, actor, &input.reason)
    })
    .await
}

async fn acknowledge_message(
    State(state): State<ApiState>,
    Path(message_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<FlowMessage>> {
    mutate(&state, &headers, |app, actor| {
        app.acknowledge_flow_message(&message_id, actor)
    })
    .await
}

async fn escalations(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<TrackingEscalation>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(
        app.escalation_inbox(&principal.id, false)?
            .into_iter()
            .cloned()
            .collect(),
    ))
}

async fn approve_flow(
    State(state): State<ApiState>,
    Path(flow_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Flow>> {
    mutate(&state, &headers, |app, actor| {
        app.approve_flow(&flow_id, actor)
    })
    .await
}

async fn accept_task(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Task>> {
    mutate(&state, &headers, |app, actor| {
        app.accept_task(&task_id, actor)
    })
    .await
}

async fn start_task(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Task>> {
    mutate(&state, &headers, |app, actor| {
        app.start_task(&task_id, actor)
    })
    .await
}

async fn heartbeat_task(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<HeartbeatInput>,
) -> ApiResult<Json<Task>> {
    mutate(&state, &headers, |app, actor| {
        app.heartbeat_task(&task_id, actor, input.note)
    })
    .await
}

async fn negotiate_task(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<NegotiateInput>,
) -> ApiResult<Json<Task>> {
    mutate(&state, &headers, |app, actor| {
        app.negotiate_task(&task_id, actor, input.effort_hours)
    })
    .await
}

async fn reassignment_candidates(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<AssignmentTarget>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.reassignment_candidates(&task_id, &principal.id)?))
}

async fn reassign_task(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<ReassignInput>,
) -> ApiResult<Json<Flow>> {
    mutate(&state, &headers, |app, actor| {
        app.reassign_task(
            &task_id,
            actor,
            &input.owner,
            &input.copilots,
            &input.reason,
        )
    })
    .await
}

async fn block_task(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<BlockInput>,
) -> ApiResult<Json<Task>> {
    mutate(&state, &headers, |app, actor| {
        app.block_task(&task_id, actor, &input.reason)
    })
    .await
}

async fn add_evidence(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<EvidenceInput>,
) -> ApiResult<Json<Evidence>> {
    mutate(&state, &headers, |app, actor| {
        app.add_evidence(&task_id, actor, &input.kind, &input.uri, &input.summary)
    })
    .await
}

async fn authorize_flight(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<AuthorizeFlightInput>,
) -> ApiResult<Json<FlightLease>> {
    mutate(&state, &headers, |app, actor| {
        app.authorize_remote_flight_with_manifest(
            &task_id,
            actor,
            &input.agent,
            input.executor,
            input.ttl_seconds,
            input.manifest,
        )
    })
    .await
}

async fn flight_leases(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<FlightLease>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.remote_flight_leases(
        &principal.id,
        principal.kind == PrincipalKind::Human,
    )?))
}

async fn claim_flight(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<ClaimFlightInput>,
) -> ApiResult<Json<FlightLease>> {
    mutate(&state, &headers, |app, actor| {
        app.claim_remote_flight(&lease_id, actor, &input.run_id)
    })
    .await
}

async fn finish_flight(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<FinishFlightInput>,
) -> ApiResult<Json<FlightLease>> {
    mutate(&state, &headers, |app, actor| {
        app.finish_remote_flight(&lease_id, actor, input.landed, input.report)
    })
    .await
}

async fn stage_flight_artifact(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    Query(query): Query<StageArtifactQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<crate::domain::StagedArtifact>> {
    let media_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_string();
    let mut app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.stage_flight_artifact(
        &lease_id,
        &query.path,
        &media_type,
        body.to_vec(),
        &principal.id,
    )?))
}

async fn flight_artifacts(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<crate::domain::StagedArtifact>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.flight_artifacts(&lease_id, &principal.id)?))
}

async fn artifact_content(
    State(state): State<ApiState>,
    Path(artifact_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    let (artifact, content) = app.artifact_content(&artifact_id, &principal.id)?;
    let filename = artifact
        .path
        .rsplit('/')
        .next()
        .unwrap_or("artifact")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    Ok((
        [
            (header::CONTENT_TYPE, artifact.media_type),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        content,
    )
        .into_response())
}

async fn office_releases(
    State(state): State<ApiState>,
    Query(query): Query<OfficeReleasesQuery>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<OfficeReleaseRequest>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.office_releases(
        &principal.id,
        query.flow_id.as_deref(),
    )?))
}

async fn request_office_release(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<RequestOfficeReleaseInput>,
) -> ApiResult<Json<OfficeReleaseRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.request_office_release(&input.task_id, input.provider, input.payload, actor)
    })
    .await
}

async fn approve_office_release(
    State(state): State<ApiState>,
    Path(release_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<OfficeReleaseRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.approve_office_release(&release_id, actor)
    })
    .await
}

async fn reject_office_release(
    State(state): State<ApiState>,
    Path(release_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<RejectOfficeReleaseInput>,
) -> ApiResult<Json<OfficeReleaseRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.reject_office_release(&release_id, &input.reason, actor)
    })
    .await
}

async fn retry_office_release(
    State(state): State<ApiState>,
    Path(release_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<OfficeReleaseRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.retry_office_release(&release_id, actor)
    })
    .await
}

async fn dispatch_office_release(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Option<OfficeReleaseRequest>>> {
    {
        let app = state.app.lock().await;
        let principal = authenticate(&app, &headers)?;
        app.authorize_notification_admin(&principal.id)?;
    }
    Ok(Json(
        dispatch_office_once(&state.app, &state.office_bridge, &state.tenant_id).await?,
    ))
}

async fn dispatch_office_once(
    app: &Arc<Mutex<RelayApp>>,
    bridge: &OfficeBridge,
    tenant_id: &str,
) -> Result<Option<OfficeReleaseRequest>> {
    let release = {
        let mut app = app.lock().await;
        app.claim_office_release()?
    };
    let Some(release) = release else {
        return Ok(None);
    };
    let dispatch_id = release
        .dispatch_id
        .as_deref()
        .ok_or_else(|| RelayError::Validation("claimed Office release has no dispatch ID".into()))?
        .to_string();
    let artifact = if let Some(artifact_id) = release.payload.artifact_id() {
        match app
            .lock()
            .await
            .artifact_content(artifact_id, &release.requested_by)
        {
            Ok((_, content)) => Some(content),
            Err(error) => {
                let outcome = Err((error.to_string(), false));
                return finish_office_dispatch(app, &release.id, &dispatch_id, outcome)
                    .await
                    .map(Some);
            }
        }
    } else {
        None
    };
    let outcome = bridge
        .dispatch(tenant_id, &release, artifact.as_deref())
        .await
        .map_err(|error| (error.message, error.indeterminate));
    finish_office_dispatch(app, &release.id, &dispatch_id, outcome)
        .await
        .map(Some)
}

async fn finish_office_dispatch(
    app: &Arc<Mutex<RelayApp>>,
    release_id: &str,
    dispatch_id: &str,
    outcome: std::result::Result<crate::domain::OfficeReleaseResult, (String, bool)>,
) -> Result<OfficeReleaseRequest> {
    let mut app = app.lock().await;
    match app.finish_office_release(release_id, dispatch_id, outcome.clone()) {
        Err(RelayError::ConcurrentModification { .. }) => {
            app.finish_office_release(release_id, dispatch_id, outcome)
        }
        result => result,
    }
}

async fn gitlab_writes(
    State(state): State<ApiState>,
    Query(query): Query<GitLabWritesQuery>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<GitLabWriteRequest>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(
        app.gitlab_writes(&principal.id, query.flow_id.as_deref())?,
    ))
}

async fn request_gitlab_write(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(input): Json<RequestGitLabWriteInput>,
) -> ApiResult<Json<GitLabWriteRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.request_gitlab_write(&input.task_id, input.payload, actor)
    })
    .await
}

async fn approve_gitlab_write(
    State(state): State<ApiState>,
    Path(write_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<GitLabWriteRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.approve_gitlab_write(&write_id, actor)
    })
    .await
}

async fn reject_gitlab_write(
    State(state): State<ApiState>,
    Path(write_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<RejectGitLabWriteInput>,
) -> ApiResult<Json<GitLabWriteRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.reject_gitlab_write(&write_id, &input.reason, actor)
    })
    .await
}

async fn retry_gitlab_write(
    State(state): State<ApiState>,
    Path(write_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<GitLabWriteRequest>> {
    mutate(&state, &headers, |app, actor| {
        app.retry_gitlab_write(&write_id, actor)
    })
    .await
}

async fn dispatch_gitlab_write(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Option<GitLabWriteRequest>>> {
    {
        let app = state.app.lock().await;
        let principal = authenticate(&app, &headers)?;
        app.authorize_notification_admin(&principal.id)?;
    }
    Ok(Json(
        dispatch_gitlab_write_once(&state.app, &state.gitlab_write_bridge, &state.tenant_id)
            .await?,
    ))
}

async fn dispatch_gitlab_write_once(
    app: &Arc<Mutex<RelayApp>>,
    bridge: &GitLabWriteBridge,
    tenant_id: &str,
) -> Result<Option<GitLabWriteRequest>> {
    let request = {
        let mut app = app.lock().await;
        app.claim_gitlab_write()?
    };
    let Some(request) = request else {
        return Ok(None);
    };
    let dispatch_id = request
        .dispatch_id
        .as_deref()
        .ok_or_else(|| RelayError::Validation("claimed GitLab write has no dispatch ID".into()))?
        .to_string();
    let outcome = bridge
        .dispatch(tenant_id, &request)
        .await
        .map_err(|error| (error.message, error.indeterminate));
    finish_gitlab_dispatch(app, &request.id, &dispatch_id, outcome)
        .await
        .map(Some)
}

async fn finish_gitlab_dispatch(
    app: &Arc<Mutex<RelayApp>>,
    write_id: &str,
    dispatch_id: &str,
    outcome: std::result::Result<crate::domain::GitLabWriteResult, (String, bool)>,
) -> Result<GitLabWriteRequest> {
    let mut app = app.lock().await;
    match app.finish_gitlab_write(write_id, dispatch_id, outcome.clone()) {
        Err(RelayError::ConcurrentModification { .. }) => {
            app.finish_gitlab_write(write_id, dispatch_id, outcome)
        }
        result => result,
    }
}

async fn revoke_flight(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<FlightLease>> {
    mutate(&state, &headers, |app, actor| {
        app.revoke_remote_flight(&lease_id, actor)
    })
    .await
}

async fn flight_recovery_options(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<RecoveryAction>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    Ok(Json(app.recovery_options(&lease_id, &principal.name)?))
}

async fn recover_flight(
    State(state): State<ApiState>,
    Path(lease_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<RecoverFlightInput>,
) -> ApiResult<Json<Option<FlightLease>>> {
    mutate(&state, &headers, |app, actor| {
        app.recover_remote_flight(
            &lease_id,
            actor,
            input.action,
            &input.reason,
            input.executor,
            input.objective,
            input.ttl_seconds,
        )
    })
    .await
}

async fn submit_task(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Task>> {
    mutate(&state, &headers, |app, actor| {
        app.submit_task(&task_id, actor)
    })
    .await
}

async fn complete_task(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Task>> {
    mutate(&state, &headers, |app, actor| {
        app.complete_task(&task_id, actor)
    })
    .await
}

async fn ack_escalation(
    State(state): State<ApiState>,
    Path(escalation_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<TrackingEscalation>> {
    mutate(&state, &headers, |app, actor| {
        app.acknowledge_escalation(&escalation_id, actor)
    })
    .await
}

async fn bridge_interaction(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<crate::domain::ExternalInteractionResult>> {
    let provider = required_webhook_header(&headers, "x-relay-provider")?;
    let delivery_id = required_webhook_header(&headers, "x-relay-delivery-id")?;
    let timestamp = required_webhook_header(&headers, "x-relay-timestamp")?;
    let signature = required_webhook_header(&headers, "x-relay-signature")?;
    state.interaction_auth.verify_bridge(
        provider,
        delivery_id,
        timestamp,
        signature,
        &body,
        Utc::now(),
    )?;
    let input: ExternalInteractionInput = serde_json::from_slice(&body)
        .map_err(|_| RelayError::Validation("invalid interaction Bridge payload".into()))?;
    let mut app = state.app.lock().await;
    Ok(Json(app.process_external_interaction(
        provider,
        delivery_id,
        &input.external_user_id,
        input.action,
        &input.target_id,
        input.reason.as_deref(),
    )?))
}

async fn slack_interaction(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<crate::domain::ExternalInteractionResult>> {
    let timestamp = required_webhook_header(&headers, "x-slack-request-timestamp")?;
    let signature = required_webhook_header(&headers, "x-slack-signature")?;
    state
        .interaction_auth
        .verify_slack(timestamp, signature, &body, Utc::now())?;
    let delivery_id = slack_delivery_id(timestamp, &body);
    let input = parse_slack_interaction(&body)?;
    let mut app = state.app.lock().await;
    Ok(Json(app.process_external_interaction(
        "slack",
        &delivery_id,
        &input.external_user_id,
        input.action,
        &input.target_id,
        input.reason.as_deref(),
    )?))
}

async fn gitlab_webhook(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<GitLabWebhookResponse>> {
    let auth = state.gitlab_webhook_auth.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::NOT_FOUND,
        message: "GitLab webhook is not configured".into(),
    })?;
    let message_id = webhook_header(&headers, "webhook-id")
        .or_else(|| webhook_header(&headers, "idempotency-key"));
    let verification = auth
        .verify(
            webhook_header(&headers, "webhook-signature"),
            message_id,
            webhook_header(&headers, "webhook-timestamp"),
            webhook_header(&headers, "x-gitlab-token"),
            message_id.or_else(|| webhook_header(&headers, "x-gitlab-event-uuid")),
            &body,
            chrono::Utc::now(),
        )
        .map_err(|_| ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid GitLab webhook authentication".into(),
        })?;
    let event = parse_webhook_event(&body, verification.occurred_at)?;
    let update = match event {
        GitLabWebhookEvent::Update(update) => *update,
        GitLabWebhookEvent::Ignored { object_kind } => {
            return Ok(Json(GitLabWebhookResponse {
                status: "ignored",
                event: object_kind,
                matched_tasks: 0,
                changed_tasks: 0,
            }));
        }
    };
    let expected_header = match update.event_kind {
        "merge_request" => "Merge Request Hook",
        "pipeline" => "Pipeline Hook",
        _ => unreachable!(),
    };
    if !webhook_header(&headers, "x-gitlab-event")
        .is_some_and(|value| value.eq_ignore_ascii_case(expected_header))
    {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "GitLab webhook event header does not match its payload".into(),
        });
    }
    let actor = format!("connector://gitlab/webhook/{}", verification.delivery_id);
    let mut app = state.app.lock().await;
    let result = app.sync_bound_external_artifact(
        "gitlab",
        &verification.delivery_id,
        "merge_request",
        &update.project,
        &update.merge_request_iid,
        verification.occurred_at,
        update.artifact,
        &actor,
    )?;
    let status = if result.duplicate {
        "duplicate"
    } else if result.stale {
        "stale"
    } else if result.matched_tasks == 0 {
        "unbound"
    } else if result.changed_tasks == 0 {
        "unchanged"
    } else {
        "accepted"
    };
    Ok(Json(GitLabWebhookResponse {
        status,
        event: update.event_kind.to_string(),
        matched_tasks: result.matched_tasks,
        changed_tasks: result.changed_tasks,
    }))
}

async fn mutate<T>(
    state: &ApiState,
    headers: &HeaderMap,
    action: impl FnOnce(&mut RelayApp, &str) -> Result<T>,
) -> ApiResult<Json<T>> {
    let mut app = state.app.lock().await;
    let principal = authenticate(&app, headers)?;
    Ok(Json(action(&mut app, &principal.name)?))
}

async fn deliver_notification_batch(
    app: &Arc<Mutex<RelayApp>>,
    limit: usize,
    force_failed: bool,
    actor: &str,
) -> Result<NotificationDispatchSummary> {
    if limit == 0 || limit > 1_000 {
        return Err(RelayError::Validation(
            "notification dispatch limit must be between 1 and 1000".into(),
        ));
    }
    let attempts = app.lock().await.notification_attempts(limit, force_failed);
    let mut summary = NotificationDispatchSummary::default();
    for (endpoint, delivery) in attempts {
        let attempt = crate::notification::deliver(&endpoint, &delivery).await;
        summary.attempted += 1;
        if attempt.delivered {
            summary.delivered += 1;
        } else {
            summary.failed += 1;
        }
        app.lock()
            .await
            .record_notification_attempt(&delivery.id, attempt, actor)?;
    }
    Ok(summary)
}

fn authenticate(app: &RelayApp, headers: &HeaderMap) -> ApiResult<Principal> {
    let token = bearer_token(headers)
        .or_else(|| cookie_value(headers, "relay_session"))
        .ok_or_else(ApiError::unauthorized)?;
    app.authenticate_api_token(token)?
        .ok_or_else(ApiError::unauthorized)
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .find_map(|value| {
            let (key, value) = value.split_once('=')?;
            (key == name && !value.is_empty()).then_some(value)
        })
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

fn webhook_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn required_webhook_header<'a>(headers: &'a HeaderMap, name: &str) -> ApiResult<&'a str> {
    webhook_header(headers, name).ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        message: format!("missing {name} header"),
    })
}

fn default_lease_ttl_seconds() -> u64 {
    3_600
}

fn default_credential_ttl_days() -> u32 {
    30
}

fn default_oidc_return_to() -> String {
    "/console".into()
}

fn default_capacity_percent() -> u8 {
    100
}

fn default_planner_kind() -> PlannerKind {
    PlannerKind::Local
}

fn default_planner_timeout_seconds() -> u64 {
    300
}

fn default_requires_ack() -> bool {
    true
}

fn default_notification_dispatch_limit() -> usize {
    50
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use chrono::{Duration as ChronoDuration, Utc};
    use hmac::{Hmac, KeyInit, Mac};
    use serde_json::json;
    use sha2::Sha256;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::*;
    use crate::domain::PrincipalKind;
    use crate::planner::PlannerKind;

    type TestHmacSha256 = Hmac<Sha256>;

    #[test]
    fn rate_limiter_resets_after_its_fixed_window() {
        let limiter = RateLimitState::default();
        let key = [9; 32];
        let now = Instant::now();
        for _ in 0..RateLimitState::LIMIT_PER_MINUTE {
            assert!(limiter.allow(key, now));
        }
        assert!(!limiter.allow(key, now));
        assert!(limiter.allow(key, now + Duration::from_secs(61)));
    }

    #[tokio::test]
    async fn fleet_routes_tokens_to_isolated_tenant_ledgers() {
        let directory = tempdir().unwrap();
        let (first_app, first_token, first_tenant) =
            tenant_app(directory.path().join("first"), "First Company");
        let (second_app, second_token, second_tenant) =
            tenant_app(directory.path().join("second"), "Second Company");
        let mut routers = BTreeMap::new();
        routers.insert(
            first_tenant.clone(),
            router(
                Arc::new(Mutex::new(first_app)),
                None,
                InteractionWebhookAuth::default(),
            ),
        );
        routers.insert(
            second_tenant.clone(),
            router(
                Arc::new(Mutex::new(second_app)),
                None,
                InteractionWebhookAuth::default(),
            ),
        );
        let fleet = Router::new()
            .fallback(fleet_dispatch)
            .with_state(FleetState {
                routers: Arc::new(routers),
                default_tenant_id: first_tenant.clone(),
            });

        let first = fleet
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/organization",
                &first_token,
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&first_body).unwrap()["organization"]["name"],
            "First Company"
        );

        let second = fleet
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/organization",
                &second_token,
            ))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&second_body).unwrap()["organization"]["name"],
            "Second Company"
        );

        let conflict = Request::builder()
            .uri("/api/v1/organization")
            .header(header::AUTHORIZATION, format!("Bearer {first_token}"))
            .header("x-relay-tenant", second_tenant)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            fleet.oneshot(conflict).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn scim_provisioning_is_authenticated_and_deactivation_revokes_sessions() {
        let directory = tempdir().unwrap();
        let mut app = RelayApp::open(directory.path()).unwrap();
        app.init_organization("Relay", "admin").unwrap();
        let shared = Arc::new(Mutex::new(app));
        let scim_token = "scim-test-token-with-more-than-thirty-two-characters";
        let service = router_with_identity(
            shared.clone(),
            ServerIntegrations {
                gitlab_webhook_auth: None,
                interaction_auth: InteractionWebhookAuth::default(),
                oidc: None,
                feishu: None,
                scim_auth: ScimAuthenticator::new(scim_token).unwrap(),
                office_bridge: OfficeBridge::disabled(),
                gitlab_write_bridge: GitLabWriteBridge::disabled(),
            },
        );
        let user_body = json!({
            "schemas": [crate::scim::USER_SCHEMA],
            "userName": "pilot@example.com",
            "displayName": "Pilot",
            "externalId": "subject-42",
            "active": true
        });
        let unauthorized = service
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/scim/v2/Users")
                    .header(header::CONTENT_TYPE, "application/scim+json")
                    .body(Body::from(serde_json::to_vec(&user_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let created = service
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/scim/v2/Users")
                    .header(header::AUTHORIZATION, format!("Bearer {scim_token}"))
                    .header(header::CONTENT_TYPE, "application/scim+json")
                    .body(Body::from(serde_json::to_vec(&user_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        assert_eq!(
            created.headers()[header::CONTENT_TYPE],
            "application/scim+json"
        );
        let body = to_bytes(created.into_body(), usize::MAX).await.unwrap();
        let user = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        let user_id = user["id"].as_str().unwrap();
        let session = {
            let mut app = shared.lock().await;
            let principal = app.oidc_principal("subject-42").unwrap();
            app.issue_oidc_session(&principal.id).unwrap()
        };
        let me = service
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/me")
                    .header(header::COOKIE, format!("relay_session={}", session.token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(me.status(), StatusCode::OK);

        let disabled = service
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/scim/v2/Users/{user_id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {scim_token}"))
                    .header(header::CONTENT_TYPE, "application/scim+json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "schemas": [crate::scim::PATCH_SCHEMA],
                            "Operations": [{"op": "replace", "path": "active", "value": false}]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disabled.status(), StatusCode::OK);
        let denied = service
            .oneshot(
                Request::builder()
                    .uri("/api/v1/me")
                    .header(header::COOKIE, format!("relay_session={}", session.token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    }

    fn tenant_app(data_dir: std::path::PathBuf, name: &str) -> (RelayApp, String, String) {
        let mut app = RelayApp::open(data_dir).unwrap();
        app.init_organization(name, "admin").unwrap();
        let team = app
            .create_team("Operations", "operations", "admin")
            .unwrap();
        let admin = app
            .register_principal(
                "Admin",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        let token = app
            .issue_api_credential(&admin.id, "fleet test", "admin")
            .unwrap()
            .token;
        let tenant_id = app.state().tenant().unwrap().id.clone();
        (app, token, tenant_id)
    }

    #[tokio::test]
    async fn web_console_assets_are_embedded_and_security_hardened() {
        let directory = tempdir().unwrap();
        let app = RelayApp::open(directory.path().join("data")).unwrap();
        let service = router(
            Arc::new(Mutex::new(app)),
            None,
            InteractionWebhookAuth::default(),
        );

        let not_ready = service
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);

        // 直接打开域名根路径的人最多，别让他们撞上 404。
        for uri in ["/"] {
            let landing = service
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(landing.status(), StatusCode::TEMPORARY_REDIRECT);
            assert_eq!(landing.headers()[header::LOCATION], "/console");
        }

        let page = service
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/console")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(page.status(), StatusCode::OK);
        assert!(page.headers().contains_key(header::CONTENT_SECURITY_POLICY));
        assert!(page.headers().contains_key("x-request-id"));
        let body = to_bytes(page.into_body(), usize::MAX).await.unwrap();
        // 断言页面骨架而不是某句文案——文案会随可读性调整而变。
        let page_html = String::from_utf8_lossy(&body);
        // 断言 SPA 的挂载点与入口脚本，不断言具体文案——前端产物是构建出来的。
        assert!(page_html.contains("/console/assets/app.js"));
        assert!(page_html.contains("id=\"root\""));

        let script = service
            .oneshot(
                Request::builder()
                    .uri("/console/assets/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(script.status(), StatusCode::OK);
        assert_eq!(
            script.headers()[header::CONTENT_TYPE],
            "text/javascript; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn readiness_metrics_and_public_http_guard_enforce_operational_boundaries() {
        let directory = tempdir().unwrap();
        let mut app = RelayApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Operations", "operations", "admin")
            .unwrap();
        let admin = app
            .register_principal(
                "Operator",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        let token = app
            .issue_api_credential(&admin.id, "metrics", "admin")
            .unwrap()
            .token;
        let service = router(
            Arc::new(Mutex::new(app)),
            None,
            InteractionWebhookAuth::default(),
        );

        let ready = service
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::OK);
        let denied = service
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let metrics = service
            .oneshot(authenticated_request("GET", "/metrics", &token))
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::OK);
        let body = to_bytes(metrics.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("relay_ledger_events"));

        let unopened = RelayApp::open(directory.path().join("public-http")).unwrap();
        let error = run(
            unopened,
            ServerOptions {
                bind: "0.0.0.0:0".parse().unwrap(),
                allow_insecure_public_http: false,
                tracker_interval_seconds: 30,
                stale_after_hours: 24,
                escalate_after_hours: 4,
                notification_interval_seconds: 15,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            RelayError::Validation(message) if message.contains("non-loopback plain HTTP")
        ));
    }

    #[tokio::test]
    async fn tenant_admin_can_manage_roles_and_manager_can_create_remote_demand() {
        let directory = tempdir().unwrap();
        let mut app = RelayApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Platform", "product,rust", "admin")
            .unwrap();
        let admin = app
            .register_principal(
                "Admin",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product",
                100,
                None,
                "admin",
            )
            .unwrap();
        let member = app
            .register_principal(
                "Engineer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "rust",
                100,
                None,
                &admin.id,
            )
            .unwrap();
        let admin_token = app
            .issue_api_credential(&admin.id, "admin api", &admin.id)
            .unwrap();
        let member_token = app
            .issue_api_credential(&member.id, "member api", &admin.id)
            .unwrap();
        let app = Arc::new(Mutex::new(app));
        let service = router(
            app.clone(),
            None,
            InteractionWebhookAuth::for_test(None, None),
        );

        let organization = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/organization",
                &member_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(organization.status(), StatusCode::OK);
        let denied = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                "/api/v1/demands",
                &member_token.token,
                json!({"summary": "Build an internal gateway"}),
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let granted = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/principals/{}/roles", member.id),
                &admin_token.token,
                json!({"role": "manager"}),
            ))
            .await
            .unwrap();
        assert_eq!(granted.status(), StatusCode::OK);
        let body = to_bytes(granted.into_body(), usize::MAX).await.unwrap();
        let binding: RoleBinding = serde_json::from_slice(&body).unwrap();
        assert_eq!(binding.role, OrganizationRole::Manager);

        let created = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                "/api/v1/demands",
                &member_token.token,
                json!({"summary": "Build an internal gateway"}),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let body = to_bytes(created.into_body(), usize::MAX).await.unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // 本地拆解直接出 Flow；需要模型的拆解会走 planning_request 那一支。
        let flow: Flow = serde_json::from_value(envelope["flow"].clone()).unwrap();
        assert!(envelope["planning_request"].is_null());
        assert_eq!(flow.demand.requester, member.name);
        assert!(app.lock().await.state().flows.contains_key(&flow.id));

        let dashboard = service
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/dashboard",
                &member_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(dashboard.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn signed_bridge_and_slack_actions_use_bound_human_identity() {
        let directory = tempdir().unwrap();
        let mut app = RelayApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Delivery", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "Leader",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        app.bind_external_identity("feishu", "ou_leader", &human.id, "admin")
            .unwrap();
        app.bind_external_identity("slack", "U_LEADER", &human.id, "admin")
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare a launch brief",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
                None,
            )
            .await
            .unwrap();
        app.approve_flow(&flow.id, &human.name).unwrap();
        let task_id = flow.tasks[0].id.clone();
        let message = app
            .post_flow_message(
                &flow.id,
                Some(&task_id),
                &human.name,
                FlowMessageKind::Command,
                std::slice::from_ref(&human.name),
                "Confirm the release window",
                true,
            )
            .unwrap();
        let app = Arc::new(Mutex::new(app));
        let bridge_secret = b"bridge-test-secret";
        let slack_secret = b"slack-test-secret";
        let service = router(
            app.clone(),
            None,
            InteractionWebhookAuth::for_test(Some(bridge_secret), Some(slack_secret)),
        );
        let timestamp = Utc::now().timestamp();
        let bridge_body = serde_json::to_vec(&json!({
            "external_user_id": "ou_leader",
            "action": "task.accept",
            "target_id": task_id
        }))
        .unwrap();
        let accepted = service
            .clone()
            .oneshot(bridge_interaction_request(
                "feishu",
                "feishu-delivery-1",
                timestamp,
                bridge_secret,
                &bridge_body,
            ))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let body = to_bytes(accepted.into_body(), usize::MAX).await.unwrap();
        let accepted: crate::domain::ExternalInteractionResult =
            serde_json::from_slice(&body).unwrap();
        assert!(!accepted.duplicate);
        assert_eq!(accepted.receipt.principal_id, human.id);
        let sequence = app.lock().await.state().last_sequence;

        let duplicate = service
            .clone()
            .oneshot(bridge_interaction_request(
                "feishu",
                "feishu-delivery-1",
                timestamp,
                bridge_secret,
                &bridge_body,
            ))
            .await
            .unwrap();
        let body = to_bytes(duplicate.into_body(), usize::MAX).await.unwrap();
        let duplicate: crate::domain::ExternalInteractionResult =
            serde_json::from_slice(&body).unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(app.lock().await.state().last_sequence, sequence);

        let slack_payload = json!({
            "type": "block_actions",
            "user": {"id": "U_LEADER"},
            "actions": [{
                "action_id": "relay.message.ack",
                "value": message.id
            }]
        });
        let slack_body = serde_urlencoded::to_string([("payload", slack_payload.to_string())])
            .unwrap()
            .into_bytes();
        let acknowledged = service
            .clone()
            .oneshot(slack_interaction_request(
                timestamp,
                slack_secret,
                &slack_body,
            ))
            .await
            .unwrap();
        assert_eq!(acknowledged.status(), StatusCode::OK);
        assert!(
            app.lock().await.state().messages[&message.id].recipient_is_acknowledged(&human.id)
        );
        assert_eq!(
            app.lock()
                .await
                .state()
                .find_task(&task_id)
                .unwrap()
                .1
                .status,
            crate::domain::TaskStatus::Accepted
        );

        let invalid = service
            .oneshot(slack_interaction_request(
                timestamp,
                b"wrong-secret",
                &slack_body,
            ))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::FORBIDDEN);
        assert_eq!(app.lock().await.state().external_interactions.len(), 2);
    }

    #[tokio::test]
    async fn bearer_identity_drives_remote_inbox_and_task_actions() {
        let directory = tempdir().unwrap();
        let mut app = RelayApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Delivery", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "Leader",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare a launch brief",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
                None,
            )
            .await
            .unwrap();
        let observer = app
            .register_principal(
                "Observer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery,operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        app.approve_flow(&flow.id, &human.name).unwrap();
        let task_id = flow.tasks[0].id.clone();
        let issued = app
            .issue_api_credential(&human.id, "test client", "admin")
            .unwrap();
        let observer_token = app
            .issue_api_credential(&observer.id, "observer client", "admin")
            .unwrap();
        assert!(
            !serde_json::to_string(&app.state().credentials)
                .unwrap()
                .contains(&issued.token)
        );

        let app = Arc::new(Mutex::new(app));
        let service = router(app.clone(), None, InteractionWebhookAuth::default());
        let unauthorized = service
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/me")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let inbox = service
            .clone()
            .oneshot(authenticated_request("GET", "/api/v1/inbox", &issued.token))
            .await
            .unwrap();
        assert_eq!(inbox.status(), StatusCode::OK);
        let body = to_bytes(inbox.into_body(), usize::MAX).await.unwrap();
        let items: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(items.as_array().unwrap().len(), 3);

        let configured = service
            .clone()
            .oneshot(authenticated_json_request(
                "PUT",
                "/api/v1/me/calendar",
                &issued.token,
                json!({
                    "utc_offset_minutes": 480,
                    "working_days": ["monday", "tuesday", "wednesday", "thursday", "friday"],
                    "day_start_minute": 540,
                    "day_end_minute": 1080
                }),
            ))
            .await
            .unwrap();
        assert_eq!(configured.status(), StatusCode::OK);
        let body = to_bytes(configured.into_body(), usize::MAX).await.unwrap();
        let calendar: WorkCalendar = serde_json::from_slice(&body).unwrap();
        assert_eq!(calendar.principal_id, human.id);
        assert_eq!(calendar.utc_offset_minutes, 480);
        assert_eq!(
            app.lock()
                .await
                .state()
                .work_calendar(&observer.id)
                .unwrap()
                .utc_offset_minutes,
            0
        );

        let starts_at = Utc::now();
        let ends_at = starts_at + chrono::Duration::days(2);
        let added = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                "/api/v1/me/time-off",
                &issued.token,
                json!({
                    "starts_at": starts_at,
                    "ends_at": ends_at,
                    "reason": "customer onsite"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(added.status(), StatusCode::OK);
        let body = to_bytes(added.into_body(), usize::MAX).await.unwrap();
        let block: AvailabilityBlock = serde_json::from_slice(&body).unwrap();
        assert_eq!(block.principal_id, human.id);
        let cancelled = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/me/time-off/{}/cancel", block.id),
                &issued.token,
            ))
            .await
            .unwrap();
        assert_eq!(cancelled.status(), StatusCode::OK);

        let accepted = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/tasks/{task_id}/accept"),
                &observer_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::FORBIDDEN);

        let accepted = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/tasks/{task_id}/accept"),
                &issued.token,
            ))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        assert_eq!(
            app.lock()
                .await
                .state()
                .find_task(&task_id)
                .unwrap()
                .1
                .status,
            crate::domain::TaskStatus::Accepted
        );

        let sent = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/flows/{}/messages", flow.id),
                &issued.token,
                json!({
                    "task_id": task_id,
                    "kind": "command",
                    "recipients": [observer.name],
                    "body": "Confirm the release window",
                    "requires_ack": true
                }),
            ))
            .await
            .unwrap();
        assert_eq!(sent.status(), StatusCode::OK);
        let body = to_bytes(sent.into_body(), usize::MAX).await.unwrap();
        let message: FlowMessage = serde_json::from_slice(&body).unwrap();

        let messages = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/messages",
                &observer_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(messages.status(), StatusCode::OK);
        let body = to_bytes(messages.into_body(), usize::MAX).await.unwrap();
        let messages: Vec<MessageInboxItem> = serde_json::from_slice(&body).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].needs_acknowledgement());

        let acknowledged = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/messages/{}/ack", message.id),
                &observer_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(acknowledged.status(), StatusCode::OK);
        let messages = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/messages",
                &observer_token.token,
            ))
            .await
            .unwrap();
        let body = to_bytes(messages.into_body(), usize::MAX).await.unwrap();
        let messages: Vec<MessageInboxItem> = serde_json::from_slice(&body).unwrap();
        assert!(messages.is_empty());

        let candidates = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                &format!("/api/v1/tasks/{task_id}/reassignment-candidates"),
                &issued.token,
            ))
            .await
            .unwrap();
        assert_eq!(candidates.status(), StatusCode::OK);
        let body = to_bytes(candidates.into_body(), usize::MAX).await.unwrap();
        let candidates: Vec<AssignmentTarget> = serde_json::from_slice(&body).unwrap();
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.id == observer.id)
        );

        let reassigned = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/tasks/{task_id}/reassign"),
                &issued.token,
                json!({
                    "owner": observer.id,
                    "reason": "Manager moved the delivery window",
                }),
            ))
            .await
            .unwrap();
        assert_eq!(reassigned.status(), StatusCode::OK);
        let accepted = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/tasks/{task_id}/accept"),
                &observer_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let negotiated = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/tasks/{task_id}/negotiate"),
                &observer_token.token,
                json!({"effort_hours": 12.0}),
            ))
            .await
            .unwrap();
        assert_eq!(negotiated.status(), StatusCode::OK);
        assert_eq!(
            app.lock()
                .await
                .state()
                .find_task(&task_id)
                .unwrap()
                .1
                .estimate
                .effort_hours,
            12.0
        );

        let started = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/tasks/{task_id}/start"),
                &observer_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(started.status(), StatusCode::OK);
        let requested = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                "/api/v1/office/releases",
                &observer_token.token,
                json!({
                    "task_id": task_id,
                    "provider": "microsoft365",
                    "payload": {
                        "kind": "send_email",
                        "account_id": "observer@example.com",
                        "to": ["team@example.com"],
                        "subject": "Launch update",
                        "body": "The launch brief is ready for review.",
                        "body_type": "text"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(requested.status(), StatusCode::OK);
        let body = to_bytes(requested.into_body(), usize::MAX).await.unwrap();
        let release: OfficeReleaseRequest = serde_json::from_slice(&body).unwrap();
        let denied = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/office/releases/{}/approve", release.id),
                &observer_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let approved = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/office/releases/{}/approve", release.id),
                &issued.token,
            ))
            .await
            .unwrap();
        assert_eq!(approved.status(), StatusCode::OK);
        let dispatched = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                "/api/v1/office/releases/dispatch",
                &issued.token,
            ))
            .await
            .unwrap();
        assert_eq!(dispatched.status(), StatusCode::OK);
        let body = to_bytes(dispatched.into_body(), usize::MAX).await.unwrap();
        let dispatched: Option<OfficeReleaseRequest> = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            dispatched.unwrap().status,
            crate::domain::OfficeReleaseStatus::Failed
        );

        let requested = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                "/api/v1/gitlab/writes",
                &observer_token.token,
                json!({
                    "task_id": task_id,
                    "payload": {
                        "kind": "create_issue",
                        "project": "platform/gateway",
                        "title": "Launch checklist",
                        "description": "Track rollout evidence.",
                        "labels": ["delivery"]
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(requested.status(), StatusCode::OK);
        let body = to_bytes(requested.into_body(), usize::MAX).await.unwrap();
        let write: GitLabWriteRequest = serde_json::from_slice(&body).unwrap();
        let denied = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/gitlab/writes/{}/approve", write.id),
                &observer_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let approved = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/gitlab/writes/{}/approve", write.id),
                &issued.token,
            ))
            .await
            .unwrap();
        assert_eq!(approved.status(), StatusCode::OK);
        let dispatched = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                "/api/v1/gitlab/writes/dispatch",
                &issued.token,
            ))
            .await
            .unwrap();
        assert_eq!(dispatched.status(), StatusCode::OK);
        let body = to_bytes(dispatched.into_body(), usize::MAX).await.unwrap();
        let dispatched: Option<GitLabWriteRequest> = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            dispatched.unwrap().status,
            crate::domain::GitLabWriteStatus::Failed
        );

        let proposed = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/flows/{}/changes", flow.id),
                &issued.token,
                json!({"summary": "Add a release checklist"}),
            ))
            .await
            .unwrap();
        assert_eq!(proposed.status(), StatusCode::OK);
        let body = to_bytes(proposed.into_body(), usize::MAX).await.unwrap();
        let change: FlowChangeRequest = serde_json::from_slice(&body).unwrap();
        assert_eq!(change.new_tasks.len(), 1);
        let denied = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/flow-changes/{}/approve", change.id),
                &observer_token.token,
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let approved = service
            .clone()
            .oneshot(authenticated_request(
                "POST",
                &format!("/api/v1/flow-changes/{}/approve", change.id),
                &issued.token,
            ))
            .await
            .unwrap();
        assert_eq!(approved.status(), StatusCode::OK);
        assert_eq!(
            app.lock().await.state().flow(&flow.id).unwrap().tasks.len(),
            4
        );

        app.lock()
            .await
            .revoke_api_credential(&issued.credential.id, "admin")
            .unwrap();
        let revoked = service
            .oneshot(authenticated_request("GET", "/api/v1/me", &issued.token))
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn human_and_agent_tokens_drive_remote_flight_lease_lifecycle() {
        let directory = tempdir().unwrap();
        let mut app = RelayApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team("Delivery", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "Engineer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        let agent = app
            .register_principal(
                "Engineer Codex",
                PrincipalKind::Agent,
                Some(&team.id),
                Some(&human.id),
                "product,delivery",
                100,
                None,
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare a launch brief",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
                None,
            )
            .await
            .unwrap();
        let task_id = flow.tasks[0].id.clone();
        app.approve_flow(&flow.id, &human.name).unwrap();
        app.accept_task(&task_id, &human.name).unwrap();
        let human_token = app
            .issue_api_credential(&human.id, "human", "admin")
            .unwrap()
            .token;
        let agent_token = app
            .issue_api_credential(&agent.id, "agent", "admin")
            .unwrap()
            .token;
        let notification_endpoint = app
            .register_notification_endpoint(
                "operations",
                "https://example.invalid/relay",
                &["task.blocked".into()],
                "RELAY_OPERATIONS_SECRET",
                "admin",
            )
            .unwrap();
        let service = router(
            Arc::new(Mutex::new(app)),
            None,
            InteractionWebhookAuth::default(),
        );

        let endpoints = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/notifications/endpoints",
                &human_token,
            ))
            .await
            .unwrap();
        assert_eq!(endpoints.status(), StatusCode::OK);
        let body = to_bytes(endpoints.into_body(), usize::MAX).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("example.invalid"));
        let endpoints: Vec<NotificationEndpointView> = serde_json::from_slice(&body).unwrap();
        assert_eq!(endpoints[0].id, notification_endpoint.id);
        let agent_endpoints = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/notifications/endpoints",
                &agent_token,
            ))
            .await
            .unwrap();
        assert_eq!(agent_endpoints.status(), StatusCode::FORBIDDEN);

        let authorized = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/tasks/{task_id}/flight-leases"),
                &human_token,
                json!({
                    "agent": agent.id,
                    "executor": "codex",
                    "ttl_seconds": 3600,
                    "manifest": {
                        "objective": "implement the release brief",
                        "resources": [{
                            "kind": "file",
                            "key": "docs/release.md",
                            "exclusive": true
                        }]
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
        let body = to_bytes(authorized.into_body(), usize::MAX).await.unwrap();
        let lease: FlightLease = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            lease.manifest.as_ref().unwrap().objective,
            "implement the release brief"
        );

        let claimed = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/flight-leases/{}/claim", lease.id),
                &agent_token,
                json!({"run_id": "WRUN-http"}),
            ))
            .await
            .unwrap();
        assert_eq!(claimed.status(), StatusCode::OK);
        let now = Utc::now();
        let finished = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/flight-leases/{}/finish", lease.id),
                &agent_token,
                json!({
                    "landed": true,
                    "report": {
                        "run_id": "WRUN-http",
                        "executor": "codex",
                        "summary": "patch ready",
                        "base_revision": "abc123",
                        "changed_files": ["src/lib.rs"],
                        "patch_sha256": "a".repeat(64),
                        "log_sha256": "b".repeat(64),
                        "started_at": now,
                        "finished_at": now,
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(finished.status(), StatusCode::OK);

        let options = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                &format!("/api/v1/flight-leases/{}/recovery-options", lease.id),
                &human_token,
            ))
            .await
            .unwrap();
        assert_eq!(options.status(), StatusCode::OK);
        let forked = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/flight-leases/{}/recover", lease.id),
                &human_token,
                json!({
                    "action": "fork",
                    "reason": "verify an alternate route",
                    "ttl_seconds": 3600
                }),
            ))
            .await
            .unwrap();
        assert_eq!(forked.status(), StatusCode::OK);
        let body = to_bytes(forked.into_body(), usize::MAX).await.unwrap();
        let forked: Option<FlightLease> = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            forked.unwrap().parent_lease_id.as_deref(),
            Some(lease.id.as_str())
        );

        let visible_to_requester = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/flight-leases",
                &human_token,
            ))
            .await
            .unwrap();
        let body = to_bytes(visible_to_requester.into_body(), usize::MAX)
            .await
            .unwrap();
        let leases: Vec<FlightLease> = serde_json::from_slice(&body).unwrap();
        assert_eq!(leases.len(), 2);
        assert!(leases.iter().any(|lease| lease.report.is_some()));

        let dashboard = service
            .clone()
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/dashboard",
                &human_token,
            ))
            .await
            .unwrap();
        assert_eq!(dashboard.status(), StatusCode::OK);
        let body = to_bytes(dashboard.into_body(), usize::MAX).await.unwrap();
        let dashboard: DashboardSnapshot = serde_json::from_slice(&body).unwrap();
        assert_eq!(dashboard.metrics.total_flows, 1);
        assert_eq!(dashboard.flights.len(), 2);

        let agent_dashboard = service
            .oneshot(authenticated_request(
                "GET",
                "/api/v1/dashboard",
                &agent_token,
            ))
            .await
            .unwrap();
        assert_eq!(agent_dashboard.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn signed_gitlab_webhooks_update_bound_tasks_idempotently_and_replay() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().join("data");
        let mut app = RelayApp::open(&data_dir).unwrap();
        app.init_organization("Test Org", "admin").unwrap();
        let team = app
            .create_team(
                "Platform",
                "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
                "admin",
            )
            .unwrap();
        let human = app
            .register_principal(
                "Leader",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare a launch brief",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
                None,
            )
            .await
            .unwrap();
        let task_id = flow.tasks[0].id.clone();
        app.approve_flow(&flow.id, &human.name).unwrap();

        let open_body = merge_request_webhook("opened", "abc123");
        let GitLabWebhookEvent::Update(binding) =
            parse_webhook_event(&open_body, Utc::now() - ChronoDuration::seconds(10)).unwrap()
        else {
            panic!("expected merge request update");
        };
        let binding = *binding;
        app.sync_external_artifacts(&task_id, &human.name, vec![binding.artifact])
            .unwrap();

        let signing_key = b"test signing key";
        let signing_token = format!("whsec_{}", BASE64_STANDARD.encode(signing_key));
        let auth = GitLabWebhookAuth::new(Some(&signing_token), None)
            .unwrap()
            .unwrap();
        let app = Arc::new(Mutex::new(app));
        let service = router(app.clone(), Some(auth), InteractionWebhookAuth::default());
        let timestamp = Utc::now().timestamp();
        let merged_body = merge_request_webhook("merged", "def456");

        let merged = service
            .clone()
            .oneshot(signed_webhook_request(
                "Merge Request Hook",
                "delivery-1",
                timestamp,
                signing_key,
                &merged_body,
            ))
            .await
            .unwrap();
        assert_eq!(merged.status(), StatusCode::OK);
        let body = to_bytes(merged.into_body(), usize::MAX).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["status"], "accepted");
        let sequence_after_merge = app.lock().await.state().last_sequence;

        let duplicate = service
            .clone()
            .oneshot(signed_webhook_request(
                "Merge Request Hook",
                "delivery-1",
                timestamp,
                signing_key,
                &merged_body,
            ))
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::OK);
        let body = to_bytes(duplicate.into_body(), usize::MAX).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["status"], "duplicate");
        assert_eq!(app.lock().await.state().last_sequence, sequence_after_merge);

        let pipeline_body = serde_json::to_vec(&json!({
            "object_kind": "pipeline",
            "project": {"id": 7, "path_with_namespace": "platform/gateway"},
            "object_attributes": {
                "id": 99, "name": "MR pipeline", "ref": "feature/gateway",
                "sha": "def456", "status": "success",
                "url": "https://gitlab.test/platform/gateway/-/pipelines/99"
            },
            "merge_request": {"iid": 42}
        }))
        .unwrap();
        let pipeline = service
            .clone()
            .oneshot(signed_webhook_request(
                "Pipeline Hook",
                "delivery-2",
                timestamp,
                signing_key,
                &pipeline_body,
            ))
            .await
            .unwrap();
        assert_eq!(pipeline.status(), StatusCode::OK);

        let stale_body = merge_request_webhook("closed", "old123");
        let stale = service
            .clone()
            .oneshot(signed_webhook_request(
                "Merge Request Hook",
                "delivery-3",
                timestamp - 30,
                signing_key,
                &stale_body,
            ))
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::OK);
        let body = to_bytes(stale.into_body(), usize::MAX).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["status"], "stale");

        let invalid = service
            .oneshot(signed_webhook_request(
                "Pipeline Hook",
                "delivery-4",
                timestamp,
                b"wrong key",
                &pipeline_body,
            ))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);

        let state = app.lock().await;
        let task = state.state().find_task(&task_id).unwrap().1;
        assert!(task.external_artifacts.iter().any(|artifact| {
            artifact.kind == "merge_request" && artifact.status == "merged" && artifact.verified
        }));
        assert!(task.external_artifacts.iter().any(|artifact| {
            artifact.kind == "pipeline" && artifact.status == "success" && artifact.verified
        }));
        assert_eq!(state.state().external_deliveries.len(), 3);
        drop(state);
        drop(app);

        let replayed = RelayApp::open(&data_dir).unwrap();
        assert_eq!(replayed.state().external_deliveries.len(), 3);
        assert!(
            replayed
                .state()
                .find_task(&task_id)
                .unwrap()
                .1
                .external_artifacts
                .iter()
                .any(|artifact| artifact.kind == "pipeline" && artifact.verified)
        );
    }

    fn authenticated_request(method: &str, uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    fn authenticated_json_request(
        method: &str,
        uri: &str,
        token: &str,
        body: serde_json::Value,
    ) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn merge_request_webhook(state: &str, revision: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "object_kind": "merge_request",
            "project": {"id": 7, "path_with_namespace": "platform/gateway"},
            "object_attributes": {
                "iid": 42,
                "title": "Ship gateway",
                "state": state,
                "draft": false,
                "url": "https://gitlab.test/platform/gateway/-/merge_requests/42",
                "last_commit": {"id": revision}
            }
        }))
        .unwrap()
    }

    fn signed_webhook_request(
        event: &str,
        delivery_id: &str,
        timestamp: i64,
        key: &[u8],
        body: &[u8],
    ) -> Request<Body> {
        let timestamp = timestamp.to_string();
        let mut message = format!("{delivery_id}.{timestamp}.").into_bytes();
        message.extend_from_slice(body);
        let mut mac = TestHmacSha256::new_from_slice(key).expect("HMAC accepts keys of any size");
        mac.update(&message);
        let signature = format!("v1,{}", BASE64_STANDARD.encode(mac.finalize().into_bytes()));
        Request::builder()
            .method("POST")
            .uri("/api/v1/connectors/gitlab/webhook")
            .header("content-type", "application/json")
            .header("x-gitlab-event", event)
            .header("webhook-id", delivery_id)
            .header("webhook-timestamp", timestamp)
            .header("webhook-signature", signature)
            .body(Body::from(body.to_vec()))
            .unwrap()
    }

    fn bridge_interaction_request(
        provider: &str,
        delivery_id: &str,
        timestamp: i64,
        key: &[u8],
        body: &[u8],
    ) -> Request<Body> {
        let timestamp = timestamp.to_string();
        let mut message = format!("{provider}.{delivery_id}.{timestamp}.").into_bytes();
        message.extend_from_slice(body);
        let mut mac = TestHmacSha256::new_from_slice(key).expect("HMAC accepts keys of any size");
        mac.update(&message);
        let signature = format!("v1,{}", BASE64_STANDARD.encode(mac.finalize().into_bytes()));
        Request::builder()
            .method("POST")
            .uri("/api/v1/connectors/interactions")
            .header("content-type", "application/json")
            .header("x-relay-provider", provider)
            .header("x-relay-delivery-id", delivery_id)
            .header("x-relay-timestamp", timestamp)
            .header("x-relay-signature", signature)
            .body(Body::from(body.to_vec()))
            .unwrap()
    }

    fn slack_interaction_request(timestamp: i64, key: &[u8], body: &[u8]) -> Request<Body> {
        let timestamp = timestamp.to_string();
        let mut message = format!("v0:{timestamp}:").into_bytes();
        message.extend_from_slice(body);
        let mut mac = TestHmacSha256::new_from_slice(key).expect("HMAC accepts keys of any size");
        mac.update(&message);
        let signature = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Request::builder()
            .method("POST")
            .uri("/api/v1/connectors/slack/actions")
            .header("content-type", "application/x-www-form-urlencoded")
            .header("x-slack-request-timestamp", timestamp)
            .header("x-slack-signature", format!("v0={signature}"))
            .body(Body::from(body.to_vec()))
            .unwrap()
    }
}
