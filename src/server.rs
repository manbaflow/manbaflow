use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::{MissedTickBehavior, interval};

use crate::MambaApp;
use crate::dashboard::DashboardSnapshot;
use crate::domain::{
    AssignmentTarget, Evidence, ExecutorKind, FlightLease, Flow, FlowMessage, FlowMessageKind,
    MessageInboxItem, Principal, PrincipalKind, RemoteFlightReport, Task, TrackingEscalation,
};
use crate::error::{MambaError, Result};
use crate::gitlab::{GitLabWebhookAuth, GitLabWebhookEvent, parse_webhook_event};

#[derive(Clone, Debug)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub tracker_interval_seconds: u64,
    pub stale_after_hours: u64,
    pub escalate_after_hours: u64,
}

#[derive(Clone)]
struct ApiState {
    app: Arc<Mutex<MambaApp>>,
    gitlab_webhook_auth: Option<GitLabWebhookAuth>,
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

impl From<MambaError> for ApiError {
    fn from(error: MambaError) -> Self {
        let status = match &error {
            MambaError::NotFound { .. } => StatusCode::NOT_FOUND,
            MambaError::InvalidTransition(_) => StatusCode::CONFLICT,
            MambaError::PermissionDenied(_) => StatusCode::FORBIDDEN,
            MambaError::Validation(_) | MambaError::InvalidWorkspace(_) => StatusCode::BAD_REQUEST,
            MambaError::OrganizationNotInitialized => StatusCode::PRECONDITION_REQUIRED,
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

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Clone, Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct InboxItem {
    flow_id: String,
    flow_title: String,
    task: Task,
    blocked_by: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct HeartbeatInput {
    #[serde(default)]
    note: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct BlockInput {
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
struct EvidenceInput {
    kind: String,
    uri: String,
    summary: String,
}

#[derive(Clone, Debug, Deserialize)]
struct NegotiateInput {
    effort_hours: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct ReassignInput {
    owner: String,
    #[serde(default)]
    copilots: Vec<String>,
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
struct MessageInboxQuery {
    #[serde(default)]
    all: bool,
}

#[derive(Clone, Debug, Deserialize)]
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
struct AuthorizeFlightInput {
    agent: String,
    executor: ExecutorKind,
    #[serde(default = "default_lease_ttl_seconds")]
    ttl_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct ClaimFlightInput {
    run_id: String,
}

#[derive(Clone, Debug, Deserialize)]
struct FinishFlightInput {
    landed: bool,
    report: RemoteFlightReport,
}

#[derive(Clone, Debug, Serialize)]
struct GitLabWebhookResponse {
    status: &'static str,
    event: String,
    matched_tasks: usize,
    changed_tasks: usize,
}

pub async fn run(app: MambaApp, options: ServerOptions) -> Result<()> {
    if options.tracker_interval_seconds == 0 {
        return Err(MambaError::Validation(
            "tracker interval must be greater than zero".into(),
        ));
    }
    let gitlab_webhook_auth = GitLabWebhookAuth::from_env()?;
    let listener = TcpListener::bind(options.bind).await?;
    println!(
        "MambaFlow control plane listening on http://{}",
        options.bind
    );
    if gitlab_webhook_auth.is_some() {
        println!("GitLab webhook receiver enabled");
    }
    let app = Arc::new(Mutex::new(app));
    spawn_tracker(app.clone(), &options);
    axum::serve(listener, router(app, gitlab_webhook_auth))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn router(app: Arc<Mutex<MambaApp>>, gitlab_webhook_auth: Option<GitLabWebhookAuth>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/me", get(me))
        .route("/api/v1/dashboard", get(dashboard))
        .route("/api/v1/inbox", get(inbox))
        .route("/api/v1/messages", get(message_inbox))
        .route("/api/v1/messages/{id}/ack", post(acknowledge_message))
        .route("/api/v1/escalations", get(escalations))
        .route("/api/v1/escalations/{id}/ack", post(ack_escalation))
        .route("/api/v1/flows/{id}/approve", post(approve_flow))
        .route(
            "/api/v1/flows/{id}/messages",
            get(flow_messages).post(post_message),
        )
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
        .route("/api/v1/tasks/{id}/submit", post(submit_task))
        .route("/api/v1/tasks/{id}/complete", post(complete_task))
        .route("/api/v1/connectors/gitlab/webhook", post(gitlab_webhook))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(ApiState {
            app,
            gitlab_webhook_auth,
        })
}

fn spawn_tracker(app: Arc<Mutex<MambaApp>>, options: &ServerOptions) {
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
                let _ = app.scan_tracking_with_policy(
                    stale_after_hours,
                    escalate_after_hours,
                    "tower://server",
                );
            }
        }
    });
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "mambaflow-control-plane",
    })
}

async fn me(State(state): State<ApiState>, headers: HeaderMap) -> ApiResult<Json<Principal>> {
    let app = state.app.lock().await;
    Ok(Json(authenticate(&app, &headers)?))
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
        app.authorize_remote_flight(
            &task_id,
            actor,
            &input.agent,
            input.executor,
            input.ttl_seconds,
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
    action: impl FnOnce(&mut MambaApp, &str) -> Result<T>,
) -> ApiResult<Json<T>> {
    let mut app = state.app.lock().await;
    let principal = authenticate(&app, headers)?;
    Ok(Json(action(&mut app, &principal.name)?))
}

fn authenticate(app: &MambaApp, headers: &HeaderMap) -> ApiResult<Principal> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(ApiError::unauthorized)?;
    let (scheme, token) = value.split_once(' ').ok_or_else(ApiError::unauthorized)?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(ApiError::unauthorized());
    }
    app.authenticate_api_token(token)?
        .ok_or_else(ApiError::unauthorized)
}

fn webhook_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn default_lease_ttl_seconds() -> u64 {
    3_600
}

fn default_requires_ack() -> bool {
    true
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

    #[tokio::test]
    async fn bearer_identity_drives_remote_inbox_and_task_actions() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
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
        let service = router(app.clone(), None);
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
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
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
        let service = router(Arc::new(Mutex::new(app)), None);

        let authorized = service
            .clone()
            .oneshot(authenticated_json_request(
                "POST",
                &format!("/api/v1/tasks/{task_id}/flight-leases"),
                &human_token,
                json!({"agent": agent.id, "executor": "codex", "ttl_seconds": 3600}),
            ))
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
        let body = to_bytes(authorized.into_body(), usize::MAX).await.unwrap();
        let lease: FlightLease = serde_json::from_slice(&body).unwrap();

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
        assert_eq!(leases.len(), 1);
        assert!(leases[0].report.is_some());

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
        assert_eq!(dashboard.flights.len(), 1);

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
        let mut app = MambaApp::open(&data_dir).unwrap();
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
        let service = router(app.clone(), Some(auth));
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

        let replayed = MambaApp::open(&data_dir).unwrap();
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
}
