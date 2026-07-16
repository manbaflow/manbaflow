use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
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
use crate::domain::{Evidence, Flow, Principal, Task, TrackingEscalation};
use crate::error::{MambaError, Result};

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

pub async fn run(app: MambaApp, options: ServerOptions) -> Result<()> {
    if options.tracker_interval_seconds == 0 {
        return Err(MambaError::Validation(
            "tracker interval must be greater than zero".into(),
        ));
    }
    let app = Arc::new(Mutex::new(app));
    spawn_tracker(app.clone(), &options);
    let listener = TcpListener::bind(options.bind).await?;
    axum::serve(listener, router(app))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn router(app: Arc<Mutex<MambaApp>>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/me", get(me))
        .route("/api/v1/inbox", get(inbox))
        .route("/api/v1/escalations", get(escalations))
        .route("/api/v1/escalations/{id}/ack", post(ack_escalation))
        .route("/api/v1/flows/{id}/approve", post(approve_flow))
        .route("/api/v1/tasks/{id}/accept", post(accept_task))
        .route("/api/v1/tasks/{id}/start", post(start_task))
        .route("/api/v1/tasks/{id}/heartbeat", post(heartbeat_task))
        .route("/api/v1/tasks/{id}/block", post(block_task))
        .route("/api/v1/tasks/{id}/evidence", post(add_evidence))
        .route("/api/v1/tasks/{id}/submit", post(submit_task))
        .route("/api/v1/tasks/{id}/complete", post(complete_task))
        .with_state(ApiState { app })
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

async fn inbox(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<InboxItem>>> {
    let app = state.app.lock().await;
    let principal = authenticate(&app, &headers)?;
    let items = app
        .inbox(&principal.id)?
        .into_iter()
        .map(|(flow, task)| InboxItem {
            flow_id: flow.id.clone(),
            flow_title: flow.prd.title.clone(),
            task: task.clone(),
        })
        .collect();
    Ok(Json(items))
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

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::*;
    use crate::domain::PrincipalKind;
    use crate::planner::PlannerKind;

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
                "operations",
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
        let service = router(app.clone());
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

    fn authenticated_request(method: &str, uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }
}
