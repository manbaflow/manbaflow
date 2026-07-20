use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::io::stdout;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chrono::Local;
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use ratatui::layout::{Alignment, Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Gauge, HighlightSpacing, List, ListItem, ListState, Paragraph,
    Row, Table, TableState, Tabs, Wrap,
};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::mpsc;

use crate::MambaApp;
use crate::dashboard::{ActionPriority, FlowHealth, build_dashboard};
use crate::domain::{
    AssignmentTarget, AttentionSeverity, ExecutorMode, FlightLease, FlightLeaseStatus, Flow,
    FlowMessageKind, FlowStatus, MessageInboxItem, OfficeReleaseStatus, PrincipalKind,
    RecoveryAction, Task, TaskStatus, TrackingEscalation,
};
use crate::error::{MambaError, Result};
use crate::event::{DomainEvent, EventEnvelope};
use crate::planner::PlannerKind;
use crate::showcase::bootstrap_showcase;

const BG: Color = Color::Rgb(13, 14, 16);
const PANEL: Color = Color::Rgb(23, 25, 29);
const PANEL_ALT: Color = Color::Rgb(30, 32, 37);
const TEXT: Color = Color::Rgb(226, 228, 232);
const MUTED: Color = Color::Rgb(135, 141, 151);
const GOLD: Color = Color::Rgb(246, 184, 61);
const PURPLE: Color = Color::Rgb(111, 76, 172);
const CYAN: Color = Color::Rgb(69, 184, 196);
const GREEN: Color = Color::Rgb(76, 190, 118);
const RED: Color = Color::Rgb(225, 89, 89);
const ORANGE: Color = Color::Rgb(232, 139, 62);

#[derive(Clone, Debug)]
pub struct TuiOptions {
    pub workspace: PathBuf,
    pub actor: Option<String>,
}

struct MouseCaptureGuard;

impl MouseCaptureGuard {
    fn enable() -> std::io::Result<Self> {
        execute!(stdout(), EnableMouseCapture)?;
        Ok(Self)
    }
}

impl Drop for MouseCaptureGuard {
    fn drop(&mut self) {
        let _ = execute!(stdout(), DisableMouseCapture);
    }
}

pub async fn run(app: &mut MambaApp, options: TuiOptions) -> Result<()> {
    let mut terminal = ratatui::init();
    let mouse_capture = match MouseCaptureGuard::enable() {
        Ok(guard) => guard,
        Err(error) => {
            ratatui::restore();
            return Err(error.into());
        }
    };
    let result = run_loop(app, &mut terminal, options).await;
    drop(mouse_capture);
    ratatui::restore();
    result
}

async fn run_loop(
    app: &mut MambaApp,
    terminal: &mut DefaultTerminal,
    options: TuiOptions,
) -> Result<()> {
    let mut state = UiState::new(app, options);
    loop {
        state.poll_tracking(app);
        state.poll_planning(app);
        state.poll_flights(app);
        state.poll_notification_outbox(app);
        terminal.draw(|frame| render(frame, app, &mut state))?;
        if event::poll(Duration::from_millis(180))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    if state.handle_key(app, key).await? {
                        return Ok(());
                    }
                }
                Event::Mouse(mouse) => {
                    if state.handle_mouse(app, mouse).await? {
                        return Ok(());
                    }
                }
                Event::Paste(value) => state.paste(value),
                _ => {}
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum View {
    Overview,
    Flows,
    Inbox,
    Roster,
    Timeline,
}

impl View {
    const ALL: [Self; 5] = [
        Self::Overview,
        Self::Flows,
        Self::Inbox,
        Self::Roster,
        Self::Timeline,
    ];

    fn title(self) -> &'static str {
        match self {
            Self::Overview => "总览 OVERVIEW",
            Self::Flows => "任务流 FLOWS",
            Self::Inbox => "收件箱 INBOX",
            Self::Roster => "阵容 ROSTER",
            Self::Timeline => "黑匣子 TIMELINE",
        }
    }

    fn index(self) -> usize {
        Self::ALL.iter().position(|view| *view == self).unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MouseAction {
    LoadShowcase,
    NewDemand,
    ApproveOrAccept,
    Advance,
    Plan,
    Execute,
    Evidence,
    Block,
    Complete,
    SendMessage,
    NegotiateEstimate,
    Reassign,
    RequestChange,
    RejectChange,
    ScanTracker,
    DispatchNotifications,
    ApproveOfficeRelease,
    AcknowledgeEscalation,
    RecoverFlight(RecoveryAction),
    CycleActor,
    Quit,
    ConfirmModal,
    CancelModal,
    SelectPlanner(PlannerKind),
    SelectAssignee(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HitTarget {
    Tab(View),
    Flow(usize),
    Task(usize),
    Inbox(usize),
    Principal(usize),
    Timeline(usize),
    Flight(usize),
    Action(MouseAction),
}

#[derive(Clone, Copy, Debug)]
struct HitRegion {
    area: Rect,
    target: HitTarget,
}

#[derive(Clone, Debug)]
enum InputPurpose {
    Demand,
    Evidence {
        task_id: String,
    },
    Block {
        task_id: String,
    },
    Message {
        flow_id: String,
        task_id: String,
        recipients: Vec<String>,
        kind: FlowMessageKind,
        requires_ack: bool,
    },
    Negotiate {
        task_id: String,
        current_hours: f64,
    },
    Reassign {
        task_id: String,
        candidates: Vec<AssignmentTarget>,
        selected: usize,
    },
    FlowChange {
        flow_id: String,
    },
    RejectChange {
        request_id: String,
    },
    Run {
        task_id: String,
        mode: ExecutorMode,
    },
    RecoverFlight {
        lease_id: String,
        action: RecoveryAction,
    },
}

#[derive(Clone, Debug)]
struct InputModal {
    purpose: InputPurpose,
    value: String,
}

#[derive(Clone, Debug)]
struct ActiveFlight {
    flow_id: String,
    task_id: String,
    actor: String,
    mode: ExecutorMode,
    started_at: chrono::DateTime<Local>,
}

#[derive(Debug)]
struct FlightResult {
    task_id: String,
    outcome: std::result::Result<LandedFlight, String>,
}

#[derive(Debug)]
struct LandedFlight {
    executor: String,
    summary: String,
    log_path: PathBuf,
}

#[derive(Clone, Debug)]
struct ActivePlanning {
    planner: PlannerKind,
    actor: String,
    summary: String,
    started_at: chrono::DateTime<Local>,
}

#[derive(Debug)]
struct PlanningResult {
    outcome: std::result::Result<PlannedFlow, String>,
}

#[derive(Debug)]
struct PlannedFlow {
    flow_id: String,
    title: String,
}

#[derive(Debug)]
struct NotificationResult {
    delivery_id: String,
    attempt: crate::notification::NotificationAttempt,
    manual: bool,
}

struct UiState {
    view: View,
    flow_index: usize,
    task_index: usize,
    inbox_index: usize,
    roster_index: usize,
    timeline_index: usize,
    flight_index: usize,
    focus_tasks: bool,
    focus_flights: bool,
    actor_id: Option<String>,
    workspace: PathBuf,
    timeline: Vec<EventEnvelope>,
    modal: Option<InputModal>,
    message: String,
    message_is_error: bool,
    active_flights: BTreeMap<String, ActiveFlight>,
    flight_tx: mpsc::UnboundedSender<FlightResult>,
    flight_rx: mpsc::UnboundedReceiver<FlightResult>,
    last_flight_reload: Instant,
    hit_regions: Vec<HitRegion>,
    demand_planner: PlannerKind,
    active_planning: Option<ActivePlanning>,
    planning_tx: mpsc::UnboundedSender<PlanningResult>,
    planning_rx: mpsc::UnboundedReceiver<PlanningResult>,
    last_tracking_scan: Option<Instant>,
    notification_tx: mpsc::UnboundedSender<NotificationResult>,
    notification_rx: mpsc::UnboundedReceiver<NotificationResult>,
    active_notification: Option<String>,
    last_notification_dispatch: Instant,
}

impl UiState {
    fn new(app: &MambaApp, options: TuiOptions) -> Self {
        let (flight_tx, flight_rx) = mpsc::unbounded_channel();
        let (planning_tx, planning_rx) = mpsc::unbounded_channel();
        let (notification_tx, notification_rx) = mpsc::unbounded_channel();
        let humans = human_ids(app);
        let flow_index = initial_flow_index(app);
        let actor_id = options
            .actor
            .as_deref()
            .and_then(|value| app.state().principal(value).ok())
            .filter(|principal| principal.kind == PrincipalKind::Human)
            .map(|principal| principal.id.clone())
            .or_else(|| humans.first().cloned());
        let mut state = Self {
            view: View::Overview,
            flow_index,
            task_index: 0,
            inbox_index: 0,
            roster_index: 0,
            timeline_index: 0,
            flight_index: 0,
            focus_tasks: false,
            focus_flights: false,
            actor_id,
            workspace: options.workspace,
            timeline: Vec::new(),
            modal: None,
            message: "塔台在线。通过标签、列表和底部操作带调度 Flow。".to_string(),
            message_is_error: false,
            active_flights: BTreeMap::new(),
            flight_tx,
            flight_rx,
            last_flight_reload: Instant::now(),
            hit_regions: Vec::new(),
            demand_planner: PlannerKind::Local,
            active_planning: None,
            planning_tx,
            planning_rx,
            last_tracking_scan: None,
            notification_tx,
            notification_rx,
            active_notification: None,
            last_notification_dispatch: Instant::now() - Duration::from_secs(15),
        };
        state.refresh_timeline(app);
        state
    }

    async fn handle_key(&mut self, app: &mut MambaApp, key: KeyEvent) -> Result<bool> {
        if self.modal.is_some() {
            return self.handle_modal_key(app, key).await;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return Ok(self.request_quit());
        }
        Ok(matches!(key.code, KeyCode::Char('q')) && self.request_quit())
    }

    async fn handle_mouse(&mut self, app: &mut MambaApp, mouse: MouseEvent) -> Result<bool> {
        let target = self.target_at(mouse.column, mouse.row);
        if self.modal.is_some() {
            if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                match target {
                    Some(HitTarget::Action(MouseAction::ConfirmModal)) => {
                        self.submit_modal(app).await;
                    }
                    Some(HitTarget::Action(MouseAction::CancelModal)) => self.modal = None,
                    Some(HitTarget::Action(MouseAction::SelectPlanner(planner))) => {
                        self.demand_planner = planner;
                    }
                    Some(HitTarget::Action(MouseAction::SelectAssignee(index))) => {
                        if let Some(InputModal {
                            purpose: InputPurpose::Reassign { selected, .. },
                            ..
                        }) = &mut self.modal
                        {
                            *selected = index;
                        }
                    }
                    _ => {}
                }
            }
            return Ok(false);
        }

        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => match target {
                Some(HitTarget::Tab(view)) => self.switch_view(app, view),
                Some(HitTarget::Flow(index)) => {
                    self.flow_index = index;
                    self.task_index = 0;
                    self.flight_index = 0;
                    self.focus_tasks = false;
                    self.focus_flights = false;
                    self.refresh_timeline(app);
                }
                Some(HitTarget::Task(index)) => {
                    self.task_index = index;
                    self.focus_tasks = true;
                }
                Some(HitTarget::Inbox(index)) => self.inbox_index = index,
                Some(HitTarget::Principal(index)) => self.roster_index = index,
                Some(HitTarget::Timeline(index)) => {
                    self.timeline_index = index;
                    self.focus_flights = false;
                }
                Some(HitTarget::Flight(index)) => {
                    self.flight_index = index;
                    self.focus_flights = true;
                    if let Some(flight) = self.selected_flight(app) {
                        self.message = flight_selection_summary(app, flight);
                        self.message_is_error = false;
                    }
                }
                Some(HitTarget::Action(action)) => {
                    return self.handle_mouse_action(app, action).await;
                }
                None => {}
            },
            MouseEventKind::ScrollUp => {
                self.focus_from_target(target);
                self.move_selection(app, -1);
            }
            MouseEventKind::ScrollDown => {
                self.focus_from_target(target);
                self.move_selection(app, 1);
            }
            MouseEventKind::ScrollLeft => match self.view {
                View::Flows => self.focus_tasks = false,
                View::Timeline => {
                    self.flow_index = shifted(self.flow_index, flow_ids(app).len(), -1);
                    self.refresh_timeline(app);
                }
                _ => {}
            },
            MouseEventKind::ScrollRight => match self.view {
                View::Flows => self.focus_tasks = true,
                View::Timeline => {
                    self.flow_index = shifted(self.flow_index, flow_ids(app).len(), 1);
                    self.refresh_timeline(app);
                }
                _ => {}
            },
            _ => {}
        }
        Ok(false)
    }

    async fn handle_mouse_action(
        &mut self,
        app: &mut MambaApp,
        action: MouseAction,
    ) -> Result<bool> {
        match action {
            MouseAction::LoadShowcase => self.load_showcase(app).await,
            MouseAction::NewDemand => self.open_demand_modal(),
            MouseAction::ApproveOrAccept => self.approve_or_accept(app),
            MouseAction::Advance => self.advance_task(app),
            MouseAction::Plan => self.open_run_confirmation(app, ExecutorMode::Plan),
            MouseAction::Execute => self.open_run_confirmation(app, ExecutorMode::Execute),
            MouseAction::Evidence => self.open_task_input(app, true),
            MouseAction::Block => self.open_task_input(app, false),
            MouseAction::Complete => self.complete_task(app),
            MouseAction::SendMessage => self.open_message_input(app),
            MouseAction::NegotiateEstimate => self.open_estimate_input(app),
            MouseAction::Reassign => self.open_reassign_input(app),
            MouseAction::RequestChange => self.open_flow_change_input(app),
            MouseAction::RejectChange => self.open_reject_change_input(app),
            MouseAction::ScanTracker => self.scan_tracker(app, true),
            MouseAction::DispatchNotifications => self.dispatch_notification_now(app),
            MouseAction::ApproveOfficeRelease => self.approve_next_office_release(app),
            MouseAction::AcknowledgeEscalation => self.acknowledge_next_inbox(app),
            MouseAction::RecoverFlight(action) => self.open_recovery_input(app, action),
            MouseAction::CycleActor => self.cycle_actor(app),
            MouseAction::Quit => return Ok(self.request_quit()),
            MouseAction::ConfirmModal => self.submit_modal(app).await,
            MouseAction::CancelModal => self.modal = None,
            MouseAction::SelectPlanner(planner) => self.demand_planner = planner,
            MouseAction::SelectAssignee(_) => {}
        }
        Ok(false)
    }

    fn focus_from_target(&mut self, target: Option<HitTarget>) {
        match target {
            Some(HitTarget::Flow(_)) => self.focus_tasks = false,
            Some(HitTarget::Task(_)) => self.focus_tasks = true,
            Some(HitTarget::Timeline(_)) => self.focus_flights = false,
            Some(HitTarget::Flight(_)) => self.focus_flights = true,
            _ => {}
        }
    }

    fn target_at(&self, column: u16, row: u16) -> Option<HitTarget> {
        self.hit_regions
            .iter()
            .rev()
            .find(|region| rect_contains(region.area, column, row))
            .map(|region| region.target)
    }

    fn register_hit(&mut self, area: Rect, target: HitTarget) {
        if area.width > 0 && area.height > 0 {
            self.hit_regions.push(HitRegion { area, target });
        }
    }

    async fn handle_modal_key(&mut self, app: &mut MambaApp, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => self.modal = None,
            KeyCode::Backspace => {
                if let Some(modal) = &mut self.modal {
                    modal.value.pop();
                }
            }
            KeyCode::Enter => self.submit_modal(app).await,
            KeyCode::Char(value) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(modal) = &mut self.modal {
                    modal.value.push(value);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    async fn submit_modal(&mut self, app: &mut MambaApp) {
        let Some(modal) = self.modal.take() else {
            return;
        };
        let value = modal.value.trim();
        if value.is_empty() {
            self.failure(MambaError::Validation("输入不能为空".to_string()));
            return;
        }
        let Some(actor) = self.actor_name(app).map(str::to_string) else {
            self.failure(MambaError::Validation(
                "请先注册 Human，或点击顶部当前球权选择操作人".to_string(),
            ));
            return;
        };

        if matches!(&modal.purpose, InputPurpose::Demand) {
            self.launch_planning(app, value.to_string(), actor);
            return;
        }

        if let InputPurpose::Run { task_id, mode } = &modal.purpose {
            let expected = confirmation_token(mode);
            if value != expected {
                self.failure(MambaError::Validation(format!(
                    "确认失败：请输入 {expected}"
                )));
                return;
            }
            self.launch_flight(app, task_id.clone(), actor, mode.clone());
            return;
        }

        if let InputPurpose::FlowChange { flow_id } = &modal.purpose {
            let result = app
                .propose_flow_change(
                    flow_id,
                    &actor,
                    value,
                    PlannerKind::Local,
                    &self.workspace,
                    30,
                )
                .await;
            match result {
                Ok(change) => {
                    self.refresh_timeline(app);
                    self.success(format!(
                        "{} 影响预览：+{} tasks · 进度 {:+.1}h + 范围 {:+.1}h = 净 {:+.1}h；请在底部批准或驳回",
                        change.id,
                        change.new_tasks.len(),
                        change.impact.baseline_p80_delta_hours,
                        change.impact.scope_p80_delta_hours,
                        change.impact.net_p80_delta_hours
                    ));
                }
                Err(error) => self.failure(error),
            }
            return;
        }

        let result = match modal.purpose {
            InputPurpose::Demand => unreachable!(),
            InputPurpose::Evidence { task_id } => app
                .add_evidence(
                    &task_id,
                    &actor,
                    "note",
                    &format!("mambaflow://task/{task_id}/evidence"),
                    value,
                )
                .map(|evidence| format!("证据 {} 已进入黑匣子", evidence.id)),
            InputPurpose::Block { task_id } => app
                .block_task(&task_id, &actor, value)
                .map(|task| format!("{} 已阻塞，塔台收到求助", task.id)),
            InputPurpose::Message {
                flow_id,
                task_id,
                recipients,
                kind,
                requires_ack,
            } => app
                .post_flow_message(
                    &flow_id,
                    Some(&task_id),
                    &actor,
                    kind,
                    &recipients,
                    value,
                    requires_ack,
                )
                .map(|message| {
                    format!(
                        "{} 已传球给 {}",
                        message.id,
                        message
                            .recipients
                            .iter()
                            .map(|recipient| recipient.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }),
            InputPurpose::Negotiate { task_id, .. } => value
                .parse::<f64>()
                .map_err(|_| MambaError::Validation("工时必须是数字".into()))
                .and_then(|hours| app.negotiate_task(&task_id, &actor, hours))
                .map(|task| {
                    format!(
                        "{} 调整为 {:.1}h；整条 Flow 已重新排期",
                        task.id, task.estimate.effort_hours
                    )
                }),
            InputPurpose::Reassign {
                task_id,
                candidates,
                selected,
            } => candidates
                .get(selected)
                .ok_or_else(|| MambaError::Validation("没有可用的换防候选人".into()))
                .and_then(|target| app.reassign_task(&task_id, &actor, &target.id, &[], value))
                .map(|flow| {
                    let task = flow
                        .task(&task_id)
                        .expect("reassigned task remains in flow");
                    format!(
                        "{} 已换防给 {}；P80 {}",
                        task.id,
                        task.assignment.as_ref().unwrap().owner.name,
                        flow.p80_finish.format("%m-%d %H:%M")
                    )
                }),
            InputPurpose::FlowChange { .. } => unreachable!(),
            InputPurpose::RejectChange { request_id } => app
                .reject_flow_change(&request_id, &actor, value)
                .map(|change| format!("{} 已驳回，正式 Flow 保持不变", change.id)),
            InputPurpose::Run { .. } => unreachable!(),
            InputPurpose::RecoverFlight { lease_id, action } => app
                .recover_remote_flight(&lease_id, &actor, action, value, None, None, 3_600)
                .map(|child| {
                    child.map_or_else(
                        || format!("{lease_id} 已转人工或停飞，监督决定已进入黑匣子"),
                        |child| format!("{lease_id} 已复飞为 {} · A{}", child.id, child.attempt),
                    )
                }),
        };
        match result {
            Ok(message) => {
                self.clamp_selection(app);
                self.refresh_timeline(app);
                self.success(message);
            }
            Err(error) => self.failure(error),
        }
    }

    fn paste(&mut self, value: String) {
        if let Some(modal) = &mut self.modal {
            modal.value.push_str(&value.replace(['\r', '\n'], " "));
        }
    }

    fn switch_view(&mut self, app: &MambaApp, view: View) {
        self.view = view;
        self.refresh_timeline(app);
    }

    async fn load_showcase(&mut self, app: &mut MambaApp) {
        match bootstrap_showcase(app, &self.workspace).await {
            Ok(showcase) => {
                self.actor_id = app
                    .state()
                    .principal("牢大")
                    .ok()
                    .map(|principal| principal.id.clone());
                self.flow_index = flow_ids(app)
                    .iter()
                    .position(|flow_id| flow_id == &showcase.highlighted_flow_id)
                    .unwrap_or_else(|| initial_flow_index(app));
                self.task_index = 0;
                self.inbox_index = 0;
                self.flight_index = 0;
                self.focus_tasks = false;
                self.focus_flights = false;
                self.view = View::Overview;
                self.refresh_timeline(app);
                self.success("Showcase 机队已进场：3 条 Flow 就位，塔台已聚焦 LLM Gateway 风险");
            }
            Err(error) => self.failure(error),
        }
    }

    fn move_selection(&mut self, app: &MambaApp, delta: isize) {
        match self.view {
            View::Overview => {
                self.flow_index = shifted(self.flow_index, flow_ids(app).len(), delta);
                self.task_index = 0;
                self.refresh_timeline(app);
            }
            View::Flows if self.focus_tasks => {
                let count = self.selected_flow(app).map_or(0, |flow| flow.tasks.len());
                self.task_index = shifted(self.task_index, count, delta);
            }
            View::Flows => {
                self.flow_index = shifted(self.flow_index, flow_ids(app).len(), delta);
                self.task_index = 0;
                self.refresh_timeline(app);
            }
            View::Inbox => {
                self.inbox_index = shifted(self.inbox_index, self.inbox_items(app).len(), delta);
            }
            View::Roster => {
                self.roster_index = shifted(self.roster_index, app.state().principals.len(), delta);
            }
            View::Timeline => {
                if self.focus_flights {
                    self.flight_index = shifted(
                        self.flight_index,
                        self.remote_flights(app).len().min(5),
                        delta,
                    );
                } else {
                    self.timeline_index = shifted(self.timeline_index, self.timeline.len(), delta);
                }
            }
        }
    }

    fn cycle_actor(&mut self, app: &MambaApp) {
        let humans = human_ids(app);
        if humans.is_empty() {
            self.actor_id = None;
            self.failure(MambaError::Validation("组织中还没有注册 Human".to_string()));
            return;
        }
        let current = self
            .actor_id
            .as_ref()
            .and_then(|id| humans.iter().position(|candidate| candidate == id))
            .unwrap_or(0);
        self.actor_id = Some(humans[(current + 1) % humans.len()].clone());
        self.inbox_index = 0;
        if let Some(actor) = self.actor_name(app) {
            self.success(format!("当前球权切换为 {actor}"));
        }
    }

    fn approve_or_accept(&mut self, app: &mut MambaApp) {
        let Some(actor) = self.actor_name(app).map(str::to_string) else {
            self.failure(MambaError::Validation(
                "没有可用的 Human 操作人".to_string(),
            ));
            return;
        };
        let result = if matches!(self.view, View::Overview)
            || (self.view == View::Flows && !self.focus_tasks)
        {
            let flow_id = self.selected_flow(app).map(|flow| flow.id.clone());
            flow_id
                .ok_or_else(|| MambaError::Validation("没有选中的 Flow".to_string()))
                .and_then(|flow_id| {
                    if let Some(change_id) = app
                        .pending_flow_change(&flow_id)
                        .map(|change| change.id.clone())
                    {
                        app.approve_flow_change(&change_id, &actor)
                            .map(|change| format!("{} 已批准，新增任务完成传球", change.id))
                    } else {
                        app.approve_flow(&flow_id, &actor)
                            .map(|flow| format!("{} 已批准，WorkRequest 完成传球", flow.id))
                    }
                })
        } else {
            let task_id = self
                .selected_task_context(app)
                .map(|(_, task)| task.id.clone());
            task_id
                .ok_or_else(|| MambaError::Validation("没有选中的任务".to_string()))
                .and_then(|task_id| app.accept_task(&task_id, &actor))
                .map(|task| format!("{} 已接球", task.id))
        };
        self.finish_action(app, result);
    }

    fn approve_next_office_release(&mut self, app: &mut MambaApp) {
        let Some(actor) = self.actor_name(app).map(str::to_string) else {
            self.failure(MambaError::Validation(
                "没有可用的 Human 操作人".to_string(),
            ));
            return;
        };
        let principal = app.state().principal(&actor).ok();
        let release_id = principal.and_then(|principal| {
            app.state()
                .office_releases
                .values()
                .filter(|release| release.status == OfficeReleaseStatus::Requested)
                .find(|release| {
                    app.state().flows.get(&release.flow_id).is_some_and(|flow| {
                        flow.demand.requester == principal.id
                            || flow.demand.requester == principal.name
                    })
                })
                .map(|release| release.id.clone())
        });
        let result = release_id
            .ok_or_else(|| MambaError::Validation("当前没有待你放行的 Office 发布".into()))
            .and_then(|release_id| app.approve_office_release(&release_id, &actor))
            .map(|release| format!("{} 已由 Human 放行，等待 Tower 发布", release.id));
        self.finish_action(app, result);
    }

    fn advance_task(&mut self, app: &mut MambaApp) {
        let Some(actor) = self.actor_name(app).map(str::to_string) else {
            self.failure(MambaError::Validation(
                "没有可用的 Human 操作人".to_string(),
            ));
            return;
        };
        let Some((_, task)) = self.selected_task_context(app) else {
            self.failure(MambaError::Validation("没有选中的任务".to_string()));
            return;
        };
        let task_id = task.id.clone();
        let status = task.status.clone();
        let result = match status {
            TaskStatus::Assigned => app
                .accept_task(&task_id, &actor)
                .map(|task| format!("{} 已接球", task.id)),
            TaskStatus::Accepted | TaskStatus::Blocked => app
                .start_task(&task_id, &actor)
                .map(|task| format!("{} 已起飞", task.id)),
            TaskStatus::InProgress => app
                .submit_task(&task_id, &actor)
                .map(|task| format!("{} 已提交验收", task.id)),
            _ => Err(MambaError::InvalidTransition(format!(
                "当前状态 {:?} 不能继续推进",
                status
            ))),
        };
        self.finish_action(app, result);
    }

    fn complete_task(&mut self, app: &mut MambaApp) {
        let Some(actor) = self.actor_name(app).map(str::to_string) else {
            self.failure(MambaError::Validation(
                "没有可用的 Human 操作人".to_string(),
            ));
            return;
        };
        let task_id = self
            .selected_task_context(app)
            .map(|(_, task)| task.id.clone());
        let result = task_id
            .ok_or_else(|| MambaError::Validation("没有选中的任务".to_string()))
            .and_then(|task_id| app.complete_task(&task_id, &actor))
            .map(|task| format!("{} 已确认落地。Mamba Out.", task.id));
        self.finish_action(app, result);
    }

    fn open_demand_modal(&mut self) {
        if self.active_planning.is_some() {
            self.failure(MambaError::Validation(
                "已有 PRD 规划任务运行中，请等待规划结果".to_string(),
            ));
            return;
        }
        self.modal = Some(InputModal {
            purpose: InputPurpose::Demand,
            value: String::new(),
        });
    }

    fn launch_planning(&mut self, app: &MambaApp, summary: String, actor: String) {
        if self.active_planning.is_some() {
            self.failure(MambaError::Validation(
                "已有 PRD 规划任务运行中".to_string(),
            ));
            return;
        }
        let planner = self.demand_planner;
        self.active_planning = Some(ActivePlanning {
            planner,
            actor: actor.clone(),
            summary: summary.clone(),
            started_at: Local::now(),
        });
        self.success(format!(
            "{} 正在生成 PRD 与任务 DAG",
            planner_label(planner)
        ));

        let data_dir = app.data_dir().to_path_buf();
        let workspace = self.workspace.clone();
        let tx = self.planning_tx.clone();
        tokio::spawn(async move {
            let outcome = async {
                let mut worker = MambaApp::open(data_dir).map_err(|error| error.to_string())?;
                worker
                    .create_demand(&summary, &actor, planner, &workspace, 300)
                    .await
                    .map(|flow| PlannedFlow {
                        flow_id: flow.id,
                        title: flow.prd.title,
                    })
                    .map_err(|error| error.to_string())
            }
            .await;
            let _ = tx.send(PlanningResult { outcome });
        });
    }

    fn poll_planning(&mut self, app: &mut MambaApp) {
        while let Ok(result) = self.planning_rx.try_recv() {
            self.active_planning = None;
            if let Err(error) = app.reload() {
                self.failure(error);
                continue;
            }
            match result.outcome {
                Ok(flow) => {
                    self.flow_index = flow_ids(app)
                        .iter()
                        .position(|flow_id| flow_id == &flow.flow_id)
                        .unwrap_or(0);
                    self.task_index = 0;
                    self.refresh_timeline(app);
                    self.success(format!("{} 已生成：{}", flow.flow_id, flow.title));
                }
                Err(error) => {
                    self.failure(MambaError::Validation(format!("PRD 规划失败：{error}")))
                }
            }
        }
    }

    fn poll_tracking(&mut self, app: &mut MambaApp) {
        if app.state().organization.is_none() {
            return;
        }
        if self
            .last_tracking_scan
            .is_some_and(|last_scan| last_scan.elapsed() < Duration::from_secs(30))
        {
            return;
        }
        self.scan_tracker(app, false);
    }

    fn scan_tracker(&mut self, app: &mut MambaApp, announce: bool) {
        self.last_tracking_scan = Some(Instant::now());
        match app.scan_tracking(24, "tower://local") {
            Ok(scan) => {
                if !scan.raised.is_empty()
                    || !scan.resolved.is_empty()
                    || !scan.escalated.is_empty()
                    || !scan.resolved_escalations.is_empty()
                {
                    self.refresh_timeline(app);
                }
                if !scan.raised.is_empty() || !scan.escalated.is_empty() {
                    self.success(format!(
                        "Tower Tracker 新增 {} 项 Attention、{} 个 Tower Call，当前活动 {} 项",
                        scan.raised.len(),
                        scan.escalated.len(),
                        scan.active.len()
                    ));
                } else if announce || !scan.resolved.is_empty() {
                    self.success(format!(
                        "Tower Tracker 已扫描 {} 个 Todo：解除 {}，活动 {}",
                        scan.scanned_tasks,
                        scan.resolved.len(),
                        scan.active.len()
                    ));
                }
            }
            Err(error) => self.failure(error),
        }
    }

    fn poll_notification_outbox(&mut self, app: &mut MambaApp) {
        while let Ok(result) = self.notification_rx.try_recv() {
            self.active_notification = None;
            let delivered = result.attempt.delivered;
            match app.record_notification_attempt(
                &result.delivery_id,
                result.attempt,
                "tower://tui-notification-dispatcher",
            ) {
                Ok(delivery) if result.manual || !delivered => {
                    if delivered {
                        self.success(format!("{} 通知安全落地", delivery.id));
                    } else {
                        self.failure(MambaError::Validation(format!(
                            "{} 通知坠机：{}",
                            delivery.id,
                            delivery.last_error.as_deref().unwrap_or("未知错误")
                        )));
                    }
                }
                Ok(_) => {}
                Err(error) => self.failure(error),
            }
            self.last_notification_dispatch = Instant::now() - Duration::from_secs(15);
            self.refresh_timeline(app);
        }
        if self.active_notification.is_none()
            && self.last_notification_dispatch.elapsed() >= Duration::from_secs(15)
        {
            self.launch_notification(app, false);
        }
    }

    fn dispatch_notification_now(&mut self, app: &MambaApp) {
        if self.active_notification.is_some() {
            self.failure(MambaError::InvalidTransition("已有通知正在投递".into()));
        } else if !self.launch_notification(app, true) {
            self.success("Notification Outbox 当前没有待投递记录");
        }
    }

    fn launch_notification(&mut self, app: &MambaApp, manual: bool) -> bool {
        let Some((endpoint, delivery)) = app.notification_attempts(1, manual).into_iter().next()
        else {
            self.last_notification_dispatch = Instant::now();
            return false;
        };
        self.active_notification = Some(delivery.id.clone());
        self.last_notification_dispatch = Instant::now();
        let sender = self.notification_tx.clone();
        tokio::spawn(async move {
            let attempt = crate::notification::deliver(&endpoint, &delivery).await;
            let _ = sender.send(NotificationResult {
                delivery_id: delivery.id,
                attempt,
                manual,
            });
        });
        true
    }

    fn acknowledge_next_inbox(&mut self, app: &mut MambaApp) {
        let Some(actor) = self.actor_name(app).map(str::to_string) else {
            self.failure(MambaError::Validation("没有可用的 Human 操作人".into()));
            return;
        };
        let message_id = self
            .actor_messages(app)
            .into_iter()
            .find(MessageInboxItem::needs_acknowledgement)
            .map(|item| item.message.id);
        if let Some(message_id) = message_id {
            match app.acknowledge_flow_message(&message_id, &actor) {
                Ok(message) => {
                    self.refresh_timeline(app);
                    self.success(format!("{} 已收到指令 {}", actor, message.id));
                }
                Err(error) => self.failure(error),
            }
            return;
        }
        let escalation_id = self
            .actor_escalations(app)
            .into_iter()
            .find(|escalation| escalation.needs_acknowledgement())
            .map(|escalation| escalation.id.clone());
        let Some(escalation_id) = escalation_id else {
            self.failure(MambaError::Validation(
                "当前没有等待确认的指令或 Tower Call".into(),
            ));
            return;
        };
        match app.acknowledge_escalation(&escalation_id, &actor) {
            Ok(escalation) => {
                self.refresh_timeline(app);
                self.success(format!("{} 已收到呼叫 {}", actor, escalation.id));
            }
            Err(error) => self.failure(error),
        }
    }

    fn open_message_input(&mut self, app: &MambaApp) {
        let Some(actor) = self.actor_name(app) else {
            self.failure(MambaError::Validation("没有可用的 Human 操作人".into()));
            return;
        };
        let Some((flow, task)) = self.selected_task_context(app) else {
            self.failure(MambaError::Validation("请先选择一个任务再传球".into()));
            return;
        };
        let requester = app.state().principal(&flow.demand.requester).ok();
        let actor_is_requester = requester.is_some_and(|requester| requester.name == actor);
        let (kind, requires_ack, recipients) = if actor_is_requester {
            let recipients = task
                .assignment
                .iter()
                .flat_map(|assignment| {
                    std::iter::once(&assignment.owner).chain(assignment.copilots.iter())
                })
                .filter(|target| target.name != actor)
                .map(|target| target.name.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            (FlowMessageKind::Command, true, recipients)
        } else {
            (
                FlowMessageKind::Update,
                false,
                vec![flow.demand.requester.clone()],
            )
        };
        if recipients.is_empty() {
            self.failure(MambaError::Validation(
                "当前任务没有可以接球的其他成员".into(),
            ));
            return;
        }
        self.modal = Some(InputModal {
            purpose: InputPurpose::Message {
                flow_id: flow.id.clone(),
                task_id: task.id.clone(),
                recipients,
                kind,
                requires_ack,
            },
            value: String::new(),
        });
    }

    fn open_estimate_input(&mut self, app: &MambaApp) {
        let Some((_, task)) = self.selected_task_context(app) else {
            self.failure(MambaError::Validation("请先选择一个任务再调整工时".into()));
            return;
        };
        self.modal = Some(InputModal {
            purpose: InputPurpose::Negotiate {
                task_id: task.id.clone(),
                current_hours: task.estimate.effort_hours,
            },
            value: String::new(),
        });
    }

    fn open_reassign_input(&mut self, app: &MambaApp) {
        let Some(actor) = self.actor_name(app) else {
            self.failure(MambaError::Validation("没有可用的 Human 操作人".into()));
            return;
        };
        let Some((_, task)) = self.selected_task_context(app) else {
            self.failure(MambaError::Validation("请先选择一个任务再换防".into()));
            return;
        };
        match app.reassignment_candidates(&task.id, actor) {
            Ok(candidates) if !candidates.is_empty() => {
                self.modal = Some(InputModal {
                    purpose: InputPurpose::Reassign {
                        task_id: task.id.clone(),
                        candidates,
                        selected: 0,
                    },
                    value: String::new(),
                });
            }
            Ok(_) => self.failure(MambaError::Validation(
                "当前没有满足能力约束的换防候选人".into(),
            )),
            Err(error) => self.failure(error),
        }
    }

    fn open_flow_change_input(&mut self, app: &MambaApp) {
        let Some(flow) = self.selected_flow(app) else {
            self.failure(MambaError::Validation("请先选择一条 Flow".into()));
            return;
        };
        if let Some(change) = app.pending_flow_change(&flow.id) {
            self.failure(MambaError::InvalidTransition(format!(
                "{} 正在等待处理；请使用底部的批准或驳回变更",
                change.id
            )));
            return;
        }
        self.modal = Some(InputModal {
            purpose: InputPurpose::FlowChange {
                flow_id: flow.id.clone(),
            },
            value: String::new(),
        });
    }

    fn open_reject_change_input(&mut self, app: &MambaApp) {
        let Some(flow) = self.selected_flow(app) else {
            self.failure(MambaError::Validation("请先选择一条 Flow".into()));
            return;
        };
        let Some(change) = app.pending_flow_change(&flow.id) else {
            self.failure(MambaError::Validation("当前 Flow 没有待处理的变更".into()));
            return;
        };
        self.modal = Some(InputModal {
            purpose: InputPurpose::RejectChange {
                request_id: change.id.clone(),
            },
            value: String::new(),
        });
    }

    fn open_task_input(&mut self, app: &MambaApp, evidence: bool) {
        let Some((_, task)) = self.selected_task_context(app) else {
            self.failure(MambaError::Validation("没有选中的任务".to_string()));
            return;
        };
        self.modal = Some(InputModal {
            purpose: if evidence {
                InputPurpose::Evidence {
                    task_id: task.id.clone(),
                }
            } else {
                InputPurpose::Block {
                    task_id: task.id.clone(),
                }
            },
            value: String::new(),
        });
    }

    fn open_run_confirmation(&mut self, app: &MambaApp, mode: ExecutorMode) {
        if !self.active_flights.is_empty() {
            self.failure(MambaError::Validation(
                "当前已有航班在空中；v0 空域一次只允许一个执行终端".to_string(),
            ));
            return;
        }
        let Some((_, task)) = self.selected_task_context(app) else {
            self.failure(MambaError::Validation("没有选中的任务".to_string()));
            return;
        };
        if !matches!(
            task.status,
            TaskStatus::Accepted | TaskStatus::InProgress | TaskStatus::Blocked
        ) {
            self.failure(MambaError::Validation(format!(
                "任务 {} 必须先接单，当前状态为 {:?}",
                task.id, task.status
            )));
            return;
        }
        let Some((flow, _)) = self.selected_task_context(app) else {
            return;
        };
        let incomplete = task
            .depends_on
            .iter()
            .filter_map(|id| flow.task(id))
            .filter(|dependency| dependency.status != TaskStatus::Completed)
            .map(|dependency| dependency.key.as_str())
            .collect::<Vec<_>>();
        if !incomplete.is_empty() {
            self.failure(MambaError::Validation(format!(
                "仍在等待前置任务：{}",
                incomplete.join(", ")
            )));
            return;
        }
        if !task_has_executor(app, task) {
            self.failure(MambaError::Validation(
                "当前 Assignment 没有 Claude Code 或 Codex 副驾".to_string(),
            ));
            return;
        }
        self.modal = Some(InputModal {
            purpose: InputPurpose::Run {
                task_id: task.id.clone(),
                mode,
            },
            value: String::new(),
        });
    }

    fn open_recovery_input(&mut self, app: &MambaApp, action: RecoveryAction) {
        let Some(flight) = self.selected_flight(app) else {
            self.failure(MambaError::Validation(
                "请先在 Flight Deck 选择一架航班".into(),
            ));
            return;
        };
        if flight.status != FlightLeaseStatus::Crashed {
            self.failure(MambaError::InvalidTransition(
                "只有坠机航班可以进入这条恢复航线".into(),
            ));
            return;
        }
        let Some(actor) = self.actor_name(app) else {
            self.failure(MambaError::Validation("请先选择 Human 操作人".into()));
            return;
        };
        match app.recovery_options(&flight.id, actor) {
            Ok(options) if options.contains(&action) => {
                self.modal = Some(InputModal {
                    purpose: InputPurpose::RecoverFlight {
                        lease_id: flight.id.clone(),
                        action,
                    },
                    value: String::new(),
                });
            }
            Ok(_) => self.failure(MambaError::PermissionDenied(
                "FlightManifest 不允许这项恢复动作".into(),
            )),
            Err(error) => self.failure(error),
        }
    }

    fn launch_flight(
        &mut self,
        app: &MambaApp,
        task_id: String,
        actor: String,
        mode: ExecutorMode,
    ) {
        let Some((flow, _)) = app.state().find_task(&task_id).ok() else {
            self.failure(MambaError::NotFound {
                entity: "task",
                id: task_id,
            });
            return;
        };
        let flight = ActiveFlight {
            flow_id: flow.id.clone(),
            task_id: task_id.clone(),
            actor: actor.clone(),
            mode: mode.clone(),
            started_at: Local::now(),
        };
        self.active_flights.insert(task_id.clone(), flight);
        self.last_flight_reload = Instant::now();
        self.success(format!(
            "{} 航班已离场，mode={}",
            task_id,
            executor_mode_label(&mode)
        ));

        let data_dir = app.data_dir().to_path_buf();
        let tx = self.flight_tx.clone();
        tokio::spawn(async move {
            let outcome = async {
                let mut worker = MambaApp::open(data_dir).map_err(|error| error.to_string())?;
                worker
                    .run_task(&task_id, &actor, None, mode, 900)
                    .await
                    .map(|record| LandedFlight {
                        executor: record.executor.to_string(),
                        summary: record.summary,
                        log_path: record.log_path,
                    })
                    .map_err(|error| error.to_string())
            }
            .await;
            let _ = tx.send(FlightResult { task_id, outcome });
        });
    }

    fn poll_flights(&mut self, app: &mut MambaApp) {
        let mut changed = false;
        while let Ok(result) = self.flight_rx.try_recv() {
            self.active_flights.remove(&result.task_id);
            match result.outcome {
                Ok(flight) => self.success(format!(
                    "{} 安全落地 · {} · {} · {}",
                    result.task_id,
                    flight.executor,
                    compact_summary(&flight.summary, 48),
                    flight.log_path.display()
                )),
                Err(error) => self.failure(MambaError::Validation(format!(
                    "{} 坠机：{}",
                    result.task_id, error
                ))),
            }
            changed = true;
        }

        let should_reload = changed
            || (!self.active_flights.is_empty()
                && self.last_flight_reload.elapsed() >= Duration::from_millis(650));
        if should_reload {
            match app.reload() {
                Ok(()) => {
                    self.clamp_selection(app);
                    self.refresh_timeline(app);
                    self.last_flight_reload = Instant::now();
                }
                Err(error) => self.failure(error),
            }
        }
    }

    fn request_quit(&mut self) -> bool {
        if self.active_flights.is_empty() && self.active_planning.is_none() {
            true
        } else {
            self.failure(MambaError::Validation(
                "仍有规划或执行任务运行中；请等待结果写入 Flow Ledger".to_string(),
            ));
            false
        }
    }

    fn finish_action<T>(&mut self, app: &MambaApp, result: Result<T>)
    where
        T: Into<String>,
    {
        match result {
            Ok(message) => {
                self.clamp_selection(app);
                self.refresh_timeline(app);
                self.success(message.into());
            }
            Err(error) => self.failure(error),
        }
    }

    fn selected_flow<'a>(&self, app: &'a MambaApp) -> Option<&'a Flow> {
        flow_ids(app)
            .get(self.flow_index)
            .and_then(|id| app.state().flows.get(id))
    }

    fn remote_flights<'a>(&self, app: &'a MambaApp) -> Vec<&'a FlightLease> {
        let selected_flow = self.selected_flow(app).map(|flow| flow.id.as_str());
        let mut flights = app
            .state()
            .flight_leases
            .values()
            .filter(|flight| selected_flow.is_none_or(|flow_id| flight.flow_id == flow_id))
            .collect::<Vec<_>>();
        flights.sort_by_key(|flight| {
            Reverse(
                flight
                    .finished_at
                    .or(flight.claimed_at)
                    .unwrap_or(flight.issued_at),
            )
        });
        flights
    }

    fn selected_flight<'a>(&self, app: &'a MambaApp) -> Option<&'a FlightLease> {
        self.remote_flights(app).get(self.flight_index).copied()
    }

    fn inbox_items<'a>(&self, app: &'a MambaApp) -> Vec<(&'a Flow, &'a Task)> {
        self.actor_name(app)
            .and_then(|actor| app.inbox(actor).ok())
            .unwrap_or_default()
    }

    fn actor_escalations<'a>(&self, app: &'a MambaApp) -> Vec<&'a TrackingEscalation> {
        let Some(actor_id) = &self.actor_id else {
            return Vec::new();
        };
        let mut escalations = app
            .state()
            .active_escalations()
            .filter(|escalation| escalation.recipient_id == *actor_id)
            .collect::<Vec<_>>();
        escalations.sort_by(|left, right| {
            right
                .needs_acknowledgement()
                .cmp(&left.needs_acknowledgement())
                .then_with(|| right.raised_at.cmp(&left.raised_at))
        });
        escalations
    }

    fn actor_messages(&self, app: &MambaApp) -> Vec<MessageInboxItem> {
        self.actor_name(app)
            .and_then(|actor| app.message_inbox(actor, false).ok())
            .unwrap_or_default()
    }

    fn selected_task_context<'a>(&self, app: &'a MambaApp) -> Option<(&'a Flow, &'a Task)> {
        if self.view == View::Inbox {
            return self.inbox_items(app).get(self.inbox_index).copied();
        }
        self.selected_flow(app)
            .and_then(|flow| flow.tasks.get(self.task_index).map(|task| (flow, task)))
    }

    fn actor_name<'a>(&self, app: &'a MambaApp) -> Option<&'a str> {
        self.actor_id
            .as_deref()
            .and_then(|id| app.state().principals.get(id))
            .map(|principal| principal.name.as_str())
    }

    fn refresh_timeline(&mut self, app: &MambaApp) {
        self.timeline = self
            .selected_flow(app)
            .and_then(|flow| app.timeline(&flow.id).ok())
            .unwrap_or_default();
        self.timeline_index = self
            .timeline_index
            .min(self.timeline.len().saturating_sub(1));
    }

    fn clamp_selection(&mut self, app: &MambaApp) {
        self.flow_index = self.flow_index.min(flow_ids(app).len().saturating_sub(1));
        self.task_index = self.task_index.min(
            self.selected_flow(app)
                .map_or(0, |flow| flow.tasks.len())
                .saturating_sub(1),
        );
        self.inbox_index = self
            .inbox_index
            .min(self.inbox_items(app).len().saturating_sub(1));
        self.flight_index = self
            .flight_index
            .min(self.remote_flights(app).len().min(5).saturating_sub(1));
    }

    fn success(&mut self, message: impl Into<String>) {
        self.message = message.into();
        self.message_is_error = false;
    }

    fn failure(&mut self, error: MambaError) {
        self.message = error.to_string();
        self.message_is_error = true;
    }
}

fn render(frame: &mut Frame, app: &MambaApp, state: &mut UiState) {
    state.hit_regions.clear();
    frame.render_widget(Block::default().style(Style::new().bg(BG)), frame.area());
    let [header, tabs, content, status, actions] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(10),
        Constraint::Length(3),
        Constraint::Length(3),
    ])
    .areas(frame.area());

    render_header(frame, app, state, header);
    render_tabs(frame, state, tabs);
    match state.view {
        View::Overview => render_overview(frame, app, state, content),
        View::Flows => render_flows(frame, app, state, content),
        View::Inbox => render_inbox(frame, app, state, content),
        View::Roster => render_roster(frame, app, state, content),
        View::Timeline => render_timeline(frame, app, state, content),
    }
    render_status(frame, state, status);
    render_actions(frame, app, state, actions);

    if let Some(modal) = state.modal.clone() {
        render_input_modal(frame, &modal, state);
    }
}

fn render_header(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    let organization = app
        .state()
        .organization
        .as_ref()
        .map(|org| org.name.as_str())
        .unwrap_or("NO ORGANIZATION");
    let actor = state.actor_name(app).unwrap_or("READ ONLY");
    let remote_flights = build_dashboard(app.state()).metrics.open_flights;
    let [brand, context, clock] = Layout::horizontal([
        Constraint::Length(32),
        Constraint::Min(24),
        Constraint::Length(22),
    ])
    .areas(area);
    state.register_hit(context, HitTarget::Action(MouseAction::CycleActor));
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" MAMBA", Style::new().fg(GOLD).bold()),
            Span::styled("FLOW ", Style::new().fg(TEXT).bold()),
            Span::styled("TOWER", Style::new().fg(PURPLE).bold()),
        ]))
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(dim_border()),
        )
        .alignment(Alignment::Left),
        brand,
    );
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(organization, Style::new().fg(TEXT).bold()),
            Span::styled("  /  球权 ", Style::new().fg(MUTED)),
            Span::styled(actor, Style::new().fg(CYAN).bold()),
            Span::styled("  /  航班 ", Style::new().fg(MUTED)),
            Span::styled(
                (state.active_flights.len() + remote_flights).to_string(),
                Style::new()
                    .fg(if state.active_flights.is_empty() && remote_flights == 0 {
                        GREEN
                    } else {
                        ORANGE
                    })
                    .bold(),
            ),
            Span::styled("  /  规划 ", Style::new().fg(MUTED)),
            Span::styled(
                if state.active_planning.is_some() {
                    "1"
                } else {
                    "0"
                },
                Style::new()
                    .fg(if state.active_planning.is_some() {
                        ORANGE
                    } else {
                        GREEN
                    })
                    .bold(),
            ),
        ]))
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(dim_border()),
        )
        .alignment(Alignment::Center),
        context,
    );
    frame.render_widget(
        Paragraph::new(Local::now().format("%Y-%m-%d  %H:%M:%S").to_string())
            .style(Style::new().fg(MUTED))
            .block(
                Block::default()
                    .borders(Borders::BOTTOM)
                    .border_style(dim_border()),
            )
            .alignment(Alignment::Right),
        clock,
    );
}

fn render_tabs(frame: &mut Frame, state: &mut UiState, area: Rect) {
    let titles = View::ALL
        .iter()
        .enumerate()
        .map(|(index, view)| format!(" {} {} ", index + 1, view.title()))
        .collect::<Vec<_>>();
    let mut x = area.x;
    for (index, title) in titles.iter().enumerate() {
        let width = Line::from(title.as_str()).width() as u16;
        state.register_hit(
            Rect::new(
                x,
                area.y,
                width.min(area.right().saturating_sub(x)),
                area.height,
            ),
            HitTarget::Tab(View::ALL[index]),
        );
        x = x.saturating_add(width).saturating_add(3);
    }
    let tabs = Tabs::new(titles)
        .select(state.view.index())
        .style(Style::new().fg(MUTED).bg(BG))
        .highlight_style(Style::new().fg(BG).bg(GOLD).bold())
        .divider(Span::styled(" · ", Style::new().fg(PURPLE)))
        .padding("", "")
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(dim_border()),
        );
    frame.render_widget(tabs, area);
}

fn render_overview(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    let compact = area.height < 20;
    let [metrics, main] = Layout::vertical([
        Constraint::Length(if compact { 4 } else { 5 }),
        Constraint::Min(6),
    ])
    .spacing(1)
    .areas(area);
    let metric_areas = Layout::horizontal([Constraint::Ratio(1, 6); 6])
        .spacing(1)
        .split(metrics);
    let dashboard = build_dashboard(app.state());
    let risk = dashboard.metrics.at_risk_tasks;
    render_metric(
        frame,
        metric_areas[0],
        "ACTIVE FLOWS",
        dashboard.metrics.active_flows.to_string(),
        CYAN,
    );
    render_metric(
        frame,
        metric_areas[1],
        "TASK PROGRESS",
        format!(
            "{}/{}",
            dashboard.metrics.completed_tasks, dashboard.metrics.total_tasks
        ),
        GREEN,
    );
    render_metric(
        frame,
        metric_areas[2],
        "AT RISK",
        risk.to_string(),
        if risk == 0 { GREEN } else { RED },
    );
    render_metric(
        frame,
        metric_areas[3],
        "WAITING HUMAN",
        dashboard.metrics.awaiting_human.to_string(),
        if dashboard.metrics.awaiting_human == 0 {
            GREEN
        } else {
            ORANGE
        },
    );
    render_metric(
        frame,
        metric_areas[4],
        "OPEN FLIGHTS",
        (dashboard.metrics.open_flights + state.active_flights.len()).to_string(),
        GOLD,
    );
    let outbox = dashboard.metrics.pending_notifications + dashboard.metrics.failed_notifications;
    render_metric(
        frame,
        metric_areas[5],
        "OUTBOX",
        outbox.to_string(),
        if dashboard.metrics.failed_notifications > 0 {
            RED
        } else if outbox > 0 {
            ORANGE
        } else {
            GREEN
        },
    );

    if compact {
        let [flows, actions] =
            Layout::horizontal([Constraint::Percentage(62), Constraint::Percentage(38)])
                .spacing(1)
                .areas(main);
        render_flow_table(frame, app, state, flows, true);
        render_action_queue(frame, app, actions);
    } else if area.width >= 100 {
        let [left, right] =
            Layout::horizontal([Constraint::Percentage(60), Constraint::Percentage(40)])
                .spacing(1)
                .areas(main);
        render_flow_table(frame, app, state, left, true);
        let [brief, actions] =
            Layout::vertical([Constraint::Percentage(57), Constraint::Percentage(43)])
                .spacing(1)
                .areas(right);
        render_tower_brief(frame, app, state.selected_flow(app), brief);
        render_action_queue(frame, app, actions);
    } else {
        let [top, bottom] =
            Layout::vertical([Constraint::Percentage(55), Constraint::Percentage(45)])
                .spacing(1)
                .areas(main);
        render_flow_table(frame, app, state, top, true);
        let [brief, actions] = Layout::horizontal([Constraint::Percentage(50); 2])
            .spacing(1)
            .areas(bottom);
        render_tower_brief(frame, app, state.selected_flow(app), brief);
        render_action_queue(frame, app, actions);
    }
}

fn render_metric(frame: &mut Frame, area: Rect, label: &str, value: String, color: Color) {
    let content = Text::from(vec![
        Line::styled(value, Style::new().fg(color).bold()),
        Line::styled(label, Style::new().fg(MUTED)),
    ]);
    frame.render_widget(
        Paragraph::new(content)
            .alignment(Alignment::Center)
            .block(panel_block("", false)),
        area,
    );
}

fn render_flow_table(
    frame: &mut Frame,
    app: &MambaApp,
    state: &mut UiState,
    area: Rect,
    focused: bool,
) {
    let ids = flow_ids(app);
    let dashboard = build_dashboard(app.state());
    let compact_columns = area.width < 70;
    let rows = ids
        .iter()
        .filter_map(|id| app.state().flows.get(id))
        .map(|flow| {
            let title = if app.pending_flow_change(&flow.id).is_some() {
                format!("[CHG] {}", flow.prd.title)
            } else {
                flow.prd.title.clone()
            };
            let health = dashboard
                .flows
                .iter()
                .find(|summary| summary.id == flow.id)
                .map(|summary| summary.health)
                .unwrap_or(FlowHealth::Draft);
            let completed = flow
                .tasks
                .iter()
                .filter(|task| task.status == TaskStatus::Completed)
                .count();
            let cells = if compact_columns {
                vec![
                    Cell::from(flow_health_label(health)).style(flow_health_style(health)),
                    Cell::from(title.clone()).style(Style::new().fg(
                        if title.starts_with("[CHG]") {
                            ORANGE
                        } else {
                            TEXT
                        },
                    )),
                    Cell::from(format!("{completed}/{}", flow.tasks.len()))
                        .style(Style::new().fg(CYAN)),
                ]
            } else {
                vec![
                    Cell::from(flow_health_label(health)).style(flow_health_style(health)),
                    Cell::from(flow.id.clone()).style(Style::new().fg(MUTED)),
                    Cell::from(title.clone()).style(Style::new().fg(
                        if title.starts_with("[CHG]") {
                            ORANGE
                        } else {
                            TEXT
                        },
                    )),
                    Cell::from(format!("{completed}/{}", flow.tasks.len()))
                        .style(Style::new().fg(CYAN)),
                    Cell::from(flow.p80_finish.format("%m-%d %H:%M").to_string())
                        .style(Style::new().fg(MUTED)),
                ]
            };
            Row::new(cells).height(1)
        })
        .collect::<Vec<_>>();
    let (headers, widths) = if compact_columns {
        (
            vec!["健康", "目标", "落地"],
            vec![
                Constraint::Length(9),
                Constraint::Min(16),
                Constraint::Length(6),
            ],
        )
    } else {
        (
            vec!["健康", "FLOW", "目标", "落地", "P80"],
            vec![
                Constraint::Length(10),
                Constraint::Length(14),
                Constraint::Min(18),
                Constraint::Length(7),
                Constraint::Length(12),
            ],
        )
    };
    let header = Row::new(headers)
        .style(Style::new().fg(GOLD).bold())
        .bottom_margin(1);
    let table = Table::new(rows, widths)
        .header(header)
        .row_highlight_style(Style::new().bg(PANEL_ALT).fg(TEXT).bold())
        .highlight_symbol("◆ ")
        .highlight_spacing(HighlightSpacing::Always)
        .column_spacing(1)
        .block(panel_block("FLOW BOARD", focused));
    let mut table_state = TableState::default();
    table_state.select((!ids.is_empty()).then_some(state.flow_index));
    register_table_rows(area, state.flow_index, ids.len(), HitTarget::Flow, state);
    frame.render_stateful_widget(table, area, &mut table_state);
}

fn render_tower_brief(frame: &mut Frame, app: &MambaApp, flow: Option<&Flow>, area: Rect) {
    let Some(flow) = flow else {
        frame.render_widget(
            Paragraph::new("点击 SHOWCASE 装载演示机队，或点击底部的新需求。")
                .style(Style::new().fg(MUTED))
                .alignment(Alignment::Center)
                .block(panel_block("TOWER BRIEF", false)),
            area,
        );
        return;
    };
    let mut attentions = app
        .state()
        .active_attentions()
        .filter(|attention| attention.flow_id == flow.id)
        .collect::<Vec<_>>();
    attentions.sort_by(|left, right| {
        right
            .severity
            .cmp(&left.severity)
            .then_with(|| left.raised_at.cmp(&right.raised_at))
    });
    let attention_count = attentions.len();
    let completed = flow
        .tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Completed)
        .count();
    let ratio = if flow.tasks.is_empty() {
        0.0
    } else {
        completed as f64 / flow.tasks.len() as f64
    };
    let dashboard = build_dashboard(app.state());
    let next_action = dashboard
        .action_items
        .iter()
        .find(|action| action.flow_id == flow.id);
    let external_actions = app
        .state()
        .external_interactions
        .values()
        .filter(|receipt| receipt.flow_id.as_deref() == Some(flow.id.as_str()))
        .count();
    let active_identities = app
        .state()
        .external_identities
        .values()
        .filter(|binding| binding.is_active())
        .count();
    let [brief, gauge] = Layout::vertical([Constraint::Min(5), Constraint::Length(3)]).areas(area);
    let lines = vec![
        Line::styled(flow.prd.title.clone(), Style::new().fg(TEXT).bold()),
        Line::from(vec![
            Span::styled("发起人  ", Style::new().fg(MUTED)),
            Span::styled(flow.demand.requester.clone(), Style::new().fg(GOLD)),
        ]),
        Line::from(vec![
            Span::styled("落地窗口  ", Style::new().fg(MUTED)),
            Span::styled(
                format!(
                    "P50 {} · P80 {}",
                    flow.p50_finish.format("%m-%d %H:%M"),
                    flow.p80_finish.format("%m-%d %H:%M")
                ),
                Style::new().fg(CYAN),
            ),
        ]),
        Line::from(vec![
            Span::styled("下一动作  ", Style::new().fg(MUTED)),
            Span::styled(
                next_action
                    .map(|action| {
                        format!(
                            "{} · {}",
                            action.owner,
                            compact_summary(&action.task_title, 28)
                        )
                    })
                    .unwrap_or_else(|| "无需管理动作".into()),
                Style::new().fg(if next_action.is_some() { ORANGE } else { GREEN }),
            ),
        ]),
        Line::from(vec![
            Span::styled("场外回执  ", Style::new().fg(MUTED)),
            Span::styled(
                format!("{external_actions} verified · {active_identities} identities"),
                Style::new().fg(if external_actions == 0 { MUTED } else { GREEN }),
            ),
        ]),
        Line::from(vec![
            Span::styled("风险  ", Style::new().fg(MUTED)),
            Span::styled(
                if attention_count == 0 {
                    "空域正常".to_string()
                } else {
                    format!(
                        "{} 项 · {}",
                        attention_count,
                        compact_summary(&attentions[0].summary, 30)
                    )
                },
                Style::new().fg(if attention_count == 0 { GREEN } else { RED }),
            ),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: true })
            .block(panel_block("TOWER BRIEF", false)),
        brief,
    );
    frame.render_widget(
        Gauge::default()
            .block(panel_block("LANDING PROGRESS", false))
            .gauge_style(Style::new().fg(GREEN).bg(PANEL_ALT).bold())
            .ratio(ratio.clamp(0.0, 1.0))
            .label(format!("{completed}/{} tasks", flow.tasks.len())),
        gauge,
    );
}

fn render_action_queue(frame: &mut Frame, app: &MambaApp, area: Rect) {
    let dashboard = build_dashboard(app.state());
    let max_items = usize::from(area.height.saturating_sub(2) / 2).max(1);
    let mut items = dashboard
        .office_releases
        .iter()
        .filter(|release| {
            matches!(
                release.status,
                OfficeReleaseStatus::Requested
                    | OfficeReleaseStatus::Failed
                    | OfficeReleaseStatus::Indeterminate
            )
        })
        .take(max_items)
        .map(|release| {
            let (marker, color, reason) = match release.status {
                OfficeReleaseStatus::Requested => (
                    "! ",
                    GOLD,
                    format!(
                        "等待 Human 放行 · SHA {}",
                        release.payload_sha256.chars().take(12).collect::<String>()
                    ),
                ),
                OfficeReleaseStatus::Failed => (
                    "!!",
                    RED,
                    release
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "Office Bridge 发布失败".into()),
                ),
                OfficeReleaseStatus::Indeterminate => {
                    ("??", ORANGE, "Provider 结果不确定，禁止自动重试".into())
                }
                _ => unreachable!(),
            };
            ListItem::new(Text::from(vec![
                Line::from(vec![
                    Span::styled(format!("{marker} "), Style::new().fg(color).bold()),
                    Span::styled(
                        compact_summary(&release.summary, 26),
                        Style::new().fg(TEXT).bold(),
                    ),
                    Span::styled(format!("  {}", release.provider), Style::new().fg(MUTED)),
                ]),
                Line::styled(
                    format!("   {}", compact_summary(&reason, 42)),
                    Style::new().fg(color),
                ),
            ]))
        })
        .collect::<Vec<_>>();
    let remaining = max_items.saturating_sub(items.len());
    items.extend(
        dashboard
            .action_items
            .iter()
            .take(remaining)
            .map(|action| {
                let (marker, color) = match action.priority {
                    ActionPriority::Critical => ("!!", RED),
                    ActionPriority::High => ("! ", ORANGE),
                    ActionPriority::Normal => ("· ", CYAN),
                };
                ListItem::new(Text::from(vec![
                    Line::from(vec![
                        Span::styled(format!("{marker} "), Style::new().fg(color).bold()),
                        Span::styled(
                            compact_summary(&action.task_title, 26),
                            Style::new().fg(TEXT).bold(),
                        ),
                        Span::styled(format!("  {}", action.owner), Style::new().fg(MUTED)),
                    ]),
                    Line::styled(
                        format!("   {}", compact_summary(&action.reason, 42)),
                        Style::new().fg(color),
                    ),
                ]))
            })
            .collect::<Vec<_>>(),
    );
    if items.is_empty() {
        items.push(ListItem::new(Line::styled(
            "当前没有需要管理者介入的事项",
            Style::new().fg(GREEN),
        )));
    }
    frame.render_widget(
        List::new(items).block(panel_block("ACTION QUEUE / 管理动作", false)),
        area,
    );
}

fn render_flows(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    if area.width < 92 {
        if area.height < 18 {
            let [flows, tasks] = Layout::vertical([Constraint::Length(6), Constraint::Min(6)])
                .spacing(1)
                .areas(area);
            render_flow_selector(frame, app, state, flows, !state.focus_tasks);
            render_tasks(frame, state, app, tasks, state.focus_tasks);
            return;
        }
        let [flows, prd, tasks] = Layout::vertical([
            Constraint::Percentage(28),
            Constraint::Percentage(27),
            Constraint::Percentage(45),
        ])
        .spacing(1)
        .areas(area);
        render_flow_selector(frame, app, state, flows, !state.focus_tasks);
        render_prd(
            frame,
            state.selected_flow(app),
            state
                .selected_flow(app)
                .and_then(|flow| app.pending_flow_change(&flow.id)),
            prd,
        );
        render_tasks(frame, state, app, tasks, state.focus_tasks);
        return;
    }
    let [left, right] =
        Layout::horizontal([Constraint::Percentage(36), Constraint::Percentage(64)])
            .spacing(1)
            .areas(area);
    render_flow_selector(frame, app, state, left, !state.focus_tasks);
    let [prd, tasks, detail] = Layout::vertical([
        Constraint::Percentage(31),
        Constraint::Percentage(39),
        Constraint::Percentage(30),
    ])
    .spacing(1)
    .areas(right);
    render_prd(
        frame,
        state.selected_flow(app),
        state
            .selected_flow(app)
            .and_then(|flow| app.pending_flow_change(&flow.id)),
        prd,
    );
    render_tasks(frame, state, app, tasks, state.focus_tasks);
    render_task_detail(
        frame,
        state.selected_task_context(app).map(|(_, task)| task),
        detail,
    );
}

fn render_prd(
    frame: &mut Frame,
    flow: Option<&Flow>,
    pending_change: Option<&crate::domain::FlowChangeRequest>,
    area: Rect,
) {
    let text = flow.map_or_else(
        || Text::from("还没有 Flow。点击 SHOWCASE 装载演示，或点击底部的新需求。"),
        |flow| {
            let mut lines = vec![
                Line::styled(flow.prd.title.clone(), Style::new().fg(GOLD).bold()),
                Line::styled(flow.prd.summary.clone(), Style::new().fg(TEXT)),
                Line::raw(""),
            ];
            if let Some(change) = pending_change {
                lines.push(Line::from(vec![
                    Span::styled("CHANGE  ", Style::new().fg(PURPLE).bold()),
                    Span::styled(
                        format!(
                            "{} · +{} tasks · 进度 {:+.1}h + 范围 {:+.1}h = 净 {:+.1}h · 底部处理",
                            change.id,
                            change.new_tasks.len(),
                            change.impact.baseline_p80_delta_hours,
                            change.impact.scope_p80_delta_hours,
                            change.impact.net_p80_delta_hours
                        ),
                        Style::new().fg(ORANGE),
                    ),
                ]));
            }
            lines.extend(flow.prd.acceptance_criteria.iter().map(|criterion| {
                Line::from(vec![
                    Span::styled("✓ ", Style::new().fg(GREEN)),
                    Span::styled(criterion.clone(), Style::new().fg(MUTED)),
                ])
            }));
            Text::from(lines)
        },
    );
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .block(panel_block("PRD / LANDING CONTRACT", false)),
        area,
    );
}

fn render_flow_selector(
    frame: &mut Frame,
    app: &MambaApp,
    state: &mut UiState,
    area: Rect,
    focused: bool,
) {
    let ids = flow_ids(app);
    let rows = ids
        .iter()
        .filter_map(|id| app.state().flows.get(id))
        .map(|flow| {
            let completed = flow
                .tasks
                .iter()
                .filter(|task| task.status == TaskStatus::Completed)
                .count();
            Row::new(vec![
                Cell::from(flow_status_label(&flow.status)).style(flow_status_style(&flow.status)),
                Cell::from(if app.pending_flow_change(&flow.id).is_some() {
                    format!("[CHG] {}", flow.prd.title)
                } else {
                    flow.prd.title.clone()
                })
                .style(Style::new().fg(
                    if app.pending_flow_change(&flow.id).is_some() {
                        ORANGE
                    } else {
                        TEXT
                    },
                )),
                Cell::from(format!("{completed}/{}", flow.tasks.len()))
                    .style(Style::new().fg(CYAN)),
            ])
        })
        .collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(9),
            Constraint::Min(12),
            Constraint::Length(5),
        ],
    )
    .header(
        Row::new(["状态", "目标", "落地"])
            .style(Style::new().fg(GOLD).bold())
            .bottom_margin(1),
    )
    .row_highlight_style(Style::new().bg(PANEL_ALT).fg(TEXT).bold())
    .highlight_symbol("◆ ")
    .highlight_spacing(HighlightSpacing::Always)
    .column_spacing(1)
    .block(panel_block("FLOW SELECTOR", focused));
    let mut table_state = TableState::default();
    table_state.select((!ids.is_empty()).then_some(state.flow_index));
    register_table_rows(area, state.flow_index, ids.len(), HitTarget::Flow, state);
    frame.render_stateful_widget(table, area, &mut table_state);
}

fn render_tasks(frame: &mut Frame, state: &mut UiState, app: &MambaApp, area: Rect, focused: bool) {
    let tasks = state
        .selected_flow(app)
        .map(|flow| flow.tasks.as_slice())
        .unwrap_or(&[]);
    let rows = tasks.iter().map(task_row).collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(13),
            Constraint::Min(18),
            Constraint::Length(14),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(["状态", "TASK", "任务", "OWNER", "P50/P80"])
            .style(Style::new().fg(GOLD).bold())
            .bottom_margin(1),
    )
    .row_highlight_style(Style::new().bg(PANEL_ALT).fg(TEXT).bold())
    .highlight_symbol("▶ ")
    .highlight_spacing(HighlightSpacing::Always)
    .column_spacing(1)
    .block(panel_block("FLIGHT MANIFESTS", focused));
    let mut table_state = TableState::default();
    table_state.select((!tasks.is_empty()).then_some(state.task_index));
    register_table_rows(area, state.task_index, tasks.len(), HitTarget::Task, state);
    frame.render_stateful_widget(table, area, &mut table_state);
}

fn render_inbox(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    let escalations = state.actor_escalations(app);
    let messages = state.actor_messages(app);
    let (comms_area, table_area, detail_area) = if escalations.is_empty() && messages.is_empty() {
        let [table, detail] =
            Layout::vertical([Constraint::Percentage(58), Constraint::Percentage(42)])
                .spacing(1)
                .areas(area);
        (None, table, detail)
    } else {
        let [calls, table, detail] = Layout::vertical([
            Constraint::Length(6),
            Constraint::Percentage(48),
            Constraint::Min(7),
        ])
        .spacing(1)
        .areas(area);
        (Some(calls), table, detail)
    };
    if let Some(comms_area) = comms_area {
        render_inbox_comms(frame, app, &messages, &escalations, comms_area);
    }
    let items = state.inbox_items(app);
    let rows = items
        .iter()
        .map(|(flow, task)| {
            let mut row = task_row(task);
            row = row.bottom_margin(0);
            let _ = flow;
            row
        })
        .collect::<Vec<_>>();
    let title = format!("INBOX / {}", state.actor_name(app).unwrap_or("READ ONLY"));
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(13),
            Constraint::Min(22),
            Constraint::Length(14),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(["状态", "TASK", "任务", "OWNER", "P50/P80"])
            .style(Style::new().fg(GOLD).bold())
            .bottom_margin(1),
    )
    .row_highlight_style(Style::new().bg(PANEL_ALT).fg(TEXT).bold())
    .highlight_symbol("● ")
    .highlight_spacing(HighlightSpacing::Always)
    .column_spacing(1)
    .block(panel_block(&title, true));
    let mut table_state = TableState::default();
    table_state.select((!items.is_empty()).then_some(state.inbox_index));
    register_table_rows(
        table_area,
        state.inbox_index,
        items.len(),
        HitTarget::Inbox,
        state,
    );
    frame.render_stateful_widget(table, table_area, &mut table_state);
    render_task_detail(
        frame,
        items.get(state.inbox_index).map(|(_, task)| *task),
        detail_area,
    );
}

fn render_inbox_comms(
    frame: &mut Frame,
    app: &MambaApp,
    messages: &[MessageInboxItem],
    escalations: &[&TrackingEscalation],
    area: Rect,
) {
    let mut items = messages
        .iter()
        .map(|item| {
            let (status, color) = if item.needs_acknowledgement() {
                ("! 指令", GOLD)
            } else {
                ("· 回传", CYAN)
            };
            ListItem::new(Line::from(vec![
                Span::styled(format!("{status:<9}"), Style::new().fg(color).bold()),
                Span::styled(
                    format!("{}  ", item.message.sender_name),
                    Style::new().fg(MUTED),
                ),
                Span::styled(
                    compact_summary(&item.message.body, 56),
                    Style::new().fg(TEXT),
                ),
            ]))
        })
        .collect::<Vec<_>>();
    items.extend(escalations.iter().map(|escalation| {
        let severity = app
            .state()
            .attentions
            .get(&escalation.attention_id)
            .map(|attention| attention.severity)
            .unwrap_or(AttentionSeverity::Warning);
        let (status, color) = if escalation.needs_acknowledgement() {
            match severity {
                AttentionSeverity::Critical => ("! CRITICAL", RED),
                AttentionSeverity::Warning => ("! WARNING", ORANGE),
            }
        } else {
            ("✓ RECEIVED", GREEN)
        };
        ListItem::new(Line::from(vec![
            Span::styled(format!("{status:<12}"), Style::new().fg(color).bold()),
            Span::styled(format!("{}  ", escalation.task_id), Style::new().fg(MUTED)),
            Span::styled(
                compact_summary(&escalation.reason, 52),
                Style::new().fg(TEXT),
            ),
        ]))
    }));
    items.truncate(area.height.saturating_sub(2) as usize);
    frame.render_widget(
        List::new(items).block(panel_block(
            &format!(
                "TOWER COMMS / {} 指令 · {} 呼叫 · g 收到",
                messages.len(),
                escalations.len()
            ),
            false,
        )),
        area,
    );
}

fn task_row(task: &Task) -> Row<'static> {
    let owner = task
        .assignment
        .as_ref()
        .map(|assignment| assignment.owner.name.clone())
        .unwrap_or_else(|| "未分配".to_string());
    Row::new(vec![
        Cell::from(task_status_label(&task.status)).style(task_status_style(&task.status)),
        Cell::from(task.id.clone()).style(Style::new().fg(MUTED)),
        Cell::from(task.title.clone()).style(Style::new().fg(TEXT)),
        Cell::from(owner).style(Style::new().fg(CYAN)),
        Cell::from(format!(
            "{:.1}/{:.1}h",
            task.estimate.p50_hours, task.estimate.p80_hours
        ))
        .style(Style::new().fg(MUTED)),
    ])
}

fn render_task_detail(frame: &mut Frame, task: Option<&Task>, area: Rect) {
    let Some(task) = task else {
        frame.render_widget(
            Paragraph::new("当前 Inbox 没有待处理任务。")
                .style(Style::new().fg(MUTED))
                .alignment(Alignment::Center)
                .block(panel_block("TASK DETAIL", false)),
            area,
        );
        return;
    };
    let copilots = task
        .assignment
        .as_ref()
        .map(|assignment| {
            assignment
                .copilots
                .iter()
                .map(|target| target.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "无".to_string());
    let mut lines = vec![
        Line::styled(task.title.clone(), Style::new().fg(TEXT).bold()),
        Line::styled(task.description.clone(), Style::new().fg(MUTED)),
        Line::from(vec![
            Span::styled("副驾  ", Style::new().fg(MUTED)),
            Span::styled(copilots, Style::new().fg(CYAN)),
            Span::styled("    Evidence  ", Style::new().fg(MUTED)),
            Span::styled(task.evidence.len().to_string(), Style::new().fg(GREEN)),
            Span::styled("    Artifacts  ", Style::new().fg(MUTED)),
            Span::styled(
                task.external_artifacts.len().to_string(),
                Style::new().fg(CYAN),
            ),
        ]),
        Line::from(vec![
            Span::styled("计划  ", Style::new().fg(MUTED)),
            Span::styled(
                format!(
                    "基础 {:.1}h · 起始 {} · P80 {}",
                    task.estimate.effort_hours,
                    task.estimate.earliest_start.format("%m-%d %H:%M"),
                    task.estimate.p80_finish.format("%m-%d %H:%M")
                ),
                Style::new().fg(CYAN),
            ),
        ]),
    ];
    if let Some(blocker) = &task.blocker {
        lines.push(Line::from(vec![
            Span::styled("BLOCKER  ", Style::new().fg(RED).bold()),
            Span::styled(blocker.clone(), Style::new().fg(RED)),
        ]));
    }
    if let Some(calendar) = task
        .estimate
        .rationale
        .iter()
        .find(|reason| reason.starts_with("work calendar:"))
    {
        lines.push(Line::from(vec![
            Span::styled("日历  ", Style::new().fg(MUTED)),
            Span::styled(
                calendar.trim_start_matches("work calendar: ").to_string(),
                Style::new().fg(ORANGE),
            ),
        ]));
    }
    lines.extend(
        task.external_artifacts
            .iter()
            .rev()
            .take(3)
            .map(|artifact| {
                Line::from(vec![
                    Span::styled(
                        format!("{}:{}  ", artifact.provider, artifact.kind),
                        Style::new().fg(CYAN),
                    ),
                    Span::styled(
                        format!("#{} {}  ", artifact.external_id, artifact.status),
                        Style::new().fg(if artifact.verified { GREEN } else { ORANGE }),
                    ),
                    Span::styled(compact_summary(&artifact.title, 42), Style::new().fg(MUTED)),
                ])
            }),
    );
    lines.extend(task.acceptance_criteria.iter().map(|criterion| {
        Line::from(vec![
            Span::styled("□ ", Style::new().fg(GOLD)),
            Span::styled(criterion.clone(), Style::new().fg(TEXT)),
        ])
    }));
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: true })
            .block(panel_block("TASK DETAIL / LANDING CONDITIONS", false)),
        area,
    );
}

fn render_roster(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    let [teams, principals] =
        Layout::horizontal([Constraint::Percentage(34), Constraint::Percentage(66)])
            .spacing(1)
            .areas(area);
    let team_items = app
        .state()
        .teams
        .values()
        .map(|team| {
            let count = app
                .state()
                .principals
                .values()
                .filter(|principal| principal.team_id.as_deref() == Some(team.id.as_str()))
                .count();
            ListItem::new(Line::from(vec![
                Span::styled("◆ ", Style::new().fg(PURPLE)),
                Span::styled(team.name.clone(), Style::new().fg(TEXT).bold()),
                Span::styled(format!("  {count}"), Style::new().fg(MUTED)),
            ]))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(team_items).block(panel_block("TEAMS / 球队", false)),
        teams,
    );

    let principals_vec = app.state().principals.values().collect::<Vec<_>>();
    let wide_roster = principals.width >= 110;
    let rows = principals_vec
        .iter()
        .map(|principal| {
            let team = principal
                .team_id
                .as_deref()
                .and_then(|id| app.state().teams.get(id))
                .map(|team| team.name.as_str())
                .unwrap_or("-");
            let terminal = principal
                .executor
                .as_ref()
                .map(|executor| executor.kind.to_string())
                .unwrap_or_else(|| match principal.kind {
                    PrincipalKind::Human => "human".to_string(),
                    PrincipalKind::Agent => "remote-worker".to_string(),
                });
            let calendar = app.state().work_calendar(&principal.id).ok();
            let calendar_label = calendar.map_or_else(
                || "未配置".into(),
                |calendar| {
                    let active_time_off = calendar
                        .time_off
                        .iter()
                        .filter(|block| block.is_active() && block.ends_at > chrono::Utc::now())
                        .count();
                    let summary = crate::calendar::summary(calendar);
                    if active_time_off == 0 {
                        summary
                    } else {
                        format!("{summary} · OFF {active_time_off}")
                    }
                },
            );
            let name = Cell::from(principal.name.clone()).style(Style::new().fg(TEXT).bold());
            let kind = Cell::from(match principal.kind {
                PrincipalKind::Human => "HUMAN",
                PrincipalKind::Agent => "AGENT",
            })
            .style(Style::new().fg(match principal.kind {
                PrincipalKind::Human => GOLD,
                PrincipalKind::Agent => CYAN,
            }));
            let capacity = Cell::from(format!("{}%", principal.capacity_percent))
                .style(Style::new().fg(GREEN));
            let calendar = Cell::from(calendar_label).style(Style::new().fg(ORANGE));
            let terminal = Cell::from(terminal).style(Style::new().fg(PURPLE));
            if wide_roster {
                Row::new(vec![
                    name,
                    kind,
                    Cell::from(team.to_string()).style(Style::new().fg(MUTED)),
                    capacity,
                    calendar,
                    terminal,
                    Cell::from(principal.capabilities.join(", ")).style(Style::new().fg(MUTED)),
                ])
            } else {
                Row::new(vec![name, kind, capacity, calendar, terminal])
            }
        })
        .collect::<Vec<_>>();
    let (widths, headers): (Vec<Constraint>, Vec<&str>) = if wide_roster {
        (
            vec![
                Constraint::Length(18),
                Constraint::Length(8),
                Constraint::Length(14),
                Constraint::Length(7),
                Constraint::Length(25),
                Constraint::Length(13),
                Constraint::Min(16),
            ],
            vec!["成员", "类型", "球队", "产能", "工作日历", "终端", "能力"],
        )
    } else {
        (
            vec![
                Constraint::Length(16),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Min(22),
                Constraint::Length(13),
            ],
            vec!["成员", "类型", "产能", "工作日历", "终端"],
        )
    };
    let table = Table::new(rows, widths)
        .header(
            Row::new(headers)
                .style(Style::new().fg(GOLD).bold())
                .bottom_margin(1),
        )
        .row_highlight_style(Style::new().bg(PANEL_ALT).fg(TEXT).bold())
        .highlight_symbol("24 ")
        .highlight_spacing(HighlightSpacing::Always)
        .column_spacing(1)
        .block(panel_block("ROSTER / 轮换阵容", true));
    let mut table_state = TableState::default();
    table_state.select((!principals_vec.is_empty()).then_some(state.roster_index));
    register_table_rows(
        principals,
        state.roster_index,
        principals_vec.len(),
        HitTarget::Principal,
        state,
    );
    frame.render_stateful_widget(table, principals, &mut table_state);
}

fn render_timeline(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    let (ledger_area, flights_area) = if area.width >= 96 {
        let [ledger, flights] =
            Layout::horizontal([Constraint::Percentage(68), Constraint::Percentage(32)])
                .spacing(1)
                .areas(area);
        (ledger, flights)
    } else {
        let [ledger, flights] =
            Layout::vertical([Constraint::Percentage(67), Constraint::Percentage(33)])
                .spacing(1)
                .areas(area);
        (ledger, flights)
    };
    let items = state
        .timeline
        .iter()
        .map(|event| {
            let style = event_style(&event.kind);
            ListItem::new(Line::from(vec![
                Span::styled(format!("#{:<4} ", event.sequence), Style::new().fg(MUTED)),
                Span::styled(
                    event.occurred_at.format("%m-%d %H:%M:%S ").to_string(),
                    Style::new().fg(MUTED),
                ),
                Span::styled(format!("{:<28}", event.kind), style),
                Span::styled(event.actor.clone(), Style::new().fg(TEXT)),
            ]))
        })
        .collect::<Vec<_>>();
    let title = if state.timeline.is_empty() {
        "FLOW LEDGER / 暂无事件".to_string()
    } else {
        format!("FLOW LEDGER / {} EVENTS", state.timeline.len())
    };
    let list = List::new(items)
        .block(panel_block(&title, true))
        .highlight_style(Style::new().bg(PANEL_ALT).fg(TEXT).bold())
        .highlight_symbol("▸ ")
        .repeat_highlight_symbol(true);
    let mut list_state = ListState::default();
    list_state.select((!state.timeline.is_empty()).then_some(state.timeline_index));
    register_list_rows(
        ledger_area,
        state.timeline_index,
        state.timeline.len(),
        HitTarget::Timeline,
        state,
    );
    frame.render_stateful_widget(list, ledger_area, &mut list_state);
    render_flights(frame, app, state, flights_area);
}

fn render_flights(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    let selected_flow = state.selected_flow(app).map(|flow| flow.id.as_str());
    let mut items = Vec::new();
    if let Some(planning) = &state.active_planning {
        items.push(ListItem::new(Text::from(vec![
            Line::from(vec![
                Span::styled("◈ PLANNING ", Style::new().fg(ORANGE).bold()),
                Span::styled(
                    planner_label(planning.planner),
                    Style::new().fg(planner_color(planning.planner)),
                ),
            ]),
            Line::styled(
                format!("  {}", compact_summary(&planning.summary, 30)),
                Style::new().fg(TEXT),
            ),
            Line::styled(
                format!(
                    "  {} · {}",
                    planning.actor,
                    planning.started_at.format("%H:%M:%S")
                ),
                Style::new().fg(MUTED),
            ),
        ])));
    }
    items.extend(
        state
            .active_flights
            .values()
            .filter(|flight| selected_flow.is_none_or(|flow_id| flight.flow_id == flow_id))
            .map(|flight| {
                ListItem::new(Text::from(vec![
                    Line::from(vec![
                        Span::styled("● AIRBORNE ", Style::new().fg(ORANGE).bold()),
                        Span::styled(executor_mode_label(&flight.mode), Style::new().fg(CYAN)),
                    ]),
                    Line::from(vec![
                        Span::styled(format!("  {}  ", flight.task_id), Style::new().fg(TEXT)),
                        Span::styled(flight.actor.clone(), Style::new().fg(MUTED)),
                    ]),
                    Line::styled(
                        format!("  takeoff {}", flight.started_at.format("%H:%M:%S")),
                        Style::new().fg(MUTED),
                    ),
                ]))
            }),
    );

    let remote_item_offset = items.len();
    let leases = state
        .remote_flights(app)
        .into_iter()
        .take(5)
        .collect::<Vec<_>>();
    let remote_count = leases.len();
    items.extend(leases.into_iter().map(|lease| {
        let (marker, label, color) = match lease.status {
            FlightLeaseStatus::Authorized => ("○", "CLEARED", GOLD),
            FlightLeaseStatus::Active => ("●", "AIRBORNE", ORANGE),
            FlightLeaseStatus::Landed => ("✓", "LANDED", GREEN),
            FlightLeaseStatus::Crashed => ("✕", "CRASHED", RED),
            FlightLeaseStatus::Revoked => ("−", "REVOKED", MUTED),
            FlightLeaseStatus::Expired => ("−", "EXPIRED", MUTED),
        };
        let resources = lease
            .manifest
            .as_ref()
            .map_or(0, |manifest| manifest.resources.len());
        let active_resources = app
            .state()
            .resource_leases
            .values()
            .filter(|resource| {
                resource.flight_lease_id == lease.id
                    && resource.status == crate::domain::ResourceLeaseStatus::Active
            })
            .count();
        let fuel = lease.manifest.as_ref().map_or_else(
            || "FUEL legacy manifest".to_string(),
            |manifest| {
                let usage = lease.report.as_ref().map(|report| &report.fuel);
                format!(
                    "FUEL {}s/{}s · CTX {}/{}",
                    usage.map_or(0, |fuel| fuel.duration_seconds),
                    manifest.fuel.max_duration_seconds,
                    format_bytes(usage.map_or(0, |fuel| fuel.context_bytes)),
                    format_bytes(manifest.fuel.max_context_bytes),
                )
            },
        );
        let failure = lease.report.as_ref().and_then(|report| {
            report
                .budget_exhaustions
                .first()
                .cloned()
                .or_else(|| report.contract_violations.first().cloned())
                .or_else(|| report.failure_class.map(|class| format!("{class:?}")))
        });
        ListItem::new(Text::from(vec![
            Line::from(vec![
                Span::styled(format!("{marker} {label} "), Style::new().fg(color).bold()),
                Span::styled(lease.executor.to_string(), Style::new().fg(CYAN)),
                Span::styled(
                    format!(
                        "  {:?} · A{}",
                        lease
                            .manifest
                            .as_ref()
                            .map_or(crate::domain::CapabilityPack::General, |manifest| {
                                manifest.capability_pack
                            }),
                        lease.attempt
                    ),
                    Style::new().fg(GOLD),
                ),
            ]),
            Line::from(vec![
                Span::styled(format!("  {}  ", lease.task_id), Style::new().fg(TEXT)),
                Span::styled(lease.principal_name.clone(), Style::new().fg(MUTED)),
            ]),
            Line::styled(format!("  {fuel}"), Style::new().fg(MUTED)),
            Line::from(vec![
                Span::styled(
                    format!("  LEASE {active_resources}/{resources} · {}", lease.id),
                    Style::new().fg(MUTED),
                ),
                Span::styled(
                    failure
                        .map(|failure| format!(" · {}", compact_summary(&failure, 22)))
                        .unwrap_or_default(),
                    Style::new().fg(RED),
                ),
            ]),
        ]))
    }));
    register_flight_rows(area, remote_item_offset, remote_count, state);

    let mut records = app
        .state()
        .executions
        .values()
        .filter(|record| selected_flow.is_none_or(|flow_id| record.flow_id == flow_id))
        .collect::<Vec<_>>();
    records.sort_by_key(|record| Reverse(record.finished_at));
    items.extend(records.into_iter().take(5).map(|record| {
        let cost = record
            .cost_usd
            .map(|value| format!(" · ${value:.3}"))
            .unwrap_or_default();
        ListItem::new(Text::from(vec![
            Line::from(vec![
                Span::styled("✓ LANDED ", Style::new().fg(GREEN).bold()),
                Span::styled(record.executor.to_string(), Style::new().fg(CYAN)),
                Span::styled(cost, Style::new().fg(MUTED)),
            ]),
            Line::from(vec![
                Span::styled(format!("  {}  ", record.task_id), Style::new().fg(TEXT)),
                Span::styled(executor_mode_label(&record.mode), Style::new().fg(PURPLE)),
            ]),
            Line::styled(
                format!("  {}", record.log_path.display()),
                Style::new().fg(MUTED),
            ),
        ]))
    }));

    items.extend(
        state
            .timeline
            .iter()
            .rev()
            .filter_map(|event| match &event.event {
                DomainEvent::ExecutorFailed {
                    task_id, reason, ..
                } => Some(ListItem::new(Text::from(vec![
                    Line::styled("✕ CRASHED", Style::new().fg(RED).bold()),
                    Line::styled(format!("  {task_id}"), Style::new().fg(TEXT)),
                    Line::styled(
                        format!("  {}", compact_summary(reason, 34)),
                        Style::new().fg(RED),
                    ),
                ]))),
                _ => None,
            })
            .take(3),
    );

    if items.is_empty() {
        items.push(ListItem::new(Text::from(vec![
            Line::styled("机队待命", Style::new().fg(MUTED)),
            Line::styled("选中任务后点击底部的规划", Style::new().fg(TEXT)),
            Line::styled("需要写入时点击执行", Style::new().fg(TEXT)),
        ])));
    }
    let mut list_state = ListState::default();
    if remote_count > 0 {
        list_state.select(Some(
            remote_item_offset + state.flight_index.min(remote_count - 1),
        ));
    }
    frame.render_stateful_widget(
        List::new(items)
            .block(panel_block(
                "FLIGHT DECK / MANIFEST · FUEL",
                state.focus_flights,
            ))
            .highlight_style(Style::new().bg(PANEL_ALT).fg(TEXT).bold())
            .highlight_symbol("▸ ")
            .highlight_spacing(HighlightSpacing::Always),
        area,
        &mut list_state,
    );
}

fn render_status(frame: &mut Frame, state: &UiState, area: Rect) {
    let (label, color) = if state.message_is_error {
        ("CRASH", RED)
    } else {
        ("TOWER", GREEN)
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(format!(" {label} "), Style::new().fg(BG).bg(color).bold()),
            Span::raw("  "),
            Span::styled(state.message.clone(), Style::new().fg(TEXT)),
        ]))
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(dim_border()),
        )
        .alignment(Alignment::Left),
        area,
    );
}

fn render_actions(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    let mut actions = match state.view {
        View::Overview => vec![
            ("新需求", MouseAction::NewDemand),
            ("变更", MouseAction::RequestChange),
            ("批准", MouseAction::ApproveOrAccept),
            ("巡航", MouseAction::ScanTracker),
            ("投递通知", MouseAction::DispatchNotifications),
        ],
        View::Flows => vec![
            ("批准/接单", MouseAction::ApproveOrAccept),
            ("变更", MouseAction::RequestChange),
            ("推进", MouseAction::Advance),
            ("传球", MouseAction::SendMessage),
            ("调时", MouseAction::NegotiateEstimate),
            ("换防", MouseAction::Reassign),
            ("规划", MouseAction::Plan),
            ("执行", MouseAction::Execute),
        ],
        View::Inbox => vec![
            ("接单", MouseAction::ApproveOrAccept),
            ("收到", MouseAction::AcknowledgeEscalation),
            ("传球", MouseAction::SendMessage),
            ("调时", MouseAction::NegotiateEstimate),
            ("换防", MouseAction::Reassign),
            ("推进", MouseAction::Advance),
            ("规划", MouseAction::Plan),
            ("执行", MouseAction::Execute),
            ("证据", MouseAction::Evidence),
            ("阻塞", MouseAction::Block),
            ("验收", MouseAction::Complete),
        ],
        View::Roster => vec![("切换球权", MouseAction::CycleActor)],
        View::Timeline => state
            .selected_flight(app)
            .filter(|flight| flight.status == FlightLeaseStatus::Crashed)
            .zip(state.actor_name(app))
            .and_then(|(flight, actor)| app.recovery_options(&flight.id, actor).ok())
            .into_iter()
            .flatten()
            .filter_map(recovery_button)
            .collect(),
    };
    if state.view == View::Overview && app.state().organization.is_none() {
        actions.insert(0, ("SHOWCASE", MouseAction::LoadShowcase));
    }
    if state
        .selected_flow(app)
        .is_some_and(|flow| app.pending_flow_change(&flow.id).is_some())
    {
        let index = match state.view {
            View::Overview => 3.min(actions.len()),
            View::Flows => 2.min(actions.len()),
            _ => actions.len(),
        };
        actions.insert(index, ("驳回变更", MouseAction::RejectChange));
    }
    if state.actor_name(app).is_some_and(|actor| {
        app.state().principal(actor).is_ok_and(|principal| {
            app.state().office_releases.values().any(|release| {
                release.status == OfficeReleaseStatus::Requested
                    && app.state().flows.get(&release.flow_id).is_some_and(|flow| {
                        flow.demand.requester == principal.id
                            || flow.demand.requester == principal.name
                    })
            })
        })
    }) {
        actions.insert(0, ("放行发布", MouseAction::ApproveOfficeRelease));
    }
    frame.render_widget(Block::default().style(Style::new().bg(PANEL)), area);
    let mut x = area.x;
    let mut y = area.y;
    for (label, action) in actions.into_iter().chain([("退出", MouseAction::Quit)]) {
        let text = format!(" {label} ");
        let width = Line::from(text.as_str()).width() as u16;
        if x > area.x && area.right().saturating_sub(x) < width {
            x = area.x;
            y = y.saturating_add(1);
        }
        if y >= area.bottom() || width > area.width {
            break;
        }
        let button = Rect::new(x, y, width, 1);
        state.register_hit(button, HitTarget::Action(action));
        frame.render_widget(
            Paragraph::new(text).style(Style::new().fg(action_color(action)).bg(PANEL_ALT).bold()),
            button,
        );
        x = x.saturating_add(width).saturating_add(1);
    }
}

fn render_input_modal(frame: &mut Frame, modal: &InputModal, state: &mut UiState) {
    let is_demand = matches!(&modal.purpose, InputPurpose::Demand);
    let is_reassign = matches!(&modal.purpose, InputPurpose::Reassign { .. });
    let has_selector = is_demand || is_reassign;
    let area = centered(frame.area(), 72, if has_selector { 11 } else { 9 });
    frame.render_widget(Clear, area);
    let (title, prompt, color): (&str, String, Color) = match &modal.purpose {
        InputPurpose::Demand => (
            "NEW DEMAND / 管理需求",
            "描述目标；点击选择 PRD 规划器".into(),
            GOLD,
        ),
        InputPurpose::Evidence { .. } => ("EVIDENCE / 交付证据", "输入证据摘要".into(), GREEN),
        InputPurpose::Block { .. } => ("BLOCK / 请求协防", "输入阻塞原因".into(), RED),
        InputPurpose::Message {
            recipients,
            requires_ack,
            ..
        } => (
            "PASS / FLOW 指令",
            format!(
                "传给 {}{}",
                recipients.join(", "),
                if *requires_ack {
                    "；等待明确回执"
                } else {
                    ""
                }
            ),
            GOLD,
        ),
        InputPurpose::Negotiate { current_hours, .. } => (
            "FLIGHT PLAN / 动态调时",
            format!(
                "当前基础工时 {:.1}h；输入新工时后同步重算下游窗口与关键路径",
                current_hours
            ),
            CYAN,
        ),
        InputPurpose::Reassign {
            candidates,
            selected,
            ..
        } => (
            "LINEUP / 任务换防",
            format!(
                "换防给 {}；输入原因，点击候选条切换人选",
                candidates
                    .get(*selected)
                    .map(|target| target.name.as_str())
                    .unwrap_or("无候选人")
            ),
            ORANGE,
        ),
        InputPurpose::FlowChange { .. } => (
            "CHANGE REQUEST / 航线变更",
            "描述新增范围；先生成影响预览，不会立即修改正式 Flow".into(),
            PURPLE,
        ),
        InputPurpose::RejectChange { .. } => (
            "REJECT CHANGE / 驳回变更",
            "输入驳回原因；申请会进入黑匣子，正式 Flow 保持不变".into(),
            RED,
        ),
        InputPurpose::Run { mode, .. } => (
            "FLIGHT CLEARANCE / 航班放行",
            match mode {
                ExecutorMode::Plan => {
                    "只读规划会调用已分配终端并产生模型费用；输入 PASS 放行".into()
                }
                ExecutorMode::Execute => {
                    "执行模式允许终端修改注册工作区；确认仓库状态后输入 MAMBA 放行".into()
                }
            },
            match mode {
                ExecutorMode::Plan => CYAN,
                ExecutorMode::Execute => ORANGE,
            },
        ),
        InputPurpose::RecoverFlight { action, .. } => (
            "BLACK BOX / 坠机处置",
            format!(
                "{}；输入本次监督决定的理由，确认后写入因果链",
                recovery_action_label(*action)
            ),
            match action {
                RecoveryAction::Retry | RecoveryAction::ReduceScope => ORANGE,
                RecoveryAction::HumanHandoff => GOLD,
                RecoveryAction::Ground => RED,
                RecoveryAction::SwitchExecutor | RecoveryAction::Fork => PURPLE,
            },
        ),
    };
    let (hint, selector_area, input, footer) = if has_selector {
        let [hint, selector, input, footer] = Layout::vertical([
            Constraint::Length(2),
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Length(2),
        ])
        .margin(1)
        .areas(area);
        (hint, Some(selector), input, footer)
    } else {
        let [hint, input, footer] = Layout::vertical([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Length(2),
        ])
        .margin(1)
        .areas(area);
        (hint, None, input, footer)
    };
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::new().fg(color))
            .style(Style::new().bg(PANEL))
            .title(Span::styled(
                format!(" {title} "),
                Style::new().fg(color).bold(),
            )),
        area,
    );
    frame.render_widget(Paragraph::new(prompt).style(Style::new().fg(MUTED)), hint);
    if let Some(selector_area) = selector_area {
        if is_demand {
            render_planner_selector(frame, selector_area, state);
        } else {
            render_assignee_selector(frame, selector_area, modal, state);
        }
    }
    let visible = format!("{}█", modal.value);
    frame.render_widget(
        Paragraph::new(visible)
            .style(Style::new().fg(TEXT).bg(PANEL_ALT))
            .block(
                Block::default()
                    .borders(Borders::BOTTOM)
                    .border_style(Style::new().fg(color)),
            ),
        input,
    );
    let [confirm, cancel] = Layout::horizontal([Constraint::Ratio(1, 2); 2])
        .spacing(1)
        .areas(footer);
    state.register_hit(confirm, HitTarget::Action(MouseAction::ConfirmModal));
    state.register_hit(cancel, HitTarget::Action(MouseAction::CancelModal));
    frame.render_widget(
        Paragraph::new("[ 确认 ]")
            .style(Style::new().fg(BG).bg(color).bold())
            .alignment(Alignment::Center),
        confirm,
    );
    frame.render_widget(
        Paragraph::new("[ 取消 ]")
            .style(Style::new().fg(TEXT).bg(PANEL_ALT))
            .alignment(Alignment::Center),
        cancel,
    );
}

fn render_assignee_selector(
    frame: &mut Frame,
    area: Rect,
    modal: &InputModal,
    state: &mut UiState,
) {
    let InputPurpose::Reassign {
        candidates,
        selected,
        ..
    } = &modal.purpose
    else {
        return;
    };
    let Some(target) = candidates.get(*selected) else {
        return;
    };
    let next = (*selected + 1) % candidates.len();
    state.register_hit(area, HitTarget::Action(MouseAction::SelectAssignee(next)));
    frame.render_widget(
        Paragraph::new(format!(
            "[ {} · {} · {}/{} ]",
            target.name,
            target_kind_label(&target.kind),
            selected + 1,
            candidates.len()
        ))
        .alignment(Alignment::Center)
        .style(Style::new().fg(BG).bg(ORANGE).bold()),
        area,
    );
}

fn render_planner_selector(frame: &mut Frame, area: Rect, state: &mut UiState) {
    let planners = [
        (PlannerKind::Local, "LOCAL"),
        (PlannerKind::ClaudeCode, "CLAUDE CODE"),
        (PlannerKind::Codex, "CODEX"),
    ];
    let areas = Layout::horizontal([Constraint::Ratio(1, 3); 3])
        .spacing(1)
        .split(area);
    for (index, (planner, label)) in planners.into_iter().enumerate() {
        let selected = state.demand_planner == planner;
        state.register_hit(
            areas[index],
            HitTarget::Action(MouseAction::SelectPlanner(planner)),
        );
        frame.render_widget(
            Paragraph::new(format!("[ {label} ]"))
                .alignment(Alignment::Center)
                .style(if selected {
                    Style::new().fg(BG).bg(planner_color(planner)).bold()
                } else {
                    Style::new().fg(MUTED).bg(PANEL_ALT)
                }),
            areas[index],
        );
    }
}

fn centered(area: Rect, width_percent: u16, height: u16) -> Rect {
    let [vertical] = Layout::vertical([Constraint::Length(height.min(area.height))])
        .flex(Flex::Center)
        .areas(area);
    let [horizontal] = Layout::horizontal([Constraint::Percentage(width_percent)])
        .flex(Flex::Center)
        .areas(vertical);
    horizontal
}

fn panel_block<'a>(title: &'a str, focused: bool) -> Block<'a> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::new().fg(if focused {
            GOLD
        } else {
            Color::Rgb(55, 59, 66)
        }))
        .style(Style::new().bg(PANEL).fg(TEXT))
        .title(Span::styled(
            format!(" {title} "),
            Style::new()
                .fg(if focused { GOLD } else { MUTED })
                .add_modifier(Modifier::BOLD),
        ))
}

fn dim_border() -> Style {
    Style::new().fg(Color::Rgb(48, 51, 57))
}

fn register_table_rows(
    area: Rect,
    selected: usize,
    count: usize,
    target: fn(usize) -> HitTarget,
    state: &mut UiState,
) {
    let visible = area.height.saturating_sub(4) as usize;
    if visible == 0 || count == 0 {
        return;
    }
    let offset = selected
        .min(count - 1)
        .saturating_add(1)
        .saturating_sub(visible);
    for (visual_row, index) in (offset..count).take(visible).enumerate() {
        state.register_hit(
            Rect::new(
                area.x.saturating_add(1),
                area.y.saturating_add(3 + visual_row as u16),
                area.width.saturating_sub(2),
                1,
            ),
            target(index),
        );
    }
}

fn register_list_rows(
    area: Rect,
    selected: usize,
    count: usize,
    target: fn(usize) -> HitTarget,
    state: &mut UiState,
) {
    let visible = area.height.saturating_sub(2) as usize;
    if visible == 0 || count == 0 {
        return;
    }
    let offset = selected
        .min(count - 1)
        .saturating_add(1)
        .saturating_sub(visible);
    for (visual_row, index) in (offset..count).take(visible).enumerate() {
        state.register_hit(
            Rect::new(
                area.x.saturating_add(1),
                area.y.saturating_add(1 + visual_row as u16),
                area.width.saturating_sub(2),
                1,
            ),
            target(index),
        );
    }
}

fn register_flight_rows(area: Rect, preceding_items: usize, count: usize, state: &mut UiState) {
    let first_row = area
        .y
        .saturating_add(1)
        .saturating_add((preceding_items as u16).saturating_mul(3));
    for index in 0..count {
        let y = first_row.saturating_add((index as u16).saturating_mul(4));
        if y >= area.bottom().saturating_sub(1) {
            break;
        }
        state.register_hit(
            Rect::new(
                area.x.saturating_add(1),
                y,
                area.width.saturating_sub(2),
                4.min(area.bottom().saturating_sub(1).saturating_sub(y)),
            ),
            HitTarget::Flight(index),
        );
    }
}

fn rect_contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x && column < area.right() && row >= area.y && row < area.bottom()
}

fn action_color(action: MouseAction) -> Color {
    match action {
        MouseAction::LoadShowcase | MouseAction::NewDemand | MouseAction::ApproveOrAccept => GOLD,
        MouseAction::Advance | MouseAction::Plan => CYAN,
        MouseAction::Execute => ORANGE,
        MouseAction::Evidence | MouseAction::Complete => GREEN,
        MouseAction::SendMessage => GOLD,
        MouseAction::NegotiateEstimate => CYAN,
        MouseAction::Reassign => ORANGE,
        MouseAction::RequestChange => PURPLE,
        MouseAction::RejectChange => RED,
        MouseAction::Block | MouseAction::Quit => RED,
        MouseAction::ScanTracker => ORANGE,
        MouseAction::DispatchNotifications => CYAN,
        MouseAction::ApproveOfficeRelease => GOLD,
        MouseAction::AcknowledgeEscalation => GOLD,
        MouseAction::RecoverFlight(RecoveryAction::Retry | RecoveryAction::ReduceScope) => ORANGE,
        MouseAction::RecoverFlight(RecoveryAction::HumanHandoff) => GOLD,
        MouseAction::RecoverFlight(RecoveryAction::Ground) => RED,
        MouseAction::RecoverFlight(RecoveryAction::SwitchExecutor | RecoveryAction::Fork) => PURPLE,
        MouseAction::CycleActor => PURPLE,
        MouseAction::ConfirmModal => GREEN,
        MouseAction::CancelModal => MUTED,
        MouseAction::SelectPlanner(planner) => planner_color(planner),
        MouseAction::SelectAssignee(_) => ORANGE,
    }
}

fn flow_ids(app: &MambaApp) -> Vec<String> {
    let mut flows = app.state().flows.values().collect::<Vec<_>>();
    flows.sort_by_key(|flow| Reverse(flow.created_at));
    flows.into_iter().map(|flow| flow.id.clone()).collect()
}

fn initial_flow_index(app: &MambaApp) -> usize {
    let ids = flow_ids(app);
    let dashboard = build_dashboard(app.state());
    ids.iter()
        .enumerate()
        .filter_map(|(index, id)| {
            dashboard
                .flows
                .iter()
                .find(|flow| flow.id == *id)
                .map(|flow| (index, flow_health_priority(flow.health)))
        })
        .min_by_key(|(index, priority)| (*priority, *index))
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn flow_health_priority(health: FlowHealth) -> u8 {
    match health {
        FlowHealth::Blocked => 0,
        FlowHealth::AtRisk => 1,
        FlowHealth::WaitingHuman => 2,
        FlowHealth::OnTrack => 3,
        FlowHealth::Draft => 4,
        FlowHealth::Completed => 5,
        FlowHealth::Cancelled => 6,
    }
}

fn human_ids(app: &MambaApp) -> Vec<String> {
    let mut humans = app
        .state()
        .principals
        .values()
        .filter(|principal| principal.active && principal.kind == PrincipalKind::Human)
        .collect::<Vec<_>>();
    humans.sort_by(|left, right| left.name.cmp(&right.name));
    humans.into_iter().map(|human| human.id.clone()).collect()
}

fn task_has_executor(app: &MambaApp, task: &Task) -> bool {
    let Some(assignment) = &task.assignment else {
        return false;
    };
    app.state().principals.values().any(|principal| {
        principal.executor.is_some()
            && (principal.id == assignment.owner.id
                || assignment
                    .copilots
                    .iter()
                    .any(|copilot| copilot.id == principal.id)
                || principal.owner_id.as_deref() == Some(assignment.owner.id.as_str()))
    })
}

fn planner_label(planner: PlannerKind) -> &'static str {
    match planner {
        PlannerKind::Local => "LOCAL",
        PlannerKind::ClaudeCode => "CLAUDE CODE",
        PlannerKind::Codex => "CODEX",
    }
}

fn planner_color(planner: PlannerKind) -> Color {
    match planner {
        PlannerKind::Local => GOLD,
        PlannerKind::ClaudeCode => PURPLE,
        PlannerKind::Codex => CYAN,
    }
}

fn confirmation_token(mode: &ExecutorMode) -> &'static str {
    match mode {
        ExecutorMode::Plan => "PASS",
        ExecutorMode::Execute => "MAMBA",
    }
}

fn executor_mode_label(mode: &ExecutorMode) -> &'static str {
    match mode {
        ExecutorMode::Plan => "PLAN",
        ExecutorMode::Execute => "EXECUTE",
    }
}

fn compact_summary(value: &str, max_chars: usize) -> String {
    let mut summary = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(max_chars)
        .collect::<String>();
    if value.chars().count() > max_chars {
        summary.push('…');
    }
    summary
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_048_576 {
        format!("{:.1}M", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1_024 {
        format!("{:.1}K", bytes as f64 / 1_024.0)
    } else {
        format!("{bytes}B")
    }
}

fn flight_selection_summary(app: &MambaApp, flight: &FlightLease) -> String {
    let objective = flight
        .manifest
        .as_ref()
        .map(|manifest| compact_summary(&manifest.objective, 42))
        .unwrap_or_else(|| "legacy flight".into());
    let active_resources = app
        .state()
        .resource_leases
        .values()
        .filter(|resource| {
            resource.flight_lease_id == flight.id
                && resource.status == crate::domain::ResourceLeaseStatus::Active
        })
        .count();
    format!(
        "{} · A{} · {} · {} active resource lease(s)",
        flight.id, flight.attempt, objective, active_resources
    )
}

fn recovery_action_label(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::Retry => "沿原航线复飞",
        RecoveryAction::SwitchExecutor => "更换执行终端复飞",
        RecoveryAction::ReduceScope => "缩小目标与上下文后复飞",
        RecoveryAction::HumanHandoff => "把球权交还 Human",
        RecoveryAction::Ground => "永久停飞",
        RecoveryAction::Fork => "从黑匣子分叉新航班",
    }
}

fn recovery_button(action: RecoveryAction) -> Option<(&'static str, MouseAction)> {
    let label = match action {
        RecoveryAction::Retry => "原地复飞",
        RecoveryAction::ReduceScope => "缩小航线",
        RecoveryAction::HumanHandoff => "转人工",
        RecoveryAction::Ground => "永久停飞",
        RecoveryAction::Fork => "分叉复飞",
        RecoveryAction::SwitchExecutor => return None,
    };
    Some((label, MouseAction::RecoverFlight(action)))
}

fn shifted(current: usize, len: usize, delta: isize) -> usize {
    if len == 0 {
        return 0;
    }
    let current = current.min(len - 1);
    if delta < 0 {
        current.saturating_sub(delta.unsigned_abs())
    } else {
        (current + delta as usize).min(len - 1)
    }
}

fn flow_status_label(status: &FlowStatus) -> &'static str {
    match status {
        FlowStatus::Draft => "DRAFT",
        FlowStatus::Approved => "READY",
        FlowStatus::Active => "ACTIVE",
        FlowStatus::Completed => "LANDED",
        FlowStatus::Cancelled => "ABORTED",
    }
}

fn flow_status_style(status: &FlowStatus) -> Style {
    Style::new().fg(match status {
        FlowStatus::Draft => MUTED,
        FlowStatus::Approved => GOLD,
        FlowStatus::Active => CYAN,
        FlowStatus::Completed => GREEN,
        FlowStatus::Cancelled => RED,
    })
}

fn flow_health_label(health: FlowHealth) -> &'static str {
    match health {
        FlowHealth::Blocked => "BLOCKED",
        FlowHealth::AtRisk => "AT RISK",
        FlowHealth::WaitingHuman => "REVIEW",
        FlowHealth::OnTrack => "ON TRACK",
        FlowHealth::Completed => "LANDED",
        FlowHealth::Draft => "DRAFT",
        FlowHealth::Cancelled => "ABORTED",
    }
}

fn flow_health_style(health: FlowHealth) -> Style {
    Style::new().fg(match health {
        FlowHealth::Blocked | FlowHealth::AtRisk | FlowHealth::Cancelled => RED,
        FlowHealth::WaitingHuman => ORANGE,
        FlowHealth::OnTrack => CYAN,
        FlowHealth::Completed => GREEN,
        FlowHealth::Draft => MUTED,
    })
}

fn task_status_label(status: &TaskStatus) -> &'static str {
    match status {
        TaskStatus::Proposed => "PROPOSED",
        TaskStatus::Assigned => "ASSIGNED",
        TaskStatus::Accepted => "ACCEPTED",
        TaskStatus::InProgress => "AIRBORNE",
        TaskStatus::Blocked => "BLOCKED",
        TaskStatus::Submitted => "REVIEW",
        TaskStatus::Completed => "LANDED",
        TaskStatus::Rejected => "REJECTED",
        TaskStatus::Cancelled => "ABORTED",
    }
}

fn target_kind_label(kind: &crate::domain::TargetKind) -> &'static str {
    match kind {
        crate::domain::TargetKind::Human => "HUMAN",
        crate::domain::TargetKind::Agent => "AGENT",
        crate::domain::TargetKind::Team => "TEAM",
    }
}

fn task_status_style(status: &TaskStatus) -> Style {
    Style::new().fg(match status {
        TaskStatus::Proposed => MUTED,
        TaskStatus::Assigned => GOLD,
        TaskStatus::Accepted => PURPLE,
        TaskStatus::InProgress => CYAN,
        TaskStatus::Blocked => RED,
        TaskStatus::Submitted => ORANGE,
        TaskStatus::Completed => GREEN,
        TaskStatus::Rejected | TaskStatus::Cancelled => RED,
    })
}

fn event_style(kind: &str) -> Style {
    let color = if kind.contains("failed")
        || kind.contains("blocked")
        || kind.contains("crashed")
        || kind.contains("rejected")
        || kind.contains("attention_raised")
    {
        RED
    } else if kind.contains("completed")
        || kind.contains("finished")
        || kind.contains("landed")
        || kind.contains("delivered")
        || kind.contains("external_interaction.processed")
        || kind.contains("attention_resolved")
    {
        GREEN
    } else if kind.contains("approved") || kind.contains("accepted") || kind.contains("queued") {
        GOLD
    } else if kind.contains("executor") || kind.contains("started") {
        CYAN
    } else {
        PURPLE
    };
    Style::new().fg(color)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use tempfile::tempdir;

    use super::*;
    use crate::domain::{ExecutorConfig, ExecutorKind, PrincipalKind};
    use crate::showcase::seed_showcase;

    #[tokio::test]
    async fn overview_renders_organization_and_flow() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Mamba Labs", "admin").unwrap();
        let team = app
            .create_team("Platform", "product,delivery", "admin")
            .unwrap();
        app.register_principal(
            "牢大",
            PrincipalKind::Human,
            Some(&team.id),
            None,
            "product,delivery",
            100,
            None,
            "admin",
        )
        .unwrap();
        app.create_demand(
            "Prepare launch brief",
            "牢大",
            PlannerKind::Local,
            directory.path(),
            10,
        )
        .await
        .unwrap();

        let mut state = UiState::new(
            &app,
            TuiOptions {
                workspace: directory.path().to_path_buf(),
                actor: Some("牢大".to_string()),
            },
        );
        let backend = TestBackend::new(120, 36);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(content.contains("MAMBAFLOW"));
        assert!(content.contains("Mamba Labs"));
        assert!(content.contains("Prepare launch brief"));
        assert!(content.contains("TOWER BRIEF"));
        assert!(content.contains("AT RISK"));

        let scan_tracker = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::ScanTracker))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(scan_tracker))
            .await
            .unwrap();
        assert!(state.last_tracking_scan.is_some());
        assert!(state.message.contains("Tower Tracker"));

        state.view = View::Timeline;
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("FLOW LEDGER"));
        assert!(content.contains("FLIGHT DECK"));

        let flows_tab = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Tab(View::Flows))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(flows_tab))
            .await
            .unwrap();
        assert_eq!(state.view, View::Flows);

        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let first_task = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Task(0))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(first_task))
            .await
            .unwrap();
        assert!(state.focus_tasks);
        state
            .handle_mouse(&mut app, mouse_scroll(first_task, false))
            .await
            .unwrap();
        assert_eq!(state.task_index, 1);

        state.view = View::Overview;
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let new_demand = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::NewDemand))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(new_demand))
            .await
            .unwrap();
        assert!(matches!(
            state.modal.as_ref().map(|modal| &modal.purpose),
            Some(InputPurpose::Demand)
        ));
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let codex_planner = state
            .hit_regions
            .iter()
            .find(|region| {
                region.target == HitTarget::Action(MouseAction::SelectPlanner(PlannerKind::Codex))
            })
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(codex_planner))
            .await
            .unwrap();
        assert_eq!(state.demand_planner, PlannerKind::Codex);
        let cancel = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::CancelModal))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(cancel))
            .await
            .unwrap();
        assert!(state.modal.is_none());
    }

    #[tokio::test]
    async fn showcase_renders_admin_metrics_actions_and_remote_flight() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Mamba Labs", "admin").unwrap();
        let team = app
            .create_team(
                "洛杉矶研发队",
                "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
                "admin",
            )
            .unwrap();
        let human = app
            .register_principal(
                "牢大",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        app.register_principal(
            "Codex 副驾",
            PrincipalKind::Agent,
            Some(&team.id),
            Some(&human.id),
            "product,delivery,backend,rust,llm-platform,security,quality,observability,operations",
            100,
            Some(ExecutorConfig {
                kind: ExecutorKind::Codex,
                workspace: directory.path().to_path_buf(),
                model: None,
                command: None,
            }),
            "admin",
        )
        .unwrap();
        let showcase = seed_showcase(&mut app, directory.path(), &human.name)
            .await
            .unwrap();
        app.configure_work_calendar(
            &human.id,
            8 * 60,
            crate::calendar::parse_workdays("mon,tue,wed,thu,fri").unwrap(),
            9 * 60,
            18 * 60,
            "admin",
        )
        .unwrap();

        let mut state = UiState::new(
            &app,
            TuiOptions {
                workspace: directory.path().to_path_buf(),
                actor: Some(human.name),
            },
        );
        let backend = TestBackend::new(120, 36);
        let mut terminal = Terminal::new(backend).unwrap();
        state.flow_index = flow_ids(&app)
            .iter()
            .position(|flow_id| flow_id == &showcase.highlighted_flow_id)
            .unwrap();
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("TASK PROGRESS"));
        assert!(content.contains("WAITING HUMAN"));
        assert!(content.contains("ACTION QUEUE"));
        assert!(content.contains("OUTBOX"));
        assert!(content.contains("LLM Gateway v0"));
        assert!(content.contains("BLOCKED"));
        let release_button = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::ApproveOfficeRelease))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(release_button))
            .await
            .unwrap();
        assert_eq!(
            app.state().office_releases[&showcase.office_release_id].status,
            OfficeReleaseStatus::Approved
        );

        let compact_backend = TestBackend::new(80, 24);
        let mut compact_terminal = Terminal::new(compact_backend).unwrap();
        compact_terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let compact_content = compact_terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(compact_content.contains("FLOW BOARD"));
        assert!(compact_content.contains("ACTION QUEUE"));
        assert!(compact_content.contains("BLOCKED"), "{compact_content}");
        assert!(state.hit_regions.iter().any(|region| {
            region.target == HitTarget::Action(MouseAction::DispatchNotifications)
        }));

        state.view = View::Timeline;
        state.refresh_timeline(&app);
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("FLIGHT DECK"));
        assert!(content.contains("CLEARED"));
        assert!(content.contains("CRASHED"));
        assert!(content.contains("FUEL"));

        let crashed_flight = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Flight(0))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(crashed_flight))
            .await
            .unwrap();
        assert!(state.focus_flights);
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let handoff = state
            .hit_regions
            .iter()
            .find(|region| {
                region.target
                    == HitTarget::Action(MouseAction::RecoverFlight(RecoveryAction::HumanHandoff))
            })
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(handoff))
            .await
            .unwrap();
        assert!(matches!(
            state.modal.as_ref().map(|modal| &modal.purpose),
            Some(InputPurpose::RecoverFlight {
                action: RecoveryAction::HumanHandoff,
                ..
            })
        ));
        state.paste("交给安全负责人确认 Secret 轮换边界".into());
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let confirm = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::ConfirmModal))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(confirm))
            .await
            .unwrap();
        assert!(state.modal.is_none());
        assert_eq!(app.state().flight_recoveries.len(), 1);

        state.view = View::Roster;
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("UTC+08:00"));
    }

    #[tokio::test]
    async fn empty_tower_loads_showcase_from_mouse_and_focuses_risk() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        let mut state = UiState::new(
            &app,
            TuiOptions {
                workspace: directory.path().to_path_buf(),
                actor: None,
            },
        );
        let backend = TestBackend::new(120, 36);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();

        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("SHOWCASE"));
        let showcase_button = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::LoadShowcase))
            .unwrap()
            .area;

        state
            .handle_mouse(&mut app, mouse_down(showcase_button))
            .await
            .unwrap();

        assert_eq!(app.state().organization().unwrap().name, "Mamba Labs");
        assert_eq!(app.state().flows.len(), 3);
        assert_eq!(state.actor_name(&app), Some("牢大"));
        assert!(
            state
                .selected_flow(&app)
                .is_some_and(|flow| flow.prd.title.contains("LLM Gateway v0"))
        );
        assert!(!state.message_is_error);

        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("TASK PROGRESS"));
        assert!(content.contains("ACTION QUEUE"));
        assert!(content.contains("LLM Gateway v0"));
        assert!(
            !state
                .hit_regions
                .iter()
                .any(|region| { region.target == HitTarget::Action(MouseAction::LoadShowcase) })
        );

        state.cycle_actor(&app);
        assert_eq!(state.actor_name(&app), Some("佐巴扬"));
        state.switch_view(&app, View::Inbox);
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("TOWER COMMS"));
        assert!(content.contains("Provider Secret"));
        let compact_backend = TestBackend::new(80, 24);
        let mut compact_terminal = Terminal::new(compact_backend).unwrap();
        compact_terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let compact_content = compact_terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(compact_content.contains("TOWER COMMS"));
        assert!(compact_content.contains("Provider Secret"));
        for action in [
            MouseAction::ApproveOrAccept,
            MouseAction::AcknowledgeEscalation,
            MouseAction::SendMessage,
            MouseAction::NegotiateEstimate,
            MouseAction::Reassign,
            MouseAction::Advance,
            MouseAction::Plan,
            MouseAction::Execute,
            MouseAction::Evidence,
            MouseAction::Block,
            MouseAction::Complete,
            MouseAction::Quit,
        ] {
            assert!(
                state
                    .hit_regions
                    .iter()
                    .any(|region| region.target == HitTarget::Action(action)),
                "compact Inbox is missing mouse action {action:?}"
            );
        }
        state.acknowledge_next_inbox(&mut app);
        assert!(state.message.contains("已收到指令"));
        assert!(app.message_inbox("佐巴扬", false).unwrap().is_empty());
        state.open_message_input(&app);
        let Some(modal) = &mut state.modal else {
            panic!("message modal should open for an assigned task");
        };
        assert!(matches!(&modal.purpose, InputPurpose::Message { .. }));
        modal.value = "Secret 边界已确认，可以恢复执行".into();
        state.submit_modal(&mut app).await;
        assert!(
            app.message_inbox("牢大", false)
                .unwrap()
                .iter()
                .any(|item| item.message.body.contains("恢复执行"))
        );

        assert_eq!(app.state().flows.len(), 3);
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        assert!(
            !state
                .hit_regions
                .iter()
                .any(|region| { region.target == HitTarget::Action(MouseAction::LoadShowcase) })
        );
    }

    #[tokio::test]
    async fn inbox_renders_and_acknowledges_tower_calls() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Mamba Labs", "admin").unwrap();
        let team = app
            .create_team("Product", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "牢大",
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
                "Prepare launch brief",
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
        app.start_task(&task_id, &human.name).unwrap();
        app.block_task(&task_id, &human.name, "waiting for access")
            .unwrap();
        let scan = app.scan_tracking(24, "tower").unwrap();
        assert_eq!(scan.escalated.len(), 1);

        let mut state = UiState::new(
            &app,
            TuiOptions {
                workspace: directory.path().to_path_buf(),
                actor: Some(human.name.clone()),
            },
        );
        state.view = View::Inbox;
        let backend = TestBackend::new(120, 36);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("TOWER COMMS"));
        assert!(content.contains("CRITICAL"));
        assert!(content.contains("waiting for access"));

        state.acknowledge_next_inbox(&mut app);
        assert!(state.message.contains("已收到呼叫"));
        assert!(!state.actor_escalations(&app)[0].needs_acknowledgement());
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(content.contains("RECEIVED"));
    }

    #[tokio::test]
    async fn tui_reassigns_and_reschedules_a_task_from_mouse_controls() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Mamba Labs", "admin").unwrap();
        let team = app
            .create_team(
                "Platform",
                "product,backend,llm-platform,security,quality,observability,operations",
                "admin",
            )
            .unwrap();
        let manager = app
            .register_principal(
                "Manager",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product,llm-platform,operations",
                100,
                None,
                "admin",
            )
            .unwrap();
        app.register_principal(
            "Fast Engineer",
            PrincipalKind::Human,
            Some(&team.id),
            None,
            "backend,llm-platform,security,quality,observability",
            100,
            None,
            "admin",
        )
        .unwrap();
        let slow = app
            .register_principal(
                "Slow Engineer",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "backend,llm-platform",
                25,
                None,
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Build an LLM Gateway this week",
                &manager.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        app.approve_flow(&flow.id, &manager.name).unwrap();
        let gateway_index = flow
            .tasks
            .iter()
            .position(|task| task.key == "gateway-core")
            .unwrap();
        let gateway_id = flow.tasks[gateway_index].id.clone();
        let old_downstream_start = flow.task("observability").unwrap().estimate.earliest_start;
        let mut state = UiState::new(
            &app,
            TuiOptions {
                workspace: directory.path().to_path_buf(),
                actor: Some(manager.name.clone()),
            },
        );
        state.view = View::Flows;
        state.focus_tasks = true;
        state.flow_index = flow_ids(&app)
            .iter()
            .position(|flow_id| flow_id == &flow.id)
            .unwrap();
        state.task_index = gateway_index;
        let backend = TestBackend::new(120, 36);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let reassign = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::Reassign))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(reassign))
            .await
            .unwrap();
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let next_candidate = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::SelectAssignee(1)))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(next_candidate))
            .await
            .unwrap();
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let first_candidate = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::SelectAssignee(0)))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(first_candidate))
            .await
            .unwrap();
        let Some(modal) = &mut state.modal else {
            panic!("reassignment modal should open");
        };
        let InputPurpose::Reassign {
            candidates,
            selected,
            ..
        } = &modal.purpose
        else {
            panic!("expected reassignment modal");
        };
        assert_eq!(candidates[*selected].name, slow.name);
        modal.value = "Fast Engineer is handling an incident".into();
        state.submit_modal(&mut app).await;
        assert_eq!(
            app.state()
                .find_task(&gateway_id)
                .unwrap()
                .1
                .assignment
                .as_ref()
                .unwrap()
                .owner
                .id,
            slow.id
        );

        state.actor_id = Some(slow.id.clone());
        state.open_estimate_input(&app);
        state.modal.as_mut().unwrap().value = "40".into();
        state.submit_modal(&mut app).await;
        let updated = app.state().flow(&flow.id).unwrap();
        assert_eq!(
            updated.task(&gateway_id).unwrap().estimate.effort_hours,
            40.0
        );
        assert!(
            updated
                .task("observability")
                .unwrap()
                .estimate
                .earliest_start
                > old_downstream_start
        );
        assert!(state.message.contains("重新排期"));

        state.actor_id = Some(manager.id.clone());
        state.focus_tasks = false;
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let change_action = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::RequestChange))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(change_action))
            .await
            .unwrap();
        let Some(modal) = &mut state.modal else {
            panic!("flow change modal should open");
        };
        modal.value = "Add a customer migration checklist".into();
        let task_count = app.state().flow(&flow.id).unwrap().tasks.len();
        state.submit_modal(&mut app).await;
        assert_eq!(app.state().flow(&flow.id).unwrap().tasks.len(), task_count);
        assert!(app.pending_flow_change(&flow.id).is_some());

        let compact_backend = TestBackend::new(80, 24);
        let mut compact_terminal = Terminal::new(compact_backend).unwrap();
        compact_terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let compact_content = compact_terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(compact_content.contains("[CHG]"), "{compact_content}");

        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let approve = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::ApproveOrAccept))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(approve))
            .await
            .unwrap();
        assert_eq!(
            app.state().flow(&flow.id).unwrap().tasks.len(),
            task_count + 1
        );
        assert!(app.pending_flow_change(&flow.id).is_none());
        assert!(state.message.contains("新增任务完成传球"));

        state.open_flow_change_input(&app);
        state.modal.as_mut().unwrap().value = "Add an optional launch survey".into();
        let approved_task_count = app.state().flow(&flow.id).unwrap().tasks.len();
        state.submit_modal(&mut app).await;
        let rejected_change_id = app.pending_flow_change(&flow.id).unwrap().id.clone();
        terminal
            .draw(|frame| render(frame, &app, &mut state))
            .unwrap();
        let reject = state
            .hit_regions
            .iter()
            .find(|region| region.target == HitTarget::Action(MouseAction::RejectChange))
            .unwrap()
            .area;
        state
            .handle_mouse(&mut app, mouse_down(reject))
            .await
            .unwrap();
        state.modal.as_mut().unwrap().value = "Not required for this release".into();
        state.submit_modal(&mut app).await;
        assert_eq!(
            app.state().flow(&flow.id).unwrap().tasks.len(),
            approved_task_count
        );
        assert!(app.pending_flow_change(&flow.id).is_none());
        assert_eq!(
            app.state().flow_changes[&rejected_change_id].status,
            crate::domain::FlowChangeStatus::Rejected
        );
        assert!(state.message.contains("正式 Flow 保持不变"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_planner_runs_in_background_and_creates_structured_flow() {
        let directory = tempdir().unwrap();
        let executable = directory.path().join("fake-codex-planner");
        fs::write(
            &executable,
            r#"#!/bin/sh
result=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then
    shift
    result="$1"
  fi
  shift
done
printf '%s' '{"prd":{"title":"Model planned launch","summary":"Ship the launch","goals":["ship"],"non_goals":[],"acceptance_criteria":["approved"]},"tasks":[{"key":"approve-launch","title":"Approve launch","description":"Review and approve the launch plan.","required_capabilities":["product"],"depends_on":[],"effort_hours":2.0,"requires_human":true,"acceptance_criteria":["launch approved"]}]}' > "$result"
printf '%s\n' '{"thread_id":"fake-planner"}'
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&executable, permissions).unwrap();

        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Mamba Labs", "admin").unwrap();
        let team = app.create_team("Product", "product", "admin").unwrap();
        let human = app
            .register_principal(
                "牢大",
                PrincipalKind::Human,
                Some(&team.id),
                None,
                "product",
                100,
                None,
                "admin",
            )
            .unwrap();
        app.register_principal(
            "Codex 规划副驾",
            PrincipalKind::Agent,
            Some(&team.id),
            Some(&human.id),
            "product",
            100,
            Some(ExecutorConfig {
                kind: ExecutorKind::Codex,
                workspace: directory.path().to_path_buf(),
                model: None,
                command: Some(executable),
            }),
            "admin",
        )
        .unwrap();

        let mut state = UiState::new(
            &app,
            TuiOptions {
                workspace: directory.path().to_path_buf(),
                actor: Some(human.name.clone()),
            },
        );
        state.demand_planner = PlannerKind::Codex;
        state.open_demand_modal();
        state.modal.as_mut().unwrap().value = "Ship the launch".to_string();
        state.submit_modal(&mut app).await;
        assert!(state.active_planning.is_some());

        for _ in 0..300 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            state.poll_planning(&mut app);
            if state.active_planning.is_none() {
                break;
            }
        }

        assert!(state.active_planning.is_none());
        assert_eq!(app.state().flows.len(), 1);
        let flow = app.state().flows.values().next().unwrap();
        assert_eq!(flow.planner, "codex");
        assert_eq!(flow.prd.title, "Model planned launch");
        assert_eq!(flow.tasks.len(), 1);
        assert!(state.message.contains("已生成"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn confirmed_plan_runs_in_background_and_returns_to_ledger() {
        let directory = tempdir().unwrap();
        let executable = directory.path().join("fake-codex");
        fs::write(
            &executable,
            r#"#!/bin/sh
result=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output-last-message" ]; then
    shift
    result="$1"
  fi
  shift
done
printf '%s' 'terminal plan complete' > "$result"
printf '%s\n' '{"thread_id":"fake-thread"}'
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&executable, permissions).unwrap();

        let mut app = MambaApp::open(directory.path().join("data")).unwrap();
        app.init_organization("Mamba Labs", "admin").unwrap();
        let team = app
            .create_team("Platform", "product,delivery", "admin")
            .unwrap();
        let human = app
            .register_principal(
                "牢大",
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
                "Codex 副驾",
                PrincipalKind::Agent,
                Some(&team.id),
                Some(&human.id),
                "delivery",
                100,
                Some(ExecutorConfig {
                    kind: ExecutorKind::Codex,
                    workspace: directory.path().to_path_buf(),
                    model: None,
                    command: Some(executable),
                }),
                "admin",
            )
            .unwrap();
        let flow = app
            .create_demand(
                "Prepare launch brief",
                &human.name,
                PlannerKind::Local,
                directory.path(),
                10,
            )
            .await
            .unwrap();
        let first_task = flow.tasks[0].id.clone();
        let agent_task = flow
            .tasks
            .iter()
            .find(|task| {
                task.assignment
                    .as_ref()
                    .is_some_and(|assignment| assignment.owner.id == agent.id)
            })
            .unwrap()
            .id
            .clone();
        app.approve_flow(&flow.id, &human.name).unwrap();
        app.accept_task(&first_task, &human.name).unwrap();
        app.start_task(&first_task, &human.name).unwrap();
        app.add_evidence(
            &first_task,
            &human.name,
            "document",
            "docs/scope.md",
            "scope approved",
        )
        .unwrap();
        app.submit_task(&first_task, &human.name).unwrap();
        app.complete_task(&first_task, &human.name).unwrap();
        app.accept_task(&agent_task, &human.name).unwrap();

        let mut state = UiState::new(
            &app,
            TuiOptions {
                workspace: directory.path().to_path_buf(),
                actor: Some(human.name.clone()),
            },
        );
        state.view = View::Flows;
        state.focus_tasks = true;
        state.task_index = app
            .state()
            .flow(&flow.id)
            .unwrap()
            .tasks
            .iter()
            .position(|task| task.id == agent_task)
            .unwrap();
        state.open_run_confirmation(&app, ExecutorMode::Plan);
        state.modal.as_mut().unwrap().value = "PASS".to_string();
        state.submit_modal(&mut app).await;
        assert_eq!(state.active_flights.len(), 1);

        for _ in 0..300 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            state.poll_flights(&mut app);
            if state.active_flights.is_empty() {
                break;
            }
        }

        assert!(state.active_flights.is_empty());
        assert_eq!(app.state().executions.len(), 1);
        assert!(state.message.contains("安全落地"));
        assert_eq!(
            app.state().find_task(&agent_task).unwrap().1.evidence.len(),
            1
        );
    }

    #[test]
    fn selection_is_clamped_and_never_wraps() {
        assert_eq!(shifted(0, 3, -1), 0);
        assert_eq!(shifted(1, 3, 1), 2);
        assert_eq!(shifted(2, 3, 1), 2);
        assert_eq!(shifted(5, 0, -1), 0);
    }

    fn mouse_down(area: Rect) -> MouseEvent {
        MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: area.x,
            row: area.y,
            modifiers: KeyModifiers::NONE,
        }
    }

    fn mouse_scroll(area: Rect, up: bool) -> MouseEvent {
        MouseEvent {
            kind: if up {
                MouseEventKind::ScrollUp
            } else {
                MouseEventKind::ScrollDown
            },
            column: area.x,
            row: area.y,
            modifiers: KeyModifiers::NONE,
        }
    }
}
