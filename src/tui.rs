use std::cmp::Reverse;
use std::path::PathBuf;
use std::time::Duration;

use chrono::Local;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Alignment, Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Gauge, HighlightSpacing, List, ListItem, ListState, Paragraph,
    Row, Table, TableState, Tabs, Wrap,
};
use ratatui::{DefaultTerminal, Frame};

use crate::MambaApp;
use crate::domain::{Flow, FlowStatus, PrincipalKind, Task, TaskStatus};
use crate::error::{MambaError, Result};
use crate::event::EventEnvelope;
use crate::planner::PlannerKind;

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

pub async fn run(app: &mut MambaApp, options: TuiOptions) -> Result<()> {
    let mut terminal = ratatui::init();
    let result = run_loop(app, &mut terminal, options).await;
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
        terminal.draw(|frame| render(frame, app, &mut state))?;
        if event::poll(Duration::from_millis(180))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    if state.handle_key(app, key).await? {
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

#[derive(Clone, Debug)]
enum InputPurpose {
    Demand,
    Evidence { task_id: String },
    Block { task_id: String },
}

#[derive(Clone, Debug)]
struct InputModal {
    purpose: InputPurpose,
    value: String,
}

struct UiState {
    view: View,
    flow_index: usize,
    task_index: usize,
    inbox_index: usize,
    roster_index: usize,
    timeline_index: usize,
    focus_tasks: bool,
    actor_id: Option<String>,
    workspace: PathBuf,
    timeline: Vec<EventEnvelope>,
    modal: Option<InputModal>,
    show_help: bool,
    message: String,
    message_is_error: bool,
}

impl UiState {
    fn new(app: &MambaApp, options: TuiOptions) -> Self {
        let humans = human_ids(app);
        let actor_id = options
            .actor
            .as_deref()
            .and_then(|value| app.state().principal(value).ok())
            .filter(|principal| principal.kind == PrincipalKind::Human)
            .map(|principal| principal.id.clone())
            .or_else(|| humans.first().cloned());
        let mut state = Self {
            view: View::Overview,
            flow_index: 0,
            task_index: 0,
            inbox_index: 0,
            roster_index: 0,
            timeline_index: 0,
            focus_tasks: false,
            actor_id,
            workspace: options.workspace,
            timeline: Vec::new(),
            modal: None,
            show_help: false,
            message: "塔台在线。按 ? 查看完整操作。".to_string(),
            message_is_error: false,
        };
        state.refresh_timeline(app);
        state
    }

    async fn handle_key(&mut self, app: &mut MambaApp, key: KeyEvent) -> Result<bool> {
        if self.show_help {
            if matches!(
                key.code,
                KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q')
            ) {
                self.show_help = false;
            }
            return Ok(false);
        }
        if self.modal.is_some() {
            return self.handle_modal_key(app, key).await;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return Ok(true);
        }

        match key.code {
            KeyCode::Char('q') => return Ok(true),
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Char('1') => self.switch_view(app, View::Overview),
            KeyCode::Char('2') => self.switch_view(app, View::Flows),
            KeyCode::Char('3') => self.switch_view(app, View::Inbox),
            KeyCode::Char('4') => self.switch_view(app, View::Roster),
            KeyCode::Char('5') => self.switch_view(app, View::Timeline),
            KeyCode::Tab => {
                let next = (self.view.index() + 1) % View::ALL.len();
                self.switch_view(app, View::ALL[next]);
            }
            KeyCode::BackTab => {
                let next = (self.view.index() + View::ALL.len() - 1) % View::ALL.len();
                self.switch_view(app, View::ALL[next]);
            }
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(app, 1),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(app, -1),
            KeyCode::Char('h') | KeyCode::Left if self.view == View::Flows => {
                self.focus_tasks = false;
            }
            KeyCode::Char('l') | KeyCode::Right if self.view == View::Flows => {
                self.focus_tasks = true;
            }
            KeyCode::Char('h') | KeyCode::Left if self.view == View::Timeline => {
                self.flow_index = shifted(self.flow_index, flow_ids(app).len(), -1);
                self.refresh_timeline(app);
            }
            KeyCode::Char('l') | KeyCode::Right if self.view == View::Timeline => {
                self.flow_index = shifted(self.flow_index, flow_ids(app).len(), 1);
                self.refresh_timeline(app);
            }
            KeyCode::Char('u') => self.cycle_actor(app),
            KeyCode::Char('r') => match app.reload() {
                Ok(()) => {
                    self.clamp_selection(app);
                    self.refresh_timeline(app);
                    self.success("已从 Flow Ledger 重建塔台状态");
                }
                Err(error) => self.failure(error),
            },
            KeyCode::Char('n') => {
                self.modal = Some(InputModal {
                    purpose: InputPurpose::Demand,
                    value: String::new(),
                });
            }
            KeyCode::Char('a') => self.approve_or_accept(app),
            KeyCode::Char('s') => self.advance_task(app),
            KeyCode::Char('c') => self.complete_task(app),
            KeyCode::Char('e') => self.open_task_input(app, true),
            KeyCode::Char('b') => self.open_task_input(app, false),
            _ => {}
        }
        Ok(false)
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
                "请先注册 Human，或按 u 选择操作人".to_string(),
            ));
            return;
        };

        let result = match modal.purpose {
            InputPurpose::Demand => app
                .create_demand(value, &actor, PlannerKind::Local, &self.workspace, 300)
                .await
                .map(|flow| format!("{} 已生成：{}", flow.id, flow.prd.title)),
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
                self.timeline_index = shifted(self.timeline_index, self.timeline.len(), delta);
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
                .and_then(|flow_id| app.approve_flow(&flow_id, &actor))
                .map(|flow| format!("{} 已批准，WorkRequest 完成传球", flow.id))
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

    fn inbox_items<'a>(&self, app: &'a MambaApp) -> Vec<(&'a Flow, &'a Task)> {
        self.actor_name(app)
            .and_then(|actor| app.inbox(actor).ok())
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
    frame.render_widget(Block::default().style(Style::new().bg(BG)), frame.area());
    let [header, tabs, content, status, help] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(10),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    render_header(frame, app, state, header);
    render_tabs(frame, state, tabs);
    match state.view {
        View::Overview => render_overview(frame, app, state, content),
        View::Flows => render_flows(frame, app, state, content),
        View::Inbox => render_inbox(frame, app, state, content),
        View::Roster => render_roster(frame, app, state, content),
        View::Timeline => render_timeline(frame, state, content),
    }
    render_status(frame, state, status);
    render_shortcuts(frame, state, help);

    if state.show_help {
        render_help_modal(frame);
    } else if let Some(modal) = &state.modal {
        render_input_modal(frame, modal);
    }
}

fn render_header(frame: &mut Frame, app: &MambaApp, state: &UiState, area: Rect) {
    let organization = app
        .state()
        .organization
        .as_ref()
        .map(|org| org.name.as_str())
        .unwrap_or("NO ORGANIZATION");
    let actor = state.actor_name(app).unwrap_or("READ ONLY");
    let [brand, context, clock] = Layout::horizontal([
        Constraint::Length(32),
        Constraint::Min(24),
        Constraint::Length(22),
    ])
    .areas(area);
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

fn render_tabs(frame: &mut Frame, state: &UiState, area: Rect) {
    let titles = View::ALL
        .iter()
        .enumerate()
        .map(|(index, view)| format!(" {} {} ", index + 1, view.title()))
        .collect::<Vec<_>>();
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
    let [metrics, main] = Layout::vertical([Constraint::Length(5), Constraint::Min(6)])
        .spacing(1)
        .areas(area);
    let metric_areas = Layout::horizontal([Constraint::Ratio(1, 4); 4])
        .spacing(1)
        .split(metrics);
    let flows = app.state().flows.values().collect::<Vec<_>>();
    let active = flows
        .iter()
        .filter(|flow| matches!(flow.status, FlowStatus::Approved | FlowStatus::Active))
        .count();
    let blocked = flows
        .iter()
        .flat_map(|flow| &flow.tasks)
        .filter(|task| task.status == TaskStatus::Blocked)
        .count();
    let landed = flows
        .iter()
        .flat_map(|flow| &flow.tasks)
        .filter(|task| task.status == TaskStatus::Completed)
        .count();
    render_metric(frame, metric_areas[0], "TOTAL FLOWS", flows.len(), GOLD);
    render_metric(frame, metric_areas[1], "AIRBORNE", active, CYAN);
    render_metric(frame, metric_areas[2], "BLOCKED", blocked, RED);
    render_metric(frame, metric_areas[3], "LANDED", landed, GREEN);

    if area.width >= 92 {
        let [left, right] =
            Layout::horizontal([Constraint::Percentage(62), Constraint::Percentage(38)])
                .spacing(1)
                .areas(main);
        render_flow_table(frame, app, state, left, true);
        render_tower_brief(frame, state.selected_flow(app), right);
    } else {
        let [top, bottom] =
            Layout::vertical([Constraint::Percentage(58), Constraint::Percentage(42)])
                .spacing(1)
                .areas(main);
        render_flow_table(frame, app, state, top, true);
        render_tower_brief(frame, state.selected_flow(app), bottom);
    }
}

fn render_metric(frame: &mut Frame, area: Rect, label: &str, value: usize, color: Color) {
    let content = Text::from(vec![
        Line::styled(format!("{value:02}"), Style::new().fg(color).bold()),
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
                Cell::from(flow.id.clone()).style(Style::new().fg(MUTED)),
                Cell::from(flow.prd.title.clone()).style(Style::new().fg(TEXT)),
                Cell::from(format!("{completed}/{}", flow.tasks.len()))
                    .style(Style::new().fg(CYAN)),
                Cell::from(flow.p80_finish.format("%m-%d %H:%M").to_string())
                    .style(Style::new().fg(MUTED)),
            ])
            .height(1)
        })
        .collect::<Vec<_>>();
    let header = Row::new(["状态", "FLOW", "目标", "落地", "P80"])
        .style(Style::new().fg(GOLD).bold())
        .bottom_margin(1);
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Min(18),
            Constraint::Length(7),
            Constraint::Length(12),
        ],
    )
    .header(header)
    .row_highlight_style(Style::new().bg(PANEL_ALT).fg(TEXT).bold())
    .highlight_symbol("◆ ")
    .highlight_spacing(HighlightSpacing::Always)
    .column_spacing(1)
    .block(panel_block("FLOW BOARD", focused));
    let mut table_state = TableState::default();
    table_state.select((!ids.is_empty()).then_some(state.flow_index));
    frame.render_stateful_widget(table, area, &mut table_state);
}

fn render_tower_brief(frame: &mut Frame, flow: Option<&Flow>, area: Rect) {
    let Some(flow) = flow else {
        frame.render_widget(
            Paragraph::new("按 n 提出第一个需求。")
                .style(Style::new().fg(MUTED))
                .alignment(Alignment::Center)
                .block(panel_block("TOWER BRIEF", false)),
            area,
        );
        return;
    };
    let blocked = flow
        .tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Blocked)
        .count();
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
    let [brief, gauge] = Layout::vertical([Constraint::Min(5), Constraint::Length(3)]).areas(area);
    let lines = vec![
        Line::styled(flow.prd.title.clone(), Style::new().fg(TEXT).bold()),
        Line::raw(""),
        Line::from(vec![
            Span::styled("需求  ", Style::new().fg(MUTED)),
            Span::styled(flow.demand.summary.clone(), Style::new().fg(TEXT)),
        ]),
        Line::from(vec![
            Span::styled("关键路径  ", Style::new().fg(MUTED)),
            Span::styled(flow.critical_path.join(" → "), Style::new().fg(CYAN)),
        ]),
        Line::from(vec![
            Span::styled("风险  ", Style::new().fg(MUTED)),
            Span::styled(
                if blocked == 0 {
                    "空域正常".to_string()
                } else {
                    format!("{blocked} 个阻塞")
                },
                Style::new().fg(if blocked == 0 { GREEN } else { RED }),
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

fn render_flows(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    if area.width < 92 {
        let [flows, prd, tasks] = Layout::vertical([
            Constraint::Percentage(28),
            Constraint::Percentage(27),
            Constraint::Percentage(45),
        ])
        .spacing(1)
        .areas(area);
        render_flow_selector(frame, app, state, flows, !state.focus_tasks);
        render_prd(frame, state.selected_flow(app), prd);
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
    render_prd(frame, state.selected_flow(app), prd);
    render_tasks(frame, state, app, tasks, state.focus_tasks);
    render_task_detail(
        frame,
        state.selected_task_context(app).map(|(_, task)| task),
        detail,
    );
}

fn render_prd(frame: &mut Frame, flow: Option<&Flow>, area: Rect) {
    let text = flow.map_or_else(
        || Text::from("还没有 Flow。按 n 提出需求。"),
        |flow| {
            let mut lines = vec![
                Line::styled(flow.prd.title.clone(), Style::new().fg(GOLD).bold()),
                Line::styled(flow.prd.summary.clone(), Style::new().fg(TEXT)),
                Line::raw(""),
            ];
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
                Cell::from(flow.prd.title.clone()).style(Style::new().fg(TEXT)),
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
    frame.render_stateful_widget(table, area, &mut table_state);
}

fn render_inbox(frame: &mut Frame, app: &MambaApp, state: &mut UiState, area: Rect) {
    let [table_area, detail_area] =
        Layout::vertical([Constraint::Percentage(58), Constraint::Percentage(42)])
            .spacing(1)
            .areas(area);
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
    frame.render_stateful_widget(table, table_area, &mut table_state);
    render_task_detail(
        frame,
        items.get(state.inbox_index).map(|(_, task)| *task),
        detail_area,
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
        Line::raw(""),
        Line::from(vec![
            Span::styled("副驾  ", Style::new().fg(MUTED)),
            Span::styled(copilots, Style::new().fg(CYAN)),
            Span::styled("    Evidence  ", Style::new().fg(MUTED)),
            Span::styled(task.evidence.len().to_string(), Style::new().fg(GREEN)),
        ]),
    ];
    if let Some(blocker) = &task.blocker {
        lines.push(Line::from(vec![
            Span::styled("BLOCKER  ", Style::new().fg(RED).bold()),
            Span::styled(blocker.clone(), Style::new().fg(RED)),
        ]));
    }
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
                .unwrap_or_else(|| "human".to_string());
            Row::new(vec![
                Cell::from(principal.name.clone()).style(Style::new().fg(TEXT).bold()),
                Cell::from(match principal.kind {
                    PrincipalKind::Human => "HUMAN",
                    PrincipalKind::Agent => "AGENT",
                })
                .style(Style::new().fg(match principal.kind {
                    PrincipalKind::Human => GOLD,
                    PrincipalKind::Agent => CYAN,
                })),
                Cell::from(team.to_string()).style(Style::new().fg(MUTED)),
                Cell::from(format!("{}%", principal.capacity_percent))
                    .style(Style::new().fg(GREEN)),
                Cell::from(terminal).style(Style::new().fg(PURPLE)),
                Cell::from(principal.capabilities.join(", ")).style(Style::new().fg(MUTED)),
            ])
        })
        .collect::<Vec<_>>();
    let table = Table::new(
        rows,
        [
            Constraint::Length(18),
            Constraint::Length(8),
            Constraint::Length(16),
            Constraint::Length(8),
            Constraint::Length(13),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(["成员", "类型", "球队", "产能", "终端", "能力"])
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
    frame.render_stateful_widget(table, principals, &mut table_state);
}

fn render_timeline(frame: &mut Frame, state: &mut UiState, area: Rect) {
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
    frame.render_stateful_widget(list, area, &mut list_state);
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

fn render_shortcuts(frame: &mut Frame, state: &UiState, area: Rect) {
    let context = match state.view {
        View::Overview => "n 新需求  a 批准",
        View::Flows => "h/l 切面板  a 批准/接单  s 推进",
        View::Inbox => "a 接单  s 开工/提交  e 证据  b 阻塞  c 验收",
        View::Roster => "u 切换球权",
        View::Timeline => "h/l 切 Flow  j/k 浏览事件",
    };
    frame.render_widget(
        Paragraph::new(format!(
            " {context}    1-5/Tab 切视图  j/k 移动  r 重放  ? 帮助  q 退出"
        ))
        .style(Style::new().fg(MUTED).bg(PANEL)),
        area,
    );
}

fn render_input_modal(frame: &mut Frame, modal: &InputModal) {
    let area = centered(frame.area(), 72, 9);
    frame.render_widget(Clear, area);
    let (title, prompt, color) = match &modal.purpose {
        InputPurpose::Demand => ("NEW DEMAND / 管理需求", "描述本周需要完成的目标", GOLD),
        InputPurpose::Evidence { .. } => ("EVIDENCE / 交付证据", "输入证据摘要", GREEN),
        InputPurpose::Block { .. } => ("BLOCK / 请求协防", "输入阻塞原因", RED),
    };
    let [hint, input, footer] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(3),
        Constraint::Length(2),
    ])
    .margin(1)
    .areas(area);
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
    frame.render_widget(
        Paragraph::new("Enter 确认    Esc 取消").style(Style::new().fg(MUTED)),
        footer,
    );
}

fn render_help_modal(frame: &mut Frame) {
    let area = centered(frame.area(), 78, 22);
    frame.render_widget(Clear, area);
    let lines = vec![
        Line::styled("塔台操作手册", Style::new().fg(GOLD).bold()),
        Line::raw(""),
        help_line("1-5 / Tab", "切换总览、Flow、Inbox、阵容与时间线"),
        help_line("j / k", "移动当前列表选择"),
        help_line("h / l", "在 Flow 列表与任务列表之间切换球权"),
        help_line("h / l (时间线)", "切换正在审计的 Flow"),
        help_line("u", "切换当前 Human 操作人"),
        help_line("n", "提出新需求，本地规划器生成 PRD 与 DAG"),
        help_line("a", "批准 Flow，或接受当前 Assignment"),
        help_line("s", "按状态接单、开工或提交验收"),
        help_line("e", "为当前任务补充 Evidence"),
        help_line("b", "报告阻塞，等待塔台协防"),
        help_line("c", "由当前 Human 完成最终验收"),
        help_line("r", "从 append-only Flow Ledger 重建状态"),
        help_line("q / Ctrl-C", "安全退出塔台"),
        Line::raw(""),
        Line::styled(
            "执行终端仍通过 task run 显式起飞；TUI 不会静默授予 workspace-write。",
            Style::new().fg(CYAN),
        ),
        Line::styled("按 ?、Esc 或 q 返回", Style::new().fg(MUTED)),
    ];
    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: true }).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::new().fg(GOLD))
                .style(Style::new().bg(PANEL))
                .title(" HELP / WHAT CAN I SAY "),
        ),
        area,
    );
}

fn help_line<'a>(key: &'a str, description: &'a str) -> Line<'a> {
    Line::from(vec![
        Span::styled(format!("{key:<14}"), Style::new().fg(GOLD).bold()),
        Span::styled(description, Style::new().fg(TEXT)),
    ])
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

fn flow_ids(app: &MambaApp) -> Vec<String> {
    let mut flows = app.state().flows.values().collect::<Vec<_>>();
    flows.sort_by_key(|flow| Reverse(flow.created_at));
    flows.into_iter().map(|flow| flow.id.clone()).collect()
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
    let color = if kind.contains("failed") || kind.contains("blocked") || kind.contains("rejected")
    {
        RED
    } else if kind.contains("completed") || kind.contains("finished") {
        GREEN
    } else if kind.contains("approved") || kind.contains("accepted") {
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
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use tempfile::tempdir;

    use super::*;
    use crate::domain::PrincipalKind;

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
    }

    #[test]
    fn selection_is_clamped_and_never_wraps() {
        assert_eq!(shifted(0, 3, -1), 0);
        assert_eq!(shifted(1, 3, 1), 2);
        assert_eq!(shifted(2, 3, 1), 2);
        assert_eq!(shifted(5, 0, -1), 0);
    }
}
