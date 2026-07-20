const API = "/api/v1";
const state = {
  token: sessionStorage.getItem("mambaflow_token") || "",
  authenticated: false,
  dashboard: null,
  recoveryFlight: null,
};

const $ = (selector) => document.querySelector(selector);

function element(tag, className, text) {
  const value = document.createElement(tag);
  if (className) value.className = className;
  if (text !== undefined) value.textContent = text;
  return value;
}

function button(label, action, className = "") {
  const value = element("button", className, label);
  value.type = "button";
  value.addEventListener("click", action);
  return value;
}

function setStatus(message, error = false) {
  const target = $("#status");
  target.textContent = message;
  target.classList.toggle("error", error);
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.token) headers.set("Authorization", `Bearer ${state.token}`);
  if (options.body) headers.set("Content-Type", "application/json");
  const response = await fetch(`${API}${path}`, { ...options, headers });
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const body = await response.json();
      message = body.error || message;
    } catch (_) {}
    if (response.status === 401) openAuth();
    throw new Error(message);
  }
  if (response.status === 204) return null;
  return response.json();
}

function openAuth() {
  const dialog = $("#auth-dialog");
  if (!dialog.open) dialog.showModal();
}

async function loadDashboard(showMessage = true) {
  try {
    if (showMessage) setStatus("正在同步 Flow Ledger...");
    const [me, organization, dashboard] = await Promise.all([
      api("/me"),
      api("/organization"),
      api("/dashboard"),
    ]);
    state.dashboard = dashboard;
    state.authenticated = true;
    $("#identity").textContent = `${me.name} · ${me.kind}`;
    $("#org-name").textContent = organization.organization.name;
    $("#service-state").textContent = `${organization.tenant.name} · ONLINE`;
    renderDashboard(dashboard);
    setStatus(`塔台同步完成 · Ledger 生成于 ${formatDate(dashboard.generated_at)}`);
  } catch (error) {
    setStatus(error.message, true);
  }
}

function renderDashboard(dashboard) {
  $("#generated-at").textContent = formatDate(dashboard.generated_at);
  renderMetrics(dashboard.metrics);
  renderActions(dashboard.action_items);
  renderFlows(dashboard.flows);
  renderFlights(dashboard.flights);
}

function renderMetrics(metrics) {
  const definitions = [
    ["活跃 Flow", metrics.active_flows, ""],
    ["任务完成", `${metrics.completed_tasks}/${metrics.total_tasks}`, "good"],
    ["风险任务", metrics.at_risk_tasks, metrics.at_risk_tasks ? "alert" : ""],
    ["显式阻塞", metrics.blocked_tasks, metrics.blocked_tasks ? "alert" : ""],
    ["等待 Human", metrics.awaiting_human, metrics.awaiting_human ? "wait" : ""],
    ["空中航班", metrics.open_flights, metrics.open_flights ? "wait" : ""],
  ];
  const target = $("#metrics");
  target.replaceChildren(...definitions.map(([label, value, tone]) => {
    const item = element("div", `metric ${tone}`.trim());
    item.append(element("strong", "", String(value)), element("span", "", label));
    return item;
  }));
}

function renderActions(actions) {
  $("#action-count").textContent = `${actions.length} 项`;
  const rows = actions.map((action) => {
    const row = document.createElement("tr");
    row.append(
      cellBadge(action.priority),
      taskCell(action.task_title, action.task_id),
      textCell(action.owner),
      textCell(action.reason),
      textCell(shortDate(action.p80_finish)),
    );
    const command = document.createElement("td");
    const next = taskAction(action.status);
    if (next) {
      command.append(button(next.label, () => mutateTask(action.task_id, next.action)));
    }
    row.append(command);
    return row;
  });
  replaceRows("#action-rows", rows, "当前没有需要 Human 处置的任务", 6);
}

function renderFlows(flows) {
  $("#flow-count").textContent = `${flows.length} 条`;
  const rows = flows.map((flow) => {
    const row = document.createElement("tr");
    const progress = element("div", "progress");
    const fill = document.createElement("i");
    fill.style.width = `${Math.max(0, Math.min(100, flow.progress_percent))}%`;
    progress.append(fill);
    const progressCell = document.createElement("td");
    progressCell.append(progress, element("span", "subline", `${flow.completed_tasks}/${flow.total_tasks} · ${flow.progress_percent}%`));
    row.append(
      cellBadge(flow.health),
      taskCell(flow.title, flow.id),
      textCell(flow.requester),
      progressCell,
      textCell(shortDate(flow.p80_finish)),
    );
    const command = document.createElement("td");
    if (flow.status === "draft") command.append(button("批准 Flow", () => approveFlow(flow.id), "primary"));
    row.append(command);
    return row;
  });
  replaceRows("#flow-rows", rows, "还没有 Flow", 6);
}

function renderFlights(flights) {
  $("#flight-count").textContent = `${flights.length} 架`;
  const target = $("#flight-list");
  if (!flights.length) {
    target.replaceChildren(element("div", "empty", "机队待命"));
    return;
  }
  target.replaceChildren(...flights.map((flight) => {
    const item = element("article", "flight");
    const stateBox = element("div");
    stateBox.append(element("span", `badge ${flight.status}`, flight.status), element("p", "", `A${flight.attempt || "-"} · ${flight.capability_pack || "local"} · ${flight.executor}`));
    const identity = element("div");
    identity.append(element("h3", "", flight.objective || flight.task_id), element("p", "", `${flight.principal} · ${flight.id}`));
    const fuel = renderFuel(
      flight.fuel,
      [...(flight.budget_exhaustions || []), ...(flight.contract_violations || [])],
    );
    const resource = element("div");
    resource.append(element("strong", "", `${flight.active_resource_leases}/${flight.total_resource_claims}`), element("p", "", `${flight.deliverable_count} ARTIFACT · ACTIVE LEASES`));
    const command = element("div");
    if (flight.status === "crashed") command.append(button("处置坠机", () => openRecovery(flight), "danger"));
    item.append(stateBox, identity, fuel, resource, command);
    return item;
  }));
}

function renderFuel(fuel, exhaustions) {
  const wrap = element("div", `fuel-meter ${exhaustions.length ? "over" : ""}`.trim());
  if (!fuel) {
    wrap.append(element("span", "", "Legacy manifest"));
    return wrap;
  }
  const ratio = fuel.duration_budget_seconds
    ? Math.min(100, Math.round((fuel.duration_used_seconds / fuel.duration_budget_seconds) * 100))
    : 0;
  wrap.append(
    element("span", "", `FUEL ${fuel.duration_used_seconds}s / ${fuel.duration_budget_seconds}s`),
    element("strong", "", `${ratio}%`),
  );
  const bar = element("div", "bar");
  const fill = document.createElement("i");
  fill.style.width = `${ratio}%`;
  bar.append(fill);
  wrap.append(bar, element("span", "subline", `CTX ${formatBytes(fuel.context_used_bytes)} / ${formatBytes(fuel.context_budget_bytes)}`));
  if (exhaustions.length) wrap.append(element("span", "subline", exhaustions[0]));
  return wrap;
}

function textCell(value) {
  const cell = document.createElement("td");
  cell.textContent = value || "-";
  return cell;
}

function taskCell(title, id) {
  const cell = element("td", "task-title", title);
  cell.append(element("span", "subline", id));
  return cell;
}

function cellBadge(value) {
  const cell = document.createElement("td");
  cell.append(element("span", `badge ${value}`, String(value).replaceAll("_", " ")));
  return cell;
}

function replaceRows(selector, rows, emptyText, columns) {
  const target = $(selector);
  if (rows.length) {
    target.replaceChildren(...rows);
    return;
  }
  const row = document.createElement("tr");
  const cell = element("td", "empty", emptyText);
  cell.colSpan = columns;
  row.append(cell);
  target.replaceChildren(row);
}

function taskAction(status) {
  if (status === "assigned") return { label: "接单", action: "accept" };
  if (status === "accepted" || status === "blocked") return { label: "开始", action: "start" };
  if (status === "submitted") return { label: "验收", action: "complete" };
  return null;
}

async function mutateTask(taskId, action) {
  try {
    setStatus(`正在推进 ${taskId}...`);
    await api(`/tasks/${encodeURIComponent(taskId)}/${action}`, { method: "POST" });
    await loadDashboard(false);
    setStatus(`${taskId} 已完成 ${action}`);
  } catch (error) { setStatus(error.message, true); }
}

async function approveFlow(flowId) {
  try {
    setStatus(`正在批准 ${flowId}...`);
    await api(`/flows/${encodeURIComponent(flowId)}/approve`, { method: "POST" });
    await loadDashboard(false);
    setStatus(`${flowId} 已批准并完成传球`);
  } catch (error) { setStatus(error.message, true); }
}

async function openRecovery(flight) {
  try {
    const options = await api(`/flight-leases/${encodeURIComponent(flight.id)}/recovery-options`);
    if (!options.length) throw new Error("FlightManifest 没有可用的恢复动作");
    state.recoveryFlight = flight;
    $("#recovery-flight").textContent = `${flight.id} · ${flight.failure_class || "unknown"} · ${flight.summary || "无摘要"}`;
    const select = $("#recovery-action");
    select.replaceChildren(...options.map((action) => {
      const option = document.createElement("option");
      option.value = action;
      option.textContent = recoveryLabel(action);
      return option;
    }));
    toggleExecutor();
    $("#recovery-reason").value = "";
    $("#recovery-objective").value = "";
    $("#recovery-dialog").showModal();
  } catch (error) { setStatus(error.message, true); }
}

function toggleExecutor() {
  $("#executor-field").hidden = $("#recovery-action").value !== "switch_executor";
}

function recoveryLabel(action) {
  return ({
    retry: "沿原航线复飞",
    switch_executor: "更换执行器",
    reduce_scope: "缩小航线",
    human_handoff: "转交 Human",
    ground: "永久停飞",
    fork: "分叉复飞",
  })[action] || action;
}

function formatDate(value) {
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "medium" }).format(new Date(value));
}

function shortDate(value) {
  return new Intl.DateTimeFormat("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }).format(new Date(value));
}

function formatBytes(value) {
  if (value >= 1048576) return `${(value / 1048576).toFixed(1)}M`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)}K`;
  return `${value}B`;
}

$("#auth-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  state.token = $("#token").value.trim();
  sessionStorage.setItem("mambaflow_token", state.token);
  $("#auth-dialog").close();
  await loadDashboard();
});

$("#auth-dialog").addEventListener("cancel", (event) => {
  if (!state.authenticated) event.preventDefault();
});

$("#oidc-login").addEventListener("click", () => {
  const tenant = $("#sso-tenant").value.trim();
  const query = new URLSearchParams({ return_to: "/console" });
  if (tenant) query.set("tenant", tenant);
  window.location.assign(`/auth/oidc/login?${query}`);
});

$("#refresh").addEventListener("click", () => loadDashboard());
$("#logout").addEventListener("click", async () => {
  await fetch("/auth/logout", { method: "POST" });
  state.token = "";
  state.authenticated = false;
  sessionStorage.removeItem("mambaflow_token");
  $("#identity").textContent = "未连接";
  openAuth();
});

$("#demand-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const summary = $("#demand-summary").value.trim();
  if (!summary) return;
  try {
    setStatus("正在生成 PRD 与任务 DAG...");
    await api("/demands", {
      method: "POST",
      body: JSON.stringify({ summary, planner: $("#demand-planner").value, timeout_seconds: 300 }),
    });
    $("#demand-summary").value = "";
    await loadDashboard(false);
    setStatus("PRD 草案已生成，等待 Human 批准");
  } catch (error) { setStatus(error.message, true); }
});

$("#recovery-action").addEventListener("change", toggleExecutor);
$("#recovery-cancel").addEventListener("click", () => $("#recovery-dialog").close());
$("#recovery-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.recoveryFlight) return;
  const action = $("#recovery-action").value;
  const payload = {
    action,
    reason: $("#recovery-reason").value.trim(),
    ttl_seconds: 3600,
  };
  const objective = $("#recovery-objective").value.trim();
  if (objective) payload.objective = objective;
  if (action === "switch_executor") payload.executor = $("#recovery-executor").value;
  try {
    await api(`/flight-leases/${encodeURIComponent(state.recoveryFlight.id)}/recover`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    $("#recovery-dialog").close();
    await loadDashboard(false);
    setStatus(`${state.recoveryFlight.id} 的监督决定已写入黑匣子`);
  } catch (error) { setStatus(error.message, true); }
});

document.querySelectorAll(".rail nav a").forEach((link) => {
  link.addEventListener("click", () => {
    document.querySelectorAll(".rail nav a").forEach((item) => item.classList.remove("active"));
    link.classList.add("active");
  });
});

loadDashboard();
setInterval(() => {
  if (state.authenticated && !document.hidden) loadDashboard(false);
}, 15000);
