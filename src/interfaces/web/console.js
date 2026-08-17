const API = "/api/v1";
const state = {
  token: sessionStorage.getItem("relay_token") || "",
  authenticated: false,
  dashboard: null,
  recoveryFlight: null,
  repositories: [],
  agents: [],
  startTask: null,
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
    if (showMessage) setStatus("正在同步…");
    const [me, organization, dashboard, repositories, principals] = await Promise.all([
      api("/me"),
      api("/organization"),
      api("/dashboard"),
      api("/repositories").catch(() => []),
      api("/principals").catch(() => []),
    ]);
    state.dashboard = dashboard;
    state.repositories = repositories;
    state.agents = principals.filter((principal) => principal.kind === "agent" && principal.active);
    state.authenticated = true;
    $("#identity").textContent = `${me.name} · ${me.kind}`;
    $("#org-name").textContent = organization.organization.name;
    $("#service-state").textContent = `${organization.tenant.name} · 在线`;
    renderRepositories(state.repositories);
    renderDashboard(dashboard);
    setStatus(`已同步 · 数据截至 ${formatDate(dashboard.generated_at)}`);
  } catch (error) {
    setStatus(error.message, true);
  }
}

function renderDashboard(dashboard) {
  $("#generated-at").textContent = formatDate(dashboard.generated_at);
  renderMetrics(dashboard.metrics);
  renderActions(
    dashboard.action_items,
    dashboard.office_releases || [],
    dashboard.gitlab_writes || [],
  );
  renderFlows(dashboard.flows);
  renderFlights(dashboard.flights);
}

function renderMetrics(metrics) {
  const definitions = [
    ["进行中的需求", metrics.active_flows, ""],
    ["任务完成", `${metrics.completed_tasks}/${metrics.total_tasks}`, "good"],
    ["可能延期", metrics.at_risk_tasks, metrics.at_risk_tasks ? "alert" : ""],
    ["卡住了", metrics.blocked_tasks, metrics.blocked_tasks ? "alert" : ""],
    ["等人确认", metrics.awaiting_human, metrics.awaiting_human ? "wait" : ""],
    ["Agent 在跑", metrics.open_flights, metrics.open_flights ? "wait" : ""],
  ];
  const target = $("#metrics");
  target.replaceChildren(...definitions.map(([label, value, tone]) => {
    const item = element("div", `metric ${tone}`.trim());
    item.append(element("strong", "", String(value)), element("span", "", label));
    return item;
  }));
}

function renderRepositories(repositories) {
  const active = repositories.filter((repository) => repository.active);
  $("#repository-count").textContent = `${active.length} 个`;

  // 提需求时的仓库下拉，只列还在用的。
  const select = $("#demand-repository");
  const previous = select.value;
  const options = [element("option", "", "不指定")];
  options[0].value = "";
  for (const repository of active) {
    const option = element("option", "", `${repository.name} (${repository.gitlab_project_path})`);
    option.value = repository.id;
    options.push(option);
  }
  select.replaceChildren(...options);
  if (active.some((repository) => repository.id === previous)) select.value = previous;

  const rows = repositories.map((repository) => {
    const row = document.createElement("tr");
    row.append(
      taskCell(repository.name, repository.id),
      textCell(repository.gitlab_project_path),
      textCell(repository.default_branch),
      cellBadge(repository.active ? "在用" : "已归档"),
    );
    const command = document.createElement("td");
    if (repository.active) {
      command.append(button("归档", () => archiveRepository(repository.id), "danger"));
    }
    row.append(command);
    return row;
  });
  replaceRows("#repository-rows", rows, "还没有登记仓库。先登记一个，才能把需求落到具体项目上。", 5);
}

async function archiveRepository(repositoryId) {
  if (!window.confirm("归档后不能再往这个仓库派新任务，历史记录会保留。确定吗？")) return;
  try {
    await api(`/repositories/${encodeURIComponent(repositoryId)}/archive`, { method: "POST" });
    await loadDashboard(false);
    setStatus("仓库已归档");
  } catch (error) { setStatus(error.message, true); }
}

function renderActions(actions, releases, gitlabWrites) {
  const releaseActions = releases.filter((release) => ["requested", "failed", "indeterminate"].includes(release.status));
  const gitlabActions = gitlabWrites.filter((request) => ["requested", "failed", "indeterminate"].includes(request.status));
  $("#action-count").textContent = `${actions.length + releaseActions.length + gitlabActions.length} 项`;
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
    // 已经接单/进行中的任务，可以直接交给 Agent 去做。
    if (["accepted", "in_progress"].includes(action.status) && state.agents.length) {
      command.append(button("让 Agent 开工", () => openStart(action), "primary"));
    }
    row.append(command);
    return row;
  });
  rows.push(...releaseActions.map((release) => {
    const row = document.createElement("tr");
    const reason = release.status === "requested"
      ? `等你放行 · 内容校验 ${release.payload_sha256.slice(0, 12)}`
      : (release.last_error || "需要你去对方系统核对结果");
    row.append(
      cellBadge(release.status === "requested" ? "high" : "critical"),
      taskCell(release.summary, release.id),
      textCell(release.requested_by),
      textCell(reason),
      textCell(shortDate(release.requested_at)),
    );
    const command = document.createElement("td");
    if (release.status === "requested") {
      command.append(
        button("放行", () => mutateRelease(release.id, "approve"), "primary"),
        button("驳回", () => rejectRelease(release.id), "danger"),
      );
    } else if (release.status === "failed") {
      command.append(button("再次放行", () => mutateRelease(release.id, "retry")));
    }
    row.append(command);
    return row;
  }));
  rows.push(...gitlabActions.map((request) => {
    const row = document.createElement("tr");
    const reason = request.status === "requested"
      ? `等你放行 · ${request.project} · 内容校验 ${request.payload_sha256.slice(0, 12)}`
      : (request.last_error || "需要你先去 GitLab 核对写入结果");
    row.append(
      cellBadge(request.status === "requested" ? "high" : "critical"),
      taskCell(request.summary, request.id),
      textCell(request.requested_by),
      textCell(reason),
      textCell(shortDate(request.requested_at)),
    );
    const command = document.createElement("td");
    if (request.status === "requested") {
      command.append(
        button("放行", () => mutateGitLabWrite(request.id, "approve"), "primary"),
        button("驳回", () => rejectGitLabWrite(request.id), "danger"),
      );
    } else {
      command.append(button(
        request.status === "indeterminate" ? "已核对，重试" : "再次放行",
        () => mutateGitLabWrite(request.id, "retry"),
      ));
    }
    row.append(command);
    return row;
  }));
  replaceRows("#action-rows", rows, "没有需要你确认的事", 6);
}

async function mutateRelease(releaseId, action, body) {
  try {
    setStatus("正在处理文档发布…");
    await api(`/office/releases/${releaseId}/${action}`, {
      method: "POST",
      body: body ? JSON.stringify(body) : undefined,
    });
    await loadDashboard(false);
    setStatus("文档发布已处理");
  } catch (error) {
    setStatus(error.message, true);
  }
}

function rejectRelease(releaseId) {
  const reason = window.prompt("驳回原因");
  if (reason && reason.trim()) mutateRelease(releaseId, "reject", { reason: reason.trim() });
}

async function mutateGitLabWrite(writeId, action, body) {
  try {
    setStatus("正在处理 GitLab 写入…");
    await api(`/gitlab/writes/${writeId}/${action}`, {
      method: "POST",
      body: body ? JSON.stringify(body) : undefined,
    });
    await loadDashboard(false);
    setStatus("GitLab 写入已处理");
  } catch (error) {
    setStatus(error.message, true);
  }
}

function rejectGitLabWrite(writeId) {
  const reason = window.prompt("驳回原因");
  if (reason && reason.trim()) {
    mutateGitLabWrite(writeId, "reject", { reason: reason.trim() });
  }
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
    if (flow.status === "draft") command.append(button("确认方案，开始执行", () => approveFlow(flow.id), "primary"));
    row.append(command);
    return row;
  });
  replaceRows("#flow-rows", rows, "还没有需求。到「提需求」开始。", 6);
}

function renderFlights(flights) {
  $("#flight-count").textContent = `${flights.length} 架`;
  const target = $("#flight-list");
  if (!flights.length) {
    target.replaceChildren(element("div", "empty", "现在没有 Agent 在跑"));
    return;
  }
  target.replaceChildren(...flights.map((flight) => {
    const item = element("article", "flight");
    const stateBox = element("div");
    const image = flight.sandbox_image_id?.replace("sha256:", "").slice(0, 12);
    const sandbox = flight.sandbox_backend === "legacy"
      ? "legacy"
      : flight.sandbox_backend
        ? `${flight.sandbox_backend}/${flight.sandbox_network || "unknown"}/${image || "host"}`
        : "pending";
    stateBox.append(
      element("span", `badge ${flight.status}`, flightStatusLabel(flight.status)),
      element("p", "", `第 ${flight.attempt || 1} 次 · ${flight.executor} · 沙箱 ${sandbox}`),
    );
    const identity = element("div");
    identity.append(element("h3", "", flight.objective || flight.task_id), element("p", "", `${flight.principal} · ${flight.id}`));
    const fuel = renderFuel(
      flight.fuel,
      [...(flight.budget_exhaustions || []), ...(flight.contract_violations || [])],
    );
    const resource = element("div");
    resource.append(
      element("strong", "", `${flight.deliverable_count || 0}`),
      element("p", "", "个产出文件"),
    );
    const command = element("div");
    if (flight.deliverable_count) {
      command.append(button("看产出", () => showArtifacts(flight)));
    }
    if (flight.status === "crashed") command.append(button("执行失败，处理", () => openRecovery(flight), "danger"));
    item.append(stateBox, identity, fuel, resource, command);
    return item;
  }));
}

function renderFuel(fuel, exhaustions) {
  const wrap = element("div", `fuel-meter ${exhaustions.length ? "over" : ""}`.trim());
  if (!fuel) {
    wrap.append(element("span", "", "无用量记录"));
    return wrap;
  }
  const ratio = fuel.duration_budget_seconds
    ? Math.min(100, Math.round((fuel.duration_used_seconds / fuel.duration_budget_seconds) * 100))
    : 0;
  wrap.append(
    element("span", "", `已用 ${fuel.duration_used_seconds}s / 上限 ${fuel.duration_budget_seconds}s`),
    element("strong", "", `${ratio}%`),
  );
  const bar = element("div", "bar");
  const fill = document.createElement("i");
  fill.style.width = `${ratio}%`;
  bar.append(fill);
  wrap.append(bar, element("span", "subline", `上下文 ${formatBytes(fuel.context_used_bytes)} / ${formatBytes(fuel.context_budget_bytes)}`));
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
    setStatus("正在更新任务…");
    await api(`/tasks/${encodeURIComponent(taskId)}/${action}`, { method: "POST" });
    await loadDashboard(false);
    setStatus(`${taskId} 已更新`);
  } catch (error) { setStatus(error.message, true); }
}

async function approveFlow(flowId) {
  try {
    setStatus(`正在批准 ${flowId}...`);
    await api(`/flows/${encodeURIComponent(flowId)}/approve`, { method: "POST" });
    await loadDashboard(false);
    setStatus(`${flowId} 已确认，任务已分配`);
  } catch (error) { setStatus(error.message, true); }
}

async function openRecovery(flight) {
  try {
    const options = await api(`/flight-leases/${encodeURIComponent(flight.id)}/recovery-options`);
    if (!options.length) throw new Error("这次执行没有可选的处理方式");
    state.recoveryFlight = flight;
    $("#recovery-flight").textContent = `${flight.objective || flight.task_id} · ${flight.summary || "没有更多信息"}`;
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
    retry: "原样重试",
    switch_executor: "换一个执行器重试",
    reduce_scope: "缩小范围再试",
    human_handoff: "转给人来做",
    ground: "放弃这个任务",
    fork: "换个方案重试",
  })[action] || action;
}

function flightStatusLabel(status) {
  return ({
    authorized: "已授权，等 Agent 领取",
    active: "正在执行",
    landed: "执行完成",
    crashed: "执行失败",
    revoked: "已撤销",
    expired: "已过期",
  })[status] || status;
}

async function showArtifacts(flight) {
  try {
    setStatus("正在读取产出…");
    const artifacts = await api(`/flight-leases/${encodeURIComponent(flight.id)}/artifacts`);
    if (!artifacts.length) {
      setStatus("这次执行没有留下产出文件");
      return;
    }
    const lines = artifacts
      .map((artifact) => `${artifact.path}（${formatBytes(artifact.size_bytes || 0)}）`)
      .join("\n");
    window.alert(`本次改动的文件：\n\n${lines}`);
    setStatus(`共 ${artifacts.length} 个产出文件`);
  } catch (error) { setStatus(error.message, true); }
}

function openStart(action) {
  if (!state.agents.length) {
    setStatus("还没有注册可用的 Agent", true);
    return;
  }
  state.startTask = action;
  $("#start-task").textContent = `${action.task_title}（${action.task_id}）`;
  $("#start-agent").replaceChildren(...state.agents.map((agent) => {
    const option = element("option", "", agent.name);
    option.value = agent.name;
    return option;
  }));
  $("#start-dialog").showModal();
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
  sessionStorage.setItem("relay_token", state.token);
  $("#auth-dialog").close();
  await loadDashboard();
});

$("#auth-dialog").addEventListener("cancel", (event) => {
  if (!state.authenticated) event.preventDefault();
});

function startLogin(path) {
  const tenant = $("#sso-tenant").value.trim();
  const query = new URLSearchParams({ return_to: "/console" });
  if (tenant) query.set("tenant", tenant);
  window.location.assign(`${path}?${query}`);
}

$("#feishu-login").addEventListener("click", () => startLogin("/auth/feishu/login"));
$("#oidc-login").addEventListener("click", () => startLogin("/auth/oidc/login"));

// 只显示本部署真正配置了的登录方式。默认全部隐藏：探测失败时留下令牌登录这条
// 一定可用的路，也好过给出一个点了就报错的按钮。
async function detectLoginMethods() {
  try {
    const response = await fetch("/api/v1/auth/methods");
    if (!response.ok) return;
    const methods = await response.json();
    $("#feishu-block").hidden = !methods.feishu;
    $("#oidc-block").hidden = !methods.oidc;
    $("#auth-divider").hidden = !(methods.feishu || methods.oidc);
  } catch {
    /* 探测失败就维持隐藏，令牌登录不受影响 */
  }
}
detectLoginMethods();

$("#refresh").addEventListener("click", () => loadDashboard());
$("#logout").addEventListener("click", async () => {
  await fetch("/auth/logout", { method: "POST" });
  state.token = "";
  state.authenticated = false;
  sessionStorage.removeItem("relay_token");
  $("#identity").textContent = "未连接";
  openAuth();
});

$("#demand-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const summary = $("#demand-summary").value.trim();
  if (!summary) return;
  try {
    setStatus("正在拆解需求…");
    await api("/demands", {
      method: "POST",
      body: JSON.stringify({
        summary,
        planner: $("#demand-planner").value,
        timeout_seconds: 300,
        ...(($("#demand-repository").value) ? { repository: $("#demand-repository").value } : {}),
      }),
    });
    $("#demand-summary").value = "";
    await loadDashboard(false);
    setStatus("方案已生成，去「等我确认」里过一遍");
  } catch (error) { setStatus(error.message, true); }
});

$("#repository-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const project = $("#repository-project").value.trim();
  if (!project) return;
  const name = $("#repository-name").value.trim();
  try {
    setStatus("正在验证 GitLab 项目…");
    await api("/repositories", {
      method: "POST",
      body: JSON.stringify({
        gitlab_project_path: project,
        ...(name ? { name } : {}),
      }),
    });
    $("#repository-project").value = "";
    $("#repository-name").value = "";
    await loadDashboard(false);
    setStatus("仓库已登记，现在可以在提需求时选它了");
  } catch (error) { setStatus(error.message, true); }
});

$("#start-cancel").addEventListener("click", () => $("#start-dialog").close());
$("#start-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.startTask) return;
  const taskId = state.startTask.task_id;
  try {
    setStatus("正在授权 Agent 执行…");
    await api(`/tasks/${encodeURIComponent(taskId)}/flight-leases`, {
      method: "POST",
      body: JSON.stringify({
        agent: $("#start-agent").value,
        executor: $("#start-executor").value,
      }),
    });
    $("#start-dialog").close();
    await loadDashboard(false);
    setStatus("已授权。Agent 领取后会开始执行，完成后到「执行与交付」看产出。");
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
    setStatus(`${state.recoveryFlight.id} 的处理决定已记录`);
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
