const BASE = "/api/v1";

// 令牌只放 sessionStorage：关掉标签页就没了，不留在磁盘上。
// 飞书 / OIDC 登录走 Cookie，不经过这里。
export const auth = {
  get token() {
    return sessionStorage.getItem("relay_token") || "";
  },
  set token(value) {
    if (value) sessionStorage.setItem("relay_token", value);
    else sessionStorage.removeItem("relay_token");
  },
};

export class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

async function request(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (auth.token) headers.set("Authorization", `Bearer ${auth.token}`);
  if (options.body) headers.set("Content-Type", "application/json");

  const response = await fetch(`${BASE}${path}`, { ...options, headers });
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const body = await response.json();
      // 服务端把原因放在 error 里，一定要显示出来——之前登记仓库失败
      // 什么都不说，只能靠猜。
      if (body.error) message = body.error;
    } catch {
      /* 非 JSON 响应就用状态码 */
    }
    throw new ApiError(message, response.status);
  }
  if (response.status === 204) return null;
  return response.json();
}

const get = (path) => request(path);
const post = (path, body) =>
  request(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body) });

export const api = {
  authMethods: () => get("/auth/methods"),
  me: () => get("/me"),
  organization: () => get("/organization"),
  dashboard: () => get("/dashboard"),
  principals: () => get("/principals"),

  repositories: () => get("/repositories"),
  registerRepository: (payload) => post("/repositories", payload),
  archiveRepository: (id) => post(`/repositories/${encodeURIComponent(id)}/archive`),

  createDemand: (payload) => post("/demands", payload),
  approveFlow: (id) => post(`/flows/${encodeURIComponent(id)}/approve`),

  authorizeFlight: (taskId, payload) =>
    post(`/tasks/${encodeURIComponent(taskId)}/flight-leases`, payload),
  recoverFlight: (id, payload) => post(`/flight-leases/${encodeURIComponent(id)}/recover`, payload),

  logout: () => fetch("/auth/logout", { method: "POST" }),
};
