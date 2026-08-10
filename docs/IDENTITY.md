# 企业身份接入

Relay 支持三种浏览器登录方式，可以同时启用：

| 方式 | 适用场景 | 是否自动开户 |
| --- | --- | --- |
| 飞书登录 | 内部团队日常使用，零安装 | **是**，按 `tenant_key` 放行 |
| OIDC + SCIM | 对接企业 IdP，需要集中生命周期管理 | 否，必须先由 SCIM 绑定 |
| Principal Token | Agent、Worker 与自动化 | 否 |

## 飞书登录

飞书**不提供** OIDC Discovery，也不签发 ID Token，因此走的是独立实现
（`src/adapters/feishu_auth.rs`），不是下面的 OIDC 链路。端点与集群内 GitLab 现用的
`oauth2_generic` 配置保持一致：

```text
authorize  https://open.feishu.cn/open-apis/authen/v1/authorize
token      https://open.feishu.cn/open-apis/authen/v2/oauth/token
user_info  https://open.feishu.cn/open-apis/authen/v1/user_info
```

**必须复用 GitLab 那个飞书应用。** 两边都以 `open_id` 作为用户标识，用同一个应用才能保证
同一个人在 GitLab 和 Relay 拿到的 `open_id` 完全相同，Flow 里的提交作者和任务负责人才对得上。

在飞书应用后台把回调地址登记为 `https://relay.example.com/auth/feishu/callback`，然后注入：

```bash
export RELAY_FEISHU_APP_ID='cli_xxxxxxxxxxxxxxxx'
export RELAY_FEISHU_APP_SECRET='secret-manager-reference'
export RELAY_FEISHU_REDIRECT_URL='https://relay.example.com/auth/feishu/callback'
export RELAY_FEISHU_ALLOWED_TENANT_KEYS='你的企业 tenant_key'
```

### 自动开户与它的边界

`RELAY_FEISHU_ALLOWED_TENANT_KEYS` **没有默认值，不配就不启用飞书登录**。这是有意的：
任何飞书用户——包括其他公司的——都能走完整个 OAuth 流程，`user_info` 返回的 `tenant_key`
是唯一能区分「本企业员工」的字段。漏配等于对全网开放。

命中白名单的人首次登录会自动创建 Principal 并授予最小的 `Member` 角色，不需要管理员预先添加。
`Member` 只能看自己的 Inbox、接任务、发 Flow 消息和提交 Evidence；创建 Demand、查看组织看板
需要管理员另行授予 `manager`。

这一条**放宽了**下面 OIDC 链路「登录不自动扩张组织边界」的约束。取舍是明确的：飞书侧已经
确认过企业归属，再要求逐个预建账号就谈不上零侵入。需要更严的边界时改用 OIDC + SCIM。

停用某人用 `relay principal` 相关命令把 Principal 置为 inactive；**停用后再次登录不会重新开户**，
会直接返回 401。

登录绑定使用 `provider = feishu`，与[交互网关](INTERACTIONS.md)共用同一条身份绑定记录——
本来就是同一个人的同一个飞书身份，所以登录之后在飞书里点按钮也能被识别，不用再单独绑一次。

### 与 OIDC 的实现差异

没有使用 PKCE。飞书文档未声明 `authen/v2/oauth/token` 接受 `code_verifier`，传未定义参数
有被拒风险。这里是机密客户端——换码必须带 `client_secret`，回调地址也已登记，授权码被截获
换不出 Token；CSRF 由 HMAC 签名的 HttpOnly state cookie 挡住。等飞书正式支持后可以再补。

其余安全约束与 OIDC 一致：state cookie 10 分钟有效、常量时间比较、回调地址必须 HTTPS
（仅 loopback 允许 HTTP）、`return_to` 只允许 `/console`、Session 8 小时。

## OIDC 登录

OIDC 这条链路不会自动扩张组织边界；IdP 返回的 `sub` 必须已经由 SCIM User 的 `externalId`
绑定到一个活动 Human。

## OIDC 登录

在 IdP 注册一个 Web Application，并把回调地址精确登记为：

```text
https://relay.example.com/auth/oidc/callback
```

启动服务前注入：

```bash
export RELAY_OIDC_ISSUER='https://id.example.com'
export RELAY_OIDC_CLIENT_ID='relay'
export RELAY_OIDC_CLIENT_SECRET='secret-manager-reference'
export RELAY_OIDC_REDIRECT_URL='https://relay.example.com/auth/oidc/callback'
```

服务会通过 Discovery 获取授权、Token 和 JWKS 地址，执行 Authorization Code + S256 PKCE，并校验
`state`、`nonce`、Issuer、Audience、签名和 ID Token 有效期。Discovery HTTP 客户端不跟随重定向；
Issuer 和回调必须使用 HTTPS，只有 loopback 开发地址允许 HTTP。

登录中间状态保存在 10 分钟有效、HMAC 签名的 HttpOnly Cookie 中，因此回调可以落在另一个 PostgreSQL
Control Plane 副本。成功后签发 8 小时 OIDC Session；数据库只保存 Token 摘要，Cookie 使用 HttpOnly、
SameSite=Lax，并在 HTTPS 部署时带 Secure。`POST /auth/logout` 会撤销服务端 Session，而不只是删除
浏览器 Cookie。SCIM 停用 Principal 后，所有现有 Session 在下一次请求时立即失效。

多 Tenant 登录从 Console 输入 Tenant ID，或直接访问：

```text
/auth/oidc/login?tenant=TEN-xxxxxxxx&return_to=/console
```

`return_to` 只允许站内绝对路径，不能用来跳转到外部站点。

## SCIM 2.0

生产环境应为每个 Tenant 创建独立的高熵 Bearer Token：

```bash
export RELAY_SCIM_TOKENS_JSON='{
  "TEN-aaaaaaaa": "replace-with-at-least-32-characters-for-apac",
  "TEN-bbbbbbbb": "replace-with-at-least-32-characters-for-emea"
}'
```

单 Tenant 兼容部署可以设置 `RELAY_SCIM_BEARER_TOKEN`，但配置 `RELAY_SCIM_TOKENS_JSON` 后只接受
对应 Tenant 的专用 Token。Token 以常量时间比较，不能写入 Ledger 或仓库。

在 IdP 中配置 SCIM Base URL：

```text
https://relay.example.com/scim/v2
```

非默认 Tenant 的 SCIM 请求必须携带 `x-relay-tenant: TEN-xxxxxxxx`，Tower 会先按 Tenant 选路，再验证
该 Tenant 的 SCIM Token。支持 User / Group 的查询、创建、PUT、PATCH 和 DELETE，以及
`ServiceProviderConfig`、`ResourceTypes` 和 `Schemas` 发现端点。

字段映射如下：

| SCIM | Relay |
| --- | --- |
| User `id` | OIDC Directory Binding ID |
| User `externalId` | OIDC `sub`，创建后不可修改 |
| User `userName` | Directory Username |
| User `displayName` | Principal Name |
| User `active` | Principal Active |
| Group `id` | Team ID |
| Group `externalId` | Directory Group ID |
| Group `members` | Principal 的 Team Assignment |

推荐先同步 Group，再同步 User，最后写入 Group membership。User 创建时会获得最小 `member` 角色；管理员、
经理和 Agent 注册仍通过 Relay 的组织授权流程处理。删除 User 会解绑 OIDC Subject、停用 Principal 并
使 Session 失效；删除 Group 会停用 Team 并解除其成员归属，但不会删除审计事件。

## 上线检查

1. 在测试 Tenant 中同步一个 Group 和一个 Human，确认 `externalId` 与 ID Token 的 `sub` 完全一致。
2. 从 `/console` 完成登录，确认浏览器没有可被 JavaScript 读取的 Session Token。
3. 在 IdP 停用测试 Human，触发 SCIM 同步，确认现有 Console Session 立即返回 `401`。
4. 用 Tenant A 的 SCIM Token 访问 Tenant B，确认返回 `401`。
5. 在两个 Control Plane 副本间重复登录，确认授权请求和回调可以命中不同副本。
6. 在 TLS 入口限制 `/scim/v2` 来源、请求速率和最大 Body，并对失败率告警。
