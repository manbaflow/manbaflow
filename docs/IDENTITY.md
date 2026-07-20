# 企业身份接入

MambaFlow 把企业身份拆成两条明确的链：SCIM 负责成员和团队的生命周期，OIDC 负责浏览器登录。登录
不会自动扩张组织边界；IdP 返回的 `sub` 必须已经由 SCIM User 的 `externalId` 绑定到一个活动 Human。

## OIDC 登录

在 IdP 注册一个 Web Application，并把回调地址精确登记为：

```text
https://mamba.example.com/auth/oidc/callback
```

启动服务前注入：

```bash
export MAMBA_OIDC_ISSUER='https://id.example.com'
export MAMBA_OIDC_CLIENT_ID='mambaflow'
export MAMBA_OIDC_CLIENT_SECRET='secret-manager-reference'
export MAMBA_OIDC_REDIRECT_URL='https://mamba.example.com/auth/oidc/callback'
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
export MAMBA_SCIM_TOKENS_JSON='{
  "TEN-aaaaaaaa": "replace-with-at-least-32-characters-for-apac",
  "TEN-bbbbbbbb": "replace-with-at-least-32-characters-for-emea"
}'
```

单 Tenant 兼容部署可以设置 `MAMBA_SCIM_BEARER_TOKEN`，但配置 `MAMBA_SCIM_TOKENS_JSON` 后只接受
对应 Tenant 的专用 Token。Token 以常量时间比较，不能写入 Ledger 或仓库。

在 IdP 中配置 SCIM Base URL：

```text
https://mamba.example.com/scim/v2
```

非默认 Tenant 的 SCIM 请求必须携带 `x-mamba-tenant: TEN-xxxxxxxx`，Tower 会先按 Tenant 选路，再验证
该 Tenant 的 SCIM Token。支持 User / Group 的查询、创建、PUT、PATCH 和 DELETE，以及
`ServiceProviderConfig`、`ResourceTypes` 和 `Schemas` 发现端点。

字段映射如下：

| SCIM | MambaFlow |
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
经理和 Agent 注册仍通过 MambaFlow 的组织授权流程处理。删除 User 会解绑 OIDC Subject、停用 Principal 并
使 Session 失效；删除 Group 会停用 Team 并解除其成员归属，但不会删除审计事件。

## 上线检查

1. 在测试 Tenant 中同步一个 Group 和一个 Human，确认 `externalId` 与 ID Token 的 `sub` 完全一致。
2. 从 `/console` 完成登录，确认浏览器没有可被 JavaScript 读取的 Session Token。
3. 在 IdP 停用测试 Human，触发 SCIM 同步，确认现有 Console Session 立即返回 `401`。
4. 用 Tenant A 的 SCIM Token 访问 Tenant B，确认返回 `401`。
5. 在两个 Control Plane 副本间重复登录，确认授权请求和回调可以命中不同副本。
6. 在 TLS 入口限制 `/scim/v2` 来源、请求速率和最大 Body，并对失败率告警。
