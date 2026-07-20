# MambaFlow 单节点生产部署

这份手册覆盖当前 v0 能支持的边界：一个 `mamba serve` 进程、一个活动 Tenant、一个 SQLite Ledger，
远程 Worker 分布在员工工作站。它不是多地域高可用方案，也不替代 OIDC/SCIM、容器沙箱或集中 Secret
Manager。

## 1. 数据目录

为服务创建独占系统用户和持久卷。不要把 `.mambaflow` 放在临时文件系统、NFS 或多个服务实例同时
写入的共享卷上。

```bash
install -d -m 0700 -o manbaflow -g manbaflow /var/lib/manbaflow
sudo -u manbaflow mamba --data-dir /var/lib/manbaflow ops doctor
```

启动时数据库使用 WAL、`synchronous=FULL`、外键检查、5 秒 busy timeout 和严格 schema 版本。Unix
数据目录和 SQLite 文件会收紧为 `0700` 与 `0600`。二进制遇到未来版本 schema 会拒绝启动，不会把
版本号静默改回去。

## 2. TLS 入口

默认监听 `127.0.0.1:7777`。推荐让 Caddy、Nginx、Envoy 或云负载均衡器终止 TLS，再转发到 loopback：

```bash
mamba --data-dir /var/lib/manbaflow serve --bind 127.0.0.1:7777
```

容器网络必须监听 `0.0.0.0` 时，只有确认入口与容器之间是可信网络后才能显式确认：

```bash
mamba --data-dir /var/lib/manbaflow serve \
  --bind 0.0.0.0:7777 \
  --allow-insecure-public-http
```

不要把这个端口直接暴露到互联网。MambaFlow 本身尚未终止 TLS，也没有 OIDC 登录。进程内固定窗口
限流只提供每个 Token/匿名键每分钟 300 次的基础保护，入口仍需配置更严格的按 IP、租户与路由限流。

仓库根目录的 `Dockerfile` 使用多阶段构建，最终进程以 UID `10001` 的非 root 用户运行，并把
`/var/lib/manbaflow` 声明为持久卷。默认容器命令已经包含非 loopback HTTP 的显式确认，只能部署在
TLS Ingress 或可信 Service 网络之后。挂载卷必须允许 UID `10001` 写入。

## 3. 身份与 Secret

为每个 Human、Agent、监控器分别签发 Token，禁止共享管理员 Token。新 Token 默认 30 天到期：

```bash
mamba --data-dir /var/lib/manbaflow principal token issue \
  --for "运维审计" --label prometheus --ttl-days 7 --by "租户管理员"
```

原始 Token 只显示一次，数据库仅保存 SHA-256 摘要。Token 有 256 位随机熵；到期或撤销后，数据库查询
和事件状态都会拒绝鉴权。Connector URL、签名 Secret、GitLab Token 仍只通过环境变量注入。环境文件应
由服务用户独占，不要写进 Git、命令参数或 Flow 消息。

## 4. 健康检查与指标

负载均衡器使用：

```text
GET /health/live   -> 进程是否存活
GET /health/ready  -> 组织已初始化，SQLite quick_check=ok
```

`/metrics` 输出 Prometheus 文本，但必须使用拥有 `dashboard_read` 权限的 Bearer Token。当前指标包括
Flow、Task、阻塞、开放航班、待投递通知和 Ledger 事件数。不要为了抓取方便移除认证。

后台 Tracker、通知投递器和 Flight Lease Reaper 的错误会写入 stderr。服务管理器应收集 stderr 并对
重复错误告警。Reaper 会把未领取且超过起飞期限的航班写成 `expired` 事件并释放资源；活动航班的资源
租期覆盖“最晚起飞时间 + 最大 Fuel 时长”，避免最后一刻起飞后文件锁提前失效。

## 5. 备份

每天至少做一次在线快照，并在重要升级前额外执行一次：

```bash
mamba --data-dir /var/lib/manbaflow ops backup \
  --output /var/backups/manbaflow/flow-20260720T120000Z.sqlite
```

命令先做 WAL checkpoint，再使用 SQLite `VACUUM INTO` 产生一致性数据库，拒绝覆盖已有目标，随后对
快照运行 `quick_check`。把备份复制到不同故障域，并使用基础设施自己的加密与保留策略。只复制正在
运行的 `flow.db` 而忽略 WAL 不是受支持的备份方式。

## 6. 恢复演练

1. 停止 `mamba serve`，确认没有 Worker 或 CLI 仍写入旧 Control Plane。
2. 保留损坏数据目录用于取证，不要在原文件上反复尝试修复。
3. 把选定快照放入一个新的空数据目录并命名为 `flow.db`。
4. 以服务用户运行 `mamba --data-dir <new-dir> ops doctor`。
5. 检查 schema、`quick_check=ok`、事件数量和活动凭据数量。
6. 在 loopback 启动服务，验证 `/health/ready`、管理员 Dashboard 和一条只读 Worker 规划航班。
7. 切换 TLS 入口，再恢复远程 Worker。

快照包含 Token 摘要、角色、组织事件和 Connector 元数据，因此按生产数据库同等级保护。原始 Connector
Secret 不在数据库内，灾备环境必须从 Secret Manager 单独恢复。

## 7. 当前限制

- 一个数据目录只能承载一个活动 Tenant；
- 只支持单个 Control Plane 写进程，不支持水平扩展；
- 没有 OIDC、SCIM、集中策略引擎和分布式限流；
- Remote Worker 使用 Git worktree 隔离，不是容器或虚拟机安全边界；
- Office Pack 只生成待 Human 发布的本地草稿，不直接写 Microsoft 365 或 Google Workspace；
- GitLab 连接器不创建、评论、合并 MR。

这些限制需要在上线评审中显式接受。涉及敏感源码、个人数据、受监管数据或不可信 Agent 时，继续使用
测试环境，直到对应的身份、数据面与执行隔离完成。
