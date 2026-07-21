# MambaFlow 团队安装

这套安装路径只创建真实的组织、首个团队、Tenant 管理员和登录 Token，不写入任何 Showcase Flow。
默认部署包含 PostgreSQL 18、MambaFlow Control Plane 和持久卷；公网模式额外启动 Caddy，自动申请和续期
TLS 证书。

## 1. 前置条件

- Linux 服务器、macOS 或 Windows + WSL2；
- Docker Engine / Docker Desktop；
- Docker Compose v2；
- 建议至少 2 CPU、4 GiB 内存和 20 GiB 可用磁盘。

公网部署还需要一台有公网地址的服务器、一个已经指向该服务器的域名，以及开放入站 `80/tcp`、
`443/tcp` 和 `443/udp`。MambaFlow 的 `7777` 端口始终只绑定宿主机 loopback。

## 2. 一条命令安装

内网或单机团队直接运行：

```bash
./deploy/install.sh
```

脚本会询问组织名、管理员、首个团队和 UTC 偏移，自动生成 PostgreSQL 密码、构建镜像、等待健康检查、
执行幂等生产初始化并输出 Console 地址和一次性管理员 Token。

无交互安装适合自动化服务器：

```bash
./deploy/install.sh \
  --local \
  --non-interactive \
  --organization "Acme" \
  --administrator "Alice" \
  --team "Platform" \
  --capabilities "product,delivery,backend,operations" \
  --utc-offset +08:00
```

浏览器打开 `http://127.0.0.1:7777/console`，输入安装时只显示一次的 Token。`.env` 包含数据库 Secret，
安装器会创建为 `0600` 且 Git 默认忽略；仍应把 Token 放进密码管理器，不要放进聊天、Issue 或仓库。

## 3. 公网 HTTPS

先设置 DNS，再在服务器运行：

```bash
./deploy/install.sh \
  --hosted flow.example.com \
  --organization "Acme" \
  --administrator "Alice" \
  --team "Platform" \
  --utc-offset +08:00
```

Caddy 监听 `80/443` 并反向代理到仅容器网络可见的 Control Plane。安装器只等待 MambaFlow 内部健康；
首次证书签发还可能需要几十秒，可通过 `./deploy/manage.sh logs caddy` 查看。公网地址为
`https://flow.example.com/console`。

不要把 Compose 中的 MambaFlow 端口改成 `0.0.0.0:7777`。如果使用云负载均衡器、Kubernetes Ingress
或公司现有网关，可以不启用 Caddy profile，但必须在可信 TLS 入口后转发到 `7777`。

## 4. 添加真实团队成员

安装器只创建首位管理员，不替你制造组织数据。管理员可以通过受认证 API、SCIM，或在服务器上运行
一次性 CLI 容器添加成员：

```bash
docker compose run --rm mamba team add \
  --name Engineering \
  --capabilities "backend,rust,quality" \
  --by Alice

docker compose run --rm mamba principal add \
  --name Bob \
  --kind human \
  --team Engineering \
  --capabilities "backend,rust" \
  --by Alice

docker compose run --rm mamba principal token issue \
  --for Bob \
  --label browser \
  --ttl-days 30 \
  --by Alice
```

随后 Bob 使用自己的 Token 登录，不能共享 Alice 的管理员 Token。企业目录应接入
[OIDC / SCIM](IDENTITY.md)，让入职、团队调整和离职停用由 IdP 驱动。

个人 Agent 也使用独立身份：

```bash
docker compose run --rm mamba principal add \
  --name "Bob 的 Codex" \
  --kind agent \
  --team Engineering \
  --owner Bob \
  --capabilities "backend,rust,quality" \
  --by Alice
```

Remote Worker 的 Docker Runtime、模型凭据和启动方式见 [Worker 沙箱](WORKER_SANDBOX.md)。

## 5. 日常运维

```bash
./deploy/manage.sh status
./deploy/manage.sh logs
./deploy/manage.sh logs mamba
./deploy/manage.sh backup
./deploy/manage.sh stop
./deploy/manage.sh start
```

`backup` 使用 PostgreSQL custom format 写入 `backups/`，文件权限为 `0600`。至少把备份复制到另一个
故障域并定期做恢复演练。恢复属于破坏性操作，先停止写入，再按 [生产部署手册](DEPLOYMENT.md) 在新的
数据库实例中执行，不要直接覆盖仍在工作的数据库。

从源码部署时，升级顺序为：

```bash
git pull --ff-only
./deploy/manage.sh upgrade
```

`upgrade` 会先备份，再拉取基础镜像、重新构建并滚动重启。未来发布预构建镜像后，可在首次安装时传
`--image registry.example.com/manbaflow/manbaflow:VERSION`；安装器和升级命令会改为 pull，不在服务器编译。

`./deploy/manage.sh stop` 保留所有卷。只有明确确认永久删除数据库、Artifact 和 TLS 状态时，才手工执行
`docker compose --profile hosted down --volumes`。

## 6. Connector 与企业身份

把实际启用的环境变量追加到 `.env`，再重新创建 MambaFlow 容器：

```bash
docker compose up -d --force-recreate mamba
```

不要保留空的 Connector 变量；空值会被配置校验拒绝。示例名称见 [.env.example](../.env.example)。

- OIDC / SCIM：[企业身份接入](IDENTITY.md)
- Microsoft 365 / Google Workspace：[Office Release Gate](OFFICE.md)
- GitLab 写入：[GitLab Human Gate](GITLAB_WRITES.md)
- Slack、飞书、Teams：[交互 Bridge](INTERACTIONS.md)

生产 Secret 应由 Secret Manager 注入。Compose `.env` 是小团队自托管的最低可用方案，不是大型企业的
最终 Secret 管理形态。

## 7. 托管服务模式

当前仓库没有运行中的官方 MambaFlow SaaS，但同一二进制已经支持 PostgreSQL 多副本和 Tenant 隔离，
运营方可以在一套 Control Plane 中为客户创建独立 Tenant：

```bash
docker compose run --rm mamba tenant create \
  --name "Customer A" --slug customer-a

docker compose run --rm mamba --tenant customer-a setup \
  --organization "Customer A" \
  --administrator "Customer Admin" \
  --team "Core Team" \
  --utc-offset +08:00
```

第二条命令为该 Tenant 单独签发管理员 Token；Token 自带 Tenant 路由提示，业务事件、凭据摘要和 Artifact
都按 Tenant 隔离。真正对外经营的 SaaS 仍需补充注册/计费、配额、区域与数据驻留、客服和 SLA，不能只把
Compose 暴露到互联网就宣称是托管产品。

## 8. 安装边界

- Compose 适合单机或小团队；高可用部署应使用托管 PostgreSQL、多个 Control Plane 副本和外部 Ingress；
- PostgreSQL 18 官方镜像的数据卷必须挂载到 `/var/lib/postgresql`，本仓库已按该路径配置；
- Docker 容器不是虚拟机，Remote Worker 处理不可信代码时仍应位于专用 Worker VM；
- Caddy 负责传输层 TLS，不替代组织 RBAC、IdP 条件访问、网络防火墙或备份。
