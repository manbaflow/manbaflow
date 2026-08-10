# 在集群里部署 Relay

Relay 部署进 k8s 而不是独立服务器，是为了让大脑能直连内网服务：GitLab、kg-agent、
summary-agent、multimodal-agent 和共享 PostgreSQL。这些都是 ClusterIP，从集群外访问需要
端口转发，不适合常驻服务。

Compose 那套（`deploy/install.sh`）保留给单机和本地开发，见 [安装手册](../../docs/INSTALLATION.md)。

## 1. 拉私有 registry 的 Secret

```bash
kubectl create namespace relay

kubectl -n relay create secret generic gitlab-registry \
  --from-file=.dockerconfigjson=<(kubectl -n readoow get secret gitlab-registry \
    -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d) \
  --type=kubernetes.io/dockerconfigjson
```

## 2. 建独立数据库和最小权限账号

用共享 PostgreSQL（`postgresql.postgresql.svc:5432`，18.4）。Relay 需要能在自己的库里创建和
修改表、索引与事务锁，但不应碰其他产品的库：

```bash
kubectl -n postgresql exec -it deploy/postgresql -- psql -U postgres <<'SQL'
CREATE ROLE relay LOGIN PASSWORD '换成随机口令';
CREATE DATABASE relay OWNER relay;
SQL
```

口令用 `openssl rand -hex 32` 生成，不要复用其他服务的。

## 3. 写入连接串 Secret

连接串里的口令必须做 URL 编码。文件里只放一行，不要有结尾换行以外的内容：

```bash
kubectl -n relay create secret generic relay-database-url \
  --from-literal=relay-database-url='postgresql://relay:已编码口令@postgresql.postgresql.svc:5432/relay'
```

Secret 的 key 必须是 `relay-database-url`——它会被挂载成
`/run/secrets/relay-database-url`，与 `RELAY_DATABASE_URL_FILE` 对应。

> 集群内是明文连接。如果之后把 PostgreSQL 换成 RDS 或跨网络实例，连接串要加
> `?sslmode=require`，见 [安装手册第 4 节](../../docs/INSTALLATION.md)。

## 3.5 飞书登录（可选，但这是零侵入入口）

**必须复用 GitLab 现在用的那个飞书应用**，否则同一个人在两边拿到的 `open_id` 不一致，
Flow 里的提交作者和任务负责人就对不上。当前 GitLab 用的是：

```bash
kubectl exec -n gitlab deploy/gitlab -c gitlab -- gitlab-rails runner \
  'p = Gitlab.config.omniauth.providers.first; puts p["app_id"]'
```

先在飞书应用后台把 `https://relay.edumind.ai/auth/feishu/callback` 加进重定向 URL，然后：

```bash
kubectl -n relay create secret generic relay-feishu \
  --from-literal=RELAY_FEISHU_APP_ID='cli_xxxxxxxxxxxxxxxx' \
  --from-literal=RELAY_FEISHU_APP_SECRET='应用凭证' \
  --from-literal=RELAY_FEISHU_REDIRECT_URL='https://relay.edumind.ai/auth/feishu/callback' \
  --from-literal=RELAY_FEISHU_ALLOWED_TENANT_KEYS='本企业 tenant_key'
```

`RELAY_FEISHU_ALLOWED_TENANT_KEYS` 不能省。**任何飞书用户——包括其他公司的——都能走完
整个 OAuth 流程**，`tenant_key` 是唯一能区分本企业员工的字段。漏配等于对全网开放，所以
适配器在缺失时直接拒绝启动。

不知道自己企业的 `tenant_key`：先用一个已知同事登录一次，失败日志里会带上被拒的 key；
或者调一次任意飞书服务端 API，响应里都有。

不建 `relay-feishu` 这个 Secret 也能正常部署，只是首页没有飞书登录入口。

## 4. 部署

```bash
kubectl apply -f deploy/k8s/relay.yaml
kubectl -n relay rollout status deploy/relay
```

`relay-setup` Job 会幂等地建组织、首个团队和管理员，并**只打印一次**登录 Token：

```bash
kubectl -n relay logs job/relay-setup
```

立刻把 Token 存进密码管理器。丢了就用 `relay setup --rotate-token` 重签，不要去 Ledger 里找——
那里只有摘要。

## 5. 加一条外部路由

集群里没有装 IngressClass，域名统一由 `platform-nginx` 转发（配置在 `platform/deploy` 仓库）。
按现有 `<服务>.edumind.ai` 的模式加一条：

```
relay.edumind.ai  →  relay.relay.svc:80
```

TLS 在 nginx 终止，所以容器里跑的是明文 HTTP（`--allow-insecure-public-http`）。**不要**把
Service 改成 LoadBalancer 直接暴露 7777。

路由生效后访问 `https://relay.edumind.ai/console`。

## 6. 验证内网连通性

大脑要接的服务都在别的命名空间，部署完先确认真的通：

```bash
kubectl -n relay exec deploy/relay -- sh -c '
  curl -sf -o /dev/null -w "gitlab        %{http_code}\n" http://gitlab.gitlab.svc/
  for svc in kg-agent:50065 summary-agent:50063 multimodal-agent:50064; do
    name=${svc%%:*}; port=${svc##*:}
    nc -z ${name}.agent.svc ${port} && echo "${name}    reachable"
  done
'
```

这几个 agent 服务是 gRPC，用 `nc` 探端口即可；真正的调用契约在
[platform/buf](https://gitlab.edumind.ai/platform/buf) 的 `proto/agent/`。

## 7. 升级

CI 把 `main` 分支推成 `registry.edumind.ai/platform/relay/main:latest` 和带版本号的 tag。
生产环境应该钉版本而不是追 `latest`：

```bash
kubectl -n relay set image deploy/relay relay=registry.edumind.ai/platform/relay/main:1.0.42
kubectl -n relay rollout status deploy/relay
```

回滚用 `kubectl -n relay rollout undo deploy/relay`。数据库迁移是向前兼容的，但跨多个版本
回滚前先确认 Ledger schema 版本。

## 备份

数据全在共享 PostgreSQL 里，所以走数据库侧的快照/PITR，**不要**用 `deploy/manage.sh backup`
（那是给内置数据库用的，外部模式会直接拒绝执行）。
