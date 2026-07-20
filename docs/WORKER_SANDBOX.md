# Remote Worker 容器沙箱

Remote Worker 默认通过 Docker 启动 Claude Code 或 Codex。Git worktree 提供事务性变更边界，容器提供
进程、文件系统、网络和资源边界；两层缺一不可。Control Plane 本身不执行模型，也不会接触员工仓库。

## 1. 构建不可变 Runtime

仓库提供一个最小 Runtime，固定安装 Codex CLI `0.144.5` 和 Claude Code `2.1.208`：

```bash
docker build \
  -f docker/agent-runtime/Dockerfile \
  -t manbaflow-agent-runtime:0.1.0 \
  .
```

Worker 起飞前执行本地 `docker image inspect`，把 tag 解析成 `sha256:` Image ID，随后用这个不可变 ID
运行且设置 `--pull=never`。一次 Worker 进程存活期间，即使同名 tag 被替换，已解析航班也不会漂移。
生产环境应在内部 Registry 构建、扫描并签名自己的 Runtime；不同语言工具链通过自定义
`--sandbox-image` 提供，不要在航班中临时下载安装。

## 2. 默认边界

每个航班使用独立容器，并固定启用：

- `--read-only`、`--cap-drop=ALL`、`no-new-privileges` 和非 root `UID:GID`；
- PID、内存、CPU 与 `/tmp` tmpfs 上限；
- `--network none`，不发布端口，不挂载 Docker socket 或宿主机 Home；
- 只挂载本次 worktree 与黑匣子输出目录；`plan` 的 worktree 只读，`execute` 才可写；
- 容器超时或 Worker 被取消时强制回收；正常退出由 `--rm` 回收；
- 只转发 `--sandbox-env` 明确列出的变量，禁止 Control Plane Token、Docker 凭据、`HOME` 和 `PATH`。

Docker 沙箱信息会随 Remote Flight Report 进入 append-only Ledger，包括 Image ID、网络、UID、资源上限
和转发的环境变量名称。值本身不会写入 Ledger。服务端拒绝 root、可写根目录、危险变量和越界资源声明。

## 3. 运行航班

默认断网适合本地模型或已经具备离线认证与模型访问代理的 Runtime：

```bash
export MAMBA_SERVER='https://mamba.example.com'
export MAMBA_TOKEN='mmb_agent_...'

mamba worker once \
  --mode execute \
  --executor codex \
  --workspace /path/to/repository
```

Claude Code 和 Codex 的云端模型需要网络及各自凭据。只有经过风险评审后才显式开启 Docker bridge，并
按名称注入最小凭据：

```bash
export OPENAI_API_KEY='...'

mamba worker once \
  --mode execute \
  --executor codex \
  --workspace /path/to/repository \
  --sandbox-network bridge \
  --sandbox-env OPENAI_API_KEY
```

`bridge` 本身不是域名级出站策略。生产节点应在主机、防火墙或透明代理层限制模型 API、Git 与依赖镜像
目的地，并禁止访问云元数据地址和内网控制面。需要长效刷新凭据时，由工作站 Secret Broker 在起飞前
提供短效 Access Token；不要向容器注入 `MAMBA_TOKEN`。

常用资源参数：

```text
--sandbox-cpus-millis 2000
--sandbox-memory-mb 4096
--sandbox-pids 256
--sandbox-tmpfs-mb 512
--sandbox-user 1000:1000
```

自定义 `--executable` 在容器模式下表示 Runtime 镜像内的命令，而不是宿主机路径。

## 4. 本地兼容模式

没有 Docker 的开发机可以显式使用：

```bash
mamba worker once --sandbox process --workspace /path/to/repository
```

`process` 仍保留 Git worktree、Lease、Fuel 和黑匣子，但不是安全边界；Dashboard 会明确显示
`process/host`。生产 Worker 不应使用该模式。Docker 容器也不是虚拟机，面对主动恶意代码时仍应运行在
专用、及时打补丁、优先采用 rootless Docker 的 Worker VM 上。

## 5. 验收检查

升级 Runtime 或 Worker 后至少验证：

1. `plan` 航班不能修改 worktree；
2. 默认网络无法访问外部地址；
3. 容器内 UID 非 0，根文件系统不可写，Docker socket 不存在；
4. 超时后 `docker ps -a --filter label=io.manbaflow.flight` 没有残留；
5. Dashboard 和黑匣子展示正确的 Image ID、资源上限及网络模式；
6. `changes.patch` 通过 `git apply --check` 后才由 Human 接纳。
