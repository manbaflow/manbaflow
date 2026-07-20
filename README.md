# MambaFlow

> 牢大带队，一人一城。Man, what can I say? 需求落地，Mamba Out。

MambaFlow 是一个使用 **Rust** 构建的企业级 **Human-Agent Flow** 平台。管理者只需要提出
业务需求，系统就能生成 PRD、拆解任务、匹配团队/员工/Agent、估算工期、派发工作，并持续追踪
Todo、阻塞、证据与交付结果，直到通过验收。

这里的“人事管理”不是工资、考勤或招聘系统，而是对企业中 **人类员工、团队和 AI Agent 的工作
关系进行统一编排**。人和 Agent 都是组织成员，都有身份、能力、权限、收件箱、任务和审计记录。

> [!IMPORTANT]
> MambaFlow 目前处于 **v0 / Preflight**。仓库已经包含可运行的 Ratatui 塔台、`mamba` 自动化命令、
> SQLite Flow Ledger、Remote Worker、隔离 Git worktree，以及本地或 Claude Code / Codex PRD 规划器和
> 执行终端适配器，以及事件化 Tenant/Role 权限、FlightManifest、Fuel、资源租约和监督树恢复。它还没有
> 生产级容器沙箱或企业 SSO/SCIM；一个数据目录仍只承载一个活动 Tenant。请先在测试仓库
> 和非敏感数据上使用。

## 一句话定位

> **把管理者的需求转化为跨团队、员工和个人 Agent 的可执行 Flow，自动完成规划、匹配、估时、
> 派发、协作、追踪、恢复与验收。**

MambaFlow 不是聊天机器人外壳，也不是只在一个对话里拉起几个 sub-agents。它管理的是一条可能
跨越多个用户、多个 Agent、多个系统和数天时间的持久化工作流程。

## Flow 到底是什么

`Flow` 不是 Agent 内部的一串 Tool Calls，而是一个企业任务从提出到交付的完整组织过程：

```text
需求提出
   │
   ▼
PRD 生成与审批
   │
   ▼
任务拆解与依赖分析
   │
   ▼
团队 / 员工 / Agent 匹配
   │
   ▼
工期、成本与风险评估
   │
   ▼
WorkRequest 派发
   │
   ▼
人类补充信息并批准执行
   │
   ▼
Agent Flight / Human Task 执行
   │
   ▼
Todo 追踪、阻塞升级与动态重排
   │
   ▼
范围变更预演、负责人批准与追加派发
   │
   ▼
证据验证、人工验收与 Flow 关闭
```

Flow 中的每个节点都有明确的负责人、权限、输入、输出、期限、验收条件和可见范围。系统可以自动
推进，但不能绕过组织权限或需要人工确认的 Gate。

## 一个完整例子

管理者在周一早上提出：

> “这周完成一个 LLM Gateway，支持多 Provider、限流、重试和基础观测。”

MambaFlow 创建 `FlowRun F-24`，然后执行以下流程。

### 1. 生成 PRD

需求 Agent 根据组织模板生成 PRD：

```text
目标
- 为业务提供统一的 LLM 调用入口
- 支持 OpenAI-compatible Provider
- 提供超时、重试、限流和请求日志

非目标
- 第一版不做复杂计费
- 第一版不提供跨地域容灾

验收标准
- 至少接入 2 个 Provider
- 关键路径具有单元测试和集成测试
- Provider 故障时能够按策略切换
- Dashboard 能查看成功率和 P95 延迟
```

管理者可以修改、补充或批准 PRD。PRD 未批准前，系统不会进入正式派发阶段。

### 2. 自动匹配

组织调度 Agent 查询组织图谱、代码所有权、技能、权限、在途工作和日历，给出建议：

```text
推荐团队：Platform / AI Infra
推荐负责人：工程师 A
协作成员：工程师 B
个人 Agent：Agent A

原因
- 工程师 A 负责现有 API Gateway
- Agent A 已被授权读取 gateway 仓库和团队规范
- 工程师 B 熟悉限流与可观测性
- 本周预计可用产能：A 60%，B 30%

预计工期
- P50：3.2 个工作日
- P80：4.8 个工作日
- 置信度：中等
- 主要风险：Provider API 差异、集成测试环境
```

管理者批准后，系统向团队、工程师和对应的个人 Agent 发送带权限链的 `WorkRequest`。

### 3. 个人 Agent 生成技术计划

Agent A 只能在工程师 A 授权的范围内读取：

- 该工程师负责的 Git 仓库；
- 现有架构、历史 PR 和 Issue；
- 团队开发规范、测试命令和交付流程；
- 与当前 Flow 相关的 PRD 和 Artifact。

Agent A 将需求拆成具体 Todo：

```text
T-01  定义 Gateway API 与错误模型       owner=A        estimate=4h
T-02  实现 Provider Adapter 接口         owner=A+AgentA estimate=6h
T-03  接入 Provider 1 / Provider 2       owner=AgentA   estimate=8h
T-04  实现限流、超时和重试策略           owner=B+AgentB estimate=8h
T-05  添加请求日志与指标                 owner=A+AgentA estimate=6h
T-06  集成测试与故障切换验证             owner=A+B      estimate=8h
T-07  PR Review 与发布说明               owner=A        estimate=4h
```

工程师在自己的 Inbox 中补充技术约束、调整任务、接受或拒绝工期，并批准 Agent 开始执行。

### 4. 执行与追踪

批准后，Agent A 启动 Coding Flight，在隔离工作区中修改代码、运行测试并创建 PR。Tracker Agent
持续消费 Git、CI、文档、人工反馈和 Flight 事件，更新 Todo 状态。

管理员看到的组织级时间线类似：

```text
09:00  Demand submitted by Manager
09:02  PRD draft generated
09:15  PRD approved by Manager
09:16  Routed to Platform / AI Infra
09:16  Assigned to Engineer A + Agent A; Captain H-08 took possession
09:18  Repository access granted by Engineer A
09:24  Technical plan generated
09:31  Engineer A requested API compatibility changes
09:36  Plan updated; ETA changed from 3.2d to 4.0d
10:02  Coding Flight H-08 took off; pilot=佐巴扬
11:40  Unit tests passed: 128 / 128
12:05  Flight H-24 crashed: upstream timeout; blackbox saved
12:08  Flight H-24 refly from W-07: 孩子们，我回来了
14:20  Pull request opened
16:10  Review requested changes
17:05  Changes pushed; CI passed
17:20  Delivery waiting for Manager acceptance
```

### 5. 验收与关闭

代码、测试结果、PR、文档和监控截图作为 Evidence 进入 Landing Validator。满足 PRD 的验收条件后，
系统请求管理者确认交付；确认后写入 `flow.closed`，CLI 可以显示：

```text
[MAMBA_OUT] Flow F-24 completed. All assignments settled.
```

这条从需求到交付、跨管理者、工程师和 Agent 的持久化因果链，就是 MambaFlow 中的 Flow。

## 产品架构

```text
┌──────────────────────────────── Enterprise Surfaces ────────────────────────────────┐
│ Web Console · mamba CLI · Feishu · Slack · Teams · Email · API                    │
└───────────────────────────────────────┬─────────────────────────────────────────────┘
                                        │ demand / command / approval
┌───────────────────────────────────────▼─────────────────────────────────────────────┐
│                           Organization Control Plane                                │
│                                                                                     │
│ Demand Intake    PRD Planner     Matcher        Scheduler       Todo Tracker         │
│ Approval Gates   Dispatcher      Escalation     Policy Engine   Flow Ledger          │
└───────────────────────┬───────────────────────┬───────────────────────┬───────────────┘
                        │                       │                       │
              ┌─────────▼────────┐    ┌─────────▼────────┐    ┌─────────▼────────┐
              │ Human / Team     │    │ Personal Agent  │    │ Org Agent        │
              │ Inbox            │    │ 一人一城         │    │ shared service   │
              └─────────┬────────┘    └─────────┬────────┘    └─────────┬────────┘
                        │                       │                       │
┌───────────────────────▼───────────────────────▼───────────────────────▼───────────────┐
│                               Execution Plane                                       │
│ Human Task · Agent Flight · Cabin · Tool · Skill · Artifact · Validator             │
└───────────────────────────────────────┬─────────────────────────────────────────────┘
                                        │ append
┌───────────────────────────────────────▼─────────────────────────────────────────────┐
│ Flow Ledger · Agent Black Box · Audit Log · Metrics · Cost · Evidence               │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

Control Plane 管组织和工作，Execution Plane 管一次具体执行。Tower 不亲自调用模型完成业务任务，
而是监督 Flow、路由 WorkRequest、管理权限、资源、预算和恢复策略。

### 代码模块

```text
src/
├── core/          # 领域对象、事件、组织状态、工作日历、错误与 ID
├── application/   # MambaApp、规划、匹配、排期、Tracker 与管理看板
│   └── app/       # 按日历、凭证、消息、通知、交互等用例拆分的应用服务
├── adapters/      # SQLite、终端执行器、GitLab、Webhook、通知与 worktree
├── interfaces/    # HTTP Server、Ratatui、Remote Worker 与 Showcase
├── bin/mamba/     # mamba CLI 入口
└── lib.rs         # 分层模块入口与向后兼容的公开导出
```

依赖方向从 Interface 进入 Application，由 Application 使用 Core 并通过 Adapter 访问外部系统。
`lib.rs` 继续导出 `manbaflow::domain`、`manbaflow::server` 等稳定路径，物理目录不会泄漏给调用方。

## 核心模型

| 实体 | 说明 |
| --- | --- |
| `Tenant` | 企业租户及其安全边界 |
| `OrgUnit` | 部门、团队、项目组或临时小队 |
| `Principal` | 可以接收任务的 Human、Team 或 Agent |
| `PersonalAgent` | 归属于某个员工的 Agent，继承有限授权而非员工全部权限 |
| `Capability` | 编程、研究、表格、演示文稿、审批等可匹配能力 |
| `Demand` | 管理者提出的原始需求 |
| `FlowDefinition` | 可复用的组织流程模板 |
| `FlowRun` | 一次需求从提出到关闭的持久化实例 |
| `WorkPlan` | PRD、任务 DAG、依赖、工期和风险 |
| `Task` | 可独立分配、跟踪和验收的工作单元 |
| `Assignment` | Principal 与 Task 之间的责任关系 |
| `WorkRequest` | 跨用户或 Agent 发送的带权限命令信封 |
| `ApprovalGate` | 需要指定 Human 或 Policy 批准的流程节点 |
| `Artifact` | PRD、代码、文档、表格、报告、PR 或其他交付物 |
| `Evidence` | 证明 Task 满足验收条件的可验证材料 |
| `Flight` | Agent 执行一个 Task 时产生的隔离运行实例 |
| `FlowLedger` | 组织级、追加式的完整流程事件源 |
| `BlackBox` | 单个 Agent Flight 的详细执行记录与航点 |

平台管理员与业务管理者是不同角色：`TenantAdmin` 管理租户、身份源、策略和系统配置；`Manager`
只能在自己的组织与授权范围内提出需求、分配工作和查看 Flow；`Requester` 可以追踪自己发起的
需求。任何角色都不会因为名称里有“管理员”就自动获得员工私人上下文或 Secret 的读取权限。

## 一人一城

每个员工经营自己的主场：Personal Agent、记忆、凭据、工作区和授权都留在本人边界内。管理者和
队友可以传来 `WorkRequest`，却不能直接接管他的 Agent。负责带队的 Captain，也就是牢大，拿到
需求后负责拆解和传球；边界清晰的任务可以让 Agent 单打，需要经验判断时就由员工与 Personal
Agent 打挡拆。谁接到 Assignment，谁才拥有这段工作的球权。

Captain 不需要包办所有执行。一份结构化 `ASSIST`、一条可验证 Evidence，往往比把完整上下文
传遍全队更像一次好助攻。Flow 可以在洛杉矶凌晨四点被 Scheduler 唤醒，但没有权限、Fuel、Lease
和终止条件，任何直升机都不能起飞。

## WorkRequest：跨用户协作协议

管理者不能通过一句 Prompt 直接操纵其他员工的 Agent。所有跨身份命令必须形成可审计的信封：

```yaml
work_request_id: WR-24
flow_id: F-24
issuer: user://manager-a
acting_as: role://engineering-manager
target: agent://engineer-a/personal-agent
authority_grant: grant://platform-team/task-assignment

objective: 为 LLM Gateway 生成技术方案并拆解 Todo
context_refs:
  - artifact://F-24/prd/v3
deadline: 2026-07-18T18:00:00+08:00
budget:
  max_tokens: 120000
  max_cost: 5.00USD

visibility:
  flow_events: manager_and_assignees
  raw_tool_output: assignee_only
requires_acceptance: true
```

接收方可以接受、拒绝、协商期限、要求补充信息或请求更高权限。Agent 只能使用信封中明确授予的
Authority，不得把权限继续传递给无权的第三方。

越权请求会被 Policy Engine 当场肘出 Flow，并以 `policy.denied` 留下可审计原因。

## 自动匹配

Matcher 先应用硬约束，再对候选 Principal 排序。

硬约束包括：

- 是否属于允许接单的组织范围；
- 是否拥有任务要求的 Capability；
- 是否能够访问必要数据和系统；
- 是否存在利益冲突或隔离要求；
- 是否满足地域、时区、合规和审批要求。

排序信号可以包括：

- 领域技能与历史所有权；
- 当前负载和日历可用性；
- 任务上下文切换成本；
- Agent 的模型、工具、预算和成功率；
- 团队协作关系和关键依赖；
- 费用、截止时间和风险偏好。

系统必须展示匹配理由和备选方案。对 Human 的分配默认需要接受或管理者确认，不能把模型推荐当成
不可申诉的人事决定，也不能使用敏感属性进行隐性绩效排名。

## 工期估算

MambaFlow 不允许 LLM 随口给出一个精确日期。工期由多个可解释部分组成：

```text
Lead Time = Queue Time
          + Execution Effort / Available Capacity
          + Dependency Time
          + Approval Time
          + Risk Buffer
```

每份 Estimate 至少包含：

- 工作量与交付周期的区分；
- P50 和 P80 时间范围；
- 置信度和使用的数据来源；
- 关键路径与并行机会；
- 人类、Agent 和外部系统的可用产能；
- 假设、风险和可能导致重估的条件。

冷启动阶段使用组织模板、任务复杂度和人工校正。积累数据后，可以使用同类任务历史、团队吞吐和
Agent 运行指标校准，但不得把未经解释的估算用于自动惩罚员工。

任务需要进入加时赛时，系统必须生成新的 Estimate 并重新审批，不能悄悄移动 Deadline。

## Todo 不是勾选框

Todo 必须是可追踪、可验收的任务 DAG，而不是 Agent 自己声称“已完成 80%”：

```yaml
task_id: T-04
flow_id: F-24
title: 实现限流、超时和重试策略
owner: user://engineer-b
copilot: agent://engineer-b/personal-agent
depends_on: [T-01, T-02]

acceptance_contract:
  - unit_tests_pass
  - retry_policy_documented
  - p95_latency_not_regressed

estimate:
  effort: 8h
  p50_finish: 2026-07-16T15:00:00+08:00
  p80_finish: 2026-07-17T12:00:00+08:00

status: in_progress
last_heartbeat: 2026-07-15T11:30:00+08:00
blocker: null
evidence: []
```

Tracker Agent 通过事件和 Evidence 更新状态：

- `task.proposed`
- `task.assigned`
- `task.accepted`
- `task.started`
- `task.heartbeat`
- `task.blocked`
- `task.submitted`
- `task.review_requested`
- `task.completed`
- `task.cancelled`
- `tracking.attention_raised`
- `tracking.attention_resolved`
- `tracking.escalation_raised`
- `tracking.escalation_acknowledged`
- `tracking.escalation_resolved`

当前 v0 Tracker 会识别派发后未接单、执行中长期没有 Heartbeat、显式阻塞、提交后等待验收和超过
P80 五类风险。每个活动 Attention 都有稳定 ID；重复扫描不会刷出重复提醒，条件消失时会追加 resolved
事件。Critical Attention 会立即升级给 Demand Requester；Warning 持续超过策略窗口后升级。Requester
必须是已注册 Human，只有接收人能确认呼叫；确认表示已经接手，不会提前解除 Attention。自动重新估时、
外部通知和重新匹配仍属于后续策略层。

即使最后一个 Task 在 Deadline 前完成绝杀，也必须带着 Evidence 经过 Validator 才能计入比分。

## Human in the Loop

Human 不是只在最后按一次“批准”，而是 Flow 中的一等节点：

员工与 Personal Agent 的默认协作就是一组挡拆：Human 保留方向、判断和授权，Agent 承担检索、
执行与整理。

- 管理者审核 PRD、匹配结果、工期和高风险变更；
- 员工接受、拒绝或协商 Assignment；
- 员工为 Personal Agent 补充上下文并授权执行；
- 敏感 Tool、数据访问和外部发送需要指定人员批准；
- Agent 遇到模糊需求或阻塞时创建 Clarification Task；
- 负责人审核中间交付物和最终 Evidence；
- 管理者可以暂停、改派、缩减范围或终止整个 Flow。

每个 Gate 可以配置为人工必审、策略自动通过、超时升级或多人会签，但所有决定都进入 Flow Ledger。

## 全流程可见

企业需要全流程可观测，但“可观测”不等于所有人默认读取所有秘密。MambaFlow 区分两层记录：

### Flow Ledger

组织级事件源，记录需求、负责人、匹配理由、审批、状态、工期变化、风险、成本、Evidence 和交付物。
管理者在授权范围内可以查看完整流程，并从任意事件还原 Flow 状态。

### Agent Black Box

Flight 级详细记录，包括模型请求、Tool Call、命令输出、工作区变更、Token、费用、航点和坠机原因。
原始内容可能包含源码、员工私人上下文或 Secret，因此按 RBAC/ABAC、数据分级和脱敏策略开放。
任何人查看受限黑匣子本身也必须留下审计记录。

## Agent Execution Plane

当 Task 被分配给 Agent 时，执行层沿用 crash-first Flight 模型：

```text
GROUNDED
    │ WorkRequest accepted + FlightManifest filed
    ▼
PREFLIGHT ──check failed────────────▶ CRASHED
    │ passed
    ▼
READY ──takeoff─────────────────────▶ AIRBORNE
                                        │
                     ┌──────────────────┼──────────────────┐
                     ▼                  ▼                  ▼
                  LANDED             CRASHED            ABORTED
```

每个 Flight 拥有独立上下文、Tool 权限、Cabin、Fuel、Landing Contract 和 Black Box。坠机后，
Tower 可以从 Waypoint 复飞、换 Agent、改派给 Human、缩小任务或将阻塞升级给管理者。

默认 24 个模型回合、81 次 Tool Call 可以留作初始预算，但曼巴精神不是无限重试。每次复飞都要
带上黑匣子里的 Evidence；主 Provider 不可用时，Pilot 可以后仰切到备用路线。

## Capability Packs

MambaFlow Core 不把某个工作领域写死。企业通过能力包让 Human 和 Agent 参与不同类型的 Flow。

### Coding

- Git 仓库、Issue、PR 和 Code Review；
- 隔离 worktree / container；
- 代码搜索、编辑、测试、构建和 CI；
- 架构规范、代码所有权和发布流程；
- 代码 Artifact、Diff 和测试 Evidence。

### Office

- 文档生成、修改、评论和审批；
- 表格读取、公式、分析与可视化；
- 演示文稿生成与品牌模板；
- 邮件、日历、会议纪要和待办；
- Microsoft 365、Google Workspace、飞书等连接器。

### Research and Operations

- Web 搜索、内部知识库和带引用报告；
- 工单、CRM、客服和运营流程；
- 数据查询、Dashboard 和定期汇报；
- 企业自定义 MCP、Tool、Skill 和 Validator。

能力包声明 Capability、权限、输入输出 Schema、成本模型和验收方式，Matcher 才能把任务合理派给
对应的团队、员工或 Agent。

## 快速开始

需要 Rust stable。构建后会得到一个 `mamba` 二进制：

```bash
cargo build --release
./target/release/mamba --help
```

最快的体验方式是直接打开一座空塔台：

```bash
rm -rf .mambaflow-showcase
./target/release/mamba --data-dir .mambaflow-showcase \
  tui --workspace .
```

进入 Ratatui 后点击底栏的 `SHOWCASE`。塔台会在界面内注册牢大、佐巴扬及各自的
Claude Code / Codex 副驾，创建三条可回放的真实 Flow，并自动聚焦风险最高的 LLM Gateway。Showcase
包含正在执行、阻塞、等待 Human 验收、已完成和远程 Flight Lease 等状态；每一步都会进入同一份
append-only Flow Ledger，不是一张静态看板。完整的五分钟展示顺序见
[TUI Showcase 演示脚本](docs/SHOWCASE.md)。

不带子命令时，`mamba` 默认进入全屏 Ratatui 塔台。也可以显式指定当前 Human 和工作区：

```bash
./target/release/mamba tui --as 牢大 --workspace .
```

塔台提供五个实时视图：组织总览、Flow/PRD/任务 DAG、个人 Inbox、团队阵容和 append-only 时间线。
写操作直接调用同一套领域 API，不会在界面里维护另一份状态。TUI 以鼠标为主要操作方式：点击顶部
标签切换视图，点击表格行选择 Flow 或任务，点击当前球权轮换 Human，滚轮移动当前列表，所有业务动作
都通过底部自动换行的操作带完成。新需求弹窗中的规划器、换防候选人、确认和取消也都可以直接点击。
模型规划在后台执行，Flight Deck 会显示 `PLANNING`，完成后从 Flow Ledger 重建状态并
自动定位到新 Flow。TUI 启动时启用 Crossterm 鼠标捕获，正常退出或发生错误时都会关闭捕获并恢复
终端，因此不会把 shell 留在无法选择文本的状态。

建议截图时将终端设为至少 120×36、字体 15–16px，进入总览或 Flow 页后隐藏其他窗口。macOS 可以按
`Command + Shift + 4`，再按空格选择终端窗口。

原来的命令行接口仍然保留，用于脚本、CI 和排障。例如：

```bash
./target/release/mamba org chart

./target/release/mamba demand create \
  "这周完成一个 LLM Gateway" \
  --requester 牢大 \
  --planner local
```

最后一条命令会生成 PRD、Task DAG、解释性匹配结果、P50/P80 工期和关键路径。记下输出中的动态
`FLOW-...` 与 `TSK-...`，然后让管理者批准计划并把球传出去：

```bash
./target/release/mamba flow show FLOW-xxxxxxxx
./target/release/mamba flow approve FLOW-xxxxxxxx --by 牢大
./target/release/mamba inbox --for 佐巴扬

./target/release/mamba task accept TSK-xxxxxxxx --by 佐巴扬
./target/release/mamba task negotiate TSK-xxxxxxxx --by 佐巴扬 --hours 12
./target/release/mamba task start TSK-xxxxxxxx --by 佐巴扬
```

执行中出现新增范围时，Requester 可以先生成一份 append-only 影响预览。预览不会修改正式任务 DAG；
批准时如果原任务状态或排期已经变化，系统会要求重新生成，避免按过期工期盲目派单：

```bash
./target/release/mamba flow change-propose FLOW-xxxxxxxx \
  "增加客户迁移检查清单" --by 牢大
./target/release/mamba flow changes FLOW-xxxxxxxx --as 牢大
./target/release/mamba flow change-approve CHG-xxxxxxxx --by 牢大

# 也可以保留原因后驳回
./target/release/mamba flow change-reject CHG-xxxxxxxx \
  --by 牢大 --reason "本次发布不需要"
```

影响预览同时给出 `scope P80` 和 `net`：前者把当前进度在同一时间点重排后，只计算新增范围带来的边际
工期；后者比较正式 Flow 现有 P80，包含任务提前或延误造成的基线变化。这样“新增任务但净工期缩短”
不会被误读成新任务让项目变快。

`negotiate` 不只修改当前任务数字：Scheduler 会以当前时刻、Owner 产能和依赖关系重新计算所有未完成任务
的窗口、Flow P50/P80 与关键路径。需求发起人也可以先查看满足硬能力约束的候选人，再执行换防：

```bash
./target/release/mamba task reassignment-candidates TSK-xxxxxxxx --by 牢大
./target/release/mamba task reassign TSK-xxxxxxxx \
  --by 牢大 --to "工程师 B" \
  --reason "工程师 A 转入线上事故响应"
```

改派会撤销旧 Assignment 状态并向新 Owner 发送 WorkRequest；新 Owner 必须重新接单。存在已领取或仍可
领取的 Remote Flight Lease 时禁止换防，必须先结束或撤销航班，避免两架直升机同时接管同一任务。

人员工作日历把“有 80% 产能”和“什么时候真的能工作”分开。未配置成员保持 24×7，配置后 Matcher 会
考虑下一可用时间，Scheduler 会跳过夜间、周末和请假，并立即重排该成员参与的 Active Flow：

```bash
./target/release/mamba principal calendar set \
  --for 佐巴扬 --utc-offset +08:00 \
  --days mon,tue,wed,thu,fri --start 09:00 --end 18:00

./target/release/mamba principal calendar time-off-add \
  --for 佐巴扬 \
  --from 2026-07-20T09:00:00+08:00 \
  --until 2026-07-22T09:00:00+08:00 \
  --reason "客户现场支持"

./target/release/mamba principal calendar show --for 佐巴扬
./target/release/mamba principal calendar time-off-cancel \
  --for 佐巴扬 OFF-xxxxxxxx
```

第一版使用显式固定 UTC 偏移，不猜测夏令时和地区节假日；后续 Calendar Connector 会把真实忙碌区间
同步为同一种 append-only 事件。Ratatui 的阵容页直接显示每位成员的工作段和有效请假数量。

外部消息通过可靠 Notification Outbox 发往飞书、Slack、Teams、Office Bridge 或内部自动化平台。业务
事件和通知入队在同一个 SQLite 事务中完成，密钥只从运行环境读取，不写入 Ledger：

```bash
export MAMBA_OPS_WEBHOOK_SECRET='replace-with-a-random-secret'

./target/release/mamba notification endpoint-add \
  --name operations \
  --url https://bridge.example.com/mambaflow \
  --secret-env MAMBA_OPS_WEBHOOK_SECRET \
  --events work_request.sent,task.blocked,task.submitted,tracking.escalation_raised,flow.completed

./target/release/mamba notification deliveries
./target/release/mamba notification dispatch --force
```

`mamba serve` 默认每 15 秒投递一次，失败后指数退避；Endpoint 停用时未发送记录会转为 `cancelled`，
不会丢失原始 payload 和审计历史。单独运行 Ratatui 时也会在后台非阻塞排空 Outbox，管理员可点击总览
底部的 `投递通知` 立即重试。请求使用稳定 Delivery ID、时间戳和 HMAC-SHA256 签名，完整接收协议见
[Notification Connector](docs/NOTIFICATIONS.md)。

不需要额外部署 Bridge 也可以直接发送原生消息卡片。供应商 Webhook URL 只放在环境变量里；以下测试传球
同样经过 Outbox 并留下可检查的落地或坠机记录：

```bash
export MAMBA_FEISHU_WEBHOOK_URL='https://open.feishu.cn/open-apis/bot/v2/hook/...'
export MAMBA_FEISHU_SIGNING_SECRET='replace-with-the-bot-signing-secret'

./target/release/mamba notification connector-add \
  --provider feishu --name engineering-feishu \
  --url-env MAMBA_FEISHU_WEBHOOK_URL \
  --secret-env MAMBA_FEISHU_SIGNING_SECRET

./target/release/mamba notification test NEND-xxxxxxxx
```

Slack 使用 Block Kit，Teams 使用 Workflows Webhook 的 Adaptive Card；完整配置见上面的 Connector 文档。

Slack WorkRequest 和待确认消息可以直接接球或回执。外部用户必须先绑定 Human 身份，Slack 请求使用 App
Signing Secret 验证；飞书和 Teams App Bot 可以调用同一套 HMAC Interaction Bridge：

```bash
./target/release/mamba principal identity bind \
  --for 佐巴扬 --provider slack --external-user U0123456789

export MAMBA_SLACK_SIGNING_SECRET='replace-with-slack-signing-secret'
./target/release/mamba serve
```

动作事件和外部 Delivery 回执在同一事务中落盘，重试不会二次接单。完整协议见
[Human Interaction Gateway](docs/INTERACTIONS.md)。

管理者可以向团队、Human 或个人 Agent 发送关联 Flow/Task 的结构化指令。要求回执的消息会停留在接收方
Inbox，直到本人或个人 Agent 的 Human Owner 明确确认：

```bash
./target/release/mamba message send FLOW-xxxxxxxx \
  "确认生产 Secret 轮换边界并回传结论" \
  --task TSK-xxxxxxxx --by 牢大 \
  --to 佐巴扬 --to "Codex 副驾"

./target/release/mamba message inbox --for 佐巴扬
./target/release/mamba message ack MSG-xxxxxxxx --by 佐巴扬
./target/release/mamba message thread FLOW-xxxxxxxx --as 牢大
```

`command`、`question`、`update` 和 `decision` 都是 Flow Ledger 事件。Requester 可以把新成员拉进 Flow；
其他参与者只能向已经在 Flow 中的人或团队传球，避免普通任务成员任意扩大信息可见范围。

任务有未完成的前置依赖时不能开工。交付前必须附上 Evidence，最终 `complete` 只能由已注册的 Human
执行：

```bash
./target/release/mamba task evidence TSK-xxxxxxxx \
  --by 佐巴扬 --kind document --uri docs/design.md --summary "方案与验证记录"
./target/release/mamba task submit TSK-xxxxxxxx --by 佐巴扬
./target/release/mamba task complete TSK-xxxxxxxx --by 牢大
./target/release/mamba timeline FLOW-xxxxxxxx
```

所有命令都支持全局 `--json`，适合由脚本、后续 Web Console 或企业连接器消费。

## 远程 Control Plane

同事不需要共享 `.mambaflow/flow.db`。本机管理员可以为每个 Principal 签发独立 Token，再启动 HTTP
Control Plane；远程请求会以 Token 对应的真实身份调用同一套 Assignment 和状态门禁：

```bash
./target/release/mamba principal token issue \
  --for 佐巴扬 --label "workstation"

./target/release/mamba serve --bind 127.0.0.1:7777
```

Token 只显示一次。Ledger 只记录 credential ID、Principal 和签发/撤销事件；SQLite 凭据表只保存
SHA-256 摘要，不保存原始 Token。客户端使用标准 Bearer Header：

```bash
export MAMBA_TOKEN='mmb_...'

curl -H "Authorization: Bearer $MAMBA_TOKEN" \
  http://127.0.0.1:7777/api/v1/me
curl -H "Authorization: Bearer $MAMBA_TOKEN" \
  http://127.0.0.1:7777/api/v1/inbox
curl -X POST -H "Authorization: Bearer $MAMBA_TOKEN" \
  http://127.0.0.1:7777/api/v1/tasks/TSK-xxxxxxxx/accept
```

每个组织初始化时会同时建立 Tenant。首位 Human 获得 `tenant_admin`，后续 Human 默认是 `member`，
Agent 只能持有 `agent`。Tenant Admin 可以授予 `organization_admin`、`manager`、`auditor` 或额外的
`member` 角色；最后一个 Tenant Admin 不能被撤销：

```bash
mamba principal role list --for 佐巴扬 --by 牢大
mamba principal role grant --for 佐巴扬 --role manager --by 牢大
mamba principal role revoke ROLE-xxxxxxxx --by 牢大
```

角色授予、撤销和旧 Ledger 的权限迁移都会进入事件流。`manager` 可以创建 Demand 和读取管理看板，
`auditor` 只能读取管理与审计视图，普通 `member` 仍只能操作自己的 Assignment 和 Flow 对话。

当前 API 覆盖：

| Endpoint | 作用 |
| --- | --- |
| `GET /console` | 内嵌管理员 Web Console；数据操作仍需 Principal Bearer Token |
| `GET /health` | 无认证健康检查 |
| `GET /api/v1/me` | 查看 Token 对应 Principal |
| `GET /api/v1/organization` | 查看当前 Tenant 与 Organization |
| `GET/POST /api/v1/teams` | 查看团队，或由组织管理员创建团队 |
| `GET/POST /api/v1/principals` | 查看成员，或由组织管理员注册远程 Human/Agent |
| `GET/POST /api/v1/principals/:id/roles` | 查看或授予 Principal 的组织角色 |
| `POST /api/v1/roles/:id/revoke` | 撤销角色绑定；禁止撤销最后一个 Tenant Admin |
| `POST /api/v1/principals/:id/credentials` | 管理员签发只显示一次的 Principal Token |
| `POST /api/v1/credentials/:id/revoke` | 立即撤销 Bearer Token |
| `POST /api/v1/demands` | Manager 远程提交 Demand 并生成 Flow 草案 |
| `GET/PUT /api/v1/me/calendar` | 查看或更新本人工作日、固定 UTC 偏移和每日工作段 |
| `POST /api/v1/me/time-off` | 登记本人不可用区间并重排相关 Flow |
| `POST /api/v1/me/time-off/:id/cancel` | 取消本人不可用区间并恢复相关排期 |
| `GET /api/v1/dashboard` | Human 查看组织 Flow、风险、Action Queue 和航班聚合快照 |
| `GET /api/v1/inbox` | 查看本人 Assignment |
| `GET /api/v1/messages` | 查看本人收到的 Flow 指令与待确认回执 |
| `GET /api/v1/notifications/{endpoints,deliveries}` | Human 查看通知配置与 Outbox 健康度 |
| `POST /api/v1/notifications/dispatch` | Human 立即投递 Outbox 或强制重试失败记录 |
| `GET /api/v1/escalations` | 查看本人 Tower Calls |
| `POST /api/v1/flows/:id/approve` | Demand Requester 批准 Flow |
| `GET/POST /api/v1/flows/:id/changes` | 查看变更历史，或生成追加任务和工期影响预览 |
| `POST /api/v1/flow-changes/:id/{approve,reject}` | Requester 批准或驳回变更；批准前不改正式 DAG |
| `GET/POST /api/v1/flows/:id/messages` | 读取有权查看的对话或向团队、Human、Agent 传球 |
| `POST /api/v1/messages/:id/ack` | 接收人或 Personal Agent 的 Human Owner 确认回执 |
| `POST /api/v1/tasks/:id/{accept,start,heartbeat,block,evidence,submit,complete}` | 推进本人任务 |
| `POST /api/v1/tasks/:id/negotiate` | Assignment 成员协商工时并触发整条 Flow 重排 |
| `GET /api/v1/tasks/:id/reassignment-candidates` | Requester 查看满足能力和 Human 约束的换防候选人 |
| `POST /api/v1/tasks/:id/reassign` | Requester 改派 Owner/Copilot 并重新计算 Flow ETA |
| `POST /api/v1/tasks/:id/flight-leases` | Human 为自己的 Personal Agent 签发限时写租约 |
| `GET /api/v1/flight-leases` | Agent 查看可执行租约；Human 查看相关航班报告 |
| `POST /api/v1/flight-leases/:id/{claim,finish,revoke}` | 领取、结束或在起飞前撤销租约 |
| `GET /api/v1/flight-leases/:id/recovery-options` | 按失败类型和 Manifest 查看可用恢复动作 |
| `POST /api/v1/flight-leases/:id/recover` | Human 选择复飞、换执行器、缩小范围、转人工、停飞或分叉 |
| `POST /api/v1/escalations/:id/ack` | 接收人确认 Tower Call |
| `POST /api/v1/connectors/gitlab/webhook` | 接收经过独立签名校验的 GitLab 事件 |
| `POST /api/v1/connectors/slack/actions` | 验证 Slack 原始请求并以绑定 Human 身份执行允许动作 |
| `POST /api/v1/connectors/interactions` | 飞书、Teams 等 App Bridge 的 HMAC 身份动作入口 |

Flow 只能由 Demand Requester 批准和最终验收；即使另一个 Human 持有合法 Token，也不能操作不属于自己
的 Assignment。服务端默认每 30 秒运行 Tracker。Token 可以立即撤销：

```bash
./target/release/mamba principal token list --for 佐巴扬
./target/release/mamba principal token revoke CRED-xxxxxxxx
```

`mamba serve` 默认只监听 `127.0.0.1`。当前版本已有事件化组织 RBAC，但一个数据目录仍只承载一个
活动 Tenant，也还没有 TLS、OIDC SSO、SCIM 或 API 限流；跨机器部署必须放在 TLS 反向代理或可信
内网后面。签发 Token 的本地 CLI `admin` 操作者仍被视为引导期 Control Plane 管理员。

服务启动后可直接打开 `http://127.0.0.1:7777/console`。Console 使用 Human 自己的 Bearer Token 登录，
Token 只放在当前浏览器标签页的 `sessionStorage`；页面壳本身不包含组织数据。管理员可以查看组织指标、
Action Queue、Flow 进度、Manifest/Fuel/资源租约与坠机分类，也可以创建 Demand、批准 Flow、推进本人
有权处理的任务，并通过监督树对坠机航班做恢复决定。页面和静态资源编译进同一个 Rust 二进制，无需
Node 服务；HTTP 响应默认带 CSP、`nosniff` 与 `no-referrer`。生产环境仍必须在可信 TLS 入口之后部署。

## Remote Worker

Personal Agent 不需要与塔台位于同一台机器，也不需要把员工仓库挂载到服务器。先注册一个没有本地
Executor 的远程 Agent，并为它签发独立身份：

```bash
./target/release/mamba principal add \
  --name "工程师 A 的 Codex" --kind agent \
  --team Platform --owner "工程师 A" \
  --capabilities "rust,backend,llm-platform"

./target/release/mamba principal token issue \
  --for "工程师 A 的 Codex" --label "workstation"
```

在工程师自己的工作站上保留 Token、仓库和已经登录的 Claude Code/Codex。默认执行一次严格只读的
规划航班：

```bash
export MAMBA_SERVER='https://mamba.example.com'
export MAMBA_TOKEN='mmb_...'

mamba worker once \
  --executor codex \
  --workspace /path/to/llm-gateway
```

也可以保持轮询；Worker 每次只处理一项任务，不会并发修改多个工作区：

```bash
mamba worker run \
  --executor claude-code \
  --workspace /path/to/repository \
  --poll-seconds 30
```

Worker 使用 Token 对应 Principal 的 Inbox 和 Assignment 权限。它会接球、通过依赖门禁后把 Task 置为
In Progress、写入起降 Heartbeat，在本机运行严格只读的 `plan` 模式，再把结构化摘要作为
`agent_plan` Evidence 回传。成功任务不会重复规划；自动轮询也不会闯入已经 In Progress 的任务。
进程在起飞后中断时，可以用 `--task TSK-xxxxxxxx` 显式恢复。起飞前 Worker 还会读取关联 Flow/Task 的
显式 Command、Question、Update 和 Decision，把它们注入执行 PASS，并确认当前仍在等待 Agent 回执的
指令；复飞从完整 Flow thread 恢复这些约束，不依赖一段临时聊天上下文。

远程写入需要另一条明确的授权链。Task 接单且依赖完成后，Personal Agent 的 Human Owner 可以签发
一个最长 24 小时、绑定 Task、Agent 和执行终端的一次性 Flight Lease。单机模式可以直接使用 CLI：

```bash
mamba task authorize TSK-xxxxxxxx \
  --by "工程师 A" \
  --agent "工程师 A 的 Codex" \
  --executor codex \
  --ttl-seconds 3600
```

Control Plane 正在运行时，应使用 Human 自己的 Bearer Token 调用 API，让服务进程立即看到授权：

```bash
curl -X POST \
  -H "Authorization: Bearer $HUMAN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"agent":"工程师 A 的 Codex","executor":"codex","ttl_seconds":3600}' \
  https://mamba.example.com/api/v1/tasks/TSK-xxxxxxxx/flight-leases
```

每次签发都会生成不可变的 `FlightManifest`。调用方也可以显式声明目标、落地条件、上下文引用、Tool
权限、统一 Fuel 预算、恢复策略和资源租约；未声明时，塔台从 Task 与 Flow 生成保守默认值：

```json
{
  "agent": "工程师 A 的 Codex",
  "executor": "codex",
  "ttl_seconds": 3600,
  "manifest": {
    "objective": "实现 Gateway 路由与回归测试",
    "landing_conditions": ["cargo test 通过", "提交可检查的 patch"],
    "context_refs": ["artifact://FLOW-24/prd/v3"],
    "tool_permissions": [
      {"tool": "filesystem", "access": "write"},
      {"tool": "git", "access": "execute"}
    ],
    "fuel": {
      "max_duration_seconds": 1800,
      "max_context_bytes": 1048576,
      "max_tokens": 120000,
      "max_tool_calls": 81,
      "max_cost_usd": 8.0
    },
    "resources": [
      {"kind": "file", "key": "src/gateway.rs", "exclusive": true}
    ]
  }
}
```

相同文件、工作区、端口、浏览器或 GPU 的冲突独占租约不会同时放行。Worker 回传耗时、上下文大小和
可获得的模型费用；预算超限由服务端改判为 `crashed`，不能靠 Worker 自报安全落地。落地、坠机或起飞前
撤销都会释放资源。

Agent 只能领取属于自己的未过期租约。领取后租约立即失效，不能被第二个进程重复消费：

```bash
export MAMBA_TOKEN='mmb_agent_...'
mamba worker once \
  --mode execute \
  --executor codex \
  --workspace /path/to/llm-gateway \
  --task TSK-xxxxxxxx
```

写入航班要求源 Git worktree 干净。Worker 从当前 `HEAD` 创建 detached 临时 worktree，Claude Code 或
Codex 只能修改这份隔离副本；结束后把已跟踪、未跟踪和删除文件统一封装为
`.mambaflow/worker-runs/<TASK>/<RUN>/changes.patch`，随后强制回收临时 worktree。源仓库不会被自动修改，
也不会自动 commit、push、创建 MR 或合并。Human 可以先检查再接纳：

```bash
git apply --check .mambaflow/worker-runs/TSK-*/WRUN-*/changes.patch
git apply         .mambaflow/worker-runs/TSK-*/WRUN-*/changes.patch
```

如果 Worker 在领取后退出，下一次轮询会按原 `run_id` 恢复残留 worktree；如果补丁报告已经生成但尚未
送达塔台，则只重传报告，不会再次执行模型。`finish` 对完全相同的报告幂等。

坠机后先检查塔台建议，再由授权 Human 做恢复决定：

```bash
mamba task recovery-options LEASE-xxxxxxxx --by "工程师 A"
mamba task recover LEASE-xxxxxxxx \
  --by "工程师 A" --action reduce-scope \
  --reason "缩小上下文，只处理路由文件" \
  --objective "完成 Gateway 路由的最小变更"
```

`retry`、`switch-executor`、`reduce-scope` 和 `fork` 会创建新 Lease；新航班记录 `parent_lease_id`、
`root_lease_id` 与递增的 `attempt`，原报告不被覆盖。`human-handoff` 和 `ground` 只记录监督决定，不会
悄悄重跑。恢复动作和资源获取都与黑匣子写入同一批 append-only 事件，重启后仍能重建完整因果链。

塔台接收的是结构化黑匣子：基准 revision、变更文件、patch SHA-256、日志 SHA-256、执行摘要和起止时间；
原始 stdout/stderr 仍只保留在员工工作站。安全落地会产生 `remote_patch` Evidence，坠机会产生
`worker_blackbox` Evidence 并阻塞 Task。Human Owner 或 Demand Requester 可以读取相关航班报告；授权人
也可以在 Agent 领取前调用 `POST /api/v1/flight-leases/:id/revoke` 撤销租约。最终提交和验收仍由 Human
完成。

## GitLab 交付同步

GitLab 是第一条外部工作事实源。MambaFlow 使用只读 REST API 检查项目，并把某个 Merge Request 与
它最新的 Pipeline 同步为 Task 的 External Artifact。GitLab.com 和自托管实例都支持；访问令牌只从
环境变量读取，不会出现在命令参数、Flow Ledger 或 SQLite 中：

```bash
export GITLAB_TOKEN='glpat-...'
# 自托管实例可选：export GITLAB_URL='https://gitlab.example.com'

./target/release/mamba gitlab check --project platform/llm-gateway
./target/release/mamba gitlab sync \
  --task TSK-xxxxxxxx \
  --project platform/llm-gateway \
  --mr 42 \
  --by 佐巴扬
```

同步者必须是该 Task 的 Owner、Copilot 或对应的 Human Owner。MR 和 Pipeline 使用稳定 Artifact ID；
状态未变化时重复同步不会制造重复事件，状态变化则会留下新的 Ledger 航点。已合并的 MR 或成功的
Pipeline 可作为提交验收所需的可验证材料，但不会自动完成 Task，最终落地仍由 Demand Requester
确认。`mamba task show TSK-xxxxxxxx` 和 Ratatui 的 Task Detail 都会展示同步状态。

第一次手动同步同时建立 Task 与 MR 的绑定。之后可以在 GitLab 项目的 Settings > Webhooks 中订阅
Merge request events 和 Pipeline events，并把 URL 指向：

```text
https://mamba.example.com/api/v1/connectors/gitlab/webhook
```

GitLab 19 推荐使用 Signing token。把 GitLab 只显示一次的 `whsec_...` 放入服务进程环境，再启动塔台：

```bash
export GITLAB_WEBHOOK_SIGNING_TOKEN='whsec_...'
./target/release/mamba serve --bind 127.0.0.1:7777
```

接收器按照 [GitLab Standard Webhooks 签名协议](https://docs.gitlab.com/user/project/integrations/webhooks/#signing-tokens)
校验完整请求体、`webhook-id` 和时间戳，拒绝超过五分钟的签名。旧版 GitLab 可以改用
`GITLAB_WEBHOOK_TOKEN` 与 `X-Gitlab-Token`，但它不能验证请求体完整性，也没有可信事件时间，只作为
兼容路径。两种 Secret 都只保留在进程内存，不写入 Ledger。

每个 delivery ID 和 MR 事件时钟都会进入 Ledger。重复通知返回 `duplicate`，乱序通知返回 `stale`；
未经过首次同步的 MR 返回 `unbound`，不会凭 Webhook 自动创建内部任务。Task 当前状态只保留该 MR
最新的 Pipeline，旧 Pipeline 仍可从事件流审计，因此新失败不能被旧成功错误掩盖。请求体上限为 1 MiB。

当前连接器读取 Project、单个 MR 及其 Pipeline，也支持 Webhook 自动刷新；它仍不创建 Issue/MR、
不评论、不合并。REST Token 至少需要读取目标项目的权限，公开项目可以不设置 Token。自托管实例也
可以给单条命令传 `--url https://gitlab.example.com`。Webhook URL 必须通过可信网络或 TLS 反向代理
暴露给 GitLab，不能直接把默认的 localhost 监听地址公开到互联网。

Tower Tracker 可以手动运行，也可以交给 cron、CI 或服务进程定时触发。默认把 24 小时没有接单、航点
或验收视为需要关注；Blocked 和超过 P80 会直接成为 Critical，Warning 持续 4 小时后升级：

```bash
./target/release/mamba track scan --stale-hours 24 --escalate-after-hours 4
./target/release/mamba track list
./target/release/mamba track list --flow FLOW-xxxxxxxx --all
./target/release/mamba track inbox --for 牢大
./target/release/mamba track ack ESC-xxxxxxxx --by 牢大
```

扫描结果和解除动作都会写入 Flow Ledger。`track list` 默认只显示活动 Attention，`--all` 可审计已经
解除的历史提醒；`track inbox` 是发起人的升级收件箱。Attention 解除时，对应 Tower Call 会自动关闭。

## Claude Code 与 Codex 终端

v0 不自己实现另一套 Coding Agent 循环，而是把已经安装并登录的 Claude Code 或 Codex CLI 注册为
组织成员名下的执行终端。先检查本机命令：

```bash
./target/release/mamba executor check claude-code
./target/release/mamba executor check codex
```

也可以不使用演示数据，显式建立组织关系：

```bash
./target/release/mamba org init --name "Acme"
./target/release/mamba team add --name Platform \
  --capabilities "rust,backend,llm-platform,security"
./target/release/mamba principal add --name "工程师 A" --kind human \
  --team Platform --capabilities "rust,backend,llm-platform,security"
./target/release/mamba principal add --name "A 的 Codex" --kind agent \
  --team Platform --owner "工程师 A" \
  --capabilities "rust,backend,llm-platform,security" \
  --executor codex --workspace /path/to/repository
```

终端只能由任务的 Owner、Copilot 或 Agent 的 Human Owner 发起。默认运行是只读规划：

```bash
./target/release/mamba task run TSK-xxxxxxxx --by "工程师 A" --mode plan
```

只有已分配的 Human 显式传入 `--mode execute`，才允许终端修改注册的工作区：

```bash
./target/release/mamba task run TSK-xxxxxxxx --by "工程师 A" --mode execute
```

在 Ratatui 中，进入 Flow 或 Inbox 选中已经接单且依赖完成的任务，点击底部 `规划`，输入
`PASS` 确认模型调用；点击 `执行`，检查工作区后输入 `MAMBA` 才会授予写权限。航班在后台运行，
界面仍可浏览其他 Flow；Flight Deck 会显示 `AIRBORNE`、`LANDED` 或 `CRASHED`、模型费用和黑匣子
路径。本地 TUI 执行终端目前仍采取单航班串行门禁；Remote Flight 则按 Manifest 中的工作区、文件、
端口、浏览器和 GPU Claim 做资源冲突控制，航班结束前不会静默退出。

当前适配器的权限映射如下：

| MambaFlow 模式 | Claude Code | Codex |
| --- | --- | --- |
| `plan` | `--permission-mode plan`，禁用 Tools | `--sandbox read-only` |
| `execute` | `--permission-mode acceptEdits` | `--sandbox workspace-write` |

Claude Code 使用[非交互 JSON 输出](https://code.claude.com/docs/en/headless)和 JSON Schema；Codex
使用 `exec --json`、`--output-schema` 与 `--output-last-message`。规划器同样可以替换为这两个终端，
并始终以只读模式运行：

```bash
./target/release/mamba demand create "准备 Q3 发布计划" \
  --requester "工程师 A" --planner claude-code
./target/release/mamba demand create "准备 Q3 发布计划" \
  --requester "工程师 A" --planner codex
```

TUI 中点击 `新需求` 后可直接选择同样的三个规划器。Claude Code / Codex 规划会复用组织中已注册同类终端的
自定义命令和模型配置；认证由本机已经登录的 CLI 负责，MambaFlow 当前不保存 Anthropic 或 OpenAI
API Key，也还没有直连 Provider API 的适配层。未安装或未登录对应 CLI 时，模型规划会失败并在状态栏
给出原因，本地规划器不调用模型。

原始 stdout、stderr、退出码、时间和终端摘要写入
`.mambaflow/runs/<FLOW-ID>/<RUN-ID>.json`。安全落地和坠机都进入同一条 Flow Ledger；即使终端未安装
或超时，也会留下可检查的失败黑匣子。这里的本地 `mamba task run --mode execute` 仍会直接作用于
注册工作区；只有上一节的 Remote Worker 写入航班使用隔离 Git worktree。生产环境还需要容器级沙箱。

## v0 已实现

- 单组织、团队、Human、Agent、Capability 与容量注册；
- Tenant、最小默认角色、事件化角色授予/撤销与旧 Ledger 权限迁移；
- 管理者提交 Demand，本地规则或 Claude Code / Codex 生成结构化 PRD 和任务 DAG；
- Matcher 按硬能力约束、Human 要求、容量和在途负载给出 Assignment 与理由；
- Scheduler 校验依赖图，给出 P50/P80、关键路径和可解释估算因子；
- Flow 审批后生成 WorkRequest，Human / Agent Inbox 支持接受、拒绝和协商；
- Flow 内结构化 Command、Question、Update、Decision，支持团队投递、Agent Owner 监督和可回放回执；
- Requester 动态改派、任务成员工时协商、依赖窗口传播、P50/P80 与关键路径增量重排；
- 固定时区工作日历、请假事件、可用性匹配，以及避开夜间和周末的可解释排期；
- 事务性 Notification Outbox、HMAC Webhook、自动退避、强制重投和停用审计；
- 飞书交互卡片、Slack Block Kit、Teams Adaptive Card 原生通知 Connector 与审计测试传球；
- 外部 Human 身份绑定、Slack 原生按钮、飞书/Teams HMAC Bridge 与事务性幂等动作回执；
- 运行中 Flow 的 append-only Change Request、任务/工期影响预览、过期检测与 Human 批准或驳回；
- Task 的依赖门禁、Heartbeat、阻塞、Evidence、提交和 Human 最终验收；
- 幂等 Todo Tracker，识别未接单、失联、阻塞、待验收和超 P80，并持久化 Attention 生命周期；
- 面向 Demand Requester 的立即/延迟升级策略、Tower Calls 收件箱与 Human 确认；
- Bearer Token 身份、远程 Human Inbox 与任务操作 HTTP Control Plane；
- 无需共享数据库或仓库的 Remote Worker，以及远程 Agent 身份；
- 一次性 Remote Flight Lease、Human 写授权、结构化黑匣子和隔离 Git worktree 补丁；
- 强制 FlightManifest、统一 Fuel 预算、资源租约、服务端预算判定与可分叉监督树恢复；
- GitLab Project/MR/Pipeline 只读连接器、签名 Webhook、乱序保护与交付门禁；
- SQLite append-only Flow Ledger，CLI 每次启动都从同一事件流重建状态；
- Ratatui 塔台总览、Flow 工作台、个人 Inbox、阵容和黑匣子时间线；
- 管理员 DashboardSnapshot、Flow 健康度、Action Queue 与可重复 Showcase 工作流；
- 内嵌管理员 Web Console、Bearer 身份、响应式管理看板与坠机恢复操作；
- TUI 内可选 Local / Claude Code / Codex 的后台 PRD 规划与 Flow Ledger 自动回放；
- TUI 后台 Flight、`PASS` / `MAMBA` 放行确认、实时状态回放与 Flight Deck；
- 基于实时布局 HitMap 的标签、表格、操作带、弹窗点击与滚轮支持；
- Claude Code / Codex 只读规划与显式执行适配器，以及每次执行的 Black Box。

目前没有 GitLab 写操作，也没有 Microsoft 365、Google Workspace 的原生 OAuth Adapter、基于 Attention
的自动改派、地区节假日同步、多 Tenant 数据面、OIDC/SCIM、生产级容器沙箱、自动 commit/push/MR、
Coding/Office Capability Pack。这一版先验证组织 Flow 是否真的比一次 Agent 对话
更适合承载跨人的长期工作。

## 为什么选择 Rust

MambaFlow 使用 Rust 实现 Control Plane、Execution Runtime 和 CLI：

- 枚举和类型系统适合表达严格的 Flow、Task、Approval 和 Flight 状态；
- 所有权模型有助于限制跨用户和跨 Agent 的共享可变状态；
- 异步 Runtime 适合消息、模型流、长任务、取消、Heartbeat 和 Lease；
- 单二进制便于部署 CLI、Worker 和边缘执行节点；
- 对进程、资源上限、隔离和审计边界有直接控制。

v0 使用 Tokio、Clap、Ratatui、Crossterm、Serde、Schemars 和 SQLite。后续是否引入 Actor Runtime、
PostgreSQL、NATS 或其他基础设施，将由实际的远程执行与多租户需求决定。

## 与 DeerFlow 的关系

[DeerFlow](https://github.com/bytedance/deer-flow) 的定位是开源 long-horizon super agent harness：
围绕一个 lead agent 编排 sub-agents、memory、sandbox、skills 和 tools，完成研究、编码和内容生成等
长任务。

MambaFlow 受到它的启发，但关注不同层级的问题：

| | DeerFlow | MambaFlow |
| --- | --- | --- |
| 中心对象 | Thread / Lead Agent | Organization / FlowRun |
| 执行拓扑 | Lead Agent 拉起内部 Sub-agents | 多 Human、Team、Personal Agent 和 Org Agent 协作 |
| 委派 | Agent 内部任务调用 | 带组织权限链的跨身份 WorkRequest |
| 人类参与 | 对话、澄清和目标控制 | 审批、接单、协商、执行、Review、验收和升级 |
| 时间尺度 | 一次长任务 | 跨用户、跨系统、持续数天或更久的企业流程 |
| 可见性 | 会话与 Agent 执行 | Flow Ledger + 权限受控的 Black Box |

MambaFlow 不是 DeerFlow 的 fork。DeerFlow 可以成为某种 Agent Execution Backend，而 MambaFlow
负责组织级需求、人员、权限、调度、追踪和审计。

## 路线图

- [x] 确立 Human-Agent Flow 产品定位
- [x] 提出组织 Control Plane 与 Agent Execution Plane 分层
- [x] Rust workspace、测试基线和 `mamba` CLI v0
- [x] SQLite Flow Ledger、事件回放、能力匹配与基础排期
- [x] Claude Code / Codex 执行终端
- [x] Ratatui 组织塔台与 Human Inbox
- [x] Ratatui 后台模型规划与鼠标操作
- [x] Todo Tracker、可回放 Attention 与 Requester Escalation
- [x] Bearer 身份、远程 Inbox 与 Control Plane HTTP API
- [x] GitLab MR/Pipeline 只读交付同步
- [x] GitLab 签名 Webhook、delivery 幂等与乱序保护
- [x] 只读 Personal Agent Remote Worker
- [x] Remote Flight Lease、Human 写授权、结构化黑匣子与隔离 worktree
- [x] FlightManifest、Fuel、资源租约、失败分类与可分叉监督恢复树
- [x] 管理员看板、Action Queue 与一键 Showcase 数据流
- [x] 运行中 Flow 变更预演、Human Gate 与追加任务派发
- [x] Principal 工作日历、请假可用性与业务时间排期
- [x] 可靠通知 Outbox、签名 Webhook 与企业 Bridge 协议
- [x] 飞书、Slack、Teams 原生通知 Connector 与环境变量凭据
- [x] Slack 双向按钮、外部 Human 身份绑定与飞书/Teams Interaction Bridge
- [ ] RFC-0001：Tenant、Org、Principal 与 Authority 模型
- [ ] RFC-0002：FlowRun、Task DAG 与事件协议
- [ ] RFC-0003：WorkRequest、Inbox、Handoff 与 Approval Gate
- [ ] RFC-0004：Capability Matching 与可解释工期估算
- [ ] RFC-0005：Todo Tracker、Heartbeat、Evidence 与 Escalation
- [ ] RFC-0006：Flow Ledger、Black Box、可见性与审计
- [ ] 贡献规范与安全披露流程
- [ ] Coding Capability Pack
- [ ] Office Capability Pack
- [x] 内嵌 Web Console、管理员看板与 Flight 恢复操作
- [ ] 更多企业连接器
- [ ] 多租户、SSO/SCIM、策略引擎和生产级隔离

## 设计原则

- **Flow over chat**：企业工作不能只存在于一个人的聊天记录里。
- **Human and Agent are principals**：两者都有身份、权限、任务、Inbox 和责任边界。
- **Authority before autonomy**：先验证授权链，再允许 Agent 行动。
- **Evidence over self-report**：Todo 完成依赖 Evidence 和 Validator，不依赖一句“已经做完”。
- **Explainable assignment**：匹配和估时必须展示依据、范围、置信度和备选方案。
- **Human right to negotiate**：员工可以拒绝、协商、补充或升级系统分配。
- **One person, one city**：个人 Agent 的上下文和凭据属于本人授权域。
- **Crash first**：Agent 一定会失败，恢复和改派必须是正常路径。
- **Boring core, expressive UX**：核心协议稳定严肃，产品语言可以鲜活有记忆点。

## 非目标

- 不做工资、考勤、招聘、绩效打分等传统 HRIS 功能；
- 不允许管理者绕过授权直接读取员工私人 Agent 上下文；
- 不把 Agent 推荐或工期估算作为自动惩罚员工的依据；
- 不允许 Agent 自行扩大权限、预算、截止时间或数据范围；
- 不用模糊的“完成百分比”代替验收条件和 Evidence；
- 不追求第一版复制 DeerFlow、Jira、Claude Code 或 Office 套件的全部能力；
- 不为表达风格牺牲 API、权限、安全、审计和错误分类的准确性。

## 参与建设

当前最需要的是产品建模、协议设计和小型原型，而不是堆功能。欢迎通过 Issue 或 RFC 参与：

- 企业组织图与跨用户授权模型；
- Flow DAG、事件信封和持久化状态机；
- Personal Agent 的所有权与代理边界；
- 能力匹配、产能建模和工期校准；
- Todo Tracker、Evidence 和自动升级；
- Coding / Office Capability Pack；
- 企业数据隔离、Secret、审计和合规；
- Rust Actor、消息系统和取消语义；
- 中文产品语言与英文公共 API 如何长期共存。

项目采用 [MIT License](./LICENSE)。正式接受大规模代码贡献前，还需要补充贡献指南、行为准则和
安全披露流程。

欢迎大家多多参与本项目建设，争取都能拿到诸如字节 SSSP 或大模型四小龙 Agent 算法 & Infra
的工作机会。

## 声明

本项目名称与术语来自中文互联网文化表达，与 Kobe Bryant、其家属、NBA 及相关球队无关，
也不代表对任何现实事故或逝者的不尊重。
