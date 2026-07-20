# MambaFlow TUI Showcase

Showcase 的主舞台就是 Ratatui 塔台。它不依赖模型账号，也不是预先画好的静态 Dashboard：点击装载后，
组织、任务、阻塞、验收和 Flight Lease 都通过正式领域接口写入 append-only Flow Ledger，随后由 TUI 从
同一份状态源渲染。

## 一键起飞

使用独立数据目录，建议终端至少 `120x36`、字体 `15-16px`：

```bash
cargo build --release
rm -rf .mambaflow-showcase
./target/release/mamba --data-dir .mambaflow-showcase \
  tui --workspace .
```

打开后直接点击底栏最左侧的 `SHOWCASE`。塔台会现场完成以下动作：

- 建立 `Mamba Labs / 洛杉矶研发队`，让牢大带队并把工程球权交给佐巴扬；
- 注册两名 Human 和归属明确的 Claude Code / Codex 个人副驾；
- 为两名 Human 配置 UTC+08:00 的工作日历，让排期避开夜间和周末；
- 生成三条真实 Flow，并写入需求、PRD、审批、Assignment、Heartbeat、Evidence 和 Tracker 事件；
- 由牢大向佐巴扬与 Codex 副驾发送一条等待回执的生产放行指令；
- 生成一份带内容摘要的 Office Release Request，等待牢大点击 `放行发布`；
- 签发一张远程 Flight Lease，然后自动聚焦风险最高的 LLM Gateway。

底栏的 `SHOWCASE` 只在空塔台出现，避免误把演示数据灌进真实组织。

## 五分钟讲解路线

### 1. 总览：管理者先看风险

装载后停留在 `总览 OVERVIEW`。第一屏同时显示 Active Flows、Task Progress、At Risk、Waiting Human、
Open Flights、Flow Health 和 Action Queue。重点说明：管理员看到的是整个组织的交付状态和下一步动作，
不是某个 Agent 的聊天记录。`OUTBOX` 同时显示等待投递或失败的企业通知；没有配置 Endpoint 时保持为零。
配置 Endpoint 后，底部 `投递通知` 可以现场触发一次非阻塞投递，落地或坠机结果会回到同一条时间线。
点击底部 `放行发布` 会把当前最早的 Office Release 从 `requested` 推进为 `approved`；演示数据不配置
真实 Provider Token，所以不会向外部收件人发送内容。

当前三条 Flow 分别是：

- `LLM Gateway v0`：Scope 已落地，Gateway Core 正在执行，鉴权任务因 Secret 轮换边界阻塞；
- `Q3 客户发布说明`：草案已经提交，同时有一封客户邮件等待牢大放行；
- `生产值班手册`：所有任务已经完成并安全落地。

点击 `阵容 ROSTER` 可以看到成员产能、终端和工作日历。Human 使用工作日时间，副驾默认保持 24×7；
这两个维度会同时进入匹配理由和工期计算，而不是把“80% 产能”误当成全天在线。

### 2. 任务流：从 PRD 到具体球权

点击 `任务流 FLOWS`，再点击左侧 LLM Gateway。右侧可以查看任务 DAG、Owner、P50/P80、状态和阻塞原因。
点击任务会移动球权；底栏可继续接单、推进、规划或在 Human 放行后执行。

### 3. 收件箱：Human 与个人 Agent 协作

点击 `收件箱 INBOX`，再点击顶部当前球权，把球权从牢大切到佐巴扬。`TOWER COMMS` 会显示
牢大发来的 Secret 轮换指令，同时覆盖发给 Codex 副驾的消息，因为个人 Agent 始终受其 Human Owner
监督。点击底栏 `收到` 后，指令从待办消失，但确认事件仍留在黑匣子中。

选中一个任务并点击 `传球`，佐巴扬的回传会自动发给 Demand Requester；切回牢大即可在 Inbox 看到。

### 4. 黑匣子：解释 Flow 为什么可追踪

点击 `黑匣子 TIMELINE`。左侧是当前 Flow 的完整事件链，右侧 Flight Deck 显示已放行的远程航班。
需求、审批、传球、执行、阻塞、升级和人工验收都能按因果顺序重建；退出再打开不会丢失。

### 5. 现场推进状态

回到 `任务流`，选中阻塞的鉴权任务并把球权切回牢大，点击 `换防`。弹窗会列出满足 `backend/security`
硬能力约束的 Human、Agent 或团队；点击候选条可以轮换，输入原因后确认。任务会向新 Owner 重新
发出 WorkRequest，Flow 的 P50/P80、下游起始时间和关键路径会现场改变。

切到新 Owner、选中该任务并点击 `调时`，把基础工时改大或改小，可以再次展示 ETA 如何沿 DAG 传播。
接单后点击 `推进`，任务会从 `BLOCKED/ASSIGNED` 恢复执行。再回到总览点击 `巡航`，风险指标与
Action Queue 会按照最新事件重新计算。

接着点击左侧 LLM Gateway Flow，把球权切回牢大并点击 `变更`，输入“增加客户迁移检查
清单”。塔台先生成追加任务和 P80 变化，正式任务列表不会立刻改变；Flow 行会出现 `[CHG]`，状态栏
显示新增范围的边际工期和相对正式排期的净变化。点击 `批准` 后，新任务才进入 DAG 并向
Owner 发出 WorkRequest。
也可以再提一份变更，点击 `驳回变更` 填写原因，演示 Human Gate 如何让正式 Flow 保持不变。
如果预览之后有人推进了原任务或改了排期，旧预览会被判定过期，必须重新计算后才能批准。

对于等待验收的发布文档 Flow，切回牢大、选中已提交任务并点击 `验收`，`WAITING HUMAN` 会随之下降，
时间线保留这次人工确认。整个演示只需要一个 TUI 窗口。

## 截图建议

使用 `120x36` 或更大的深色终端，装载 Showcase 后停在总览页；这张图能同时呈现组织品牌、关键指标、
三条 Flow、风险和 Action Queue。第二张图切到黑匣子页，让 Flow Ledger 与 Flight Deck 同屏。

macOS 可按 `Command + Shift + 4`，再按空格选择终端窗口。

## 自动化接口

`demo --showcase`、`dashboard --as 牢大` 和 `GET /api/v1/dashboard` 仍然保留，供测试脚本、Web Console
或集成方读取同一份数据；它们不是现场展示的前置步骤。
