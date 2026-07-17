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

打开后直接点击底栏最左侧的 `SHOWCASE`，也可以按 `d`。塔台会现场完成以下动作：

- 建立 `Mamba Labs / 洛杉矶研发队`，让牢大带队并把工程球权交给佐巴扬；
- 注册两名 Human 和归属明确的 Claude Code / Codex 个人副驾；
- 生成三条真实 Flow，并写入需求、PRD、审批、Assignment、Heartbeat、Evidence 和 Tracker 事件；
- 签发一张远程 Flight Lease，然后自动聚焦风险最高的 LLM Gateway。

底栏的 `SHOWCASE` 只在空塔台出现，避免误把演示数据灌进真实组织。

## 五分钟讲解路线

### 1. 总览：管理者先看风险

装载后停留在 `总览 OVERVIEW`。第一屏同时显示 Active Flows、Task Progress、At Risk、Waiting Human、
Open Flights、Flow Health 和 Action Queue。重点说明：管理员看到的是整个组织的交付状态和下一步动作，
不是某个 Agent 的聊天记录。

当前三条 Flow 分别是：

- `LLM Gateway v0`：Scope 已落地，Gateway Core 正在执行，鉴权任务因 Secret 轮换边界阻塞；
- `Q3 客户发布说明`：草案已经提交，正在等待牢大验收；
- `生产值班手册`：所有任务已经完成并安全落地。

### 2. 任务流：从 PRD 到具体球权

点击 `任务流 FLOWS`，再点击左侧 LLM Gateway。右侧可以查看任务 DAG、Owner、P50/P80、状态和阻塞原因。
点击任务会移动球权；底栏可继续接单、推进、规划或在 Human 放行后执行。

### 3. 收件箱：Human 与个人 Agent 协作

点击 `收件箱 INBOX`，再点击顶部当前球权或按 `u`，在牢大与佐巴扬之间轮换。两个人看到的 Assignment、
Tower Call 和待验收事项不同，个人 Agent 的权限始终挂在其 Human Owner 之下。

### 4. 黑匣子：解释 Flow 为什么可追踪

点击 `黑匣子 TIMELINE`。左侧是当前 Flow 的完整事件链，右侧 Flight Deck 显示已放行的远程航班。
需求、审批、传球、执行、阻塞、升级和人工验收都能按因果顺序重建；退出再打开不会丢失。

### 5. 现场推进状态

回到 `任务流`，选中阻塞任务，将顶部球权切到佐巴扬，然后点击底栏 `推进`。任务会从 `BLOCKED` 复飞为
`IN PROGRESS`。再回到总览点击 `巡航`，风险指标与 Action Queue 会按照最新事件重新计算。

对于等待验收的发布文档 Flow，切回牢大、选中已提交任务并点击 `验收`，`WAITING HUMAN` 会随之下降，
时间线保留这次人工确认。整个演示只需要一个 TUI 窗口。

## 截图建议

使用 `120x36` 或更大的深色终端，装载 Showcase 后停在总览页；这张图能同时呈现组织品牌、关键指标、
三条 Flow、风险和 Action Queue。第二张图切到黑匣子页，让 Flow Ledger 与 Flight Deck 同屏。

macOS 可按 `Command + Shift + 4`，再按空格选择终端窗口。

## 自动化接口

`demo --showcase`、`dashboard --as 牢大` 和 `GET /api/v1/dashboard` 仍然保留，供测试脚本、Web Console
或集成方读取同一份数据；它们不是现场展示的前置步骤。
