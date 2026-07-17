# MambaFlow Showcase

这套演示不依赖模型账号，也不伪造一张静态看板。`demo --showcase` 会调用与正式 CLI、HTTP API 和 TUI
相同的领域接口，把每一步写入 append-only Flow Ledger。

## 准备

使用独立数据目录，避免与自己的开发数据混在一起：

```bash
cargo build --release
rm -rf .mambaflow-showcase

./target/release/mamba --data-dir .mambaflow-showcase --json \
  demo --showcase --workspace . > /tmp/mambaflow-showcase.json
```

查看管理员数据快照：

```bash
./target/release/mamba --data-dir .mambaflow-showcase \
  dashboard --as 牢大
```

预期会看到三条 Flow：

- `LLM Gateway v0`：需求已批准，Scope 已落地，Gateway Core 正在执行，鉴权任务因 Secret 轮换边界阻塞；
- `Q3 客户发布说明`：草案已经提交，等待牢大验收；
- `生产值班手册`：全部任务完成并安全落地。

## 打开塔台

建议终端至少 `120x36`，字体 `15-16px`：

```bash
./target/release/mamba --data-dir .mambaflow-showcase \
  tui --as 牢大 --workspace .
```

塔台会自动聚焦风险最高的 LLM Gateway Flow。

1. `总览`：讲解 Active Flow、Task Progress、At Risk、Waiting Human、Open Flights 和 Action Queue。
2. `任务流`：展示 PRD、依赖任务、Owner、P50/P80 与阻塞原因。
3. `收件箱`：按 `u` 把球权切到佐巴扬，展示 Human 与 Personal Agent 的共同 Assignment。
4. `黑匣子`：展示 Demand、PRD、审批、传球、Heartbeat、阻塞、Attention、Escalation 和 Flight Lease 事件。

## 现场解除阻塞

从初始化输出中取出任务 ID：

```bash
BLOCKED_TASK=$(jq -r '.showcase.blocked_task_id' /tmp/mambaflow-showcase.json)
REVIEW_TASK=$(jq -r '.showcase.waiting_review_task_id' /tmp/mambaflow-showcase.json)
```

在另一个终端让佐巴扬确认安全边界并复飞：

```bash
./target/release/mamba --data-dir .mambaflow-showcase \
  task start "$BLOCKED_TASK" --by 佐巴扬
./target/release/mamba --data-dir .mambaflow-showcase \
  task heartbeat "$BLOCKED_TASK" --by 佐巴扬 \
  --note "Secret 轮换边界已确认，安全检查恢复执行"
./target/release/mamba --data-dir .mambaflow-showcase track scan
```

再由牢大验收发布文档：

```bash
./target/release/mamba --data-dir .mambaflow-showcase \
  task complete "$REVIEW_TASK" --by 牢大
```

回到 TUI 按 `r`。`AT RISK` 和 `WAITING HUMAN` 会下降，Action Queue 会重排；Timeline 会保留阻塞、
复飞、Attention 解除和人工验收的完整因果链。这就是 MambaFlow 中 `Flow` 的含义：不是一次聊天记录，
而是管理者、员工、Personal Agent 和外部系统共同推进的一条可追踪工作流。

## HTTP 展示

为牢大签发 Token 并启动 Control Plane 后，同一份看板可以作为 JSON 提供给 Web Console 或集成方：

```bash
./target/release/mamba --data-dir .mambaflow-showcase \
  principal token issue --for 牢大 --label showcase
./target/release/mamba --data-dir .mambaflow-showcase serve

curl -H "Authorization: Bearer $MAMBA_TOKEN" \
  http://127.0.0.1:7777/api/v1/dashboard
```
