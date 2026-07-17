# MambaFlow Notification Connector

Notification Connector 把 Flow Ledger 中需要人或外部系统关注的事件可靠投递到企业 Webhook。接收方可以是
飞书、Slack、Teams，也可以是自动化平台、Office 服务或公司自己的消息网关。

## 注册 Endpoint

密钥只存在于投递进程的环境变量中，Ledger 仅保存变量名：

```bash
export MAMBA_OPS_WEBHOOK_SECRET='replace-with-a-random-secret'

mamba notification endpoint-add \
  --name operations \
  --url https://bridge.example.com/mambaflow \
  --secret-env MAMBA_OPS_WEBHOOK_SECRET \
  --events work_request.sent,task.blocked,task.submitted,tracking.escalation_raised,flow.completed
```

不传 `--events` 时订阅默认的派单、Flow 消息、阻塞、提交验收、升级、变更、坠机和完成事件。传 `*` 可以
订阅除 `notification.*` 之外的全部领域事件。

## 原生消息 Connector

飞书、Slack 和 Teams 的 Webhook URL 本身都是凭据。MambaFlow 只把环境变量名写入 Ledger，不保存或通过
管理 API 返回真实 URL：

```bash
export MAMBA_FEISHU_WEBHOOK_URL='https://open.feishu.cn/open-apis/bot/v2/hook/...'
export MAMBA_FEISHU_SIGNING_SECRET='replace-with-the-bot-signing-secret'

mamba notification connector-add \
  --provider feishu \
  --name engineering-feishu \
  --url-env MAMBA_FEISHU_WEBHOOK_URL \
  --secret-env MAMBA_FEISHU_SIGNING_SECRET \
  --events work_request.sent,task.blocked,tracking.escalation_raised,flow.completed
```

Slack 和 Teams 不需要单独的 `--secret-env`，因为随机凭据已经包含在 Webhook URL 中：

```bash
export MAMBA_SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'
export MAMBA_TEAMS_WORKFLOW_URL='https://...'

mamba notification connector-add \
  --provider slack --name operations-slack \
  --url-env MAMBA_SLACK_WEBHOOK_URL

mamba notification connector-add \
  --provider teams --name leadership-teams \
  --url-env MAMBA_TEAMS_WORKFLOW_URL
```

三种 Connector 会分别生成飞书交互卡片、Slack Block Kit 和 Teams Adaptive Card。Teams 应使用 Workflows
中的 `When a Teams webhook request is received`，而不是新建即将退役的 Microsoft 365 Connector。当前支持
可直接 `POST` 的 Workflow URL；要求 Entra 身份令牌的 Trigger 仍需要外部 OAuth Bridge。

注册后可以发送一条测试卡片。测试同样先进入 Outbox，结果会作为 `notification.delivered` 或
`notification.failed` 留在 Ledger：

```bash
mamba notification endpoint-list
mamba notification test NEND-xxxxxxxx
```

## HTTP 协议

MambaFlow 发送 `POST application/json`：

```json
{
  "specversion": "1.0",
  "id": "NTF-xxxxxxxx",
  "source": "mambaflow://organizations/ORG-xxxxxxxx",
  "type": "task.blocked",
  "subject": "mambaflow://flows/FLOW-xxxxxxxx",
  "time": "2026-07-17T09:00:00Z",
  "actor": "佐巴扬",
  "data": {
    "type": "task_blocked",
    "data": {}
  }
}
```

请求头：

```text
webhook-id: NTF-xxxxxxxx
webhook-timestamp: 1784278800
webhook-signature: v1,<base64-hmac-sha256>
```

签名原文是以下字节的直接拼接：

```text
webhook-id + "." + webhook-timestamp + "." + raw_request_body
```

接收方必须使用原始 Body 验证签名、限制时间戳偏差，并把 `webhook-id` 作为幂等键。任意 `2xx` 代表安全
落地；网络错误和非 `2xx` 会记录为 `failed`。

## Outbox 与重试

业务事件和 `notification.queued` 在同一个 SQLite 事务中提交。Control Plane 默认每 15 秒扫描 Outbox，
失败投递从 30 秒开始指数退避，最长约一小时。Ratatui 也会非阻塞逐条投递，`OUTBOX` 指标显示积压或
失败数量，底部 `投递通知` 可以立即尝试。也可以通过 CLI 查看或强制重投：

```bash
mamba notification deliveries
mamba notification deliveries --all
mamba notification dispatch --force --limit 50
```

停用 Endpoint 会把它尚未发送的记录转成 `cancelled`，不会删除 payload 或审计历史：

```bash
mamba notification endpoint-disable NEND-xxxxxxxx
```

## Office 与双向交互

当前原生 Connector 负责单向通知，收到 `work_request.sent`、`task.blocked`、升级或验收事件时生成适合
供应商的卡片。它不冒充员工读取聊天，也不把按钮点击直接当作 Human 批准。需要从聊天中接单、审批或回复
时，应使用供应商 App/Bot 的 OAuth 与回调接口，把已经验证的用户身份映射为 MambaFlow Principal，再调用
Control Plane API。

日历方向使用 Control Plane 已有的 `GET/PUT /api/v1/me/calendar` 与 `POST /api/v1/me/time-off`：Bridge
读取 Microsoft 365、Google Workspace 或飞书日历的忙碌区间，再以员工自己的 Bearer 身份同步。这样厂商
连接器不会获得修改其他员工日历的隐式权限。

供应商协议参考：[飞书自定义机器人](https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot)、
[Slack Incoming Webhooks](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/) 和
[Teams Webhook](https://learn.microsoft.com/en-us/connectors/teams/#microsoft-teams-webhook)。
