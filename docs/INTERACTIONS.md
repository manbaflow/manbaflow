# MambaFlow Human Interaction Gateway

Interaction Gateway 把聊天工具里的按钮动作还原为一个经过验证的 MambaFlow Human 操作。它不会相信请求
Body 自报的用户名：供应商请求先验签，外部用户 ID 再通过 append-only 身份绑定解析为 Human Principal，
最后继续执行 MambaFlow 原有的 Assignment、Recipient 和 Escalation 权限检查。

## 身份绑定

绑定只能由本机管理 CLI 创建。一个 Human 在同一 Provider 下最多有一个有效外部身份，一个外部用户也不能
同时代表多个人：

```bash
mamba principal identity bind \
  --for 佐巴扬 \
  --provider slack \
  --external-user U0123456789 \
  --by admin

mamba principal identity list --for 佐巴扬
mamba principal identity unbind XID-xxxxxxxx --by admin
```

解绑不会删除历史。每条 `external_interaction.processed` 回执都保留当时解析出的 Principal、动作、目标和
Provider Delivery ID。

## Slack 原生动作

Slack Connector 会在以下 Block Kit 消息中加入按钮：

- `work_request.sent`：`接球`，执行 `task.accept`；
- 要求回执的 `flow_message.posted`：`确认收到`，执行 `message.ack`；
- `tracking.escalation_raised`：`接手处理`，执行 `escalation.ack`。

在 Slack App 的 Interactivity 中把 Request URL 设置为：

```text
https://mamba.example.com/api/v1/connectors/slack/actions
```

然后在 Control Plane 进程配置同一个 App 的 Signing Secret：

```bash
export MAMBA_SLACK_SIGNING_SECRET='replace-with-slack-signing-secret'
mamba serve --bind 127.0.0.1:7777
```

MambaFlow 使用原始 `application/x-www-form-urlencoded` Body 验证 `X-Slack-Signature`，签名原文为
`v0:{timestamp}:{raw_body}`，并拒绝超过五分钟的请求。同一请求的稳定摘要作为 Delivery ID，因此 Slack
重试不会重复执行按钮动作。

Slack Incoming Webhook 和 Interactivity 必须属于同一个已正确配置的 Slack App。仅配置 Webhook URL 不会
让按钮回调自动生效。

## 飞书与 Teams Bridge

飞书自定义群机器人和 Teams Workflows Incoming Webhook 都是通知入口，不提供可直接信任的双向 Human
身份。供应商 App/Bot 或 Power Automate Flow 应先验证自己的 OAuth/回调身份，再调用：

```text
POST /api/v1/connectors/interactions
```

请求 Body：

```json
{
  "external_user_id": "ou_xxxxxxxxx",
  "action": "task.accept",
  "target_id": "TSK-xxxxxxxx",
  "reason": null
}
```

允许的动作是 `task.accept`、`task.reject`、`message.ack` 和 `escalation.ack`。`task.reject` 必须提供非空
`reason`。最终验收、Flow 批准、改派和 Agent 写入授权没有放进聊天按钮允许列表。

Bridge 与 MambaFlow 之间使用独立密钥：

```bash
export MAMBA_INTERACTION_WEBHOOK_SECRET='replace-with-a-random-secret'
```

请求头：

```text
x-mamba-provider: feishu
x-mamba-delivery-id: feishu-event-xxxxxxxx
x-mamba-timestamp: 1784278800
x-mamba-signature: v1,<base64-hmac-sha256>
```

签名原文是：

```text
provider + "." + delivery_id + "." + timestamp + "." + raw_request_body
```

Bridge 必须为每个供应商事件使用稳定且唯一的 Delivery ID。MambaFlow 在五分钟窗口内验签，并将动作事件与
回执放进同一个 SQLite 事务；重放返回原回执且不产生新事件。

## 可见性

Ratatui 总览的 `场外回执` 显示当前 Flow 已验证动作数和组织有效身份数。动作同时进入 Flow Ledger，管理员
可以看到执行者、事件顺序和后续状态变化。未绑定身份、签名错误、过期请求和越权动作都不会写入成功回执。

协议参考：[Slack 请求验签](https://docs.slack.dev/authentication/verifying-requests-from-slack/)、
[Slack block_actions](https://docs.slack.dev/reference/interaction-payloads/block_actions-payload) 和
[Teams Webhook 限制](https://learn.microsoft.com/en-us/connectors/teams/#microsoft-teams-webhook)。
