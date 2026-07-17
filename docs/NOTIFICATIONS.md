# MambaFlow Notification Connector

Notification Connector 把 Flow Ledger 中需要人或外部系统关注的事件可靠投递到企业 Webhook。接收方可以是
飞书、Slack、Teams 的内部 Bridge，也可以是自动化平台、Office 服务或公司自己的消息网关。

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

## Office Bridge

Bridge 根据 `type` 映射到供应商动作，例如把 `work_request.sent` 发成 Teams Adaptive Card、把
`tracking.escalation_raised` 发到飞书群、把 `task.submitted` 创建为审批卡片。供应商 OAuth Token 和聊天
目标保留在 Bridge，不进入 MambaFlow Ledger。

日历方向使用 Control Plane 已有的 `GET/PUT /api/v1/me/calendar` 与 `POST /api/v1/me/time-off`：Bridge
读取 Microsoft 365、Google Workspace 或飞书日历的忙碌区间，再以员工自己的 Bearer 身份同步。这样厂商
连接器不会获得修改其他员工日历的隐式权限。
