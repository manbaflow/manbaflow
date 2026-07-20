# Office Human Release Gate

MambaFlow 把 Office 协作拆成三个独立阶段：Agent 在隔离 worktree 生成草稿，Worker 把具体字节暂存为
不可变 Artifact，Human 审查内容和目标后放行。只有最后一步由 Tower 使用服务端 OAuth Token 调用云端
API。Agent 看不到 Token，也不能把“写草稿”扩成“发邮件”。

## 支持的动作

| 动作 | Microsoft 365 | Google Workspace |
| --- | --- | --- |
| 文件发布 | Graph DriveItem content upload | Drive multipart create 或指定 File ID update |
| 邮件发送 | Graph `sendMail` | Gmail `messages.send` |
| 日历创建 | Graph Calendar Event + `transactionId` | Calendar Event + 稳定 Event ID |

实现对应 [Microsoft Graph sendMail](https://learn.microsoft.com/en-us/graph/api/user-sendmail?view=graph-rest-1.0)、
[DriveItem content upload](https://learn.microsoft.com/en-us/graph/api/driveitem-put-content?view=graph-rest-1.0)、
[Graph Calendar Event](https://learn.microsoft.com/en-us/graph/api/user-post-events?view=graph-rest-1.0)、
[Google Drive files.create](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/create)、
[Gmail messages.send](https://developers.google.com/workspace/gmail/api/guides/sending) 和
[Google Calendar events.insert](https://developers.google.com/workspace/calendar/api/v3/reference/events/insert)。

## OAuth 与 Scope

MambaFlow 不保存 Refresh Token，也不实现厂商 Consent Portal。企业 Credential Broker 获取并刷新短期
Access Token，再通过 Secret Manager 注入。优先为每个 Tenant 配独立 Token：

```bash
export MAMBA_MICROSOFT_GRAPH_TOKENS_JSON='{
  "TEN-aaaaaaaa": "microsoft-access-token"
}'
export MAMBA_GOOGLE_WORKSPACE_TOKENS_JSON='{
  "TEN-aaaaaaaa": "google-access-token"
}'
```

单 Tenant 测试环境可以使用 `MAMBA_MICROSOFT_GRAPH_TOKEN` 和 `MAMBA_GOOGLE_WORKSPACE_TOKEN`。一旦配置
Tenant JSON 映射，未命中的 Tenant 不会回退到通用 Token。

按启用动作授予最小权限。Microsoft Delegated 权限通常分别为 `Files.ReadWrite`、`Mail.Send` 和
`Calendars.ReadWrite`。Google 分别优先使用 `drive.file`、`gmail.send` 和 `calendar.events`；发布到应用
未创建或未由用户选择的现有 Drive 文件时，可能需要更宽权限，应单独评审。不要为了方便一次性授予全部
组织文件或邮箱权限。

## Artifact Staging

Office Remote Worker 会在清理 worktree 前自动上传所有变更文件。手工调试可以发送原始字节：

```bash
curl -X PUT \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H 'Content-Type: application/pdf' \
  --data-binary @release.pdf \
  "$MAMBA_SERVER/api/v1/flight-leases/$LEASE_ID/artifacts?path=reports%2Frelease.pdf"
```

仅航班所属 Agent 能在 `active` Office Flight 中暂存，单次航班的全部文件最多 25 MiB。路径必须位于
FlightManifest Output Contract 内。同一航班的同一路径一旦暂存就不可换内容；重复上传相同字节返回
原 Artifact。内容按 SHA-256 去重，元数据进入 append-only Flow Ledger，字节进入 SQLite `artifacts`
或 PostgreSQL `mamba_artifacts`。

## 创建和放行

任务执行者或其个人 Agent 可以申请副作用。下面的请求只创建 Gate，不发送邮件：

```bash
curl -X POST "$MAMBA_SERVER/api/v1/office/releases" \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id": "TSK-xxxxxxxx",
    "provider": "microsoft365",
    "payload": {
      "kind": "send_email",
      "account_id": "owner@example.com",
      "to": ["team@example.com"],
      "cc": [],
      "bcc": [],
      "subject": "Weekly delivery update",
      "body": "All acceptance checks passed.",
      "body_type": "text"
    }
  }'
```

Web Console 的 Human Gates 和 Ratatui Action Queue 会显示目标摘要与 Payload SHA-256。只有 Flow 的 Human
Demand Requester 可以执行：

```text
POST /api/v1/office/releases/{id}/approve
POST /api/v1/office/releases/{id}/reject   {"reason":"recipient list is incomplete"}
```

批准后后台 Dispatcher 自动领取。Tenant Admin 也可调用 `POST /api/v1/office/releases/dispatch` 立即处理
一项，主要用于演练和排障。Provider 返回的外部 ID、URL、状态码和时间会写入同一 Release。

## 恢复规则

Tower 每次只能为一项 Release 创建一个 `dispatch_id`。多副本依靠 PostgreSQL stream head 冲突保证只有
一个副本领取。进程在 Dispatch 中断超过五分钟后按动作恢复：

| 动作 | 自动恢复 |
| --- | --- |
| Microsoft 文件路径覆盖 | 可以，PUT 内容相同 |
| Google 指定 File ID 更新 | 可以，PATCH 内容相同 |
| Microsoft / Google 日历创建 | 可以，使用稳定事务或 Event ID |
| Microsoft / Google 邮件发送 | 不可以，进入 `indeterminate` |
| Google Drive 新建文件 | 不可以，进入 `indeterminate` |

`failed` 表示请求确定没有产生副作用，Human 可以再次放行；`indeterminate` 表示 Provider 可能已经执行，
必须先去邮箱、日历或云盘核对，再创建新的 Release。MambaFlow 不会用自动 retry 掩盖重复发送风险。
