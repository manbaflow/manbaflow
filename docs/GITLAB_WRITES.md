# GitLab Human Write Gate

MambaFlow 把 GitLab 读取和写入拆成两套凭据。只读 Connector 用于 MR/Pipeline 交付同步；Writer 只在
任务执行者提交具体 Payload、Flow 的 Human Demand Requester 放行后，才创建或评论外部对象。Agent
看不到写 Token，Tower 也不提供 merge 动作。

## 支持边界

| 动作 | GitLab REST API | 自动 merge |
| --- | --- | --- |
| 创建 Issue | `POST /projects/:id/issues` | 不适用 |
| 评论 Issue | `POST /projects/:id/issues/:issue_iid/notes` | 不适用 |
| 创建 MR | `POST /projects/:id/merge_requests` | 否 |
| 评论 MR | `POST /projects/:id/merge_requests/:merge_request_iid/notes` | 否 |

字段和路径遵循 GitLab 官方的 [Issues API](https://docs.gitlab.com/api/issues/)、
[Notes API](https://docs.gitlab.com/api/notes/) 和
[Merge requests API](https://docs.gitlab.com/api/merge_requests/)。MambaFlow 没有实现 merge endpoint，
MR 最终合并继续受 GitLab 自身审批、保护分支和 Human 操作约束。

## Tenant Token

生产环境优先为每个 Tenant 配独立 Project Access Token，并只授予目标项目和必要 API 权限。Token 的
角色、Scope、到期和轮换由 GitLab 管理，参考
[Project access tokens](https://docs.gitlab.com/user/project/settings/project_access_tokens/)。这些 REST 写入需要
`api` Scope；`write_repository` 只用于 Git-over-HTTP，不能代替 API 鉴权。Token 仍应使用能完成目标动作的
最低项目角色，并设置到期与轮换策略。

```bash
export MAMBA_GITLAB_WRITE_URL='https://gitlab.example.com'
export MAMBA_GITLAB_WRITE_TOKENS_JSON='{
  "TEN-aaaaaaaa": "glpat-tenant-a-token",
  "TEN-bbbbbbbb": "glpat-tenant-b-token"
}'
```

单 Tenant 测试环境可用 `MAMBA_GITLAB_WRITE_TOKEN`。Writer 不读取 `GITLAB_TOKEN`，因此只读凭据不会
意外升级为写凭据。一旦配置 Tenant JSON 映射，未命中的 Tenant 不会回退到通用 Token。Secret 只保留
在服务进程内存，不进入 Flow Ledger、Dashboard 或错误消息。

## 创建 Gate

任务必须处于 `in_progress` 或 `submitted`，调用者必须是该 Task 的 Owner、Copilot 或对应 Personal
Agent。下面的请求只写入一个待审批 Gate，不会立即调用 GitLab：

```bash
curl -X POST "$MAMBA_SERVER/api/v1/gitlab/writes" \
  -H "Authorization: Bearer $AGENT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "task_id": "TSK-xxxxxxxx",
    "payload": {
      "kind": "create_merge_request",
      "project": "platform/llm-gateway",
      "source_branch": "feature/provider-routing",
      "target_branch": "main",
      "title": "Add provider routing",
      "description": "Implements the approved gateway task.",
      "labels": ["delivery", "mambaflow"],
      "remove_source_branch": true,
      "draft": true
    }
  }'
```

Payload 的 SHA-256、请求人、目标项目和完整动作进入 append-only Ledger。相同执行者对同一 Task 重复
提交相同 Payload 会返回原 Gate，不制造重复待办。

## Human 放行

Web Console 和 Ratatui Action Queue 都展示待审批写入。只有该 Flow 的 Human Demand Requester 能执行：

```text
POST /api/v1/gitlab/writes/{id}/approve
POST /api/v1/gitlab/writes/{id}/reject   {"reason":"target branch is incorrect"}
```

批准后后台 Dispatcher 抢占一项写入并调用 GitLab。Tenant Admin 可用
`POST /api/v1/gitlab/writes/dispatch` 立即触发一次。成功响应和对应 External Artifact 在同一笔 Ledger
提交中落盘，Tower 重启后不会丢失“已写但未登记”的中间状态。

## 未知结果与复飞

创建和评论都可能产生不可重复的副作用。网络超时、重定向、GitLab 5xx/429，或成功响应无法解析时，
Writer 将 Gate 标为 `indeterminate`。Tower 重启后发现占用超过五分钟的 Dispatch 也采用同一状态，
不会自动 retry。

Human 必须先在 GitLab 搜索目标 Issue、MR 或评论，确认原请求是否已经成功，再在 Console 点击“已核对，
复飞”或调用 `POST /api/v1/gitlab/writes/{id}/retry`。明确的 4xx 拒绝记为 `failed`，修正外部权限或配置后
也需要 Human 再次放行。每次抢占使用新的 `dispatch_id`，原错误和因果链保留在黑匣子中。
