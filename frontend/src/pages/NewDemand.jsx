import { useEffect, useState } from "react";
import { App, Alert, Button, Card, Descriptions, Form, Input, Select, Typography, List, Tag } from "antd";
import { useNavigate } from "react-router-dom";

import { api } from "../api.js";

export default function NewDemand() {
  const { message } = App.useApp();
  const navigate = useNavigate();
  const [repositories, setRepositories] = useState([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  // 生成完直接把方案摊开在这一页——之前点完按钮什么都看不到，
  // 结果其实躺在另一个区块里。
  const [plan, setPlan] = useState(null);
  // 需要模型的拆解不在控制面里跑，会先排队等 Worker 领走。
  const [queued, setQueued] = useState(null);

  useEffect(() => {
    api.repositories().then(setRepositories).catch(() => {});
  }, []);

  async function submit(values) {
    setBusy(true);
    setError("");
    setPlan(null);
    setQueued(null);
    try {
      const result = await api.createDemand({
        summary: values.summary.trim(),
        planner: values.planner,
        timeout_seconds: 300,
        ...(values.repository ? { repository: values.repository } : {}),
      });
      if (result.planning_request) {
        setQueued(result.planning_request);
        message.info("已排队，等 Worker 领走");
      } else {
        setPlan(result.flow);
        message.success("方案已生成");
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        提需求
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        用一句话说清楚要做什么。系统会拆成任务、估算工期、分配给人或 Agent，等你确认后才开始。
      </Typography.Paragraph>

      <Card size="small">
        {error && <Alert type="error" message={error} showIcon style={{ marginBottom: 12 }} />}
        <Form layout="vertical" onFinish={submit} initialValues={{ planner: "local" }}>
          <Form.Item
            name="summary"
            label="要做什么"
            rules={[{ required: true, message: "写一句话" }]}
          >
            <Input.TextArea
              rows={3}
              maxLength={4000}
              placeholder="例如：教师端加一个「今日课表一键导出」"
            />
          </Form.Item>
          <Form.Item name="repository" label="在哪个仓库">
            <Select
              allowClear
              placeholder="不指定"
              options={repositories.map((repo) => ({
                value: repo.id,
                label: `${repo.name}（${repo.gitlab_project_path}）`,
              }))}
            />
          </Form.Item>
          <Form.Item name="planner" label="谁来拆解">
            <Select
              options={[
                { value: "local", label: "按模板（控制面直接生成，立刻出结果）" },
                { value: "claude_code", label: "Claude Code（排队，等 Worker 领走）" },
                { value: "codex", label: "Codex（排队，等 Worker 领走）" },
              ]}
            />
          </Form.Item>
          <Button type="primary" htmlType="submit" loading={busy}>
            生成方案
          </Button>
        </Form>
      </Card>

      {queued && (
        <Card size="small" title="已排队" style={{ marginTop: 16 }}>
          <Alert
            type="info"
            showIcon
            message={`${queued.id} 正在等待 ${queued.planner} 拆解`}
            description="控制面不跑模型，这条请求要由 Worker 领走执行。Worker 没在线的话会一直排队——去「Agent」页看有没有在线的执行器。"
          />
          <Descriptions size="small" column={2} style={{ marginTop: 12 }}>
            <Descriptions.Item label="Flow">{queued.flow_id}</Descriptions.Item>
            <Descriptions.Item label="状态">{queued.status}</Descriptions.Item>
          </Descriptions>
        </Card>
      )}

      {plan && (
        <Card
          size="small"
          title="生成的方案"
          style={{ marginTop: 16 }}
          extra={
            <Button type="primary" onClick={() => navigate("/approvals")}>
              去确认
            </Button>
          }
        >
          <Descriptions size="small" column={2} style={{ marginBottom: 12 }}>
            <Descriptions.Item label="Flow">{plan.id}</Descriptions.Item>
            <Descriptions.Item label="状态">{plan.status}</Descriptions.Item>
            <Descriptions.Item label="仓库">{plan.repository_id || "未指定"}</Descriptions.Item>
            <Descriptions.Item label="任务数">{(plan.tasks || []).length}</Descriptions.Item>
          </Descriptions>
          <List
            size="small"
            dataSource={plan.tasks || []}
            renderItem={(task) => (
              <List.Item>
                <List.Item.Meta title={task.title} description={task.objective} />
                <Tag>{task.assignee || "待分配"}</Tag>
              </List.Item>
            )}
          />
        </Card>
      )}
    </>
  );
}
