import { useEffect, useState } from "react";
import {
  Alert,
  App,
  Button,
  Card,
  Empty,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  Descriptions,
} from "antd";

import { api } from "../api.js";

const STATUS_COLOR = {
  queued: "default",
  claimed: "processing",
  planned: "success",
  failed: "error",
};

export default function Agents() {
  const { message, modal } = App.useApp();
  const [agents, setAgents] = useState([]);
  const [requests, setRequests] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [creating, setCreating] = useState(false);
  // 令牌只在签发那一刻能看到一次，之后服务端只留哈希。
  const [issued, setIssued] = useState(null);
  const [form] = Form.useForm();

  async function load() {
    setLoading(true);
    try {
      const [principals, planning] = await Promise.all([
        api.principals(),
        api.planningRequests(),
      ]);
      setAgents(principals.filter((principal) => principal.kind === "agent"));
      setRequests(planning);
      setError("");
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // 排队状态要能看到变化，30 秒轮询一次就够——这不是实时看板。
    const timer = setInterval(load, 30_000);
    return () => clearInterval(timer);
  }, []);

  async function register(values) {
    try {
      await api.createPrincipal({
        name: values.name.trim(),
        kind: "agent",
        capabilities: values.capabilities || "",
        executor: { kind: values.executor, ...(values.model ? { model: values.model } : {}) },
      });
      message.success("已注册");
      setCreating(false);
      form.resetFields();
      load();
    } catch (err) {
      message.error(err.message);
    }
  }

  async function issueToken(agent) {
    try {
      const credential = await api.issueCredential(agent.id, {
        label: `${agent.name} worker`,
        ttl_days: 90,
      });
      setIssued({ agent: agent.name, token: credential.token });
      load();
    } catch (err) {
      message.error(err.message);
    }
  }

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        执行器
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        执行器负责拆解需求与执行任务。可部署在本地工作站或集群中，两者使用相同的接入协议。
      </Typography.Paragraph>

      {error && <Alert type="error" message={error} showIcon style={{ marginBottom: 12 }} />}

      <Card
        size="small"
        title="已注册执行器"
        extra={
          <Button type="primary" size="small" onClick={() => setCreating(true)}>
            注册执行器
          </Button>
        }
      >
        <Table
          size="middle"
          rowKey="id"
          loading={loading}
          dataSource={agents}
          pagination={false}
          locale={{ emptyText: <Empty description="暂无执行器" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
          columns={[
            { title: "名称", dataIndex: "name" },
            {
              title: "执行器",
              render: (_, agent) =>
                agent.executor ? <Tag>{agent.executor.kind}</Tag> : <Tag color="warning">未配置</Tag>,
            },
            { title: "模型", render: (_, agent) => agent.executor?.model || "默认" },
            { title: "能力", render: (_, agent) => (agent.capabilities || []).join("、") || "—" },
            {
              title: "状态",
              dataIndex: "active",
              render: (active) => (active ? <Tag color="green">启用</Tag> : <Tag>停用</Tag>),
            },
            {
              title: "",
              align: "right",
              render: (_, agent) => (
                <Button size="small" onClick={() => issueToken(agent)}>
                  签发令牌
                </Button>
              ),
            },
          ]}
        />
      </Card>

      <Card size="small" title="拆解队列" style={{ marginTop: 16 }}>
        <Table
          size="middle"
          rowKey="id"
          loading={loading}
          dataSource={requests}
          pagination={false}
          locale={{ emptyText: <Empty description="队列为空" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
          columns={[
            {
              title: "状态",
              dataIndex: "status",
              render: (status) => <Tag color={STATUS_COLOR[status]}>{status}</Tag>,
            },
            { title: "需求", render: (_, row) => row.demand?.summary },
            { title: "执行器", dataIndex: "planner" },
            { title: "尝试次数", dataIndex: "attempt" },
            {
              title: "失败原因",
              dataIndex: "error",
              render: (value) => value || "—",
            },
            {
              title: "",
              align: "right",
              render: (_, row) =>
                row.status === "queued" || row.status === "claimed" ? (
                  <Button
                    size="small"
                    danger
                    onClick={() =>
                      modal.confirm({
                        title: "放弃该拆解请求",
                        content: "需求记录保留，但不再分配执行器处理。",
                        onOk: async () => {
                          await api.failPlanning(row.id, "人工放弃");
                          load();
                        },
                      })
                    }
                  >
                    放弃
                  </Button>
                ) : null,
            },
          ]}
        />
      </Card>

      <Modal
        title="注册执行器"
        open={creating}
        onCancel={() => setCreating(false)}
        onOk={() => form.submit()}
        okText="注册"
      >
        <Form form={form} layout="vertical" onFinish={register} initialValues={{ executor: "claude_code" }}>
          <Form.Item name="name" label="名称" rules={[{ required: true, message: "请填写名称" }]}>
            <Input placeholder="例如：集群执行器" />
          </Form.Item>
          <Form.Item name="executor" label="执行器">
            <Select
              options={[
                { value: "claude_code", label: "Claude Code" },
                { value: "codex", label: "Codex" },
              ]}
            />
          </Form.Item>
          <Form.Item name="model" label="模型（可选）" extra="留空则使用执行器默认模型">
            <Input placeholder="claude-opus-5" />
          </Form.Item>
          <Form.Item name="capabilities" label="能力标签（逗号分隔，可选）">
            <Input placeholder="backend,frontend" />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="令牌只显示一次"
        open={Boolean(issued)}
        onCancel={() => setIssued(null)}
        footer={<Button onClick={() => setIssued(null)}>已保存</Button>}
      >
        <Descriptions column={1} size="small" style={{ marginBottom: 12 }}>
          <Descriptions.Item label="Agent">{issued?.agent}</Descriptions.Item>
        </Descriptions>
        <Input.TextArea value={issued?.token} readOnly autoSize />
        <Typography.Paragraph type="secondary" style={{ marginTop: 12, marginBottom: 0 }}>
          服务端仅保留哈希值，关闭后无法再次查看。请配置到执行器的 RELAY_TOKEN。
        </Typography.Paragraph>
      </Modal>
    </>
  );
}
