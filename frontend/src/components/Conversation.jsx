import { useEffect, useState } from "react";
import { App, Avatar, Button, Card, Empty, Form, Input, List, Select, Space, Tag, Typography } from "antd";
import { RobotOutlined, UserOutlined } from "@ant-design/icons";

import { api } from "../api.js";

const KIND_LABEL = {
  command: { text: "指令", color: "purple" },
  question: { text: "提问", color: "blue" },
  update: { text: "进展", color: "default" },
  decision: { text: "决定", color: "gold" },
};

/**
 * 一条需求上的对话。人和 Agent 在同一条线程里说话。
 *
 * 这不只是留言板：执行器每次开工前会把这条线程读成指令上下文，所以在这里
 * 追加一句话，等同于给下一轮执行补充要求——多轮对话就是这么形成的。
 */
export default function Conversation({ flowId, tasks = [] }) {
  const { message: toast } = App.useApp();
  const [messages, setMessages] = useState([]);
  const [participants, setParticipants] = useState([]);
  const [busy, setBusy] = useState(false);
  const [form] = Form.useForm();

  async function load() {
    try {
      const [thread, principals] = await Promise.all([
        api.flowMessages(flowId),
        api.principalsList().catch(() => []),
      ]);
      setMessages(thread);
      setParticipants(principals);
    } catch {
      /* 没有权限或线程为空时静默：对话区不该挡住页面其余部分 */
    }
  }

  useEffect(() => {
    load();
    const timer = setInterval(load, 15_000);
    return () => clearInterval(timer);
  }, [flowId]);

  async function send(values) {
    setBusy(true);
    try {
      await api.postMessage(flowId, {
        kind: values.kind,
        recipients: values.recipients || [],
        body: values.body.trim(),
        requires_ack: values.kind === "command",
        ...(values.task_id ? { task_id: values.task_id } : {}),
      });
      form.resetFields(["body"]);
      load();
    } catch (err) {
      toast.error(err.message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card size="small" title="对话" style={{ marginTop: 16 }}>
      <Typography.Paragraph type="secondary" style={{ marginTop: 0 }}>
        执行器开工前会读取这条线程。在这里补充的要求会带进下一轮执行。
      </Typography.Paragraph>

      {messages.length === 0 ? (
        <Empty description="还没有消息" image={Empty.PRESENTED_IMAGE_SIMPLE} />
      ) : (
        <List
          itemLayout="horizontal"
          dataSource={messages}
          renderItem={(item) => {
            const kind = KIND_LABEL[item.kind] || { text: item.kind, color: "default" };
            const isAgent = item.sender_name?.includes("Agent") || item.sender_id?.startsWith("AGT");
            return (
              <List.Item
                actions={
                  item.requires_ack && (item.acknowledgements || []).length === 0
                    ? [
                        <Button
                          key="ack"
                          size="small"
                          onClick={async () => {
                            await api.ackMessage(item.id);
                            load();
                          }}
                        >
                          确认收到
                        </Button>,
                      ]
                    : undefined
                }
              >
                <List.Item.Meta
                  avatar={
                    <Avatar
                      icon={isAgent ? <RobotOutlined /> : <UserOutlined />}
                      style={{ background: isAgent ? "#4f46e5" : "#64748b" }}
                    />
                  }
                  title={
                    <Space size={6}>
                      <span>{item.sender_name}</span>
                      <Tag color={kind.color}>{kind.text}</Tag>
                      {item.task_id && <Tag>{item.task_id}</Tag>}
                      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                        {new Date(item.created_at).toLocaleString("zh-CN")}
                      </Typography.Text>
                    </Space>
                  }
                  description={
                    <Typography.Paragraph style={{ marginBottom: 0, whiteSpace: "pre-wrap" }}>
                      {item.body}
                    </Typography.Paragraph>
                  }
                />
              </List.Item>
            );
          }}
        />
      )}

      <Form
        form={form}
        layout="vertical"
        onFinish={send}
        initialValues={{ kind: "command" }}
        style={{ marginTop: 12 }}
      >
        <Form.Item name="body" rules={[{ required: true, message: "写点什么" }]}>
          <Input.TextArea rows={3} placeholder="例如：导出格式改成 Excel，并保留课程颜色" />
        </Form.Item>
        <Space wrap>
          <Form.Item name="kind" noStyle>
            <Select
              style={{ width: 120 }}
              options={[
                { value: "command", label: "指令" },
                { value: "question", label: "提问" },
                { value: "update", label: "进展" },
                { value: "decision", label: "决定" },
              ]}
            />
          </Form.Item>
          <Form.Item name="task_id" noStyle>
            <Select
              allowClear
              style={{ width: 220 }}
              placeholder="针对某个任务（可选）"
              options={tasks.map((task) => ({ value: task.id, label: task.title }))}
            />
          </Form.Item>
          <Form.Item name="recipients" noStyle>
            <Select
              mode="multiple"
              allowClear
              style={{ minWidth: 220 }}
              placeholder="发给谁（可选）"
              options={participants.map((p) => ({
                value: p.name,
                label: `${p.name}（${p.kind === "agent" ? "Agent" : "人"}）`,
              }))}
            />
          </Form.Item>
          <Button type="primary" htmlType="submit" loading={busy}>
            发送
          </Button>
        </Space>
      </Form>
    </Card>
  );
}
