import { useEffect, useState } from "react";
import { App, Alert, Button, Card, Empty, Form, Input, List, Modal, Space, Tag, Typography } from "antd";
import { Link } from "react-router-dom";

import { api } from "../api.js";

const STATUS = {
  assigned: { text: "待接单", color: "warning" },
  accepted: { text: "已接单", color: "processing" },
  in_progress: { text: "进行中", color: "processing" },
  blocked: { text: "已阻塞", color: "error" },
  submitted: { text: "待验收", color: "warning" },
  completed: { text: "已完成", color: "success" },
};

/**
 * 「我的任务」：当前登录者被分到的活。
 *
 * 没有这一页，多人协作在界面上是断的——除了管理员，谁都看不到分给自己的任务，
 * 只能靠通知或口头转达。
 */
export default function Inbox() {
  const { message } = App.useApp();
  const [items, setItems] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [acting, setActing] = useState(null);
  const [form] = Form.useForm();

  async function load() {
    setLoading(true);
    try {
      setItems(await api.inbox());
      setError("");
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const timer = setInterval(load, 30_000);
    return () => clearInterval(timer);
  }, []);

  async function act(taskId, action) {
    try {
      await api.taskAction(taskId, action);
      message.success("已更新");
      load();
    } catch (err) {
      message.error(err.message);
    }
  }

  async function submitForm(values) {
    const { task, kind } = acting;
    try {
      if (kind === "block") {
        await api.blockTask(task.id, { reason: values.reason });
        message.success("已上报阻塞");
      } else {
        // 提交接口不收正文，交付说明发成一条线程消息，验收的人能看到。
        await api.postMessage(task.flow_id || acting.task.flow_id, {
          kind: "update",
          recipients: [],
          body: values.reason,
          requires_ack: false,
          task_id: task.id,
        }).catch(() => {});
        await api.submitTask(task.id);
        message.success("已提交验收");
      }
      setActing(null);
      form.resetFields();
      load();
    } catch (err) {
      message.error(err.message);
    }
  }

  if (error) return <Alert type="error" message={error} showIcon />;

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        我的任务
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        分配给你的任务。接单后可自行执行，也可以交给你自己的执行器完成。
      </Typography.Paragraph>

      <Card size="small" loading={loading}>
        {items.length === 0 ? (
          <Empty description="暂无分配给你的任务" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ) : (
          <List
            dataSource={items}
            renderItem={(item) => {
              const task = item.task || item;
              const meta = STATUS[task.status] || { text: task.status, color: "default" };
              return (
                <List.Item
                  actions={[
                    task.status === "assigned" && (
                      <Button key="accept" size="small" type="primary" onClick={() => act(task.id, "accept")}>
                        接单
                      </Button>
                    ),
                    task.status === "accepted" && (
                      <Button key="start" size="small" type="primary" onClick={() => act(task.id, "start")}>
                        开始
                      </Button>
                    ),
                    task.status === "in_progress" && (
                      <Button key="submit" size="small" onClick={() => setActing({ task, kind: "submit" })}>
                        提交验收
                      </Button>
                    ),
                    !["completed", "submitted"].includes(task.status) && (
                      <Button key="block" size="small" danger onClick={() => setActing({ task, kind: "block" })}>
                        上报阻塞
                      </Button>
                    ),
                  ].filter(Boolean)}
                >
                  <List.Item.Meta
                    title={
                      <Space>
                        <Tag color={meta.color}>{meta.text}</Tag>
                        <span>{task.title}</span>
                        {item.flow_id && <Link to={`/flows/${item.flow_id}`}>查看方案</Link>}
                      </Space>
                    }
                    description={task.objective || task.description}
                  />
                </List.Item>
              );
            }}
          />
        )}
      </Card>

      <Modal
        open={Boolean(acting)}
        title={acting?.kind === "block" ? "上报阻塞" : "提交验收"}
        onCancel={() => setActing(null)}
        onOk={() => form.submit()}
        okText="提交"
      >
        <Typography.Paragraph type="secondary">{acting?.task?.title}</Typography.Paragraph>
        <Form form={form} layout="vertical" onFinish={submitForm}>
          <Form.Item
            name="reason"
            label={acting?.kind === "block" ? "卡在哪里" : "交付说明"}
            rules={[{ required: true, message: "写清楚，会记进 Ledger" }]}
          >
            <Input.TextArea rows={3} />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}
