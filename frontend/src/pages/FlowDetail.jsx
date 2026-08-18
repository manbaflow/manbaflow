import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  Alert,
  App,
  Button,
  Card,
  Col,
  Descriptions,
  List,
  Row,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from "antd";

import { api } from "../api.js";
import Conversation from "../components/Conversation.jsx";

const TASK_STATUS = {
  pending: { text: "未开始", color: "default" },
  assigned: { text: "已分配", color: "default" },
  accepted: { text: "已接单", color: "processing" },
  in_progress: { text: "进行中", color: "processing" },
  blocked: { text: "已阻塞", color: "error" },
  submitted: { text: "待验收", color: "warning" },
  completed: { text: "已完成", color: "success" },
};

export default function FlowDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const { message } = App.useApp();
  const [flow, setFlow] = useState(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function load() {
    try {
      setFlow(await api.flow(id));
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    load();
  }, [id]);

  if (error) return <Alert type="error" message={error} showIcon />;
  if (!flow) return <Spin />;

  const prd = flow.prd || {};
  const tasks = flow.tasks || [];
  const draft = flow.status === "draft";

  return (
    <>
      <Space align="baseline" style={{ justifyContent: "space-between", width: "100%" }}>
        <div>
          <Typography.Title level={4} style={{ marginTop: 0, marginBottom: 4 }}>
            {prd.title || flow.demand?.summary}
          </Typography.Title>
          <Typography.Text type="secondary">
            {flow.id} · 提出人 {flow.demand?.requester} · {tasks.length} 个任务
          </Typography.Text>
        </div>
        {draft && (
          <Button
            type="primary"
            loading={busy}
            onClick={async () => {
              setBusy(true);
              try {
                await api.approveFlow(flow.id);
                message.success("已确认，进入执行");
                load();
              } catch (err) {
                message.error(err.message);
              } finally {
                setBusy(false);
              }
            }}
          >
            确认并执行
          </Button>
        )}
      </Space>

      <Card size="small" title="需求说明" style={{ marginTop: 16 }}>
        <Typography.Paragraph style={{ marginBottom: 12 }}>{prd.summary}</Typography.Paragraph>
        <Row gutter={16}>
          <Col span={8}>
            <Typography.Text strong>目标</Typography.Text>
            <List
              size="small"
              dataSource={prd.goals || []}
              locale={{ emptyText: "未列出" }}
              renderItem={(item) => <List.Item>{item}</List.Item>}
            />
          </Col>
          <Col span={8}>
            <Typography.Text strong>不做什么</Typography.Text>
            <List
              size="small"
              dataSource={prd.non_goals || []}
              locale={{ emptyText: "未列出" }}
              renderItem={(item) => <List.Item>{item}</List.Item>}
            />
          </Col>
          <Col span={8}>
            <Typography.Text strong>验收标准</Typography.Text>
            <List
              size="small"
              dataSource={prd.acceptance_criteria || []}
              locale={{ emptyText: "未列出" }}
              renderItem={(item) => <List.Item>{item}</List.Item>}
            />
          </Col>
        </Row>
      </Card>

      <Card size="small" title="任务拆解" style={{ marginTop: 16 }}>
        <Table
          rowKey="id"
          size="middle"
          dataSource={tasks}
          pagination={false}
          tableLayout="fixed"
          expandable={{
            // 目标和验收标准是审阅方案时真正要看的东西，但放进列里会把表格撑爆。
            expandedRowRender: (task) => (
              <Descriptions size="small" column={1} style={{ margin: 0 }}>
                <Descriptions.Item label="目标">{task.objective || "—"}</Descriptions.Item>
                <Descriptions.Item label="验收标准">
                  {(task.acceptance_criteria || []).join("；") || "—"}
                </Descriptions.Item>
                <Descriptions.Item label="依赖">
                  {(task.depends_on || []).join("、") || "无"}
                </Descriptions.Item>
              </Descriptions>
            ),
          }}
          columns={[
            { title: "任务", dataIndex: "title", ellipsis: true },
            {
              title: "负责人",
              width: 160,
              render: (_, task) =>
                task.assignment?.target?.name || task.assignee || <Tag color="warning">待分配</Tag>,
            },
            {
              title: "能力",
              width: 180,
              ellipsis: true,
              render: (_, task) => (task.required_capabilities || []).join("、") || "—",
            },
            {
              title: "工时",
              width: 90,
              align: "right",
              render: (_, task) => (task.estimate?.hours ? `${task.estimate.hours}h` : "—"),
            },
            {
              title: "状态",
              width: 110,
              dataIndex: "status",
              render: (status) => {
                const meta = TASK_STATUS[status] || { text: status, color: "default" };
                return <Tag color={meta.color}>{meta.text}</Tag>;
              },
            },
          ]}
        />
      </Card>

      <Conversation flowId={flow.id} tasks={tasks} />

      <Button style={{ marginTop: 16 }} onClick={() => navigate(-1)}>
        返回
      </Button>
    </>
  );
}
