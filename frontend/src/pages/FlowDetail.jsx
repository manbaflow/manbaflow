import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  Alert,
  App,
  Button,
  Card,
  Col,
  Descriptions,
  Form,
  Input,
  List,
  Modal,
  Row,
  Select,
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
  // 任务级操作：授权开工、转派、上报阻塞。三个都在同一个抽屉里完成，
  // 省得为每种操作各开一页。
  const [action, setAction] = useState(null);
  const [candidates, setCandidates] = useState([]);
  const [actionForm] = Form.useForm();

  async function runAction(values) {
    const { task, kind } = action;
    try {
      if (kind === "start") {
        await api.authorizeFlight(task.id, {
          agent: values.agent,
          executor: values.executor,
        });
        message.success("已授权开工");
      } else if (kind === "reassign") {
        await api.reassignTask(task.id, { owner: values.owner, reason: values.reason });
        message.success("已转派");
      } else {
        await api.blockTask(task.id, { reason: values.reason });
        message.success("已上报阻塞");
      }
      setAction(null);
      actionForm.resetFields();
      load();
    } catch (err) {
      message.error(err.message);
    }
  }

  async function openAction(task, kind) {
    setAction({ task, kind });
    actionForm.resetFields();
    if (kind === "reassign") {
      setCandidates(await api.reassignCandidates(task.id).catch(() => []));
    } else if (kind === "start") {
      setCandidates((await api.principalsList().catch(() => [])).filter((p) => p.kind === "agent"));
    }
  }

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
            {
              title: "",
              width: 210,
              align: "right",
              render: (_, task) => (
                <Space size={4}>
                  {!draft && task.status !== "completed" && (
                    <Button size="small" type="primary" onClick={() => openAction(task, "start")}>
                      让 Agent 开工
                    </Button>
                  )}
                  <Button size="small" onClick={() => openAction(task, "reassign")}>
                    转派
                  </Button>
                  <Button size="small" danger onClick={() => openAction(task, "block")}>
                    上报阻塞
                  </Button>
                </Space>
              ),
            },
          ]}
        />
      </Card>

      <Conversation flowId={flow.id} tasks={tasks} onChanged={load} />

      <Modal
        open={Boolean(action)}
        title={
          action?.kind === "start"
            ? "授权开工"
            : action?.kind === "reassign"
              ? "转派任务"
              : "上报阻塞"
        }
        onCancel={() => setAction(null)}
        onOk={() => actionForm.submit()}
        okText="提交"
      >
        <Typography.Paragraph type="secondary">{action?.task?.title}</Typography.Paragraph>
        <Form form={actionForm} layout="vertical" onFinish={runAction}>
          {action?.kind === "start" && (
            <>
              <Form.Item name="agent" label="交给哪个执行器" rules={[{ required: true }]}>
                <Select
                  options={candidates.map((c) => ({ value: c.name, label: c.name }))}
                  placeholder="选择一个 Agent"
                />
              </Form.Item>
              <Form.Item name="executor" label="用什么执行" initialValue="claude_code">
                <Select
                  options={[
                    { value: "claude_code", label: "Claude Code" },
                    { value: "codex", label: "Codex" },
                  ]}
                />
              </Form.Item>
              <Typography.Text type="secondary">
                执行器会在隔离副本里改代码，完成后推分支并开草稿 MR。
              </Typography.Text>
            </>
          )}
          {action?.kind === "reassign" && (
            <Form.Item name="owner" label="转派给" rules={[{ required: true }]}>
              <Select
                options={(candidates || []).map((c) => ({
                  value: c.name || c.id,
                  label: c.name || c.id,
                }))}
                placeholder="选择接手的人或 Agent"
              />
            </Form.Item>
          )}
          {action?.kind !== "start" && (
            <Form.Item name="reason" label="原因" rules={[{ required: true, message: "写清楚原因" }]}>
              <Input.TextArea rows={3} placeholder="转派或阻塞的理由会记进 Ledger" />
            </Form.Item>
          )}
        </Form>
      </Modal>

      <Button style={{ marginTop: 16 }} onClick={() => navigate(-1)}>
        返回
      </Button>
    </>
  );
}
