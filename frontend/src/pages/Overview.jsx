import { useEffect, useState } from "react";
import { Card, Col, Row, Statistic, Typography, Alert, Spin, Empty } from "antd";
import { Link } from "react-router-dom";

import { api } from "../api.js";

const CARDS = [
  { key: "active_flows", title: "进行中的需求" },
  { key: "awaiting_human", title: "等你确认", tone: "#b45309" },
  { key: "blocked_tasks", title: "被阻塞的任务", tone: "#b91c1c" },
  { key: "at_risk_tasks", title: "有风险的任务", tone: "#b45309" },
  { key: "open_flights", title: "Agent 正在执行" },
  { key: "completed_tasks", title: "已完成任务", tone: "#047857" },
];

export default function Overview() {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    api.dashboard().then(setData).catch((err) => setError(err.message));
  }, []);

  if (error) return <Alert type="error" message={error} showIcon />;
  if (!data) return <Spin />;

  const metrics = data.metrics || {};

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        概览
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        这一页只回答一件事：现在有什么需要你。要动手就去左边对应的页面。
      </Typography.Paragraph>

      <Row gutter={[12, 12]}>
        {CARDS.map((card) => (
          <Col key={card.key} xs={12} md={8} xl={4}>
            <Card size="small">
              <Statistic
                title={card.title}
                value={metrics[card.key] ?? 0}
                valueStyle={metrics[card.key] ? { color: card.tone } : undefined}
              />
            </Card>
          </Col>
        ))}
      </Row>

      <Card size="small" title="最近的需求" style={{ marginTop: 16 }} extra={<Link to="/flows">全部</Link>}>
        {(data.flows || []).length === 0 ? (
          <Empty description="还没有需求，去「提需求」开一条" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ) : (
          (data.flows || []).slice(0, 5).map((flow) => (
            <div
              key={flow.id}
              style={{ display: "flex", justifyContent: "space-between", padding: "8px 0" }}
            >
              <span>{flow.title}</span>
              <Typography.Text type="secondary">
                {flow.completed_tasks}/{flow.total_tasks} · {flow.status}
              </Typography.Text>
            </div>
          ))
        )}
      </Card>
    </>
  );
}
