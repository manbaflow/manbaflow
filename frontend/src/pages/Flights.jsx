import { useEffect, useState } from "react";
import { Alert, Card, Empty, Progress, Space, Tag, Typography } from "antd";

import { api } from "../api.js";

export default function Flights() {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    api.dashboard().then(setData).catch((err) => setError(err.message));
  }, []);

  if (error) return <Alert type="error" message={error} showIcon />;

  const flights = data?.flights || [];

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        执行与交付
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        执行器在隔离的工作副本中修改代码，不影响本地工作区。完成后提交变更供验收。
      </Typography.Paragraph>

      {flights.length === 0 ? (
        <Empty description="暂无执行中的任务" image={Empty.PRESENTED_IMAGE_SIMPLE} />
      ) : (
        <Space direction="vertical" style={{ width: "100%" }} size={10}>
          {flights.map((flight) => {
            const used = flight.fuel?.duration_used_seconds ?? 0;
            const budget = flight.fuel?.duration_budget_seconds ?? 0;
            return (
              <Card key={flight.id} size="small">
                <Space direction="vertical" style={{ width: "100%" }} size={4}>
                  <Space>
                    <Tag color={flight.status === "crashed" ? "red" : "blue"}>{flight.status}</Tag>
                    <Typography.Text strong>{flight.task_title || flight.task_id}</Typography.Text>
                    <Typography.Text type="secondary">{flight.executor}</Typography.Text>
                  </Space>
                  {budget > 0 && (
                    <Progress
                      percent={Math.min(100, Math.round((used / budget) * 100))}
                      size="small"
                      status={used > budget ? "exception" : "active"}
                      format={() => `已用 ${used}s / 上限 ${budget}s`}
                    />
                  )}
                </Space>
              </Card>
            );
          })}
        </Space>
      )}
    </>
  );
}
