import { useEffect, useState } from "react";
import { App, Alert, Button, Card, Empty, Space, Table, Tag, Typography } from "antd";

import { api } from "../api.js";

export default function Approvals() {
  const { message } = App.useApp();
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  async function load() {
    try {
      setData(await api.dashboard());
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    load();
  }, []);

  if (error) return <Alert type="error" message={error} showIcon />;

  const drafts = (data?.flows || []).filter((flow) => flow.status === "draft");
  const items = data?.action_items || [];

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        等我确认
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        方案生成后需经确认才会进入执行。确认后任务将按依赖顺序派发。
      </Typography.Paragraph>

      <Card size="small" title="待确认的方案" loading={!data}>
        {drafts.length === 0 ? (
          <Empty description="暂无待确认方案" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ) : (
          <Table
            size="middle"
            rowKey="id"
            dataSource={drafts}
            pagination={false}
            columns={[
              { title: "需求", dataIndex: "title" },
              { title: "提出人", dataIndex: "requester" },
              { title: "任务数", dataIndex: "total_tasks" },
              {
                title: "",
                align: "right",
                render: (_, flow) => (
                  <Button
                    type="primary"
                    size="small"
                    onClick={async () => {
                      try {
                        await api.approveFlow(flow.id);
                        message.success("已确认，进入执行");
                        load();
                      } catch (err) {
                        message.error(err.message);
                      }
                    }}
                  >
                    确认并执行
                  </Button>
                ),
              },
            ]}
          />
        )}
      </Card>

      <Card size="small" title="其他待处理事项" style={{ marginTop: 16 }}>
        {items.length === 0 ? (
          <Empty description="暂无" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ) : (
          <Table
            size="middle"
            rowKey={(row) => row.id || row.task_id}
            dataSource={items}
            pagination={false}
            columns={[
              {
                title: "紧急度",
                dataIndex: "severity",
                render: (value) => <Tag color={value === "critical" ? "red" : "orange"}>{value}</Tag>,
              },
              { title: "事项", dataIndex: "title" },
              { title: "负责人", dataIndex: "owner" },
              { title: "事由", dataIndex: "reason" },
            ]}
          />
        )}
      </Card>
    </>
  );
}
