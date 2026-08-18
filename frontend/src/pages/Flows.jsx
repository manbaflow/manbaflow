import { useEffect, useState } from "react";
import { Alert, Progress, Table, Tag, Typography } from "antd";

import { api } from "../api.js";

const HEALTH = { on_track: "green", at_risk: "orange", blocked: "red", draft: "default" };

export default function Flows() {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    api.dashboard().then(setData).catch((err) => setError(err.message));
  }, []);

  if (error) return <Alert type="error" message={error} showIcon />;

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        进行中
      </Typography.Title>
      <Typography.Paragraph type="secondary">已确认并正在执行的需求。</Typography.Paragraph>
      <Table
        size="middle"
        rowKey="id"
        loading={!data}
        dataSource={data?.flows || []}
        pagination={false}
        columns={[
          {
            title: "状态",
            dataIndex: "health",
            render: (value) => <Tag color={HEALTH[value] || "default"}>{value}</Tag>,
          },
          { title: "需求", dataIndex: "title" },
          { title: "提出人", dataIndex: "requester" },
          {
            title: "进度",
            render: (_, flow) => (
              <Progress
                percent={flow.progress_percent || 0}
                size="small"
                format={() => `${flow.completed_tasks}/${flow.total_tasks}`}
              />
            ),
          },
          { title: "最晚完成", dataIndex: "p80_finish", render: (v) => v || "—" },
        ]}
      />
    </>
  );
}
