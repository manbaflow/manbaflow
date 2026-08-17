import { useEffect, useState } from "react";
import { App, Button, Card, Form, Input, Space, Table, Tag, Typography, Alert } from "antd";

import { api } from "../api.js";

export default function Repositories() {
  const { message } = App.useApp();
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  // 失败原因就显示在表单旁边，不再丢到页面顶部去——之前登记失败什么都看不到。
  const [error, setError] = useState("");
  const [form] = Form.useForm();

  async function load() {
    setLoading(true);
    try {
      setRows(await api.repositories());
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function register(values) {
    setSubmitting(true);
    setError("");
    try {
      const repository = await api.registerRepository({
        gitlab_project_path: values.gitlab_project_path.trim(),
        ...(values.name?.trim() ? { name: values.name.trim() } : {}),
      });
      message.success(`已登记 ${repository.name}`);
      form.resetFields();
      load();
    } catch (err) {
      setError(err.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        代码仓库
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        先把要开发的项目登记进来，提需求时才能选。登记时会连到 GitLab 校验项目是否存在、凭据是否有权限，
        所以路径要写完整的 <code>group/project</code>。
      </Typography.Paragraph>

      <Card size="small" style={{ marginBottom: 16 }}>
        {error && (
          <Alert type="error" message={error} showIcon closable style={{ marginBottom: 12 }} />
        )}
        <Form form={form} layout="inline" onFinish={register}>
          <Form.Item
            name="gitlab_project_path"
            label="GitLab 项目路径"
            rules={[{ required: true, message: "例如 acme/web-app" }]}
          >
            <Input placeholder="acme/web-app" style={{ width: 260 }} />
          </Form.Item>
          <Form.Item name="name" label="显示名称">
            <Input placeholder="默认取路径最后一段" style={{ width: 200 }} />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" loading={submitting}>
              登记仓库
            </Button>
          </Form.Item>
        </Form>
      </Card>

      <Table
        size="middle"
        rowKey="id"
        loading={loading}
        dataSource={rows}
        pagination={false}
        columns={[
          { title: "名称", dataIndex: "name" },
          { title: "GitLab 项目", dataIndex: "gitlab_project_path" },
          { title: "默认分支", dataIndex: "default_branch" },
          {
            title: "状态",
            dataIndex: "active",
            render: (active) =>
              active ? <Tag color="green">在用</Tag> : <Tag>已归档</Tag>,
          },
          {
            title: "",
            align: "right",
            render: (_, row) =>
              row.active ? (
                <Space>
                  <Button
                    size="small"
                    onClick={async () => {
                      try {
                        await api.archiveRepository(row.id);
                        message.success("已归档");
                        load();
                      } catch (err) {
                        message.error(err.message);
                      }
                    }}
                  >
                    归档
                  </Button>
                </Space>
              ) : null,
          },
        ]}
      />
    </>
  );
}
