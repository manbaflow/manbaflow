import { useEffect, useState } from "react";
import { Alert, App, Button, Card, Descriptions, Form, Input, Select, Tag, Typography } from "antd";

import { api } from "../api.js";

export default function ModelProvider() {
  const { message, modal } = App.useApp();
  const [current, setCurrent] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [form] = Form.useForm();

  async function load() {
    try {
      setCurrent(await api.myProvider());
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function save(values) {
    setBusy(true);
    setError("");
    try {
      await api.setMyProvider({
        provider: values.provider,
        api_key: values.api_key.trim(),
        ...(values.base_url?.trim() ? { base_url: values.base_url.trim() } : {}),
        ...(values.model?.trim() ? { model: values.model.trim() } : {}),
      });
      message.success("已保存");
      form.resetFields(["api_key"]);
      load();
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <Typography.Title level={4} style={{ marginTop: 0 }}>
        我的模型配置
      </Typography.Title>
      <Typography.Paragraph type="secondary">
        你提的需求由 Worker 用<strong>你自己的</strong> Key 去拆解，费用记在你头上。
        没配的话，Worker 会用它自己环境里的凭据——本机跑的 Worker 通常已经登录过。
      </Typography.Paragraph>

      {error && <Alert type="error" message={error} showIcon style={{ marginBottom: 12 }} />}

      <Card size="small" title="当前配置" style={{ marginBottom: 16 }}>
        {current?.configured ? (
          <>
            <Descriptions size="small" column={2}>
              <Descriptions.Item label="服务商">{current.provider}</Descriptions.Item>
              <Descriptions.Item label="模型">{current.model || "默认"}</Descriptions.Item>
              <Descriptions.Item label="baseURL">{current.base_url || "官方地址"}</Descriptions.Item>
              <Descriptions.Item label="API Key">
                <Tag>{current.api_key_hint}</Tag>
              </Descriptions.Item>
            </Descriptions>
            <Button
              danger
              size="small"
              style={{ marginTop: 12 }}
              onClick={() =>
                modal.confirm({
                  title: "删除我的模型配置？",
                  content: "删除后你提的需求会改用 Worker 自身环境里的凭据。",
                  onOk: async () => {
                    await api.deleteMyProvider();
                    message.success("已删除");
                    load();
                  },
                })
              }
            >
              删除
            </Button>
          </>
        ) : (
          <Typography.Text type="secondary">还没配置</Typography.Text>
        )}
      </Card>

      <Card size="small" title={current?.configured ? "替换配置" : "新增配置"}>
        <Form
          form={form}
          layout="vertical"
          onFinish={save}
          initialValues={{ provider: "anthropic" }}
        >
          <Form.Item name="provider" label="服务商">
            <Select
              options={[
                { value: "anthropic", label: "Anthropic（Claude Code）" },
                { value: "openai", label: "OpenAI（Codex）" },
              ]}
            />
          </Form.Item>
          <Form.Item
            name="base_url"
            label="baseURL（可选）"
            extra="留空用官方地址。中转或自建网关填在这里。"
          >
            <Input placeholder="https://api.example.com/v1" />
          </Form.Item>
          <Form.Item name="model" label="模型（可选）" extra="留空用执行器默认模型">
            <Input placeholder="claude-opus-5" />
          </Form.Item>
          <Form.Item
            name="api_key"
            label="API Key"
            rules={[{ required: true, message: "填一个 Key" }]}
            extra="保存后只能看到掩码，看不到原文。要换就重新填一次。"
          >
            <Input.Password placeholder="sk-..." autoComplete="off" />
          </Form.Item>
          <Button type="primary" htmlType="submit" loading={busy}>
            保存
          </Button>
        </Form>
      </Card>
    </>
  );
}
