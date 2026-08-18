import { useEffect, useState } from "react";
import { Card, Button, Input, Form, Typography, Divider, Alert, Space } from "antd";

import { api, auth } from "./api.js";

export default function LoginDialog({ onSignedIn }) {
  // 默认两种 SSO 都不显示：探测失败时至少令牌登录一定在，好过给一个
  // 点下去只返回 JSON 报错的按钮。
  const [methods, setMethods] = useState({ feishu: false, oidc: false });
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.authMethods().then(setMethods).catch(() => {});
  }, []);

  function startLogin(path) {
    window.location.assign(`${path}?return_to=${encodeURIComponent("/console")}`);
  }

  async function signIn({ token }) {
    setBusy(true);
    setError("");
    auth.token = token.trim();
    try {
      await api.me();
      onSignedIn();
    } catch (err) {
      auth.token = "";
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ display: "grid", placeItems: "center", minHeight: "100vh", background: "#f7f8fa" }}>
      <Card style={{ width: 380 }} styles={{ body: { padding: 28 } }}>
        <Typography.Title level={4} style={{ marginTop: 0 }}>
          进入 Relay
        </Typography.Title>

        {error && <Alert type="error" message={error} showIcon style={{ marginBottom: 16 }} />}

        {(methods.feishu || methods.oidc) && (
          <Space direction="vertical" style={{ width: "100%" }}>
            {methods.feishu && (
              <Button type="primary" block onClick={() => startLogin("/auth/feishu/login")}>
                用飞书登录
              </Button>
            )}
            {methods.oidc && (
              <Button block onClick={() => startLogin("/auth/oidc/login")}>
                企业 SSO
              </Button>
            )}
            <Divider plain style={{ margin: "12px 0" }}>
              或
            </Divider>
          </Space>
        )}

        <Form layout="vertical" onFinish={signIn} requiredMark={false}>
          <Form.Item
            name="token"
            label="接入令牌"
            rules={[{ required: true, message: "请填写令牌" }]}
            extra="令牌仅保存在当前标签页，适用于自动化接入。"
          >
            <Input.Password placeholder="rly_..." autoComplete="off" />
          </Form.Item>
          <Button htmlType="submit" block loading={busy}>
            用令牌进入
          </Button>
        </Form>
      </Card>
    </div>
  );
}
