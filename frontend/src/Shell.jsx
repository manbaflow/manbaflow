import { useEffect, useState } from "react";
import { Link, Outlet, useLocation, useNavigate } from "react-router-dom";
import { Layout, Menu, Button, Space, Typography, Spin } from "antd";
import {
  DashboardOutlined,
  DatabaseOutlined,
  PlusCircleOutlined,
  CheckSquareOutlined,
  NodeIndexOutlined,
  RocketOutlined,
} from "@ant-design/icons";

import { api, auth } from "./api.js";
import LoginDialog from "./LoginDialog.jsx";

const { Sider, Header, Content } = Layout;

const NAV = [
  { key: "overview", icon: <DashboardOutlined />, label: "概览" },
  { key: "repositories", icon: <DatabaseOutlined />, label: "代码仓库" },
  { key: "demands/new", icon: <PlusCircleOutlined />, label: "提需求" },
  { key: "approvals", icon: <CheckSquareOutlined />, label: "等我确认" },
  { key: "flows", icon: <NodeIndexOutlined />, label: "进行中" },
  { key: "flights", icon: <RocketOutlined />, label: "执行与交付" },
];

export default function Shell() {
  const location = useLocation();
  const navigate = useNavigate();
  const [identity, setIdentity] = useState(null);
  const [checking, setChecking] = useState(true);

  async function probe() {
    try {
      const me = await api.me();
      setIdentity(me);
    } catch {
      setIdentity(null);
    } finally {
      setChecking(false);
    }
  }

  useEffect(() => {
    probe();
  }, []);

  const selected = NAV.find((item) => location.pathname.startsWith(`/${item.key}`))?.key || "overview";

  if (checking) {
    return (
      <div style={{ display: "grid", placeItems: "center", height: "100vh" }}>
        <Spin size="large" />
      </div>
    );
  }

  if (!identity) return <LoginDialog onSignedIn={probe} />;

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Sider theme="light" width={220} style={{ borderRight: "1px solid #f0f0f0" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "18px 20px" }}>
          <div
            style={{
              width: 30,
              height: 30,
              display: "grid",
              placeItems: "center",
              borderRadius: 8,
              background: "#4f46e5",
              color: "#fff",
              fontWeight: 700,
            }}
          >
            R
          </div>
          <Typography.Text strong>Relay</Typography.Text>
        </div>
        <Menu
          mode="inline"
          selectedKeys={[selected]}
          style={{ borderInlineEnd: 0 }}
          items={NAV.map((item) => ({
            key: item.key,
            icon: item.icon,
            label: <Link to={`/${item.key}`}>{item.label}</Link>,
          }))}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            gap: 12,
            padding: "0 28px",
            background: "#fff",
            borderBottom: "1px solid #f0f0f0",
          }}
        >
          <Space>
            <Typography.Text type="secondary">{identity.name}</Typography.Text>
            <Button
              onClick={async () => {
                await api.logout();
                auth.token = "";
                setIdentity(null);
                navigate("/overview");
              }}
            >
              退出
            </Button>
          </Space>
        </Header>
        <Content style={{ padding: "24px 28px 64px", background: "#f7f8fa" }}>
          <Outlet context={{ identity }} />
        </Content>
      </Layout>
    </Layout>
  );
}
