import React from "react";
import ReactDOM from "react-dom/client";
import { ConfigProvider, App as AntApp } from "antd";
import zhCN from "antd/locale/zh_CN";
import { RouterProvider, createBrowserRouter, Navigate } from "react-router-dom";
import dayjs from "dayjs";
import "dayjs/locale/zh-cn";

import Shell from "./Shell.jsx";
import Overview from "./pages/Overview.jsx";
import Repositories from "./pages/Repositories.jsx";
import NewDemand from "./pages/NewDemand.jsx";
import Approvals from "./pages/Approvals.jsx";
import Flows from "./pages/Flows.jsx";
import Flights from "./pages/Flights.jsx";
import Agents from "./pages/Agents.jsx";
import ModelProvider from "./pages/ModelProvider.jsx";
import FlowDetail from "./pages/FlowDetail.jsx";
import Inbox from "./pages/Inbox.jsx";

dayjs.locale("zh-cn");

// 路由挂在 /console 下：nginx 把整个前缀转给控制面，控制面对未知子路径
// 也返回同一个 index.html，刷新任意页面都不会 404。
const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <Shell />,
      children: [
        { index: true, element: <Navigate to="overview" replace /> },
        { path: "overview", element: <Overview /> },
        { path: "inbox", element: <Inbox /> },
        { path: "repositories", element: <Repositories /> },
        { path: "demands/new", element: <NewDemand /> },
        { path: "approvals", element: <Approvals /> },
        { path: "flows", element: <Flows /> },
        { path: "flows/:id", element: <FlowDetail /> },
        { path: "flights", element: <Flights /> },
        { path: "agents", element: <Agents /> },
        { path: "settings/model", element: <ModelProvider /> },
      ],
    },
  ],
  { basename: "/console" },
);

const theme = {
  token: {
    colorPrimary: "#4f46e5",
    borderRadius: 8,
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "PingFang SC", "Microsoft YaHei", sans-serif',
  },
};

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <ConfigProvider locale={zhCN} theme={theme}>
      <AntApp>
        <RouterProvider router={router} />
      </AntApp>
    </ConfigProvider>
  </React.StrictMode>,
);
