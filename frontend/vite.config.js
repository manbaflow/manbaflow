import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// 产物直接写进 Rust 的资源目录，并且**不带内容 hash**：
// 控制面用三个 include_str! 把它们编译进二进制，文件名必须是固定的。
// 换成 hash 文件名就得引入目录级嵌入和动态路由，为这点收益不值得。
export default defineConfig({
  plugins: [react()],
  base: "/console/",
  build: {
    outDir: "../src/interfaces/web",
    emptyOutDir: false,
    rollupOptions: {
      output: {
        entryFileNames: "assets/app.js",
        chunkFileNames: "assets/app.js",
        assetFileNames: "assets/app.[ext]",
        // antd 很大，但内网部署没有 CDN 可依赖，全部打进单文件反而最省事：
        // 一次请求、可离线、不用管 chunk 之间的相对路径。
        manualChunks: undefined,
      },
    },
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:7777",
      "/auth": "http://127.0.0.1:7777",
    },
  },
});
