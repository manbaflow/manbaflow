# 集群内 Worker 镜像：relay 二进制 + 模型 CLI。
#
# 为什么不能复用控制面镜像：那个只有 relay 一个二进制，故意不装 claude / codex——
# 控制面装上模型工具就等于把凭据和任意代码执行塞进管状态的进程里。
#
# 为什么不能直接用 agent-runtime：那个只有 CLI，没有 relay，跑不了 worker 子命令。
#
# 基础镜像和 CLI 版本参数化：公开构建走公网，内网 CI 用 --build-arg 换成内部源。
ARG NODE_IMAGE=docker.io/library/node:22-bookworm-slim
ARG RELAY_IMAGE=relay:latest

FROM ${RELAY_IMAGE} AS relay

FROM ${NODE_IMAGE}

ARG CODEX_VERSION=0.144.5
ARG CLAUDE_CODE_VERSION=2.1.208
ARG OCI_REVISION=unknown
LABEL org.opencontainers.image.revision=$OCI_REVISION

# 官方 node 镜像的 apt/npm 源都在境外，从国内构建慢到不可用——实测装 perl
# 一个包就花了 20 分钟。内网的基础镜像本来改过源，换成官方镜像就丢了这层优化，
# 所以这里自己参数化。境外构建把这两个 ARG 留空即可。
ARG APT_MIRROR=deb.debian.org
ARG NPM_REGISTRY=https://registry.npmjs.org/

RUN if [ "${APT_MIRROR}" != "deb.debian.org" ]; then \
        sed -i "s|deb.debian.org|${APT_MIRROR}|g; s|security.debian.org|${APT_MIRROR}|g" \
            /etc/apt/sources.list.d/debian.sources; \
    fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        bash ca-certificates curl git jq python3 ripgrep \
    && rm -rf /var/lib/apt/lists/*

RUN npm config set registry "${NPM_REGISTRY}" \
    && npm install --global \
        "@openai/codex@${CODEX_VERSION}" \
        "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
    && npm cache clean --force \
    && codex --version \
    && claude --version

COPY --from=relay /usr/local/bin/relay /usr/local/bin/relay

# 和控制面用同一个 UID：两边挂同一个卷时属主才对得上。
RUN useradd --uid 10001 --create-home --shell /bin/bash relay \
    && install -d -m 0700 -o relay -g relay /var/lib/relay

# 自动更新会在只读文件系统下失败并刷屏。
ENV DISABLE_AUTOUPDATER=1 \
    DISABLE_UPDATES=1

WORKDIR /workspace
USER 10001:10001

# 具体参数由 Deployment 给：--mode decompose、--server、--executor。
ENTRYPOINT ["/usr/local/bin/relay", "--data-dir", "/var/lib/relay/data"]
