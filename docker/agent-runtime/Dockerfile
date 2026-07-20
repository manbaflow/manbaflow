FROM node:22-bookworm-slim

ARG CODEX_VERSION=0.144.5
ARG CLAUDE_CODE_VERSION=2.1.208

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        git \
        jq \
        python3 \
        ripgrep \
    && rm -rf /var/lib/apt/lists/*

RUN npm install --global \
        "@openai/codex@${CODEX_VERSION}" \
        "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
    && npm cache clean --force \
    && codex --version \
    && claude --version

RUN useradd --uid 10001 --create-home --shell /bin/bash mamba

ENV DISABLE_AUTOUPDATER=1 \
    DISABLE_UPDATES=1

WORKDIR /workspace
USER 10001:10001
