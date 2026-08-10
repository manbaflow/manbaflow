FROM rust:bookworm AS builder

# 国内直连 crates.io 会卡到不可用，默认走阿里云镜像。
# 在国外构建时用 --build-arg CARGO_MIRROR=sparse+https://index.crates.io/ 关掉。
ARG CARGO_MIRROR=sparse+https://mirrors.aliyun.com/crates.io-index/
RUN mkdir -p "$CARGO_HOME" && printf '%s\n' \
        '[source.crates-io]' \
        'replace-with = "mirror"' \
        '[source.mirror]' \
        "registry = \"${CARGO_MIRROR}\"" \
        > "$CARGO_HOME/config.toml"

WORKDIR /build
# 不复制 rust-toolchain.toml：它写的是 channel = "stable"，而镜像里的工具链按
# 版本号命名，rustup 会为了满足 "stable" 再下载一整套。容器里用镜像自带的即可。
COPY Cargo.toml Cargo.lock README.md LICENSE ./
RUN cargo fetch --locked
COPY src ./src
RUN cargo build --release --locked --offline

FROM debian:bookworm-slim

# 国内直连 deb.debian.org 极慢，默认走阿里云镜像。
# 在国外构建时用 --build-arg APT_MIRROR=deb.debian.org 关掉。
ARG APT_MIRROR=mirrors.aliyun.com
RUN sed -i "s|deb.debian.org|${APT_MIRROR}|g; s|security.debian.org|${APT_MIRROR}|g" \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home relay \
    && install -d -m 0700 -o relay -g relay /var/lib/relay

COPY --from=builder /build/target/release/relay /usr/local/bin/relay

USER relay
VOLUME ["/var/lib/relay"]
EXPOSE 7777
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:7777/health/ready > /dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/relay", "--data-dir", "/var/lib/relay"]
CMD ["serve", "--bind", "0.0.0.0:7777", "--allow-insecure-public-http"]
