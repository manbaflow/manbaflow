FROM rust:bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml README.md LICENSE ./
COPY src ./src
RUN cargo build --release --locked

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
