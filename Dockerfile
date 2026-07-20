FROM rust:bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml README.md LICENSE ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home manbaflow \
    && install -d -m 0700 -o manbaflow -g manbaflow /var/lib/manbaflow

COPY --from=builder /build/target/release/mamba /usr/local/bin/mamba

USER manbaflow
VOLUME ["/var/lib/manbaflow"]
EXPOSE 7777
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:7777/health/ready > /dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/mamba", "--data-dir", "/var/lib/manbaflow"]
CMD ["serve", "--bind", "0.0.0.0:7777", "--allow-insecure-public-http"]
