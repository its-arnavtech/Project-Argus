# Multi-stage build for the Argus ingestion service (Chunk 10).
#
# Builder: rust:1.96-slim-trixie (current stable Rust on current Debian
# stable, verified against Docker Hub tags at build time -- not assumed).
# build-essential + cmake are required by aws-lc-sys, the rustls crypto
# backend the Azure SDK crates pull in (present in Cargo.lock).
#
# Runtime: debian:trixie-slim + ca-certificates + libssl3. Found
# empirically during the first Linux build: the Azure crates' dependency
# tree links BOTH rustls (AMQP path) and openssl-sys (reqwest's native-tls
# inside azure_core's HTTP path) on Linux -- on Windows the same tree used
# schannel, which is why this never surfaced locally. Image kept small on
# purpose: size drives Container Apps cold-start latency, which matters
# for scale-to-zero.

FROM rust:1.96-slim-trixie AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build

# Dependency layer cache: build a dummy main against the real manifests so
# the (slow) dependency compile only reruns when Cargo.toml/lock change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && cargo build --release --bin ingestion \
    && rm -rf src

COPY src ./src
# touch so cargo notices the real sources are newer than the dummy build
RUN touch src/main.rs src/lib.rs && cargo build --release --bin ingestion

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 argus
USER argus
WORKDIR /app
COPY --from=builder /build/target/release/ingestion /app/ingestion

# Service defaults for the deployed container (all overridable):
# stay alive after (or without) input, send to the real hub, dead-letter
# locally. IDENTITY_ENDPOINT (set by Container Apps) flips the credential
# chain to managed identity automatically -- see lib.rs::azure_credential.
ENV ARGUS_MODE=service \
    ARGUS_SINK=eventhub \
    ARGUS_DEAD_LETTER_PATH=/tmp/dead_letter.jsonl \
    ARGUS_INPUT_JSONL=/data/input.jsonl

ENTRYPOINT ["/app/ingestion"]
