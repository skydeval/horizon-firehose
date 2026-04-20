# syntax=docker/dockerfile:1.7
#
# horizon-firehose — Rust ATProto firehose consumer.
#
# Two-stage build:
#   1. `builder` — `rust:1.95-bookworm`. Matches our dev toolchain so
#      the image compiles whatever the tree actually uses rather than
#      whatever the manifest's MSRV claim allows. (Cargo.toml claims
#      1.88 — proto-blue's effective minimum for let-chains — but we
#      develop on 1.95; pinning the image at the lower MSRV would
#      work today but drifts from dev and risks silently reintroducing
#      feature gates.) Debian 12 glibc is ABI-compatible with the
#      distroless runtime. Produces a release binary with LTO + strip
#      (profile.release in Cargo.toml already sets those).
#   2. `runtime` — `gcr.io/distroless/cc-debian12:nonroot`. Ships glibc
#      + libssl3 (needed for native-tls, the default TLS path) and a
#      baked-in UID 65532 `nonroot` user. No shell, no package manager,
#      no Rust toolchain in the final image.
#
# Target image size: under 50 MB. The two big contributors are the
# stripped horizon-firehose binary (~15–20 MB with both rustls and
# native-tls compiled in) and the distroless base (~25 MB).
#
# Build:
#   docker build -t horizon-firehose:local .
#
# Run (mount config into /app/config.toml):
#   docker run --rm \
#     -v "$(pwd)/config.toml:/app/config.toml:ro" \
#     horizon-firehose:local
#
# Signals: the binary handles SIGTERM / SIGINT via tokio::signal and
# drives the DESIGN.md §3 shutdown cascade within a 30 s budget.
# Docker's default `docker stop` sends SIGTERM, waits 10 s, then
# SIGKILLs — increase the grace with `--time 35` if you want the
# full budget.

# ─── Stage 1: builder ────────────────────────────────────────────────
FROM rust:1.95-bookworm AS builder

# native-tls (the default TLS path) links against libssl/libcrypto and
# needs headers + pkg-config at build time. Everything else the
# `rust:bookworm` image already provides (gcc, make, etc).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Layer-caching trick: compile the dep graph on its own layer before
# the real source lands. Deps only re-resolve when Cargo.toml /
# Cargo.lock change, which is far less often than src/ changes.
# We stub both bin targets (`horizon-firehose` and `capture-fixtures`)
# so cargo's manifest resolution doesn't error on missing paths.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src src/bin \
    && echo 'fn main() {}' > src/main.rs \
    && echo 'fn main() {}' > src/bin/capture-fixtures.rs \
    && cargo build --release --locked --bin horizon-firehose \
    && rm -rf src target/release/deps/horizon_firehose* \
        target/release/horizon-firehose \
        target/release/horizon-firehose.d

# Real source now. Only this layer rebuilds on a code change.
COPY src ./src
RUN cargo build --release --locked --bin horizon-firehose

# ─── Stage 2: runtime ────────────────────────────────────────────────
# `:nonroot` variant pre-sets USER to UID 65532. Use the debug-free
# image (no shell, no busybox) — if you need to poke around in a
# container, rebuild against `:debug-nonroot` temporarily.
FROM gcr.io/distroless/cc-debian12:nonroot

# OCI metadata for image registries and tooling that surface labels
# (e.g. `docker inspect`, container scanners, `skopeo`).
LABEL org.opencontainers.image.title="horizon-firehose"
LABEL org.opencontainers.image.description="Rust ATProto firehose consumer with CBOR/CAR decoding, per-relay failover, and Redis stream publishing"
LABEL org.opencontainers.image.source="https://github.com/skydeval/horizon-firehose"
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
LABEL org.opencontainers.image.authors="Chrysanthemum Heart <chrys@chrysanthemum.dev>"

WORKDIR /app

COPY --from=builder /build/target/release/horizon-firehose /app/horizon-firehose

# Config: operators mount their config.toml into /app/config.toml.
# Alternatively, set HF_CONFIG_PATH to point elsewhere. We don't
# bundle a default config — the consumer needs a real relay URL,
# Redis URL, etc. and there's no universally-correct default.
#
# No HEALTHCHECK: horizon-firehose is a stream consumer with no HTTP
# endpoint. Process-liveness (Docker / Kubernetes / systemd all
# observe it) is the signal orchestrators should rely on; the binary
# exits non-zero on unrecoverable error and restarts are expected
# (DESIGN.md §3 shutdown ordering).

# `:nonroot` already set USER to 65532:65532 — re-stated here so a
# future maintainer who flips to a non-`:nonroot` base doesn't
# silently run as root.
USER nonroot:nonroot

ENTRYPOINT ["/app/horizon-firehose"]
