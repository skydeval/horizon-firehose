# horizon-firehose

[![CI](https://github.com/skydeval/horizon-firehose/actions/workflows/ci.yml/badge.svg)](https://github.com/skydeval/horizon-firehose/actions/workflows/ci.yml)

A Rust-based ATProto firehose consumer with CBOR/CAR decoding, per-relay failover, and Redis stream publishing.

**Status:** in active development. See [DESIGN.md](DESIGN.md) for the full specification.

## Running with Docker

The [Dockerfile](Dockerfile) produces a production image on `gcr.io/distroless/cc-debian12:nonroot`: no shell, no package manager, runs as non-root (UID 65532) by default. CI gates the image at ≤ 50 MiB on every push.

### Build

```sh
docker build -t horizon-firehose:local .
```

The build is two-stage. The `rust:1.85-bookworm` builder stage produces a release binary with LTO + strip (both already set in `[profile.release]`). The distroless runtime stage carries only glibc + libssl3 (needed for native-tls, the default TLS path) plus the binary.

### Run

Mount your `config.toml` into `/app/config.toml`:

```sh
docker run --rm \
    -v "$(pwd)/config.toml:/app/config.toml:ro" \
    horizon-firehose:local
```

Override individual fields with the `HORIZON_FIREHOSE_*` env-var schema (see [config.example.toml](config.example.toml) for the full list). Common overrides:

```sh
docker run --rm \
    -v "$(pwd)/config.toml:/app/config.toml:ro" \
    -e HORIZON_FIREHOSE_REDIS__URL=redis://host.docker.internal:6379 \
    -e HORIZON_FIREHOSE_LOGGING__LEVEL=debug \
    horizon-firehose:local
```

On startup you'll see a single `startup_metrics` JSON line listing the resolved relay, fallbacks, Redis host, and whether `tls_extra_ca_file` loaded (see DESIGN.md §3 "TLS verification"). After that the binary idles on the firehose and emits one log line per reconnect / filter drop / XADD batch.

### Shutdown

`SIGTERM` and `SIGINT` drive the DESIGN.md §3 cascade with a 30 s total budget (WebSocket → decoder → publisher → cursor flush). Docker's default `docker stop` sends SIGTERM and waits 10 s before escalating to SIGKILL — bump the grace period with `--time 35` if you want the full budget:

```sh
docker stop --time 35 <container>
```

The included [examples/docker-compose.yml](examples/docker-compose.yml) sets `stop_grace_period: 35s` for the same reason.

### Docker Compose

[examples/docker-compose.yml](examples/docker-compose.yml) wires horizon-firehose against a Redis service on a private compose network. It's reference material — copy it out of `examples/` and adapt to your infrastructure (managed Redis, secret management, log shipping, etc.).

## Working with proto-blue

This project depends on [proto-blue](https://github.com/dollspace-gay/proto-blue) (Doll's comprehensive ATProto Rust SDK), currently pinned by commit SHA because the project hasn't cut tagged releases yet.

To upgrade proto-blue:
1. Find the target commit SHA from the proto-blue repository
2. Update the `rev = "..."` value for all proto-blue dependencies in `Cargo.toml`
3. Run `cargo check` to verify compatibility
4. Update `.github/expected-proto-blue-sha.txt` to the new SHA (CI's integrity check — see `.github/verify-proto-blue-sha.sh`)
5. Run the full test suite (`cargo test`) before committing the bump
6. Commit `Cargo.toml`, `Cargo.lock`, and the expected-sha file together. Note the SHA change in the commit message.

When proto-blue publishes tagged releases, this project will switch to tag-based pinning for easier tracking.

## License

MIT OR Apache-2.0
