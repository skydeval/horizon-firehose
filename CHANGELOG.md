# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Dependency updates for RustSec advisories: rustls 0.23.46,
  rustls-webpki 0.103.16, webpki-roots 1.0.11, h2 0.4.20, tokio 1.54.1,
  anyhow 1.0.104.
- RUSTSEC-2026-0387 (crypto-bigint 0.5.5) is ignored in CI: it is
  reached only through proto-blue's signing code, which this project
  never calls and which holds no secrets here. It can be removed once
  proto-blue moves to newer RustCrypto crates.

## [0.2.0] - 2026-11-03

All new stream behaviour is opt-in. With the new options at their
defaults, the stream is the same as 0.1.0 apart from the `_cap` field,
and existing config files load unchanged (no `config_version` bump).

### Added

- `_cap` on every stream entry's `data`: `{"v": 1, "caps": [...]}`
  listing the enabled optional behaviours (`sync`, `resume`,
  `oplessFiltered`), plus `record_types` when the filter is non-empty.
  Also reported in `startup_metrics` as `cap`.
- `publisher.publish_sync` (default false): publish `#sync` frames as
  `sync` entries (`did`, `rev`, `time`, `_relay`) instead of skipping
  them.
- `publisher.publish_resume` (default false): publish a `resume` entry
  at each point where events were lost: an `OutdatedCursor` handled
  under `live_tip`, a dial with no saved cursor, or a relay sequence
  reset accepted under `live_tip`. Fields: `reason`, `saved_cursor`,
  `first_lost_seq`, `continues_from`, `_relay`. Resume entries have no
  stream-level `seq`, and are never published for frames the replay
  guard drops.
- `filter.forward_opless_commits` (default false): with a non-empty
  record filter, forward a commit whose ops all filter out as a commit
  with `ops: []` instead of dropping it, keeping each repo's rev chain
  whole.
- Commit events carry the frame's `since` (the rev of the repo's
  previous commit), or `null` when absent.
- Replay guard: events at or below a relay's published cursor are
  dropped before XADD. A long run of drops
  (`cursor.relay_reset_after_seconds`, default 600) is treated as a
  relay sequence reset and handled by `on_stale_cursor`.
- Public documentation under `docs/`: design, deployment guide and
  retrospective.

### Changed

- Repository reorganized: the crate, its Dockerfile, example config
  and Compose example now live in `horizon-firehose/`, which is also
  the Docker build context. Repo-level files (README, CHANGELOG,
  `docs/`, CI) stay at the root.
- Repository checkup: internal notes and local tooling state are no
  longer tracked. The root `DESIGN.md`, `DEPLOYMENT.md` and
  `RETROSPECTIVE.md` are replaced by the docs under `docs/`.

### Fixed

- Every reconnect, including the SDK's internal reconnects after a
  stall, now resumes from the current cursor. Previously they re-used
  the cursor from the original connection and could replay hours of
  history.
- A commit dropped entirely by the record filter now moves the cursor
  past it. Previously, with a narrow filter, the cursor lagged behind
  and a restart re-read those commits.

## [0.1.0] - 2026-04-20

First release.

### Added

- Firehose consumer: connects to a relay's
  `com.atproto.sync.subscribeRepos` WebSocket, decodes CBOR/CAR frames
  with proto-blue, and publishes `#commit`, `#identity`, `#account`,
  `#handle` and `#tombstone` events as JSON to a Redis stream
  (`type`, `data`, `seq` fields; `MAXLEN ~` trimming).
- Record-type filtering by NSID. An empty list publishes everything.
  Records with an unknown `$type` pass through as plain JSON.
- Per-relay cursors in Redis. A cursor is the last seq confirmed
  written, so a crash never leaves a gap. The publisher is the only
  cursor writer, and `#sync` skips travel in order with events.
- Relay failover across a primary and ordered fallbacks, with
  per-relay cooldown and a failure-count reset after 60 seconds of
  clean operation.
- Configurable policies for stale cursors, protocol error frames and
  oversized events.
- Hostile-frame defences: 5 MB frame limit, CBOR nesting-depth
  preflight, panic isolation in the decoder, and a circuit breaker
  that fails over after 10 consecutive decode failures.
- TLS through the platform's native stack by default, or rustls with
  an extra CA file (`tls_extra_ca_file`), validated at startup.
- Credential safety: relay URLs with userinfo are rejected, URLs are
  sanitised in all output, and startup metrics use a field allowlist.
- Indefinite Redis retry with backoff; Redis outages never cause an
  exit.
- Drain-first graceful shutdown on SIGTERM/SIGINT with a 30-second
  budget and a final cursor flush.
- Structured `tracing` logs (JSON or pretty) with startup, periodic
  and shutdown metrics.
- TOML config with `config_version` checking and `HORIZON_FIREHOSE_*`
  environment overrides; `HF_CONFIG_PATH` selects the file.
- `capture-fixtures` binary for recording raw frames as test fixtures.
- Distroless, non-root container image, with an example Compose file.
- CI: fmt, clippy, tests, proto-blue SHA verification, `cargo audit`,
  and a Docker build with a 50 MiB size gate and smoke test.
