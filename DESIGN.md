# horizon-firehose — design doc

*version 1.0 — chrys + claude, 2026-04-18*

---

## 1. context

horizon-firehose is a Rust-based ATProto firehose consumer that connects to relay WebSocket streams, decodes CBOR/CAR frames into structured events, and publishes them to a Redis stream for downstream consumers. It replaces the Python `firehose_consumer.py` in the Horizon Prism stack while preserving the existing fan-out architecture that allows multiple independent consumers (the Postgres indexer, the Hideaway block scanner, and future services) to read from the same stream.

The current pipeline is two Python services:

1. `firehose_consumer.py` — synchronous Python using the `atproto` library. Connects to `wss://bsky.network`, parses commits, extracts operations with action/path/record data, and pushes JSON-encoded events to a Redis stream (`firehose:events`) with fields `type`, `data`, and `seq`. Cursor persisted to Redis every 5 seconds.

2. `redis_consumer_worker.py` — async Python, reads from Redis via XREADGROUP under consumer group `firehose-processors`, runs 5 parallel consumer pipelines, writes to Prism's Postgres database (`atproto` DB). Throughput is approximately 1,500 events/sec.

horizon-firehose replaces service 1 only. Service 2 continues to function unchanged — it reads from the same Redis stream in the same format. This allows a zero-risk cutover: swap the firehose consumer, verify events flow correctly, and the downstream worker never knows the difference.

The service uses Doll's proto-blue SDK (`github.com/dollspace-gay/proto-blue`) for CBOR/CAR decoding, WebSocket management, and ATProto type awareness. proto-blue is a 14-crate Rust workspace covering the full ATProto protocol stack with 390 tests. Doll has confirmed it's available for use as a dependency.

This project is also a portfolio artifact for the Navigators Guild apprentice program and is designed to be reusable by anyone running an ATProto AppView — not just Horizon.

---

## 2. goals and non-goals

### goals

1. **Compatible replacement for `firehose_consumer.py`.** The Rust consumer publishes events to the same Redis stream (`firehose:events`) in a format that is a strict subset of what `redis_consumer_worker.py` accepts. Compatibility is verified by running both consumers on identical captured frames and confirming the existing worker produces identical Postgres writes.

2. **CBOR/CAR decoding in Rust via proto-blue.** Firehose frames are decoded using proto-blue's `proto-blue-lex-cbor` and `proto-blue-repo` crates instead of the Python `atproto` library. Records are deserialized into typed Rust structs before being serialized to JSON for the Redis stream.

3. **Configurable record type filtering.** Not every deployment needs every record type. The consumer accepts configuration specifying which record types to process and silently drops events for unconfigured types. This makes the tool useful beyond Horizon's specific needs.

4. **Cursor management with crash recovery.** The consumer tracks the last successfully published sequence number and persists it to Redis. On restart, it resumes from the saved cursor. Cursor semantics are "last successfully XADDed to Redis" — never "last seen from WebSocket" — so panics cannot create gaps.

5. **Graceful shutdown with cursor persistence.** On SIGTERM/SIGINT, the consumer drains channels, saves the cursor, and exits cleanly.

6. **Structured logging via `tracing`.** Every connection, reconnection, error, cursor save, and throughput metric is logged with structured fields. No `println!`, no unstructured strings.

7. **Single static binary, Docker-deployable.** Compiles to one binary with no runtime dependencies. Includes a Dockerfile for deployment alongside the existing Prism Docker Compose stack.

8. **Measurable throughput comparison.** The README documents before/after throughput numbers comparing the Python consumer to the Rust consumer on the same hardware.

9. **Optional relay failover with per-relay cursor tracking.** Support configuring one or more fallback relays. Cursors are stored per-relay because sequence numbers are relay-scoped. On failover to a relay with no prior cursor, the consumer resumes from live tip and logs the gap at WARN.

10. **Accurate protocol-level error handling.** The consumer handles firehose protocol error frames (`#info` events with `OutdatedCursor` and other error types) distinctly from WebSocket transport errors.

11. **Config schema versioning.** The config file carries a `config_version` field. The consumer validates this at startup and fails with clear migration guidance on mismatch.

### non-goals

1. **Replacing `redis_consumer_worker.py`.** The Postgres writer stays as Python for now. Replacing it is potential Phase 2 work and is explicitly out of scope for v1.

2. **Eliminating Redis.** Redis stays as the fan-out buffer. The block scanner, future Hideaway event detection, and future custom lexicon indexing all depend on being able to join the stream independently via consumer groups.

3. **Custom lexicon indexing.** Teaching the consumer about `app.nearhorizon.*` record types is a future feature, not a v1 concern. v1 handles standard `app.bsky.*` and `com.atproto.*` record types, and passes through unknown `$type` values as generic JSON.

4. **Hideaway-specific firehose consumption.** The separate Docker container subscribing to `hearth.nearhorizon.site` for Hideaway's own PDS events is a different concern. horizon-firehose targets the main relay firehose.

5. **Web dashboard or metrics endpoint.** The consumer logs metrics to stdout via tracing. Dashboard integration is a follow-up, not v1.

6. **Multi-relay simultaneous consumption with deduplication.** v1 supports failover (one active relay at a time). Consuming from multiple relays simultaneously and deduplicating events is a legitimate feature but deserves its own design pass and is out of scope for v1.

7. **Exit-on-Redis-down.** Earlier drafts proposed exiting the process if Redis was down longer than a threshold. This is explicitly rejected: the orchestrator would restart into the same outage. Instead, the consumer retries Redis indefinitely with bounded backoff and allows WebSocket backpressure to preserve state naturally.

---

## 3. architecture

### high-level flow

```
  wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos
  (or configurable relay URL — must include the XRPC path)
          │
          │  WebSocket frames (CBOR-encoded)
          ▼
  ┌─────────────────────────────────────────────┐
  │  horizon-firehose                           │
  │                                             │
  │  ┌──────────────┐   ┌─────────────────┐    │
  │  │  ws reader   │──▶│  frame decoder  │    │
  │  │  (proto-blue)│   │  (proto-blue)   │    │
  │  └──────────────┘   └─────────────────┘    │
  │          │                   │              │
  │          │ cursor             │ events      │
  │          ▼                   ▼              │
  │  ┌──────────────┐   ┌─────────────────┐    │
  │  │  cursor      │   │  record router  │    │
  │  │  persister   │   │  (filter by NSID)│   │
  │  └──────────────┘   └─────────────────┘    │
  │          │                   │              │
  └──────────┼───────────────────┼──────────────┘
             │                   │
             ▼                   ▼
        ┌────────────────────────────┐
        │        Redis               │
        │  ├─ firehose:cursor:{host} │
        │  └─ firehose:events (XADD) │
        └────────────────────────────┘
                     │
                     │  (consumed by downstream services
                     │   via XREADGROUP — unchanged)
                     ▼
        ┌────────────────────────────┐
        │  redis_consumer_worker.py  │
        │  block scanner             │
        │  future consumers          │
        └────────────────────────────┘
```

### tasks

The service runs four concurrent tokio tasks:

1. **WebSocket task** (relay supervisor). Uses `proto-blue-ws` to maintain the connection to the currently-active relay. Handles auto-reconnection with exponential backoff. Manages failover across configured relays. Pushes raw frames to an in-process bounded channel (`tokio::sync::mpsc::channel(buffer_size)`).

2. **Decoder task.** Pulls raw frames from the channel, decodes them using proto-blue's CBOR/CAR crates, extracts operations, filters by configured record types, and pushes typed events to a second bounded channel.

3. **Publisher task.** Pulls typed events from the decoder channel, serializes to JSON (matching the §4 schema), XADDs to the Redis stream, and advances the cursor tracker **only on confirmed Redis success**. It is also the **sole writer** to the in-memory cursor — see "Cursor ownership" below.

4. **Cursor persister task.** Wakes every 5 seconds, reads the current cursor from the in-memory tracker, and writes it to Redis (`firehose:cursor:{base64url(relay_url)}`). Also runs once during graceful shutdown.

Bounded channels provide backpressure. Cursor advancement tied to XADD success ensures no gaps on task panic.

### cursor ownership — publisher is the sole writer

An earlier draft let the decoder advance the in-memory cursor directly whenever a `#sync` frame arrived (rationale: `#sync` carries no record body, so there's nothing for the publisher to XADD, but the cursor still needs to move past it so replay doesn't stall forever on a `#sync`-heavy repo). Phase 8 adversarial review surfaced this as finding 1.1: the decoder runs ahead of the publisher with up to 3072 slots of channel buffering between them, so decoder-advancing the cursor on `#sync` can move it *past* commits still queued for XADD. On hard crash or graceful-shutdown-with-Redis-down, the persister then writes the drifted cursor and earlier commits are silently lost on restart.

The fix: route skip-advances as an **in-band** `PublishOp::Skip { relay, seq }` alongside `PublishOp::Publish(event, seq)` on the decoder→router→publisher channel. The publisher processes ops in channel order, so a commit queued before a skip is guaranteed to have been XADDed (or oversize-skipped) before the cursor moves past the skip's seq. This reduces the cursor-writer set to one — the publisher — so the "cursor = last fully-processed seq" invariant holds by construction rather than by careful coordination.

### drain-first shutdown (biased select!)

### drain-first shutdown (biased select!)

Tokio's `select!` is **unbiased by default**: when multiple branches are ready at the same time, it picks one at random. In our shutdown path this is dangerous — if the shutdown signal and pending channel events race, random selection will sometimes exit with unpublished events still in the channel, losing work the upstream relay has already replayed past.

Router and publisher both use `tokio::select! { biased; … }` with the input-channel receive checked **before** the shutdown watch. This makes drain-first shutdown deterministic: pending events are always processed until the channel closes; the shutdown branch is only taken when the channel is idle (or closed). This matches the §3 "Publisher task: drain its input channel, XADD all remaining events" ordering without relying on the scheduler's coin flip.

Mid-retry shutdown is a separate concern: the publisher's Redis retry loop has its own `biased` select with shutdown first, so an operator calling shutdown during a Redis outage gets an immediate bounded-time exit. The cursor invariant still holds — it reflects only XADDed events, so the in-flight event on force-exit is replayed on next start.

### backpressure — accurate description

When downstream is slower than the firehose, bounded channel backpressure causes the WebSocket reader to stop consuming frames. TCP buffers fill, and the relay will eventually disconnect the consumer as a slow reader (typically after 60-120 seconds of no reads). On reconnect, the consumer resumes from its last saved cursor, and the relay replays events from that point.

This is the correct behavior — no events are lost. The consumer will be temporarily behind live tip while catching up, which is expected operational behavior under degraded conditions.

### modules

```
horizon-firehose/
├── Cargo.toml
├── Dockerfile
├── config.example.toml
├── src/
│   ├── main.rs               # config load, task startup, shutdown coordination
│   ├── config.rs             # TOML config parsing, validation, version check
│   ├── ws_reader.rs          # relay supervisor + WebSocket task
│   ├── decoder.rs            # CBOR/CAR decode via proto-blue
│   ├── router.rs             # record type filter, NSID matching
│   ├── publisher.rs          # Redis XADD, event serialization, size limits
│   ├── cursor.rs             # per-relay cursor tracker + persister
│   ├── event.rs              # event struct, JSON serialization per schema
│   ├── metrics.rs            # throughput tracking, structured metric events
│   └── error.rs              # error types
├── bin/
│   └── capture-fixtures.rs   # raw frame capture tool for test fixtures
└── tests/
    ├── integration.rs        # end-to-end with mock WebSocket + miniredis
    ├── golden/               # pre-computed Python consumer output for compatibility checks
    └── fixtures/             # sample raw frames for decoder tests
```

### error handling

- **WebSocket disconnect.** Auto-reconnect via proto-blue-ws with exponential backoff (start 1s, cap at 60s). On reconnect, resume from the last saved cursor for the active relay. Log each attempt at INFO; escalate to WARN after 5 consecutive failures. Every reconnect logs with `total_reconnects_since_start` and `reconnects_last_hour` counters so operators can alert on their own baselines.

- **Firehose protocol error frames.** On a `#info` frame with type `OutdatedCursor`, behave per `on_stale_cursor` config. On any other error frame type, log at ERROR and reconnect based on `on_protocol_error` config (default: `reconnect_from_live_tip`).

- **CBOR/CAR decode failure.** Log at WARN with sequence number and hex dump of first 64 bytes. Increment `decode_errors` counter. Drop the frame and continue.

- **Frame types: handled vs known-skip vs unknown.** The decoder recognizes `#commit`, `#identity`, `#account`, `#handle`, `#tombstone`, `#sync`, and `#info` (plus `op == -1` error frames). `#commit`/`#identity`/`#account`/`#handle`/`#tombstone` republish to Redis. `#info` and error frames are routed by name (`OutdatedCursor` follows `on_stale_cursor`; others follow `on_protocol_error`). **Known-skip frame types** (`#sync` today, plus any future frames that are architecturally reasonable to ignore at this layer — e.g. things that carry no per-record data the downstream Postgres indexer would write) are dropped silently with INFO-level logging and a `skipped_frames` counter increment. **Genuinely unknown frame types** (new types ATProto adds that horizon-firehose doesn't yet recognize) are logged at WARN with the type name and increment an `unknown_frame_types` counter, so operators can file issues or update the consumer. This distinction prevents alert fatigue from known-uninteresting frames while ensuring new protocol additions aren't missed.

- **Unknown `$type` values.** Pass through as generic JSON with the original `$type` preserved. Never reject. Increments `unknown_type_count` metric. (Distinct from "unknown frame type" above: this is the inner record's lexicon, not the outer firehose envelope.)

- **Oversized events.** Per `max_event_size_bytes` config. On exceed, behave per `on_oversize` config (`skip_with_log` default, or `fail_hard`). Increment `oversize_events` counter.

- **Redis disconnect.** Retry indefinitely with exponential backoff (cap at 30s). Log at WARN every 30s with total downtime. Events accumulate in the publisher channel, which backpressures the WebSocket as described above. When Redis returns, drain the channel and resume. **The consumer does not exit on Redis outage** — it gracefully degrades.

- **Cursor rejection (stale).** Handled per `on_stale_cursor` config. Default `live_tip`.

- **Task panic.** `tokio::select!` in main watches all tasks. On panic, log at ERROR, attempt graceful shutdown (save cursor for active relay), and exit non-zero for orchestrator restart.

- **TLS verification.** `rustls` with system roots, verification enforced in release builds. Optional additive CA file via `tls_extra_ca_file` config. `--insecure-dev-mode` CLI flag is `#[cfg(debug_assertions)]` gated and cannot compile into release.

### relay supervisor (failover)

Per-relay state tracked: `last_failed_at`, `consecutive_failures`, `cooldown_until`, `last_cursor`.

Selection algorithm:
1. Prefer primary if not in cooldown
2. Else try fallbacks in order, skipping any in cooldown
3. If all in cooldown, use the one whose cooldown expires soonest
4. On successful connection maintaining 60 seconds of clean operation (WebSocket connected AND at least 10 events successfully published), reset that relay's failure counter

This prevents oscillation when primary is consistently broken and uses the full fallback list correctly when multiple relays are degraded.

### shutdown ordering

On SIGTERM or SIGINT:

1. Signal received, log at INFO with `shutdown_initiated` event
2. WebSocket reader task: cease reading, abort connection without waiting for close handshake (TCP RST acceptable), return control to shutdown coordinator
3. Decoder task: drain its input channel, process remaining frames, close its output channel
4. Publisher task: drain its input channel, XADD all remaining events to Redis
5. Cursor persister task: write the final cursor (which now reflects all XADDed events) to Redis
6. All tasks exit, main joins them, emit `shutdown_metrics` event, process exits zero

**Signal fan-out.** Only `ws_reader` observes the global shutdown signal directly. The downstream tasks (decoder, router, publisher, cursor persister) don't need to — they exit via their input channel closing, which only happens when the previous stage has finished draining. This gives §3's drain-first ordering "for free" without per-task shutdown logic, and keeps the coordinator's job narrow: flip one watch, then await each stage in order. The §3 "drain-first (biased select!)" subsection above explains why each stage's `tokio::select!` must prioritise input over shutdown for this to work.

Total shutdown budget: 30 seconds. If exceeded, log at ERROR and force-exit non-zero — the cursor won't be perfectly accurate but we won't hang the orchestrator.

### reconnect duplication

On reconnect, the relay replays events from the saved cursor. Some events may already be in the Redis stream. `redis_consumer_worker.py` deduplicates downstream. MAXLEN trimming handles stream size. Duplicates are transient and self-correct. No special handling in horizon-firehose is warranted; the behavior is documented as expected.

---

## 4. data model

### decoder preflight (frame size + CBOR depth)

Before invoking any recursive decoder, every inbound WebSocket frame passes two fast, allocation-free gates (both added in Phase 8.5 from adversarial-review findings 3.1 and 3.2):

1. **Byte-length gate.** Frames larger than 5 MB (the ATProto firehose spec maximum) are rejected with `DecodeError::FrameTooLarge`. `tungstenite`'s default cap is 64 MiB and `proto-blue-ws` does not currently plumb through a `WebSocketConfig` override (tracking: [proto-blue#5](https://github.com/dollspace-gay/proto-blue/issues/5)), so this is the only thing protecting peak memory from a 64 MB frame → 200–500 MB in-memory tree expansion during decode. The gate sits inside `decode_frame` so both the live pipeline and unit tests see the same enforcement.
2. **CBOR depth preflight.** A byte-level iterative scan walks DAG-CBOR length prefixes, tracking nesting depth without building any trees. Frames exceeding 64 levels of map/array nesting are rejected with `DecodeError::FrameTooDeep`. Without this gate, a sufficiently-deep hostile frame would stack-overflow one of three recursive decoders (ciborium, `cbor_to_lex`, `lex_to_json`) and abort the process via SIGABRT — not catchable by `catch_unwind`, not caught by the tokio panic handler, and resulting in a crashloop because the relay replays the same frame from the preserved cursor after orchestrator restart. `lex_to_json` carries a depth counter of its own as defense-in-depth for the case where the preflight ever lets something slip through.

Decoder task isolation: every `decode_frame` call is wrapped in `std::panic::catch_unwind(AssertUnwindSafe(...))`. Caught panics are logged at ERROR with seq-if-available and the frame's hex prefix, counter `decoder_panics_total` increments, and the task continues. Stack overflow is still not recoverable (SIGABRT bypasses unwinding) but the preflight makes that path unreachable on well-formed-CBOR-that-happens-to-be-deep.

Decoder circuit breaker: after 10 consecutive decode failures the decoder signals the ws_reader supervisor (via a shared `Notify`) to drop the current connection and run failover selection. Prevents a consistently-broken relay from pinning the consumer forever. Counter on success reset; metric `decoder_circuit_opens_total`.

### event JSON schema (authoritative)

The Rust consumer produces events conforming to this schema. Downstream consumers can rely on this contract.

```
commit_event := {
  "repo": string,              // DID, e.g. "did:plc:..."
  "commit": string,            // CID in its transmitted form (CIDv0 or CIDv1)
  "rev": string,               // TID, e.g. "3k2la..."
  "ops": [operation],
  "time": string,              // ISO 8601 UTC with ms precision, e.g. "2026-04-18T12:34:56.789Z"
  "_relay": string              // always emitted; the relay URL this event came from
}

operation := {
  "action": "create" | "update" | "delete",
  "path": string,              // "{collection}/{rkey}", e.g. "app.bsky.feed.post/3k2la..."
  "cid": string | null,        // CID in transmitted form; null for delete actions
  "record": object | null      // deserialized record; null for delete actions
}

identity_event := {
  "did": string,
  "handle": string | null,
  "_relay": string             // always emitted
}

account_event := {
  "did": string,
  "active": boolean,
  "status": string | null,
  "_relay": string             // always emitted
}
```

**Encoding rules (authoritative):**

- CIDs serialize as the multibase-base32-lower string form (e.g. `bafyrei…`), which is the canonical and only legal CIDv1 wire form for ATProto. The data-model spec blesses CIDv1 exclusively (DASL: version `0x01`, codec `dag-cbor (0x71)` or `raw (0x55)`, hash `sha-256 (0x12)`); CIDv0 is prohibited and cannot appear on the firehose. proto-blue's CBOR decoder enforces this with a hard error on non-v1 input, so any v0 byte sequence would surface as a `decode_errors` increment, not a silent normalization. Verified empirically against 79,651 CIDs across 1000 captured frames: 100% v1.
- Bytes fields serialize as lowercase hexadecimal (no `0x` prefix).
- Timestamps serialize as ISO 8601 UTC with millisecond precision.
- Nested record objects pass through as deserialized by proto-blue, preserving field order per DAG-CBOR rules.
- Null fields are emitted as JSON `null`, not omitted.
- `_relay` is always emitted regardless of single-relay or multi-relay deployment.
- Unknown `$type` records pass through as generic JSON objects, preserving `$type`.

### Redis stream format

**Stream key:** `firehose:events`

**Max length:** Configurable, default 500,000 entries (XADD with `MAXLEN ~ 500000`)

**Fields per entry:**
- `type`: string — the event type, one of `"commit"`, `"identity"`, `"account"`, `"handle"`, `"tombstone"`
- `data`: string — JSON-encoded event payload per the schema above
- `seq`: string — the sequence number from the firehose frame

**Frames that produce no Redis stream entry:**
- `#sync` frames carry a repo's current head CID without operations and are emitted by the relay for fast catch-up by indexers that need only the head pointer. The downstream Postgres indexer is record-driven and has nothing to write for them, so horizon-firehose drops them at the decoder layer with INFO-level logging — they advance the cursor but produce no `XADD`. See §3 "Frame types: handled vs known-skip vs unknown" for the broader policy.
- `#info` / error frames are control signals routed by the supervisor (cursor handling, reconnects) rather than republished as events.

**Cursor keys:** `firehose:cursor:{base64url(relay_url)}`
- Value: integer (as string), the last successfully XADDed sequence number for that relay
- One key per configured relay. Unknown relay keys can be optionally cleaned up (see config).

### MAXLEN guidance

Default MAXLEN is 500,000 entries. At typical firehose throughput (~1,500 events/sec), this represents approximately 5 minutes of buffer.

Operators should raise MAXLEN if downstream consumers may be slow or unavailable for longer periods. A startup warning logs if observed throughput would burn through the buffer in under 60 seconds.

The publisher emits an `oldest_event_age_seconds` metric so operators can observe how close they are to trim-loss.

### structured metrics schema

Three event types emitted via tracing:

**startup_metrics** (emitted once at startup):
```json
{
  "level": "info",
  "target": "horizon_firehose::metrics",
  "type": "startup_metrics",
  "config_version": 1,
  "consumer_version": "1.0.0",
  "relay_primary": "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
  "relay_fallbacks": [],
  "redis_url_host": "redis",
  "max_stream_len": 500000,
  "filter_nsid_count": 0
}
```

Config allowlist for `startup_metrics`: only fields in this list are emitted. New config additions must be explicitly added to the allowlist. Credentials and credential-adjacent fields (passwords, tokens, full Redis URLs with userinfo) are never emitted.

**periodic_metrics** (emitted every 10 seconds):
```json
{
  "level": "info",
  "target": "horizon_firehose::metrics",
  "type": "periodic_metrics",
  "window_seconds": 10,
  "events_in_window": 14983,
  "events_per_sec_in_window": 1498.3,
  "bytes_in_window": 12847291,
  "decode_errors_in_window": 2,
  "redis_errors_in_window": 0,
  "relay_reconnects_in_window": 0,
  "total_reconnects_since_start": 3,
  "reconnects_last_hour": 1,
  "unknown_type_count_in_window": 0,
  "oversize_events_in_window": 0,
  "skipped_frames_in_window": 0,
  "unknown_frame_types_in_window": 0,
  "active_relay": "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
  "channel_depths": {
    "ws_to_decoder": 0,
    "decoder_to_router": 0,
    "router_to_publisher": 3
  },
  "cursor_ages_seconds": {
    "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos": 0,
    "wss://relay.fyi/xrpc/com.atproto.sync.subscribeRepos": 42
  },
  "oldest_event_age_seconds": 42,
  "uptime_seconds": 3600
}
```

**`cursor_ages_seconds` is a per-relay object, not a scalar.** Each configured relay has an independent, relay-scoped sequence number; a multi-relay deployment with one healthy primary and a backlogged fallback would hide the fallback's staleness if we flattened to a single value. The field is keyed by the full relay URL (same key used for the cursor's Redis entry, pre-base64url). Operators with a single relay see a one-entry object. Values are whole seconds since the publisher last advanced that relay's cursor. Field name is *plural* (`cursor_ages_seconds`) to make the "multiple" shape obvious to anyone grepping for it.

**`channel_depths` reflects the three-stage pipeline** introduced by Phase 4's router: `ws_to_decoder`, `decoder_to_router`, `router_to_publisher`. An absent key (`null` in JSON) means that channel's producer has already dropped its sender — the channel is draining, not buffering.

**shutdown_metrics** (emitted once on graceful shutdown):
```json
{
  "level": "info",
  "target": "horizon_firehose::metrics",
  "type": "shutdown_metrics",
  "uptime_seconds": 86400,
  "total_events_published": 129456789,
  "total_decode_errors": 23,
  "total_redis_errors": 0,
  "total_reconnects": 17,
  "final_cursor_per_relay": {
    "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos": "987654321"
  }
}
```

### configuration (TOML)

```toml
config_version = 1

[relay]
url = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
fallbacks = []

# example multi-relay (each fallback also requires the XRPC path):
# fallbacks = [
#   "wss://relay.fyi/xrpc/com.atproto.sync.subscribeRepos",
#   "wss://relay.blacksky.app/xrpc/com.atproto.sync.subscribeRepos",
# ]

reconnect_initial_delay_ms = 1000
reconnect_max_delay_ms = 60000
failover_threshold = 5
failover_cooldown_seconds = 600
tls_extra_ca_file = ""

[redis]
url = "redis://redis:6379"
stream_key = "firehose:events"
max_stream_len = 500000
cleanup_unknown_cursors = false

[filter]
# empty = all record types pass through
record_types = []

[cursor]
save_interval_seconds = 5
on_stale_cursor = "live_tip"        # or "exit"
on_protocol_error = "reconnect_from_live_tip"  # or "exit"

[publisher]
max_event_size_bytes = 1048576       # 1MB
on_oversize = "skip_with_log"         # or "fail_hard"

[logging]
level = "info"
format = "json"                       # or "pretty"
```

**`[relay]` and `[redis]` are required sections — the consumer fails
to start if either is missing.** This prevents silent misconfiguration
where defaults could point an operator at wrong infrastructure (the
public `bsky.network` relay or a cache at `redis://redis:6379` that
isn't theirs). Other sections (`[filter]`, `[cursor]`, `[publisher]`,
`[logging]`) have universal defaults and can be omitted.

**Relay URLs must include the full XRPC endpoint path
(`/xrpc/com.atproto.sync.subscribeRepos`)** because horizon-firehose
uses a generic WebSocket client (proto-blue-ws on top of
tokio-tungstenite) that does not auto-route to the subscription path
the way higher-level ATProto libraries (e.g. Python `atproto`) do.
Sequence numbers are also relay-scoped, so the path is part of each
relay's identity for cursor-key purposes — this is consistent with the
default in `config.example.toml` and the URL hard-coded as the
`--relay-url` default in the `capture-fixtures` binary.

Config loaded at startup. Environment variables with the `HORIZON_FIREHOSE_` prefix override TOML values, using **`__` (double underscore) as the section separator** and a single underscore inside field names. Examples:

```
HORIZON_FIREHOSE_REDIS__URL=redis://prod-redis:6379
HORIZON_FIREHOSE_RELAY__URL=wss://relay.fyi
HORIZON_FIREHOSE_RELAY__RECONNECT_INITIAL_DELAY_MS=2000
HORIZON_FIREHOSE_LOGGING__LEVEL=debug
HORIZON_FIREHOSE_CONFIG_VERSION=1
```

The double-underscore convention is deliberate. Many of our field names already contain single underscores (`reconnect_initial_delay_ms`, `failover_cooldown_seconds`, `max_event_size_bytes`, `cleanup_unknown_cursors`, etc.), so a single-underscore separator would be ambiguous: `HORIZON_FIREHOSE_RELAY_RECONNECT_INITIAL_DELAY_MS` could split as `relay.reconnect.initial.delay.ms` (wrong) or `relay.reconnect_initial_delay_ms` (right). Splitting on `__` makes the intent unambiguous and matches figment's standard convention.

The path that selects which config file to load is itself controlled by `HORIZON_FIREHOSE_CONFIG` (no double underscore — this overrides the file path, not a TOML field). Default is `./config.toml`.

### config version bump policy

**Major version bump required for:** any required field added, any field removed, any field renamed, any default value changed in a way that alters existing behavior, any semantic change to an existing field.

**No bump required for:** adding optional fields with backward-compatible defaults, adding new sections.

The consumer rejects unknown top-level version numbers with a migration guide reference. It accepts `config_version` equal to or less than its compiled version. Logs at WARN if the config version is older than the consumer's current version.

---

## 5. verification criteria

### compatibility

- [ ] Event output validates against the §4 JSON schema for all captured real frames across at least 20 distinct `$type` values, at least 50 samples per type, at least 10,000 total samples
- [ ] For each distinct `$type` in the fixture set, `redis_consumer_worker.py` produces identical Postgres writes when fed the Rust consumer's output vs pre-computed golden files from the Python consumer
- [ ] Postgres diff compares row counts per table, primary key sets, and all persisted fields except timestamps reflecting processing time

**Excluded fields from Postgres diff** (fields that use `DEFAULT NOW()` or are auto-generated):
- `notifications.indexed_at`
- `notifications.id`
- `posts.indexed_at`
- `post_viewer_states.computed_at`
- Any field added in future schema with `DEFAULT NOW()`

### throughput and stability

- [ ] Sustained throughput meets or exceeds the Python consumer (~1,500 events/sec) on equivalent hardware
- [ ] Memory usage stays bounded under sustained load — no unbounded growth over 24 hours
- [ ] CPU usage is meaningfully lower than the Python consumer on equivalent hardware (documented with actual numbers)
- [ ] No event loss under sustained load (verified by comparing firehose sequence numbers against Redis entries over a bounded window)

### cursor and failover

- [ ] Cursors stored per-relay at `firehose:cursor:{base64url(relay_url)}`
- [ ] On failover to a relay with no saved cursor, consumer resumes from live tip and logs the gap at WARN
- [ ] Relay supervisor skips relays currently in cooldown
- [ ] Successful connection + 60s clean operation (connected + ≥10 events published) resets a relay's failure counter
- [ ] Cursor keys for relay URLs with non-standard ports are correctly stored, retrieved, and cleaned up
- [ ] On task panic, cursor reflects only successfully XADDed events (no gap on restart)
- [ ] The publisher is the **sole** writer to the in-memory cursor. `#sync` and other skip-advance frames travel as `PublishOp::Skip { relay, seq }` on the same channel that carries events, so cursor advancement preserves channel order. *(Phase 8.5 finding 1.1.)*
- [ ] **Timing invariant:** with `commit@100`, `skip@101`, `commit@102` queued and the backend blocked on the first XADD, the in-memory cursor remains `None`. Dropping the block and draining produces cursor = 102 with observations passing through 100 → 101 → 102 in order. Covered by `publisher::tests::skip_never_advances_cursor_past_pending_commits` and `skip_advances_cursor_in_channel_order_after_commit_succeeds`.
- [ ] Malformed cursor in Redis (non-u64 or non-UTF-8 bytes) fails startup with `BackendError::MalformedCursor` rather than silently resuming from live tip. *(Phase 8.5 finding 4.5.)*

### hostile-frame resilience *(Phase 8.5 findings 3.1, 3.2, 3.5)*

- [ ] Frames larger than 5 MB are rejected with `DecodeError::FrameTooLarge` before any CBOR decoder runs.
- [ ] Frames with CBOR nesting depth > 64 are rejected with `DecodeError::FrameTooDeep` by the preflight scan. Verified with hand-crafted fixtures for deeply-nested maps, arrays, and indefinite-length containers (DAG-CBOR disallows the latter).
- [ ] `lex_to_json` returns `DecodeError::RecordTooDeep` on deeply-nested `LexValue` trees even if the preflight scanner is bypassed.
- [ ] A panic inside `decode_frame` is isolated by `catch_unwind`: logged at ERROR, `decoder_panics_total` increments, decoder continues with the next frame. (Stack overflow is not catchable and the preflight scan blocks the paths that could cause one.)
- [ ] 10 consecutive decode failures trigger `decoder_circuit_opens_total` + a forced ws_reader failover, preventing pinning on a persistently-broken relay.

### security posture *(Phase 8.5 findings 4.4, 4.9)*

- [ ] Relay URLs with userinfo (`wss://user:pass@host`) are rejected at config load with a validation error.
- [ ] `config::sanitize_ws_url()` strips userinfo from any relay URL used in a log line, startup/shutdown metrics payload, or event `_relay` field — defense-in-depth for any future validation regression.
- [ ] No credentials, tokens, or full URLs with userinfo appear in any log output.

### operational resilience

- [ ] Under sustained downstream slowdown: consumer disconnects from relay, Redis reconnect attempts continue indefinitely, cursor is preserved, catch-up is clean when downstream recovers
- [ ] Redis outage does not cause consumer exit
- [ ] Only config errors, task panics, or explicit shutdown signals cause non-zero exit
- [ ] Graceful shutdown completes within 30 seconds, with cursor accuracy within 1 event under normal conditions
- [ ] Forced shutdown (SIGKILL) resume works correctly — at most 5 seconds of cursor drift

### protocol and edge cases

- [ ] `#info OutdatedCursor` frames handled per `on_stale_cursor` config
- [ ] Other protocol error frames handled per `on_protocol_error` config
- [ ] Consumer publishes records with unknown `$type` values as raw JSON preserving `$type`, without erroring
- [ ] Oversized events (exceeding `max_event_size_bytes`) are handled per `on_oversize` config

### security

- [ ] Release builds cannot be compiled with `--insecure-dev-mode`
- [ ] TLS certificate verification enforced in release builds against system roots
- [ ] `tls_extra_ca_file` config loads additional CA bundles additive to system roots
- [ ] `startup_metrics` only emits config fields on the explicit allowlist
- [ ] No credentials, tokens, or full URLs with userinfo appear in any log output

### configuration

- [ ] Consumer fails to start with clear error if required config is missing
- [ ] Env vars with `HORIZON_FIREHOSE_` prefix override TOML config values
- [ ] `config_version` mismatch produces clear migration guidance
- [ ] `record_types = []` passes all record types through
- [ ] `record_types = ["app.bsky.feed.post"]` publishes only post events, silently drops others

### fixture coverage

- [ ] Fixtures include at least 3 samples of each distinct `$type` observed in a 24-hour firehose window
- [ ] Fixtures include at least one sample each of: long facet lists (>10 facets), large embeds (>100KB), unicode-heavy text, null optional fields, deletion operations
- [ ] CI fails if fixture `$type` coverage drops below 20 types or 10,000 total samples
- [ ] Golden files regenerated deliberately when fixtures change; diffs surfaced for review

### deployment

- [ ] `cargo build --release` produces a single static binary under 20MB
- [ ] Dockerfile produces a working container image
- [ ] Container runs in the existing Prism Docker Compose stack alongside or replacing `python-firehose`
- [ ] `redis_consumer_worker.py` continues working without changes when pointed at the Rust consumer's output

### portfolio

- [ ] README documents throughput comparison with before/after numbers
- [ ] README documents how to use horizon-firehose outside of Horizon (generic ATProto AppView deployment)
- [ ] DESIGN.md published in repo with full adversarial review summary
- [ ] Design completed five rounds of adversarial review before v1 cut

---

## 6. build plan

### phase 0: scaffolding

1. `cargo new horizon-firehose --bin`
2. Add proto-blue as git dependency pinned to a specific tag:
   ```toml
   [dependencies]
   proto-blue-api = { git = "https://github.com/dollspace-gay/proto-blue", tag = "v0.1.0" }
   proto-blue-ws = { git = "https://github.com/dollspace-gay/proto-blue", tag = "v0.1.0" }
   proto-blue-lex-cbor = { git = "https://github.com/dollspace-gay/proto-blue", tag = "v0.1.0" }
   proto-blue-repo = { git = "https://github.com/dollspace-gay/proto-blue", tag = "v0.1.0" }
   ```
3. Add CI step to verify the resolved SHA matches an expected value (catch force-pushes and silent drift)
4. Create "Working with proto-blue" section in README describing upgrade process
5. Set up module skeleton per §3
6. Add baseline dependencies: `tokio`, `tracing`, `tracing-subscriber`, `serde`, `serde_json`, `redis`, `thiserror`, `figment`, `tokio::signal`, `rustls`, `base64`
7. Commit: "scaffolding: module layout, baseline dependencies, proto-blue pinned"

### phase 1: config and startup

1. Implement `config.rs` — TOML parsing, env var overrides, version check, validation
2. Implement `main.rs` — load config, init tracing, emit `startup_metrics`, start tasks, handle shutdown signals
3. The service starts, logs config (allowlisted fields only), and idles
4. Tests: config loading with valid, invalid, env-overridden inputs; version mismatch handling
5. Commit: "config loading, startup, shutdown handling"

### phase 2: WebSocket connection and relay supervisor

1. Implement `ws_reader.rs` — connect via proto-blue-ws, push raw frames to bounded mpsc channel
2. Implement relay supervisor with per-relay state, failover, cooldown, return-to-primary logic
3. Log connection events with structured fields including `total_reconnects_since_start` and `reconnects_last_hour`
4. Integration test: mock WebSocket server, verify connection, disconnection, reconnection, failover
5. Commit: "WebSocket connection with relay failover"

### phase 2.5: capture-fixtures tool

1. Build `bin/capture-fixtures.rs` as a minimal binary
2. Connects via proto-blue-ws, writes raw frames to disk with metadata (timestamp, sequence number)
3. Runs until stopped or N frames captured
4. Run capture over several hours to build initial fixture corpus
5. Check fixtures into `tests/fixtures/`
6. Document in README: how to capture additional fixtures as ecosystem evolves
7. Commit: "capture-fixtures tool and initial fixture corpus"

### phase 3: decoder

1. Implement `decoder.rs` — pull frames, decode CBOR/CAR via proto-blue, extract operations
2. Implement `event.rs` — typed event struct and JSON serialization matching §4 schema
3. Handle unknown `$type` pass-through
4. Tests: decode each captured fixture, verify JSON output validates against schema
5. Tests: per-`$type` coverage, edge cases (long facets, large embeds, unicode, nulls, deletions)
6. Commit: "CBOR/CAR decoding and event serialization"

### phase 4: router and publisher

1. Implement `router.rs` — filter events by configured NSIDs
2. Implement `publisher.rs` — XADD to Redis with MAXLEN trimming, size limit enforcement
3. Implement `cursor.rs` — per-relay in-memory tracker + periodic persister with base64url keys
4. Cursor advancement tied to XADD success
5. End-to-end test in isolation: mock WebSocket → decoder → router → miniredis, verify full pipeline
6. Commit: "record routing, Redis publishing, cursor persistence"

### phase 5: error handling and edge cases

1. Implement all error handling per §3
2. Implement operational safeguards (Redis indefinite retry, oversized event handling, protocol error handling)
3. Implement graceful shutdown ordering with 30s budget
4. Tests for each error path
5. Commit: "error handling and operational safeguards"

### phase 6: metrics and logging

1. Implement `metrics.rs` — periodic metrics event emission
2. Wire startup_metrics and shutdown_metrics
3. Ensure no `println!` anywhere (clippy lint)
4. Verify credential allowlist in `startup_metrics`
5. Commit: "structured metrics and logging"

### phase 7: CI with compatibility tests

1. Set up GitHub Actions workflow
2. Run Python consumer once against fixture set, capture output as golden files, check into `tests/golden/`
3. CI runs: unit tests, integration tests, schema validation against fixtures, golden file comparison
4. CI runs `cargo audit` for dependency vulnerabilities
5. CI verifies proto-blue pinned SHA matches expected
6. Commit: "CI pipeline with backwards-compatibility tests"

### phase 8: first adversarial review

1. Open fresh conversation, paste code, attack with security adversary prompt
2. Fix real findings
3. Repeat until findings are structural tightening rather than new problems
4. Document findings and responses in DESIGN.md
5. Commit: "round 1 review findings addressed"

### phase 9: deployment

1. Write Dockerfile (multi-stage build for small final image)
2. Test container in local Docker Compose setup alongside real Redis
3. Deploy to staging (or side-by-side with Python consumer in production) for comparison
4. Collect throughput and resource usage numbers
5. Commit: "Dockerfile and deployment verified"

### phase 10: second adversarial review

1. Fresh conversation, same drill, with deployment artifacts and production observations
2. Fix any remaining findings
3. Commit: "round 2 review findings addressed"

### phase 11: production cutover

1. Stop `python-firehose` in the Prism stack
2. Start `horizon-firehose` pointed at the same Redis
3. Verify `redis_consumer_worker.py` continues processing without issues
4. Observe for 24 hours
5. Commit: "production cutover complete, python-firehose retired"

### phase 12: README and portfolio

1. Write README: project description, why it exists, architecture, configuration reference, throughput comparison, deployment guide, how to use outside Horizon
2. Write retrospective section (what surprised you, what you learned, what adversarial reviews caught)
3. Final commit: "v1 documentation and retrospective"
4. Publish DESIGN.md with adversarial review summary

---

## 7. future work

**Phase 2: Replace `redis_consumer_worker.py` with a Rust worker.** At that point, an internal optimization becomes available: the firehose consumer and Rust worker could share a process and communicate via in-memory channels rather than JSON-over-Redis, eliminating serialization overhead. Redis would remain for fan-out to third-party consumers (block scanner, future Hideaway services). The v1 design is compatible with this migration — no refactor required, only addition of the Rust worker as a new task or new service.

**Custom lexicon indexing.** Teach the consumer about `app.nearhorizon.*` record types for richer typed handling of Hideaway-specific records. Currently these pass through as generic JSON, which works but loses type safety at the decoder layer.

**Prometheus metrics endpoint.** Parse the structured metrics events and expose them as scrapable Prometheus metrics for operators who want native observability stack integration.

**Multi-relay simultaneous consumption with CID-based deduplication.** Run multiple WebSocket connections in parallel, dedupe events by CID, provide redundancy beyond failover. Complex but useful for high-availability deployments.

---

## 8. adversarial review summary

Five rounds of adversarial review completed. 48 findings evaluated; 36 led to design changes, 12 withdrawn as adversary reaching. Review concluded when round 5's ratio of invented-to-real findings flipped, signaling maximum viable refinement.

**Round 1** — 12 findings, all real, structural issues surfaced. Initial design had implicit assumptions that wouldn't survive production. "Drop-in replacement" was undefined; cursor strategy contradicted itself; backpressure described a mechanism that doesn't exist; exit-on-Redis-down would cause restart oscillation. Compatibility reframed as "strict subset"; per-relay cursors became default; exit-on-Redis-down removed; backpressure rewritten; config versioning added.

**Round 2** — 12 findings, all real, refinement of round 1 fixes. CID version normalization silently changed output; fixture thresholds could be gamed; failover cooldown had concurrency hazard; `_relay` optionality was contract trap; task panic could advance cursor past published events. All resolved with explicit behavior definitions.

**Round 3** — 8 findings, tightening, no structural issues. Metrics naming confusion (`events_total` implied cumulative); capture tool ordering (chicken-and-egg dependency); unknown `$type` behavior; metrics lifecycle events; Postgres diff exclusions. All resolved with more explicit specifications.

**Round 4** — 7 findings, 6 real + 1 withdrawn. Operational edge cases: shutdown ordering, credential safety, CI enforcement, unbounded record sizes. Explicit shutdown sequence, config allowlist for startup metrics, CI compatibility tests, event size cap.

**Round 5** — 8 findings, 3 real + 5 withdrawn. Exit signal reached. Real: WebSocket close could hang during shutdown; CI depending on live Python consumer was over-engineered; `truncate_record` option was underspecified. Withdrawn: wall-clock time for fixture capture, metrics ring buffer implementation, `config_version` runtime access, retrospective contents, Cargo panic strategy. When the adversary flags things like "the retrospective section isn't pre-specified," the design has reached maximum viable refinement.

### Key design decisions surfaced by review

- **Per-relay cursors, not global.** Sequence numbers are relay-scoped. Treating them as global creates silent data loss on failover.
- **No exit on Redis down.** Exiting causes restart oscillation during outages. Graceful degradation with indefinite retry is correct.
- **Cursor semantics are "last successfully published."** Prevents data loss on publisher-task panic.
- **Unknown `$type` values pass through as generic JSON.** Keeps horizon-firehose useful for deployments that index custom lexicons.
- **Compatibility testing via golden files.** CI doesn't depend on running Python consumer live; deliberate regeneration surfaces behavior differences as reviewable diffs.
- **Graceful shutdown with abrupt WebSocket close.** TCP RST is fine; what matters is channel drain and cursor accuracy.
- **Config schema versioning.** Breaking changes require version bumps and migration guidance.

---

*This document was adversarially reviewed in five passes with 48 total findings. The design stopped moving structurally on pass three; passes four and five were tightening and operational hardening. Every decision traces to a specific finding or requirement. Every verification criterion is testable. Every configurable option has documented defaults and rationale.*