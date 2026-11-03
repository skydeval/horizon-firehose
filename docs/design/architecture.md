# Architecture

## Data flow

```
  relay: wss://<host>/xrpc/com.atproto.sync.subscribeRepos
          │  WebSocket frames (DAG-CBOR)
          ▼
  ┌──────────────────────────────────────────────────────────┐
  │ horizon-firehose                                         │
  │                                                          │
  │  ws_reader ──▶ decoder ──▶ router ──▶ publisher ──┐      │
  │  (relay        (CBOR/CAR   (NSID      (XADD,      │      │
  │   supervisor)   decode)     filter)    cursor)    │      │
  │       ▲                                    │      │      │
  │       └──── current cursor per relay ──────┘      │      │
  │                                                   │      │
  │  cursor persister (every 5 s) ────────────────────┤      │
  │  metrics emitter (every 10 s, logs only)          │      │
  └───────────────────────────────────────────────────┼──────┘
                                                      ▼
                     Redis
                       ├─ firehose:events            (XADD, MAXLEN ~)
                       └─ firehose:cursor:{base64url(relay_url)}
                                                      │
                                                      ▼
                     downstream consumers (XREADGROUP)
```

## Tasks

Each stage is its own tokio task, joined by bounded channels of 1024
slots each.

1. **ws_reader (relay supervisor).** Holds the connection to the
   active relay through proto-blue's `WebSocketKeepAlive`, chooses
   which relay to use, handles cooldown and failover, and pushes raw
   frames downstream. See
   [cursors-and-relays.md](cursors-and-relays.md).
2. **decoder.** Checks frame size and nesting depth, decodes the
   frame, extracts operations, and turns each frame into either a
   `Publish` op (an event to write) or a `Skip` op (a seq to move the
   cursor past without writing anything, such as a `#sync` frame). It
   also routes `#info` and error frames to the configured policies.
3. **router.** Drops commit operations whose collection is not in
   `filter.record_types`. An empty list passes everything.
4. **publisher.** Serialises each event to JSON, enforces the size
   limit, applies the replay guard, XADDs to the stream, and advances
   the in-memory cursor only after Redis confirms the write. It is the
   only code that writes the cursor.
5. **cursor persister.** Every `cursor.save_interval_seconds` (default
   5) it writes each relay's in-memory cursor to Redis. It runs once
   more at shutdown.
6. **metrics emitter.** Every 10 seconds it logs a `periodic_metrics`
   line. It reads counters only and is not on the data path.

All channels between stages carry the same item type (`PublishOp`), so
skips travel in order with the events around them. This is what keeps
the cursor correct; see "Cursor ownership" in
[cursors-and-relays.md](cursors-and-relays.md).

## Backpressure

Every channel is bounded. If Redis or a downstream stage slows down,
the channels fill and the WebSocket reader stops reading. TCP buffers
then fill, and the relay eventually drops the connection as a slow
reader (typically after one to two minutes). On reconnect the consumer
resumes from its cursor and the relay replays from there.

No events are lost in this path. The consumer falls behind live tip
for a while and catches up once the slowdown clears.

## Redis outages

The publisher retries Redis indefinitely, with exponential backoff
capped at 30 seconds and a WARN log every 30 seconds that includes
total downtime. Events wait in the channels, which backpressures the
relay as described above. When Redis returns the channels drain and
publishing continues. A Redis outage never makes the process exit.

## Shutdown

On SIGTERM or SIGINT:

1. `shutdown_initiated` is logged.
2. The ws_reader stops reading and drops the connection without
   waiting for a close handshake.
3. The decoder drains its input, then closes its output.
4. The router does the same.
5. The publisher drains its input and XADDs everything left.
6. The cursor persister writes the final cursor, which now covers
   every written event.
7. `shutdown_metrics` is logged and the process exits zero.

Only the ws_reader watches the shutdown signal. Every later stage
exits when its input channel closes, which happens only after the
stage before it has finished draining. That gives drain-first ordering
without per-stage shutdown logic.

Each stage's `tokio::select!` is `biased` with the input channel
checked before the shutdown signal. Tokio's `select!` otherwise picks
at random among ready branches, which would sometimes exit with events
still queued.

The publisher's Redis retry loop is the exception: there, shutdown is
checked first, so shutting down during a Redis outage exits promptly.
The in-flight event is not lost; the cursor does not cover it, so it
is replayed on the next start.

The whole sequence has a 30-second budget. If it runs over, the
process logs at ERROR and exits non-zero. Give your orchestrator a
stop grace period of at least 35 seconds (see
[../deployment.md](../deployment.md)).

## Failures and exit codes

The process exits non-zero only for:

- invalid configuration at startup,
- a task panicking (it attempts a graceful shutdown first),
- a policy set to `exit` firing (`on_stale_cursor`,
  `on_protocol_error`, `on_oversize = "fail_hard"`),
- the shutdown budget running out.

Relay and Redis problems are retried, not treated as fatal.

## Modules

```
src/
├── main.rs            config load, task startup, shutdown coordination
├── config.rs          TOML + env parsing, validation, version check
├── ws_reader.rs       relay supervisor and WebSocket task
├── decoder.rs         frame preflight, CBOR/CAR decode, frame policies
├── router.rs          NSID filtering
├── publisher.rs       serialisation, size limit, replay guard, XADD
├── backend.rs         Redis backend and in-memory test backend
├── cursor.rs          per-relay cursor tracker and persister
├── event.rs           event types and JSON serialisation
├── metrics.rs         counters and the periodic metrics emitter
├── error.rs           error types
└── bin/
    └── capture-fixtures.rs   records raw frames for test fixtures
```
