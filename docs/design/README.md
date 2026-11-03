# horizon-firehose design

horizon-firehose is an ATProto firehose consumer written in Rust. It
connects to a relay's `com.atproto.sync.subscribeRepos` WebSocket,
decodes the CBOR/CAR frames into structured events, and publishes them
as JSON to a Redis stream. Any number of downstream services can then
read that stream independently through Redis consumer groups.

These pages describe the system as it is built today. They are written
for people who want to run it, change it, or understand why it behaves
the way it does.

| Page | Covers |
| --- | --- |
| [architecture.md](architecture.md) | The pipeline, its tasks and channels, backpressure, shutdown |
| [cursors-and-relays.md](cursors-and-relays.md) | Cursor semantics, relay failover, reconnects, the replay guard |
| [event-format.md](event-format.md) | The event JSON schema, Redis stream layout, metrics |
| [hardening.md](hardening.md) | Hostile-frame defences, TLS, credential handling |
| [review-history.md](review-history.md) | How the design was reviewed and what changed because of it |

Configuration and operations are covered in
[../deployment.md](../deployment.md).

## Why it exists

It was built to replace a Python firehose consumer in a running
AppView stack. That consumer published to a Redis stream, and a
separate worker read the stream and wrote to Postgres. horizon-firehose
replaces only the publishing side and keeps the stream format, so the
downstream worker does not change. That made it possible to run both
consumers side by side against the same stream before deciding on any
cutover.

It is meant to be useful to anyone running an ATProto AppView or
indexer, not only the stack it was built for.

## Goals

1. **Compatible stream output.** Events go to a Redis stream with
   `type`, `data` and `seq` fields, in a format that is a strict subset
   of what the existing downstream worker accepts.
2. **Decoding in Rust.** Frames are decoded with the
   [proto-blue](https://github.com/dollspace-gay/proto-blue) SDK's CBOR,
   CAR and repo crates. proto-blue is the project's only ATProto
   dependency.
3. **Record-type filtering.** Operators can list the NSIDs they want;
   everything else is dropped. An empty list passes everything through.
4. **No gaps after a crash.** The saved cursor means "last event
   confirmed written to Redis", never "last frame read from the
   socket".
5. **Clean shutdown.** On SIGTERM or SIGINT the pipeline drains, saves
   the cursor, and exits within a fixed budget.
6. **Structured logs.** Everything is logged through `tracing` with
   structured fields. There are no `println!` calls.
7. **One binary, small image.** A single release binary and a
   distroless container image, gated at 50 MiB in CI.
8. **Relay failover.** One primary relay plus optional fallbacks, with
   a cursor stored per relay because sequence numbers are relay-scoped.
9. **Protocol-aware errors.** Firehose error frames (such as
   `OutdatedCursor`) are handled separately from transport errors,
   each with its own configurable policy.
10. **Versioned config.** The config file carries `config_version`,
    checked at startup.
11. **A self-describing stream (0.2.0).** Every entry carries a `_cap`
    stamp saying what the consumer publishes. Optional `sync` and
    `resume` entries and op-less filtered commits let a downstream
    indexer track each repo's rev chain and every loss point exactly.
    All three are off by default, so an existing deployment's stream
    is unchanged apart from `_cap`.

## Non-goals

- **Writing to a database.** horizon-firehose publishes to Redis and
  stops there. Indexing is a downstream concern.
- **Removing Redis.** Redis is the fan-out point that lets several
  independent consumers share one firehose connection.
- **Custom lexicon handling.** Records with unknown `$type` values pass
  through as generic JSON. Typed handling of custom lexicons is future
  work.
- **A metrics endpoint.** Metrics are structured log lines. A
  Prometheus endpoint is future work.
- **Consuming several relays at once.** Failover keeps one relay
  active at a time. Parallel consumption with deduplication would need
  its own design.
- **Exiting when Redis is down.** The process would restart straight
  back into the same outage. It retries Redis indefinitely instead and
  lets backpressure hold the relay.

## Current status

- Running in production alongside the Python consumer it was built to
  replace, publishing to the same stream. The downstream worker
  deduplicates, so double publication is safe.
- CI runs fmt, clippy (warnings denied), the full test suite, a
  proto-blue SHA integrity check, `cargo audit`, and a Docker build
  with a size gate and smoke test.

Known open items:

- **Golden-file compatibility check.** The harness that compares this
  consumer's output against the Python consumer's is in place
  (`src/golden_test.rs`), but the golden files have not been generated
  yet, so that test currently skips. See
  [tests/golden/REGENERATE.md](../../tests/golden/REGENERATE.md).
- **Custom-CA TLS handshake.** Loading `tls_extra_ca_file` is
  unit-tested, but no test yet performs a full handshake against a
  server signed by a private CA.
- **Real-Redis checks.** Approximate `MAXLEN ~` trimming and cursor
  round-trips are tested against an in-memory backend; a few behaviours
  are marked in `src/backend.rs` for confirmation against real Redis.

## Future work

- A Rust replacement for the downstream Postgres worker. Running it in
  the same process would allow in-memory hand-off instead of
  JSON-over-Redis, while Redis stays for other consumers.
- Typed handling of custom lexicons.
- A Prometheus metrics endpoint built from the existing metric events.
- Consuming several relays in parallel with CID-based deduplication.
