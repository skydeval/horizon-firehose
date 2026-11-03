# Review history

horizon-firehose went through adversarial review twice over: five
rounds against the design document before any code was written, and
two rounds against the code. Each code review ran in a fresh session
with no access to the project's history, so the reviewer had nothing
invested in earlier decisions.

## Design review: five rounds, 48 findings

36 findings led to design changes and 12 were withdrawn as reaching.
Review stopped when the share of invented findings overtook the real
ones.

| Round | Findings | Character |
| --- | --- | --- |
| 1 | 12, all real | Structural. "Drop-in replacement" was undefined; the cursor strategy contradicted itself; the backpressure description relied on a mechanism that does not exist; exiting on Redis outage would cause restart loops. |
| 2 | 12, all real | Refining round 1. CID normalisation silently changed output; fixture thresholds could be gamed; failover cooldown had a concurrency hazard; an optional `_relay` field was a contract trap; a task panic could advance the cursor past unpublished events. |
| 3 | 8 | Tightening. Metric names implied the wrong semantics; the fixture-capture tool had an ordering problem; unknown `$type` handling and metric lifecycles were underspecified. |
| 4 | 7 (6 real) | Operational edges: shutdown ordering, credential safety, CI enforcement, unbounded record sizes. |
| 5 | 8 (3 real) | WebSocket close could hang shutdown; CI depending on a live Python consumer was over-engineered; one config option was underspecified. The rest were reaching. |

Decisions that came out of it:

- **Per-relay cursors.** Seq numbers belong to a relay. A global cursor
  silently loses data on failover.
- **Never exit on Redis outage.** Retry indefinitely and let
  backpressure hold the relay.
- **Cursor means "last confirmed write".** A panic cannot open a gap.
- **Unknown `$type` passes through.** Keeps the consumer useful for
  custom lexicons.
- **Compatibility via golden files.** CI compares against recorded
  output instead of running the old consumer.
- **Abrupt WebSocket close on shutdown.** What matters is draining the
  channels and saving an accurate cursor, not a polite close handshake.
- **Versioned config.** Breaking changes need a version bump and
  migration guidance.

## Code review, round 1

Run once the pipeline, CI and metrics were in place. The fixes that
shaped the current design:

- **Cursor ownership.** The decoder could move the cursor past queued
  commits on `#sync` frames. Skips now travel in-band and the publisher
  is the only cursor writer
  ([cursors-and-relays.md](cursors-and-relays.md)).
- **Hostile frames.** Added the 5 MB size gate, the CBOR depth
  preflight, panic isolation and the decoder circuit breaker
  ([hardening.md](hardening.md)).
- **Inert custom CA.** `tls_extra_ca_file` was parsed but not used.
  It is now wired into every handshake and fails at startup if it
  cannot load.
- **Credentials and cursors.** Userinfo in relay URLs is rejected and
  stripped from output; a malformed cursor in Redis stops startup
  instead of falling back to live tip.

Several findings needed changes in proto-blue. Those were filed
upstream (silent-relay timeouts and reconnect budgets, a custom TLS
connector hook, WebSocket config pass-through) and adopted once
released, which let horizon-firehose delete its own workarounds.

## Code review, round 2

Run after the Docker image and deployment. Ten findings, two High and
eight Medium, all fixed. Examples:

- The config-path variable `HORIZON_FIREHOSE_CONFIG` collided with the
  env-override prefix and was rejected as an unknown field. It is now
  `HF_CONFIG_PATH`, outside that namespace.
- Integration tests skipped silently in CI because the fixture corpus
  was not committed. A small fixture set is now committed so they run.
- CI's audit job promised not to block merges but did not set
  `continue-on-error`; fork PRs failed for permission reasons. Both
  fixed.
- The decoder now applies `on_stale_cursor` and `on_protocol_error`
  with clear exit attribution.

## After release

The October 2026 replay incident, where SDK-internal reconnects
re-dialled a stale cursor, was not caught by either review. It is
written up in [cursors-and-relays.md](cursors-and-relays.md) along with
the per-dial cursor fix and the replay guard that followed.
