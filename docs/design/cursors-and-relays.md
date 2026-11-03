# Cursors and relays

## What the cursor means

A firehose cursor is the sequence number (`seq`) to resume from. In
horizon-firehose the cursor for a relay is **the last seq that was
written to Redis, or deliberately skipped**. It is never the last seq
read from the socket.

That distinction is what prevents gaps. If the process crashes with
events still in its channels, the saved cursor does not cover them,
so the relay sends them again on restart.

Cursors are stored per relay, at
`firehose:cursor:{base64url-no-pad(relay_url)}`, because each relay
numbers its own sequence. Using one relay's seq against another would
silently skip or replay events. A relay with no saved cursor starts
at live tip, and the gap is logged at WARN.

A cursor value in Redis that is not a valid unsigned integer stops
startup with an error that names the key and shows how to inspect,
repair, or delete it. The consumer does not quietly fall back to live
tip.

## Cursor ownership

The publisher is the only writer of the in-memory cursor.

An earlier version let the decoder advance the cursor itself when it
saw a `#sync` frame, since those produce nothing to publish. Review
found the flaw: the decoder runs ahead of the publisher by up to two
full channels. Moving the cursor at decode time could move it past
commits that were still queued. A crash at that moment, or a shutdown
while Redis was down, would save the cursor past events that were
never written.

The fix sends skips through the same channels as events, as
`PublishOp::Skip { relay, seq }` next to `PublishOp::Publish(event,
seq)`. The publisher handles them in order, so any commit queued
before a skip has been written before the cursor moves past the skip.
Tests cover the exact case: with `commit@100`, `skip@101`,
`commit@102` queued and the first XADD blocked, the cursor stays unset,
and once unblocked it moves 100 → 101 → 102.

Cursor advances are forward-only.

## Relay selection and failover

The supervisor tracks, per relay: last failure time, consecutive
failures, cooldown expiry, and last cursor.

It picks a relay like this:

1. The primary, unless it is in cooldown.
2. Otherwise the first fallback, in config order, that is not in
   cooldown.
3. If every relay is in cooldown, the one whose cooldown ends soonest.

A relay goes into cooldown (`failover_cooldown_seconds`, default 600)
after `failover_threshold` consecutive failures (default 5). A
connection that stays up for 60 seconds and publishes at least 10
events resets that relay's failure count. This keeps the consumer from
bouncing back to a primary that is still broken.

## Reconnects inside the SDK

proto-blue's `WebSocketKeepAlive` reconnects on its own. Two settings
control it:

- **`per_recv_timeout`** (60 s). If a relay sends nothing for this
  long the SDK drops the connection and reconnects. The firehose is a
  constant stream, so a minute of silence means something is wrong.
- **`max_reconnect_attempts`** (5). After this many consecutive failed
  connection attempts, the SDK returns `ReconnectExhausted`. The
  supervisor then puts that relay straight into cooldown and moves to
  the next one.

The counter is for failed *connections*, not timeouts. A relay that
accepts connections but then sends nothing keeps reconnecting. If
what it sends is garbage, the decoder's circuit breaker (see
[hardening.md](hardening.md)) forces the same cooldown-and-failover
path.

## Every dial uses the current cursor

The SDK calls a URL function (`WebSocketKeepAlive::with_url_fn`) on the
first dial and on every internal reconnect. That function reads the
publisher's current cursor and appends `?cursor=N`. Every connection,
whether the supervisor or the SDK opened it, resumes from the last
confirmed write. Each dial is logged as `dialling relay` with its
`connect_url`.

**Why this matters (October 2026).** Originally the supervisor built
the URL once, with the cursor as it was at connect time, and gave the
SDK that fixed string. In production, two relays stalled at the same
time. The SDK's internal reconnects re-dialled the old URL, whose
cursor was 21 and 27 hours stale. Each relay replayed that whole span,
roughly 32 and 36 million events, at about 8,000 events per second.
The shared stream's length cap was far smaller than that, so the
downstream consumer kept hitting trim gaps for over an hour.

Turning the SDK's reconnects off and letting the supervisor redial was
considered and rejected. With a zero reconnect budget, the first stall
would come back as `ReconnectExhausted`, and the supervisor would put a
healthy primary into cooldown over a single stall. Passing a URL
function keeps the backoff, reconnect budget and failover behaviour
unchanged.

The regression test is
`ws_reader::tests::reconnect_after_stall_uses_latest_saved_cursor`: a
silent mock relay records each handshake's query string while the
cursor advances between dials. On the old code it sees two dials with
the same stale cursor.

## Replay guard

A second line of defence sits in the publisher. Before each XADD, an
event whose `seq` is **at or below** the cursor already published for
its relay is dropped instead of written.

- The comparison uses that relay's own cursor. A relay with no cursor
  yet is never guarded.
- Equal is dropped: the cursor is the last written seq, so an equal
  seq is already in the stream.
- There is no race, because the publisher is the only cursor writer.
- After any reconnect, a few thousand drops are normal. Frames from
  the old connection that were still in the channels overlap with the
  start of the new connection.

Observability:

- counter `replayed_events_dropped` (`replayed_events_dropped_in_window`
  in periodic metrics, `total_replayed_events_dropped` at shutdown);
- one WARN `replay_drop_started` when a run of drops begins (relay,
  first dropped seq, cursor);
- one INFO `replay_drop_ended` when the relay moves past the cursor
  (count, first and last seq, duration).

### Relay sequence resets

A relay that is rebuilt and restarts its numbering sends live events
with seqs below the saved cursor. Dropping them forever would be a
silent outage. So if drops continue for
`cursor.relay_reset_after_seconds` (default 600) without any event
moving that relay's cursor forward, the guard treats it as a reset,
counts `relay_seq_resets`, and applies `on_stale_cursor`:

- **`live_tip`**: logs WARN `relay_seq_reset`, moves the cursor back to
  just below the current seq, and resumes publishing. Events dropped
  during the window are lost, which is what `live_tip` already means
  when a relay rejects a cursor.
- **`exit`**: logs ERROR `policy_exit` with a reason that names the
  cursor key and the `redis-cli DEL` command to clear it, then runs
  the normal shutdown and exits non-zero. A relay that really did reset
  will cause the same exit after every restart until the key is
  deleted, which makes the problem visible.

A false positive needs a relay that ignores `?cursor=` and replays more
than `relay_reset_after_seconds` of history. Under `exit` the process
restarts and redials with the saved cursor; under `live_tip` the rest
of the replay is republished as duplicates. Both recover without help,
which an endless silent drop would not.

## Protocol error frames

- `#info` with `OutdatedCursor`: handled by `on_stale_cursor`
  (`live_tip` by default, or `exit`).
- Any other error frame: logged at ERROR and handled by
  `on_protocol_error` (`reconnect_from_live_tip` by default, or
  `exit`).

## Duplicates

The design favours at-least-once delivery. A crash between an XADD
and the next cursor save (at most `save_interval_seconds` of drift),
or a forced exit mid-retry, can re-publish a few events on restart.
Downstream consumers should treat writes as idempotent.
