# Event format

This is the contract downstream consumers can rely on.

## Redis stream

- **Key:** `redis.stream_key`, default `firehose:events`.
- **Length cap:** `XADD … MAXLEN ~ redis.max_stream_len`, default
  500,000. The trim is approximate, so the stream can sit slightly
  above the cap.
- **Fields per entry:**
  - `type`: one of `commit`, `identity`, `account`, `handle`,
    `tombstone`, and with the options below, `sync` and `resume`
  - `data`: the event as a JSON string (schema below)
  - `seq`: the frame's sequence number, as a string. **Absent on
    `resume` entries**, which are not firehose frames, so per-relay
    seq tracking never sees a duplicate.

At around 1,500 events per second, 500,000 entries is roughly five
minutes of history. If your consumers can fall further behind than
that, raise `max_stream_len`. The `oldest_event_age_seconds` metric
shows how close the stream is to trimming events nobody has read.

## Frames

| Frame | Result |
| --- | --- |
| `#commit`, `#identity`, `#account`, `#handle`, `#tombstone` | Published as an event |
| `#sync` | With `publisher.publish_sync`: published as a `sync` event. Otherwise a known skip: no stream entry, cursor still advances, `skipped_frames` counter |
| `#info`, error frames (`op == -1`) | Control signals; handled by the cursor and protocol-error policies |
| Anything else | Unknown: logged at WARN with its name, `unknown_frame_types` counter |

The split between "known skip" and "unknown" keeps routine frames out
of the warning logs while still surfacing new frame types the
protocol adds.

## Event JSON

```
commit_event := {
  "repo":   string,          // DID
  "commit": string,          // commit CID
  "rev":    string,          // TID
  "since":  string | null,   // rev of the repo's previous commit; null if absent
  "ops":    [operation],
  "time":   string,          // ISO 8601 UTC, millisecond precision
  "_relay": string           // relay URL this event came from
}

operation := {
  "action": "create" | "update" | "delete",
  "path":   string,          // "{collection}/{rkey}"
  "cid":    string | null,   // null for deletes
  "record": object | null    // decoded record; null for deletes
}

identity_event  := { "did": string, "handle": string | null, "_relay": string }
account_event   := { "did": string, "active": boolean, "status": string | null, "_relay": string }
handle_event    := { "did": string, "handle": string, "_relay": string }
tombstone_event := { "did": string, "_relay": string }

// publisher.publish_sync
sync_event      := { "did": string, "rev": string, "time": string | null, "_relay": string }

// publisher.publish_resume; no stream-level seq
resume_event := {
  "reason":         "outdated_cursor" | "no_saved_cursor" | "relay_seq_reset",
  "saved_cursor":   integer | null,  // relay's cursor before the loss; null if none
  "first_lost_seq": integer | null,  // first seq not published; null if unknown
  "continues_from": integer,         // seq of the first event after the loss
  "_relay":         string
}

// on every entry above, as the last key
"_cap": { "v": 1, "caps": [string], "record_types": [string] }
```

### `_cap`

Every entry's `data` ends with `_cap`, which describes what this
consumer puts on the stream:

- `v`: the stamp's format version, currently 1.
- `caps`: the optional behaviours that are on, in this order:
  `"sync"` (`publisher.publish_sync`), `"resume"`
  (`publisher.publish_resume`), `"oplessFiltered"`
  (`filter.forward_opless_commits`). Empty when none are.
- `record_types`: the filter's NSIDs, present only when the filter is
  non-empty.

Because every entry carries it, a reader can tell from any single
entry what to expect. An entry with no `_cap` comes from a version
before 0.2.0. A capability missing from `caps` means that behaviour is
off.

### `resume`

A `resume` entry marks a point where events on one relay were not
published. Events with seqs in `[first_lost_seq, continues_from)` are
missing. It is published only at real loss points:

| `reason` | When | `saved_cursor` / `first_lost_seq` |
| --- | --- | --- |
| `outdated_cursor` | The relay rejected the cursor (`OutdatedCursor`) under `on_stale_cursor = "live_tip"` and continued from its oldest retained event | The cursor, and cursor + 1 |
| `no_saved_cursor` | A dial went out with no saved cursor, so the relay started at its live tip (first start, or failover to a relay with no cursor yet) | Usually null. Not null if events from an earlier connection were published before this dial's first event |
| `relay_seq_reset` | The replay guard accepted a relay sequence reset under `live_tip` (see [cursors-and-relays.md](cursors-and-relays.md#relay-sequence-resets)) | The old cursor, and the first seq dropped during the reset window |

The first two are held until the first event (or `#sync` / filtered
commit) on that relay that moves its cursor forward, and published
just before it. Frames at or below the cursor, such as the overlap
after an ordinary reconnect, are dropped by the replay guard and never
trigger one. If that first event is the very next seq after the
cursor, nothing was lost and no `resume` is published.

A `resume` entry does not move the cursor; the event after it does.
After a crash between the two, the same loss is reported again on
restart. A `no_saved_cursor` resume can also repeat while a relay is
being redialled before anything has been published. Treat repeats as
the same loss.

Encoding rules:

- CIDs are written as base32-lower multibase strings (`bafyrei…`).
  ATProto allows only CIDv1 on the wire; proto-blue rejects anything
  else, which shows up as a decode error rather than a silent
  conversion. A sample of 79,651 CIDs from 1,000 captured frames was
  100% CIDv1.
- Byte fields are lowercase hex with no `0x` prefix.
- Null fields are written as `null`, never omitted.
- `_relay` is always present, even with a single relay. Credentials
  are stripped from it (see [hardening.md](hardening.md)).
- Records keep their DAG-CBOR field order.
- Records with an unknown `$type` pass through as plain JSON with
  `$type` preserved, and increment `unknown_type_count`. They are never
  rejected.

### Size limit

Events larger than `publisher.max_event_size_bytes` (default 1 MiB)
are handled by `publisher.on_oversize`: `skip_with_log` (default)
drops the event, logs at WARN and moves the cursor past it;
`fail_hard` exits non-zero. Either way `oversize_events` is counted.

## Filtering

`filter.record_types` is a list of NSIDs such as
`["app.bsky.feed.post"]`. Commit operations in other collections are
dropped. An empty list publishes everything. Non-record events
(identity, account, handle, tombstone, sync) always pass.

A commit whose operations all filter out is dropped, and its seq still
moves the cursor. With `filter.forward_opless_commits`, it is
published instead as an ordinary `commit` entry with `"ops": []`
(`repo`, `commit`, `rev`, `since` and `time` intact), so a consumer
following each repo's `since` → `rev` chain sees every link.

## Metrics

Metrics are structured log lines under the target
`horizon_firehose::metrics`, identified by an `event_type` field.

**`startup_metrics`** (once). Only fields on a fixed allowlist are
included, so new config fields cannot leak into logs by accident:
`config_version`, `consumer_version`, `relay_primary`,
`relay_fallbacks`, `redis_url_host` (host only, never the full URL),
`max_stream_len`, `filter_nsid_count`, `tls_custom_ca_loaded`, and
`cap` (the `_cap` stamp every entry will carry).

**`resume_published`** (INFO, once per `resume` entry): `relay`,
`reason`, `saved_cursor`, `first_lost_seq`, `continues_from`.

**`periodic_metrics`** (every 10 seconds):

| Field | Meaning |
| --- | --- |
| `window_seconds` | Length of the window |
| `events_in_window`, `events_per_sec_in_window`, `bytes_in_window` | Throughput |
| `decode_errors_in_window`, `redis_errors_in_window` | Errors |
| `relay_reconnects_in_window`, `total_reconnects_since_start`, `reconnects_last_hour` | Connection churn |
| `unknown_type_count_in_window`, `unknown_frame_types_in_window`, `skipped_frames_in_window`, `oversize_events_in_window` | Frame handling |
| `replayed_events_dropped_in_window`, `relay_seq_resets_in_window` | Replay guard |
| `active_relay` | Relay in use |
| `channel_depths` | Items queued in `ws_to_decoder`, `decoder_to_router`, `router_to_publisher`; `null` means that channel is draining after its producer exited |
| `cursor_ages_seconds` | Per relay: seconds since its cursor last moved. An object, not one number, so a stale fallback is not hidden behind a healthy primary |
| `oldest_event_age_seconds` | Age of the oldest entry in the stream; `null` if empty or unreadable |
| `redis_healthy` | Whether the last Redis read succeeded |
| `uptime_seconds` | Process uptime |

**`shutdown_metrics`** (once, on clean shutdown): `uptime_seconds`,
`total_events_published`, `total_decode_errors`, `total_redis_errors`,
`total_reconnects`, `total_replayed_events_dropped`,
`total_relay_seq_resets`, `final_cursor_per_relay`.

Other counters that appear in logs: `decoder_panics_total` and
`decoder_circuit_opens_total` (see [hardening.md](hardening.md)).
