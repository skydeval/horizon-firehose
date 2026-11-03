# Deploying horizon-firehose

horizon-firehose is a single process. It needs network access to an
ATProto relay and to a Redis instance, and nothing else: no database,
no HTTP port, no disk beyond its config file.

## Requirements

- **Redis** reachable from the consumer. Redis 7 is what the example
  uses. Persistence (AOF or RDB) is recommended so cursors survive a
  Redis restart.
- **A relay** firehose endpoint, for example
  `wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos`.
- **Docker** (recommended), or a Rust toolchain at 1.88 or newer to
  build from source.
- **Memory:** typical use is well under 512 MB. The example limits are
  512 MB reserved and 2 GB maximum.
- **Build memory:** the release build uses LTO; give the Docker builder
  about 4 GB.

## 1. Write the config

Copy [config.example.toml](../horizon-firehose/config.example.toml) to `config.toml`
and edit it. Every field is documented in that file. The ones you will
usually change:

```toml
config_version = 1

[relay]
url = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
fallbacks = []

[redis]
url = "redis://redis:6379"
stream_key = "firehose:events"
max_stream_len = 500000

[filter]
record_types = []          # empty = publish every record type

[logging]
level = "info"
format = "json"            # "pretty" for local development
```

Things to know:

- `[relay]` and `[redis]` are required. The process will not start
  without them.
- Relay URLs, including fallbacks, must end in
  `/xrpc/com.atproto.sync.subscribeRepos`. The path is not added for
  you.
- Relay URLs must not contain credentials (`user:pass@`). They are
  rejected at startup.
- Size `max_stream_len` for your slowest consumer. 500,000 entries is
  about five minutes at typical firehose rates. If a consumer can be
  down longer than that, raise it or that consumer will miss events.

### Optional stream entries

Three options add to what goes on the stream. All are off by default.
Turn them on only once every consumer of the stream understands them:
a reader that rejects unknown entry types will fail on `sync` and
`resume`.

```toml
[filter]
forward_opless_commits = false   # fully filtered commits as commits with ops: []

[publisher]
publish_sync = false             # #sync frames as "sync" entries
publish_resume = false           # a "resume" entry at each loss point
```

Every entry also carries a `_cap` field listing which of these are on.
It needs no setting. See
[design/event-format.md](design/event-format.md#_cap) for the formats.

### Environment overrides

Any field can be overridden with `HORIZON_FIREHOSE_` plus the section
and field, using a **double** underscore between section and field:

```sh
HORIZON_FIREHOSE_REDIS__URL=redis://prod-redis:6379
HORIZON_FIREHOSE_RELAY__RECONNECT_INITIAL_DELAY_MS=2000
HORIZON_FIREHOSE_LOGGING__LEVEL=debug
HORIZON_FIREHOSE_CONFIG_VERSION=1        # top-level field, no __
```

The config file path is set separately with `HF_CONFIG_PATH` (default
`./config.toml`; `/app/config.toml` in the container).

Use environment variables for anything secret, such as a Redis URL
with a password, rather than writing it into the TOML.

## 2. Run it

### Docker

```sh
docker build -t horizon-firehose:local horizon-firehose

docker run -d --name horizon-firehose \
    --restart unless-stopped \
    --stop-timeout 35 \
    -v "$(pwd)/config.toml:/app/config.toml:ro" \
    horizon-firehose:local
```

The first build takes several minutes. Later builds reuse the cached
dependency layer unless `Cargo.toml` or `Cargo.lock` changes.

The image is distroless and runs as non-root (UID 65532). It has no
shell, so `docker exec` into it will not work; use logs for
diagnostics.

### Docker Compose

[examples/docker-compose.yml](../horizon-firehose/examples/docker-compose.yml)
runs the consumer next to a Redis service on a private network. Copy it
out of `horizon-firehose/examples/` and adapt it. The parts that matter for any compose setup:

```yaml
  horizon-firehose:
    build: { context: ./horizon-firehose }   # path to the crate folder
    volumes:
      - ./config.toml:/app/config.toml:ro
    depends_on:
      - redis
    restart: unless-stopped
    stop_grace_period: 35s
```

The consumer depends only on Redis. It does not need to wait for any
downstream service.

### From source

The crate lives in the `horizon-firehose/` folder of the repository.

```sh
cargo build --release --locked --manifest-path horizon-firehose/Cargo.toml --bin horizon-firehose
HF_CONFIG_PATH=./config.toml ./horizon-firehose/target/release/horizon-firehose
```

## 3. Check it started

With `format = "json"`, look for these in order:

1. `startup_metrics`: the resolved relays, Redis host, and whether a
   custom CA loaded.
2. `connecting to relay` / `dialling relay`, then `relay_connected`.
3. `pipeline_started`.
4. `periodic_metrics` every 10 seconds, with `events_per_sec_in_window`
   above zero.

Then confirm entries are arriving:

```sh
redis-cli XLEN firehose:events
redis-cli XREVRANGE firehose:events + - COUNT 1
```

## Shutdown and restarts

SIGTERM or SIGINT starts a drain: the consumer stops reading, writes
everything already queued, saves the cursor, and exits. It allows
itself 30 seconds. Docker's default stop timeout is 10 seconds, so set
it to 35 (`--stop-timeout 35`, `docker stop --time 35`, or
`stop_grace_period: 35s`), or the drain may be cut short. A cut-short
drain does not lose events; it just means a few are published twice
after restart.

There is no health-check endpoint. The process exits non-zero on
anything it cannot recover from, so let your orchestrator restart it
(`restart: unless-stopped` or equivalent). Relay and Redis outages are
retried internally and do not cause exits.

## Cursors

Each relay's cursor is stored in Redis at
`firehose:cursor:<base64url of the relay URL, no padding>`:

```sh
redis-cli KEYS 'firehose:cursor:*'

URL='wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos'
KEY="firehose:cursor:$(printf '%s' "$URL" | base64 -w0 | tr '+/' '-_' | tr -d '=')"
redis-cli GET "$KEY"
```

On restart the consumer resumes from that seq. Deleting the key makes
it start from the relay's live tip, accepting a gap. If a stored cursor
is not a valid number, startup fails with instructions to inspect,
repair or delete it.

## Running beside an existing consumer

You can run horizon-firehose next to another firehose publisher on the
same stream, then retire the old one later. This is safe only if your
downstream consumers handle duplicate events (for example with
`ON CONFLICT` upserts), because every event will arrive twice.

During the overlap, compare event rates between the two publishers,
watch your consumers' error rates and write rates, and keep an eye on
`oldest_event_age_seconds` since the stream fills twice as fast.

To cut over, stop the old publisher and watch your consumers for an
hour. Rates should stay about the same. If anything looks wrong,
start the old publisher again; nothing is lost.

## Metrics to watch

All metrics are fields on the `periodic_metrics` log line. See
[design/event-format.md](design/event-format.md#metrics) for the full
list.

| Field | Healthy | Investigate when |
| --- | --- | --- |
| `events_per_sec_in_window` | Steady, roughly 1,000–2,000 on the main network | Drops to zero |
| `cursor_ages_seconds` (active relay) | A few seconds | Keeps growing |
| `channel_depths` | Near zero | Pinned near 1024: something downstream is slow |
| `redis_errors_in_window`, `redis_healthy` | 0, true | Any sustained errors |
| `relay_reconnects_in_window` | 0 | Repeated reconnects |
| `decode_errors_in_window` | 0 or close | A steady stream of errors |
| `oldest_event_age_seconds` | Comfortably above your slowest consumer's lag | Approaching that lag |
| `replayed_events_dropped_in_window` | 0, or a burst after a reconnect | Sustained for minutes |

## Troubleshooting

**Exits immediately at startup.** Read the error line. The usual causes
are a missing `[relay]` or `[redis]` section, an unknown field, a relay
URL without the XRPC path, a `config_version` newer than the binary, or
a `tls_extra_ca_file` that cannot be loaded.

**Running but publishing nothing.** Check the relay URL includes
`/xrpc/com.atproto.sync.subscribeRepos`. Check `filter.record_types`
isn't excluding everything you expect. Check `redis_healthy`.

**Cursor not advancing.** Look at `relay_reconnects_in_window`. A relay
that keeps dropping the connection, or that sends frames that fail to
decode, will be put in cooldown and replaced by a fallback if you have
one configured.

**Many replayed events dropped.** A few thousand right after a
reconnect is normal. If it lasts, the relay is replaying history; look
for `replay_drop_started` and `replay_drop_ended`. If drops continue for
`cursor.relay_reset_after_seconds` (default 600), the consumer assumes
the relay reset its numbering:

- under `on_stale_cursor = "live_tip"` it logs `relay_seq_reset` and
  continues from the relay's new numbering;
- under `"exit"` it logs `policy_exit` and exits. The message names the
  cursor key; delete it to resume from live tip.

**Stream keeps hitting its length cap and consumers miss events.**
Raise `redis.max_stream_len`, or find out why consumers are behind.

**Image build fails.** Usually the builder ran out of memory during the
LTO link; give it about 4 GB. Build with `--locked` so the pinned
dependency versions are used.

## Upgrading

Rebuild the image and restart. The cursor is in Redis, so the new
version resumes where the old one stopped. If a release bumps
`config_version`, the startup error explains what to change.
