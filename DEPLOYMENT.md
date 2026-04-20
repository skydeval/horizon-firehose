# Deployment

Production deployment notes for horizon-firehose, including the Phase 11a parallel-run configuration currently running in the Horizon client's AppView stack.

## Current deployment status

horizon-firehose is deployed in parallel-run mode alongside the existing Python firehose consumer in the Horizon client's appview, Horizon-Prism (a fork of [Aurora-Prism](https://github.com/dollspace-gay/Aurora-Prism)). Both consumers read from the same ATProto relay (`wss://bsky.network`) and publish to the same Redis stream (`firehose:events`). The downstream Python worker deduplicates events at the Postgres layer via `ON CONFLICT` clauses, so parallel publication is safe.

This is a Phase 11a deployment, deployment proven, not cutover. The Python consumer remains the primary publisher. horizon-firehose runs alongside to validate real-world operational behavior before any cutover decision.

## Prerequisites

- Docker and Docker Compose on the deployment host
- An existing Redis instance accessible to the container
- Network access to an ATProto firehose relay
- Configuration file mounted into the container at `/app/config.toml`

## docker-compose service definition

Add the following service to your `docker-compose.yml`, adjusting paths and resource limits to match your environment:

```yaml
  horizon-firehose:
    build:
      context: ./horizon-firehose
      dockerfile: Dockerfile
    environment:
      - HF_CONFIG_PATH=/app/config.toml
    volumes:
      - ./horizon-firehose/config.toml:/app/config.toml:ro
    depends_on:
      redis:
        condition: service_healthy
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 512M
```

Notes on the service definition:

- `build.context` points to your horizon-firehose repo clone. Use `image:` instead if you're pulling a pre-built image from a registry.
- The config file is mounted read-only at `/app/config.toml` (the default path the Dockerfile expects).
- `HF_CONFIG_PATH` is set explicitly as a safety net against future Dockerfile changes.
- Memory limits are conservative. Actual usage during parallel-run with default settings is well below 512M.
- `depends_on` only requires Redis. horizon-firehose does not depend on Postgres or any downstream service.

## config.toml template

The production `config.toml` lives outside the horizon-firehose repo (it's gitignored by default). Here's a template for a parallel-run deployment:

```toml
config_version = 1

[relay]
url = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
fallbacks = []
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
record_types = []

[cursor]
save_interval_seconds = 5
on_stale_cursor = "live_tip"
on_protocol_error = "reconnect_from_live_tip"

[publisher]
max_event_size_bytes = 1048576
on_oversize = "skip_with_log"

[logging]
level = "info"
format = "json"
```

Key decisions for parallel-run:

- Same relay URL as the Python consumer for apples-to-apples parity.
- Same Redis stream key so events feed the same downstream worker.
- Empty filter (`record_types = []`) to pass all records through, matching Python's behavior.
- JSON log format for easy parsing during observation.
- Conservative cursor policies (`live_tip` and `reconnect_from_live_tip`) that prefer continued operation over exit on protocol edge cases.

See `config.example.toml` in the repo for field-by-field documentation.

## Deployment steps

1. Clone horizon-firehose into a directory accessible to your Docker build context.
2. Create `config.toml` in the horizon-firehose directory using the template above, adjusted for your environment.
3. Add the service definition to your `docker-compose.yml`.
4. Validate the compose file:

```
   docker compose config --services
```

5. Build the image:

```
   docker compose build horizon-firehose
```

   First build takes 5 to 10 minutes (Rust release compile with LTO, plus image layer pulls). Subsequent rebuilds are fast unless Cargo.toml or Cargo.lock changes.

6. Start the service:

```
   docker compose up -d horizon-firehose
```

7. Tail logs to verify startup:

```
   docker compose logs -f horizon-firehose
```

Expected startup sequence (JSON logs):

1. `startup_metrics` event showing the loaded config
2. `connecting to relay` followed by `relay_connected` within a second or two
3. `pipeline_started` event once all internal tasks are running
4. `periodic_metrics` events every 10 seconds reporting event rates and health

## Cursor inspection

horizon-firehose persists its cursor to Redis under a base64url-encoded key derived from the relay URL. To inspect:

```
# List all cursor keys
docker compose exec redis redis-cli KEYS '*cursor*'

# Read horizon-firehose's cursor value
docker compose exec redis redis-cli GET firehose:cursor:<base64url-encoded-relay-url>
```

The cursor advances continuously while the service is running. It persists across restarts, so on restart horizon-firehose resumes from the exact sequence it last wrote.

## Observing parallel-run

During parallel-run, both horizon-firehose and the Python consumer publish to the same Redis stream. Things to watch:

- **Event rate parity.** Both consumers should show comparable events/sec over time. Short-term variance is expected (different reporting windows, relay load balancer behavior). Sustained divergence is worth investigating.
- **Stream length.** The Redis stream should stay near its configured `max_stream_len` cap with small overage due to XADD batching.
- **Downstream worker.** The Python worker consumes from the shared stream. Watch for its processing rate and any error spike correlated with horizon-firehose coming online.
- **horizon-firehose specific metrics.** The `periodic_metrics` log line includes channel depths, decode errors, Redis errors, reconnects, and redis health. All should be steady under normal operation.
- **Duplicate events.** The downstream worker dedupes via Postgres `ON CONFLICT`. Parallel publication produces double the write attempts for the same logical event, but only one succeeds. Postgres write rate should increase only marginally.
- **Memory and CPU.** horizon-firehose's memory usage during parallel-run should stay well under the 512M reservation.

## Cutover (Phase 11b, future)

If you decide to cut over from Python to horizon-firehose as the sole publisher:

1. Verify horizon-firehose has been running cleanly alongside Python for an extended observation window.
2. Stop the Python firehose service: `docker compose stop python-firehose`.
3. Monitor the downstream worker and Postgres write rate for the next hour. Rates should remain roughly unchanged (horizon-firehose now publishing all events alone).
4. If anything unexpected surfaces, restart Python: `docker compose start python-firehose`. horizon-firehose and Python will return to parallel-run, and nothing is lost.

Cutover is optional and has no timeline requirement. Parallel-run is a valid long-term deployment posture.

## Troubleshooting

**Container restarts repeatedly at startup.**
Check `docker compose logs horizon-firehose` for the error. Most likely causes: invalid config, Redis not reachable, TLS handshake failure on the relay connection.

**No events published to Redis.**
Verify the relay URL is correct and includes the XRPC path (`/xrpc/com.atproto.sync.subscribeRepos`). horizon-firehose uses a generic WebSocket client; the path is not auto-appended.

**Cursor stuck or resetting.**
Check the `cursor_ages_seconds` field in `periodic_metrics` logs. A large age means the cursor isn't advancing. Most common cause: the service is reconnecting repeatedly. Check `relay_reconnects_in_window` in the same log line.

**Image build fails with Rust compile errors.**
Ensure your Docker builder has enough memory (~4GB recommended for LTO build). Verify your Cargo.lock matches the pinned proto-blue SHA; if not, `cargo update` before building.

## See also

- `DESIGN.md` — full architecture and design decisions
- `RETROSPECTIVE.md` — the story of building this
- `config.example.toml` — field-by-field config documentation
- `README.md` — project overview and basic usage