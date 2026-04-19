# Upstream issue draft — proto-blue-ws

**Filed as:** `dollspace-gay/proto-blue` issue #5 (or commented on #4 if scope merges).

**Intended submitter:** Chrys

## Title

Expose `WebSocketConfig` on `WebSocketKeepAlive` (max_message_size + TLS ClientConfig)

## Body

Hi! First off, huge thanks for proto-blue — the CBOR/CAR decoder + repo primitives have saved me a lot of work. I'm using it downstream in [horizon-firehose](https://github.com/skydeval/horizon-firehose), and Phase 8 adversarial review surfaced two consumer-side concerns that both trace back to `WebSocketKeepAlive`'s internal use of `tokio_tungstenite::connect_async` with no caller-supplied configuration.

### Concern 1: default max_message_size is 64 MiB; AT-spec max is 5 MB

`tokio-tungstenite::connect_async(url)` defers to `tungstenite::protocol::WebSocketConfig::default()`, which caps `max_message_size` at 64 MiB. The AT Protocol firehose spec says 5 MB:

> "Firehose event stream messages have a hard maximum size limit of 5 MBytes, measured as WebSocket frames."

A hostile or compromised relay can push a single 64MB frame. On my side the decoder then runs three recursive decoders (`ciborium::from_reader` → `cbor_to_lex` → `lex_to_json`), peaking at several hundred MB during decode, OOMing the process on typical container memory limits (<1GB). I've backstopped this with a decoder-level byte-length gate (reject > 5 MB before calling `decode_all`), but the frame has already been fully accepted and held by tungstenite at that point — the real fix is capping at the wire.

### Concern 2: no way to pass a custom rustls `ClientConfig` (related to issue #4)

Same root cause: the call site in `keepalive.rs` is `connect_async(&self.url)`, so downstream can't plumb through `--tls-extra-ca-file`-style config. Already tracked as #4 for the additive-CA case; flagging here because both land in the same `connect_async_with_config` switch.

### Proposed shape

Add an optional `WebSocketConfig` field (or a newtype that wraps it) to `WebSocketKeepAliveOpts`:

```rust
pub struct WebSocketKeepAliveOpts {
    pub max_reconnect_seconds: u64,
    pub heartbeat_interval_ms: u64,
    pub ws_config: Option<tungstenite::protocol::WebSocketConfig>,
    pub tls_connector: Option<tokio_tungstenite::Connector>,  // or similar
}
```

and call `connect_async_with_config(url, ws_config, tls_connector)` (via `connect_async_tls_with_config`) inside `connect`. Default behavior unchanged — only caller-supplied overrides take effect.

Happy to submit a PR if that'd help.

## Horizon-firehose side

- Decoder-level byte gate: [src/decoder.rs — `MAX_FRAME_BYTES` + `DecodeError::FrameTooLarge`](https://github.com/skydeval/horizon-firehose/blob/main/src/decoder.rs)
- Inert `tls_extra_ca_file` today, validated-but-unused: [src/main.rs — `validate_tls_extra_ca`](https://github.com/skydeval/horizon-firehose/blob/main/src/main.rs)
- DESIGN.md §3 cursor ownership + §4 decoder preflight sections document the workarounds.
