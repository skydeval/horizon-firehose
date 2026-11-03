# Hardening

A firehose consumer reads untrusted bytes from the network all day.
These are the defences between a relay and the rest of the process.

## Frame preflight

Before any recursive decoder runs, every frame passes two cheap checks
that allocate nothing:

1. **Size.** Frames over 5 MB (the firehose maximum) are rejected with
   `DecodeError::FrameTooLarge`. The WebSocket library would otherwise
   accept frames up to 64 MiB, and decoding one could expand to
   several hundred MB in memory.
2. **Nesting depth.** A byte-level scan walks the DAG-CBOR length
   prefixes and tracks nesting without building anything. Frames
   nested deeper than 64 levels are rejected with
   `DecodeError::FrameTooDeep`.

The depth check matters because a deep enough frame would overflow the
stack in one of the recursive decoders. A stack overflow aborts the
process outright: it cannot be caught, and on restart the relay would
send the same frame from the same cursor, so the process would crash
in a loop. The JSON conversion step keeps its own depth limit
(`DecodeError::RecordTooDeep`) in case anything ever gets past the
preflight.

## Panic isolation

Every `decode_frame` call runs inside `catch_unwind`. A caught panic is
logged at ERROR with the seq (when known) and a hex prefix of the
frame, `decoder_panics_total` goes up, and the decoder moves to the
next frame.

## Decode failures and the circuit breaker

A frame that fails to decode is logged at WARN with its seq and the
first 64 bytes in hex, `decode_errors` goes up, and the frame is
dropped.

After 10 failures in a row, the decoder tells the supervisor to drop
the connection and pick another relay, and `decoder_circuit_opens_total`
goes up. This stops one broken relay from holding the consumer
indefinitely. Any successful decode resets the count.

## TLS

Two TLS paths are compiled in, and config picks one:

- **Default (`tls_extra_ca_file = ""`).** The platform's native TLS
  stack (OpenSSL on Linux), verifying against system roots.
- **Custom CA (`tls_extra_ca_file = "/path/ca.pem"`).** rustls with the
  system roots plus every certificate in the given PEM file, applied to
  every handshake.

The custom CA file is loaded at startup, before anything connects. A
missing file, bad PEM, a file with no certificates, or a certificate
rustls rejects all stop startup with an error naming the path. Failing
early is deliberate: an ignored CA would otherwise surface later as a
confusing handshake failure.

There is an `--insecure-dev-mode` flag for local testing. It only
exists in debug builds and cannot be compiled into a release binary.

## Credentials

- Relay URLs that contain userinfo (`wss://user:pass@host`) are
  rejected when the config loads.
- As a second layer, `config::sanitize_ws_url` strips userinfo from any
  relay URL before it is logged, put in metrics, or written to an
  event's `_relay` field.
- `startup_metrics` uses a fixed field allowlist and reports only the
  Redis host, never the full Redis URL.

## Config validation

- `[relay]` and `[redis]` are required. There are no defaults that
  could point a deployment at the wrong relay or someone else's Redis.
- Unknown fields are rejected.
- Relay URLs must include `/xrpc/com.atproto.sync.subscribeRepos`. The
  WebSocket client does not add it, and the full URL is part of the
  relay's identity for cursor keys.
- `config_version` newer than the binary supports is rejected with a
  migration message. Older versions are accepted with a WARN.

A version bump is required for any new required field, any removed or
renamed field, any default change that alters behaviour, and any change
in meaning of an existing field. Adding optional fields with
backward-compatible defaults, or new sections, does not need one.

## Supply chain

proto-blue is pinned by commit SHA in `Cargo.toml`, and CI checks the
SHA resolved in `Cargo.lock` against
`.github/expected-proto-blue-sha.txt` to catch upstream force-pushes.
`cargo audit` runs on every push.

## Container

The runtime image is `gcr.io/distroless/cc-debian12:nonroot`: no
shell, no package manager, running as UID 65532. CI fails the build if
the image exceeds 50 MiB.
