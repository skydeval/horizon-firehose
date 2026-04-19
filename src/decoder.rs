//! Decode raw firehose frames into typed events.
//!
//! # Wire format
//!
//! Each binary WebSocket frame from `com.atproto.sync.subscribeRepos`
//! contains *two* consecutive DAG-CBOR objects:
//!
//! 1. **Header** — `{op: i8, t?: string}`. `op == 1` is a normal
//!    event with `t` naming the type (`#commit`, `#identity`, …);
//!    `op == -1` is an error frame with body `{error, message?}`.
//! 2. **Body** — payload object whose shape depends on `t`. For
//!    commits the `blocks` field is a CAR file packing the CIDs and
//!    CBOR bodies of every record touched by the commit.
//!
//! # Error policy
//!
//! Every decode path returns `Result<_, DecodeError>`; nothing
//! panics or unwraps on wire data. Per DESIGN.md §3 "CBOR/CAR decode
//! failure", callers (the supervisor) increment a counter and drop
//! the frame on `Err`. `OutdatedCursor` is surfaced as its own
//! `DecodedFrame` variant so the cursor module can dispatch per
//! `on_stale_cursor` config.

use std::collections::BTreeMap;

use proto_blue_lex_cbor::{CborError, decode, decode_all};
use proto_blue_lex_data::{Cid, LexValue};
use proto_blue_repo::read_car;
use thiserror::Error;

use crate::event::{
    AccountEvent, CommitEvent, Event, HandleEvent, IdentityEvent, Operation,
    TombstoneEvent,
};

/// Top-level result of decoding one binary frame.
#[derive(Debug)]
pub enum DecodedFrame {
    Event(Event),
    /// `#info` / error frame with name `OutdatedCursor`. Supervisor
    /// branches on `cursor.on_stale_cursor` config.
    OutdatedCursor { message: Option<String> },
    /// Any other protocol-level info or error frame.
    Info { name: String, message: Option<String> },
    /// A known frame type we deliberately do not republish (e.g.
    /// `#sync`, which carries a repo's head CID without operations
    /// and has nothing for the downstream Postgres indexer to write).
    /// Distinct from `UnknownFrameType` — the latter is a real decode
    /// failure for a type the protocol added without us noticing.
    Skipped { kind: &'static str },
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("CBOR decode failed: {0}")]
    Cbor(#[from] CborError),

    #[error("CAR file parse failed: {0}")]
    Car(String),

    #[error("frame had {got} top-level CBOR objects, expected 2 (header + body)")]
    TruncatedFrame { got: usize },

    #[error("expected map for {context}, got {got}")]
    ExpectedMap { context: &'static str, got: &'static str },

    #[error("missing required field `{0}`")]
    MissingField(&'static str),

    #[error("field `{field}` expected {expected}, got {got}")]
    TypeMismatch {
        field: &'static str,
        expected: &'static str,
        got: &'static str,
    },

    #[error("unknown frame type `{0}`")]
    UnknownFrameType(String),

    #[error("op references CID {cid} that is not present in the CAR blocks")]
    MissingCarBlock { cid: String },

    #[error("nested record decode at path `{path}` failed: {source}")]
    RecordDecode {
        path: String,
        #[source]
        source: Box<DecodeError>,
    },
}

/// Decode one binary firehose frame.
pub fn decode_frame(bytes: &[u8], relay: &str) -> Result<DecodedFrame, DecodeError> {
    let values = decode_all(bytes)?;
    if values.len() < 2 {
        return Err(DecodeError::TruncatedFrame { got: values.len() });
    }
    let header = expect_map(&values[0], "header")?;
    let op = map_int(header, "op")?;

    if op == -1 {
        // Error frame: body is `{error: string, message?: string}`.
        let body = expect_map(&values[1], "error body")?;
        let name = map_str(body, "error")?.to_string();
        let message = map_opt_str(body, "message")?.map(str::to_string);
        return Ok(if name == "OutdatedCursor" {
            DecodedFrame::OutdatedCursor { message }
        } else {
            DecodedFrame::Info { name, message }
        });
    }

    let t = map_str(header, "t")?;
    let body = expect_map(&values[1], "body")?;

    match t {
        "#commit" => decode_commit(body, relay),
        "#identity" => decode_identity(body, relay),
        "#account" => decode_account(body, relay),
        "#handle" => decode_handle(body, relay),
        "#tombstone" => decode_tombstone(body, relay),
        // `#sync` is the post-2025 firehose event that announces a
        // repo's current head without operations (used for fast
        // catch-up). It carries no per-record data, so the Postgres
        // indexer downstream has nothing to do with it.
        "#sync" => Ok(DecodedFrame::Skipped { kind: "#sync" }),
        "#info" => {
            // `#info` envelope (different from `op=-1` errors). DESIGN.md
            // §3 calls these out specifically for OutdatedCursor handling.
            let name = map_str(body, "name")?.to_string();
            let message = map_opt_str(body, "message")?.map(str::to_string);
            Ok(if name == "OutdatedCursor" {
                DecodedFrame::OutdatedCursor { message }
            } else {
                DecodedFrame::Info { name, message }
            })
        }
        other => Err(DecodeError::UnknownFrameType(other.to_string())),
    }
}

// ─── per-frame decoders ────────────────────────────────────────────

fn decode_commit(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    let repo = map_str(body, "repo")?.to_string();
    let commit_cid = map_cid(body, "commit")?.to_string();
    let rev = map_str(body, "rev")?.to_string();
    let time = map_str(body, "time")?.to_string();
    let blocks_bytes = map_bytes(body, "blocks")?;
    let ops_arr = map_array(body, "ops")?;

    let (_roots, block_map) =
        read_car(blocks_bytes).map_err(|e| DecodeError::Car(e.to_string()))?;

    let mut ops = Vec::with_capacity(ops_arr.len());
    for op_val in ops_arr {
        let op_map = expect_map(op_val, "ops[i]")?;
        let action = map_str(op_map, "action")?.to_string();
        let path = map_str(op_map, "path")?.to_string();

        let cid_ref: Option<&Cid> = match op_map.get("cid") {
            None | Some(LexValue::Null) => None,
            Some(LexValue::Cid(c)) => Some(c),
            Some(other) => {
                return Err(DecodeError::TypeMismatch {
                    field: "ops.cid",
                    expected: "cid|null",
                    got: lex_kind(other),
                });
            }
        };
        let cid_str = cid_ref.map(Cid::to_string);

        let record = if action == "delete" {
            None
        } else if let Some(cid) = cid_ref {
            let bytes = block_map.get(cid).ok_or_else(|| DecodeError::MissingCarBlock {
                cid: cid.to_string(),
            })?;
            let lex = decode(bytes).map_err(|e| DecodeError::RecordDecode {
                path: path.clone(),
                source: Box::new(DecodeError::Cbor(e)),
            })?;
            Some(lex_to_json(&lex))
        } else {
            // create/update without a CID is malformed per spec but we
            // shouldn't panic — emit a null record and let downstream
            // decide.
            None
        };

        ops.push(Operation {
            action,
            path,
            cid: cid_str,
            record,
        });
    }

    Ok(DecodedFrame::Event(Event::Commit(CommitEvent {
        repo,
        commit: commit_cid,
        rev,
        ops,
        time,
        relay: relay.into(),
    })))
}

fn decode_identity(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    Ok(DecodedFrame::Event(Event::Identity(IdentityEvent {
        did: map_str(body, "did")?.to_string(),
        handle: map_opt_str(body, "handle")?.map(str::to_string),
        relay: relay.into(),
    })))
}

fn decode_account(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    Ok(DecodedFrame::Event(Event::Account(AccountEvent {
        did: map_str(body, "did")?.to_string(),
        active: map_bool(body, "active")?,
        status: map_opt_str(body, "status")?.map(str::to_string),
        relay: relay.into(),
    })))
}

fn decode_handle(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    Ok(DecodedFrame::Event(Event::Handle(HandleEvent {
        did: map_str(body, "did")?.to_string(),
        handle: map_str(body, "handle")?.to_string(),
        relay: relay.into(),
    })))
}

fn decode_tombstone(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    Ok(DecodedFrame::Event(Event::Tombstone(TombstoneEvent {
        did: map_str(body, "did")?.to_string(),
        relay: relay.into(),
    })))
}

// ─── LexValue → serde_json::Value ──────────────────────────────────

/// Recursively convert a decoded record body to JSON. Unknown `$type`
/// values pass through as-is — we never reject — so deployments that
/// index custom lexicons keep their data (DESIGN.md §3 "Unknown
/// `$type` values").
pub(crate) fn lex_to_json(v: &LexValue) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        LexValue::Null => J::Null,
        LexValue::Bool(b) => J::Bool(*b),
        LexValue::Integer(i) => J::Number((*i).into()),
        LexValue::String(s) => J::String(s.clone()),
        LexValue::Bytes(b) => J::String(bytes_hex(b)),
        LexValue::Cid(c) => J::String(c.to_string()),
        LexValue::Array(arr) => J::Array(arr.iter().map(lex_to_json).collect()),
        LexValue::Map(m) => {
            let mut obj = serde_json::Map::with_capacity(m.len());
            for (k, v) in m {
                obj.insert(k.clone(), lex_to_json(v));
            }
            J::Object(obj)
        }
    }
}

fn bytes_hex(b: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        let _ = write!(s, "{byte:02x}");
    }
    s
}

// ─── small typed-getters over LexValue maps ────────────────────────

fn expect_map<'a>(
    v: &'a LexValue,
    ctx: &'static str,
) -> Result<&'a BTreeMap<String, LexValue>, DecodeError> {
    match v {
        LexValue::Map(m) => Ok(m),
        other => Err(DecodeError::ExpectedMap {
            context: ctx,
            got: lex_kind(other),
        }),
    }
}

fn map_str<'a>(
    m: &'a BTreeMap<String, LexValue>,
    key: &'static str,
) -> Result<&'a str, DecodeError> {
    match m.get(key) {
        Some(LexValue::String(s)) => Ok(s),
        Some(other) => Err(DecodeError::TypeMismatch {
            field: key,
            expected: "string",
            got: lex_kind(other),
        }),
        None => Err(DecodeError::MissingField(key)),
    }
}

fn map_opt_str<'a>(
    m: &'a BTreeMap<String, LexValue>,
    key: &'static str,
) -> Result<Option<&'a str>, DecodeError> {
    match m.get(key) {
        None | Some(LexValue::Null) => Ok(None),
        Some(LexValue::String(s)) => Ok(Some(s)),
        Some(other) => Err(DecodeError::TypeMismatch {
            field: key,
            expected: "string|null",
            got: lex_kind(other),
        }),
    }
}

fn map_int(m: &BTreeMap<String, LexValue>, key: &'static str) -> Result<i64, DecodeError> {
    match m.get(key) {
        Some(LexValue::Integer(i)) => Ok(*i),
        Some(other) => Err(DecodeError::TypeMismatch {
            field: key,
            expected: "integer",
            got: lex_kind(other),
        }),
        None => Err(DecodeError::MissingField(key)),
    }
}

fn map_bool(m: &BTreeMap<String, LexValue>, key: &'static str) -> Result<bool, DecodeError> {
    match m.get(key) {
        Some(LexValue::Bool(b)) => Ok(*b),
        Some(other) => Err(DecodeError::TypeMismatch {
            field: key,
            expected: "bool",
            got: lex_kind(other),
        }),
        None => Err(DecodeError::MissingField(key)),
    }
}

fn map_bytes<'a>(
    m: &'a BTreeMap<String, LexValue>,
    key: &'static str,
) -> Result<&'a [u8], DecodeError> {
    match m.get(key) {
        Some(LexValue::Bytes(b)) => Ok(b),
        Some(other) => Err(DecodeError::TypeMismatch {
            field: key,
            expected: "bytes",
            got: lex_kind(other),
        }),
        None => Err(DecodeError::MissingField(key)),
    }
}

fn map_cid<'a>(
    m: &'a BTreeMap<String, LexValue>,
    key: &'static str,
) -> Result<&'a Cid, DecodeError> {
    match m.get(key) {
        Some(LexValue::Cid(c)) => Ok(c),
        Some(other) => Err(DecodeError::TypeMismatch {
            field: key,
            expected: "cid",
            got: lex_kind(other),
        }),
        None => Err(DecodeError::MissingField(key)),
    }
}

fn map_array<'a>(
    m: &'a BTreeMap<String, LexValue>,
    key: &'static str,
) -> Result<&'a [LexValue], DecodeError> {
    match m.get(key) {
        Some(LexValue::Array(a)) => Ok(a),
        Some(other) => Err(DecodeError::TypeMismatch {
            field: key,
            expected: "array",
            got: lex_kind(other),
        }),
        None => Err(DecodeError::MissingField(key)),
    }
}

fn lex_kind(v: &LexValue) -> &'static str {
    match v {
        LexValue::Null => "null",
        LexValue::Bool(_) => "bool",
        LexValue::Integer(_) => "integer",
        LexValue::String(_) => "string",
        LexValue::Bytes(_) => "bytes",
        LexValue::Cid(_) => "cid",
        LexValue::Array(_) => "array",
        LexValue::Map(_) => "map",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::path::Path;

    /// Pure-data unit test: hex encoder spits lowercase, no `0x`.
    #[test]
    fn bytes_hex_matches_design_spec() {
        assert_eq!(bytes_hex(&[]), "");
        assert_eq!(bytes_hex(&[0xab, 0xcd, 0xef, 0x00, 0xff]), "abcdef00ff");
    }

    /// Pure-data unit test: lex_to_json over a small handcrafted map.
    #[test]
    fn lex_to_json_handles_every_variant() {
        use std::collections::BTreeMap;
        let mut inner = BTreeMap::new();
        inner.insert("k".to_string(), LexValue::String("v".into()));
        let v = LexValue::Map({
            let mut m = BTreeMap::new();
            m.insert("null".into(), LexValue::Null);
            m.insert("bool".into(), LexValue::Bool(true));
            m.insert("int".into(), LexValue::Integer(-5));
            m.insert("str".into(), LexValue::String("hi".into()));
            m.insert("bytes".into(), LexValue::Bytes(vec![0xde, 0xad]));
            m.insert("arr".into(), LexValue::Array(vec![LexValue::Integer(1)]));
            m.insert("nested".into(), LexValue::Map(inner));
            m
        });
        let j = lex_to_json(&v);
        assert!(j["null"].is_null());
        assert_eq!(j["bool"], serde_json::Value::Bool(true));
        assert_eq!(j["int"], serde_json::json!(-5));
        assert_eq!(j["str"], "hi");
        assert_eq!(j["bytes"], "dead");
        assert_eq!(j["arr"][0], 1);
        assert_eq!(j["nested"]["k"], "v");
    }

    /// End-to-end: decode every captured fixture, gather stats.
    /// Skips cleanly when the fixture corpus is missing (CI without
    /// fixtures will see a SKIP message, not a failure).
    #[test]
    fn decodes_all_captured_fixtures() {
        let manifest_path = Path::new("tests/fixtures/manifest.json");
        if !manifest_path.exists() {
            eprintln!(
                "SKIP decoder fixture test: tests/fixtures/manifest.json not present \
                 (run `cargo run --bin capture-fixtures` to generate)"
            );
            return;
        }

        let manifest_str = fs::read_to_string(manifest_path).unwrap();
        let manifest: serde_json::Value = serde_json::from_str(&manifest_str).unwrap();
        let frames = manifest["frames"].as_array().expect("frames array");

        let relay = "ws://test-relay/xrpc/com.atproto.sync.subscribeRepos";
        let mut total = 0usize;
        let mut decoded_ok = 0usize;
        let mut frame_kinds: BTreeMap<&'static str, u32> = BTreeMap::new();
        let mut record_types: BTreeMap<String, u32> = BTreeMap::new();
        let mut failures: Vec<(String, String)> = Vec::new();

        for frame in frames {
            let filename = frame["filename"].as_str().unwrap();
            let path = Path::new("tests/fixtures").join(filename);
            let bytes = match fs::read(&path) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("could not read {filename}: {e}");
                    continue;
                }
            };
            total += 1;

            // Wrap the decode in catch_unwind: a successful decode is
            // expected, an error is a useful finding, but a *panic* is
            // a defect we want to surface clearly per the brief
            // ("don't panic, don't use unwrap").
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decode_frame(&bytes, relay)
            }));

            match result {
                Ok(Ok(frame)) => {
                    decoded_ok += 1;
                    *frame_kinds.entry(frame_kind_label(&frame)).or_insert(0) += 1;

                    if let DecodedFrame::Event(ev) = &frame {
                        // Verify JSON serialisation works and the
                        // _relay invariant holds.
                        let json = serde_json::to_value(ev).expect("event serialises");
                        assert!(json.is_object(), "event JSON must be an object");
                        assert_eq!(
                            json["_relay"].as_str(),
                            Some(relay),
                            "_relay invariant violated for {filename}"
                        );

                        if let Event::Commit(c) = ev {
                            for op in &c.ops {
                                if let Some(rec) = &op.record {
                                    if let Some(t) =
                                        rec.get("$type").and_then(|v| v.as_str())
                                    {
                                        *record_types.entry(t.into()).or_insert(0) += 1;
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Err(e)) => failures.push((filename.to_string(), e.to_string())),
                Err(_) => failures.push((filename.to_string(), "PANIC during decode".into())),
            }
        }

        let rate = if total > 0 {
            decoded_ok as f64 / total as f64 * 100.0
        } else {
            0.0
        };

        eprintln!();
        eprintln!("══════════════════════════════════════════════════════════");
        eprintln!(" Phase 3 decoder fixture stats");
        eprintln!("══════════════════════════════════════════════════════════");
        eprintln!(
            " frames: {total} total / {decoded_ok} decoded / {} failed ({rate:.2}%)",
            failures.len(),
        );
        eprintln!();
        eprintln!(" frame kinds:");
        for (k, c) in &frame_kinds {
            eprintln!("   {c:>5}  {k}");
        }
        eprintln!();
        eprintln!(" record $types ({} distinct):", record_types.len());
        let mut rec: Vec<_> = record_types.iter().collect();
        rec.sort_by_key(|(_, c)| std::cmp::Reverse(**c));
        for (t, c) in &rec {
            let marker = if **c >= 3 { "OK  " } else { "WARN" };
            eprintln!("   [{marker}] {c:>4}  {t}");
        }
        if !failures.is_empty() {
            eprintln!();
            eprintln!(" first 10 failures:");
            for (file, err) in failures.iter().take(10) {
                eprintln!("   {file}: {err}");
            }
        }
        eprintln!("══════════════════════════════════════════════════════════");
        eprintln!();

        // Phase 3 is exploratory: edge-case decode failures are useful
        // findings, not hard failures. Only fail if the rate is
        // catastrophically low (suggesting a structural bug).
        assert!(
            rate >= 50.0,
            "decode success rate {rate:.2}% is below 50% — structural bug likely"
        );
        // No panics anywhere.
        assert!(
            !failures.iter().any(|(_, e)| e.contains("PANIC")),
            "decoder panicked on at least one frame"
        );
    }

    fn frame_kind_label(f: &DecodedFrame) -> &'static str {
        match f {
            DecodedFrame::Event(Event::Commit(_)) => "commit",
            DecodedFrame::Event(Event::Identity(_)) => "identity",
            DecodedFrame::Event(Event::Account(_)) => "account",
            DecodedFrame::Event(Event::Handle(_)) => "handle",
            DecodedFrame::Event(Event::Tombstone(_)) => "tombstone",
            DecodedFrame::OutdatedCursor { .. } => "outdated_cursor",
            DecodedFrame::Info { .. } => "info",
            DecodedFrame::Skipped { .. } => "skipped",
        }
    }
}
