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
use std::sync::Arc;
use std::sync::atomic::Ordering;

use proto_blue_lex_cbor::{CborError, decode, decode_all};
use proto_blue_lex_data::{Cid, LexValue};
use proto_blue_repo::read_car;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::event::{
    AccountEvent, CommitEvent, Event, HandleEvent, IdentityEvent, Operation, TombstoneEvent,
};
use crate::metrics::Metrics;
use crate::publisher::PublishOp;
use crate::ws_reader::{WsReader, WsReaderControl};

/// Top-level result of decoding one binary frame.
#[derive(Debug)]
pub enum DecodedFrame {
    /// A republishable event, carrying the firehose sequence number
    /// that Phase 4's publisher uses as the cursor advance target.
    Event { event: Event, seq: u64 },
    /// `#info` / error frame with name `OutdatedCursor`. Supervisor
    /// branches on `cursor.on_stale_cursor` config.
    OutdatedCursor { message: Option<String> },
    /// Any other protocol-level info or error frame.
    Info {
        name: String,
        message: Option<String>,
    },
    /// A known frame type we deliberately do not republish (e.g.
    /// `#sync`, which carries a repo's head CID without operations
    /// and has nothing for the downstream Postgres indexer to write).
    /// Distinct from `UnknownFrameType` — the latter is a real decode
    /// failure for a type the protocol added without us noticing.
    /// `seq` is surfaced so the cursor tracker can still advance past
    /// a skipped frame (otherwise replays would loop forever on a
    /// `#sync` at tip).
    Skipped { kind: &'static str, seq: u64 },
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
    ExpectedMap {
        context: &'static str,
        got: &'static str,
    },

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

    /// Phase 8.5 review finding 3.2: frame exceeds the ATProto spec's
    /// 5MB ceiling. tungstenite accepts up to its 64MB default and
    /// proto-blue-ws doesn't plumb through a `WebSocketConfig`
    /// override (tracking: proto-blue#5), so we backstop at the
    /// decoder to cap peak memory during decode.
    #[error("frame size {size} bytes exceeds AT-spec maximum {max} bytes")]
    FrameTooLarge { size: usize, max: usize },

    /// Phase 8.5 review finding 3.1: a preflight byte-scan walks
    /// the CBOR nesting depth without decoding; anything deeper than
    /// `MAX_PREFLIGHT_DEPTH` is rejected before allocation begins.
    /// A hostile frame at depth > this would otherwise stack-overflow
    /// one of three recursive decoders (ciborium, cbor_to_lex,
    /// lex_to_json), aborting the process via SIGABRT.
    #[error("frame exceeds maximum CBOR nesting depth: depth {depth} > max {max}")]
    FrameTooDeep { depth: u32, max: u32 },

    /// Defense-in-depth twin for `FrameTooDeep`: the preflight scan is
    /// the primary shield, but `lex_to_json` also carries a depth
    /// argument so a future regression in the scanner (or a legitimate
    /// but deep record that somehow slips past) can't recurse without
    /// bound.
    #[error("record exceeds maximum JSON nesting depth at depth {depth}")]
    RecordTooDeep { depth: u32 },

    /// The preflight scanner found a byte sequence that isn't
    /// well-formed DAG-CBOR (reserved info bits, indefinite-length
    /// item where DAG-CBOR requires definite length, etc.). Returned
    /// as a decode error rather than a decoder panic.
    #[error("malformed CBOR in preflight scan: {0}")]
    MalformedCbor(&'static str),
}

/// Phase 8.5 review finding 3.2: ATProto firehose spec hard maximum.
/// tungstenite's default `max_message_size` is 64 MiB, and
/// proto-blue-ws doesn't plumb through a `WebSocketConfig` override
/// yet (tracking: proto-blue#5). We backstop in the decoder so a
/// hostile or misbehaving relay can't expand a 64MB frame into
/// several hundred MB of in-memory CBOR + LexValue + JSON trees.
pub const MAX_FRAME_BYTES: usize = 5 * 1024 * 1024;

/// Phase 8.5 review finding 3.1: CBOR nesting depth cap. ATProto
/// records in the wild rarely exceed depth 8–12; 64 is generous. The
/// preflight scanner ([`preflight_scan`]) walks the byte stream and
/// rejects frames deeper than this before any recursive decoder is
/// called.
pub const MAX_PREFLIGHT_DEPTH: u32 = 64;

/// Defense-in-depth twin of `MAX_PREFLIGHT_DEPTH`: applied inside
/// [`lex_to_json`] so a regression in the preflight scanner — or a
/// deeply-nested LexValue produced some other way — can't recurse
/// without bound. Matched value, different enforcement layer.
pub const MAX_JSON_DEPTH: u32 = 64;

/// Decode one binary firehose frame.
pub fn decode_frame(bytes: &[u8], relay: &str) -> Result<DecodedFrame, DecodeError> {
    // Phase 8.5 review finding 3.2: cheap byte-length gate before
    // anything allocates.
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(DecodeError::FrameTooLarge {
            size: bytes.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    // Phase 8.5 review finding 3.1: depth preflight. Walks CBOR
    // length prefixes iteratively without building a tree; rejects
    // deeply-nested frames before any recursive decoder touches
    // them.
    preflight_scan(bytes, MAX_PREFLIGHT_DEPTH)?;

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
        // indexer downstream has nothing to do with it. We still pull
        // `seq` so the cursor can advance past it.
        "#sync" => Ok(DecodedFrame::Skipped {
            kind: "#sync",
            seq: map_seq(body)?,
        }),
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
    let seq = map_seq(body)?;
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
            let bytes = block_map
                .get(cid)
                .ok_or_else(|| DecodeError::MissingCarBlock {
                    cid: cid.to_string(),
                })?;
            let lex = decode(bytes).map_err(|e| DecodeError::RecordDecode {
                path: path.clone(),
                source: Box::new(DecodeError::Cbor(e)),
            })?;
            Some(lex_to_json(&lex).map_err(|e| DecodeError::RecordDecode {
                path: path.clone(),
                source: Box::new(e),
            })?)
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

    Ok(DecodedFrame::Event {
        event: Event::Commit(CommitEvent {
            repo,
            commit: commit_cid,
            rev,
            ops,
            time,
            relay: relay.into(),
        }),
        seq,
    })
}

fn decode_identity(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    Ok(DecodedFrame::Event {
        event: Event::Identity(IdentityEvent {
            did: map_str(body, "did")?.to_string(),
            handle: map_opt_str(body, "handle")?.map(str::to_string),
            relay: relay.into(),
        }),
        seq: map_seq(body)?,
    })
}

fn decode_account(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    Ok(DecodedFrame::Event {
        event: Event::Account(AccountEvent {
            did: map_str(body, "did")?.to_string(),
            active: map_bool(body, "active")?,
            status: map_opt_str(body, "status")?.map(str::to_string),
            relay: relay.into(),
        }),
        seq: map_seq(body)?,
    })
}

fn decode_handle(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    Ok(DecodedFrame::Event {
        event: Event::Handle(HandleEvent {
            did: map_str(body, "did")?.to_string(),
            handle: map_str(body, "handle")?.to_string(),
            relay: relay.into(),
        }),
        seq: map_seq(body)?,
    })
}

fn decode_tombstone(
    body: &BTreeMap<String, LexValue>,
    relay: &str,
) -> Result<DecodedFrame, DecodeError> {
    Ok(DecodedFrame::Event {
        event: Event::Tombstone(TombstoneEvent {
            did: map_str(body, "did")?.to_string(),
            relay: relay.into(),
        }),
        seq: map_seq(body)?,
    })
}

/// Extract the firehose `seq` field as `u64`. The wire type is CBOR
/// signed integer; the protocol guarantees it's non-negative and
/// monotonically increasing per relay, so we reject negatives rather
/// than silently wrapping.
fn map_seq(body: &BTreeMap<String, LexValue>) -> Result<u64, DecodeError> {
    let s = map_int(body, "seq")?;
    if s < 0 {
        return Err(DecodeError::TypeMismatch {
            field: "seq",
            expected: "non-negative integer",
            got: "negative integer",
        });
    }
    Ok(s as u64)
}

// ─── LexValue → serde_json::Value ──────────────────────────────────

/// Recursively convert a decoded record body to JSON. Unknown `$type`
/// values pass through as-is — we never reject — so deployments that
/// index custom lexicons keep their data (DESIGN.md §3 "Unknown
/// `$type` values").
///
/// Depth-bounded per Phase 8.5 review finding 3.1 — the preflight
/// scan is the primary defense, this is backup.
pub(crate) fn lex_to_json(v: &LexValue) -> Result<serde_json::Value, DecodeError> {
    lex_to_json_bounded(v, 0)
}

fn lex_to_json_bounded(v: &LexValue, depth: u32) -> Result<serde_json::Value, DecodeError> {
    if depth > MAX_JSON_DEPTH {
        return Err(DecodeError::RecordTooDeep { depth });
    }
    use serde_json::Value as J;
    Ok(match v {
        LexValue::Null => J::Null,
        LexValue::Bool(b) => J::Bool(*b),
        LexValue::Integer(i) => J::Number((*i).into()),
        LexValue::String(s) => J::String(s.clone()),
        LexValue::Bytes(b) => J::String(bytes_hex(b)),
        LexValue::Cid(c) => J::String(c.to_string()),
        LexValue::Array(arr) => J::Array(
            arr.iter()
                .map(|x| lex_to_json_bounded(x, depth + 1))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        LexValue::Map(m) => {
            let mut obj = serde_json::Map::with_capacity(m.len());
            for (k, v) in m {
                obj.insert(k.clone(), lex_to_json_bounded(v, depth + 1)?);
            }
            J::Object(obj)
        }
    })
}

/// Walk DAG-CBOR bytes iteratively, tracking the maximum nesting
/// depth reached. Returns `Err` if any container opens at a depth
/// exceeding `max_depth`. Does not allocate a tree — uses an
/// explicit `Vec<u64>` of remaining-items-per-level (bounded at
/// `max_depth + 2`).
///
/// DAG-CBOR is definite-length only (no indefinite-length containers)
/// so every map/array declares its child count in the length prefix.
/// That makes a non-recursive walk straightforward: push the child
/// count on entry, decrement on each completed child, pop when zero.
///
/// # What it doesn't catch
///
/// - Malformed CBOR that ciborium would later reject. We pass-through;
///   the primary decoder still runs and surfaces those errors.
/// - Byte-length truncation at the end of the frame. Same reasoning:
///   decode_all sees it, we don't try to double-validate.
pub(crate) fn preflight_scan(bytes: &[u8], max_depth: u32) -> Result<(), DecodeError> {
    // Stack: remaining items at each nesting level, outermost first.
    // The top-level of a firehose frame is two items (header + body).
    let mut stack: Vec<u64> = Vec::with_capacity(16);
    stack.push(2);

    let mut pos = 0usize;

    while let Some(&top) = stack.last() {
        // If the innermost container is complete, pop upward until we
        // find one that still has pending items (or empty the stack).
        if top == 0 {
            stack.pop();
            if let Some(parent) = stack.last_mut() {
                *parent = parent.saturating_sub(1);
            }
            continue;
        }

        if pos >= bytes.len() {
            // Ran out of bytes mid-structure. Let `decode_all` in the
            // main path report truncation with its own error; the
            // preflight's job is depth enforcement, not completeness.
            return Ok(());
        }

        // Reject depth beyond the cap *before* opening a new container.
        // The stack length includes the top-level slot (= 1) plus one
        // entry per container. So `stack.len() - 1` is the current
        // container depth.
        let depth_now = (stack.len() - 1) as u32;
        if depth_now > max_depth {
            return Err(DecodeError::FrameTooDeep {
                depth: depth_now,
                max: max_depth,
            });
        }

        let initial = bytes[pos];
        pos += 1;
        let major = initial >> 5;
        let info = initial & 0x1F;

        let arg: u64 = match info {
            0..=23 => info as u64,
            24 => {
                if pos >= bytes.len() {
                    return Ok(());
                }
                let v = bytes[pos] as u64;
                pos += 1;
                v
            }
            25 => {
                if pos + 2 > bytes.len() {
                    return Ok(());
                }
                let v = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as u64;
                pos += 2;
                v
            }
            26 => {
                if pos + 4 > bytes.len() {
                    return Ok(());
                }
                let v = u32::from_be_bytes([
                    bytes[pos],
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                ]) as u64;
                pos += 4;
                v
            }
            27 => {
                if pos + 8 > bytes.len() {
                    return Ok(());
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes[pos..pos + 8]);
                let v = u64::from_be_bytes(buf);
                pos += 8;
                v
            }
            28..=30 => {
                return Err(DecodeError::MalformedCbor("reserved additional-info bits"));
            }
            31 => {
                // Indefinite-length container: not allowed in DAG-CBOR.
                return Err(DecodeError::MalformedCbor(
                    "indefinite-length item (DAG-CBOR is definite-only)",
                ));
            }
            _ => unreachable!("info is 5 bits"),
        };

        match major {
            0 | 1 | 7 => {
                // Integer / simple / float — fully-consumed scalar.
                *stack.last_mut().unwrap() = stack.last().unwrap().saturating_sub(1);
            }
            2 | 3 => {
                // Bytes / text string — skip `arg` content bytes.
                pos = pos.saturating_add(arg as usize);
                *stack.last_mut().unwrap() = stack.last().unwrap().saturating_sub(1);
            }
            4 => {
                // Array of `arg` items.
                if arg == 0 {
                    *stack.last_mut().unwrap() = stack.last().unwrap().saturating_sub(1);
                } else {
                    stack.push(arg);
                }
            }
            5 => {
                // Map of `arg` pairs = 2 * arg items.
                if arg == 0 {
                    *stack.last_mut().unwrap() = stack.last().unwrap().saturating_sub(1);
                } else {
                    stack.push(arg.saturating_mul(2));
                }
            }
            6 => {
                // Tag: the next item is the tagged value and counts as
                // the same item in the parent, so don't decrement here.
                // (Tag + tagged-value together = one item.)
            }
            _ => unreachable!("major is 3 bits"),
        }
    }

    Ok(())
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

// ─── Phase 5 pipeline task ─────────────────────────────────────────

/// Phase 8.5 review finding 3.4: K consecutive decode failures before
/// the circuit breaker trips and asks ws_reader to force a failover.
/// Chosen low enough that a single malformed relay is penalised
/// quickly; high enough that transient network glitches don't cause
/// oscillation.
pub const DECODER_CIRCUIT_THRESHOLD: u32 = 10;

/// Counters collected by the decoder task, emitted when the task exits.
#[derive(Debug, Default, Clone, Copy)]
pub struct DecoderStats {
    pub frames_in: u64,
    pub events_out: u64,
    pub decode_errors: u64,
    pub skipped_frames: u64,
    pub info_frames: u64,
    pub outdated_cursors: u64,
    /// Phase 8.5 review finding 3.5: decode panics caught by the
    /// task's `catch_unwind` wrapper. Stack overflows are SIGABRT
    /// and not counted here (not catchable); everything else that
    /// went wrong in the three-crate decoder chain is.
    pub decoder_panics: u64,
    /// Phase 8.5 review finding 3.4: times the consecutive-error
    /// threshold was reached and we asked for a forced failover.
    pub circuit_opens: u64,
}

/// Handle to a spawned decoder task.
pub struct DecoderHandle {
    pub(crate) task: JoinHandle<DecoderStats>,
}

impl DecoderHandle {
    /// Await the task and return its cumulative stats. The task exits
    /// when the upstream [`WsReader`] has been shut down (its frames
    /// channel closes), at which point the decoder has already
    /// forwarded everything it decoded.
    pub async fn join(self) -> DecoderStats {
        self.task.await.unwrap_or_default()
    }
}

/// Spawn the decoder task. Pulls `(bytes, relay)` frames from the
/// `WsReader`, runs [`decode_frame`] (wrapped in `catch_unwind` per
/// finding 3.5), and forwards republishable events — and skip
/// advances for `#sync` frames — to the publisher as
/// [`PublishOp`]s. The publisher is the sole writer to the
/// in-memory cursor; the decoder no longer touches it directly
/// (Phase 8.5 review finding 1.1).
///
/// Decode errors and caught panics are logged at WARN/ERROR with a
/// short hex prefix of the offending frame. After
/// `DECODER_CIRCUIT_THRESHOLD` consecutive failures the decoder
/// signals `ws_reader` to force a failover (finding 3.4); the
/// counter resets on the next successful decode.
pub fn spawn(
    mut reader: WsReader,
    event_tx: mpsc::Sender<PublishOp>,
    metrics: Arc<Metrics>,
    control: WsReaderControl,
) -> DecoderHandle {
    let task = crate::spawn_instrumented("decoder", async move {
        let mut stats = DecoderStats::default();
        let mut consecutive_errors: u32 = 0;

        while let Some((bytes, relay)) = reader.recv().await {
            stats.frames_in += 1;

            // Phase 8.5 review finding 3.5: `catch_unwind` stops
            // panics from killing the decoder task. Rust stack
            // overflows are SIGABRT (not unwinding), so this covers
            // arithmetic panics, slice OOB, and unwraps in deps —
            // not the stack-overflow attack (which is blocked by
            // the preflight-depth scan in `decode_frame` itself).
            let decode_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decode_frame(&bytes, &relay)
            }));

            let frame = match decode_result {
                Ok(Ok(frame)) => {
                    consecutive_errors = 0;
                    frame
                }
                Ok(Err(e)) => {
                    stats.decode_errors += 1;
                    metrics.decode_errors_total.fetch_add(1, Ordering::Relaxed);
                    if matches!(e, DecodeError::UnknownFrameType(_)) {
                        metrics
                            .unknown_frame_types_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let hex_prefix = hex_prefix(&bytes, 32);
                    warn!(
                        relay = %relay,
                        error = %e,
                        frame_hex_prefix = %hex_prefix,
                        "CBOR/CAR decode failed; dropping frame"
                    );
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    maybe_trip_circuit(
                        &mut consecutive_errors,
                        &mut stats,
                        &metrics,
                        &control,
                        &relay,
                    );
                    continue;
                }
                Err(payload) => {
                    stats.decoder_panics += 1;
                    metrics.decoder_panics_total.fetch_add(1, Ordering::Relaxed);
                    // Panic payloads are `Box<dyn Any + Send>` — try
                    // to extract a string message, fall back to a
                    // generic note. Either way we don't re-raise.
                    let msg = panic_payload_str(&payload);
                    let hex_prefix = hex_prefix(&bytes, 32);
                    error!(
                        relay = %relay,
                        panic_message = %msg,
                        frame_hex_prefix = %hex_prefix,
                        "decoder panicked on frame (isolated); dropping and continuing"
                    );
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    maybe_trip_circuit(
                        &mut consecutive_errors,
                        &mut stats,
                        &metrics,
                        &control,
                        &relay,
                    );
                    continue;
                }
            };

            match frame {
                DecodedFrame::Event { event, seq } => {
                    if event_tx.send(PublishOp::Publish(event, seq)).await.is_err() {
                        debug!("decoder: downstream channel closed");
                        break;
                    }
                    stats.events_out += 1;
                }
                DecodedFrame::Skipped { kind, seq } => {
                    stats.skipped_frames += 1;
                    metrics.skipped_frames_total.fetch_add(1, Ordering::Relaxed);
                    // Phase 8.5 review finding 1.1: send an in-band
                    // skip-advance through the pipeline instead of
                    // advancing the shared cursor directly. The
                    // publisher processes this in channel order, so
                    // commits queued before a `#sync` are guaranteed
                    // to XADD (or be oversize-skipped) before the
                    // cursor moves past the `#sync` seq.
                    if event_tx
                        .send(PublishOp::Skip {
                            relay: relay.clone(),
                            seq,
                        })
                        .await
                        .is_err()
                    {
                        debug!("decoder: downstream channel closed");
                        break;
                    }
                    debug!(kind, seq, relay = %relay, "skipped frame; forwarded as PublishOp::Skip");
                }
                DecodedFrame::OutdatedCursor { message } => {
                    stats.outdated_cursors += 1;
                    warn!(
                        relay = %relay,
                        message = ?message,
                        "received OutdatedCursor from relay (policy handling is phase-next)",
                    );
                }
                DecodedFrame::Info { name, message } => {
                    stats.info_frames += 1;
                    info!(
                        relay = %relay,
                        info_name = %name,
                        message = ?message,
                        "relay info frame"
                    );
                }
            }
        }

        debug!(?stats, "decoder task exiting");
        stats
    });

    DecoderHandle { task }
}

fn hex_prefix(b: &[u8], n: usize) -> String {
    let take = b.len().min(n);
    let mut s = String::with_capacity(take * 2);
    use std::fmt::Write;
    for byte in &b[..take] {
        let _ = write!(s, "{byte:02x}");
    }
    s
}

/// Best-effort string extraction from a `catch_unwind` panic payload.
/// Rust panics commonly carry either `&'static str` or `String`; for
/// anything else we emit a generic marker.
fn panic_payload_str(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<panic payload not representable as str>".to_string()
    }
}

/// Phase 8.5 review finding 3.4: if we've seen
/// `DECODER_CIRCUIT_THRESHOLD` failures in a row, ask the supervisor
/// to drop the current connection and re-run relay selection.
/// Resets the counter so the next failure starts a new window —
/// otherwise a persistently broken relay would trigger the force on
/// every single frame after the first K.
fn maybe_trip_circuit(
    consecutive: &mut u32,
    stats: &mut DecoderStats,
    metrics: &Metrics,
    control: &WsReaderControl,
    relay: &str,
) {
    if *consecutive >= DECODER_CIRCUIT_THRESHOLD {
        stats.circuit_opens += 1;
        metrics
            .decoder_circuit_opens_total
            .fetch_add(1, Ordering::Relaxed);
        error!(
            target: "horizon_firehose::metrics",
            event_type = "decoder_circuit_open",
            relay = %relay,
            consecutive_errors = *consecutive,
            threshold = DECODER_CIRCUIT_THRESHOLD,
            "decoder consecutive-error threshold reached; requesting relay failover"
        );
        control.trigger_force_reconnect();
        *consecutive = 0;
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
        let j = lex_to_json(&v).unwrap();
        assert!(j["null"].is_null());
        assert_eq!(j["bool"], serde_json::Value::Bool(true));
        assert_eq!(j["int"], serde_json::json!(-5));
        assert_eq!(j["str"], "hi");
        assert_eq!(j["bytes"], "dead");
        assert_eq!(j["arr"][0], 1);
        assert_eq!(j["nested"]["k"], "v");
    }

    // ─── Phase 8.5 adversarial fixtures for preflight scan ─────────
    // Hand-crafted CBOR bytes rather than committed .cbor files:
    // easier to read at the point of use, no fixture-directory churn.

    /// Build `depth` nested maps each with one key pointing to the
    /// next. Terminates with a null at the bottom. `{"a": {"a": {...}}}`.
    /// Encodes as: `a1 61 61 …` repeated `depth` times, then `f6`.
    fn nested_maps(depth: u32) -> Vec<u8> {
        let mut out = Vec::with_capacity(depth as usize * 3 + 1);
        for _ in 0..depth {
            out.push(0xA1); // map of 1 pair
            out.push(0x61); // text string, len 1
            out.push(b'a');
        }
        out.push(0xF6); // null (terminating value)
        out
    }

    /// Build a firehose-like frame: header `{op:1, t:"#identity"}`
    /// then body (the supplied payload). Two top-level items.
    fn frame_with_body(body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        // Header: map of 2 pairs
        out.push(0xA2);
        out.extend_from_slice(&[0x62, b'o', b'p']); // key "op"
        out.push(0x01); // value 1
        out.extend_from_slice(&[0x61, b't']); // key "t"
        out.push(0x69); // text string, len 9
        out.extend_from_slice(b"#identity");
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn preflight_accepts_shallow_frame() {
        let body = nested_maps(10);
        let frame = frame_with_body(&body);
        assert!(preflight_scan(&frame, MAX_PREFLIGHT_DEPTH).is_ok());
    }

    #[test]
    fn preflight_rejects_deeply_nested_frame() {
        let body = nested_maps(128);
        let frame = frame_with_body(&body);
        let err = preflight_scan(&frame, MAX_PREFLIGHT_DEPTH).unwrap_err();
        match err {
            DecodeError::FrameTooDeep { depth, max } => {
                assert!(depth > max, "reported depth {depth} should exceed {max}");
                assert_eq!(max, MAX_PREFLIGHT_DEPTH);
            }
            other => panic!("expected FrameTooDeep, got {other:?}"),
        }
    }

    #[test]
    fn preflight_rejects_indefinite_length_container() {
        // DAG-CBOR disallows indefinite-length items; 0x9F is one.
        let body = vec![0x9F, 0x01, 0xFF];
        let frame = frame_with_body(&body);
        let err = preflight_scan(&frame, MAX_PREFLIGHT_DEPTH).unwrap_err();
        assert!(matches!(err, DecodeError::MalformedCbor(_)));
    }

    #[test]
    fn decode_frame_rejects_oversize_before_scan() {
        let bytes = vec![0u8; 6 * 1024 * 1024];
        let err = decode_frame(&bytes, "ws://t").unwrap_err();
        match err {
            DecodeError::FrameTooLarge { size, max } => {
                assert_eq!(size, 6 * 1024 * 1024);
                assert_eq!(max, MAX_FRAME_BYTES);
            }
            other => panic!("expected FrameTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn decode_frame_rejects_deep_nesting_before_recursive_decoders() {
        // Phase 8.5 finding 3.1 attack: a 1000-deep frame would
        // stack-overflow the CBOR → LexValue → JSON recursion
        // absent the preflight, but is well below the 5 MB size
        // gate.
        let body = nested_maps(1000);
        let frame = frame_with_body(&body);
        let err = decode_frame(&frame, "ws://t").unwrap_err();
        assert!(
            matches!(err, DecodeError::FrameTooDeep { .. }),
            "expected FrameTooDeep, got {err:?}"
        );
    }

    #[test]
    fn lex_to_json_rejects_overly_deep_lex_tree() {
        fn nested_lex_map(depth: u32) -> LexValue {
            if depth == 0 {
                LexValue::Null
            } else {
                let mut m = BTreeMap::new();
                m.insert("a".to_string(), nested_lex_map(depth - 1));
                LexValue::Map(m)
            }
        }
        let deep = nested_lex_map(MAX_JSON_DEPTH + 10);
        let err = lex_to_json(&deep).unwrap_err();
        assert!(matches!(err, DecodeError::RecordTooDeep { .. }));
    }

    /// End-to-end: decode every captured fixture, gather stats.
    /// Skips cleanly when the fixture corpus is missing (CI without
    /// fixtures will see a SKIP message, not a failure).
    #[test]
    fn decodes_all_captured_fixtures() {
        // Absolute path via CARGO_MANIFEST_DIR — the config tests in
        // this crate use `figment::Jail`, which changes the process
        // CWD. A relative path here races with them under
        // `cargo test` (default multi-threaded test runner) and
        // intermittently fails with ENOENT even though the file is
        // on disk.
        let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let manifest_path = fixtures_dir.join("manifest.json");
        if !manifest_path.exists() {
            eprintln!(
                "SKIP decoder fixture test: tests/fixtures/manifest.json not present \
                 (run `cargo run --bin capture-fixtures` to generate)"
            );
            return;
        }

        let manifest_str = fs::read_to_string(&manifest_path).unwrap();
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
            let path = fixtures_dir.join(filename);
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

                    if let DecodedFrame::Event { event: ev, .. } = &frame {
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
                                if let Some(rec) = &op.record
                                    && let Some(t) = rec.get("$type").and_then(|v| v.as_str())
                                {
                                    *record_types.entry(t.into()).or_insert(0) += 1;
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

    /// Phase 8.5 follow-up finding 3.6: a curated subset of frames
    /// committed under `tests/golden/fixtures/curated_*.bin` gives CI
    /// real decoder coverage without requiring the 1000-frame bulk
    /// corpus (which is gitignored and only exists on dev machines
    /// that have run `capture-fixtures`).
    ///
    /// Every committed frame must decode cleanly. Unlike
    /// `decodes_all_captured_fixtures`, which is exploratory and
    /// tolerates up to 50% decode failures across the unvetted bulk
    /// corpus, this test is strict: a regression that breaks any
    /// curated frame fails CI immediately. Adding a new curated
    /// fixture that doesn't decode is a review signal ("why is this
    /// here?") rather than a permanent warning.
    #[test]
    fn decodes_all_curated_fixtures() {
        let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/fixtures");
        if !fixtures_dir.is_dir() {
            eprintln!("SKIP curated-fixture test: tests/golden/fixtures/ missing");
            return;
        }

        let mut curated_paths: Vec<std::path::PathBuf> = std::fs::read_dir(&fixtures_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.extension().and_then(|s| s.to_str()) == Some("bin")
                    && p.file_stem()
                        .and_then(|s| s.to_str())
                        .map(|n| n.starts_with("curated_"))
                        .unwrap_or(false)
            })
            .collect();
        curated_paths.sort();

        if curated_paths.is_empty() {
            eprintln!(
                "SKIP curated-fixture test: no tests/golden/fixtures/curated_*.bin \
                 committed. Populate via the script under Phase 8.5 follow-up 3.6."
            );
            return;
        }

        let mut kinds: BTreeMap<&'static str, u32> = BTreeMap::new();
        let mut record_types: BTreeMap<String, u32> = BTreeMap::new();
        let mut failures: Vec<(String, String)> = Vec::new();

        for path in &curated_paths {
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("?");
            let bytes = match std::fs::read(path) {
                Ok(b) => b,
                Err(e) => {
                    failures.push((name.to_string(), format!("read: {e}")));
                    continue;
                }
            };
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decode_frame(&bytes, "ws://curated-test-relay")
            })) {
                Ok(Ok(frame)) => {
                    *kinds.entry(frame_kind_label(&frame)).or_insert(0) += 1;
                    if let DecodedFrame::Event {
                        event: Event::Commit(c),
                        ..
                    } = &frame
                    {
                        for op in &c.ops {
                            if let Some(rec) = &op.record
                                && let Some(t) = rec.get("$type").and_then(|v| v.as_str())
                            {
                                *record_types.entry(t.into()).or_insert(0) += 1;
                            }
                        }
                    }
                }
                Ok(Err(e)) => failures.push((name.to_string(), e.to_string())),
                Err(_) => failures.push((name.to_string(), "PANIC during decode".into())),
            }
        }

        eprintln!(
            "curated fixture coverage: {} frame(s), kinds={:?}, $types={}",
            curated_paths.len(),
            kinds,
            record_types.len()
        );

        assert!(
            failures.is_empty(),
            "committed curated fixtures must all decode. Failures: {failures:?}"
        );
    }

    fn frame_kind_label(f: &DecodedFrame) -> &'static str {
        match f {
            DecodedFrame::Event {
                event: Event::Commit(_),
                ..
            } => "commit",
            DecodedFrame::Event {
                event: Event::Identity(_),
                ..
            } => "identity",
            DecodedFrame::Event {
                event: Event::Account(_),
                ..
            } => "account",
            DecodedFrame::Event {
                event: Event::Handle(_),
                ..
            } => "handle",
            DecodedFrame::Event {
                event: Event::Tombstone(_),
                ..
            } => "tombstone",
            DecodedFrame::OutdatedCursor { .. } => "outdated_cursor",
            DecodedFrame::Info { .. } => "info",
            DecodedFrame::Skipped { .. } => "skipped",
        }
    }
}
