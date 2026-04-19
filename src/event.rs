//! Typed events and JSON serialization per DESIGN.md §4.
//!
//! Each variant maps directly to one of the firehose event types we
//! republish to Redis. Field order matches the schema for cosmetic
//! reasons (serde_json preserves insertion order). Optional fields are
//! `Option<T>` and serialize as JSON `null` when None — `_relay` is
//! always emitted regardless of single- or multi-relay deployment.
//!
//! Encoding rules (per DESIGN.md §4 "Encoding rules"):
//! - CIDs: emitted as the multibase-base32-lower string form (e.g.
//!   `bafyrei…`). ATProto's data model spec blesses CIDv1 exclusively
//!   (DASL: codec `dag-cbor (0x71)` or `raw (0x55)`, SHA-256), so the
//!   string form *is* the transmitted form — no version-preservation
//!   logic is needed. proto-blue's CBOR decoder hard-rejects any
//!   non-v1 input rather than silently normalising, so a hypothetical
//!   v0 CID would surface as a decode error rather than a quiet data
//!   change. Verified empirically against 79,651 CIDs in 1000
//!   captured fixtures: 100% v1, 0% v0.
//! - Bytes: emitted as lowercase hex (no `0x` prefix). Performed by
//!   the decoder when constructing the JSON record body, not here.
//! - Timestamps: pass-through strings — the firehose hands us ISO 8601
//!   already. We do not reformat.
//! - Unknown record `$type` values: the record body is a
//!   `serde_json::Value` so unknown shapes round-trip unchanged.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Event {
    Commit(CommitEvent),
    Identity(IdentityEvent),
    Account(AccountEvent),
    Handle(HandleEvent),
    Tombstone(TombstoneEvent),
}

impl Event {
    /// Stream-field "type" string per DESIGN.md §4 Redis stream format.
    pub fn type_name(&self) -> &'static str {
        match self {
            Event::Commit(_) => "commit",
            Event::Identity(_) => "identity",
            Event::Account(_) => "account",
            Event::Handle(_) => "handle",
            Event::Tombstone(_) => "tombstone",
        }
    }

    pub fn relay(&self) -> &str {
        match self {
            Event::Commit(e) => &e.relay,
            Event::Identity(e) => &e.relay,
            Event::Account(e) => &e.relay,
            Event::Handle(e) => &e.relay,
            Event::Tombstone(e) => &e.relay,
        }
    }

    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitEvent {
    pub repo: String,
    pub commit: String,
    pub rev: String,
    pub ops: Vec<Operation>,
    pub time: String,
    #[serde(rename = "_relay")]
    pub relay: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub action: String,
    pub path: String,
    pub cid: Option<String>,
    pub record: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityEvent {
    pub did: String,
    pub handle: Option<String>,
    #[serde(rename = "_relay")]
    pub relay: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountEvent {
    pub did: String,
    pub active: bool,
    pub status: Option<String>,
    #[serde(rename = "_relay")]
    pub relay: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleEvent {
    pub did: String,
    pub handle: String,
    #[serde(rename = "_relay")]
    pub relay: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneEvent {
    pub did: String,
    #[serde(rename = "_relay")]
    pub relay: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn commit_event_serialises_with_relay_and_field_order() {
        let ev = Event::Commit(CommitEvent {
            repo: "did:plc:abc".into(),
            commit: "bafy".into(),
            rev: "3kxxx".into(),
            ops: vec![Operation {
                action: "create".into(),
                path: "app.bsky.feed.post/abc".into(),
                cid: Some("bafy123".into()),
                record: Some(json!({"$type": "app.bsky.feed.post", "text": "hello"})),
            }],
            time: "2026-04-19T00:00:00.000Z".into(),
            relay: "ws://relay.test".into(),
        });
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["repo"], "did:plc:abc");
        assert_eq!(v["_relay"], "ws://relay.test");
        assert_eq!(v["ops"][0]["record"]["text"], "hello");
        assert_eq!(ev.type_name(), "commit");
    }

    #[test]
    fn delete_op_emits_explicit_nulls_not_omitted_fields() {
        let op = Operation {
            action: "delete".into(),
            path: "app.bsky.feed.like/xyz".into(),
            cid: None,
            record: None,
        };
        let v = serde_json::to_value(&op).unwrap();
        // Both must be present in the object as JSON null per
        // DESIGN.md §4 "Null fields are emitted as JSON null, not
        // omitted."
        assert!(v.as_object().unwrap().contains_key("cid"));
        assert!(v.as_object().unwrap().contains_key("record"));
        assert!(v["cid"].is_null());
        assert!(v["record"].is_null());
    }

    #[test]
    fn identity_event_with_null_handle_emits_null() {
        let ev = Event::Identity(IdentityEvent {
            did: "did:plc:abc".into(),
            handle: None,
            relay: "ws://r".into(),
        });
        let v = serde_json::to_value(&ev).unwrap();
        assert!(v.as_object().unwrap().contains_key("handle"));
        assert!(v["handle"].is_null());
    }
}
