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
    /// `#sync`, published only with `publisher.publish_sync = true`.
    Sync(SyncEvent),
    /// A loss point, published only with `publisher.publish_resume =
    /// true`. Built by the publisher, never decoded from a frame, and
    /// XADDed without a stream-level `seq`.
    Resume(ResumeEvent),
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
            Event::Sync(_) => "sync",
            Event::Resume(_) => "resume",
        }
    }

    pub fn relay(&self) -> &str {
        match self {
            Event::Commit(e) => &e.relay,
            Event::Identity(e) => &e.relay,
            Event::Account(e) => &e.relay,
            Event::Handle(e) => &e.relay,
            Event::Tombstone(e) => &e.relay,
            Event::Sync(e) => &e.relay,
            Event::Resume(e) => &e.relay,
        }
    }

    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Serialise with the `_cap` field appended as the last key.
    /// `cap_field` is [`Capabilities::field_bytes`]: `"_cap":{…}`,
    /// computed once at startup. Every event serialises as a non-empty
    /// JSON object (all carry `_relay`), so splicing before the closing
    /// brace is always valid and avoids building a `serde_json::Value`
    /// per event.
    pub fn to_json_bytes_with_cap(&self, cap_field: &[u8]) -> serde_json::Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec(self)?;
        debug_assert_eq!(bytes.last(), Some(&b'}'));
        bytes.pop();
        bytes.push(b',');
        bytes.extend_from_slice(cap_field);
        bytes.push(b'}');
        Ok(bytes)
    }
}

/// What this sidecar puts on the stream, stamped on every entry as
/// `_cap` so a consumer can tell from any single entry what to expect:
/// `{"v": 1, "caps": [...], "record_types": [...]}`. `caps` lists the
/// enabled optional behaviours in a fixed order; `record_types` is
/// present only when the record filter is non-empty. An entry with no
/// `_cap` at all comes from a sidecar older than 0.2.0.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// `publisher.publish_sync`.
    pub sync: bool,
    /// `publisher.publish_resume`.
    pub resume: bool,
    /// `filter.forward_opless_commits`.
    pub opless_filtered: bool,
    /// `filter.record_types`.
    pub record_types: Vec<String>,
}

impl Capabilities {
    pub const VERSION: u32 = 1;

    pub fn caps(&self) -> Vec<&'static str> {
        let mut caps = Vec::new();
        if self.sync {
            caps.push("sync");
        }
        if self.resume {
            caps.push("resume");
        }
        if self.opless_filtered {
            caps.push("oplessFiltered");
        }
        caps
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut v = serde_json::json!({ "v": Self::VERSION, "caps": self.caps() });
        if !self.record_types.is_empty() {
            v["record_types"] = serde_json::json!(self.record_types);
        }
        v
    }

    /// `"_cap":{…}` as bytes, for [`Event::to_json_bytes_with_cap`].
    pub fn field_bytes(&self) -> Vec<u8> {
        let mut out = b"\"_cap\":".to_vec();
        out.extend_from_slice(self.to_json().to_string().as_bytes());
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitEvent {
    pub repo: String,
    pub commit: String,
    pub rev: String,
    /// The `#commit` frame's `since`: the rev of the repo's previous
    /// commit, or null (the repo's first commit, or a frame that did
    /// not carry a string). Consumers use it to tell whether they have
    /// seen every commit of a repo in sequence.
    pub since: Option<String>,
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

/// `#sync`: a repo's current head, announced without operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEvent {
    pub did: String,
    pub rev: String,
    /// The frame's `time`, or null if it did not carry a string.
    pub time: Option<String>,
    #[serde(rename = "_relay")]
    pub relay: String,
}

/// Why a `resume` entry was published.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeReason {
    /// The relay rejected the cursor as stale (`OutdatedCursor`, under
    /// `on_stale_cursor = "live_tip"`) and continued from its oldest
    /// retained event.
    OutdatedCursor,
    /// A dial went out with no saved cursor for the relay, so the
    /// relay started at its live tip.
    NoSavedCursor,
    /// The replay guard concluded the relay reset its numbering and
    /// rebased the cursor (`on_stale_cursor = "live_tip"`).
    RelaySeqReset,
}

/// A loss point on one relay. Events with seqs in
/// `[first_lost_seq, continues_from)` were not published. Either bound
/// can be unknown: `saved_cursor` and `first_lost_seq` are null when
/// there was no cursor for the relay yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeEvent {
    pub reason: ResumeReason,
    /// The relay's published cursor before the loss.
    pub saved_cursor: Option<u64>,
    /// First seq not published.
    pub first_lost_seq: Option<u64>,
    /// Seq of the first event after the loss.
    pub continues_from: u64,
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
            since: Some("3kwww".into()),
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
        assert_eq!(v["since"], "3kwww");
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

    fn tombstone() -> Event {
        Event::Tombstone(TombstoneEvent {
            did: "did:plc:abc".into(),
            relay: "ws://r".into(),
        })
    }

    #[test]
    fn cap_with_nothing_enabled_is_version_and_empty_caps() {
        let caps = Capabilities::default();
        assert_eq!(caps.to_json(), json!({"v": 1, "caps": []}));
        let bytes = tombstone()
            .to_json_bytes_with_cap(&caps.field_bytes())
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["_cap"], json!({"v": 1, "caps": []}));
        assert_eq!(v["did"], "did:plc:abc");
        assert_eq!(v["_relay"], "ws://r");
    }

    #[test]
    fn cap_lists_enabled_caps_in_fixed_order_and_record_types_only_when_filtering() {
        let all = Capabilities {
            sync: true,
            resume: true,
            opless_filtered: true,
            record_types: vec!["app.bsky.feed.post".into()],
        };
        assert_eq!(
            all.to_json(),
            json!({
                "v": 1,
                "caps": ["sync", "resume", "oplessFiltered"],
                "record_types": ["app.bsky.feed.post"],
            })
        );
        let unfiltered = Capabilities {
            sync: true,
            ..Capabilities::default()
        };
        let v = unfiltered.to_json();
        assert_eq!(v["caps"], json!(["sync"]));
        assert!(v.get("record_types").is_none());
    }

    #[test]
    fn cap_is_appended_to_every_event_kind_as_valid_json() {
        let cap = Capabilities::default().field_bytes();
        let events = vec![
            tombstone(),
            Event::Sync(SyncEvent {
                did: "did:plc:abc".into(),
                rev: "3kaaa".into(),
                time: None,
                relay: "ws://r".into(),
            }),
            Event::Resume(ResumeEvent {
                reason: ResumeReason::NoSavedCursor,
                saved_cursor: None,
                first_lost_seq: None,
                continues_from: 7,
                relay: "ws://r".into(),
            }),
        ];
        for ev in events {
            let bytes = ev.to_json_bytes_with_cap(&cap).unwrap();
            let v: serde_json::Value = serde_json::from_slice(&bytes)
                .unwrap_or_else(|e| panic!("{} not valid JSON: {e}", ev.type_name()));
            assert_eq!(v["_cap"]["v"], 1, "{}", ev.type_name());
        }
    }

    #[test]
    fn sync_and_resume_serialise_with_explicit_nulls() {
        let sync = serde_json::to_value(Event::Sync(SyncEvent {
            did: "did:plc:abc".into(),
            rev: "3kaaa".into(),
            time: None,
            relay: "ws://r".into(),
        }))
        .unwrap();
        assert_eq!(sync["rev"], "3kaaa");
        assert!(sync.as_object().unwrap().contains_key("time"));

        let resume = Event::Resume(ResumeEvent {
            reason: ResumeReason::OutdatedCursor,
            saved_cursor: Some(10),
            first_lost_seq: Some(11),
            continues_from: 50,
            relay: "ws://r".into(),
        });
        assert_eq!(resume.type_name(), "resume");
        let v = serde_json::to_value(&resume).unwrap();
        assert_eq!(v["reason"], "outdated_cursor");
        assert_eq!(v["saved_cursor"], 10);
        assert_eq!(v["first_lost_seq"], 11);
        assert_eq!(v["continues_from"], 50);
        assert_eq!(v["_relay"], "ws://r");
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
