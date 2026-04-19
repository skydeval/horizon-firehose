//! Phase 4: record-type filter.
//!
//! Sits between the decoder and the publisher. Takes `(Event, seq)`
//! pairs, drops or rewrites them according to `filter.record_types`
//! config, forwards survivors to the publisher's channel.
//!
//! # Filter semantics (DESIGN.md §2 goal 3, §5 fixture-coverage)
//!
//! - **Empty filter** (`record_types = []`) → pass everything through
//!   unchanged.
//! - **Commits** — filter each op:
//!     - create/update: match the record's `$type` against the filter
//!       set.
//!     - delete: the record body is gone, so there's no `$type`; match
//!       on the *collection prefix* of the op path instead
//!       (`"app.bsky.feed.post/3k…"` → `"app.bsky.feed.post"`). This
//!       mirrors how `redis_consumer_worker.py` dispatches deletes: by
//!       the collection derived from the path, not by the absent record.
//!
//!   A commit whose entire op list filters out is dropped — we never
//!   emit a commit with zero ops, because the downstream worker
//!   interprets that as "nothing happened" and would waste a read.
//! - **Identity / account / handle / tombstone** — always pass through.
//!   They're not record-bearing; filtering them would break AppViews
//!   that rely on identity updates regardless of record-type config.

use std::collections::HashSet;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::debug;

use crate::event::{CommitEvent, Event};
use crate::publisher::PublishOp;

/// Outcome of filtering a single event. No `PartialEq` — `Event`
/// contains `serde_json::Value` (records) which isn't `Eq` (it can hold
/// f64). Tests use `matches!` / pattern match instead.
#[derive(Debug)]
pub enum FilterOutcome {
    /// Forward the (possibly-rewritten) event.
    Pass(Event),
    /// Drop the event entirely.
    Drop,
}

/// Pure filter function. Exposed separately from [`spawn`] so it's
/// trivial to unit-test without wiring up channels.
pub fn filter_event(event: Event, filter: &HashSet<String>) -> FilterOutcome {
    if filter.is_empty() {
        return FilterOutcome::Pass(event);
    }

    match event {
        Event::Commit(commit) => filter_commit(commit, filter),
        // Non-record-bearing events always pass through per DESIGN.md
        // §3 — filtering them would lose identity updates that the
        // downstream indexer uses regardless of what record types it
        // otherwise cares about.
        other => FilterOutcome::Pass(other),
    }
}

fn filter_commit(mut commit: CommitEvent, filter: &HashSet<String>) -> FilterOutcome {
    commit.ops.retain(|op| op_matches_filter(op, filter));
    if commit.ops.is_empty() {
        FilterOutcome::Drop
    } else {
        FilterOutcome::Pass(Event::Commit(commit))
    }
}

fn op_matches_filter(op: &crate::event::Operation, filter: &HashSet<String>) -> bool {
    // Prefer the record body's `$type` — that's authoritative. If
    // there's no record (delete op, or a malformed create), fall back
    // to the path's collection prefix.
    if let Some(record) = &op.record {
        if let Some(t) = record.get("$type").and_then(|v| v.as_str()) {
            return filter.contains(t);
        }
    }
    // Path format: `{collection}/{rkey}`. We never see delete ops
    // without a `/`, but `split_once` guards us anyway.
    let collection = op.path.split_once('/').map(|(c, _)| c).unwrap_or(&op.path);
    filter.contains(collection)
}

/// Options for the spawned router task.
#[derive(Debug, Clone)]
pub struct RouterOptions {
    /// Allow-list of NSIDs. Empty = pass everything.
    pub record_types: Vec<String>,
}

/// Counters surfaced on shutdown.
#[derive(Debug, Default, Clone, Copy)]
pub struct RouterStats {
    pub events_in: u64,
    pub events_out: u64,
    pub events_dropped: u64,
}

pub struct RouterHandle {
    pub(crate) task: JoinHandle<RouterStats>,
    shutdown_tx: watch::Sender<bool>,
}

impl RouterHandle {
    /// Signal shutdown, await the task, return cumulative stats.
    pub async fn shutdown(self) -> RouterStats {
        let _ = self.shutdown_tx.send(true);
        self.task.await.unwrap_or_default()
    }
}

/// Spawn the router task. Reads [`PublishOp`]s from `input`, applies
/// the filter to the event variant, and forwards survivors to
/// `output`. Skip-advance ops pass through unchanged — they're not
/// record-bearing (Phase 8.5 review finding 1.1).
///
/// The task exits when:
/// - `input` closes (decoder has exited and drained) — the normal
///   shutdown path via §3 ordering.
/// - `shutdown_tx` flips — caller-initiated early stop.
/// - `output` is closed — the publisher has gone away; no point
///   continuing.
pub fn spawn(
    opts: RouterOptions,
    mut input: mpsc::Receiver<PublishOp>,
    output: mpsc::Sender<PublishOp>,
) -> RouterHandle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let filter: HashSet<String> = opts.record_types.into_iter().collect();

    let task = tokio::spawn(async move {
        let mut stats = RouterStats::default();

        loop {
            // Same drain-first ordering as the publisher (see
            // publisher.rs for the rationale): pending events are
            // processed before an explicit shutdown signal is honoured.
            tokio::select! {
                biased;
                recv = input.recv() => {
                    let Some(op) = recv else {
                        debug!(?stats, "router: input channel closed");
                        break;
                    };
                    stats.events_in += 1;
                    let forwarded = match op {
                        // Skip-advance passes through untouched; it
                        // carries no record body to filter on.
                        PublishOp::Skip { .. } => Some(op),
                        PublishOp::Publish(event, seq) => {
                            match filter_event(event, &filter) {
                                FilterOutcome::Pass(ev) => {
                                    Some(PublishOp::Publish(ev, seq))
                                }
                                FilterOutcome::Drop => {
                                    stats.events_dropped += 1;
                                    None
                                }
                            }
                        }
                    };
                    if let Some(op) = forwarded {
                        if output.send(op).await.is_err() {
                            debug!("router: output channel closed");
                            break;
                        }
                        stats.events_out += 1;
                    }
                }
                _ = shutdown_rx.changed() => {
                    debug!(?stats, "router: shutdown signalled with empty channel");
                    break;
                }
            }
        }

        stats
    });

    RouterHandle { task, shutdown_tx }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use crate::event::{
        AccountEvent, CommitEvent, HandleEvent, IdentityEvent, Operation, TombstoneEvent,
    };

    fn commit(ops: Vec<Operation>) -> Event {
        Event::Commit(CommitEvent {
            repo: "did:plc:abc".into(),
            commit: "bafycommit".into(),
            rev: "3k2la".into(),
            ops,
            time: "2026-04-19T00:00:00.000Z".into(),
            relay: "ws://test".into(),
        })
    }

    fn op_with_type(t: &str, action: &str) -> Operation {
        Operation {
            action: action.into(),
            path: format!("{t}/abc123"),
            cid: Some("bafy".into()),
            record: Some(json!({"$type": t, "text": "hi"})),
        }
    }

    fn delete_op(collection: &str) -> Operation {
        Operation {
            action: "delete".into(),
            path: format!("{collection}/xyz"),
            cid: None,
            record: None,
        }
    }

    fn filter(nsids: &[&str]) -> HashSet<String> {
        nsids.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn empty_filter_passes_everything() {
        let f = HashSet::new();
        let ev = commit(vec![op_with_type("app.bsky.feed.post", "create")]);
        assert!(matches!(filter_event(ev, &f), FilterOutcome::Pass(_)));
    }

    #[test]
    fn filter_retains_only_matching_ops_on_a_commit() {
        let f = filter(&["app.bsky.feed.post"]);
        let ev = commit(vec![
            op_with_type("app.bsky.feed.post", "create"),
            op_with_type("app.bsky.feed.like", "create"),
            op_with_type("app.bsky.graph.follow", "create"),
        ]);
        match filter_event(ev, &f) {
            FilterOutcome::Pass(Event::Commit(c)) => {
                assert_eq!(c.ops.len(), 1);
                assert_eq!(
                    c.ops[0].record.as_ref().unwrap()["$type"],
                    "app.bsky.feed.post"
                );
            }
            other => panic!("wrong outcome: {other:?}"),
        }
    }

    #[test]
    fn commit_with_all_ops_filtered_is_dropped() {
        let f = filter(&["app.bsky.feed.post"]);
        let ev = commit(vec![
            op_with_type("app.bsky.feed.like", "create"),
            op_with_type("app.bsky.graph.follow", "create"),
        ]);
        assert!(matches!(filter_event(ev, &f), FilterOutcome::Drop));
    }

    #[test]
    fn delete_ops_match_by_collection_prefix() {
        let f = filter(&["app.bsky.feed.post"]);
        let ev = commit(vec![delete_op("app.bsky.feed.post")]);
        match filter_event(ev, &f) {
            FilterOutcome::Pass(Event::Commit(c)) => {
                assert_eq!(c.ops.len(), 1);
                assert_eq!(c.ops[0].action, "delete");
            }
            other => panic!("delete should pass through: {other:?}"),
        }

        let f2 = filter(&["app.bsky.feed.like"]);
        let ev2 = commit(vec![delete_op("app.bsky.feed.post")]);
        assert!(matches!(filter_event(ev2, &f2), FilterOutcome::Drop));
    }

    #[test]
    fn identity_events_always_pass_regardless_of_filter() {
        let f = filter(&["app.bsky.feed.post"]);
        let ev = Event::Identity(IdentityEvent {
            did: "did:plc:x".into(),
            handle: Some("alice.test".into()),
            relay: "ws://t".into(),
        });
        assert!(matches!(filter_event(ev, &f), FilterOutcome::Pass(_)));
    }

    #[test]
    fn account_events_always_pass_regardless_of_filter() {
        let f = filter(&["app.bsky.feed.post"]);
        let ev = Event::Account(AccountEvent {
            did: "did:plc:x".into(),
            active: false,
            status: Some("deactivated".into()),
            relay: "ws://t".into(),
        });
        assert!(matches!(filter_event(ev, &f), FilterOutcome::Pass(_)));
    }

    #[test]
    fn handle_and_tombstone_events_always_pass_regardless_of_filter() {
        let f = filter(&["app.bsky.feed.post"]);
        assert!(matches!(
            filter_event(
                Event::Handle(HandleEvent {
                    did: "did:plc:x".into(),
                    handle: "x.test".into(),
                    relay: "ws://t".into(),
                }),
                &f,
            ),
            FilterOutcome::Pass(_)
        ));
        assert!(matches!(
            filter_event(
                Event::Tombstone(TombstoneEvent {
                    did: "did:plc:x".into(),
                    relay: "ws://t".into(),
                }),
                &f,
            ),
            FilterOutcome::Pass(_)
        ));
    }

    #[test]
    fn unknown_type_is_dropped_when_filter_is_nonempty_and_no_match() {
        // Unknown `$type` with record present but type not in filter.
        let f = filter(&["app.bsky.feed.post"]);
        let op = Operation {
            action: "create".into(),
            path: "app.custom.thing/abc".into(),
            cid: Some("bafy".into()),
            record: Some(json!({"$type": "app.custom.thing", "x": 1})),
        };
        assert!(matches!(
            filter_event(commit(vec![op]), &f),
            FilterOutcome::Drop,
        ));
    }

    #[tokio::test]
    async fn spawn_forwards_only_surviving_events() {
        let (in_tx, in_rx) = mpsc::channel::<PublishOp>(16);
        let (out_tx, mut out_rx) = mpsc::channel::<PublishOp>(16);

        let handle = spawn(
            RouterOptions {
                record_types: vec!["app.bsky.feed.post".into()],
            },
            in_rx,
            out_tx,
        );

        // Send 3 events: one passes, one drops (wrong type), one is
        // identity (always passes).
        in_tx
            .send(PublishOp::Publish(
                commit(vec![op_with_type("app.bsky.feed.post", "create")]),
                1,
            ))
            .await
            .unwrap();
        in_tx
            .send(PublishOp::Publish(
                commit(vec![op_with_type("app.bsky.feed.like", "create")]),
                2,
            ))
            .await
            .unwrap();
        in_tx
            .send(PublishOp::Publish(
                Event::Identity(IdentityEvent {
                    did: "did:plc:x".into(),
                    handle: None,
                    relay: "ws://t".into(),
                }),
                3,
            ))
            .await
            .unwrap();

        // Drop the input so the router exits cleanly.
        drop(in_tx);

        let mut seqs = Vec::new();
        while let Some(op) = out_rx.recv().await {
            if let PublishOp::Publish(_, seq) = op {
                seqs.push(seq);
            }
        }
        assert_eq!(seqs, vec![1, 3]);

        let stats = handle.shutdown().await;
        assert_eq!(stats.events_in, 3);
        assert_eq!(stats.events_out, 2);
        assert_eq!(stats.events_dropped, 1);
    }
}
