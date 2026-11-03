# Regenerating golden compatibility files

**Status:** empty / placeholder. See the TODO at the top of
`src/golden_test.rs` — the golden test silently skips while
`tests/golden/expected/` has no files. This is deliberate for
phases 7–10; the real goldens land before production cutover in
phase 11.

## What this proves

Horizon-firehose replaces `firehose_consumer.py` in the Prism stack.
The downstream worker (`redis_consumer_worker.py`) reads from
`firehose:events` and doesn't care which producer wrote to the stream
— as long as the per-event JSON payload matches what it expects.

The golden files are the Python consumer's exact output for a curated
set of captured firehose frames. CI runs the Rust decoder on the
same inputs and diffs the JSON output. A mismatch is either a
compatibility regression (we broke the contract) or a deliberate
schema change (update the goldens with intent).

This is the compatibility-testing strategy from DESIGN.md §5
"compatibility" and adversarial round-5 finding F42 ("CI depending
on live Python consumer was over-engineered") — pre-compute once,
commit to the repo, regenerate only on schema changes.

## Directory layout

```
tests/golden/
├── REGENERATE.md           # you are here
├── fixtures/               # curated subset of captured frames
│   ├── 001-post-create.bin
│   ├── 002-post-delete.bin
│   ├── 003-like.bin
│   └── …                   # 20–50 frames, diverse $type coverage
└── expected/               # Python consumer's output for each
    ├── 001-post-create.json
    ├── 002-post-delete.json
    ├── 003-like.json
    └── …                   # exactly one JSON per fixture file
```

Filenames must match between `fixtures/` and `expected/` (sans
extension). The test pairs them by basename.

## Selection criteria for fixtures

Pick frames (from `tests/fixtures/` captures, or fresh
`capture-fixtures` runs) that cover:

- Every `$type` we need downstream compatibility for — at minimum:
  `app.bsky.feed.post` (create, update, delete), `app.bsky.feed.like`,
  `app.bsky.feed.repost`, `app.bsky.graph.follow`, `app.bsky.actor.profile`,
  `app.bsky.graph.block`.
- Non-commit events: `#identity`, `#account`, `#handle`, `#tombstone`.
- At least one `#info` frame with `OutdatedCursor`.
- Edge cases: long facet lists (>10 facets), large embeds
  (>100 KB), unicode-heavy text, null optional fields.

If you capture fresh frames, keep each one small (ideally <10 KB
when possible — avoid the 1 MB outliers unless they *are* the edge
case under test). Rename to the `NNN-description.bin` form above so
the test output is readable.

## Regeneration procedure

Pre-requisite: a working copy of the Hideaway dev environment with
the Python firehose consumer importable. On `hearth.nearhorizon.site`
or a local clone.

1. Assemble the fixture set under `tests/golden/fixtures/`. You can
   reuse existing `.bin` files from `tests/fixtures/` — copy, don't
   move (the bulk corpus there is for phase-3 decoder stats, not
   compatibility testing).

2. For each fixture `NNN-name.bin`, run the Python consumer's
   single-frame decoder and capture its Redis-stream-payload output
   as pretty-printed JSON:

   ```bash
   python3 scripts/decode_one_frame.py tests/golden/fixtures/NNN-name.bin \
       > tests/golden/expected/NNN-name.json
   ```

   (`scripts/decode_one_frame.py` is a thin wrapper around
   `firehose_consumer.py`'s per-frame decode path; it lives in the
   Prism repo and needs to be kept in sync with any firehose
   consumer changes there.)

3. Canonicalize the JSON so diffs are meaningful:

   ```bash
   for f in tests/golden/expected/*.json; do
       jq -S . "$f" > "$f.tmp" && mv "$f.tmp" "$f"
   done
   ```

   `jq -S` sorts object keys alphabetically. The Rust test canonicalizes
   its own output the same way before comparing.

4. Commit **both** the fixtures and the expected files in one change.

5. **Manually review the diff.** A regeneration that changes dozens
   of expected files unexpectedly is usually a sign the Python side
   drifted, not the Rust side — investigate before merging.

## What the Rust test checks

For every `tests/golden/fixtures/NNN-name.bin`:

1. Read the binary frame.
2. Call `decoder::decode_frame(&bytes, "ws://test-relay")`.
3. If the decoded variant is `DecodedFrame::Event { event, .. }`,
   serialize `event` via `event.to_json_bytes()`, parse back as
   `serde_json::Value`, canonicalize (sort keys), and compare to
   the expected JSON file loaded the same way.
4. If the decoded variant is `Skipped` / `Info` / `OutdatedCursor`,
   expect a matching sentinel shape in the expected file
   (TBD — see phase-11 follow-up; this is the "skip-or-control"
   bucket and the goldens for those are thinner).

The test does **not** exercise the publisher, router, or Redis —
only the decoder → event → JSON path. Publisher-level behavior
(MAXLEN trimming, cursor advancement) is already covered by
`src/pipeline_test.rs` and `src/pipeline_main_test.rs`.

## When the test fires vs when it skips

- **Skip**: `tests/golden/fixtures/` is empty, or every fixture has
  no corresponding `expected/NNN.json`. The test prints a SKIP
  message (stderr) and returns OK. CI does not fail.
- **Fail**: any fixture/expected pair mismatches. The test prints
  the first diff it finds with a unified-diff-style view.
- **Pass**: every pair matches.

This means committing new fixtures without regenerating expecteds
is safe (they just don't participate in the check). Committing new
expecteds without fixtures is also safe (same). The test only
compares pairs that exist on both sides.
