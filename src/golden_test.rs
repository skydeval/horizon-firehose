//! Phase 7: golden-file compatibility test harness.
//!
//! Pairs captured firehose frames under `tests/golden/fixtures/`
//! with expected JSON under `tests/golden/expected/` (pre-computed
//! by the Python consumer), runs the Rust decoder on each fixture,
//! and diffs canonicalised output against the expected file.
//!
//! **Phase 7 state:** both directories are empty/stubbed. The test
//! below is the full harness — it scans the directories, finds any
//! matching pairs, and skips cleanly when there are zero pairs.
//! Populating the goldens is a follow-up before phase 11 production
//! cutover; see `tests/golden/REGENERATE.md` for procedure.
//!
//! The test is designed to **never fail CI just because goldens are
//! absent**. It fails only when a fixture/expected pair mismatches.
//! This keeps CI green while the harness is in place, and flips to
//! a real signal the moment goldens land.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::decoder::{DecodedFrame, decode_frame};

const RELAY_FOR_GOLDENS: &str = "ws://golden-test-relay";

/// A fixture/expected pair ready for comparison.
struct GoldenPair {
    name: String,
    fixture: PathBuf,
    expected: PathBuf,
}

/// Enumerate `.bin` files under `fixtures/` that have a matching
/// `.json` file under `expected/`. Fixtures without expected-side
/// counterparts are *skipped* (not an error) — it's valid to commit
/// raw frames that you haven't regenerated goldens for yet.
fn collect_pairs() -> Vec<GoldenPair> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let fixtures_dir = root.join("fixtures");
    let expected_dir = root.join("expected");
    if !fixtures_dir.is_dir() || !expected_dir.is_dir() {
        return Vec::new();
    }

    let mut pairs = Vec::new();
    let entries = match std::fs::read_dir(&fixtures_dir) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("bin") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let expected = expected_dir.join(format!("{stem}.json"));
        if expected.exists() {
            pairs.push(GoldenPair {
                name: stem.to_string(),
                fixture: path,
                expected,
            });
        }
    }
    pairs.sort_by(|a, b| a.name.cmp(&b.name));
    pairs
}

/// Recursively sort object keys so two semantically equal JSON
/// documents compare byte-equal after re-serialisation. Matches what
/// `jq -S` does for the Python goldens (see REGENERATE.md).
fn canonicalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<String, serde_json::Value> =
                map.into_iter().map(|(k, v)| (k, canonicalize(v))).collect();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
}

#[test]
fn golden_pairs_roundtrip_through_decoder() {
    let pairs = collect_pairs();
    if pairs.is_empty() {
        // TODO(phase-11-before-cutover): this SKIP flips to a hard
        // expectation once the Python-generated goldens are committed.
        // Goldens are empty until regeneration — skip or warn.
        // See tests/golden/REGENERATE.md for procedure.
        eprintln!(
            "SKIP golden_pairs_roundtrip_through_decoder: \
             no fixture/expected pairs under tests/golden/. \
             See tests/golden/REGENERATE.md."
        );
        return;
    }

    let mut failures: Vec<String> = Vec::new();

    for pair in &pairs {
        let bytes = match std::fs::read(&pair.fixture) {
            Ok(b) => b,
            Err(e) => {
                failures.push(format!("{}: could not read fixture: {e}", pair.name));
                continue;
            }
        };
        let expected_raw = match std::fs::read_to_string(&pair.expected) {
            Ok(s) => s,
            Err(e) => {
                failures.push(format!("{}: could not read expected: {e}", pair.name));
                continue;
            }
        };
        let expected_val: serde_json::Value = match serde_json::from_str(&expected_raw) {
            Ok(v) => canonicalize(v),
            Err(e) => {
                failures.push(format!("{}: expected JSON parse failed: {e}", pair.name));
                continue;
            }
        };

        let decoded = match decode_frame(&bytes, RELAY_FOR_GOLDENS) {
            Ok(d) => d,
            Err(e) => {
                failures.push(format!("{}: decoder errored: {e}", pair.name));
                continue;
            }
        };

        let got_val = match &decoded {
            DecodedFrame::Event { event, .. } => {
                let bytes = event.to_json_bytes().expect("event serialises");
                let parsed: serde_json::Value = serde_json::from_slice(&bytes)
                    .expect("event JSON round-trips to serde_json::Value");
                canonicalize(parsed)
            }
            // Non-event variants: sentinel shapes. The
            // `REGENERATE.md` spells out the contract (and notes it's
            // underspecified until phase 11). For now, fail with a
            // clear message so the human regenerating knows to design
            // the sentinel shape for the variant they're committing.
            other => {
                failures.push(format!(
                    "{}: fixture decoded to non-event variant ({other:?}); \
                     sentinel shape for non-event goldens is TBD — \
                     see REGENERATE.md phase-11 note",
                    pair.name
                ));
                continue;
            }
        };

        if got_val != expected_val {
            // Keep the failure message short; the full diff is easy to
            // get from `jq` locally. We print the first mismatching
            // top-level key so the cause is obvious.
            let first_diff = diff_top_level(&got_val, &expected_val);
            failures.push(format!(
                "{}: output differs from expected — first diff at: {first_diff}",
                pair.name
            ));
        }
    }

    if failures.is_empty() {
        eprintln!(
            "golden_pairs_roundtrip_through_decoder: {} pair(s) matched",
            pairs.len()
        );
    } else {
        let joined = failures.join("\n  ");
        panic!(
            "{}/{} golden pair(s) mismatched:\n  {joined}",
            failures.len(),
            pairs.len()
        );
    }
}

/// Report the first top-level JSON key whose value differs between
/// `got` and `expected`. Useful for a short, readable failure line
/// without dumping the whole diff.
fn diff_top_level(got: &serde_json::Value, expected: &serde_json::Value) -> String {
    use serde_json::Value::Object;
    let (Object(g), Object(e)) = (got, expected) else {
        return "(top-level type mismatch)".to_string();
    };
    for (k, gv) in g {
        match e.get(k) {
            Some(ev) if ev == gv => continue,
            Some(_) => return format!("field `{k}` differs"),
            None => return format!("field `{k}` present in got but missing in expected"),
        }
    }
    for k in e.keys() {
        if !g.contains_key(k) {
            return format!("field `{k}` present in expected but missing in got");
        }
    }
    "(no top-level diff found — check nested arrays/objects)".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonicalize_sorts_nested_keys_and_preserves_arrays() {
        let v = json!({
            "b": 2,
            "a": {"zz": 1, "aa": 2},
            "arr": [{"y": 1, "x": 2}, {"q": 3, "p": 4}],
        });
        let c = canonicalize(v);
        let s = serde_json::to_string(&c).unwrap();
        // Top-level: keys sorted.
        assert!(s.starts_with(r#"{"a":"#));
        // Nested object keys sorted: "aa" before "zz".
        assert!(s.contains(r#""aa":2,"zz":1"#));
        // Arrays preserve order; array-inner-object keys sorted.
        let expected_arr = r#""arr":[{"x":2,"y":1},{"p":4,"q":3}]"#;
        assert!(s.contains(expected_arr), "expected {expected_arr} in {s}");
    }

    #[test]
    fn diff_top_level_flags_missing_or_differing_fields() {
        let a = json!({"x": 1, "y": 2});
        let b = json!({"x": 1, "y": 3});
        let d = diff_top_level(&a, &b);
        assert!(d.contains("y"), "got: {d}");

        let c = json!({"x": 1});
        let d2 = diff_top_level(&a, &c);
        assert!(d2.contains("y"), "got: {d2}");
    }
}
