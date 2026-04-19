#!/usr/bin/env bash
# Verify the proto-blue git dependency resolves to the SHA recorded in
# `.github/expected-proto-blue-sha.txt`. CI integrity check per
# DESIGN.md round-3 finding F26 (catch force-pushes and silent drift).
#
# proto-blue ships as six crates from one repo. Cargo.lock records
# each with its own `source` line. Normally:
#   source = "git+https://github.com/dollspace-gay/proto-blue?rev=<sha>#<sha>"
# While a temporary `[patch]` override redirects to a fork (see
# Cargo.toml for why):
#   source = "git+https://github.com/<fork>/proto-blue?branch=<name>#<sha>"
# We match either form and extract the SHA *after the `#`* — this
# is the resolved commit Cargo actually compiles, regardless of the
# declared pin shape. When the patch is dropped, the expected SHA
# file should flip back to the upstream rev.
#
# Exit codes:
#   0 — all proto-blue crates resolve to the expected SHA.
#   1 — mismatch, or proto-blue crates disagree with each other.
#   2 — environment / parsing error (missing file, unexpected format).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXPECTED_FILE="$SCRIPT_DIR/expected-proto-blue-sha.txt"
CARGO_LOCK="$REPO_ROOT/Cargo.lock"

if [[ ! -r "$EXPECTED_FILE" ]]; then
    echo "error: $EXPECTED_FILE is not readable" >&2
    exit 2
fi
if [[ ! -r "$CARGO_LOCK" ]]; then
    echo "error: $CARGO_LOCK is not readable — run 'cargo fetch' to generate" >&2
    exit 2
fi

EXPECTED="$(tr -d '[:space:]' < "$EXPECTED_FILE")"
if [[ ! "$EXPECTED" =~ ^[a-f0-9]{40}$ ]]; then
    echo "error: expected SHA is not a 40-char hex string: '$EXPECTED'" >&2
    exit 2
fi

# Extract every proto-blue git source line from Cargo.lock, pull the
# resolved SHA after `#` (works for both `?rev=…` upstream pins and
# `?branch=…` fork-patch redirects), dedup. We expect one unique value.
RESOLVED="$(grep -E '^source = "git\+https://github\.com/[^/]+/proto-blue[?#]' "$CARGO_LOCK" \
    | sed -E 's/.*#([a-f0-9]{40}).*/\1/' \
    | sort -u || true)"

if [[ -z "$RESOLVED" ]]; then
    echo "error: no proto-blue git sources found in Cargo.lock" >&2
    exit 2
fi

if [[ "$(echo "$RESOLVED" | wc -l)" -gt 1 ]]; then
    echo "error: proto-blue crates disagree on SHA — Cargo.toml should pin them all to the same rev" >&2
    echo "resolved SHAs:" >&2
    echo "$RESOLVED" >&2
    exit 1
fi

if [[ "$RESOLVED" != "$EXPECTED" ]]; then
    cat >&2 <<EOF
error: proto-blue SHA drift detected
  expected: $EXPECTED  (from .github/expected-proto-blue-sha.txt)
  resolved: $RESOLVED  (from Cargo.lock)

If this drift is intentional:
  1. Update Cargo.toml's 'rev' for every proto-blue-* crate.
  2. Run 'cargo update -p proto-blue-api' (or 'cargo build') to refresh Cargo.lock.
  3. Update .github/expected-proto-blue-sha.txt to the new SHA.
  4. Commit both Cargo.lock and the expected-sha file in the same change.

If you didn't touch proto-blue, this likely means upstream force-pushed
the tag/branch we're tracking. Do NOT blindly update — review the new
commits first.
EOF
    exit 1
fi

echo "proto-blue SHA matches expected: $EXPECTED"
