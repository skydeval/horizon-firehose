# horizon-firehose

[![CI](https://github.com/skydeval/horizon-firehose/actions/workflows/ci.yml/badge.svg)](https://github.com/skydeval/horizon-firehose/actions/workflows/ci.yml)

A Rust-based ATProto firehose consumer with CBOR/CAR decoding, per-relay failover, and Redis stream publishing.

**Status:** in active development. See [DESIGN.md](DESIGN.md) for the full specification.

## Working with proto-blue

This project depends on [proto-blue](https://github.com/dollspace-gay/proto-blue) (Doll's comprehensive ATProto Rust SDK), currently pinned by commit SHA because the project hasn't cut tagged releases yet.

To upgrade proto-blue:
1. Find the target commit SHA from the proto-blue repository
2. Update the `rev = "..."` value for all proto-blue dependencies in `Cargo.toml`
3. Run `cargo check` to verify compatibility
4. Update `.github/expected-proto-blue-sha.txt` to the new SHA (CI's integrity check — see `.github/verify-proto-blue-sha.sh`)
5. Run the full test suite (`cargo test`) before committing the bump
6. Commit `Cargo.toml`, `Cargo.lock`, and the expected-sha file together. Note the SHA change in the commit message.

When proto-blue publishes tagged releases, this project will switch to tag-based pinning for easier tracking.

## License

MIT OR Apache-2.0