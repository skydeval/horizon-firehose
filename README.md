# horizon-firehose

A Rust-based ATProto firehose consumer with CBOR/CAR decoding, per-relay failover, and Redis stream publishing.

**Status:** in active development. See [DESIGN.md](DESIGN.md) for the full specification.

## Working with proto-blue

This project depends on [proto-blue](https://github.com/dollspace-gay/proto-blue) (Doll's comprehensive ATProto Rust SDK), currently pinned by commit SHA because the project hasn't cut tagged releases yet.

To upgrade proto-blue:
1. Find the target commit SHA from the proto-blue repository
2. Update the `rev = "..."` value for all proto-blue dependencies in `Cargo.toml`
3. Run `cargo check` to verify compatibility
4. Run the full test suite (`cargo test`) before committing the bump
5. Note the SHA change in the commit message

When proto-blue publishes tagged releases, this project will switch to tag-based pinning for easier tracking.

## License

MIT OR Apache-2.0