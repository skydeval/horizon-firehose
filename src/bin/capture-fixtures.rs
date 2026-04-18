//! capture-fixtures — raw frame capture tool for test fixtures.
//!
//! Connects to an ATProto relay via WebSocket and writes raw frames to disk
//! with metadata (timestamp, sequence number). Used to build the fixture
//! corpus for decoder testing and golden file generation.
//!
//! Implementation lives in phase 2.5 of the build plan. This stub exists so
//! the project compiles cleanly from phase 0.

fn main() {
    eprintln!("capture-fixtures is not yet implemented (see DESIGN.md phase 2.5)");
    std::process::exit(1);
}
