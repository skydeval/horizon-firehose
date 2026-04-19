# Upstream issue draft — proto-blue-repo

**Filed as:** `dollspace-gay/proto-blue` issue (new).

**Intended submitter:** Chrys

## Title

Varint overflow in `read_car` causes decoder panic on hostile CAR input

## Body

Hi! Surfaced during Phase 8 adversarial review of [horizon-firehose](https://github.com/skydeval/horizon-firehose).

### Claim

`read_car` in `proto-blue-repo/src/car.rs` reads `block_len: u64` from a varint, casts to `usize`, then computes `pos + block_len` before slicing `&data[pos..pos + block_len]`. On 64-bit release builds this wraps silently (no overflow-checks in release), and the slice access panics on `start > end`.

Attack vector: a hostile relay crafts a CAR (inside a `#commit` frame's `blocks` field) whose varint block-length byte sequence decodes to a value near `u64::MAX`. On our end the decoder task catches the panic via `catch_unwind`, but repeated occurrences push the pipeline toward forced failover without actually decoding anything, and on older tokio runtimes the panic can still crash the task.

### Reproduction

```rust
// A CAR preamble followed by a varint encoding u64::MAX - 1, then no content.
// read_car's `pos + block_len` overflows to a small usize, then:
//   &data[pos..pos + block_len]   // panics: slice index start > end
```

### Proposed fix

```rust
let end = pos
    .checked_add(block_len)
    .and_then(|candidate| if candidate > data.len() { None } else { Some(candidate) })
    .ok_or(CarError::BlockLenOverflow { pos, block_len, data_len: data.len() })?;
let block = &data[pos..end];
```

Also rejects legitimately-too-long blocks (`block_len > data.len() - pos`) with a structured error instead of panicking.

### Horizon-firehose side

- We catch the panic at the decoder task boundary via `catch_unwind` ([src/decoder.rs Phase 8.5 panic isolation](https://github.com/skydeval/horizon-firehose/blob/main/src/decoder.rs)), so a single malformed CAR doesn't kill us.
- Still counts as a decoder panic and contributes to the 10-consecutive-error circuit breaker that forces relay failover.
- Happy to submit a PR with the `checked_add` fix.
