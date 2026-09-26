# Changelog

All notable changes to `ringfire` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Network mirrors** (`ringfire::replication`): `ReplicaServer` streams a ring to other
  hosts over TCP; `Mirror` keeps a ring with the same geometry and the same sequence
  numbers in the local `/dev/shm`, so readers attach to it as they would on the source
  host. Raw slot payloads, own binary protocol with a 16-byte frame header
  (`HELLO` / `GEOMETRY` / `DATA` / `GAP` / `HEARTBEAT`), batching, resume from the last
  local sequence, restart detection, busy-poll mode. CLI: `ringfire serve <ring> --bind
  <addr>` and `ringfire mirror <source> <ring>`.
- **UDP multicast delivery** (`ReplicaServer::multicast`, `MulticastConfig`, CLI
  `--multicast GROUP:PORT --iface ADDR --mtu N --ttl N`): each `DATA` frame goes out once
  as a datagram to every mirror; the TCP connection carries `NAK` retransmission from the
  source ring and a `MULTICAST` handshake frame. Mirrors hold back datagrams that overtake
  a hole, so rings are still written in order; a 1 ms multicast heartbeat exposes a lost
  last datagram; a per-source session byte drops datagrams from an earlier incarnation.
- Frame linger (`ReplicaServer::linger`, CLI `--linger-us`): fill a frame for a bounded
  time before sending it; adaptive by default (batch only while frames go out back to
  back, up to 50 µs). Without it every record above ~20,000/s costs its own datagram and
  system call, and latency jumps from ~50 µs to over a millisecond on a kernel stack.
- `examples/replication_stress.rs`: open-loop two-host stress with delivery ratio,
  round-trip percentiles and per-mirror NAK/retransmission/gap counters. It found and this
  release fixes: multicast heartbeats announcing a sequence whose datagram had not been
  sent yet (spurious NAKs under load), and `Mirror::step` draining datagrams without bound
  so a caller interleaving its own work never got control back.
- `FLAG_SPARSE` (0x0400): rings whose sequence numbers may have holes. `RingConsumer`
  skips to the next message present instead of waiting on a hole (checked every 64 empty
  polls, only on sparse rings; other rings are unchanged).
- `RingfireError::Unsupported` and `RingfireError::Protocol`.
- `examples/replication_latency.rs`: one-way latency and burst throughput over loopback.

## [0.4.0] - 2026-09-22

Correctness release. The v0.3.0 review found torn reads, hangs, file truncation and
layout bugs across the Rust core, FFI, C header, Python bindings and CLI; each fix below
has a regression test in `tests/regression_tests.rs`.

### Breaking
- **Wire protocol v2 (`RINGFIRE_VERSION = 2`)**: v1 and v2 readers/writers refuse each other.
  - Writers mark a slot `SLOT_WRITING` (`u64::MAX`) before overwriting its payload.
  - `RingHeader` gains `slots_offset` (taken from padding; header is still 128 bytes).
    Rust, FFI, C and Python readers locate slots through it instead of guessing.
- `LayoutSignature` hashes the type name without module paths, so the same struct defined
  in two crates matches. Signatures differ from v0.3.0 (irrelevant across the protocol bump).
- `MpmcQueueConsumer` delivery is documented and implemented as at-most-once: overrun
  items are dropped and counted (`dropped_count()`) instead of hanging the consumer.
- `ReaderRegistry::reader_lag` / `headroom` (and the `RingProducer`/`BlobProducer`
  wrappers) take the last published sequence and count unread messages consistently;
  `BlobProducer::reader_lag` was off by one.
- Attaching a `RingConsumer` to a `LosslessBackpressure` ring whose registry is full now
  fails with `NoAvailableReaderSlots` instead of silently leaving the reader unprotected.
- `ringfire_blackboard_read` (FFI) returns `-2` and `BlackboardConsumer::read` returns
  `RingfireError::WriterStalled` when a slot stays mid-write for over 100 ms.

### Fixed
- **Torn reads** (all readers): a reader one slot short of being lapped accepted payloads
  the writer was overwriting (1875 torn out of 16.9M reads in 3 s on a 7950X). Fixed by
  the v2 slot marker plus acquire/release fences for AArch64.
- **`BlobConsumer` hang**: `recv`/`view` spun forever once the arena wrapped before the
  descriptor ring did. Lapped payloads are now skipped and counted; descriptors are
  bounds-checked before the arena is touched.
- **Live ring truncation**: a second `create` on a live path truncated the file before
  failing the `flock`, so the running producer and readers hit `SIGBUS`. All producers
  (ring, blob, MPMC, blackboard, FFI, Python) now lock first; a stale ring from a dead
  producer is replaced by a fresh inode so orphaned readers stay consistent.
- **`push_batch` ignored `LosslessBackpressure`.**
- **Lossless producer cost**: every `push` scanned the registry and checked each
  reader's liveness via `/proc` (about 830 ns per push+recv). The producer now caches the slowest
  cursor and rescans only when approaching it or every half ring; liveness checks run only
  while blocked. Cost is on par with lossy mode.
- **MPMC**: queue consumers hung on overwritten tickets; concurrent producers one lap apart
  could interleave payloads in a slot. Slots are now taken over by CAS in lap order; a slot
  that stays mid-write for over 10 ms is never taken over (its writer may just be
  descheduled): the waiting producer drops its message instead of tearing the payload.
- **Reader registry**: reclaiming a dead reader could wipe a registration that had just
  replaced it (non-CAS store); `update_cursor` used `Relaxed`, letting the producer
  overwrite a slot on AArch64 before the reader finished copying it.
- **Blackboard**: writer/reader fences for AArch64; readers no longer spin forever on a
  slot left odd by a crashed writer.
- **Attach validation**: truncated or foreign files are rejected (`CorruptLayout`) instead
  of being read out of bounds; the header magic is published last.
- **Python**: `BlobConsumer` read the arena mask from the `reserved` counter (wrong
  payloads for most offsets) and never checked for arena lapping; added
  `try_recv_copy`. Producers take an exclusive `flock`.
- **C header / FFI consumer** ignored the reader registry offset and read registry bytes
  as slots on lossless and blob rings. The C header struct is updated to the v2 layout.
- **CLI**: `dump` printed the header as slots on rings without a registry; the retained
  window was off by one; JSON output did not escape strings; `prune` used non-CAS stores.
- **`AsyncRingConsumer` as `Stream`** spawned a Tokio task per pending poll and busy-looped
  while idle; it now yields a few times and then waits on a timer.
- **`FutexWait` lost wake-ups**: a consumer going to sleep could miss a message published at
  the same instant and sleep for the full timeout (50 ms by default; about 1 in 1000 round
  trips in a ping-pong, averaging 53 µs per round trip). Fixed with an asymmetric barrier:
  producers register with `membarrier(REGISTER_GLOBAL_EXPEDITED)` and a consumer issues
  `membarrier(GLOBAL_EXPEDITED)` before sleeping, so the producer hot path stays barrier-free.
  Futex ping-pong round trip: 2.3 µs (Unix socket: 4.8 µs). Sleeps stay bounded as a fallback
  (`timeout: None` = 10 ms) for producers that cannot register.
- `SPMC` producers wake all sleeping readers (broadcast), not just one.
- `CycleStamp` on AArch64 reads `cntvct_el0` (it was a +1 counter); added
  `CycleStamp::counter_frequency_hz()`.
- `ConsumerStartMode::Sequence(0)` no longer reports a phantom lapped message.

### Added
- `tests/regression_tests.rs` (19 tests), `examples/quickstart.rs` (README snippets, run in CI).
- `benches/ipc_compare.rs`: 64-byte round trip over ringfire (spin and futex), Unix socket,
  pipe and TCP loopback, measured with one harness; results at the top of the README.
- `rust-version = "1.88"` (edition 2024, let-chains).
- GitHub Actions CI: Linux x86-64 and macOS AArch64, clippy with and without `tokio`.

## [0.3.0] - 2026-09-22

### Added
- **Lossless Backpressure Flow Control (`FlowControl::LosslessBackpressure`)**:
  - Optional lossless flow control policy configured via `RingProducerBuilder::flow_control(FlowControl::LosslessBackpressure)`.
  - Producer coordinates with `ReaderRegistry` to guarantee the slowest active reader is never overwritten.
  - Blocking `push()` automatically throttles via spin/yield backoff when headroom is exhausted; non-blocking `try_push()` returns `Err(RingfireError::BackpressureBufferFull)`.
  - Verified safe RAII drop order ensuring shared memory remains mapped while registry slots are deregistered.
- **Channel Multiplexing (`RingMultiplexer` & `AsyncRingMultiplexer`)**:
  - `RingMultiplexer`: Multi-channel ingestion engine supporting fair Round-Robin polling (`try_recv_any`), strict Priority scheduling (`try_recv_priority`), and batch draining (`recv_batch_any`) across multiple distinct ring buffers.
  - `AsyncRingMultiplexer`: Cooperative Tokio async multiplexer (`recv_any().await`) with zero task starvation.
- **Python Variable-Length Payload Support (`BlobConsumer`)**:
  - Implemented `BlobConsumer` in `python/ringfire` returning `(metadata, memoryview)` directly into the mapped `PayloadArena` with zero memory copies.
- **CLI Monitoring & Diagnostics Tool (`ringfire`)**:
  - Zero-dependency diagnostics binary:
    - `stat`: Inspect buffer headers, sequence state, arena allocation, and reader lag (human-readable or `--json`).
    - `top`: Real-time terminal dashboard with instantaneous message rate (`msg/s`), throughput (`MB/s`), and consumer lag.
    - `dump`: Inspect recent slots and hex/ASCII payload snippets.
    - `prune`: Clean up and reclaim abandoned reader slots from terminated processes.

## [0.2.0] - 2026-09-21

### Added
- **Consumer Start Modes (`ConsumerStartMode`)**:
  - `Latest`: Instant jump to the newest published sequence without reading historical backlog.
  - `Head` / `Oldest`: Start reading from the oldest available non-overwritten sequence.
  - `Sequence(u64)`: Position consumer at an exact target sequence number.
  - Dynamic repositioning via `RingConsumer::seek(ConsumerStartMode)`.
- **Lock-Free SHM Offset Checkpointing (`OffsetCheckpoint`)**:
  - Cache-line aligned (64 bytes) persistent offset storage in `/dev/shm`.
  - Sub-10ns atomic commits (`commit_offset(seq)`) with Acquire/Release synchronization.
  - Automatic consumer crash recovery and resumption upon restart.
- **Fluent Consumer Builder (`RingConsumerBuilder`)**:
  - Convenient construction API: `start_from_latest()`, `start_from_head()`, `start_from_sequence(seq)`, `offset_shm(path)`.
- **Python Integration**:
  - Updated `ringfire.py` with `ConsumerStartMode` support and atomic offset checkpointing.

## [0.1.0] - 2026-09-21

### Added
- **Core SPMC Ring Buffer**: Lock-free single-producer multi-consumer ring buffer in `/dev/shm` with 128-byte cache-line aligned `RingHeader`, atomic Acquire/Release synchronization, and zero heap allocations.
- **MPMC Ring Buffer**: Multi-producer support with atomic ticket reservation (`claim_seq`) and competing worker-queue consumers (`MpmcQueueConsumer`).
- **Variable-Length Payload Arena (`PayloadArena`, `BlobProducer`, `BlobConsumer`)**:
  - Out-of-band circular payload arena for large, variable-sized binary messages.
  - Zero heap allocation during push/pull.
- **Reader Registry & Heartbeat (`ReaderRegistry`)**:
  - Liveness monitoring, consumer registration, and dead-reader detection via cycle stamps.
- **ABI & Layout Validation (`LayoutSignature`, `CycleStamp`)**:
  - Compile-time and runtime validation signatures in SHM header preventing ABI drift between producers and consumers.
- **LatestWins Overflow Policy**: Non-blocking writer with zero stalls; lagged readers detect lapping (`RecvStatus::Lapped`), track dropped metrics, and jump cleanly to active surviving stream frames.
- **Tear-Read Memory Safety**: Two-phase seqlock validation ensuring zero torn reads during concurrent writer wraps.
- **Wait Strategies**:
  - `BusySpin`: sub-20ns memory spin loop.
  - `YieldBackoff`: adaptive OS thread yielding.
  - `Futex`: Linux kernel sleep/wake with 0% CPU idle usage and atomic `waiting_consumers` guard (0 syscalls during active streaming).
- **Tokio Async Integration (`AsyncRingConsumer`)**:
  - `recv().await` and `recv_batch(&mut [T]).await`.
  - Adaptive spinning $\to$ cooperative `tokio::task::yield_now().await` $\to$ async sleep, eliminating worker thread starvation.
  - Implementation of `futures_core::Stream`.
- **Shared State Blackboard**:
  - $O(1)$ state snapshot table (`BlackboardProducer`, `BlackboardConsumer`) in `/dev/shm`.
  - Per-slot 64-bit seqlock with 64-byte cache line alignment.
  - Benchmarked at **4.09 ns** read and **1.19 ns** write on AMD Ryzen 9 7950X.
- **Polyglot Bindings**:
  - Pure C11 header [`include/ringfire.h`](include/ringfire.h) with standalone inline functions and C-ABI export.
  - Zero-dependency Python package [`python/ringfire`](python/ringfire) using `mmap` and `ctypes.Structure`.
- **Hardware Benchmarks**: Comprehensive Criterion suite verified on AMD Ryzen 9 7950X (`booster`), achieving 588M msg/s push throughput and 83M msg/s recv throughput.
