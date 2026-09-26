# ringfire: Engineering Roadmap & Architecture Specification

## Overview

**`ringfire`** is a high-performance, general-purpose, zero-copy lock-free **Inter-Process Communication (IPC)** ring buffer and shared memory bus for Linux.

It is designed as the cross-process counterpart to [**`rapidfire`**](https://github.com/alex09x/rapidfire):
- **`rapidfire`**: In-process MPMC & MPSC channels across threads and async tasks.
- **`ringfire`**: Cross-process zero-copy shared memory queues and state tables across distinct processes.

The library is **strictly general-purpose**: it does not embed application-specific or exchange-specific schemas. Instead, it operates over fixed-size binary records (`T: Copy` / `[u8; N]`), making it equally suitable for market data streaming, order routing, execution reports, telemetry, and distributed robot control.

---

## Target Platform & Performance Goals

- **Target Verification Environment**: AMD Ryzen (Ryzen 9 7950X / 9950X, multi-core host `booster`).
- **Steady-State IPC Latency**: **< 50 nanoseconds** per transfer (atomic store-release / load-acquire).
- **Throughput**: **> 30,000,000 messages / second** on modern x86-64 Zen4/Zen5 architectures.
- **Zero Allocations & Zero Syscalls**: In the hot streaming path, operations do not invoke the kernel.
- **Fault Isolation**: Slow, paused, or crashed reader processes must never block or corrupt the writer.

---

## Architecture & Data Layout

### 1. Memory-Mapped File Header (`RingHeader`, protocol v2)
The shared memory region (`/dev/shm/<name>`) begins with a 128-byte cache-line aligned header.
Producers write `magic` last, so a reader never attaches to a half-initialized ring.

```rust
#[repr(C, align(128))]
pub struct RingHeader {
    pub magic: u64,                  // 0x5249_4E47_4649_5245 ("RINGFIRE"), written last
    pub version: u32,                // Protocol version (2)
    pub element_size: u32,           // Slot<T> stride in bytes
    pub capacity: u64,               // Number of slots (power of 2)
    pub mask: u64,                   // capacity - 1
    pub write_seq: AtomicU64,        // Highest published sequence number
    pub claim_seq: AtomicU64,        // MPMC: next ticket to claim
    pub flags: u32,                  // Policy / mode / arena / registry flags
    pub futex_word: AtomicU32,       // Futex notification word
    pub waiting_consumers: AtomicU32,// Sleeping consumers
    pub _align_pad: u32,
    pub read_seq: AtomicU64,         // MPMC work queue: next ticket to consume
    pub schema_sig: u64,             // Layout fingerprint of T (0 = unverified)
    pub arena_offset: u64,           // PayloadArena offset (0 = none)
    pub arena_size: u64,             // PayloadArena capacity in bytes
    pub reader_registry_offset: u32, // ReaderRegistry offset (0 = none)
    pub reader_registry_count: u32,  // ReaderRegistry slots
    pub slots_offset: u64,           // Offset of slot 0: readers never guess the layout
    pub _pad: [u8; 16],
}
```

Every reader (Rust, FFI, C header, Python, CLI) validates magic, version, slot stride,
capacity/mask, and that the slots, registry and arena all fit inside the mapping.

### 2. Slot Structure (`Slot<T>`)
Slots start at `slots_offset`:

```rust
#[repr(C)]
pub struct Slot<T> {
    pub seq: AtomicU64,  // 0 = never written, u64::MAX = being overwritten, N = holds message N
    pub data: T,         // Fixed-size payload (T: Copy or [u8; N])
}
```

### 3. Publishing Protocol (Single Producer)
1. `idx = seq & mask`.
2. `slot.seq.store(SLOT_WRITING, Relaxed)`; `fence(Release)`.
3. Copy the payload into `slot.data`.
4. `slot.seq.store(seq, Release)`, then `header.write_seq.store(seq, Release)`.

Multiple producers (MPMC) claim tickets from `claim_seq` and take the slot over with a CAS
from the previous lap's sequence to `SLOT_WRITING`, so writers one lap apart never interleave.

### 4. Consumption Protocol (Multi Consumer)
1. `s1 = slot.seq.load(Acquire)`.
2. `s1 == cursor`: copy the payload, `fence(Acquire)`, `s2 = slot.seq.load(Relaxed)`;
   accept only if `s2 == cursor`, then advance.
3. `s1 < cursor` or `s1 == SLOT_WRITING`: not published yet (caught up).
4. `s1 > cursor`, or the copy was invalidated: lapped. Jump to the oldest retained
   message (`max(write_seq, s1) - capacity + 1`) and add the gap to the lapped counter.

Readers registered in the `ReaderRegistry` publish their cursor with `Release` after
copying, which is what lets a `LosslessBackpressure` producer reuse slots safely.

---

## Implementation Roadmap

### Phase 1: Core SPMC & MPMC Ring Buffer & Memory Safety
- [x] Initial `RingHeader`, `Slot<T>`, `RingProducer`, and `RingConsumer` implementation.
- [x] Implement robust POSIX permissions (0o660), cleanup flags (`unlink` on drop / persistence modes), and `libc::flock` file locking for exclusive producer ownership.
- [x] Add explicit overflow & lapping policies:
  - `LatestWins` (lossy, non-blocking: reader skips missed slots, writer never blocks).
  - `BatchRead`: zero-copy batch drain (`recv_batch(&mut [T]) -> usize`) to amortize atomic synchronization.
- [x] Add MPMC support (`MpmcProducer<T>` and `MpmcQueueConsumer<T>`).

### Phase 2: Flexible Wait Strategies & Tokio Async Integration
- [x] **`BusySpin`**: Pure memory polling with `core::hint::spin_loop()` (< 20 ns latency).
- [x] **`YieldBackoff`**: Adaptive pause / `std::thread::yield_now()` for background tasks.
- [x] **`Futex` / `Eventfd`**: Linux kernel-assisted sleep/wake with 0% CPU idle usage and zero-syscall fast path (`waiting_consumers` atomic guard).
- [x] **Tokio Async Integration (`AsyncRingConsumer`)**:
  - `recv().await` and `recv_batch(&mut [T]).await`
  - Adaptive spinning $\to$ cooperative `tokio::task::yield_now().await` $\to$ async `tokio::time::sleep` (zero task starvation).
  - `futures_core::Stream` implementation for streaming consumers.

### Phase 3: Shared State Blackboard (O(1) Snapshot Table)
- [x] Implement `BlackboardProducer<V>` and `BlackboardConsumer<V>` in `/dev/shm`:
  - Contiguous table of fixed-size slots indexed by integer key (e.g., symbol ID).
  - Per-slot 64-bit seqlock (even = valid, odd = write in progress) with 64-byte cache line alignment.
  - Sub-5ns O(1) state reads verified on hardware (4.09 ns on AMD Ryzen 9 7950X).

### Phase 4: Language Bindings & Multi-Language Access
- [x] **C-ABI Header (`include/ringfire.h`)**: Pure C11 header for direct inclusion in C/C++ execution engines with standalone inline functions and exported Rust C-ABI symbols.
- [x] **Python Module (`python/ringfire`)**: Zero-dependency Python wrapper using `mmap` and `ctypes.Structure` with full interop tests.

### Phase 5: Verification & Benchmarking on AMD Ryzen (host `booster`)
- [x] **Stress Testing**: High-concurrency multi-process tests with 1 writer + 8 readers under heavy saturation (`tests/multiprocess_stress.rs`).
- [x] **Tokio Concurrency Stress Test**: Background tasks verify zero starvation during heavy stream ingestion (`tests/tokio_tests.rs`).
- [x] **Criterion Benchmark Suite on AMD Ryzen 9 7950X (`booster`)**:
  - **SPMC Push Throughput**: **588.70 Million messages / sec** (1.70 ns per push)
  - **SPMC Recv Throughput**: **83.74 Million messages / sec** (11.94 ns per try_recv)
  - **Ping-Pong RTT Latency**: **245.80 ns** round-trip across threads via shared memory (~61 ns one-way)
  - **Blackboard Read**: **4.09 ns**
  - **Blackboard Write**: **1.19 ns**

### Phase 6: Variable-Length Payload Arena, Reader Registry & Checkpoints (v0.2.0)
- [x] **Variable-Length Payload Arena (`PayloadArena`, `BlobProducer`, `BlobConsumer`)**:
  - Contiguous cyclic shared-memory byte arena inspired by Firedancer `dcache`.
  - Zero memory fragmentation with ring-boundary wrapping to index 0.
  - In-place reading directly from shared memory without intermediate copies.
- [x] **Lock-Free Reader Registry (`ReaderRegistry`, `ReaderRegistration`)**:
  - Shared memory registration table tracking active readers, cursors, and lag.
  - Automatic reclamation of departed or crashed consumer processes.
- [x] **CycleStamp High-Resolution Timestamps**:
  - Zero-syscall cycle counters using x86-64 `rdtsc` and AArch64 `cntvct_el0`.
- [x] **Consumer Start Modes (`ConsumerStartMode`)**:
  - `Latest`: Jump immediately to latest message for HFT and live trading bots.
  - `Head`: Wait only for future messages.
  - `Oldest`: Replay from oldest retained message.
  - `Sequence(u64)`: Replay from exact sequence with automatic lapping detection.
- [x] **Lock-Free SHM Offset Checkpoint (`OffsetCheckpoint`)**:
  - Sub-10ns offset commits in `/dev/shm` using 64-bit seqlock.
  - Tear-free cross-process crash recovery.

### Phase 7: Advanced Flow Control, Channel Multiplexing & Observability (v0.3.0)
- [x] **Lossless Backpressure Flow Control (`FlowControl::LosslessBackpressure`)**:
  - Configurable flow control policy via `RingProducerBuilder::flow_control`.
  - Writers throttle via spin/yield backoff when slowest reader is within capacity window.
  - Non-blocking `try_push()` returns `Err(RingfireError::BackpressureBufferFull)`.
- [x] **Channel Multiplexing (`RingMultiplexer` & `AsyncRingMultiplexer`)**:
  - Fair Round-Robin polling across multiple ring buffer channels (`try_recv_any`).
  - Strict Priority scheduling across critical channels (`try_recv_priority`).
  - Batch draining (`recv_batch_any`) and cooperative Tokio async integration (`recv_any().await`).
- [x] **Python Variable-Length Blob Support (`BlobConsumer`)**:
  - Zero-copy Python reading from `PayloadArena` via `memoryview`.
  - Full interop test suite verifying Rust `BlobProducer` to Python `BlobConsumer`.
- [x] **CLI Monitoring & Diagnostics Tool (`ringfire`)**:
  - `stat`: Inspect buffer headers, sequence state, arena, and reader lag (table or `--json`).
  - `top`: Interactive real-time terminal dashboard with msg/s throughput and reader lag.
  - `dump`: Inspect recent slots and hex/ASCII payload snippets.
  - `prune`: Clean up dead reader slots whose processes have terminated.


### Phase 8: Correctness Hardening & Protocol v2 (v0.4.0)
- [x] **Protocol v2**: `SLOT_WRITING` marker makes every reader tear-free; explicit `slots_offset`.
- [x] **Acquire/Release fences for AArch64** in slot, blackboard and registry protocols.
- [x] **Safe producer creation**: `flock` before any modification; stale rings replaced by a new inode.
- [x] **Layout validation** on every attach path (Rust, FFI, C, Python, CLI).
- [x] **Blob arena lapping**: skip and count, never hang; bounds-checked descriptors.
- [x] **Lossless backpressure without syscalls**: cached gating sequence, liveness checks only while blocked.
- [x] **MPMC**: CAS slot takeover in lap order; queue consumers drop overrun tickets instead of hanging.
- [x] **Registry**: CAS-only ownership changes; refusing unprotected readers on lossless rings.
- [x] **Regression suite** (`tests/regression_tests.rs`) and CI on Linux x86-64 + macOS AArch64.

### Phase 9: Network Mirrors (unreleased)
- [x] **Ring replication over TCP** (`ringfire::replication`): `ReplicaServer` on the source host, `Mirror` on each other host, byte-identical rings under the source's sequence numbers, own binary protocol, resume and restart detection, `ringfire serve` / `ringfire mirror` CLI.
- [x] **`FLAG_SPARSE`** and hole skipping in `RingConsumer` for rings that join a stream mid-way.
- [ ] UDP multicast transport with NAK-based retransmission from the source ring (one packet for every mirror).
- [ ] Mirror rings with a payload arena (`BlobProducer` streams).
- [ ] `FLAG_SPARSE` awareness in the C header and Python readers.

### Phase 10: Next
- [ ] **Container-safe liveness**: heartbeat-based reader liveness (PID checks fail across PID namespaces).
- [ ] **Lossless MPMC work queue**: producers gate on `read_seq` for exactly-once delivery.
- [ ] **Python / C registry participation** so non-Rust readers are protected by lossless flow control.
- [ ] **Lossless blob channel**: gate both descriptor ring and arena on the slowest reader.
- [ ] **Model checking**: `loom` models of the slot, registry and MPMC protocols; fuzzing of attach/validation.
- [ ] **Benchmarks for lossless mode and cross-core latency with pinned threads.**
