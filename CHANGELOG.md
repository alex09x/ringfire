# Changelog

All notable changes to `ringfire` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
