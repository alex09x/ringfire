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

### 1. Memory-Mapped File Header (`RingHeader`)
The shared memory region (`/dev/shm/<name>`) begins with a 128-byte cache-line aligned header:

```rust
#[repr(C, align(128))]
pub struct RingHeader {
    pub magic: u64,             // 0x5249_4E47_4649_5245 ("RINGFIRE")
    pub version: u32,           // Protocol version (1)
    pub element_size: u32,      // Fixed payload size in bytes
    pub capacity: u64,          // Number of slots (must be power of 2)
    pub mask: u64,              // capacity - 1
    pub write_seq: AtomicU64,   // Highest published sequence number
    pub flags: u32,             // Control flags (e.g., lossy vs backpressure)
    pub _pad: [u8; 84],         // Padding to prevent false sharing
}
```

### 2. Slot Structure (`Slot<T>`)
Slots are laid out contiguously immediately following the header:

```rust
#[repr(C)]
pub struct Slot<T> {
    pub seq: AtomicU64,         // Monotonically increasing sequence number
    pub data: T,                // Fixed-size payload (T: Copy or [u8; N])
}
```

### 3. Publishing Protocol (Single Producer)
1. Producer determines target slot index: `idx = seq & mask`.
2. Writes data directly into `slot.data` (zero-copy memory copy).
3. Executes atomic store with `Ordering::Release` on `slot.seq`.
4. Updates global `header.write_seq` with `Ordering::Release`.

### 4. Consumption Protocol (Multi Consumer)
1. Consumer maintains local `cursor`.
2. Inspects `slot.seq` with `Ordering::Acquire`.
3. If `slot.seq == cursor`: reads `slot.data` and increments `cursor`.
4. If `slot.seq > cursor`: consumer fell behind (lapped). Jumps cursor to `slot.seq` (latest-wins) and records lap counter.
5. If `slot.seq < cursor`: slot not yet published.

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
