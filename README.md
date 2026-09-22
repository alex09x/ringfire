# ringfire 🔥

[![Crates.io](https://img.shields.io/badge/crates.io-v0.3.0-orange.svg)](https://crates.io/crates/ringfire)
[![Documentation](https://docs.rs/ringfire/badge.svg)](https://docs.rs/ringfire)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-brightgreen.svg)](https://www.rust-lang.org)

**`ringfire`** is an ultra-low-latency, zero-copy, lock-free **Inter-Process Communication (IPC)** ring buffer and shared memory state bus for Linux.

It is engineered for high-frequency trading (HFT) engines, real-time market data ingestion, telemetry buses, and performance-critical distributed pipelines where microsecond socket latencies and kernel overhead are unacceptable.

---

## 💡 The Problem: Why Traditional IPC Fails Under High Load

When communicating between processes on the same host, developers usually default to Unix Domain Sockets (UDS), TCP loopback, pipes, ZeroMQ, or broker-based message queues (NATS, Redis). In high-throughput, low-latency environments, these primitives introduce severe architectural bottlenecks:

| IPC Mechanism | Kernel Overhead | Memory Copies | Typical Latency | Backpressure / Crash Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Unix Domain Sockets (UDS)** | 2 syscalls (`send`/`recv`) + context switch | User $\to$ Kernel $\to$ User (2 copies) | 2,000 – 15,000 ns (2–15 µs) | Socket buffer fills up; blocks producer or drops packets |
| **TCP Loopback (`127.0.0.1`)** | Full TCP/IP stack + packetization | Multiple copies + TCP buffers | 5,000 – 25,000 ns (5–25 µs) | Heavy CPU jitter, flow control stalls |
| **Pipes / FIFOs** | Pipe inode lock + syscalls | Buffer copy through VFS | 1,500 – 8,000 ns (1.5–8 µs) | Blocking write when pipe buffer (64 KB) fills |
| **Message Brokers (Redis / NATS)** | Network stack + daemon context switch | Multi-hop serialization | 50,000 – 500,000 ns (50–500 µs) | High GC/memory pressure, single point of failure |
| **`ringfire` (Shared Memory)** | **0 syscalls on hot path** | **0 copies (in-place memory access)** | **< 30 nanoseconds** | **Writer is unblockable; crash-isolated** |

### The Three Critical Pain Points:
1. **The Syscall & Context Switch Tax**: Every `write()` and `read()` triggers CPU privilege elevation from user-space to kernel-space and back, polluting CPU L1/L2 caches and branch predictors.
2. **Buffer Bloat & Head-of-Line Blocking**: If a consumer process stalls (e.g. garbage collection pause in Python, disk IO hiccup, or debug pause), standard socket buffers fill up immediately, stalling the critical producer or blowing up memory.
3. **Serialization Overhead**: Marshalling data to and from JSON, Protobuf, or even compact binary encoders consumes valuable CPU cycles and allocates memory on the hot path.

---

## 🎯 The Solution & Vision: What `ringfire` Solves

`ringfire` moves the communication fabric directly into physical RAM via POSIX shared memory (`/dev/shm`):

- **Pure Shared Memory (`/dev/shm`)**: Producer and consumers map the exact same physical memory region directly into their virtual address spaces.
- **Atomic Acquire/Release Synchronization**: State is coordinated via 64-bit atomic sequence numbers using CPU-level memory barriers (`core::sync::atomic`), completely bypassing the operating system kernel.
- **Single-Writer Freedom (`LatestWins` Policy)**: The producer always writes to the ring. Slow, paused, or dead consumers can never block, stall, or crash the producer. If a consumer falls behind the ring buffer capacity, it detects that it was lapped and skips cleanly to the live stream.
- **Cache-Line Isolated Layout**: Memory structures are aligned to 128-byte cache lines to eliminate false sharing between producer write heads and consumer read heads.
- **O(1) State Blackboard**: Besides sequential stream events, `ringfire` provides a direct seqlock-synchronized slot table. Consumers can instantly inspect the latest state (e.g., current Best Bid & Offer for 500 coins) in ~5 nanoseconds without replaying historical events.
- **Polyglot First-Class Support**: Because the layout in `/dev/shm` is standard C-ABI memory, consumers can be written in Rust, C, C++, or Python (`mmap` + `ctypes`/`numpy`) with zero bridge penalty.

---

## 🤝 The Rapidfire Family

`ringfire` is designed as the cross-process counterpart to [**`rapidfire`**](https://github.com/alex09x/rapidfire):

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          IN-PROCESS (Single Process)                        │
│                                                                             │
│                                  rapidfire                                  │
│           • Intra-process MPMC / MPSC channels across threads & Tokio       │
│           • Ultra-low latency (< 15 ns), zero heap allocations              │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CROSS-PROCESS (Multi-Process IPC)                   │
│                                                                             │
│                                  ringfire                                   │
│           • Inter-process lock-free ring buffer via /dev/shm                │
│           • O(1) Shared State Blackboard (Seqlock)                          │
│           • Sub-50ns latency, C11 header, Python zero-copy bindings         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Verified Benchmarks (AMD Ryzen 9 7950X on Linux `booster`)

Benchmarked using Criterion directly against POSIX shared memory (`/dev/shm`) on host `booster` (16 Cores / 32 Threads, Linux 6.8):

| Metric | Measured Value | Rate / Target |
| :--- | :--- | :--- |
| **SPMC Single-Message Push** | **1.70 ns** | **588.70 Million msgs / sec** |
| **SPMC Non-Blocking `try_recv`** | **11.94 ns** | **83.74 Million msgs / sec** |
| **SPMC Batch Drain (`recv_batch(32)`)** | **262 ns** (8.1 ns / msg) | **121.9 Million msgs / sec** |
| **Roundtrip Latency (Ping-Pong RTT)** | **245.80 ns** | ~61 ns one-way cross-thread IPC |
| **Blackboard Seqlock Read (O(1))** | **4.09 ns** | Direct sub-5ns snapshot read |
| **Blackboard Seqlock Write (O(1))** | **1.19 ns** | Instant atomic update |
| **Multi-Process Saturation** | **1 writer + 8 readers** | 100% verified zero gaps / zero corruption |

---

## 📦 Quickstart (Rust)

### 1. Producer: Publish Fixed-Size Binary Records

```rust
use ringfire::RingProducer;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MarketTicker {
    asset_id: u32,
    bid_px: u64,
    ask_px: u64,
    timestamp_ns: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creates a 65,536 slot ring buffer in /dev/shm/hft_ticker_stream
    let mut producer = RingProducer::<MarketTicker>::create("/dev/shm/hft_ticker_stream", 65536)?;

    let ticker = MarketTicker {
        asset_id: 42,
        bid_px: 64_250_000_000,
        ask_px: 64_250_500_000,
        timestamp_ns: 1_726_870_000_000_000,
    };

    // Pushes ticker directly to shared memory (1.70 ns)
    producer.push(&ticker);

    Ok(())
}
```

### 2. Tokio Async Consumer: Cooperative Non-Blocking Streaming

In async bots, busy loops starve the Tokio runtime. `AsyncRingConsumer` solves this with adaptive fast-path spinning, cooperative `tokio::task::yield_now()`, and 0% CPU idle sleep:

```rust
use ringfire::AsyncRingConsumer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = AsyncRingConsumer::<MarketTicker>::attach("/dev/shm/hft_ticker_stream")?;

    println!("Attached async consumer. Streaming market data...");

    loop {
        // Yields cooperatively to other Tokio tasks when no traffic is present
        let ticker = consumer.recv().await;
        // Process ticker without stalling the Tokio runtime
    }
}
```

### 3. Synchronous Low-Latency Consumer (Dedicated Cores)

```rust
use ringfire::{RingConsumer, BusySpin};

let mut consumer = RingConsumer::<MarketTicker>::attach("/dev/shm/hft_ticker_stream")?;
let mut wait = BusySpin::new();

loop {
    // Spin polling for sub-20ns reaction time
    let ticker = consumer.recv_blocking(&mut wait);
    // Process ticker
}
```

### 4. Shared State Blackboard (O(1) Snapshot Table)

```rust
use ringfire::{BlackboardProducer, BlackboardConsumer};

// Producer updates snapshot table
let mut bb_prod = BlackboardProducer::<MarketTicker>::create("/dev/shm/hft_state_table", 1024)?;
bb_prod.write(42, &ticker)?; // 1.19 ns write

// Consumer reads instantaneous current state by asset ID
let bb_cons = BlackboardConsumer::<MarketTicker>::attach("/dev/shm/hft_state_table")?;
if let Some(state) = bb_cons.read(42)? { // 4.09 ns O(1) tear-free read
    println!("Current BBO for asset 42: bid={}, ask={}", state.bid_px, state.ask_px);
}
```

### 5. Raw Binary Payloads (`[u8; N]`) & Pointer Casting

You can also operate over raw byte buffers without declaring fixed structs:

```rust
use ringfire::{RingProducer, AsyncRingConsumer};

// Producer sends a raw 64-byte binary packet
let mut producer = RingProducer::<[u8; 64]>::create("/dev/shm/raw_stream", 65536)?;
let raw_bytes = [0xAAu8; 64];
producer.push(&raw_bytes);

// Consumer reads the 64 bytes and casts to struct in-place
let mut consumer = AsyncRingConsumer::<[u8; 64]>::attach("/dev/shm/raw_stream")?;
let bytes: [u8; 64] = consumer.recv().await;
let ticker: &MarketTicker = unsafe { &*(bytes.as_ptr() as *const MarketTicker) };
```

### 6. Variable-Length Payloads (`BlobProducer` & `BlobConsumer`)

For variable-sized messages (e.g. L2/L3 order book snapshots, compressed frames, or variable trade batches), `ringfire` pairs fixed ring descriptors with a contiguous shared-memory `PayloadArena`:

```rust
use ringfire::{BlobProducerBuilder, BlobConsumer};

// Create a ring with 65,536 descriptor slots and a 16 MB byte arena
let mut producer = BlobProducerBuilder::new(65536, 16 * 1024 * 1024)
    .build::<u32>("/dev/shm/orderbook_stream")?;

// Push variable-length JSON, protobuf, or raw bytes directly into the arena
let raw_json = br#"{"event":"snapshot","bids":[[82000.5,1.2]],"asks":[[82001.0,0.8]]}"#;
producer.push_blob(&42, raw_json)?;

// Consumer receives the metadata and views the arena payload zero-copy in-place
let mut consumer = BlobConsumer::<u32>::attach("/dev/shm/orderbook_stream")?;
let mut scratch_buffer = vec![0u8; 4096];
if let Ok(meta) = consumer.recv_copy(&mut scratch_buffer) {
    println!("Received snapshot for symbol {}", meta);
}
```

### 7. Consumer Start Modes & Lock-Free SHM Checkpointing

Consumers can configure where in the stream to begin reading, and persist their read cursors lock-free into `/dev/shm` (<10ns seqlock commit):

```rust
use ringfire::{RingConsumerBuilder, ConsumerStartMode, OffsetCheckpoint};

let mut consumer = RingConsumerBuilder::new()
    // Modes: Latest (instant jump), Head (wait for future), Oldest (replay backlog), Sequence(N)
    .start_mode(ConsumerStartMode::Latest)
    .attach::<MarketTicker>("/dev/shm/hft_ticker_stream")?;

// Or attach with persistent offset checkpoint in /dev/shm:
let mut persistent_consumer = RingConsumerBuilder::new()
    .offset_path("/dev/shm/hft_ticker_stream_worker1.offset")
    .attach::<MarketTicker>("/dev/shm/hft_ticker_stream")?;

// In consumer loop, periodically or per-batch commit offset:
persistent_consumer.commit_offset()?;
```

### 8. Lossless Backpressure Flow Control

While default `ringfire` channels operate in `LatestWins` lossy mode (writer never blocks), streaming pipelines requiring zero message drops can enable `LosslessBackpressure`:

```rust
use ringfire::{RingProducerBuilder, FlowControl, RingConsumer};

let mut producer = RingProducerBuilder::new()
    .flow_control(FlowControl::LosslessBackpressure)
    .create::<MarketTicker>("/dev/shm/reliable_stream", 4096)?;

// Writer throttles (via spin/yield backoff) if slowest registered reader is about to be lapped:
producer.push(&ticker);

// Or use non-blocking try_push:
match producer.try_push(&ticker) {
    Ok(()) => println!("Pushed successfully"),
    Err(RingfireError::BackpressureBufferFull) => println!("Slow reader lag detected, backpressure applied"),
    Err(e) => return Err(e.into()),
}
```

### 9. Multi-Channel Multiplexing (`RingMultiplexer` & `AsyncRingMultiplexer`)

Multiplex across multiple distinct ring buffer streams with fair Round-Robin or strict Priority scheduling:

```rust
use ringfire::{RingConsumer, RingMultiplexer};

let c1 = RingConsumer::<MarketTicker>::attach("/dev/shm/stream_btc")?;
let c2 = RingConsumer::<MarketTicker>::attach("/dev/shm/stream_eth")?;

let mut mux = RingMultiplexer::new();
mux.add_channel(c1);
mux.add_channel(c2);

// Fair Round-Robin across all channels
if let Some((channel_idx, ticker)) = mux.try_recv_any() {
    println!("Channel {} received ticker {}", channel_idx, ticker.asset_id);
}

// Or Tokio async multiplexing:
// let (idx, ticker) = async_mux.recv_any().await;
```

### 10. CLI Diagnostic & Monitoring Tool (`ringfire`)

The bundled `ringfire` binary provides real-time terminal monitoring and inspection:

```bash
# View buffer configuration, sequence counters, and registered consumer lag
cargo run --bin ringfire -- stat /dev/shm/hft_ticker_stream

# Machine-readable JSON output for automated telemetry:
cargo run --bin ringfire -- stat /dev/shm/hft_ticker_stream --json

# Real-time interactive terminal dashboard with msg/s and MB/s throughput:
cargo run --bin ringfire -- top /dev/shm/hft_ticker_stream --interval-ms 500

# Dump recent slots and payloads in hex or ASCII:
cargo run --bin ringfire -- dump /dev/shm/hft_ticker_stream --tail 10 --hex

# Clean up dead reader slots from crashed processes:
cargo run --bin ringfire -- prune /dev/shm/hft_ticker_stream
```

---

## 🛡️ Memory Safety: Why Raw Pointers into Shared Memory Are Dangerous

In cross-process shared memory with a non-blocking writer (`LatestWins`), returning a raw pointer (`*const T`) directly into the mapped `/dev/shm` buffer is **fundamentally unsafe**:
- If a consumer holds a raw pointer to slot $K$, and the writer laps the buffer and begins overwriting slot $K$ on another CPU core, the consumer will observe a **torn read** (half old data, half new data).
- In high-frequency trading and order book streaming, a torn read corrupts prices and sizes, leading to disastrous trading errors.

### The `ringfire` Solution: Two-Phase Seqlock Validation
`ringfire` enforces tear-free memory safety without locks:
1. Consumer loads slot sequence $s_1$ with `Ordering::Acquire`.
2. Copies the payload into the consumer's local registers/stack (takes **1 CPU clock cycle** for 32–64B via `vmovups`).
3. Loads slot sequence $s_2$ with `Ordering::Acquire`.
4. If $s_1 == s_2$, the copy is mathematically guaranteed to be **consistent, uncorrupted, and tear-free**.
5. If $s_1 \neq s_2$ (writer updated the slot mid-read), the consumer immediately discards the partial copy and re-evaluates the latest sequence.

> [!WARNING]
> **Anti-Pattern: Returning Raw Pointers into Shared Memory (`*const T`)**
> Some naive IPC designs attempt to return a direct pointer or slice `&[u8]` into `/dev/shm` to claim "zero-memcpy". In a multi-process architecture with a non-blocking writer (`LatestWins`), this is a dangerous anti-pattern: the writer can overwrite that memory slot at any microsecond while the reader is parsing it, causing undefined behavior, silent data races, and torn reads.
> 
> `ringfire` deliberately copies the slot payload into the reader's stack/register space inside a seqlock validation boundary (`s1 == s2`). For modern x86_64/ARM64 architectures, copying 32–64 bytes takes **~1 CPU clock cycle** (via `vmovups`) and is orders of magnitude faster than recovering from corrupted state or dealing with UB.


---

## ⚡ Wait Strategies

`ringfire` supports selectable wait strategies depending on CPU budget:

- **`WaitStrategy::BusySpin`**: Sub-30ns reaction time. Spins tightly on CPU (`core::hint::spin_loop()`). Recommended for dedicated HFT cores.
- **`WaitStrategy::YieldBackoff`**: Spins for $K$ iterations then calls `std::thread::yield_now()`. Balanced CPU usage with ~150ns reaction time.
- **`WaitStrategy::Futex`**: Sleeps on Linux `futex` when the queue is idle. Consumes 0% CPU when waiting, wakes up via atomic event signaling.

---

## 🐍 Polyglot Access (C / C++ / Python)

- **C11 Header**: Include `include/ringfire.h` in any C/C++ project without linking overhead.
- **Python (`ringfire-py`)**: Read shared memory slots directly via Python `mmap` and `ctypes.Structure` with no serialization penalty.

---

## 👥 Author

**Alexander Panasenko**
- Email: [alex@prod.codes](mailto:alex@prod.codes)
- GitHub: [@alex09x](https://github.com/alex09x)

---

## 📜 License

Licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
