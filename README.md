# ringfire 🔥

[![Crates.io](https://img.shields.io/badge/crates.io-v0.1.0-orange.svg)](https://crates.io/crates/ringfire)
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

## 🚀 Performance Goals

Engineered and benchmarked on modern hardware (AMD Ryzen 9 7950X / 9950X Linux fleet):

- **End-to-End Latency**: `< 30 nanoseconds` (p50), `< 80 nanoseconds` (p99.9).
- **Throughput**: `> 40,000,000 messages / second` on a single producer core.
- **Heap Allocations**: Exactly **0 bytes** allocated after initialization.
- **State Read Latency**: `< 8 nanoseconds` for $O(1)$ Blackboard seqlock reads.

---

## 🏗️ Architecture

```text
                            [ Producer Process ]
                       (e.g., Node Market Data Ingest)
                                     │
                 Atomic Seq Release  │  Write Slot In-Place
                                     ▼
      ┌─────────────────────────────────────────────────────────────┐
      │                  Shared Memory (/dev/shm)                   │
      │                                                             │
      │   ┌─────────────────────────────────────────────────────┐   │
      │   │ Header: Producer Seq, Capacity, Slot Size, Magic    │   │ (128-byte aligned)
      │   └─────────────────────────────────────────────────────┘   │
      │   ┌─────────────────────────────────────────────────────┐   │
      │   │ Circular Ring Slots: [ Slot 0 ] [ Slot 1 ] [ ... ]  │   │ (T: Copy or [u8; N])
      │   └─────────────────────────────────────────────────────┘   │
      │   ┌─────────────────────────────────────────────────────┐   │
      │   │ State Blackboard: O(1) Key-Value Table (Seqlock)    │   │ (Instant BBO / Status)
      │   └─────────────────────────────────────────────────────┘   │
      └───────┬──────────────────────┬──────────────────────┬───────┘
              │                      │                      │
    Zero-Copy │ Acquire    Zero-Copy │ Acquire    Zero-Copy │ Acquire
              ▼                      ▼                      ▼
      [ Rust Strategy ]        [ C++ Execution ]      [ Python ML Model ]
      (BusySpin, <30ns)       (YieldBackoff, <60ns)   (mmap zero-copy, <1µs)
```

---

## 📦 Quickstart (Rust)

### 1. Producer: Publish Fixed-Size Binary Records

```rust
use ringfire::{RingProducer, ProducerConfig};

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MarketTicker {
    asset_id: u32,
    bid_px: u64,
    ask_px: u64,
    timestamp_ns: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creates a 65,536 slot ring buffer in /dev/shm/hft_ticker_stream
    let config = ProducerConfig::new("hft_ticker_stream", 65536);
    let mut producer = RingProducer::<MarketTicker>::create(config)?;

    let ticker = MarketTicker {
        asset_id: 42,
        bid_px: 64_250_000_000,
        ask_px: 64_250_500_000,
        timestamp_ns: 1_726_870_000_000_000,
    };

    // Pushes ticker directly to shared memory (< 30 ns)
    producer.push(&ticker);

    Ok(())
}
```

### 2. Consumer: Stream Records Without Allocations

```rust
use ringfire::{RingConsumer, WaitStrategy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Opens the existing shared memory ring buffer
    let mut consumer = RingConsumer::<MarketTicker>::open("hft_ticker_stream")?;

    println!("Consumer attached. Reading stream...");

    loop {
        // Zero-copy read using BusySpin wait strategy (< 30 ns reaction time)
        match consumer.recv(WaitStrategy::BusySpin) {
            Ok(ticker) => {
                // Process ticker without any memory allocations
            }
            Err(ringfire::Error::Lapped { skipped }) => {
                eprintln!("Consumer fell behind! Skipped {} old messages.", skipped);
            }
            Err(e) => eprintln!("Error: {:?}", e),
        }
    }
}
```

### 3. Batch Consumption for High-Throughput Pipelines

```rust
let mut batch = [MarketTicker::default(); 128];
let read_count = consumer.recv_batch(&mut batch, WaitStrategy::YieldBackoff)?;
for ticker in &batch[..read_count] {
    // Process multiple messages in a tight cache-friendly loop
}
```

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
