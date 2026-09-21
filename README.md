# ringfire

Ultra-low-latency, zero-copy lock-free **Inter-Process Communication (IPC)** ring buffer and shared memory bus for Rust.

Designed for high-frequency trading (HFT) engines, market data distribution, and real-time cross-process pipelines where microsecond socket latencies are unacceptable.

Pairs naturally with [**`rapidfire`**](https://github.com/alex09x/rapidfire):
- **`rapidfire`**: In-process MPMC & MPSC channels across threads and async tasks.
- **`ringfire`**: Cross-process zero-copy shared memory queues and state tables across distinct processes.

## Key Features

- **Zero-Copy Transfers**: Consumers read directly from `/dev/shm` without memory allocations or syscalls during streaming.
- **Sub-Microsecond Latency**: End-to-end IPC transmission in **< 50 nanoseconds**.
- **SPMC & MPMC Ring Buffer**: Single/Multi-producer, multi-consumer lock-free ring with sequence numbers.
- **Blackboard State Table**: O(1) direct memory lookup table for latest state (e.g. instant BBO prices in ~5 ns).
- **Crash Isolation**: If a consumer process hangs, pauses, or terminates, the producer is completely unblocked.
- **Multi-Language Access**: Standard C-ABI layout enables direct consumption from Python (`mmap`), C/C++, and Go.

## Architecture

```text
                  [ Producer (e.g. Market Data Ingest) ]
                                    │
                                    ▼ (atomic release store)
                     ┌──────────────────────────────┐
                     │   Shared Memory (/dev/shm)   │
                     │  1. Lock-free Ring Buffer    │
                     │  2. O(1) State Blackboard    │
                     └──────────────┬───────────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         │ (atomic acquire read)    │                          │
         ▼                          ▼                          ▼
[ Strategy Bot A ]         [ Execution Bot B ]        [ Remote Gateway ]
   (< 50 ns, Rust)            (< 50 ns, C++)          (Streams to WAN)
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
