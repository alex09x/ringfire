//! # ringfire
//!
//! Ultra-low-latency, zero-copy lock-free Inter-Process Communication (IPC)
//! ring buffer and shared memory bus for Linux.
//!
//! Designed as the high-performance cross-process counterpart to `rapidfire`,
//! operating over memory-mapped `/dev/shm` files with 128-byte cache-line aligned headers,
//! atomic release/acquire synchronization, `LatestWins` lossy overflow handling,
//! configurable wait strategies (BusySpin, YieldBackoff, Futex sleep while idle),
//! and an O(1) seqlock-backed Blackboard state table.

pub mod arena;
pub mod blackboard;
pub mod blob;
pub mod checkpoint;
pub mod error;
pub mod ffi;
pub mod header;
pub mod mpmc;
pub mod multiplexer;
pub mod registry;
pub mod replication;
pub mod signature;
mod shm;
pub mod spmc;
pub mod tsc;
pub mod wait;

#[cfg(feature = "tokio")]
pub mod async_ring;

// Re-export primary types
pub use arena::{ArenaHeader, BlobRef, PayloadArena};
pub use blackboard::{BlackboardConsumer, BlackboardProducer};
pub use blob::{BlobConsumer, BlobPacket, BlobProducer, BlobProducerBuilder, BlobRecvStatus};
pub use checkpoint::OffsetCheckpoint;
pub use error::{Result, RingfireError};
pub use multiplexer::RingMultiplexer;
#[cfg(feature = "tokio")]
pub use multiplexer::AsyncRingMultiplexer;
pub use header::{
    BlackboardHeader, BlackboardSlot, ReaderSlot, RingHeader, Slot, BLACKBOARD_MAGIC,
    BLACKBOARD_VERSION, FLAG_MODE_MPMC, FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS,
    FLAG_POLICY_LOSSLESS_BACKPRESSURE, FLAG_SPARSE, FLAG_WITH_ARENA, FLAG_WITH_REGISTRY, RINGFIRE_MAGIC,
    RINGFIRE_VERSION, SLOT_WRITING,
};
pub use mpmc::{MpmcProducer, MpmcQueueConsumer};
pub use registry::{ReaderInfo, ReaderRegistration, ReaderRegistry, DEFAULT_MAX_READERS};
pub use replication::{
    Geometry, Mirror, MirrorBuilder, MirrorHandle, MirrorStart, MulticastConfig, ReplicaServer,
};
pub use signature::{compute_layout_signature, fnv1a64, LayoutSignature};
pub use spmc::{
    CleanupMode, ConsumerStartMode, FlowControl, RecvStatus, RingConsumer, RingConsumerBuilder,
    RingProducer, RingProducerBuilder,
};
pub use tsc::CycleStamp;
pub use wait::{BusySpin, FutexWait, WaitStrategy, YieldBackoff};

#[cfg(feature = "tokio")]
pub use async_ring::AsyncRingConsumer;

#[cfg(test)]
#[path = "../tests/unit/lib.rs"]
mod tests;
