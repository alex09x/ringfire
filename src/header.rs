use std::sync::atomic::{AtomicU32, AtomicU64};

pub const RINGFIRE_MAGIC: u64 = 0x5249_4E47_4649_5245; // "RINGFIRE" in ASCII
pub const RINGFIRE_VERSION: u32 = 1;

pub const BLACKBOARD_MAGIC: u64 = 0x5249_4E47_4242_4F52; // "RINGBBOR" in ASCII
pub const BLACKBOARD_VERSION: u32 = 1;

pub const FLAG_POLICY_LATEST_WINS: u32 = 0x0001;
pub const FLAG_MODE_SPMC: u32 = 0x0010;
pub const FLAG_MODE_MPMC: u32 = 0x0020;
pub const FLAG_WITH_ARENA: u32 = 0x0100;
pub const FLAG_WITH_REGISTRY: u32 = 0x0200;

/// Header stored at the beginning of the ring buffer shared memory region.
/// 128-byte cache-line aligned to prevent false sharing.
#[repr(C, align(128))]
pub struct RingHeader {
    /// Magic signature bytes ("RINGFIRE")
    pub magic: u64,
    /// Protocol version
    pub version: u32,
    /// Fixed element (Slot<T>) size in bytes
    pub element_size: u32,
    /// Buffer capacity (number of slots, power of 2)
    pub capacity: u64,
    /// Bitmask for modulo indexing (capacity - 1)
    pub mask: u64,
    /// Highest published sequence number
    pub write_seq: AtomicU64,
    /// Highest claimed sequence number (for MPMC producers)
    pub claim_seq: AtomicU64,
    /// Operational flags
    pub flags: u32,
    /// Futex notification word for 0% CPU idle consumers
    pub futex_word: AtomicU32,
    /// Count of consumers currently sleeping on the futex
    pub waiting_consumers: AtomicU32,
    /// Internal padding for 8-byte alignment
    pub _align_pad: u32,
    /// Shared atomic read sequence for MPMC work-queue mode
    pub read_seq: AtomicU64,
    /// Schema signature / type fingerprint (0 = unverified)
    pub schema_sig: u64,
    /// Byte offset to PayloadArena from the start of the mapping (0 = no arena)
    pub arena_offset: u64,
    /// Total capacity of PayloadArena in bytes (0 = no arena)
    pub arena_size: u64,
    /// Byte offset to ReaderRegistry from the start of the mapping (0 = disabled)
    pub reader_registry_offset: u32,
    /// Maximum number of readers in registry
    pub reader_registry_count: u32,
    /// Cache line padding to exactly 128 bytes
    pub _pad: [u8; 24],
}

// Compile-time checks for RingHeader size and alignment
const _: () = {
    assert!(std::mem::size_of::<RingHeader>() == 128);
    assert!(std::mem::align_of::<RingHeader>() == 128);
};

/// An entry in the shared-memory ReaderRegistry.
/// 64-byte cache-line aligned to prevent false sharing between concurrent readers.
#[repr(C, align(64))]
pub struct ReaderSlot {
    /// PID of the registered reader process (0 = free slot)
    pub pid: AtomicU32,
    /// Active state flag (1 = active, 0 = inactive/departed)
    pub active: AtomicU32,
    /// Current read sequence cursor of this reader
    pub cursor_seq: AtomicU64,
    /// Last heartbeat timestamp (monotonic / TSC cycles)
    pub heartbeat_tsc: AtomicU64,
    /// Human-readable reader process/thread name
    pub name: [u8; 32],
    /// Alignment padding to exactly 64 bytes
    pub _pad: [u8; 8],
}

const _: () = {
    assert!(std::mem::size_of::<ReaderSlot>() == 64);
    assert!(std::mem::align_of::<ReaderSlot>() == 64);
};

/// An individual slot in the ring buffer.
#[repr(C)]
pub struct Slot<T> {
    /// Sequence number of this slot (0 = unwritten, N = published)
    pub seq: AtomicU64,
    /// Generic fixed-size payload
    pub data: T,
}

/// Header stored at the beginning of a Blackboard shared memory region.
#[repr(C, align(128))]
pub struct BlackboardHeader {
    pub magic: u64,
    pub version: u32,
    pub value_size: u32,
    pub slot_size: u32,
    pub slot_count: u32,
    pub _reserved: [u64; 2],
    pub _pad: [u8; 88],
}

const _: () = {
    assert!(std::mem::size_of::<BlackboardHeader>() == 128);
    assert!(std::mem::align_of::<BlackboardHeader>() == 128);
};

/// An individual slot in the Blackboard state table.
/// 64-byte cache-line aligned to eliminate false sharing between keys.
#[repr(C, align(64))]
pub struct BlackboardSlot<V> {
    /// Seqlock sequence: even = idle/consistent, odd = write in progress
    pub seqlock: AtomicU64,
    /// Fixed-size value
    pub value: V,
}
