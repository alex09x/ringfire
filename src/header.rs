use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::error::{Result, RingfireError};

pub const RINGFIRE_MAGIC: u64 = 0x5249_4E47_4649_5245; // "RINGFIRE" in ASCII
/// Wire protocol version.
///
/// v2: slots are published with a write-in-progress marker ([`SLOT_WRITING`]) and the
/// header carries an explicit `slots_offset`. v1 readers must not attach to v2 rings.
pub const RINGFIRE_VERSION: u32 = 2;

pub const BLACKBOARD_MAGIC: u64 = 0x5249_4E47_4242_4F52; // "RINGBBOR" in ASCII
pub const BLACKBOARD_VERSION: u32 = 1;

pub const FLAG_POLICY_LATEST_WINS: u32 = 0x0001;
pub const FLAG_POLICY_LOSSLESS_BACKPRESSURE: u32 = 0x0002;
pub const FLAG_MODE_SPMC: u32 = 0x0010;
pub const FLAG_MODE_MPMC: u32 = 0x0020;
pub const FLAG_WITH_ARENA: u32 = 0x0100;
pub const FLAG_WITH_REGISTRY: u32 = 0x0200;

/// Slot sequence value stored while a writer is overwriting the slot payload.
///
/// Writers store it before touching the payload and replace it with the real sequence
/// afterwards, so a reader that observes the same non-marker sequence before and after
/// copying the payload knows the copy is not torn.
pub const SLOT_WRITING: u64 = u64::MAX;

/// Header stored at the beginning of the ring buffer shared memory region.
/// 128-byte cache-line aligned to prevent false sharing.
#[repr(C, align(128))]
pub struct RingHeader {
    /// Magic signature bytes ("RINGFIRE"); written last, once the region is initialized
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
    /// Byte offset to the first slot from the start of the mapping
    pub slots_offset: u64,
    /// Cache line padding to exactly 128 bytes
    pub _pad: [u8; 16],
}

// Compile-time checks for RingHeader size and alignment
const _: () = {
    assert!(std::mem::size_of::<RingHeader>() == 128);
    assert!(std::mem::align_of::<RingHeader>() == 128);
};

/// Parameters describing a ring region, used to initialize its header.
pub(crate) struct RingLayout {
    pub capacity: u64,
    pub slot_size: usize,
    pub flags: u32,
    pub schema_sig: u64,
    pub claim_seq: u64,
    pub read_seq: u64,
    pub registry_offset: usize,
    pub registry_count: usize,
    pub slots_offset: usize,
    pub arena_offset: usize,
    pub arena_size: usize,
}

impl RingHeader {
    /// Writes a header with `magic = 0`. Call [`RingHeader::publish`] once the rest of the
    /// region (slots, registry, arena) is initialized so attaching readers never observe a
    /// half-initialized ring.
    ///
    /// # Safety
    /// `ptr` must point to writable memory of at least 128 bytes, 128-byte aligned.
    pub(crate) unsafe fn initialize(ptr: *mut RingHeader, l: &RingLayout) {
        unsafe {
            ptr.write(RingHeader {
                magic: 0,
                version: RINGFIRE_VERSION,
                element_size: l.slot_size as u32,
                capacity: l.capacity,
                mask: l.capacity - 1,
                write_seq: AtomicU64::new(0),
                claim_seq: AtomicU64::new(l.claim_seq),
                flags: l.flags,
                futex_word: AtomicU32::new(0),
                waiting_consumers: AtomicU32::new(0),
                _align_pad: 0,
                read_seq: AtomicU64::new(l.read_seq),
                schema_sig: l.schema_sig,
                arena_offset: l.arena_offset as u64,
                arena_size: l.arena_size as u64,
                reader_registry_offset: l.registry_offset as u32,
                reader_registry_count: l.registry_count as u32,
                slots_offset: l.slots_offset as u64,
                _pad: [0; 16],
            });
        }
    }

    /// Makes an initialized ring visible to attaching readers by storing its magic.
    ///
    /// # Safety
    /// `ptr` must point to a header written by [`RingHeader::initialize`].
    pub(crate) unsafe fn publish(ptr: *mut RingHeader) {
        std::sync::atomic::fence(Ordering::Release);
        unsafe { std::ptr::write_volatile(&mut (*ptr).magic, RINGFIRE_MAGIC) };
    }
}

/// Validated location of the parts of a mapped ring region.
#[derive(Debug, Clone, Copy)]
pub struct RingView {
    pub capacity: u64,
    pub mask: u64,
    pub slot_size: usize,
    pub slots_offset: usize,
}

/// Validates that a mapped region of `len` bytes at `base` holds a well-formed ring whose
/// slots are `expected_slot_size` bytes (or whatever the header says, if `None`) and
/// `slot_align`-aligned, and that every region the header points to lies inside the mapping.
///
/// # Safety
/// `base` must point to `len` readable bytes, 128-byte aligned.
pub unsafe fn validate_ring(
    base: *const u8,
    len: usize,
    expected_slot_size: Option<usize>,
    slot_align: usize,
) -> Result<RingView> {
    if len < std::mem::size_of::<RingHeader>() {
        return Err(RingfireError::CorruptLayout("mapping smaller than the ring header"));
    }
    let header = unsafe { &*(base as *const RingHeader) };
    let magic = unsafe { std::ptr::read_volatile(&header.magic) };
    std::sync::atomic::fence(Ordering::Acquire);
    if magic != RINGFIRE_MAGIC {
        return Err(RingfireError::InvalidMagic {
            expected: RINGFIRE_MAGIC,
            actual: magic,
        });
    }
    if header.version != RINGFIRE_VERSION {
        return Err(RingfireError::VersionMismatch {
            expected: RINGFIRE_VERSION,
            actual: header.version,
        });
    }
    let slot_size = header.element_size as usize;
    if let Some(expected) = expected_slot_size
        && slot_size != expected
    {
        return Err(RingfireError::ElementSizeMismatch {
            expected: slot_size,
            actual: expected,
        });
    }
    if slot_size < 8 {
        return Err(RingfireError::CorruptLayout("slot size smaller than its sequence word"));
    }
    let capacity = header.capacity;
    if !capacity.is_power_of_two() || header.mask != capacity - 1 {
        return Err(RingfireError::CorruptLayout("capacity is not a power of two or mask mismatch"));
    }
    let slots_offset = header.slots_offset as usize;
    if slots_offset < std::mem::size_of::<RingHeader>() || !slots_offset.is_multiple_of(slot_align.max(8)) {
        return Err(RingfireError::CorruptLayout("misplaced or misaligned slots"));
    }
    let slots_end = (capacity as usize)
        .checked_mul(slot_size)
        .and_then(|b| b.checked_add(slots_offset));
    if slots_end.is_none_or(|end| end > len) {
        return Err(RingfireError::CorruptLayout("slots extend past the end of the mapping"));
    }
    if header.reader_registry_offset != 0 {
        let off = header.reader_registry_offset as usize;
        let end = off + header.reader_registry_count as usize * std::mem::size_of::<ReaderSlot>();
        if !off.is_multiple_of(64) || end > len {
            return Err(RingfireError::CorruptLayout("reader registry outside the mapping"));
        }
    }
    if header.arena_offset != 0 {
        let off = header.arena_offset as usize;
        let end = (header.arena_size as usize)
            .checked_add(off)
            .and_then(|e| e.checked_add(64));
        if !off.is_multiple_of(64) || end.is_none_or(|e| e > len) {
            return Err(RingfireError::CorruptLayout("payload arena outside the mapping"));
        }
    }
    Ok(RingView {
        capacity,
        mask: capacity - 1,
        slot_size,
        slots_offset,
    })
}

/// Checks the header's schema signature against `expected` (0 in the header = unverified).
pub(crate) fn check_schema(header: &RingHeader, expected: u64, type_name: &'static str) -> Result<()> {
    if header.schema_sig != 0 && header.schema_sig != expected {
        return Err(RingfireError::SchemaMismatch {
            expected: header.schema_sig,
            actual: expected,
            type_name,
        });
    }
    Ok(())
}

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
    /// Sequence number of this slot (0 = unwritten, [`SLOT_WRITING`] = being overwritten,
    /// N = holds message N)
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
