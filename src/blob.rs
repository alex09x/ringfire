//! # BlobProducer & BlobConsumer
//!
//! High-throughput variable-sized and large payload IPC channel combining
//! a ring buffer of lightweight descriptors (`BlobPacket<M>`) with a contiguous
//! shared-memory byte arena (`PayloadArena`) and reader registry (`ReaderRegistry`).
//!
//! Ideal for L2/L3 order book updates, large trade batches, and variable network frames.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use memmap2::MmapMut;

use crate::arena::{BlobRef, PayloadArena};
use crate::error::{Result, RingfireError};
use crate::header::{
    check_schema, validate_ring, ReaderSlot, RingHeader, RingLayout, Slot, FLAG_MODE_SPMC,
    FLAG_POLICY_LATEST_WINS, FLAG_WITH_ARENA, FLAG_WITH_REGISTRY,
};
use crate::registry::{ReaderInfo, ReaderRegistration, ReaderRegistry, DEFAULT_MAX_READERS};
use crate::shm::create_backing_file;
use crate::signature::LayoutSignature;
use crate::spmc::{oldest_retained, read_slot, write_slot, CleanupMode, SlotRead};
use crate::wait::wake_futex;

/// Descriptor stored in each ring buffer slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct BlobPacket<M> {
    pub meta: M,
    pub blob_ref: BlobRef,
}

/// Status returned by `BlobConsumer::recv_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobRecvStatus {
    /// New blob payload successfully received with `payload_len` bytes.
    Ok { payload_len: usize },
    /// Queue is currently empty (no new blobs).
    Empty,
    /// Consumer was lapped by writer; skipped `skipped` messages and caught up.
    Lapped { skipped: u64, payload_len: usize },
}

/// Builder for configuring and creating a `BlobProducer`.
pub struct BlobProducerBuilder {
    capacity: u64,
    arena_capacity: usize,
    max_readers: usize,
    cleanup_mode: CleanupMode,
    mode: u32,
    exclusive_lock: bool,
}

impl BlobProducerBuilder {
    pub fn new(capacity: u64, arena_capacity: usize) -> Self {
        Self {
            capacity,
            arena_capacity,
            max_readers: DEFAULT_MAX_READERS,
            cleanup_mode: CleanupMode::UnlinkOnDrop,
            mode: 0o660,
            exclusive_lock: true,
        }
    }

    pub fn max_readers(mut self, count: usize) -> Self {
        self.max_readers = count;
        self
    }

    pub fn cleanup_mode(mut self, mode: CleanupMode) -> Self {
        self.cleanup_mode = mode;
        self
    }

    pub fn file_mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    pub fn exclusive_lock(mut self, lock: bool) -> Self {
        self.exclusive_lock = lock;
        self
    }

    pub fn build<M: Copy + 'static, P: AsRef<Path>>(self, path: P) -> Result<BlobProducer<M>> {
        BlobProducer::create_with_options(path, self)
    }
}

/// Producer for variable-sized binary blobs and metadata in shared memory.
pub struct BlobProducer<M: Copy + 'static> {
    path: PathBuf,
    file: Option<File>,
    header: *mut RingHeader,
    slots: *mut Slot<BlobPacket<M>>,
    arena: PayloadArena,
    registry: ReaderRegistry,
    capacity: u64,
    mask: u64,
    seq: u64,
    cleanup_mode: CleanupMode,
    _marker: PhantomData<M>,
    _mmap: MmapMut,
}

unsafe impl<M: Copy + Send + 'static> Send for BlobProducer<M> {}

impl<M: Copy + 'static> BlobProducer<M> {
    /// Creates a new blob producer with default options.
    /// `capacity` is the number of ring slots (power of 2).
    /// `arena_capacity` is the byte capacity of the payload arena (power of 2, e.g. 16MB).
    pub fn create<P: AsRef<Path>>(path: P, capacity: u64, arena_capacity: usize) -> Result<Self> {
        BlobProducerBuilder::new(capacity, arena_capacity).build(path)
    }

    /// Creates a new blob producer with custom builder options.
    pub fn create_with_options<P: AsRef<Path>>(
        path: P,
        options: BlobProducerBuilder,
    ) -> Result<Self> {
        let capacity = options.capacity;
        let arena_capacity = options.arena_capacity;
        let max_readers = options.max_readers;

        if !capacity.is_power_of_two() {
            return Err(RingfireError::InvalidCapacity(capacity));
        }
        if !arena_capacity.is_power_of_two() || arena_capacity < 64 {
            return Err(RingfireError::InvalidCapacity(arena_capacity as u64));
        }

        // Layout calculation:
        // [RingHeader: 128B]
        // [ReaderRegistry: max_readers * 64B]
        // [Slots: capacity * size_of::<Slot<BlobPacket<M>>>()]
        // [PayloadArena: 64B Header + arena_capacity]
        let header_size = std::mem::size_of::<RingHeader>(); // 128
        let registry_offset = header_size;
        let registry_size = max_readers * std::mem::size_of::<ReaderSlot>();

        let slots_offset = (registry_offset + registry_size + 127) & !127;
        let slot_size = std::mem::size_of::<Slot<BlobPacket<M>>>();
        let slots_size = capacity as usize * slot_size;

        let arena_offset = (slots_offset + slots_size + 127) & !127;
        let arena_header_size = 64;
        let arena_total = arena_header_size + arena_capacity;

        let total_file_size = arena_offset + arena_total;
        let path_buf = path.as_ref().to_path_buf();

        let file = create_backing_file(&path_buf, options.mode, options.exclusive_lock, total_file_size as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let base_ptr = mmap.as_mut_ptr();

        let header_ptr = base_ptr.cast::<RingHeader>();
        unsafe {
            RingHeader::initialize(
                header_ptr,
                &RingLayout {
                    capacity,
                    slot_size,
                    flags: FLAG_POLICY_LATEST_WINS | FLAG_MODE_SPMC | FLAG_WITH_ARENA | FLAG_WITH_REGISTRY,
                    schema_sig: M::layout_signature(),
                    claim_seq: 0,
                    read_seq: 0,
                    registry_offset,
                    registry_count: max_readers,
                    slots_offset,
                    arena_offset,
                    arena_size: arena_capacity,
                },
            );
        }

        let registry = unsafe {
            ReaderRegistry::init(base_ptr.add(registry_offset), max_readers)
        };

        let slots_ptr = unsafe { base_ptr.add(slots_offset).cast::<Slot<BlobPacket<M>>>() };
        for i in 0..capacity {
            unsafe {
                (*slots_ptr.add(i as usize)).seq = std::sync::atomic::AtomicU64::new(0);
            }
        }

        let arena = unsafe {
            PayloadArena::init(base_ptr.add(arena_offset), arena_capacity)?
        };
        unsafe { RingHeader::publish(header_ptr) };

        Ok(Self {
            path: path_buf,
            file: Some(file),
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            arena,
            registry,
            capacity,
            mask: capacity - 1,
            seq: 1,
            cleanup_mode: options.cleanup_mode,
            _marker: PhantomData,
        })
    }

    #[inline(always)]
    fn publish_packet(&mut self, packet: BlobPacket<M>) -> u64 {
        let seq = self.seq;
        unsafe {
            write_slot(self.slots.add((seq & self.mask) as usize), seq, &packet);
            (*self.header).write_seq.store(seq, Ordering::Release);
            wake_futex(&*self.header, i32::MAX);
        }
        self.seq += 1;
        seq
    }

    /// Publishes a message with metadata and binary payload.
    /// Returns the assigned sequence number.
    #[inline]
    pub fn push(&mut self, meta: &M, payload: &[u8]) -> Result<u64> {
        let blob_ref = self.arena.write_blob(payload, 0)?;
        Ok(self.publish_packet(BlobPacket { meta: *meta, blob_ref }))
    }

    /// Publishes with zero-copy writing directly into the payload arena.
    #[inline]
    pub fn push_with<F, R>(&mut self, meta: &M, len: usize, f: F) -> Result<(u64, R)>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let (blob_ref, result) = self.arena.write_blob_with(len, 0, f)?;
        Ok((self.publish_packet(BlobPacket { meta: *meta, blob_ref }), result))
    }

    /// Minimum sequence across all currently living registered readers.
    #[inline]
    pub fn min_reader_seq(&self) -> Option<u64> {
        self.registry.min_reader_seq()
    }

    /// Maximum lag across registered readers.
    #[inline]
    pub fn reader_lag(&self) -> u64 {
        self.registry.reader_lag(self.seq - 1)
    }

    /// Remaining write headroom before the slowest reader is lapped.
    #[inline]
    pub fn headroom(&self) -> u64 {
        self.registry.headroom(self.seq - 1, self.capacity)
    }

    /// Status of all registered active readers.
    pub fn active_readers(&self) -> Vec<ReaderInfo> {
        self.registry.active_readers(self.seq - 1)
    }

    /// Current sequence number of the producer.
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.seq - 1
    }
}

impl<M: Copy + 'static> Drop for BlobProducer<M> {
    fn drop(&mut self) {
        if self.cleanup_mode == CleanupMode::UnlinkOnDrop {
            let _ = std::fs::remove_file(&self.path);
        }
        if let Some(file) = self.file.take() {
            unsafe {
                libc::flock(file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

/// Convenience methods for producers with no metadata (`M = ()`).
impl BlobProducer<()> {
    #[inline]
    pub fn push_payload(&mut self, payload: &[u8]) -> Result<u64> {
        self.push(&(), payload)
    }
}

/// Consumer for reading variable-sized binary blobs and metadata from shared memory.
pub struct BlobConsumer<M: Copy + 'static> {
    header: *const RingHeader,
    slots: *const Slot<BlobPacket<M>>,
    arena: PayloadArena,
    registry: Option<ReaderRegistry>,
    registration: Option<ReaderRegistration>,
    capacity: u64,
    mask: u64,
    cursor: u64,
    lapped_total: u64,
    _marker: PhantomData<M>,
    _mmap: MmapMut,
}

unsafe impl<M: Copy + Send + 'static> Send for BlobConsumer<M> {}

impl<M: Copy + 'static> BlobConsumer<M> {
    /// Attach to an existing blob ring buffer at `path`.
    pub fn attach<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::attach_with_name(path, "anonymous")
    }

    /// Attach to an existing blob ring buffer and register in the `ReaderRegistry`.
    pub fn attach_with_name<P: AsRef<Path>>(path: P, reader_name: &str) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let view = unsafe {
            validate_ring(
                mmap.as_ptr(),
                mmap.len(),
                Some(std::mem::size_of::<Slot<BlobPacket<M>>>()),
                std::mem::align_of::<Slot<BlobPacket<M>>>(),
            )?
        };
        let header_ptr = mmap.as_ptr().cast::<RingHeader>();
        let header = unsafe { &*header_ptr };
        check_schema(header, M::layout_signature(), std::any::type_name::<M>())?;

        if header.arena_offset == 0 {
            return Err(RingfireError::CorruptLayout(
                "shared memory queue does not contain a PayloadArena",
            ));
        }
        let arena = unsafe {
            PayloadArena::from_ptr(mmap.as_mut_ptr().add(header.arena_offset as usize))?
        };
        if arena.capacity() as u64 != header.arena_size {
            return Err(RingfireError::CorruptLayout("arena header disagrees with ring header"));
        }

        let capacity = view.capacity;
        let slots_ptr = unsafe { mmap.as_ptr().add(view.slots_offset).cast::<Slot<BlobPacket<M>>>() };
        let cursor = oldest_retained(header.write_seq.load(Ordering::Acquire), capacity);

        let (registry, registration) = if header.reader_registry_offset != 0 {
            let reg = unsafe {
                ReaderRegistry::from_ptr(
                    mmap.as_mut_ptr().add(header.reader_registry_offset as usize),
                    header.reader_registry_count as usize,
                )
            };
            let reg_handle = reg.register(reader_name, cursor).ok();
            (Some(reg), reg_handle)
        } else {
            (None, None)
        };

        Ok(Self {
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            arena,
            registry,
            registration,
            capacity,
            mask: view.mask,
            cursor,
            lapped_total: 0,
            _marker: PhantomData,
        })
    }

    #[cold]
    #[inline(never)]
    fn skip_overwritten(&mut self, seen: u64) -> u64 {
        let write_seq = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        let oldest = oldest_retained(write_seq.max(seen), self.capacity);
        if oldest > self.cursor {
            let skipped = oldest - self.cursor;
            self.cursor = oldest;
            self.lapped_total += skipped;
            skipped
        } else {
            0
        }
    }

    /// Drops the message at the cursor (its payload was overwritten or its descriptor is bogus).
    #[cold]
    #[inline(never)]
    fn skip_current(&mut self) {
        self.cursor += 1;
        self.lapped_total += 1;
    }

    /// Core receive loop. `consume` gets the metadata and the payload slice while it still
    /// lives in the arena; its result is kept only if the payload was not overwritten
    /// during the call. Returns the result, the payload length and the skipped count.
    #[inline(always)]
    fn next_blob<R>(
        &mut self,
        mut check: impl FnMut(usize) -> Result<()>,
        mut consume: impl FnMut(&M, &[u8]) -> R,
    ) -> Result<Option<(R, usize, u64)>> {
        let mut skipped = 0;
        loop {
            let slot = unsafe { self.slots.add((self.cursor & self.mask) as usize) };
            match unsafe { read_slot(slot, self.cursor) } {
                SlotRead::Item(packet) => {
                    let blob_ref = packet.blob_ref;
                    if !self.arena.is_in_bounds(blob_ref) || self.arena.is_lapped(blob_ref) {
                        self.skip_current();
                        skipped += 1;
                        continue;
                    }
                    let len = blob_ref.len as usize;
                    check(len)?;
                    let result = self.arena.view_blob(blob_ref, |bytes| consume(&packet.meta, bytes));
                    if self.arena.is_lapped(blob_ref) {
                        self.skip_current();
                        skipped += 1;
                        continue;
                    }
                    self.cursor += 1;
                    if let Some(ref reg) = self.registration {
                        reg.update_cursor(self.cursor);
                    }
                    return Ok(Some((result, len, skipped)));
                }
                SlotRead::Overwritten(seen) => {
                    let s = self.skip_overwritten(seen);
                    if s == 0 {
                        return Ok(None);
                    }
                    skipped += s;
                }
                SlotRead::Pending => return Ok(None),
            }
        }
    }

    /// Read next blob message with explicit status.
    ///
    /// Messages whose payload was already overwritten in the arena are skipped and
    /// counted as lapped. Returns `BufferTooSmall` (without consuming the message) if
    /// `out` cannot hold the payload.
    #[inline]
    pub fn recv_status(&mut self, meta: &mut M, out: &mut [u8]) -> Result<BlobRecvStatus> {
        let out_len = out.len();
        let got = self.next_blob(
            |len| {
                if out_len < len {
                    Err(RingfireError::BufferTooSmall {
                        required: len,
                        provided: out_len,
                    })
                } else {
                    Ok(())
                }
            },
            |m, bytes| {
                out[..bytes.len()].copy_from_slice(bytes);
                *m
            },
        )?;
        Ok(match got {
            Some((m, payload_len, 0)) => {
                *meta = m;
                BlobRecvStatus::Ok { payload_len }
            }
            Some((m, payload_len, skipped)) => {
                *meta = m;
                BlobRecvStatus::Lapped { skipped, payload_len }
            }
            None => BlobRecvStatus::Empty,
        })
    }

    /// Non-blocking receive returning `Ok(Some(payload_bytes))` or `Ok(None)`.
    #[inline]
    pub fn recv(&mut self, meta: &mut M, out: &mut [u8]) -> Result<Option<usize>> {
        match self.recv_status(meta, out)? {
            BlobRecvStatus::Ok { payload_len } => Ok(Some(payload_len)),
            BlobRecvStatus::Lapped { payload_len, .. } => Ok(Some(payload_len)),
            BlobRecvStatus::Empty => Ok(None),
        }
    }

    /// Inspect next blob payload in-place inside the arena closure.
    ///
    /// The closure reads shared memory the producer may be overwriting: if the payload
    /// is lapped during the call, the result is discarded, the message is counted as
    /// lapped and the next one is offered. Keep the closure free of side effects that
    /// cannot be repeated, and treat the bytes as untrusted until it returns.
    #[inline]
    pub fn view<R>(&mut self, mut f: impl FnMut(&M, &[u8]) -> R) -> Result<Option<R>> {
        Ok(self.next_blob(|_| Ok(()), |m, bytes| f(m, bytes))?.map(|(r, _, _)| r))
    }

    /// Current consumer sequence cursor.
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Total messages skipped due to writer buffer lapping.
    #[inline]
    pub fn lapped_count(&self) -> u64 {
        self.lapped_total
    }

    /// Number of available messages waiting to be read.
    #[inline]
    pub fn lag(&self) -> u64 {
        let current_write = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        current_write.saturating_sub(self.cursor.saturating_sub(1))
    }

    /// Access the reader registry if enabled.
    #[inline]
    pub fn registry(&self) -> Option<&ReaderRegistry> {
        self.registry.as_ref()
    }
}

impl<M: Copy + 'static> std::fmt::Debug for BlobProducer<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobProducer")
            .field("capacity", &self.capacity)
            .field("seq", &self.seq)
            .field("arena_capacity", &self.arena.capacity())
            .finish()
    }
}

impl<M: Copy + 'static> std::fmt::Debug for BlobConsumer<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobConsumer")
            .field("capacity", &self.capacity)
            .field("cursor", &self.cursor)
            .field("lapped_total", &self.lapped_total)
            .finish()
    }
}

/// Convenience methods for consumers with no metadata (`M = ()`).
impl BlobConsumer<()> {
    #[inline]
    pub fn recv_payload(&mut self, out: &mut [u8]) -> Result<Option<usize>> {
        let mut meta = ();
        self.recv(&mut meta, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_producer_consumer_roundtrip() {
        let tmp_path = std::env::temp_dir().join("test_ringfire_blob.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let mut producer = BlobProducer::<u64>::create(&tmp_path, 64, 65536).unwrap();
        let mut consumer = BlobConsumer::<u64>::attach_with_name(&tmp_path, "test_cons").unwrap();

        let mut out = [0u8; 1024];
        let mut meta = 0u64;

        assert_eq!(consumer.recv(&mut meta, &mut out).unwrap(), None);

        let payload1 = b"Hello from Ringfire PayloadArena!";
        producer.push(&101, payload1).unwrap();

        let payload2 = vec![0xFEu8; 512];
        producer.push(&102, &payload2).unwrap();

        let len1 = consumer.recv(&mut meta, &mut out).unwrap().unwrap();
        assert_eq!(meta, 101);
        assert_eq!(&out[..len1], payload1);

        let len2 = consumer.recv(&mut meta, &mut out).unwrap().unwrap();
        assert_eq!(meta, 102);
        assert_eq!(&out[..len2], &payload2[..]);

        assert_eq!(consumer.recv(&mut meta, &mut out).unwrap(), None);
    }

    #[test]
    fn test_blob_view_in_place() {
        let tmp_path = std::env::temp_dir().join("test_ringfire_blob_view.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let mut producer = BlobProducer::<()>::create(&tmp_path, 64, 65536).unwrap();
        let mut consumer = BlobConsumer::<()>::attach(&tmp_path).unwrap();

        let payload = b"In-place inspection test data";
        producer.push_payload(payload).unwrap();

        let inspected_len = consumer
            .view(|_, slice| {
                assert_eq!(slice, payload);
                slice.len()
            })
            .unwrap()
            .unwrap();

        assert_eq!(inspected_len, payload.len());
    }
}
