//! # BlobProducer & BlobConsumer
//!
//! High-throughput variable-sized and large payload IPC channel combining
//! a ring buffer of lightweight descriptors (`BlobPacket<M>`) with a contiguous
//! shared-memory byte arena (`PayloadArena`) and reader registry (`ReaderRegistry`).
//!
//! Ideal for L2/L3 order book updates, large trade batches, and variable network frames.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use memmap2::MmapMut;

use crate::arena::{BlobRef, PayloadArena};
use crate::error::{Result, RingfireError};
use crate::header::{
    ReaderSlot, RingHeader, Slot, FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS, FLAG_WITH_ARENA,
    FLAG_WITH_REGISTRY, RINGFIRE_MAGIC, RINGFIRE_VERSION,
};
use crate::registry::{ReaderInfo, ReaderRegistration, ReaderRegistry, DEFAULT_MAX_READERS};
use crate::signature::LayoutSignature;
use crate::spmc::CleanupMode;
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

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(options.mode)
            .open(&path_buf)?;

        if options.exclusive_lock {
            let fd = file.as_raw_fd();
            let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
            if ret != 0 {
                return Err(RingfireError::ProducerAlreadyExists);
            }
        }

        file.set_len(total_file_size as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let base_ptr = mmap.as_mut_ptr();

        let header_ptr = base_ptr.cast::<RingHeader>();
        unsafe {
            header_ptr.write(RingHeader {
                magic: RINGFIRE_MAGIC,
                version: RINGFIRE_VERSION,
                element_size: slot_size as u32,
                capacity,
                mask: capacity - 1,
                write_seq: std::sync::atomic::AtomicU64::new(0),
                claim_seq: std::sync::atomic::AtomicU64::new(0),
                flags: FLAG_POLICY_LATEST_WINS | FLAG_MODE_SPMC | FLAG_WITH_ARENA | FLAG_WITH_REGISTRY,
                futex_word: std::sync::atomic::AtomicU32::new(0),
                waiting_consumers: std::sync::atomic::AtomicU32::new(0),
                _align_pad: 0,
                read_seq: std::sync::atomic::AtomicU64::new(0),
                schema_sig: M::layout_signature(),
                arena_offset: arena_offset as u64,
                arena_size: arena_capacity as u64,
                reader_registry_offset: registry_offset as u32,
                reader_registry_count: max_readers as u32,
                _pad: [0; 24],
            });
        }

        let registry = unsafe {
            ReaderRegistry::init(base_ptr.add(registry_offset), max_readers)
        };

        let slots_ptr = unsafe { base_ptr.add(slots_offset).cast::<Slot<BlobPacket<M>>>() };
        for i in 0..capacity {
            unsafe {
                let slot = slots_ptr.add(i as usize);
                (*slot).seq = std::sync::atomic::AtomicU64::new(0);
            }
        }

        let arena = unsafe {
            PayloadArena::init(base_ptr.add(arena_offset), arena_capacity)?
        };

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

    /// Publishes a message with metadata and binary payload.
    /// Returns the assigned sequence number.
    #[inline]
    pub fn push(&mut self, meta: &M, payload: &[u8]) -> Result<u64> {
        let blob_ref = self.arena.write_blob(payload, 0)?;

        let packet = BlobPacket {
            meta: *meta,
            blob_ref,
        };

        let idx = (self.seq & self.mask) as usize;
        unsafe {
            let slot = self.slots.add(idx);
            std::ptr::copy_nonoverlapping(&packet, &mut (*slot).data, 1);
            (*slot).seq.store(self.seq, Ordering::Release);
            (*self.header).write_seq.store(self.seq, Ordering::Release);
            wake_futex(&*self.header, 1);
        }

        let current_seq = self.seq;
        self.seq += 1;
        Ok(current_seq)
    }

    /// Publishes with zero-copy writing directly into the payload arena.
    #[inline]
    pub fn push_with<F, R>(&mut self, meta: &M, len: usize, f: F) -> Result<(u64, R)>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let (blob_ref, result) = self.arena.write_blob_with(len, 0, f)?;

        let packet = BlobPacket {
            meta: *meta,
            blob_ref,
        };

        let idx = (self.seq & self.mask) as usize;
        unsafe {
            let slot = self.slots.add(idx);
            std::ptr::copy_nonoverlapping(&packet, &mut (*slot).data, 1);
            (*slot).seq.store(self.seq, Ordering::Release);
            (*self.header).write_seq.store(self.seq, Ordering::Release);
            wake_futex(&*self.header, 1);
        }

        let current_seq = self.seq;
        self.seq += 1;
        Ok((current_seq, result))
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
        if let Some(file) = self.file.take() {
            let fd = file.as_raw_fd();
            unsafe {
                libc::flock(fd, libc::LOCK_UN);
            }
        }

        if self.cleanup_mode == CleanupMode::UnlinkOnDrop {
            let _ = std::fs::remove_file(&self.path);
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
        let path_ref = path.as_ref();
        let file = OpenOptions::new().read(true).write(true).open(path_ref)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let base_ptr = mmap.as_ptr();

        let header_ptr = base_ptr.cast::<RingHeader>();
        let header = unsafe { &*header_ptr };

        if header.magic != RINGFIRE_MAGIC {
            return Err(RingfireError::InvalidMagic {
                expected: RINGFIRE_MAGIC,
                actual: header.magic,
            });
        }
        if header.version != RINGFIRE_VERSION {
            return Err(RingfireError::VersionMismatch {
                expected: RINGFIRE_VERSION,
                actual: header.version,
            });
        }

        let slot_size = std::mem::size_of::<Slot<BlobPacket<M>>>();
        if header.element_size as usize != slot_size {
            return Err(RingfireError::ElementSizeMismatch {
                expected: header.element_size as usize,
                actual: slot_size,
            });
        }

        let expected_sig = M::layout_signature();
        if header.schema_sig != 0 && header.schema_sig != expected_sig {
            return Err(RingfireError::SchemaMismatch {
                expected: header.schema_sig,
                actual: expected_sig,
                type_name: std::any::type_name::<M>(),
            });
        }

        if header.arena_offset == 0 {
            return Err(RingfireError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Shared memory queue does not contain a PayloadArena",
            )));
        }

        let capacity = header.capacity;
        let mask = header.mask;

        // Locate registry
        let (registry, registration) = if header.reader_registry_offset != 0 {
            let reg = unsafe {
                ReaderRegistry::from_ptr(
                    mmap.as_mut_ptr().add(header.reader_registry_offset as usize),
                    header.reader_registry_count as usize,
                )
            };
            let reg_handle = reg.register(reader_name, 1).ok();
            (Some(reg), reg_handle)
        } else {
            (None, None)
        };

        // Locate slots
        let registry_size = (header.reader_registry_count as usize) * std::mem::size_of::<ReaderSlot>();
        let slots_offset = (header.reader_registry_offset as usize + registry_size + 127) & !127;
        let slots_ptr = unsafe { base_ptr.add(slots_offset).cast::<Slot<BlobPacket<M>>>() };

        // Locate arena
        let arena = unsafe {
            PayloadArena::from_ptr(mmap.as_mut_ptr().add(header.arena_offset as usize))?
        };

        let current_write = header.write_seq.load(Ordering::Acquire);
        let cursor = if current_write > capacity {
            current_write - capacity + 1
        } else {
            1
        };

        if let Some(ref reg) = registration {
            reg.update_cursor(cursor);
        }

        Ok(Self {
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            arena,
            registry,
            registration,
            capacity,
            mask,
            cursor,
            lapped_total: 0,
            _marker: PhantomData,
        })
    }

    /// Read next blob message with explicit status.
    #[inline]
    pub fn recv_status(&mut self, meta: &mut M, out: &mut [u8]) -> Result<BlobRecvStatus> {
        unsafe {
            let mut skipped = 0;
            let current_write = (*self.header).write_seq.load(Ordering::Acquire);

            if current_write > self.cursor && (current_write - self.cursor) >= self.capacity {
                let oldest = current_write - self.capacity + 1;
                skipped = oldest - self.cursor;
                self.lapped_total += skipped;
                self.cursor = oldest;
            }

            loop {
                let idx = (self.cursor & self.mask) as usize;
                let slot = self.slots.add(idx);
                let s1 = (*slot).seq.load(Ordering::Acquire);

                if s1 < self.cursor {
                    return Ok(BlobRecvStatus::Empty);
                }

                if s1 > self.cursor {
                    let slot_skipped = s1 - self.cursor;
                    skipped += slot_skipped;
                    self.lapped_total += slot_skipped;
                    self.cursor = s1;
                    continue;
                }

                // Copy slot packet (metadata + blob_ref)
                let packet = (*slot).data;
                let payload_len = packet.blob_ref.len as usize;

                if out.len() < payload_len {
                    return Err(RingfireError::BufferTooSmall {
                        required: payload_len,
                        provided: out.len(),
                    });
                }

                // Copy payload from arena
                self.arena.read_blob(packet.blob_ref, out)?;

                // Two-phase consistency verification: slot sequence must match and arena not lapped
                let s2 = (*slot).seq.load(Ordering::Acquire);
                if s1 == s2 && !self.arena.is_lapped(packet.blob_ref) {
                    *meta = packet.meta;
                    self.cursor += 1;

                    if let Some(ref reg) = self.registration {
                        reg.update_cursor(self.cursor);
                    }

                    if skipped > 0 {
                        return Ok(BlobRecvStatus::Lapped { skipped, payload_len });
                    } else {
                        return Ok(BlobRecvStatus::Ok { payload_len });
                    }
                }

                // Slot or arena was modified concurrently, re-read latest
                core::hint::spin_loop();
            }
        }
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
    #[inline]
    pub fn view<R>(&mut self, mut f: impl FnMut(&M, &[u8]) -> R) -> Result<Option<R>> {
        unsafe {
            let current_write = (*self.header).write_seq.load(Ordering::Acquire);

            if current_write > self.cursor && (current_write - self.cursor) >= self.capacity {
                let oldest = current_write - self.capacity + 1;
                self.lapped_total += oldest - self.cursor;
                self.cursor = oldest;
            }

            loop {
                let idx = (self.cursor & self.mask) as usize;
                let slot = self.slots.add(idx);
                let s1 = (*slot).seq.load(Ordering::Acquire);

                if s1 < self.cursor {
                    return Ok(None);
                }

                if s1 > self.cursor {
                    self.lapped_total += s1 - self.cursor;
                    self.cursor = s1;
                    continue;
                }

                let packet = (*slot).data;
                let res = self.arena.view_blob(packet.blob_ref, |slice| f(&packet.meta, slice));

                let s2 = (*slot).seq.load(Ordering::Acquire);
                if s1 == s2 && !self.arena.is_lapped(packet.blob_ref) {
                    self.cursor += 1;
                    if let Some(ref reg) = self.registration {
                        reg.update_cursor(self.cursor);
                    }
                    return Ok(Some(res));
                }

                core::hint::spin_loop();
            }
        }
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
