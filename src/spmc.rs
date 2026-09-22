use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use memmap2::MmapMut;

use crate::checkpoint::OffsetCheckpoint;
use crate::error::{Result, RingfireError};
use crate::header::{
    RingHeader, Slot, FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS, FLAG_POLICY_LOSSLESS_BACKPRESSURE,
    FLAG_WITH_REGISTRY, RINGFIRE_MAGIC, RINGFIRE_VERSION,
};
use crate::registry::{ReaderRegistration, ReaderRegistry, DEFAULT_MAX_READERS};
use crate::signature::LayoutSignature;
use crate::wait::{wake_futex, WaitStrategy};

/// Cleanup behavior for the shared memory file when the producer is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupMode {
    /// Automatically remove the file from `/dev/shm` on drop.
    UnlinkOnDrop,
    /// Keep the file on disk/shm across process termination.
    Persistent,
}

/// Flow control policy for ring buffer publishing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowControl {
    /// Writer always writes; lagging readers are lapped (LatestWins, lossy).
    LossyLatestWins,
    /// Writer paces itself and waits if writing would overwrite the slowest active registered reader (guaranteed delivery).
    LosslessBackpressure,
}

/// Status returned by `RingConsumer::recv_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvStatus<T> {
    /// New item successfully received.
    Ok(T),
    /// No new items currently available.
    Empty,
    /// Consumer was lapped by writer; skipped `skipped` messages and caught up to `item`.
    Lapped { skipped: u64, item: T },
}

/// Builder for configuring and creating a `RingProducer`.
pub struct RingProducerBuilder {
    capacity: u64,
    cleanup_mode: CleanupMode,
    flow_control: FlowControl,
    max_readers: usize,
    mode: u32,
    exclusive_lock: bool,
}

impl RingProducerBuilder {
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            cleanup_mode: CleanupMode::UnlinkOnDrop,
            flow_control: FlowControl::LossyLatestWins,
            max_readers: 0,
            mode: 0o660,
            exclusive_lock: true,
        }
    }

    pub fn cleanup_mode(mut self, mode: CleanupMode) -> Self {
        self.cleanup_mode = mode;
        self
    }

    pub fn flow_control(mut self, flow_control: FlowControl) -> Self {
        self.flow_control = flow_control;
        if flow_control == FlowControl::LosslessBackpressure && self.max_readers == 0 {
            self.max_readers = DEFAULT_MAX_READERS;
        }
        self
    }

    pub fn max_readers(mut self, max: usize) -> Self {
        self.max_readers = max;
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

    pub fn build<T: Copy + 'static, P: AsRef<Path>>(self, path: P) -> Result<RingProducer<T>> {
        RingProducer::create_with_options(path, self)
    }
}

/// Single-producer ring buffer writer backed by shared memory (`/dev/shm`).
pub struct RingProducer<T: Copy + 'static> {
    path: PathBuf,
    file: Option<File>,
    header: *mut RingHeader,
    slots: *mut Slot<T>,
    capacity: u64,
    mask: u64,
    seq: u64,
    cleanup_mode: CleanupMode,
    flow_control: FlowControl,
    registry: Option<ReaderRegistry>,
    _marker: PhantomData<T>,
    _mmap: MmapMut,
}

unsafe impl<T: Copy + Send + 'static> Send for RingProducer<T> {}

impl<T: Copy + 'static> RingProducer<T> {
    /// Creates a new shared memory ring buffer at `path` with `capacity` slots.
    /// `capacity` must be a power of two.
    pub fn create<P: AsRef<Path>>(path: P, capacity: u64) -> Result<Self> {
        RingProducerBuilder::new(capacity).build(path)
    }

    /// Creates a new ring buffer with custom builder options.
    pub fn create_with_options<P: AsRef<Path>>(
        path: P,
        options: RingProducerBuilder,
    ) -> Result<Self> {
        let capacity = options.capacity;
        if !capacity.is_power_of_two() {
            return Err(RingfireError::InvalidCapacity(capacity));
        }

        let max_readers = options.max_readers;
        let registry_bytes = if max_readers > 0 {
            max_readers * std::mem::size_of::<crate::header::ReaderSlot>()
        } else {
            0
        };

        let slot_size = std::mem::size_of::<Slot<T>>();
        let total_size = std::mem::size_of::<RingHeader>() + registry_bytes + (capacity as usize * slot_size);

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

        file.set_len(total_size as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let flags = (match options.flow_control {
            FlowControl::LossyLatestWins => FLAG_POLICY_LATEST_WINS,
            FlowControl::LosslessBackpressure => FLAG_POLICY_LOSSLESS_BACKPRESSURE,
        }) | FLAG_MODE_SPMC
            | (if max_readers > 0 {
                FLAG_WITH_REGISTRY
            } else {
                0
            });

        let header_ptr = mmap.as_mut_ptr().cast::<RingHeader>();
        unsafe {
            header_ptr.write(RingHeader {
                magic: RINGFIRE_MAGIC,
                version: RINGFIRE_VERSION,
                element_size: slot_size as u32,
                capacity,
                mask: capacity - 1,
                write_seq: std::sync::atomic::AtomicU64::new(0),
                claim_seq: std::sync::atomic::AtomicU64::new(0),
                flags,
                futex_word: std::sync::atomic::AtomicU32::new(0),
                waiting_consumers: std::sync::atomic::AtomicU32::new(0),
                _align_pad: 0,
                read_seq: std::sync::atomic::AtomicU64::new(0),
                schema_sig: T::layout_signature(),
                arena_offset: 0,
                arena_size: 0,
                reader_registry_offset: if max_readers > 0 {
                    std::mem::size_of::<RingHeader>() as u32
                } else {
                    0
                },
                reader_registry_count: max_readers as u32,
                _pad: [0; 24],
            });
        }

        let registry = if max_readers > 0 {
            let reg_ptr = unsafe { mmap.as_mut_ptr().add(std::mem::size_of::<RingHeader>()) };
            let reg = unsafe { ReaderRegistry::init(reg_ptr, max_readers) };
            Some(reg)
        } else {
            None
        };

        let slots_ptr = unsafe {
            mmap.as_mut_ptr()
                .add(std::mem::size_of::<RingHeader>() + registry_bytes)
                .cast::<Slot<T>>()
        };

        // Initialize slots sequence numbers to 0
        for i in 0..capacity {
            unsafe {
                let slot = slots_ptr.add(i as usize);
                (*slot).seq = std::sync::atomic::AtomicU64::new(0);
            }
        }

        Ok(Self {
            path: path_buf,
            file: Some(file),
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity,
            mask: capacity - 1,
            seq: 1,
            cleanup_mode: options.cleanup_mode,
            flow_control: options.flow_control,
            registry,
            _marker: PhantomData,
        })
    }

    /// Publishes a message into the ring buffer.
    /// Under `FlowControl::LosslessBackpressure`, waits if writing would overwrite the slowest active registered reader.
    /// Uses release ordering so consumers see the complete payload.
    #[inline(always)]
    pub fn push(&mut self, item: &T) {
        if self.flow_control == FlowControl::LosslessBackpressure {
            self.wait_for_headroom();
        }

        let idx = (self.seq & self.mask) as usize;
        unsafe {
            let slot = self.slots.add(idx);
            std::ptr::copy_nonoverlapping(item, &mut (*slot).data, 1);
            (*slot).seq.store(self.seq, Ordering::Release);
            (*self.header).write_seq.store(self.seq, Ordering::Release);

            if (*self.header).waiting_consumers.load(Ordering::Relaxed) > 0 {
                wake_futex(&*self.header, 1);
            }
        }
        self.seq += 1;
    }

    /// Attempts to publish a message into the ring buffer without blocking.
    /// Under `FlowControl::LosslessBackpressure`, returns `Err(RingfireError::BackpressureBufferFull)`
    /// if the buffer is full and would overwrite the slowest reader.
    #[inline]
    pub fn try_push(&mut self, item: &T) -> Result<()> {
        if self.flow_control == FlowControl::LosslessBackpressure
            && let Some(min_seq) = self.registry.as_ref().and_then(|reg| reg.min_reader_seq())
        {
            let next_seq = self.seq;
            if next_seq >= self.capacity && (next_seq - self.capacity + 1) > min_seq {
                return Err(RingfireError::BackpressureBufferFull);
            }
        }
        self.push(item);
        Ok(())
    }

    /// Blocks until there is room to write without overwriting the slowest active registered reader.
    #[cold]
    #[inline(never)]
    pub fn wait_for_headroom(&self) {
        if let Some(ref reg) = self.registry {
            let mut spins = 0u32;
            loop {
                if let Some(min_seq) = reg.min_reader_seq() {
                    let next_seq = self.seq;
                    if next_seq >= self.capacity && (next_seq - self.capacity + 1) > min_seq {
                        spins += 1;
                        if spins < 100 {
                            core::hint::spin_loop();
                        } else if spins < 1000 {
                            std::thread::yield_now();
                        } else {
                            reg.prune_dead_readers();
                            std::thread::sleep(std::time::Duration::from_micros(10));
                        }
                        continue;
                    }
                }
                break;
            }
        }
    }

    /// Returns the configured flow control mode.
    #[inline]
    pub fn flow_control(&self) -> FlowControl {
        self.flow_control
    }

    /// Returns a reference to the shared-memory `ReaderRegistry` if enabled.
    #[inline]
    pub fn registry(&self) -> Option<&ReaderRegistry> {
        self.registry.as_ref()
    }

    /// Available write headroom (number of messages before the slowest active reader is lapped).
    #[inline]
    pub fn headroom(&self) -> u64 {
        if let Some(ref reg) = self.registry {
            reg.headroom(self.seq, self.capacity)
        } else {
            self.capacity
        }
    }

    /// Maximum lag across all active readers in messages.
    #[inline]
    pub fn reader_lag(&self) -> u64 {
        if let Some(ref reg) = self.registry {
            reg.reader_lag(self.seq)
        } else {
            0
        }
    }

    /// Publishes a batch of messages consecutively into the ring buffer.
    /// Amortizes header write sequence and futex wake syscalls.
    #[inline]
    pub fn push_batch(&mut self, items: &[T]) {
        if items.is_empty() {
            return;
        }

        for item in items {
            let idx = (self.seq & self.mask) as usize;
            unsafe {
                let slot = self.slots.add(idx);
                std::ptr::copy_nonoverlapping(item, &mut (*slot).data, 1);
                (*slot).seq.store(self.seq, Ordering::Release);
            }
            self.seq += 1;
        }

        let last_seq = self.seq - 1;
        unsafe {
            (*self.header).write_seq.store(last_seq, Ordering::Release);
            wake_futex(&*self.header, 1);
        }
    }

    /// Sequence number of the last published message (0 if no messages pushed yet).
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.seq - 1
    }

    /// Sequence number that will be assigned to the next pushed message.
    #[inline]
    pub fn next_sequence(&self) -> u64 {
        self.seq
    }

    /// Current sequence published in the shared memory header visible to consumers.
    #[inline]
    pub fn published_sequence(&self) -> u64 {
        unsafe { (*self.header).write_seq.load(Ordering::Acquire) }
    }

    /// Capacity of the ring buffer.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Shared memory file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<T: Copy + 'static> Drop for RingProducer<T> {
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

impl<T: Copy + 'static> std::fmt::Debug for RingProducer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingProducer")
            .field("capacity", &self.capacity)
            .field("seq", &self.seq)
            .finish()
    }
}

/// Initial cursor positioning strategy when attaching a consumer to a ring buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerStartMode {
    /// Start reading from the latest published sequence in the buffer.
    ///
    /// Jumps directly to the head of the stream for live execution without
    /// reading historical backlog.
    Latest,

    /// Start reading from the next sequence to be published (strictly future messages).
    Head,

    /// Start reading from the oldest available message surviving in the buffer.
    ///
    /// Replays all retained messages (up to buffer capacity).
    Oldest,

    /// Start reading from a specific sequence number.
    ///
    /// If the sequence has already been overwritten due to buffer wraparound,
    /// the consumer automatically catches up to the oldest available sequence
    /// and tracks the lapped count.
    Sequence(u64),
}

/// Builder for configuring and attaching a `RingConsumer`.
#[derive(Debug, Clone)]
pub struct RingConsumerBuilder<T: Copy + LayoutSignature + 'static> {
    start_mode: ConsumerStartMode,
    offset_file: Option<PathBuf>,
    consumer_name: Option<String>,
    _marker: PhantomData<T>,
}

impl<T: Copy + LayoutSignature + 'static> Default for RingConsumerBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy + LayoutSignature + 'static> RingConsumerBuilder<T> {
    /// Creates a new `RingConsumerBuilder` defaulting to `ConsumerStartMode::Latest`.
    pub fn new() -> Self {
        Self {
            start_mode: ConsumerStartMode::Latest,
            offset_file: None,
            consumer_name: None,
            _marker: PhantomData,
        }
    }

    /// Sets the initial start mode when no offset file is present.
    pub fn start_mode(mut self, mode: ConsumerStartMode) -> Self {
        self.start_mode = mode;
        self
    }

    /// Configure consumer to start from the latest message published in the buffer.
    pub fn start_from_latest(mut self) -> Self {
        self.start_mode = ConsumerStartMode::Latest;
        self
    }

    /// Configure consumer to start from the next message to be published (head).
    pub fn start_from_head(mut self) -> Self {
        self.start_mode = ConsumerStartMode::Head;
        self
    }

    /// Configure consumer to start from the oldest message available in the buffer.
    pub fn start_from_oldest(mut self) -> Self {
        self.start_mode = ConsumerStartMode::Oldest;
        self
    }

    /// Configure consumer to start from a specific sequence number.
    pub fn start_from_sequence(mut self, seq: u64) -> Self {
        self.start_mode = ConsumerStartMode::Sequence(seq);
        self
    }

    /// Sets an explicit shared memory path for persisting and loading consumer offsets.
    ///
    /// Stores the cursor in a 64-byte cache-aligned atomic struct directly in `/dev/shm`.
    pub fn offset_file<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.offset_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Alias for `offset_file` targeting shared memory paths.
    pub fn offset_shm<P: AsRef<Path>>(self, path: P) -> Self {
        self.offset_file(path)
    }

    /// Identifies this consumer with a human-readable name.
    ///
    /// If no explicit `offset_file` is specified, automatically derives a dedicated
    /// shared memory offset path: `/dev/shm/{ring_basename}_{consumer_name}.offset`.
    pub fn consumer_name(mut self, name: &str) -> Self {
        self.consumer_name = Some(name.to_string());
        self
    }

    /// Attaches to the shared memory ring buffer at `path` using the configured builder options.
    pub fn attach<P: AsRef<Path>>(self, path: P) -> Result<RingConsumer<T>> {
        RingConsumer::attach_with_options(path, self)
    }
}

/// Multi-consumer ring buffer reader backed by shared memory (`/dev/shm`).
pub struct RingConsumer<T: Copy + 'static> {
    header: *const RingHeader,
    slots: *const Slot<T>,
    capacity: u64,
    mask: u64,
    cursor: u64,
    lapped_total: u64,
    checkpoint: Option<OffsetCheckpoint>,
    registry: Option<ReaderRegistry>,
    registration: Option<ReaderRegistration>,
    _marker: PhantomData<T>,
    _mmap: MmapMut,
}

unsafe impl<T: Copy + Send + 'static> Send for RingConsumer<T> {}

impl<T: Copy + 'static> std::fmt::Debug for RingConsumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingConsumer")
            .field("capacity", &self.capacity)
            .field("cursor", &self.cursor)
            .field("lapped_total", &self.lapped_total)
            .field("checkpoint", &self.checkpoint)
            .finish()
    }
}

impl<T: Copy + LayoutSignature + 'static> RingConsumer<T> {
    /// Returns a new `RingConsumerBuilder` for configuring attach options.
    pub fn builder() -> RingConsumerBuilder<T> {
        RingConsumerBuilder::new()
    }

    /// Attaches to an existing ring buffer, starting from the oldest available message.
    ///
    /// Preserves standard replay behavior.
    pub fn attach<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::builder().start_from_oldest().attach(path)
    }

    /// Attaches to an existing ring buffer, starting from the latest published message.
    ///
    /// Skips historical messages to connect immediately to live market data.
    pub fn attach_latest<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::builder().start_from_latest().attach(path)
    }

    /// Attaches to an existing ring buffer, resuming from the specified offset file.
    ///
    /// If the offset file does not exist, starts from the latest published message.
    pub fn attach_from_offset<P: AsRef<Path>, O: AsRef<Path>>(
        path: P,
        offset_file: O,
    ) -> Result<Self> {
        Self::builder()
            .offset_file(offset_file)
            .start_from_latest()
            .attach(path)
    }

    /// Attaches to an existing shared memory ring buffer at `path` with custom builder options.
    pub fn attach_with_options<P: AsRef<Path>>(
        path: P,
        options: RingConsumerBuilder<T>,
    ) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let header_ptr = mmap.as_ptr().cast::<RingHeader>();
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

        let slot_size = std::mem::size_of::<Slot<T>>();
        if header.element_size as usize != slot_size {
            return Err(RingfireError::ElementSizeMismatch {
                expected: header.element_size as usize,
                actual: slot_size,
            });
        }

        let expected_sig = T::layout_signature();
        if header.schema_sig != 0 && header.schema_sig != expected_sig {
            return Err(RingfireError::SchemaMismatch {
                expected: header.schema_sig,
                actual: expected_sig,
                type_name: std::any::type_name::<T>(),
            });
        }

        let capacity = header.capacity;
        let mask = header.mask;
        let slots_offset = if header.reader_registry_offset != 0 {
            header.reader_registry_offset as usize
                + (header.reader_registry_count as usize * std::mem::size_of::<crate::header::ReaderSlot>())
        } else {
            std::mem::size_of::<RingHeader>()
        };
        let slots_ptr = unsafe {
            mmap.as_ptr()
                .add(slots_offset)
                .cast::<Slot<T>>()
        };

        let current_write = header.write_seq.load(Ordering::Acquire);
        let oldest = if current_write > capacity {
            current_write - capacity + 1
        } else {
            1
        };

        let (checkpoint, cursor, lapped_total) = if options.offset_file.is_some() || options.consumer_name.is_some() {
            let name = options.consumer_name.as_deref().unwrap_or("consumer");
            let cp = if let Some(ref offset_path) = options.offset_file {
                OffsetCheckpoint::open_or_create(offset_path, name).map_err(RingfireError::Io)?
            } else {
                OffsetCheckpoint::for_consumer(&path, name).map_err(RingfireError::Io)?
            };

            let (target_cur, lapped) = match cp.load() {
                Some(saved_seq) => {
                    let target = saved_seq + 1;
                    if target < oldest {
                        (oldest, oldest - target)
                    } else {
                        (target, 0)
                    }
                }
                None => match options.start_mode {
                    ConsumerStartMode::Latest => {
                        let cur = if current_write > 0 { current_write } else { 1 };
                        (cur, 0)
                    }
                    ConsumerStartMode::Head => (current_write + 1, 0),
                    ConsumerStartMode::Oldest => (oldest, 0),
                    ConsumerStartMode::Sequence(seq) => {
                        if seq < oldest {
                            (oldest, oldest - seq)
                        } else {
                            (seq, 0)
                        }
                    }
                },
            };
            (Some(cp), target_cur, lapped)
        } else {
            let (target_cur, lapped) = match options.start_mode {
                ConsumerStartMode::Latest => {
                    let cur = if current_write > 0 { current_write } else { 1 };
                    (cur, 0)
                }
                ConsumerStartMode::Head => (current_write + 1, 0),
                ConsumerStartMode::Oldest => (oldest, 0),
                ConsumerStartMode::Sequence(seq) => {
                    if seq < oldest {
                        (oldest, oldest - seq)
                    } else {
                        (seq, 0)
                    }
                }
            };
            (None, target_cur, lapped)
        };

        let (registry, registration) = if header.reader_registry_offset != 0 {
            let reg = unsafe {
                ReaderRegistry::from_ptr(
                    mmap.as_mut_ptr().add(header.reader_registry_offset as usize),
                    header.reader_registry_count as usize,
                )
            };
            let reg_name = options.consumer_name.as_deref().unwrap_or("consumer");
            let reg_handle = reg.register(reg_name, cursor).ok();
            (Some(reg), reg_handle)
        } else {
            (None, None)
        };

        Ok(Self {
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity,
            mask,
            cursor,
            lapped_total,
            checkpoint,
            registry,
            registration,
            _marker: PhantomData,
        })
    }

    /// Attempts to read the next message with explicit status (Ok, Empty, or Lapped).
    #[inline]
    pub fn recv_status(&mut self) -> RecvStatus<T> {
        unsafe {
            let mut skipped = 0;
            let current_write = (*self.header).write_seq.load(Ordering::Acquire);

            // If reader fell behind by more than buffer capacity, jump to oldest available
            if current_write > self.cursor && (current_write - self.cursor) >= self.capacity {
                let oldest = current_write - self.capacity + 1;
                skipped = oldest - self.cursor;
                self.lapped_total += skipped;
                self.cursor = oldest;
            }

            let idx = (self.cursor & self.mask) as usize;
            let slot = self.slots.add(idx);
            let s1 = (*slot).seq.load(Ordering::Acquire);

            if s1 < self.cursor {
                return RecvStatus::Empty;
            }

            if s1 > self.cursor {
                let slot_skipped = s1 - self.cursor;
                skipped += slot_skipped;
                self.lapped_total += slot_skipped;
                self.cursor = s1;
            }

            let data = std::ptr::read_volatile(&(*slot).data);
            let s2 = (*slot).seq.load(Ordering::Acquire);

            if s1 != s2 {
                // Writer updated the slot concurrently during read
                self.cursor = s2;
                return RecvStatus::Empty;
            }

            self.cursor += 1;
            if let Some(ref reg) = self.registration {
                reg.update_cursor(self.cursor);
            }
            if skipped > 0 {
                RecvStatus::Lapped { skipped, item: data }
            } else {
                RecvStatus::Ok(data)
            }
        }
    }

    /// Attempts to read the next available message without blocking.
    /// Returns `Some(T)` if a new message was published (even if lapped), or `None` if caught up.
    #[inline(always)]
    pub fn try_recv(&mut self) -> Option<T> {
        match self.recv_status() {
            RecvStatus::Ok(item) | RecvStatus::Lapped { item, .. } => Some(item),
            RecvStatus::Empty => None,
        }
    }

    /// Reads up to `buf.len()` available messages into the provided slice.
    /// Returns the number of messages read.
    #[inline]
    pub fn recv_batch(&mut self, buf: &mut [T]) -> usize {
        if buf.is_empty() {
            return 0;
        }

        let mut count = 0;
        while count < buf.len() {
            let idx = (self.cursor & self.mask) as usize;
            unsafe {
                let slot = self.slots.add(idx);
                let s1 = (*slot).seq.load(Ordering::Acquire);

                if s1 < self.cursor {
                    break;
                }

                if s1 > self.cursor {
                    let skipped = s1 - self.cursor;
                    self.lapped_total += skipped;
                    self.cursor = s1;
                }

                let data = std::ptr::read_volatile(&(*slot).data);
                let s2 = (*slot).seq.load(Ordering::Acquire);

                if s1 != s2 {
                    self.cursor = s2;
                    break;
                }

                buf[count] = data;
                self.cursor += 1;
                count += 1;
            }
        }

        if let Some(ref reg) = self.registration {
            reg.update_cursor(self.cursor);
        }

        count
    }

    /// Reads the next message, blocking according to the provided `WaitStrategy`.
    pub fn recv_blocking<W: WaitStrategy>(&mut self, wait: &mut W) -> T {
        loop {
            if let Some(item) = self.try_recv() {
                wait.reset();
                return item;
            }
            unsafe {
                wait.wait(&*self.header, self.cursor);
            }
        }
    }

    /// Jumps cursor directly to the latest published sequence, skipping any backlog.
    /// Returns the count of skipped messages.
    pub fn jump_to_latest(&mut self) -> u64 {
        let write_seq = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        if write_seq > self.cursor {
            let skipped = write_seq - self.cursor;
            self.lapped_total += skipped;
            self.cursor = write_seq;
            if let Some(ref reg) = self.registration {
                reg.update_cursor(self.cursor);
            }
            skipped
        } else {
            0
        }
    }

    /// Jumps cursor to the oldest message still surviving in the buffer.
    /// Returns the count of skipped messages.
    pub fn jump_to_oldest(&mut self) -> u64 {
        let write_seq = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        let oldest = if write_seq > self.capacity {
            write_seq - self.capacity + 1
        } else {
            1
        };
        if oldest > self.cursor {
            let skipped = oldest - self.cursor;
            self.lapped_total += skipped;
            self.cursor = oldest;
            if let Some(ref reg) = self.registration {
                reg.update_cursor(self.cursor);
            }
            skipped
        } else {
            0
        }
    }

    /// Returns the total count of messages skipped due to writer lapping.
    #[inline]
    pub fn lapped_count(&self) -> u64 {
        self.lapped_total
    }

    /// Current reader cursor sequence number.
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Ring buffer capacity.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Number of published messages currently pending for this consumer.
    #[inline]
    pub fn lag(&self) -> u64 {
        let write_seq = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        if write_seq >= self.cursor {
            write_seq - self.cursor + 1
        } else {
            0
        }
    }

    /// Seeks reader cursor to a specific sequence number.
    ///
    /// If `target_seq` is older than the oldest surviving message in the ring,
    /// cursor automatically advances to the oldest available sequence and returns
    /// the number of skipped messages.
    pub fn seek(&mut self, target_seq: u64) -> u64 {
        let current_write = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        let oldest = if current_write > self.capacity {
            current_write - self.capacity + 1
        } else {
            1
        };
        let skipped = if target_seq < oldest {
            let s = oldest - target_seq;
            self.lapped_total += s;
            self.cursor = oldest;
            s
        } else {
            self.cursor = target_seq;
            0
        };

        if let Some(ref reg) = self.registration {
            reg.update_cursor(self.cursor);
        }

        skipped
    }

    /// Reference to the attached shared memory offset checkpoint, if configured.
    #[inline]
    pub fn checkpoint(&self) -> Option<&OffsetCheckpoint> {
        self.checkpoint.as_ref()
    }

    /// Path to the shared memory offset file, if configured.
    #[inline]
    pub fn offset_path(&self) -> Option<&Path> {
        self.checkpoint.as_ref().map(|cp| cp.path())
    }

    /// Reference to the ReaderRegistry, if enabled in the ring header.
    #[inline]
    pub fn registry(&self) -> Option<&ReaderRegistry> {
        self.registry.as_ref()
    }

    /// Reference to this consumer's ReaderRegistration, if registered.
    #[inline]
    pub fn registration(&self) -> Option<&ReaderRegistration> {
        self.registration.as_ref()
    }

    /// Atomically persists the last processed sequence number (`cursor - 1`) directly into shared memory (<10ns).
    #[inline(always)]
    pub fn commit_offset(&self) -> Result<()> {
        if let Some(ref cp) = self.checkpoint {
            cp.save(self.last_processed_sequence());
            if let Some(ref reg) = self.registration {
                reg.update_cursor(self.cursor);
            }
            Ok(())
        } else {
            Err(RingfireError::NoOffsetFileConfigured)
        }
    }

    /// Atomically persists the last processed sequence number (`cursor - 1`) to a specified shared memory path.
    pub fn commit_offset_to<P: AsRef<Path>>(&self, path: P, consumer_name: &str) -> Result<()> {
        let cp = OffsetCheckpoint::open_or_create(path, consumer_name).map_err(RingfireError::Io)?;
        cp.save(self.last_processed_sequence());
        if let Some(ref reg) = self.registration {
            reg.update_cursor(self.cursor);
        }
        Ok(())
    }

    /// Sequence number of the last successfully processed message (`cursor - 1`).
    #[inline]
    pub fn last_processed_sequence(&self) -> u64 {
        self.cursor.saturating_sub(1)
    }
}
