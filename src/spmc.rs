use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{fence, Ordering};
use memmap2::MmapMut;

use crate::checkpoint::OffsetCheckpoint;
use crate::error::{Result, RingfireError};
use crate::header::{
    check_schema, validate_ring, RingHeader, RingLayout, Slot, FLAG_MODE_MPMC, FLAG_MODE_SPMC,
    FLAG_POLICY_LATEST_WINS, FLAG_POLICY_LOSSLESS_BACKPRESSURE, FLAG_SPARSE, FLAG_WITH_REGISTRY,
    SLOT_WRITING,
};
use crate::registry::{ReaderRegistration, ReaderRegistry, DEFAULT_MAX_READERS};
use crate::shm::create_backing_file;
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
    /// Lossless mode: `seq` may be published without consulting the registry while
    /// `seq < gate_limit`. Derived from the slowest reader's cursor at the last scan.
    gate_limit: u64,
    blocked_polls: u32,
    cleanup_mode: CleanupMode,
    flow_control: FlowControl,
    registry: Option<ReaderRegistry>,
    _marker: PhantomData<T>,
    _mmap: MmapMut,
}

unsafe impl<T: Copy + Send + 'static> Send for RingProducer<T> {}

/// Writes `item` into `slot` as message `seq` using the v2 slot protocol: mark the slot
/// [`SLOT_WRITING`], copy the payload, then publish `seq`.
///
/// # Safety
/// `slot` must be valid for writes and not concurrently written by another producer.
#[inline(always)]
pub(crate) unsafe fn write_slot<T>(slot: *mut Slot<T>, seq: u64, item: &T) {
    unsafe {
        (*slot).seq.store(SLOT_WRITING, Ordering::Relaxed);
        fence(Ordering::Release);
        std::ptr::copy_nonoverlapping(item, &mut (*slot).data, 1);
        (*slot).seq.store(seq, Ordering::Release);
    }
}

/// Outcome of reading one slot.
pub(crate) enum SlotRead<T> {
    /// The slot held message `want` and was copied without interference.
    Item(T),
    /// Message `want` is not published yet (or the slot is mid-write).
    Pending,
    /// Message `want` was overwritten; carries the newer sequence observed (0 if unknown).
    Overwritten(u64),
}

/// Copies a payload the writer may be overwriting concurrently.
///
/// The copy is only trusted after the caller re-checks the slot sequence behind an acquire
/// fence. A plain `memcpy` (vector loads) is used on purpose: `read_volatile` on an
/// aggregate lowers to byte-by-byte loads, which doubled `try_recv` latency for 64-byte T.
///
/// # Safety
/// `src` must be valid for reads of `T`.
#[inline(always)]
pub(crate) unsafe fn racy_copy<T: Copy>(src: *const T) -> T {
    let mut out = std::mem::MaybeUninit::<T>::uninit();
    unsafe {
        std::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), 1);
        out.assume_init()
    }
}

/// Reads message `want` from `slot`, rejecting torn copies.
///
/// # Safety
/// `slot` must be valid for reads.
#[inline(always)]
pub(crate) unsafe fn read_slot<T: Copy>(slot: *const Slot<T>, want: u64) -> SlotRead<T> {
    unsafe {
        let s1 = (*slot).seq.load(Ordering::Acquire);
        if s1 == want {
            let data = racy_copy(&(*slot).data);
            fence(Ordering::Acquire);
            let s2 = (*slot).seq.load(Ordering::Relaxed);
            if s2 == want {
                SlotRead::Item(data)
            } else {
                SlotRead::Overwritten(if s2 == SLOT_WRITING { 0 } else { s2 })
            }
        } else if s1 == SLOT_WRITING || s1 < want {
            SlotRead::Pending
        } else {
            SlotRead::Overwritten(s1)
        }
    }
}

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

        let mut max_readers = options.max_readers;
        if options.flow_control == FlowControl::LosslessBackpressure && max_readers == 0 {
            max_readers = DEFAULT_MAX_READERS;
        }
        let header_size = std::mem::size_of::<RingHeader>();
        let registry_bytes = max_readers * std::mem::size_of::<crate::header::ReaderSlot>();
        let slot_size = std::mem::size_of::<Slot<T>>();
        let slot_align = std::mem::align_of::<Slot<T>>().max(64);
        let slots_offset = (header_size + registry_bytes).next_multiple_of(slot_align);
        let total_size = slots_offset + (capacity as usize * slot_size);

        let path_buf = path.as_ref().to_path_buf();
        let file = create_backing_file(&path_buf, options.mode, options.exclusive_lock, total_size as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let flags = (match options.flow_control {
            FlowControl::LossyLatestWins => FLAG_POLICY_LATEST_WINS,
            FlowControl::LosslessBackpressure => FLAG_POLICY_LOSSLESS_BACKPRESSURE,
        }) | FLAG_MODE_SPMC
            | (if max_readers > 0 { FLAG_WITH_REGISTRY } else { 0 });

        let base = mmap.as_mut_ptr();
        let header_ptr = base.cast::<RingHeader>();
        unsafe {
            RingHeader::initialize(
                header_ptr,
                &RingLayout {
                    capacity,
                    slot_size,
                    flags,
                    schema_sig: T::layout_signature(),
                    claim_seq: 0,
                    read_seq: 0,
                    registry_offset: if max_readers > 0 { header_size } else { 0 },
                    registry_count: max_readers,
                    slots_offset,
                    arena_offset: 0,
                    arena_size: 0,
                },
            );
        }

        let registry = if max_readers > 0 {
            Some(unsafe { ReaderRegistry::init(base.add(header_size), max_readers) })
        } else {
            None
        };

        let slots_ptr = unsafe { base.add(slots_offset).cast::<Slot<T>>() };
        for i in 0..capacity {
            unsafe {
                (*slots_ptr.add(i as usize)).seq = std::sync::atomic::AtomicU64::new(0);
            }
        }
        unsafe { RingHeader::publish(header_ptr) };

        Ok(Self {
            path: path_buf,
            file: Some(file),
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity,
            mask: capacity - 1,
            seq: 1,
            gate_limit: 0,
            blocked_polls: 0,
            cleanup_mode: options.cleanup_mode,
            flow_control: options.flow_control,
            registry,
            _marker: PhantomData,
        })
    }

    #[inline(always)]
    fn write_one(&mut self, item: &T) {
        let idx = (self.seq & self.mask) as usize;
        unsafe { write_slot(self.slots.add(idx), self.seq, item) };
        self.seq += 1;
    }

    #[inline(always)]
    fn publish_progress(&self) {
        unsafe {
            (*self.header).write_seq.store(self.seq - 1, Ordering::Release);
            wake_futex(&*self.header, i32::MAX);
        }
    }

    /// Publishes a message into the ring buffer.
    /// Under `FlowControl::LosslessBackpressure`, waits if writing would overwrite the slowest active registered reader.
    /// Uses release ordering so consumers see the complete payload.
    #[inline(always)]
    pub fn push(&mut self, item: &T) {
        if self.flow_control == FlowControl::LosslessBackpressure && self.seq >= self.gate_limit {
            self.wait_for_headroom();
        }
        self.write_one(item);
        self.publish_progress();
    }

    /// Attempts to publish a message into the ring buffer without blocking.
    /// Under `FlowControl::LosslessBackpressure`, returns `Err(RingfireError::BackpressureBufferFull)`
    /// if the buffer is full and would overwrite the slowest reader.
    #[inline]
    pub fn try_push(&mut self, item: &T) -> Result<()> {
        if self.flow_control == FlowControl::LosslessBackpressure
            && self.seq >= self.gate_limit
            && !self.refresh_gate()
        {
            self.blocked_polls = self.blocked_polls.wrapping_add(1);
            if self.blocked_polls.is_multiple_of(1024)
                && let Some(reg) = self.registry.as_ref()
                && reg.prune_dead_readers() > 0
                && self.refresh_gate()
            {
                self.push(item);
                return Ok(());
            }
            return Err(RingfireError::BackpressureBufferFull);
        }
        self.push(item);
        Ok(())
    }

    /// Rescans the registry and recomputes `gate_limit`. Returns whether `self.seq` may be
    /// published now.
    ///
    /// The limit is also capped at half a ring ahead so readers registering later are
    /// picked up within `capacity / 2` messages.
    #[cold]
    #[inline(never)]
    fn refresh_gate(&mut self) -> bool {
        let rescan_window = (self.capacity / 2).max(1);
        let min = self.registry.as_ref().and_then(|reg| reg.min_cursor());
        let limit = match min {
            Some(min_cursor) => (min_cursor + self.capacity).min(self.seq + rescan_window),
            None => self.seq + rescan_window,
        };
        self.gate_limit = limit;
        self.seq < limit
    }

    /// Blocks until there is room to write without overwriting the slowest active registered reader.
    #[cold]
    #[inline(never)]
    pub fn wait_for_headroom(&mut self) {
        if self.registry.is_none() || self.refresh_gate() {
            return;
        }
        // Readers sleeping on the futex must see everything published so far, or a
        // producer blocked mid-batch and a sleeping reader would wait on each other.
        self.publish_progress();
        let mut spins = 0u32;
        loop {
            spins = spins.saturating_add(1);
            if spins < 100 {
                core::hint::spin_loop();
            } else if spins < 1000 {
                std::thread::yield_now();
            } else {
                if let Some(reg) = self.registry.as_ref() {
                    reg.prune_dead_readers();
                }
                std::thread::sleep(std::time::Duration::from_micros(10));
            }
            if self.refresh_gate() {
                return;
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
            reg.headroom(self.seq - 1, self.capacity)
        } else {
            self.capacity
        }
    }

    /// Unread message count of the slowest active reader.
    #[inline]
    pub fn reader_lag(&self) -> u64 {
        if let Some(ref reg) = self.registry {
            reg.reader_lag(self.seq - 1)
        } else {
            0
        }
    }

    /// Publishes a batch of messages consecutively into the ring buffer.
    /// Amortizes the header write sequence update and futex wake.
    /// Honors `FlowControl::LosslessBackpressure` per message.
    #[inline]
    pub fn push_batch(&mut self, items: &[T]) {
        if items.is_empty() {
            return;
        }
        let lossless = self.flow_control == FlowControl::LosslessBackpressure;
        for item in items {
            if lossless && self.seq >= self.gate_limit {
                self.wait_for_headroom();
            }
            self.write_one(item);
        }
        self.publish_progress();
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
        // Unlink while still holding the lock so no new producer can adopt the path first.
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
    /// Ring written by multiple producers: a slot can stay unpublished (dead producer).
    multi_producer: bool,
    /// Sequence numbers may have holes (`FLAG_SPARSE`: network mirrors). A stale slot
    /// behind `write_seq` is then skipped instead of waited for.
    sparse: bool,
    empty_polls: u32,
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

        let view = unsafe {
            validate_ring(
                mmap.as_ptr(),
                mmap.len(),
                Some(std::mem::size_of::<Slot<T>>()),
                std::mem::align_of::<Slot<T>>(),
            )?
        };
        let header_ptr = mmap.as_ptr().cast::<RingHeader>();
        let header = unsafe { &*header_ptr };
        check_schema(header, T::layout_signature(), std::any::type_name::<T>())?;

        let capacity = view.capacity;
        let slots_ptr = unsafe { mmap.as_ptr().add(view.slots_offset).cast::<Slot<T>>() };

        let current_write = header.write_seq.load(Ordering::Acquire);
        let oldest = oldest_retained(current_write, capacity);
        let resolve = |mode: &ConsumerStartMode| match *mode {
            ConsumerStartMode::Latest => (current_write.max(1), 0),
            ConsumerStartMode::Head => (current_write + 1, 0),
            ConsumerStartMode::Oldest => (oldest, 0),
            ConsumerStartMode::Sequence(seq) => {
                let seq = seq.max(1);
                if seq < oldest { (oldest, oldest - seq) } else { (seq, 0) }
            }
        };

        let (checkpoint, cursor, lapped_total) =
            if options.offset_file.is_some() || options.consumer_name.is_some() {
                let name = options.consumer_name.as_deref().unwrap_or("consumer");
                let cp = if let Some(ref offset_path) = options.offset_file {
                    OffsetCheckpoint::open_or_create(offset_path, name).map_err(RingfireError::Io)?
                } else {
                    OffsetCheckpoint::for_consumer(&path, name).map_err(RingfireError::Io)?
                };

                let (target_cur, lapped) = match cp.load() {
                    Some(saved_seq) => {
                        let target = saved_seq + 1;
                        if target < oldest { (oldest, oldest - target) } else { (target, 0) }
                    }
                    None => resolve(&options.start_mode),
                };
                (Some(cp), target_cur, lapped)
            } else {
                let (target_cur, lapped) = resolve(&options.start_mode);
                (None, target_cur, lapped)
            };

        let lossless = header.flags & FLAG_POLICY_LOSSLESS_BACKPRESSURE != 0;
        let (registry, registration) = if header.reader_registry_offset != 0 {
            let reg = unsafe {
                ReaderRegistry::from_ptr(
                    mmap.as_mut_ptr().add(header.reader_registry_offset as usize),
                    header.reader_registry_count as usize,
                )
            };
            let reg_name = options.consumer_name.as_deref().unwrap_or("consumer");
            let reg_handle = match reg.register(reg_name, cursor) {
                Ok(handle) => Some(handle),
                // A lossless ring cannot protect an unregistered reader: refuse instead of
                // silently degrading to lossy delivery.
                Err(e) if lossless => return Err(e),
                Err(_) => None,
            };
            (Some(reg), reg_handle)
        } else {
            (None, None)
        };

        let mut consumer = Self {
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity,
            mask: view.mask,
            cursor,
            lapped_total,
            multi_producer: header.flags & FLAG_MODE_MPMC != 0,
            sparse: header.flags & FLAG_SPARSE != 0,
            empty_polls: 0,
            checkpoint,
            registry,
            registration,
            _marker: PhantomData,
        };
        // The producer may have overwritten the start position between computing it and
        // registering: from here on the registration protects us, so re-check once.
        if consumer.registration.is_some() {
            consumer.skip_overwritten(0);
        }
        Ok(consumer)
    }

    /// Advances the cursor past messages that can no longer be read because the writer
    /// lapped this reader. `seen` is a newer sequence observed in the cursor's slot.
    /// Returns the number of skipped messages.
    #[cold]
    #[inline(never)]
    fn skip_overwritten(&mut self, seen: u64) -> u64 {
        let write_seq = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        let oldest = oldest_retained(write_seq.max(seen), self.capacity);
        if oldest > self.cursor {
            let skipped = oldest - self.cursor;
            self.cursor = oldest;
            self.lapped_total += skipped;
            if let Some(ref reg) = self.registration {
                reg.update_cursor(self.cursor);
            }
            skipped
        } else {
            0
        }
    }

    /// Sparse rings only (`FLAG_SPARSE`): if the writer has published past the cursor while
    /// the cursor's slot still holds an older lap, message `cursor` was never written into
    /// this ring (a mirror joined mid-stream or resynchronized after a gap). Moves the
    /// cursor to the next message present in the retained window, or just past `write_seq`
    /// when none is. Returns the number of sequences skipped.
    #[cold]
    #[inline(never)]
    fn skip_hole(&mut self) -> u64 {
        let write_seq = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        if write_seq < self.cursor {
            return 0;
        }
        // The message may have landed between the slot load and the header load.
        let seen = unsafe { (*self.slots.add((self.cursor & self.mask) as usize)).seq.load(Ordering::Acquire) };
        if seen == SLOT_WRITING || seen >= self.cursor {
            return 0;
        }
        let mut next = oldest_retained(write_seq, self.capacity).max(self.cursor + 1);
        while next <= write_seq {
            let s = unsafe { (*self.slots.add((next & self.mask) as usize)).seq.load(Ordering::Acquire) };
            if s == next {
                break;
            }
            next += 1;
        }
        let skipped = next - self.cursor;
        self.cursor = next;
        self.lapped_total += skipped;
        if let Some(ref reg) = self.registration {
            reg.update_cursor(self.cursor);
        }
        skipped
    }

    /// Reads the next message, skipping over lapped ones. Returns the item (if any) and
    /// the number of messages skipped on the way.
    #[inline(always)]
    fn next_item(&mut self) -> (Option<T>, u64) {
        let mut skipped = 0;
        loop {
            let slot = unsafe { self.slots.add((self.cursor & self.mask) as usize) };
            match unsafe { read_slot(slot, self.cursor) } {
                SlotRead::Item(data) => {
                    self.cursor += 1;
                    return (Some(data), skipped);
                }
                SlotRead::Overwritten(seen) => {
                    let s = self.skip_overwritten(seen);
                    if s == 0 {
                        return (None, skipped);
                    }
                    skipped += s;
                }
                SlotRead::Pending => {
                    // Normally just "caught up". With multiple producers a slot can stay
                    // unpublished (producer died mid-publish): once the writer is a full
                    // ring ahead, the message is gone either way.
                    if !self.multi_producer {
                        if self.sparse {
                            self.empty_polls = self.empty_polls.wrapping_add(1);
                            if self.empty_polls & 63 == 0 {
                                let s = self.skip_hole();
                                if s > 0 {
                                    skipped += s;
                                    continue;
                                }
                            }
                        }
                        return (None, skipped);
                    }
                    let write_seq = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
                    if write_seq >= self.cursor + self.capacity {
                        let s = self.skip_overwritten(0);
                        if s > 0 {
                            skipped += s;
                            continue;
                        }
                    }
                    return (None, skipped);
                }
            }
        }
    }

    /// Attempts to read the next message with explicit status (Ok, Empty, or Lapped).
    #[inline]
    pub fn recv_status(&mut self) -> RecvStatus<T> {
        let (item, skipped) = self.next_item();
        match item {
            Some(data) => {
                if let Some(ref reg) = self.registration {
                    reg.update_cursor(self.cursor);
                }
                if skipped > 0 {
                    RecvStatus::Lapped { skipped, item: data }
                } else {
                    RecvStatus::Ok(data)
                }
            }
            None => RecvStatus::Empty,
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
        let mut count = 0;
        while count < buf.len() {
            match self.next_item().0 {
                Some(data) => {
                    buf[count] = data;
                    count += 1;
                }
                None => break,
            }
        }
        if count > 0
            && let Some(ref reg) = self.registration
        {
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
        let oldest = oldest_retained(write_seq, self.capacity);
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
        let oldest = oldest_retained(current_write, self.capacity);
        let target_seq = target_seq.max(1);
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

/// Oldest sequence still retained in a ring of `capacity` slots whose newest message is `write_seq`.
#[inline]
pub(crate) fn oldest_retained(write_seq: u64, capacity: u64) -> u64 {
    if write_seq >= capacity { write_seq - capacity + 1 } else { 1 }
}
