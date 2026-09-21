use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use memmap2::MmapMut;

use crate::error::{Result, RingfireError};
use crate::header::{RingHeader, Slot, FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS, RINGFIRE_MAGIC, RINGFIRE_VERSION};
use crate::wait::{wake_futex, WaitStrategy};

/// Cleanup behavior for the shared memory file when the producer is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupMode {
    /// Automatically remove the file from `/dev/shm` on drop.
    UnlinkOnDrop,
    /// Keep the file on disk/shm across process termination.
    Persistent,
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
    mode: u32,
    exclusive_lock: bool,
}

impl RingProducerBuilder {
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            cleanup_mode: CleanupMode::UnlinkOnDrop,
            mode: 0o660,
            exclusive_lock: true,
        }
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

    pub fn build<T: Copy, P: AsRef<Path>>(self, path: P) -> Result<RingProducer<T>> {
        RingProducer::create_with_options(path, self)
    }
}

/// Single-producer ring buffer writer backed by shared memory (`/dev/shm`).
pub struct RingProducer<T: Copy> {
    path: PathBuf,
    file: Option<File>,
    _mmap: MmapMut,
    header: *mut RingHeader,
    slots: *mut Slot<T>,
    capacity: u64,
    mask: u64,
    seq: u64,
    cleanup_mode: CleanupMode,
    _marker: PhantomData<T>,
}

unsafe impl<T: Copy + Send> Send for RingProducer<T> {}

impl<T: Copy> RingProducer<T> {
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

        let slot_size = std::mem::size_of::<Slot<T>>();
        let total_size = std::mem::size_of::<RingHeader>() + (capacity as usize * slot_size);

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
                flags: FLAG_POLICY_LATEST_WINS | FLAG_MODE_SPMC,
                futex_word: std::sync::atomic::AtomicU32::new(0),
                waiting_consumers: std::sync::atomic::AtomicU32::new(0),
                _align_pad: 0,
                read_seq: std::sync::atomic::AtomicU64::new(0),
                _reserved: 0,
                _pad: [0; 48],
            });
        }

        let slots_ptr = unsafe {
            mmap.as_mut_ptr()
                .add(std::mem::size_of::<RingHeader>())
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
            _marker: PhantomData,
        })
    }

    /// Publishes a message into the ring buffer.
    /// Uses release ordering so consumers see the complete payload.
    #[inline(always)]
    pub fn push(&mut self, item: &T) {
        let idx = (self.seq & self.mask) as usize;
        unsafe {
            let slot = self.slots.add(idx);
            std::ptr::copy_nonoverlapping(item, &mut (*slot).data, 1);
            (*slot).seq.store(self.seq, Ordering::Release);
            (*self.header).write_seq.store(self.seq, Ordering::Release);
            wake_futex(&*self.header, 1);
        }
        self.seq += 1;
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

impl<T: Copy> Drop for RingProducer<T> {
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

/// Multi-consumer ring buffer reader backed by shared memory (`/dev/shm`).
pub struct RingConsumer<T: Copy> {
    _mmap: MmapMut,
    header: *const RingHeader,
    slots: *const Slot<T>,
    capacity: u64,
    mask: u64,
    cursor: u64,
    lapped_total: u64,
    _marker: PhantomData<T>,
}

unsafe impl<T: Copy + Send> Send for RingConsumer<T> {}

impl<T: Copy> RingConsumer<T> {
    /// Attaches to an existing shared memory ring buffer at `path`.
    pub fn attach<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

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

        let capacity = header.capacity;
        let mask = header.mask;
        let slots_ptr = unsafe {
            mmap.as_ptr()
                .add(std::mem::size_of::<RingHeader>())
                .cast::<Slot<T>>()
        };

        // Start reading from the oldest available message in the buffer
        let current_write = header.write_seq.load(Ordering::Acquire);
        let cursor = if current_write > capacity {
            current_write - capacity + 1
        } else {
            1
        };

        Ok(Self {
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity,
            mask,
            cursor,
            lapped_total: 0,
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
}
