//! Multi-producer ring buffer and competing-consumer work queue.
//!
//! Delivery is lossy (latest-wins): producers never block on consumers. When producers
//! run a full ring ahead of the queue consumers, the oldest unconsumed items are dropped
//! and counted by [`MpmcQueueConsumer::dropped_count`]. Each item that *is* delivered is
//! delivered to exactly one queue consumer.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{fence, Ordering};
use std::time::{Duration, Instant};
use memmap2::MmapMut;

use crate::error::Result;
use crate::header::{
    check_schema, validate_ring, RingHeader, RingLayout, Slot, FLAG_MODE_MPMC,
    FLAG_POLICY_LATEST_WINS, SLOT_WRITING,
};
use crate::shm::create_backing_file;
use crate::signature::LayoutSignature;
use crate::spmc::{racy_copy, CleanupMode};
use crate::wait::{wake_futex, WaitStrategy};

/// Spins before a producer waiting on its slot starts checking the clock.
const SLOT_SPINS_BEFORE_CLOCK: u32 = 1 << 12;
/// How long a producer waits for the previous lap of its slot before taking it over
/// (older lap) or dropping its own message (slot mid-write).
const SLOT_STALL_DEADLINE: Duration = Duration::from_millis(10);

/// Multi-producer writer for shared memory ring buffer.
/// Allows multiple concurrent processes to safely publish messages into the same ring buffer.
pub struct MpmcProducer<T: Copy + 'static> {
    path: PathBuf,
    _file: File,
    _mmap: MmapMut,
    header: *mut RingHeader,
    slots: *mut Slot<T>,
    capacity: u64,
    mask: u64,
    cleanup_mode: CleanupMode,
    _marker: PhantomData<T>,
}

unsafe impl<T: Copy + Send + 'static> Send for MpmcProducer<T> {}
unsafe impl<T: Copy + Sync + 'static> Sync for MpmcProducer<T> {}

impl<T: Copy + 'static> MpmcProducer<T> {
    /// Creates a new MPMC shared memory ring buffer at `path`.
    ///
    /// The creating producer holds an exclusive `flock` on the file; further producers
    /// join with [`MpmcProducer::attach`].
    pub fn create<P: AsRef<Path>>(path: P, capacity: u64) -> Result<Self> {
        if !capacity.is_power_of_two() {
            return Err(crate::error::RingfireError::InvalidCapacity(capacity));
        }

        let header_size = std::mem::size_of::<RingHeader>();
        let slot_size = std::mem::size_of::<Slot<T>>();
        let slots_offset = header_size.next_multiple_of(std::mem::align_of::<Slot<T>>());
        let total_size = slots_offset + (capacity as usize * slot_size);
        let path_buf = path.as_ref().to_path_buf();

        let file = create_backing_file(&path_buf, 0o660, true, total_size as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let header_ptr = mmap.as_mut_ptr().cast::<RingHeader>();
        unsafe {
            RingHeader::initialize(
                header_ptr,
                &RingLayout {
                    capacity,
                    slot_size,
                    flags: FLAG_POLICY_LATEST_WINS | FLAG_MODE_MPMC,
                    schema_sig: T::layout_signature(),
                    claim_seq: 1,
                    read_seq: 1,
                    registry_offset: 0,
                    registry_count: 0,
                    slots_offset,
                    arena_offset: 0,
                    arena_size: 0,
                },
            );
        }

        let slots_ptr = unsafe { mmap.as_mut_ptr().add(slots_offset).cast::<Slot<T>>() };
        for i in 0..capacity {
            unsafe {
                (*slots_ptr.add(i as usize)).seq = std::sync::atomic::AtomicU64::new(0);
            }
        }
        unsafe { RingHeader::publish(header_ptr) };

        Ok(Self {
            path: path_buf,
            _file: file,
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity,
            mask: capacity - 1,
            cleanup_mode: CleanupMode::UnlinkOnDrop,
            _marker: PhantomData,
        })
    }

    /// Attaches an additional producer to an existing MPMC shared memory ring buffer.
    pub fn attach<P: AsRef<Path>>(path: P) -> Result<Self> {
        crate::wait::register_producer_barrier();
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(&path_buf)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let view = unsafe {
            validate_ring(
                mmap.as_ptr(),
                mmap.len(),
                Some(std::mem::size_of::<Slot<T>>()),
                std::mem::align_of::<Slot<T>>(),
            )?
        };
        let header_ptr = mmap.as_mut_ptr().cast::<RingHeader>();
        check_schema(unsafe { &*header_ptr }, T::layout_signature(), std::any::type_name::<T>())?;
        let slots_ptr = unsafe { mmap.as_mut_ptr().add(view.slots_offset).cast::<Slot<T>>() };

        Ok(Self {
            path: path_buf,
            _file: file,
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity: view.capacity,
            mask: view.mask,
            cleanup_mode: CleanupMode::Persistent,
            _marker: PhantomData,
        })
    }

    /// Sets the cleanup mode when this producer is dropped.
    pub fn set_cleanup_mode(&mut self, mode: CleanupMode) {
        self.cleanup_mode = mode;
    }

    /// Publishes a message into the ring buffer using atomic ticket claiming.
    /// Returns the sequence number assigned to the message.
    ///
    /// Producers that land on the same slot one lap apart are serialized through the
    /// slot's sequence word, so two writers never interleave their payloads. A producer
    /// that finds a newer lap already in its slot drops its (stale) message, as does one
    /// whose slot stays mid-write by another producer for more than 10 ms.
    #[inline(always)]
    pub fn push(&self, item: &T) -> u64 {
        unsafe {
            let seq = (*self.header).claim_seq.fetch_add(1, Ordering::Relaxed);
            let slot = self.slots.add((seq & self.mask) as usize);
            let prev_lap = seq.saturating_sub(self.capacity);

            // Wait for the previous lap to be published, then own the slot via CAS: only
            // the CAS winner writes, so payloads never interleave. A slot left at an older
            // lap past the deadline (its producer died between claim and publish) is taken
            // over by CAS as well. A slot that stays `SLOT_WRITING` is never taken over:
            // its writer may only be descheduled, and two writers would tear the payload.
            // After the deadline this message is dropped instead; readers skip the hole.
            let mut spins = 0u32;
            let mut deadline: Option<Instant> = None;
            let mut stalled = false;
            loop {
                let cur = (*slot).seq.load(Ordering::Acquire);
                if cur != SLOT_WRITING && cur > seq {
                    // A later lap already owns the slot: this message is obsolete.
                    return seq;
                }
                if (cur == prev_lap || (stalled && cur != SLOT_WRITING))
                    && (*slot)
                        .seq
                        .compare_exchange(cur, SLOT_WRITING, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                {
                    break;
                }
                spins = spins.wrapping_add(1);
                if spins >= SLOT_SPINS_BEFORE_CLOCK && spins.is_multiple_of(64) {
                    let limit = *deadline.get_or_insert_with(|| Instant::now() + SLOT_STALL_DEADLINE);
                    if Instant::now() >= limit {
                        if cur == SLOT_WRITING {
                            return seq;
                        }
                        stalled = true;
                    }
                    std::thread::yield_now();
                } else {
                    core::hint::spin_loop();
                }
            }

            fence(Ordering::Release);
            std::ptr::copy_nonoverlapping(item, &mut (*slot).data, 1);
            (*slot).seq.store(seq, Ordering::Release);
            (*self.header).write_seq.fetch_max(seq, Ordering::Release);
            wake_futex(&*self.header, i32::MAX);
            seq
        }
    }

    /// Returns ring buffer capacity.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

impl<T: Copy> Drop for MpmcProducer<T> {
    fn drop(&mut self) {
        if self.cleanup_mode == CleanupMode::UnlinkOnDrop {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Multi-consumer worker queue reader: multiple consumers compete for items,
/// each delivered item is consumed by exactly one consumer.
pub struct MpmcQueueConsumer<T: Copy + 'static> {
    _mmap: MmapMut,
    header: *const RingHeader,
    slots: *const Slot<T>,
    capacity: u64,
    mask: u64,
    /// Ticket claimed from `read_seq` whose message was not published yet.
    pending: Option<u64>,
    dropped: u64,
    _marker: PhantomData<T>,
}

unsafe impl<T: Copy + Send + 'static> Send for MpmcQueueConsumer<T> {}

enum Ticket<T> {
    Ready(T),
    NotYet,
    Lost,
}

impl<T: Copy + 'static> MpmcQueueConsumer<T> {
    /// Attaches to an MPMC ring buffer as a competing queue consumer.
    pub fn attach<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let view = unsafe {
            validate_ring(
                mmap.as_ptr(),
                mmap.len(),
                Some(std::mem::size_of::<Slot<T>>()),
                std::mem::align_of::<Slot<T>>(),
            )?
        };
        let header_ptr = mmap.as_ptr().cast::<RingHeader>();
        check_schema(unsafe { &*header_ptr }, T::layout_signature(), std::any::type_name::<T>())?;
        let slots_ptr = unsafe { mmap.as_ptr().add(view.slots_offset).cast::<Slot<T>>() };

        Ok(Self {
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity: view.capacity,
            mask: view.mask,
            pending: None,
            dropped: 0,
            _marker: PhantomData,
        })
    }

    /// Items this consumer found overwritten before it could read them.
    #[inline]
    pub fn dropped_count(&self) -> u64 {
        self.dropped
    }

    #[inline]
    fn read_ticket(&self, ticket: u64) -> Ticket<T> {
        unsafe {
            let slot = self.slots.add((ticket & self.mask) as usize);
            let s1 = (*slot).seq.load(Ordering::Acquire);
            if s1 == ticket {
                let data = racy_copy(&(*slot).data);
                fence(Ordering::Acquire);
                if (*slot).seq.load(Ordering::Relaxed) == ticket {
                    Ticket::Ready(data)
                } else {
                    Ticket::Lost
                }
            } else if s1 != SLOT_WRITING && s1 > ticket {
                Ticket::Lost
            } else if (*self.header).claim_seq.load(Ordering::Acquire) > ticket + self.capacity {
                // Producers are a full lap past this ticket: its slot was (or is being)
                // reused, or its producer died before publishing.
                Ticket::Lost
            } else {
                Ticket::NotYet
            }
        }
    }

    /// Attempts to dequeue the next item. Never blocks: returns `None` when the queue is
    /// empty or the claimed item is still being published.
    pub fn try_recv(&mut self) -> Option<T> {
        loop {
            if let Some(ticket) = self.pending {
                match self.read_ticket(ticket) {
                    Ticket::Ready(item) => {
                        self.pending = None;
                        return Some(item);
                    }
                    Ticket::NotYet => return None,
                    Ticket::Lost => {
                        self.pending = None;
                        self.dropped += 1;
                    }
                }
            }

            let header = unsafe { &*self.header };
            let read_seq = header.read_seq.load(Ordering::Acquire);
            let claim_seq = header.claim_seq.load(Ordering::Acquire);
            if read_seq >= claim_seq {
                return None;
            }

            // Tickets more than a ring behind the producers are gone: skip them in one step.
            let floor = claim_seq.saturating_sub(self.capacity);
            if read_seq < floor {
                if header
                    .read_seq
                    .compare_exchange(read_seq, floor, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    self.dropped += floor - read_seq;
                }
                continue;
            }

            if header
                .read_seq
                .compare_exchange(read_seq, read_seq + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.pending = Some(read_seq);
            }
        }
    }

    /// Blocking receive on competing queue consumer.
    pub fn recv_blocking<W: WaitStrategy>(&mut self, wait: &mut W) -> T {
        loop {
            if let Some(item) = self.try_recv() {
                wait.reset();
                return item;
            }
            unsafe {
                let target_seq = self
                    .pending
                    .unwrap_or_else(|| (*self.header).read_seq.load(Ordering::Relaxed));
                wait.wait(&*self.header, target_seq);
            }
        }
    }
}
