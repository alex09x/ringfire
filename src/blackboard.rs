use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{fence, Ordering};
use std::time::{Duration, Instant};
use memmap2::MmapMut;

use crate::error::{Result, RingfireError};
use crate::header::{
    BlackboardHeader, BlackboardSlot, BLACKBOARD_MAGIC, BLACKBOARD_VERSION,
};
use crate::shm::create_backing_file;
use crate::spmc::{racy_copy, CleanupMode};

/// How long a reader waits on a slot that stays mid-write before reporting
/// [`RingfireError::WriterStalled`].
const STALL_TIMEOUT: Duration = Duration::from_millis(100);

/// Producer for a shared memory Blackboard state table.
/// Provides O(1) tear-free updates using per-slot 64-bit seqlocks.
pub struct BlackboardProducer<V: Copy> {
    path: PathBuf,
    _file: File,
    _mmap: MmapMut,
    _header: *mut BlackboardHeader,
    slots: *mut BlackboardSlot<V>,
    slot_count: usize,
    cleanup_mode: CleanupMode,
    _marker: PhantomData<V>,
}

unsafe impl<V: Copy + Send> Send for BlackboardProducer<V> {}
unsafe impl<V: Copy + Sync> Sync for BlackboardProducer<V> {}

impl<V: Copy> BlackboardProducer<V> {
    /// Creates a new Blackboard table at `path` capable of holding `slot_count` keys.
    ///
    /// The producer holds an exclusive `flock` on the file; a second producer on the same
    /// path fails with `ProducerAlreadyExists` without disturbing the live table.
    pub fn create<P: AsRef<Path>>(path: P, slot_count: usize) -> Result<Self> {
        let value_size = std::mem::size_of::<V>();
        let slot_size = std::mem::size_of::<BlackboardSlot<V>>();
        let total_size = std::mem::size_of::<BlackboardHeader>() + (slot_count * slot_size);
        let path_buf = path.as_ref().to_path_buf();

        if slot_count > u32::MAX as usize {
            return Err(RingfireError::InvalidCapacity(slot_count as u64));
        }
        let file = create_backing_file(&path_buf, 0o660, true, total_size as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let header_ptr = mmap.as_mut_ptr().cast::<BlackboardHeader>();
        unsafe {
            header_ptr.write(BlackboardHeader {
                magic: 0,
                version: BLACKBOARD_VERSION,
                value_size: value_size as u32,
                slot_size: slot_size as u32,
                slot_count: slot_count as u32,
                _reserved: [0; 2],
                _pad: [0; 88],
            });
        }

        let slots_ptr = unsafe {
            mmap.as_mut_ptr()
                .add(std::mem::size_of::<BlackboardHeader>())
                .cast::<BlackboardSlot<V>>()
        };

        for i in 0..slot_count {
            unsafe {
                let slot = slots_ptr.add(i);
                (*slot).seqlock = std::sync::atomic::AtomicU64::new(0);
            }
        }
        fence(Ordering::Release);
        unsafe { std::ptr::write_volatile(&mut (*header_ptr).magic, BLACKBOARD_MAGIC) };

        Ok(Self {
            path: path_buf,
            _file: file,
            _mmap: mmap,
            _header: header_ptr,
            slots: slots_ptr,
            slot_count,
            cleanup_mode: CleanupMode::UnlinkOnDrop,
            _marker: PhantomData,
        })
    }

    /// Sets cleanup mode on drop.
    pub fn set_cleanup_mode(&mut self, mode: CleanupMode) {
        self.cleanup_mode = mode;
    }

    /// Writes or updates the value associated with `key`.
    /// Executes atomic seqlock update (even -> odd -> even) with Release memory ordering.
    #[inline]
    pub fn write(&mut self, key: usize, val: &V) -> Result<()> {
        if key >= self.slot_count {
            return Err(RingfireError::KeyOutOfRange {
                key,
                capacity: self.slot_count,
            });
        }

        unsafe {
            let slot = self.slots.add(key);
            let s = (*slot).seqlock.load(Ordering::Relaxed);
            let write_s = if (s & 1) == 0 { s + 1 } else { s + 2 };

            // Step 1: Mark write in progress (odd seqlock); the fence keeps the payload
            // stores below from becoming visible before the odd marker.
            (*slot).seqlock.store(write_s, Ordering::Relaxed);
            fence(Ordering::Release);

            // Step 2: Write payload
            std::ptr::copy_nonoverlapping(val, &mut (*slot).value, 1);

            // Step 3: Mark write complete (even seqlock)
            (*slot).seqlock.store(write_s + 1, Ordering::Release);
        }

        Ok(())
    }

    /// Number of slots in the blackboard.
    #[inline]
    pub fn slot_count(&self) -> usize {
        self.slot_count
    }
}

impl<V: Copy> Drop for BlackboardProducer<V> {
    fn drop(&mut self) {
        if self.cleanup_mode == CleanupMode::UnlinkOnDrop {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Consumer for reading from a shared memory Blackboard state table.
/// Provides sub-10ns O(1) lock-free, tear-free reads.
pub struct BlackboardConsumer<V: Copy> {
    _mmap: MmapMut,
    _header: *const BlackboardHeader,
    slots: *const BlackboardSlot<V>,
    slot_count: usize,
    _marker: PhantomData<V>,
}

unsafe impl<V: Copy + Send> Send for BlackboardConsumer<V> {}
unsafe impl<V: Copy + Sync> Sync for BlackboardConsumer<V> {}

impl<V: Copy> BlackboardConsumer<V> {
    /// Attaches to an existing Blackboard table at `path`.
    pub fn attach<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if mmap.len() < std::mem::size_of::<BlackboardHeader>() {
            return Err(RingfireError::CorruptLayout("mapping smaller than the blackboard header"));
        }

        let header_ptr = mmap.as_ptr().cast::<BlackboardHeader>();
        let header = unsafe { &*header_ptr };

        let magic = unsafe { std::ptr::read_volatile(&header.magic) };
        fence(Ordering::Acquire);
        if magic != BLACKBOARD_MAGIC {
            return Err(RingfireError::InvalidMagic {
                expected: BLACKBOARD_MAGIC,
                actual: magic,
            });
        }

        if header.version != BLACKBOARD_VERSION {
            return Err(RingfireError::VersionMismatch {
                expected: BLACKBOARD_VERSION,
                actual: header.version,
            });
        }

        let value_size = std::mem::size_of::<V>();
        if header.value_size as usize != value_size {
            return Err(RingfireError::ValueSizeMismatch {
                expected: header.value_size as usize,
                actual: value_size,
            });
        }

        let slot_count = header.slot_count as usize;
        if header.slot_size as usize != std::mem::size_of::<BlackboardSlot<V>>() {
            return Err(RingfireError::CorruptLayout("blackboard slot stride mismatch"));
        }
        let end = slot_count
            .checked_mul(header.slot_size as usize)
            .and_then(|b| b.checked_add(std::mem::size_of::<BlackboardHeader>()));
        if end.is_none_or(|e| e > mmap.len()) {
            return Err(RingfireError::CorruptLayout("blackboard slots extend past the mapping"));
        }
        let slots_ptr = unsafe {
            mmap.as_ptr()
                .add(std::mem::size_of::<BlackboardHeader>())
                .cast::<BlackboardSlot<V>>()
        };

        Ok(Self {
            _mmap: mmap,
            _header: header_ptr,
            slots: slots_ptr,
            slot_count,
            _marker: PhantomData,
        })
    }

    /// Reads the value for `key` in O(1) time using seqlock consistency validation.
    /// Returns `None` if the slot has never been written, and
    /// [`RingfireError::WriterStalled`] if the slot stays mid-write for over 100 ms
    /// (the writer was descheduled for that long or died mid-update).
    #[inline]
    pub fn read(&self, key: usize) -> Result<Option<V>> {
        if key >= self.slot_count {
            return Err(RingfireError::KeyOutOfRange {
                key,
                capacity: self.slot_count,
            });
        }

        unsafe {
            let slot = self.slots.add(key);
            let mut spins = 0u32;
            let mut stalled_since: Option<Instant> = None;
            loop {
                let s1 = (*slot).seqlock.load(Ordering::Acquire);
                if s1 == 0 {
                    return Ok(None);
                }

                if s1 & 1 == 0 {
                    let val = racy_copy(&(*slot).value);
                    fence(Ordering::Acquire);
                    if (*slot).seqlock.load(Ordering::Relaxed) == s1 {
                        return Ok(Some(val));
                    }
                }

                // Writer in progress (odd) or raced us: back off.
                spins = spins.saturating_add(1);
                if spins < 10_000 {
                    core::hint::spin_loop();
                } else {
                    let since = *stalled_since.get_or_insert_with(Instant::now);
                    if since.elapsed() > STALL_TIMEOUT {
                        return Err(RingfireError::WriterStalled { key });
                    }
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Reads the value directly into `out`. Returns `true` if read, `false` if unwritten.
    #[inline]
    pub fn read_copy(&self, key: usize, out: &mut V) -> Result<bool> {
        match self.read(key)? {
            Some(val) => {
                *out = val;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Number of slots in the blackboard.
    #[inline]
    pub fn slot_count(&self) -> usize {
        self.slot_count
    }
}
