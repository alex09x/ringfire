use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use memmap2::MmapMut;

use crate::error::{Result, RingfireError};
use crate::header::{
    BlackboardHeader, BlackboardSlot, BLACKBOARD_MAGIC, BLACKBOARD_VERSION,
};
use crate::spmc::CleanupMode;

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
    pub fn create<P: AsRef<Path>>(path: P, slot_count: usize) -> Result<Self> {
        let value_size = std::mem::size_of::<V>();
        let slot_size = std::mem::size_of::<BlackboardSlot<V>>();
        let total_size = std::mem::size_of::<BlackboardHeader>() + (slot_count * slot_size);
        let path_buf = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o660)
            .open(&path_buf)?;

        file.set_len(total_size as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let header_ptr = mmap.as_mut_ptr().cast::<BlackboardHeader>();
        unsafe {
            header_ptr.write(BlackboardHeader {
                magic: BLACKBOARD_MAGIC,
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

            // Step 1: Mark write in progress (odd seqlock)
            (*slot).seqlock.store(write_s, Ordering::Release);

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

        let header_ptr = mmap.as_ptr().cast::<BlackboardHeader>();
        let header = unsafe { &*header_ptr };

        if header.magic != BLACKBOARD_MAGIC {
            return Err(RingfireError::InvalidMagic {
                expected: BLACKBOARD_MAGIC,
                actual: header.magic,
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
    /// Returns `None` if the slot has never been written.
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
            let mut spins = 0;
            loop {
                let s1 = (*slot).seqlock.load(Ordering::Acquire);
                if s1 == 0 {
                    return Ok(None);
                }

                // If odd, writer is currently in progress
                if s1 & 1 != 0 {
                    core::hint::spin_loop();
                    spins += 1;
                    if spins > 10_000 {
                        std::thread::yield_now();
                    }
                    continue;
                }

                // Read value
                let val = std::ptr::read_volatile(&(*slot).value);

                // Check seqlock didn't change
                let s2 = (*slot).seqlock.load(Ordering::Acquire);
                if s1 == s2 {
                    return Ok(Some(val));
                }

                core::hint::spin_loop();
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
