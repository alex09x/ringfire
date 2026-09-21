use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use memmap2::MmapMut;

use crate::error::{Result, RingfireError};
use crate::header::{
    RingHeader, Slot, FLAG_MODE_MPMC, FLAG_POLICY_LATEST_WINS, RINGFIRE_MAGIC, RINGFIRE_VERSION,
};
use crate::signature::LayoutSignature;
use crate::spmc::CleanupMode;
use crate::wait::{wake_futex, WaitStrategy};

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
    pub fn create<P: AsRef<Path>>(path: P, capacity: u64) -> Result<Self> {
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
            .mode(0o660)
            .open(&path_buf)?;

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
                claim_seq: std::sync::atomic::AtomicU64::new(1),
                flags: FLAG_POLICY_LATEST_WINS | FLAG_MODE_MPMC,
                futex_word: std::sync::atomic::AtomicU32::new(0),
                waiting_consumers: std::sync::atomic::AtomicU32::new(0),
                _align_pad: 0,
                read_seq: std::sync::atomic::AtomicU64::new(1),
                schema_sig: T::layout_signature(),
                arena_offset: 0,
                arena_size: 0,
                reader_registry_offset: 0,
                reader_registry_count: 0,
                _pad: [0; 24],
            });
        }

        let slots_ptr = unsafe {
            mmap.as_mut_ptr()
                .add(std::mem::size_of::<RingHeader>())
                .cast::<Slot<T>>()
        };

        for i in 0..capacity {
            unsafe {
                let slot = slots_ptr.add(i as usize);
                (*slot).seq = std::sync::atomic::AtomicU64::new(0);
            }
        }

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
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(&path_buf)?;

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
        let slots_ptr = unsafe {
            mmap.as_ptr()
                .add(std::mem::size_of::<RingHeader>())
                .cast::<Slot<T>>()
        };

        Ok(Self {
            path: path_buf,
            _file: file,
            _mmap: mmap,
            header: header_ptr as *mut RingHeader,
            slots: slots_ptr as *mut Slot<T>,
            capacity,
            mask,
            cleanup_mode: CleanupMode::Persistent,
            _marker: PhantomData,
        })
    }

    /// Sets the cleanup mode when this producer is dropped.
    pub fn set_cleanup_mode(&mut self, mode: CleanupMode) {
        self.cleanup_mode = mode;
    }

    /// Publishes a message into the ring buffer using atomic ticket claiming.
    #[inline(always)]
    pub fn push(&self, item: &T) -> u64 {
        unsafe {
            let seq = (*self.header).claim_seq.fetch_add(1, Ordering::Relaxed);
            let idx = (seq & self.mask) as usize;
            let slot = self.slots.add(idx);

            std::ptr::copy_nonoverlapping(item, &mut (*slot).data, 1);
            (*slot).seq.store(seq, Ordering::Release);
            (*self.header).write_seq.fetch_max(seq, Ordering::Release);
            wake_futex(&*self.header, 1);
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
/// each item is consumed by exactly one consumer.
pub struct MpmcQueueConsumer<T: Copy + 'static> {
    _mmap: MmapMut,
    header: *const RingHeader,
    slots: *const Slot<T>,
    mask: u64,
    _marker: PhantomData<T>,
}

unsafe impl<T: Copy + Send + 'static> Send for MpmcQueueConsumer<T> {}

impl<T: Copy + 'static> MpmcQueueConsumer<T> {
    /// Attaches to an MPMC ring buffer as a competing queue consumer.
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

        let mask = header.mask;
        let slots_ptr = unsafe {
            mmap.as_ptr()
                .add(std::mem::size_of::<RingHeader>())
                .cast::<Slot<T>>()
        };

        Ok(Self {
            _mmap: mmap,
            header: header_ptr,
            slots: slots_ptr,
            mask,
            _marker: PhantomData,
        })
    }

    /// Attempts to dequeue the next item.
    pub fn try_recv(&mut self) -> Option<T> {
        unsafe {
            let read_seq = (*self.header).read_seq.load(Ordering::Relaxed);
            let claim_seq = (*self.header).claim_seq.load(Ordering::Relaxed);

            if read_seq >= claim_seq {
                return None;
            }

            // Attempt to claim this ticket
            if (*self.header)
                .read_seq
                .compare_exchange_weak(
                    read_seq,
                    read_seq + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                return None;
            }

            let idx = (read_seq & self.mask) as usize;
            let slot = self.slots.add(idx);

            // Wait until the producer finishes publishing this slot
            while (*slot).seq.load(Ordering::Acquire) != read_seq {
                core::hint::spin_loop();
            }

            Some((*slot).data)
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
                let target_seq = (*self.header).read_seq.load(Ordering::Relaxed);
                wait.wait(&*self.header, target_seq);
            }
        }
    }
}
