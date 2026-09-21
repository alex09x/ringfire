//! # ringfire
//!
//! Ultra-low-latency, zero-copy lock-free Inter-Process Communication (IPC)
//! ring buffer and shared memory bus for Rust.

use std::fs::OpenOptions;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use memmap2::MmapMut;

pub const RINGFIRE_MAGIC: u64 = 0x5249_4E47_4649_5245; // "RINGFIRE" in ASCII
pub const RINGFIRE_VERSION: u32 = 1;

/// Common header stored at the beginning of the shared memory region.
#[repr(C, align(128))]
pub struct RingHeader {
    pub magic: u64,
    pub version: u32,
    pub element_size: u32,
    pub capacity: u64,
    pub mask: u64,
    pub write_seq: AtomicU64,
    _pad: [u8; 88], // Cache-line padding to 128 bytes
}

/// An entry in the ring buffer containing sequence number and payload.
#[repr(C)]
pub struct Slot<T> {
    pub seq: AtomicU64,
    pub data: T,
}

/// Single-producer ring buffer writer backed by shared memory (`/dev/shm`).
pub struct RingProducer<T: Copy> {
    mmap: MmapMut,
    header: *mut RingHeader,
    slots: *mut Slot<T>,
    capacity: u64,
    mask: u64,
    seq: u64,
    _marker: PhantomData<T>,
}

unsafe impl<T: Copy + Send> Send for RingProducer<T> {}

impl<T: Copy> RingProducer<T> {
    /// Creates a new shared memory ring buffer at `path` with `capacity` slots.
    /// Capacity must be a power of two.
    pub fn create<P: AsRef<Path>>(path: P, capacity: u64) -> io::Result<Self> {
        if !capacity.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Capacity must be a power of two",
            ));
        }

        let slot_size = std::mem::size_of::<Slot<T>>();
        let total_size = std::mem::size_of::<RingHeader>() + (capacity as usize * slot_size);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

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
                write_seq: AtomicU64::new(0),
                _pad: [0; 88],
            });
        }

        let slots_ptr = unsafe {
            mmap.as_mut_ptr()
                .add(std::mem::size_of::<RingHeader>())
                .cast::<Slot<T>>()
        };

        // Initialize slots sequence numbers
        for i in 0..capacity {
            unsafe {
                let slot = slots_ptr.add(i as usize);
                (*slot).seq = AtomicU64::new(0);
            }
        }

        Ok(Self {
            mmap,
            header: header_ptr,
            slots: slots_ptr,
            capacity,
            mask: capacity - 1,
            seq: 1,
            _marker: PhantomData,
        })
    }

    /// Publishes a message into the ring buffer.
    /// Uses release ordering so consumers see complete payload.
    #[inline(always)]
    pub fn push(&mut self, item: &T) {
        let idx = (self.seq & self.mask) as usize;
        unsafe {
            let slot = self.slots.add(idx);
            // Write data payload
            std::ptr::copy_nonoverlapping(item, &mut (*slot).data, 1);
            // Publish sequence number with release ordering
            (*slot).seq.store(self.seq, Ordering::Release);
            // Update global header sequence
            (*self.header).write_seq.store(self.seq, Ordering::Release);
        }
        self.seq += 1;
    }

    /// Current sequence number of the producer.
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.seq - 1
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
    _marker: PhantomData<T>,
}

unsafe impl<T: Copy + Send> Send for RingConsumer<T> {}

impl<T: Copy> RingConsumer<T> {
    /// Attaches to an existing shared memory ring buffer at `path`.
    pub fn attach<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let header_ptr = mmap.as_ptr().cast::<RingHeader>();
        let header = unsafe { &*header_ptr };

        if header.magic != RINGFIRE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid ringfire magic header",
            ));
        }

        let slot_size = std::mem::size_of::<Slot<T>>();
        if header.element_size as usize != slot_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Element size mismatch: header expects {}, struct is {}",
                    header.element_size, slot_size
                ),
            ));
        }

        let capacity = header.capacity;
        let mask = header.mask;
        let slots_ptr = unsafe {
            mmap.as_ptr()
                .add(std::mem::size_of::<RingHeader>())
                .cast::<Slot<T>>()
        };

        // Start at current write sequence or 1
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
            _marker: PhantomData,
        })
    }

    /// Attempts to read the next available message without blocking.
    /// Returns `Some(T)` if a new message was published, or `None` if caught up.
    #[inline(always)]
    pub fn try_recv(&mut self) -> Option<T> {
        let idx = (self.cursor & self.mask) as usize;
        unsafe {
            let slot = self.slots.add(idx);
            let published_seq = (*slot).seq.load(Ordering::Acquire);

            if published_seq == self.cursor {
                let data = (*slot).data;
                self.cursor += 1;
                Some(data)
            } else if published_seq > self.cursor {
                // Reader fell behind (lapped by producer)
                self.cursor = published_seq;
                let data = (*slot).data;
                self.cursor += 1;
                Some(data)
            } else {
                None
            }
        }
    }

    /// Current reader cursor sequence.
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    struct TestTrade {
        time_ns: u64,
        px: u64,
        sz: u64,
        side: u8,
    }

    #[test]
    fn test_ring_buffer_push_and_recv() {
        let tmp_path = std::env::temp_dir().join("test_ringfire.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let mut producer = RingProducer::<TestTrade>::create(&tmp_path, 1024).unwrap();
        let mut consumer = RingConsumer::<TestTrade>::attach(&tmp_path).unwrap();

        assert_eq!(consumer.try_recv(), None);

        let t1 = TestTrade { time_ns: 100, px: 81200, sz: 15, side: b'B' };
        let t2 = TestTrade { time_ns: 200, px: 81205, sz: 20, side: b'S' };

        producer.push(&t1);
        producer.push(&t2);

        assert_eq!(consumer.try_recv(), Some(t1));
        assert_eq!(consumer.try_recv(), Some(t2));
        assert_eq!(consumer.try_recv(), None);

        let _ = std::fs::remove_file(&tmp_path);
    }
}
