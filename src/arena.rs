//! # PayloadArena & BlobRef
//!
//! Ultra-high-performance contiguous byte arena in shared memory for variable-sized
//! and large IPC messages (e.g. L2/L3 order books, network packets, serialized frames).
//!
//! Inspired by Firedancer's `dcache` architecture:
//! - Contiguous cyclic byte arena mapped alongside the ring buffer.
//! - Fixed-size ring buffer slots carry only lightweight metadata (`BlobRef`, 16 bytes).
//! - Guaranteed zero memory fragmentation: allocations that would wrap around the ring
//!   boundary are automatically aligned to index 0, ensuring every `BlobRef` maps
//!   to a strictly contiguous memory slice (`&[u8]`).

use std::sync::atomic::{AtomicU64, Ordering};
use crate::error::{Result, RingfireError};

/// 16-byte descriptor referencing a payload stored in `PayloadArena`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct BlobRef {
    /// Global cumulative offset in the arena
    pub offset: u64,
    /// Exact payload length in bytes
    pub len: u32,
    /// Application-defined flags (e.g. codec, schema, compression tag)
    pub flags: u32,
}

impl BlobRef {
    pub const EMPTY: Self = Self {
        offset: 0,
        len: 0,
        flags: 0,
    };

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Header for `PayloadArena` located at the start of the arena section.
/// 64-byte cache-line aligned.
#[repr(C, align(64))]
pub struct ArenaHeader {
    /// Total arena capacity in bytes (must be a power of two)
    pub capacity: u64,
    /// Bitmask for circular indexing (`capacity - 1`)
    pub mask: u64,
    /// Cumulative reserved byte counter
    pub reserved: AtomicU64,
    /// Padding to exactly 64 bytes
    pub _pad: [u8; 40],
}

const _: () = {
    assert!(std::mem::size_of::<ArenaHeader>() == 64);
    assert!(std::mem::align_of::<ArenaHeader>() == 64);
};

/// High-throughput shared memory byte arena.
pub struct PayloadArena {
    header: *mut ArenaHeader,
    data: *mut u8,
    capacity: usize,
    mask: usize,
}

unsafe impl Send for PayloadArena {}
unsafe impl Sync for PayloadArena {}

impl PayloadArena {
    /// Initialize a new `PayloadArena` over pre-mapped shared memory.
    ///
    /// # Safety
    /// `ptr` must point to valid, writable memory of at least `size_of::<ArenaHeader>() + capacity` bytes.
    /// `capacity` must be a power of two and a multiple of 64.
    pub unsafe fn init(ptr: *mut u8, capacity: usize) -> Result<Self> {
        if !capacity.is_power_of_two() || capacity < 64 {
            return Err(RingfireError::InvalidCapacity(capacity as u64));
        }

        let header = ptr as *mut ArenaHeader;
        unsafe {
            (*header).capacity = capacity as u64;
            (*header).mask = (capacity - 1) as u64;
            (*header).reserved = AtomicU64::new(0);
            (*header)._pad = [0u8; 40];
        }

        let data = unsafe { ptr.add(std::mem::size_of::<ArenaHeader>()) };
        Ok(Self {
            header,
            data,
            capacity,
            mask: capacity - 1,
        })
    }

    /// Open an existing `PayloadArena` from pre-mapped shared memory.
    ///
    /// # Safety
    /// `ptr` must point to an initialized `PayloadArena` region.
    pub unsafe fn from_ptr(ptr: *mut u8) -> Result<Self> {
        let header = ptr as *mut ArenaHeader;
        let capacity = unsafe { (*header).capacity as usize };
        if !capacity.is_power_of_two() || capacity < 64 {
            return Err(RingfireError::InvalidCapacity(capacity as u64));
        }

        let mask = unsafe { (*header).mask as usize };
        let data = unsafe { ptr.add(std::mem::size_of::<ArenaHeader>()) };

        Ok(Self {
            header,
            data,
            capacity,
            mask,
        })
    }

    /// Total capacity of the payload arena in bytes.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Atomically reserve `len` bytes in the arena.
    ///
    /// If the allocation would cross the circular buffer boundary, it skips
    /// to index 0, ensuring that every returned `BlobRef` maps to a contiguous slice.
    #[inline]
    pub fn reserve(&self, len: usize, flags: u32) -> Result<BlobRef> {
        if len == 0 {
            return Ok(BlobRef::EMPTY);
        }
        if len > self.capacity {
            return Err(RingfireError::ArenaPayloadTooLarge {
                len,
                max_capacity: self.capacity,
            });
        }

        // Align allocation up to 64 bytes (cache line)
        let slot_size = (len + 63) & !63;

        let header = unsafe { &*self.header };
        loop {
            let curr = header.reserved.load(Ordering::Relaxed);
            let from_ix = (curr & (self.mask as u64)) as usize;

            // Check if contiguous slice fits before end of arena
            let (actual_offset, next) = if from_ix + len > self.capacity {
                // Wrap around: align to capacity boundary (index 0 on next lap)
                let aligned_to_end = (curr | (self.mask as u64)) + 1;
                (aligned_to_end, aligned_to_end + slot_size as u64)
            } else {
                (curr, curr + slot_size as u64)
            };

            if header
                .reserved
                .compare_exchange_weak(curr, next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(BlobRef {
                    offset: actual_offset,
                    len: len as u32,
                    flags,
                });
            }
        }
    }

    /// Write payload bytes into the arena, returning a `BlobRef`.
    #[inline]
    pub fn write_blob(&self, data: &[u8], flags: u32) -> Result<BlobRef> {
        let blob_ref = self.reserve(data.len(), flags)?;
        if !blob_ref.is_empty() {
            let dest = self.slice_mut(blob_ref);
            dest.copy_from_slice(data);
        }
        Ok(blob_ref)
    }

    /// Zero-copy write directly into reserved arena slice.
    #[inline]
    pub fn write_blob_with<F, R>(&self, len: usize, flags: u32, f: F) -> Result<(BlobRef, R)>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let blob_ref = self.reserve(len, flags)?;
        let result = if !blob_ref.is_empty() {
            let dest = self.slice_mut(blob_ref);
            f(dest)
        } else {
            f(&mut [])
        };
        Ok((blob_ref, result))
    }

    /// Copy payload bytes out of the arena into `out`.
    #[inline]
    pub fn read_blob(&self, blob_ref: BlobRef, out: &mut [u8]) -> Result<usize> {
        let len = blob_ref.len as usize;
        if len == 0 {
            return Ok(0);
        }
        if out.len() < len {
            return Err(RingfireError::BufferTooSmall {
                required: len,
                provided: out.len(),
            });
        }

        let src = self.slice(blob_ref);
        out[..len].copy_from_slice(src);
        Ok(len)
    }

    /// Inspect payload in-place without copying.
    #[inline]
    pub fn view_blob<R>(&self, blob_ref: BlobRef, f: impl FnOnce(&[u8]) -> R) -> R {
        if blob_ref.is_empty() {
            f(&[])
        } else {
            let src = self.slice(blob_ref);
            f(src)
        }
    }

    /// Check if a `BlobRef` has been overwritten by the producer wrapping around.
    #[inline]
    pub fn is_lapped(&self, blob_ref: BlobRef) -> bool {
        let curr = unsafe { (*self.header).reserved.load(Ordering::Acquire) };
        curr.saturating_sub(blob_ref.offset) > self.capacity as u64
    }

    #[inline]
    fn slice(&self, blob_ref: BlobRef) -> &[u8] {
        let offset = (blob_ref.offset & (self.mask as u64)) as usize;
        unsafe { std::slice::from_raw_parts(self.data.add(offset), blob_ref.len as usize) }
    }

    #[inline]
    fn slice_mut(&self, blob_ref: BlobRef) -> &mut [u8] {
        let offset = (blob_ref.offset & (self.mask as u64)) as usize;
        unsafe { std::slice::from_raw_parts_mut(self.data.add(offset), blob_ref.len as usize) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_contiguous_wraparound() {
        let capacity = 1024;
        let total_size = std::mem::size_of::<ArenaHeader>() + capacity;
        let mut buffer = vec![0u8; total_size + 128];
        let ptr = buffer.as_mut_ptr();
        let offset = (64 - (ptr as usize % 64)) % 64;
        let aligned_ptr = unsafe { ptr.add(offset) };

        let arena = unsafe { PayloadArena::init(aligned_ptr, capacity).unwrap() };

        // Write 900 bytes
        let data1 = vec![0xAAu8; 900];
        let ref1 = arena.write_blob(&data1, 1).unwrap();
        assert_eq!(ref1.len, 900);
        assert_eq!(ref1.offset, 0);

        let mut read_buf = vec![0u8; 900];
        arena.read_blob(ref1, &mut read_buf).unwrap();
        assert_eq!(read_buf, data1);

        // Writing 200 bytes would overflow 900 + 200 > 1024 -> must wrap to index 0!
        let data2 = vec![0xBBu8; 200];
        let ref2 = arena.write_blob(&data2, 2).unwrap();
        assert_eq!(ref2.len, 200);
        // Offset must be aligned to next cycle (1024)
        assert_eq!(ref2.offset, 1024);

        let mut read_buf2 = vec![0u8; 200];
        arena.read_blob(ref2, &mut read_buf2).unwrap();
        assert_eq!(read_buf2, data2);
    }
}
