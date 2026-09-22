//! # ReaderRegistry & Reader Tracking
//!
//! Shared-memory registry for monitoring active consumers, tracking reader lag,
//! calculating safe write headroom, and automatically reclaiming dead processes.

#[cfg(target_os = "linux")]
use std::path::Path;
use std::sync::atomic::Ordering;
use crate::error::{Result, RingfireError};
use crate::header::ReaderSlot;

pub const DEFAULT_MAX_READERS: usize = 32;

/// Snapshot of an active reader's status for monitoring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderInfo {
    pub slot_index: usize,
    pub pid: u32,
    pub name: String,
    pub cursor_seq: u64,
    pub lag: u64,
}

/// Shared memory registry holding reader registration slots.
pub struct ReaderRegistry {
    slots: *mut ReaderSlot,
    count: usize,
}

unsafe impl Send for ReaderRegistry {}
unsafe impl Sync for ReaderRegistry {}

impl ReaderRegistry {
    /// Initialize a new `ReaderRegistry` over pre-mapped shared memory.
    ///
    /// # Safety
    /// `ptr` must point to writable memory of at least `count * size_of::<ReaderSlot>()` bytes.
    pub unsafe fn init(ptr: *mut u8, count: usize) -> Self {
        let slots = ptr as *mut ReaderSlot;
        for i in 0..count {
            let slot = unsafe { &mut *slots.add(i) };
            slot.pid.store(0, Ordering::Relaxed);
            slot.active.store(0, Ordering::Relaxed);
            slot.cursor_seq.store(0, Ordering::Relaxed);
            slot.heartbeat_tsc.store(0, Ordering::Relaxed);
            slot.name = [0u8; 32];
            slot._pad = [0u8; 8];
        }

        Self { slots, count }
    }

    /// Open an existing `ReaderRegistry` from pre-mapped shared memory.
    ///
    /// # Safety
    /// `ptr` must point to an initialized `ReaderRegistry` region of `count` slots.
    pub unsafe fn from_ptr(ptr: *mut u8, count: usize) -> Self {
        Self {
            slots: ptr as *mut ReaderSlot,
            count,
        }
    }

    /// Total number of reader slots.
    pub fn capacity(&self) -> usize {
        self.count
    }

    /// Register a new reader in the shared memory registry.
    pub fn register(&self, name: &str, initial_cursor: u64) -> Result<ReaderRegistration> {
        let current_pid = std::process::id();

        // 1. Try to find a free slot (or reclaim dead process slot)
        for i in 0..self.count {
            let slot = unsafe { &*self.slots.add(i) };
            let pid = slot.pid.load(Ordering::Acquire);

            let is_free = pid == 0;
            let is_dead = pid != 0 && !is_process_alive(pid);

            if (is_free || is_dead)
                && slot
                    .pid
                    .compare_exchange(pid, current_pid, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            {
                slot.cursor_seq.store(initial_cursor, Ordering::Relaxed);
                slot.heartbeat_tsc.store(0, Ordering::Relaxed);

                let slot_mut = unsafe { &mut *self.slots.add(i) };
                let mut name_buf = [0u8; 32];
                let bytes = name.as_bytes();
                let len = bytes.len().min(31);
                name_buf[..len].copy_from_slice(&bytes[..len]);
                slot_mut.name = name_buf;

                slot.active.store(1, Ordering::Release);

                return Ok(ReaderRegistration {
                    slot_index: i,
                    slot: self.slots,
                });
            }
        }

        Err(RingfireError::NoAvailableReaderSlots)
    }

    /// Find the minimum sequence cursor across all active, alive readers.
    pub fn min_reader_seq(&self) -> Option<u64> {
        let mut min_seq = None;

        for i in 0..self.count {
            let slot = unsafe { &*self.slots.add(i) };
            if slot.active.load(Ordering::Acquire) == 1 {
                let pid = slot.pid.load(Ordering::Relaxed);
                if is_process_alive(pid) {
                    let seq = slot.cursor_seq.load(Ordering::Relaxed);
                    min_seq = Some(min_seq.map_or(seq, |curr: u64| curr.min(seq)));
                } else {
                    // Mark dead reader inactive
                    slot.active.store(0, Ordering::Release);
                    slot.pid.store(0, Ordering::Release);
                }
            }
        }

        min_seq
    }

    /// Calculate the lag of the slowest active reader relative to `write_seq`.
    pub fn reader_lag(&self, write_seq: u64) -> u64 {
        match self.min_reader_seq() {
            Some(min_seq) => write_seq.saturating_sub(min_seq),
            None => 0,
        }
    }

    /// Calculate how many messages the writer can write before the slowest active reader is lapped.
    pub fn headroom(&self, write_seq: u64, ring_capacity: u64) -> u64 {
        match self.min_reader_seq() {
            Some(min_seq) => {
                let lag = write_seq.saturating_sub(min_seq);
                ring_capacity.saturating_sub(lag)
            }
            None => ring_capacity,
        }
    }

    /// List all currently active readers and their lag.
    pub fn active_readers(&self, write_seq: u64) -> Vec<ReaderInfo> {
        let mut result = Vec::new();

        for i in 0..self.count {
            let slot = unsafe { &*self.slots.add(i) };
            if slot.active.load(Ordering::Acquire) == 1 {
                let pid = slot.pid.load(Ordering::Relaxed);
                if is_process_alive(pid) {
                    let cursor_seq = slot.cursor_seq.load(Ordering::Relaxed);
                    let name_len = slot.name.iter().position(|&b| b == 0).unwrap_or(32);
                    let name = String::from_utf8_lossy(&slot.name[..name_len]).into_owned();

                    result.push(ReaderInfo {
                        slot_index: i,
                        pid,
                        name,
                        cursor_seq,
                        lag: write_seq.saturating_sub(cursor_seq),
                    });
                } else {
                    slot.active.store(0, Ordering::Release);
                    slot.pid.store(0, Ordering::Release);
                }
            }
        }

        result
    }

    /// Scan and clear any abandoned slots from dead processes.
    pub fn prune_dead_readers(&self) -> usize {
        let mut pruned = 0;
        for i in 0..self.count {
            let slot = unsafe { &*self.slots.add(i) };
            let pid = slot.pid.load(Ordering::Acquire);
            if pid != 0 && !is_process_alive(pid) {
                slot.active.store(0, Ordering::Release);
                slot.pid.store(0, Ordering::Release);
                pruned += 1;
            }
        }
        pruned
    }
}

/// RAII registration handle for a consumer in `ReaderRegistry`.
pub struct ReaderRegistration {
    slot_index: usize,
    slot: *mut ReaderSlot,
}

unsafe impl Send for ReaderRegistration {}
unsafe impl Sync for ReaderRegistration {}

impl ReaderRegistration {
    /// Update the reader's current sequence cursor.
    #[inline]
    pub fn update_cursor(&self, seq: u64) {
        let slot = unsafe { &*self.slot.add(self.slot_index) };
        slot.cursor_seq.store(seq, Ordering::Relaxed);
    }

    /// Index of this reader slot.
    #[inline]
    pub fn slot_index(&self) -> usize {
        self.slot_index
    }
}

impl Drop for ReaderRegistration {
    fn drop(&mut self) {
        let slot = unsafe { &*self.slot.add(self.slot_index) };
        slot.active.store(0, Ordering::Release);
        slot.pid.store(0, Ordering::Release);
    }
}

/// Check if a process with `pid` is currently alive.
#[inline]
pub fn is_process_alive(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        Path::new(&format!("/proc/{}", pid)).exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader_registry_lifecycle() {
        let count = 4;
        let total_size = count * std::mem::size_of::<ReaderSlot>();
        let mut memory = vec![0u8; total_size + 128];
        let ptr = memory.as_mut_ptr();
        let offset = (64 - (ptr as usize % 64)) % 64;
        let aligned_ptr = unsafe { ptr.add(offset) };

        let registry = unsafe { ReaderRegistry::init(aligned_ptr, count) };

        assert_eq!(registry.min_reader_seq(), None);

        let reg1 = registry.register("worker-1", 100).unwrap();
        assert_eq!(registry.min_reader_seq(), Some(100));

        let reg2 = registry.register("worker-2", 150).unwrap();
        assert_eq!(registry.min_reader_seq(), Some(100));

        reg1.update_cursor(120);
        assert_eq!(registry.min_reader_seq(), Some(120));

        assert_eq!(registry.reader_lag(200), 80);
        assert_eq!(registry.headroom(200, 1024), 1024 - 80);

        drop(reg1);
        // worker-1 dropped, so min is now worker-2 at 150
        assert_eq!(registry.min_reader_seq(), Some(150));
        drop(reg2);
        assert_eq!(registry.min_reader_seq(), None);
    }
}
