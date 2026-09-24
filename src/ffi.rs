#![allow(clippy::missing_safety_doc)]

use std::ffi::CStr;
use std::fs::{File, OpenOptions};
use std::os::raw::c_char;
use std::path::PathBuf;
use std::sync::atomic::{fence, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use memmap2::MmapMut;

use crate::header::{
    validate_ring, BlackboardHeader, RingHeader, RingLayout, BLACKBOARD_MAGIC, BLACKBOARD_VERSION,
    FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS, SLOT_WRITING,
};
use crate::shm::create_backing_file;
use crate::spmc::oldest_retained;
use crate::wait::wake_futex;

pub struct RingProducerRaw {
    _path: PathBuf,
    _file: Option<File>,
    _mmap: MmapMut,
    header: *mut RingHeader,
    slots_base: *mut u8,
    slot_size: usize,
    element_size: usize,
    mask: u64,
    seq: u64,
}

pub struct RingConsumerRaw {
    _mmap: MmapMut,
    header: *const RingHeader,
    slots_base: *const u8,
    slot_size: usize,
    element_size: usize,
    capacity: u64,
    mask: u64,
    cursor: u64,
    lapped_total: u64,
}

pub struct BlackboardRaw {
    _file: File,
    _mmap: MmapMut,
    _header: *mut BlackboardHeader,
    slots_base: *mut u8,
    slot_size: usize,
    value_size: usize,
    slot_count: usize,
    is_producer: bool,
    path: PathBuf,
}

/// Slot stride for an `element_size`-byte payload: 8-byte sequence + payload, 8-aligned.
/// Matches `size_of::<Slot<T>>()` for any `T` with alignment <= 8.
fn raw_slot_size(element_size: u32) -> usize {
    (8 + element_size as usize).next_multiple_of(8)
}

unsafe fn path_from_c(path: *const c_char) -> Option<PathBuf> {
    if path.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(path) }.to_str().ok().map(PathBuf::from)
}

/// Creates a ring for `element_size`-byte records. Returns NULL on invalid arguments, I/O
/// failure, or if another producer holds the ring.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_producer_create(
    path: *const c_char,
    capacity: u64,
    element_size: u32,
) -> *mut RingProducerRaw {
    let Some(path_buf) = (unsafe { path_from_c(path) }) else {
        return std::ptr::null_mut();
    };
    if !capacity.is_power_of_two() || element_size == 0 {
        return std::ptr::null_mut();
    }

    let slot_size = raw_slot_size(element_size);
    let slots_offset = std::mem::size_of::<RingHeader>();
    let total_size = slots_offset + (capacity as usize * slot_size);

    let Ok(file) = create_backing_file(&path_buf, 0o660, true, total_size as u64) else {
        return std::ptr::null_mut();
    };
    let Ok(mut mmap) = (unsafe { MmapMut::map_mut(&file) }) else {
        return std::ptr::null_mut();
    };

    let header_ptr = mmap.as_mut_ptr().cast::<RingHeader>();
    unsafe {
        RingHeader::initialize(
            header_ptr,
            &RingLayout {
                capacity,
                slot_size,
                flags: FLAG_POLICY_LATEST_WINS | FLAG_MODE_SPMC,
                schema_sig: 0,
                claim_seq: 0,
                read_seq: 0,
                registry_offset: 0,
                registry_count: 0,
                slots_offset,
                arena_offset: 0,
                arena_size: 0,
            },
        );
    }

    let slots_base = unsafe { mmap.as_mut_ptr().add(slots_offset) };
    for i in 0..capacity as usize {
        unsafe { slots_base.add(i * slot_size).cast::<AtomicU64>().write(AtomicU64::new(0)) };
    }
    unsafe { RingHeader::publish(header_ptr) };

    Box::into_raw(Box::new(RingProducerRaw {
        _path: path_buf,
        _file: Some(file),
        _mmap: mmap,
        header: header_ptr,
        slots_base,
        slot_size,
        element_size: element_size as usize,
        mask: capacity - 1,
        seq: 1,
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_producer_push(
    prod: *mut RingProducerRaw,
    data: *const u8,
) -> i32 {
    if prod.is_null() || data.is_null() {
        return -1;
    }
    unsafe {
        let p = &mut *prod;
        let slot_ptr = p.slots_base.add((p.seq & p.mask) as usize * p.slot_size);
        let seq_word = &*slot_ptr.cast::<AtomicU64>();

        seq_word.store(SLOT_WRITING, Ordering::Relaxed);
        fence(Ordering::Release);
        std::ptr::copy_nonoverlapping(data, slot_ptr.add(8), p.element_size);
        seq_word.store(p.seq, Ordering::Release);
        (*p.header).write_seq.store(p.seq, Ordering::Release);
        wake_futex(&*p.header, i32::MAX);

        p.seq += 1;
    }
    0
}

/// Closes the producer. The ring file is left in place (a later create replaces it).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_producer_close(prod: *mut RingProducerRaw) {
    if !prod.is_null() {
        drop(unsafe { Box::from_raw(prod) });
    }
}

/// Attaches to a ring of `element_size`-byte records, starting at the oldest retained
/// message. Returns NULL if the file is missing, not a v2 ring, or has another record size.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_attach(
    path: *const c_char,
    element_size: u32,
) -> *mut RingConsumerRaw {
    let Some(path_buf) = (unsafe { path_from_c(path) }) else {
        return std::ptr::null_mut();
    };
    if element_size == 0 {
        return std::ptr::null_mut();
    }
    let Ok(file) = OpenOptions::new().read(true).write(true).open(&path_buf) else {
        return std::ptr::null_mut();
    };
    let Ok(mmap) = (unsafe { MmapMut::map_mut(&file) }) else {
        return std::ptr::null_mut();
    };

    let slot_size = raw_slot_size(element_size);
    let Ok(view) = (unsafe { validate_ring(mmap.as_ptr(), mmap.len(), Some(slot_size), 8) }) else {
        return std::ptr::null_mut();
    };
    let header = mmap.as_ptr().cast::<RingHeader>();
    let write_seq = unsafe { (*header).write_seq.load(Ordering::Acquire) };
    let slots_base = unsafe { mmap.as_ptr().add(view.slots_offset) };

    Box::into_raw(Box::new(RingConsumerRaw {
        _mmap: mmap,
        header,
        slots_base,
        slot_size,
        element_size: element_size as usize,
        capacity: view.capacity,
        mask: view.mask,
        cursor: oldest_retained(write_seq, view.capacity),
        lapped_total: 0,
    }))
}

impl RingConsumerRaw {
    fn skip_overwritten(&mut self, seen: u64) -> bool {
        let write_seq = unsafe { (*self.header).write_seq.load(Ordering::Acquire) };
        let oldest = oldest_retained(write_seq.max(seen), self.capacity);
        if oldest > self.cursor {
            self.lapped_total += oldest - self.cursor;
            self.cursor = oldest;
            true
        } else {
            false
        }
    }

    /// Copies the next message into `out`. Returns false when caught up.
    unsafe fn next_into(&mut self, out: *mut u8) -> bool {
        loop {
            let want = self.cursor;
            let slot_ptr = unsafe { self.slots_base.add((want & self.mask) as usize * self.slot_size) };
            let seq_word = unsafe { &*slot_ptr.cast::<AtomicU64>() };
            let s1 = seq_word.load(Ordering::Acquire);
            let seen = if s1 == want {
                unsafe { std::ptr::copy_nonoverlapping(slot_ptr.add(8), out, self.element_size) };
                fence(Ordering::Acquire);
                let s2 = seq_word.load(Ordering::Relaxed);
                if s2 == want {
                    self.cursor += 1;
                    return true;
                }
                if s2 == SLOT_WRITING { 0 } else { s2 }
            } else if s1 == SLOT_WRITING || s1 < want {
                return false;
            } else {
                s1
            };
            if !self.skip_overwritten(seen) {
                return false;
            }
        }
    }
}

/// Returns 1 if a message was copied into `out_data`, 0 if the ring is caught up, -1 on
/// invalid arguments. Messages lost to lapping are skipped (see `ringfire_consumer_lapped_count`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_try_recv(
    cons: *mut RingConsumerRaw,
    out_data: *mut u8,
) -> i32 {
    if cons.is_null() || out_data.is_null() {
        return -1;
    }
    unsafe { (*cons).next_into(out_data) as i32 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_recv_batch(
    cons: *mut RingConsumerRaw,
    out_buf: *mut u8,
    max_count: usize,
) -> usize {
    if cons.is_null() || out_buf.is_null() {
        return 0;
    }
    let c = unsafe { &mut *cons };
    let mut count = 0;
    while count < max_count && unsafe { c.next_into(out_buf.add(count * c.element_size)) } {
        count += 1;
    }
    count
}

/// Total number of messages this consumer skipped because the producer lapped it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_lapped_count(cons: *const RingConsumerRaw) -> u64 {
    if cons.is_null() {
        return 0;
    }
    unsafe { (*cons).lapped_total }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_close(cons: *mut RingConsumerRaw) {
    if !cons.is_null() {
        drop(unsafe { Box::from_raw(cons) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_create(
    path: *const c_char,
    slot_count: usize,
    value_size: usize,
) -> *mut BlackboardRaw {
    let Some(path_buf) = (unsafe { path_from_c(path) }) else {
        return std::ptr::null_mut();
    };
    if slot_count == 0 || value_size == 0 || slot_count > u32::MAX as usize || value_size > u32::MAX as usize {
        return std::ptr::null_mut();
    }

    let slot_size = (8 + value_size).next_multiple_of(64);
    let total_size = std::mem::size_of::<BlackboardHeader>() + (slot_count * slot_size);

    let Ok(file) = create_backing_file(&path_buf, 0o660, true, total_size as u64) else {
        return std::ptr::null_mut();
    };
    let Ok(mut mmap) = (unsafe { MmapMut::map_mut(&file) }) else {
        return std::ptr::null_mut();
    };

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

    let slots_base = unsafe { mmap.as_mut_ptr().add(std::mem::size_of::<BlackboardHeader>()) };
    for i in 0..slot_count {
        unsafe { slots_base.add(i * slot_size).cast::<AtomicU64>().write(AtomicU64::new(0)) };
    }
    fence(Ordering::Release);
    unsafe { std::ptr::write_volatile(&mut (*header_ptr).magic, BLACKBOARD_MAGIC) };

    Box::into_raw(Box::new(BlackboardRaw {
        _file: file,
        _mmap: mmap,
        _header: header_ptr,
        slots_base,
        slot_size,
        value_size,
        slot_count,
        is_producer: true,
        path: path_buf,
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_attach(
    path: *const c_char,
    value_size: usize,
) -> *mut BlackboardRaw {
    let Some(path_buf) = (unsafe { path_from_c(path) }) else {
        return std::ptr::null_mut();
    };
    if value_size == 0 {
        return std::ptr::null_mut();
    }
    let Ok(file) = OpenOptions::new().read(true).write(true).open(&path_buf) else {
        return std::ptr::null_mut();
    };
    let Ok(mmap) = (unsafe { MmapMut::map_mut(&file) }) else {
        return std::ptr::null_mut();
    };
    if mmap.len() < std::mem::size_of::<BlackboardHeader>() {
        return std::ptr::null_mut();
    }

    let header_ptr = mmap.as_ptr().cast::<BlackboardHeader>();
    let header = unsafe { &*header_ptr };
    let magic = unsafe { std::ptr::read_volatile(&header.magic) };
    fence(Ordering::Acquire);
    if magic != BLACKBOARD_MAGIC || header.version != BLACKBOARD_VERSION {
        return std::ptr::null_mut();
    }
    if header.value_size as usize != value_size {
        return std::ptr::null_mut();
    }

    let slot_count = header.slot_count as usize;
    let slot_size = header.slot_size as usize;
    let end = slot_count
        .checked_mul(slot_size)
        .and_then(|b| b.checked_add(std::mem::size_of::<BlackboardHeader>()));
    if slot_size < 8 + value_size || end.is_none_or(|e| e > mmap.len()) {
        return std::ptr::null_mut();
    }
    let slots_base = unsafe { mmap.as_ptr().add(std::mem::size_of::<BlackboardHeader>()) };

    Box::into_raw(Box::new(BlackboardRaw {
        _file: file,
        _mmap: mmap,
        _header: header_ptr as *mut BlackboardHeader,
        slots_base: slots_base as *mut u8,
        slot_size,
        value_size,
        slot_count,
        is_producer: false,
        path: path_buf,
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_write(
    bb: *mut BlackboardRaw,
    key: usize,
    data: *const u8,
) -> i32 {
    if bb.is_null() || data.is_null() {
        return -1;
    }
    unsafe {
        let b = &mut *bb;
        if key >= b.slot_count {
            return -1;
        }

        let slot_ptr = b.slots_base.add(key * b.slot_size);
        let seqlock = &*slot_ptr.cast::<AtomicU64>();
        let s = seqlock.load(Ordering::Relaxed);
        let write_s = if s % 2 == 0 { s + 1 } else { s + 2 };

        seqlock.store(write_s, Ordering::Relaxed);
        fence(Ordering::Release);
        std::ptr::copy_nonoverlapping(data, slot_ptr.add(8), b.value_size);
        seqlock.store(write_s + 1, Ordering::Release);
    }
    0
}

/// Returns 1 if read, 0 if the key was never written, -1 on invalid arguments, and -2 if
/// the slot stayed mid-write for over 100 ms (writer stalled or crashed).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_read(
    bb: *const BlackboardRaw,
    key: usize,
    out_data: *mut u8,
) -> i32 {
    if bb.is_null() || out_data.is_null() {
        return -1;
    }
    unsafe {
        let b = &*bb;
        if key >= b.slot_count {
            return -1;
        }

        let slot_ptr = b.slots_base.add(key * b.slot_size);
        let seqlock = &*slot_ptr.cast::<AtomicU64>();

        let mut spins = 0u32;
        let mut stalled_since: Option<Instant> = None;
        loop {
            let s1 = seqlock.load(Ordering::Acquire);
            if s1 == 0 {
                return 0;
            }
            if s1 & 1 == 0 {
                std::ptr::copy_nonoverlapping(slot_ptr.add(8), out_data, b.value_size);
                fence(Ordering::Acquire);
                if seqlock.load(Ordering::Relaxed) == s1 {
                    return 1;
                }
            }
            spins = spins.saturating_add(1);
            if spins < 10_000 {
                core::hint::spin_loop();
            } else {
                let since = *stalled_since.get_or_insert_with(Instant::now);
                if since.elapsed() > Duration::from_millis(100) {
                    return -2;
                }
                std::thread::yield_now();
            }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_close(bb: *mut BlackboardRaw) {
    if !bb.is_null() {
        let b = unsafe { Box::from_raw(bb) };
        if b.is_producer {
            let _ = std::fs::remove_file(&b.path);
        }
    }
}
