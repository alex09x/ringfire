#![allow(clippy::missing_safety_doc, clippy::manual_div_ceil)]

use std::ffi::CStr;
use std::fs::{File, OpenOptions};
use std::os::raw::c_char;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use memmap2::MmapMut;

use crate::header::{
    BlackboardHeader, RingHeader, BLACKBOARD_MAGIC, BLACKBOARD_VERSION,
    FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS, RINGFIRE_MAGIC, RINGFIRE_VERSION,
};
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
    _header: *const RingHeader,
    slots_base: *const u8,
    slot_size: usize,
    element_size: usize,
    mask: u64,
    cursor: u64,
}

pub struct BlackboardRaw {
    _mmap: MmapMut,
    _header: *mut BlackboardHeader,
    slots_base: *mut u8,
    slot_size: usize,
    value_size: usize,
    slot_count: usize,
    is_producer: bool,
    path: PathBuf,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_producer_create(
    path: *const c_char,
    capacity: u64,
    element_size: u32,
) -> *mut RingProducerRaw {
    unsafe {
        if path.is_null() || !capacity.is_power_of_two() || element_size == 0 {
            return std::ptr::null_mut();
        }

        let c_str = CStr::from_ptr(path);
        let path_str = match c_str.to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };
        let path_buf = PathBuf::from(path_str);

        let slot_size = ((8 + element_size as usize + 7) / 8) * 8;
        let total_size = std::mem::size_of::<RingHeader>() + (capacity as usize * slot_size);

        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o660)
            .open(&path_buf)
        {
            Ok(f) => f,
            Err(_) => return std::ptr::null_mut(),
        };

        let fd = file.as_raw_fd();
        if libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) != 0 {
            return std::ptr::null_mut();
        }

        if file.set_len(total_size as u64).is_err() {
            return std::ptr::null_mut();
        }

        let mut mmap = match MmapMut::map_mut(&file) {
            Ok(m) => m,
            Err(_) => return std::ptr::null_mut(),
        };

        let header_ptr = mmap.as_mut_ptr().cast::<RingHeader>();
        header_ptr.write(RingHeader {
            magic: RINGFIRE_MAGIC,
            version: RINGFIRE_VERSION,
            element_size: slot_size as u32,
            capacity,
            mask: capacity - 1,
            write_seq: AtomicU64::new(0),
            claim_seq: AtomicU64::new(0),
            flags: FLAG_POLICY_LATEST_WINS | FLAG_MODE_SPMC,
            futex_word: std::sync::atomic::AtomicU32::new(0),
            waiting_consumers: std::sync::atomic::AtomicU32::new(0),
            _align_pad: 0,
            read_seq: AtomicU64::new(0),
            schema_sig: 0,
            arena_offset: 0,
            arena_size: 0,
            reader_registry_offset: 0,
            reader_registry_count: 0,
            _pad: [0; 24],
        });

        let slots_base = mmap
            .as_mut_ptr()
            .add(std::mem::size_of::<RingHeader>());

        for i in 0..capacity {
            let slot_ptr = slots_base.add(i as usize * slot_size).cast::<AtomicU64>();
            slot_ptr.write(AtomicU64::new(0));
        }

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
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_producer_push(
    prod: *mut RingProducerRaw,
    data: *const u8,
) -> i32 {
    unsafe {
        if prod.is_null() || data.is_null() {
            return -1;
        }

        let p = &mut *prod;
        let idx = (p.seq & p.mask) as usize;
        let slot_ptr = p.slots_base.add(idx * p.slot_size);
        let seq_ptr = slot_ptr.cast::<AtomicU64>();
        let data_ptr = slot_ptr.add(8);

        std::ptr::copy_nonoverlapping(data, data_ptr, p.element_size);
        (*seq_ptr).store(p.seq, Ordering::Release);
        (*p.header).write_seq.store(p.seq, Ordering::Release);
        wake_futex(&*p.header, 1);

        p.seq += 1;
        0
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_producer_close(prod: *mut RingProducerRaw) {
    unsafe {
        if !prod.is_null() {
            drop(Box::from_raw(prod));
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_attach(
    path: *const c_char,
    element_size: u32,
) -> *mut RingConsumerRaw {
    unsafe {
        if path.is_null() || element_size == 0 {
            return std::ptr::null_mut();
        }

        let c_str = CStr::from_ptr(path);
        let path_str = match c_str.to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };

        let file = match OpenOptions::new().read(true).write(true).open(path_str) {
            Ok(f) => f,
            Err(_) => return std::ptr::null_mut(),
        };

        let mmap = match MmapMut::map_mut(&file) {
            Ok(m) => m,
            Err(_) => return std::ptr::null_mut(),
        };

        let header_ptr = mmap.as_ptr().cast::<RingHeader>();
        let header = &*header_ptr;

        if header.magic != RINGFIRE_MAGIC || header.version != RINGFIRE_VERSION {
            return std::ptr::null_mut();
        }

        let slot_size = ((8 + element_size as usize + 7) / 8) * 8;
        if header.element_size as usize != slot_size {
            return std::ptr::null_mut();
        }

        let capacity = header.capacity;
        let mask = header.mask;
        let slots_base = mmap.as_ptr().add(std::mem::size_of::<RingHeader>());

        let current_write = header.write_seq.load(Ordering::Acquire);
        let cursor = if current_write > capacity {
            current_write - capacity + 1
        } else {
            1
        };

        Box::into_raw(Box::new(RingConsumerRaw {
            _mmap: mmap,
            _header: header_ptr,
            slots_base,
            slot_size,
            element_size: element_size as usize,
            mask,
            cursor,
        }))
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_try_recv(
    cons: *mut RingConsumerRaw,
    out_data: *mut u8,
) -> i32 {
    unsafe {
        if cons.is_null() || out_data.is_null() {
            return -1;
        }

        let c = &mut *cons;
        let idx = (c.cursor & c.mask) as usize;
        let slot_ptr = c.slots_base.add(idx * c.slot_size);
        let seq_ptr = slot_ptr.cast::<AtomicU64>();
        let s1 = (*seq_ptr).load(Ordering::Acquire);

        if s1 < c.cursor {
            return 0; // Empty
        }

        if s1 > c.cursor {
            c.cursor = s1; // Lapped
        }

        let data_ptr = slot_ptr.add(8);
        std::ptr::copy_nonoverlapping(data_ptr, out_data, c.element_size);

        let s2 = (*seq_ptr).load(Ordering::Acquire);
        if s1 != s2 {
            c.cursor = s2;
            return 0; // Overwritten during read, retry next time
        }

        c.cursor += 1;
        1 // Success
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_recv_batch(
    cons: *mut RingConsumerRaw,
    out_buf: *mut u8,
    max_count: usize,
) -> usize {
    unsafe {
        if cons.is_null() || out_buf.is_null() || max_count == 0 {
            return 0;
        }

        let c = &mut *cons;
        let mut count = 0;

        while count < max_count {
            let idx = (c.cursor & c.mask) as usize;
            let slot_ptr = c.slots_base.add(idx * c.slot_size);
            let seq_ptr = slot_ptr.cast::<AtomicU64>();
            let s1 = (*seq_ptr).load(Ordering::Acquire);

            if s1 < c.cursor {
                break;
            }

            if s1 > c.cursor {
                c.cursor = s1;
            }

            let data_ptr = slot_ptr.add(8);
            let dst = out_buf.add(count * c.element_size);
            std::ptr::copy_nonoverlapping(data_ptr, dst, c.element_size);

            let s2 = (*seq_ptr).load(Ordering::Acquire);
            if s1 != s2 {
                c.cursor = s2;
                break;
            }

            c.cursor += 1;
            count += 1;
        }

        count
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_consumer_close(cons: *mut RingConsumerRaw) {
    unsafe {
        if !cons.is_null() {
            drop(Box::from_raw(cons));
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_create(
    path: *const c_char,
    slot_count: usize,
    value_size: usize,
) -> *mut BlackboardRaw {
    unsafe {
        if path.is_null() || slot_count == 0 || value_size == 0 {
            return std::ptr::null_mut();
        }

        let c_str = CStr::from_ptr(path);
        let path_str = match c_str.to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };
        let path_buf = PathBuf::from(path_str);

        let raw_slot_size = 8 + value_size;
        let slot_size = ((raw_slot_size + 63) / 64) * 64;
        let total_size = std::mem::size_of::<BlackboardHeader>() + (slot_count * slot_size);

        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o660)
            .open(&path_buf)
        {
            Ok(f) => f,
            Err(_) => return std::ptr::null_mut(),
        };

        if file.set_len(total_size as u64).is_err() {
            return std::ptr::null_mut();
        }

        let mut mmap = match MmapMut::map_mut(&file) {
            Ok(m) => m,
            Err(_) => return std::ptr::null_mut(),
        };

        let header_ptr = mmap.as_mut_ptr().cast::<BlackboardHeader>();
        header_ptr.write(BlackboardHeader {
            magic: BLACKBOARD_MAGIC,
            version: BLACKBOARD_VERSION,
            value_size: value_size as u32,
            slot_size: slot_size as u32,
            slot_count: slot_count as u32,
            _reserved: [0; 2],
            _pad: [0; 88],
        });

        let slots_base = mmap
            .as_mut_ptr()
            .add(std::mem::size_of::<BlackboardHeader>());

        for i in 0..slot_count {
            let seqlock_ptr = slots_base.add(i * slot_size).cast::<AtomicU64>();
            seqlock_ptr.write(AtomicU64::new(0));
        }

        Box::into_raw(Box::new(BlackboardRaw {
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
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_attach(
    path: *const c_char,
    value_size: usize,
) -> *mut BlackboardRaw {
    unsafe {
        if path.is_null() || value_size == 0 {
            return std::ptr::null_mut();
        }

        let c_str = CStr::from_ptr(path);
        let path_str = match c_str.to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        };

        let file = match OpenOptions::new().read(true).write(true).open(path_str) {
            Ok(f) => f,
            Err(_) => return std::ptr::null_mut(),
        };

        let mmap = match MmapMut::map_mut(&file) {
            Ok(m) => m,
            Err(_) => return std::ptr::null_mut(),
        };

        let header_ptr = mmap.as_ptr().cast::<BlackboardHeader>();
        let header = &*header_ptr;

        if header.magic != BLACKBOARD_MAGIC || header.version != BLACKBOARD_VERSION {
            return std::ptr::null_mut();
        }

        if header.value_size as usize != value_size {
            return std::ptr::null_mut();
        }

        let slot_count = header.slot_count as usize;
        let slot_size = header.slot_size as usize;
        let slots_base = mmap
            .as_ptr()
            .add(std::mem::size_of::<BlackboardHeader>());

        Box::into_raw(Box::new(BlackboardRaw {
            _mmap: mmap,
            _header: header_ptr as *mut BlackboardHeader,
            slots_base: slots_base as *mut u8,
            slot_size,
            value_size,
            slot_count,
            is_producer: false,
            path: PathBuf::from(path_str),
        }))
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_write(
    bb: *mut BlackboardRaw,
    key: usize,
    data: *const u8,
) -> i32 {
    unsafe {
        if bb.is_null() || data.is_null() {
            return -1;
        }

        let b = &mut *bb;
        if key >= b.slot_count {
            return -1;
        }

        let slot_ptr = b.slots_base.add(key * b.slot_size);
        let seqlock_ptr = slot_ptr.cast::<AtomicU64>();
        let s = (*seqlock_ptr).load(Ordering::Relaxed);
        let write_s = if s % 2 == 0 { s + 1 } else { s + 2 };

        (*seqlock_ptr).store(write_s, Ordering::Release);
        let val_ptr = slot_ptr.add(8);
        std::ptr::copy_nonoverlapping(data, val_ptr, b.value_size);
        (*seqlock_ptr).store(write_s + 1, Ordering::Release);

        0
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_read(
    bb: *const BlackboardRaw,
    key: usize,
    out_data: *mut u8,
) -> i32 {
    unsafe {
        if bb.is_null() || out_data.is_null() {
            return -1;
        }

        let b = &*bb;
        if key >= b.slot_count {
            return -1;
        }

        let slot_ptr = b.slots_base.add(key * b.slot_size);
        let seqlock_ptr = slot_ptr.cast::<AtomicU64>();
        let val_ptr = slot_ptr.add(8);

        let mut spins = 0;
        loop {
            let s1 = (*seqlock_ptr).load(Ordering::Acquire);
            if s1 == 0 {
                return 0; // Unwritten
            }

            if s1 & 1 != 0 {
                core::hint::spin_loop();
                spins += 1;
                if spins > 10_000 {
                    std::thread::yield_now();
                }
                continue;
            }

            std::ptr::copy_nonoverlapping(val_ptr, out_data, b.value_size);
            let s2 = (*seqlock_ptr).load(Ordering::Acquire);
            if s1 == s2 {
                return 1; // Read successfully
            }

            core::hint::spin_loop();
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ringfire_blackboard_close(bb: *mut BlackboardRaw) {
    unsafe {
        if !bb.is_null() {
            let b = Box::from_raw(bb);
            if b.is_producer {
                let _ = std::fs::remove_file(&b.path);
            }
        }
    }
}
