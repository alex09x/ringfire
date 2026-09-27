use std::ffi::CString;
use std::fs::OpenOptions;
use std::os::raw::c_char;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::MmapMut;
use ringfire::ffi::*;
use ringfire::header::BlackboardHeader;

fn temp_path(name: &str) -> (PathBuf, CString) {
    let path = std::env::temp_dir().join(format!("ringfire_ffi_{}_{}.shm", name, std::process::id()));
    let _ = std::fs::remove_file(&path);
    let cstr = CString::new(path.to_str().unwrap()).unwrap();
    (path, cstr)
}

#[test]
fn test_producer_create_invalid_args_and_null() {
    let (path, c_path) = temp_path("prod_create_inv");

    // Null path
    let p = unsafe { ringfire_producer_create(ptr::null(), 64, 8) };
    assert!(p.is_null());

    // Invalid UTF-8 path
    let bad_bytes = [0xff, 0xfe, 0x00];
    let p = unsafe { ringfire_producer_create(bad_bytes.as_ptr() as *const c_char, 64, 8) };
    assert!(p.is_null());

    // Capacity not power of two (or 0)
    let p = unsafe { ringfire_producer_create(c_path.as_ptr(), 0, 8) };
    assert!(p.is_null());
    let p = unsafe { ringfire_producer_create(c_path.as_ptr(), 65, 8) };
    assert!(p.is_null());

    // Element size 0
    let p = unsafe { ringfire_producer_create(c_path.as_ptr(), 64, 0) };
    assert!(p.is_null());

    // Invalid directory path
    let bad_dir = CString::new("/nonexistent_dir_ringfire_12345/ring.shm").unwrap();
    let p = unsafe { ringfire_producer_create(bad_dir.as_ptr(), 64, 8) };
    assert!(p.is_null());

    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_producer_push_null_and_close_null() {
    let dummy = 42u64;

    // Push with null producer or data
    assert_eq!(unsafe { ringfire_producer_push(ptr::null_mut(), ptr::null()) }, -1);
    assert_eq!(
        unsafe { ringfire_producer_push(ptr::null_mut(), &dummy as *const u64 as *const u8) },
        -1
    );

    // Close null producer (safe no-op)
    unsafe { ringfire_producer_close(ptr::null_mut()) };

    // Valid producer with null push
    let (path, c_path) = temp_path("prod_push_null");
    let prod = unsafe { ringfire_producer_create(c_path.as_ptr(), 64, 8) };
    assert!(!prod.is_null());

    assert_eq!(unsafe { ringfire_producer_push(prod, ptr::null()) }, -1);
    assert_eq!(
        unsafe { ringfire_producer_push(prod, &dummy as *const u64 as *const u8) },
        0
    );

    unsafe { ringfire_producer_close(prod) };
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_consumer_attach_failures() {
    let (path, c_path) = temp_path("cons_attach_fail");

    // Null path
    let c = unsafe { ringfire_consumer_attach(ptr::null(), 8) };
    assert!(c.is_null());

    // Invalid UTF-8 path
    let bad_bytes = [0xff, 0xfe, 0x00];
    let c = unsafe { ringfire_consumer_attach(bad_bytes.as_ptr() as *const c_char, 8) };
    assert!(c.is_null());

    // Element size 0
    let c = unsafe { ringfire_consumer_attach(c_path.as_ptr(), 0) };
    assert!(c.is_null());

    // Missing file
    let c = unsafe { ringfire_consumer_attach(c_path.as_ptr(), 8) };
    assert!(c.is_null());

    // Corrupt file (not a valid ring)
    std::fs::write(&path, b"short corrupt file content").unwrap();
    let c = unsafe { ringfire_consumer_attach(c_path.as_ptr(), 8) };
    assert!(c.is_null());
    let _ = std::fs::remove_file(&path);

    // Mismatched element size
    let prod = unsafe { ringfire_producer_create(c_path.as_ptr(), 64, 8) };
    assert!(!prod.is_null());
    let c = unsafe { ringfire_consumer_attach(c_path.as_ptr(), 16) };
    assert!(c.is_null());

    unsafe { ringfire_producer_close(prod) };
    let _ = std::fs::remove_file(&path);

    // Close null consumer (safe no-op)
    unsafe { ringfire_consumer_close(ptr::null_mut()) };
}

#[test]
fn test_consumer_null_checks() {
    let mut out = 0u64;

    assert_eq!(unsafe { ringfire_consumer_try_recv(ptr::null_mut(), ptr::null_mut()) }, -1);
    assert_eq!(
        unsafe { ringfire_consumer_try_recv(ptr::null_mut(), &mut out as *mut u64 as *mut u8) },
        -1
    );
    assert_eq!(unsafe { ringfire_consumer_recv_batch(ptr::null_mut(), ptr::null_mut(), 5) }, 0);
    assert_eq!(
        unsafe {
            ringfire_consumer_recv_batch(ptr::null_mut(), &mut out as *mut u64 as *mut u8, 5)
        },
        0
    );
    assert_eq!(unsafe { ringfire_consumer_lapped_count(ptr::null()) }, 0);

    let (path, c_path) = temp_path("cons_null_checks");
    let prod = unsafe { ringfire_producer_create(c_path.as_ptr(), 64, 8) };
    let cons = unsafe { ringfire_consumer_attach(c_path.as_ptr(), 8) };
    assert!(!cons.is_null());

    assert_eq!(unsafe { ringfire_consumer_try_recv(cons, ptr::null_mut()) }, -1);
    assert_eq!(unsafe { ringfire_consumer_recv_batch(cons, ptr::null_mut(), 5) }, 0);

    unsafe {
        ringfire_consumer_close(cons);
        ringfire_producer_close(prod);
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_producer_consumer_lifecycle_and_batch() {
    let (path, c_path) = temp_path("prod_cons_lifecycle");
    let prod = unsafe { ringfire_producer_create(c_path.as_ptr(), 64, 8) };
    assert!(!prod.is_null());
    let cons = unsafe { ringfire_consumer_attach(c_path.as_ptr(), 8) };
    assert!(!cons.is_null());

    assert_eq!(unsafe { ringfire_consumer_lapped_count(cons) }, 0);

    let mut out = 0u64;
    assert_eq!(
        unsafe { ringfire_consumer_try_recv(cons, &mut out as *mut u64 as *mut u8) },
        0
    );

    let mut batch_buf = [0u64; 16];
    assert_eq!(
        unsafe {
            ringfire_consumer_recv_batch(cons, batch_buf.as_mut_ptr() as *mut u8, 16)
        },
        0
    );

    // Push 10 messages
    for i in 1..=10u64 {
        assert_eq!(
            unsafe { ringfire_producer_push(prod, &i as *const u64 as *const u8) },
            0
        );
    }

    // Single receive
    assert_eq!(
        unsafe { ringfire_consumer_try_recv(cons, &mut out as *mut u64 as *mut u8) },
        1
    );
    assert_eq!(out, 1);

    // Batch receive 4 items: should get 2, 3, 4, 5
    let count = unsafe {
        ringfire_consumer_recv_batch(cons, batch_buf.as_mut_ptr() as *mut u8, 4)
    };
    assert_eq!(count, 4);
    assert_eq!(&batch_buf[..4], &[2, 3, 4, 5]);

    // Batch receive rest
    let count = unsafe {
        ringfire_consumer_recv_batch(cons, batch_buf.as_mut_ptr() as *mut u8, 10)
    };
    assert_eq!(count, 5);
    assert_eq!(&batch_buf[..5], &[6, 7, 8, 9, 10]);

    // Caught up
    assert_eq!(
        unsafe { ringfire_consumer_try_recv(cons, &mut out as *mut u64 as *mut u8) },
        0
    );

    unsafe {
        ringfire_consumer_close(cons);
        ringfire_producer_close(prod);
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_consumer_lapping() {
    let (path, c_path) = temp_path("cons_lapping");
    // Small ring of capacity 8
    let prod = unsafe { ringfire_producer_create(c_path.as_ptr(), 8, 8) };
    let cons = unsafe { ringfire_consumer_attach(c_path.as_ptr(), 8) };

    // Push 1 message, read it
    let val1 = 100u64;
    unsafe { ringfire_producer_push(prod, &val1 as *const u64 as *const u8) };
    let mut out = 0u64;
    assert_eq!(
        unsafe { ringfire_consumer_try_recv(cons, &mut out as *mut u64 as *mut u8) },
        1
    );
    assert_eq!(out, 100);

    // Now push 30 messages so capacity 8 is completely overwritten
    for i in 2..=31u64 {
        unsafe { ringfire_producer_push(prod, &i as *const u64 as *const u8) };
    }

    // Consumer try_recv should skip overwritten messages and receive the oldest retained message
    let res = unsafe { ringfire_consumer_try_recv(cons, &mut out as *mut u64 as *mut u8) };
    assert_eq!(res, 1);
    assert_eq!(out, 24); // oldest retained: 31 - 8 + 1 = 24

    let lapped = unsafe { ringfire_consumer_lapped_count(cons) };
    assert!(lapped > 0);

    unsafe {
        ringfire_consumer_close(cons);
        ringfire_producer_close(prod);
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_blackboard_create_attach_failures() {
    let (path, c_path) = temp_path("bb_attach_fail");

    // Null path
    let bb = unsafe { ringfire_blackboard_create(ptr::null(), 8, 16) };
    assert!(bb.is_null());

    // Invalid UTF-8 path
    let bad_bytes = [0xff, 0xfe, 0x00];
    let bb = unsafe { ringfire_blackboard_create(bad_bytes.as_ptr() as *const c_char, 8, 16) };
    assert!(bb.is_null());

    // Slot count 0
    let bb = unsafe { ringfire_blackboard_create(c_path.as_ptr(), 0, 16) };
    assert!(bb.is_null());

    // Value size 0
    let bb = unsafe { ringfire_blackboard_create(c_path.as_ptr(), 8, 0) };
    assert!(bb.is_null());

    // Bad directory path
    let bad_dir = CString::new("/nonexistent_dir_bb_12345/bb.shm").unwrap();
    let bb = unsafe { ringfire_blackboard_create(bad_dir.as_ptr(), 8, 16) };
    assert!(bb.is_null());

    // Close null blackboard (safe no-op)
    unsafe { ringfire_blackboard_close(ptr::null_mut()) };

    // Attach failures: null path
    let bb = unsafe { ringfire_blackboard_attach(ptr::null(), 16) };
    assert!(bb.is_null());

    // Attach failures: invalid UTF-8
    let bb = unsafe { ringfire_blackboard_attach(bad_bytes.as_ptr() as *const c_char, 16) };
    assert!(bb.is_null());

    // Attach failures: value size 0
    let bb = unsafe { ringfire_blackboard_attach(c_path.as_ptr(), 0) };
    assert!(bb.is_null());

    // Attach failures: missing file
    let bb = unsafe { ringfire_blackboard_attach(c_path.as_ptr(), 16) };
    assert!(bb.is_null());

    // Short file (< sizeof(BlackboardHeader))
    std::fs::write(&path, [0u8; 10]).unwrap();
    let bb = unsafe { ringfire_blackboard_attach(c_path.as_ptr(), 16) };
    assert!(bb.is_null());

    // Bad magic
    std::fs::write(&path, [0x55u8; 256]).unwrap();
    let bb = unsafe { ringfire_blackboard_attach(c_path.as_ptr(), 16) };
    assert!(bb.is_null());
    let _ = std::fs::remove_file(&path);

    // Slot size smaller than 8 + value_size
    let (p_bad, c_bad) = temp_path("bb_bad_slotsize");
    let bb_hdr = BlackboardHeader {
        magic: ringfire::header::BLACKBOARD_MAGIC,
        version: ringfire::header::BLACKBOARD_VERSION,
        value_size: 16,
        slot_size: 8, // < 8 + 16 = 24
        slot_count: 4,
        _reserved: [0; 2],
        _pad: [0; 88],
    };
    std::fs::write(&p_bad, unsafe {
        std::slice::from_raw_parts(&bb_hdr as *const _ as *const u8, std::mem::size_of::<BlackboardHeader>())
    }).unwrap();
    assert!(unsafe { ringfire_blackboard_attach(c_bad.as_ptr(), 16) }.is_null());
    let _ = std::fs::remove_file(&p_bad);

    // Mismatched value size
    let bb_prod = unsafe { ringfire_blackboard_create(c_path.as_ptr(), 8, 16) };
    assert!(!bb_prod.is_null());
    let bb_cons = unsafe { ringfire_blackboard_attach(c_path.as_ptr(), 32) };
    assert!(bb_cons.is_null());

    unsafe { ringfire_blackboard_close(bb_prod) };
}

#[test]
fn test_blackboard_read_write_lifecycle_and_consumer_close() {
    let (path, c_path) = temp_path("bb_read_write");
    let bb_prod = unsafe { ringfire_blackboard_create(c_path.as_ptr(), 4, 16) };
    assert!(!bb_prod.is_null());

    let val = [0x42u8; 16];
    let mut out = [0u8; 16];

    // Write null checks
    assert_eq!(unsafe { ringfire_blackboard_write(ptr::null_mut(), 0, ptr::null()) }, -1);
    assert_eq!(unsafe { ringfire_blackboard_write(bb_prod, 0, ptr::null()) }, -1);
    assert_eq!(unsafe { ringfire_blackboard_write(ptr::null_mut(), 0, val.as_ptr()) }, -1);
    assert_eq!(unsafe { ringfire_blackboard_write(bb_prod, 4, val.as_ptr()) }, -1); // key >= slot_count

    // Read null checks
    assert_eq!(unsafe { ringfire_blackboard_read(ptr::null(), 0, ptr::null_mut()) }, -1);
    assert_eq!(unsafe { ringfire_blackboard_read(bb_prod, 0, ptr::null_mut()) }, -1);
    assert_eq!(unsafe { ringfire_blackboard_read(ptr::null(), 0, out.as_mut_ptr()) }, -1);
    assert_eq!(unsafe { ringfire_blackboard_read(bb_prod, 4, out.as_mut_ptr()) }, -1); // key >= slot_count

    // Unwritten slot
    assert_eq!(unsafe { ringfire_blackboard_read(bb_prod, 1, out.as_mut_ptr()) }, 0);

    // Write slot 1
    assert_eq!(unsafe { ringfire_blackboard_write(bb_prod, 1, val.as_ptr()) }, 0);

    // Read slot 1 from producer handle
    assert_eq!(unsafe { ringfire_blackboard_read(bb_prod, 1, out.as_mut_ptr()) }, 1);
    assert_eq!(out, val);

    // Attach consumer handle
    let bb_cons = unsafe { ringfire_blackboard_attach(c_path.as_ptr(), 16) };
    assert!(!bb_cons.is_null());
    let mut out2 = [0u8; 16];
    assert_eq!(unsafe { ringfire_blackboard_read(bb_cons, 1, out2.as_mut_ptr()) }, 1);
    assert_eq!(out2, val);

    // Close consumer: must NOT remove file
    unsafe { ringfire_blackboard_close(bb_cons) };
    assert!(path.exists());

    // Close producer: MUST remove file
    unsafe { ringfire_blackboard_close(bb_prod) };
    assert!(!path.exists());
}

#[test]
fn test_blackboard_timeout_and_odd_sequence_recovery() {
    let (path, c_path) = temp_path("bb_timeout_odd");
    let bb_prod = unsafe { ringfire_blackboard_create(c_path.as_ptr(), 4, 8) };
    assert!(!bb_prod.is_null());

    let val = 12345u64;
    assert_eq!(
        unsafe { ringfire_blackboard_write(bb_prod, 0, &val as *const u64 as *const u8) },
        0
    );

    // Tamper slot 0 seqlock word to 1 (odd number simulating mid-write crash)
    let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header_size = std::mem::size_of::<BlackboardHeader>();
    let seqlock_ptr = unsafe { mmap.as_mut_ptr().add(header_size) as *mut AtomicU64 };
    unsafe { (*seqlock_ptr).store(1, Ordering::SeqCst) };

    // Reader should timeout and return -2
    let mut out = 0u64;
    let res = unsafe { ringfire_blackboard_read(bb_prod, 0, &mut out as *mut u64 as *mut u8) };
    assert_eq!(res, -2);

    // Writer should recover from odd seqlock (s % 2 != 0 branch) and succeed
    let val2 = 99999u64;
    assert_eq!(
        unsafe { ringfire_blackboard_write(bb_prod, 0, &val2 as *const u64 as *const u8) },
        0
    );

    // Now reader should see valid updated value
    let res = unsafe { ringfire_blackboard_read(bb_prod, 0, &mut out as *mut u64 as *mut u8) };
    assert_eq!(res, 1);
    assert_eq!(out, val2);

    unsafe { ringfire_blackboard_close(bb_prod) };
}
