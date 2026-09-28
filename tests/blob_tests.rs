use std::fs::OpenOptions;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ringfire::{
    BlobConsumer, BlobProducer, BlobProducerBuilder, BlobRecvStatus, CycleStamp, LayoutSignature,
    RingConsumer, RingProducer, RingfireError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct OrderBookHeader {
    symbol_id: u32,
    seq_num: u64,
    bids_count: u32,
    asks_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct IncompatibleHeader {
    symbol_id: u64, // Same size (24 bytes) as OrderBookHeader, but completely different types/fields!
    seq_num: u64,
    exchange_flags: u64,
}

#[test]
fn test_blob_producer_consumer_multi_sizes() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_blob_multi.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let mut producer = BlobProducer::<OrderBookHeader>::create(&tmp_path, 128, 512 * 1024).unwrap();
    let mut consumer =
        BlobConsumer::<OrderBookHeader>::attach_with_name(&tmp_path, "market_eval").unwrap();

    let mut meta = OrderBookHeader {
        symbol_id: 0,
        seq_num: 0,
        bids_count: 0,
        asks_count: 0,
    };
    let mut out_buf = vec![0u8; 65536];

    // Empty check
    assert_eq!(consumer.recv(&mut meta, &mut out_buf).unwrap(), None);

    // Test multiple varying payload sizes
    let sizes = [16, 64, 256, 1024, 8192, 32768];
    for (i, &sz) in sizes.iter().enumerate() {
        let sent_meta = OrderBookHeader {
            symbol_id: 42,
            seq_num: (i + 1) as u64,
            bids_count: sz as u32 / 16,
            asks_count: sz as u32 / 16,
        };

        let sent_payload: Vec<u8> = (0..sz).map(|b| ((b * 7 + i) & 0xFF) as u8).collect();
        let seq = producer.push(&sent_meta, &sent_payload).unwrap();
        assert_eq!(seq, (i + 1) as u64);

        let received_len = consumer.recv(&mut meta, &mut out_buf).unwrap().unwrap();
        assert_eq!(received_len, sz);
        assert_eq!(meta, sent_meta);
        assert_eq!(&out_buf[..received_len], &sent_payload[..]);
    }

    // Must be empty again
    assert_eq!(consumer.recv(&mut meta, &mut out_buf).unwrap(), None);
}

#[test]
fn test_blob_zero_copy_push_and_view() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_blob_zerocopy.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let mut producer = BlobProducer::<u32>::create(&tmp_path, 64, 65536).unwrap();
    let mut consumer = BlobConsumer::<u32>::attach(&tmp_path).unwrap();

    let target_len = 1000;
    let (seq, res_code) = producer
        .push_with(&999, target_len, |buf| {
            buf.fill(0x5A);
            42 // Custom return value from closure
        })
        .unwrap();

    assert_eq!(seq, 1);
    assert_eq!(res_code, 42);

    let inspected = consumer
        .view(|meta, slice| {
            assert_eq!(*meta, 999);
            assert_eq!(slice.len(), target_len);
            assert!(slice.iter().all(|&b| b == 0x5A));
            true
        })
        .unwrap()
        .unwrap();

    assert!(inspected);
}

#[test]
fn test_schema_signature_mismatch_detection() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_schema_mismatch.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let _producer = RingProducer::<OrderBookHeader>::create(&tmp_path, 64).unwrap();

    // Consumer with identical schema should attach successfully
    let valid_consumer = RingConsumer::<OrderBookHeader>::attach(&tmp_path);
    assert!(valid_consumer.is_ok());

    // Consumer with altered schema struct must be rejected
    let invalid_consumer = RingConsumer::<IncompatibleHeader>::attach(&tmp_path);
    match invalid_consumer {
        Err(RingfireError::SchemaMismatch {
            expected,
            actual,
            type_name,
        }) => {
            assert_eq!(expected, OrderBookHeader::layout_signature());
            assert_eq!(actual, IncompatibleHeader::layout_signature());
            assert!(type_name.contains("IncompatibleHeader"));
        }
        other => panic!("Expected SchemaMismatch, got {:?}", other),
    }
}

#[test]
fn test_reader_registry_multi_reader_monitoring() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_registry_mon.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let mut producer = BlobProducer::<()>::create(&tmp_path, 128, 65536).unwrap();

    let mut c1 = BlobConsumer::<()>::attach_with_name(&tmp_path, "fast_reader").unwrap();
    let mut c2 = BlobConsumer::<()>::attach_with_name(&tmp_path, "slow_reader").unwrap();

    // Push 20 messages
    for i in 1..=20 {
        producer.push_payload(&[i as u8]).unwrap();
    }

    assert_eq!(producer.sequence(), 20);

    // c1 drains all 20 messages
    let mut buf = [0u8; 16];
    for _ in 1..=20 {
        assert!(c1.recv_payload(&mut buf).unwrap().is_some());
    }

    // c2 only drains 5 messages
    for _ in 1..=5 {
        assert!(c2.recv_payload(&mut buf).unwrap().is_some());
    }

    // c1 cursor is 21, c2 cursor is 6
    assert_eq!(c1.cursor(), 21);
    assert_eq!(c2.cursor(), 6);

    // Min reader seq must reflect the slowest reader (c2 at 6)
    assert_eq!(producer.min_reader_seq(), Some(6));
    // c2 consumed 1..=5, so 6..=20 (15 messages) are still unread
    assert_eq!(producer.reader_lag(), 15);
    assert_eq!(producer.headroom(), 128 - 15);

    let active = producer.active_readers();
    assert_eq!(active.len(), 2);
    let names: Vec<String> = active.iter().map(|r| r.name.clone()).collect();
    assert!(names.contains(&"fast_reader".to_string()));
    assert!(names.contains(&"slow_reader".to_string()));

    // Drop c2
    drop(c2);

    // Now only c1 is active (cursor at 21, lag = 0)
    assert_eq!(producer.min_reader_seq(), Some(21));
    assert_eq!(producer.reader_lag(), 0);
    assert_eq!(producer.headroom(), 128);
}

#[test]
fn test_blob_arena_lapping_behavior() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_blob_lap.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let capacity = 32;
    let arena_capacity = 32 * 1024; // 32 KB arena
    let mut producer = BlobProducer::<u32>::create(&tmp_path, capacity, arena_capacity).unwrap();
    let mut consumer = BlobConsumer::<u32>::attach(&tmp_path).unwrap();

    let chunk = vec![0xABu8; 256];

    // Push 100 messages into a 32-capacity queue, lapping the reader
    for i in 1..=100 {
        producer.push(&(i as u32), &chunk).unwrap();
    }

    let mut meta = 0u32;
    let mut out = vec![0u8; 512];

    let status = consumer.recv_status(&mut meta, &mut out).unwrap();
    match status {
        BlobRecvStatus::Lapped {
            skipped,
            payload_len,
        } => {
            assert_eq!(skipped, 68); // 100 - 32 = 68
            assert_eq!(payload_len, 256);
            assert_eq!(meta, 69);
            assert_eq!(&out[..256], &chunk[..]);
        }
        other => panic!("Expected BlobRecvStatus::Lapped, got {:?}", other),
    }

    assert_eq!(consumer.lapped_count(), 68);
}

#[test]
fn test_cycle_stamp_hardware_rdtsc() {
    let t1 = CycleStamp::now();
    for _ in 0..5000 {
        core::hint::spin_loop();
    }
    let t2 = CycleStamp::now();

    assert!(t2.tsc > t1.tsc);
    let elapsed = t2.diff_cycles(&t1);
    assert!(elapsed > 0);

    // Converted to ns at typical 4.5 GHz Ryzen frequency
    let ns = CycleStamp::cycles_to_ns(elapsed, 4.5);
    assert!(ns > 0.0);
}

#[test]
fn test_blob_consumer_recv_view_and_oversized_payloads() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_blob_extra_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);

    let mut producer = BlobProducer::<u32>::create(&ring_path, 8, 1024).unwrap();
    assert!(producer.min_reader_seq().is_none());

    // 1. Oversized payloads reject with ArenaPayloadTooLarge
    let oversized = vec![0xCC; 2048];
    assert!(producer.push(&1, &oversized).is_err());
    assert!(producer.push_with(&2, 2048, |_| ()).is_err());

    let mut consumer = BlobConsumer::<u32>::attach(&ring_path).unwrap();
    assert_eq!(consumer.cursor(), 1);

    // 2. Push valid items and test recv()
    let payload = b"hello blob";
    producer.push(&10, payload).unwrap();
    assert_eq!(producer.min_reader_seq(), Some(1));

    let mut meta = 0u32;
    let mut buf = vec![0u8; 64];
    assert_eq!(
        consumer.recv(&mut meta, &mut buf).unwrap(),
        Some(payload.len())
    );
    assert_eq!(meta, 10);
    assert_eq!(&buf[..payload.len()], payload);

    // Empty recv returns None
    assert_eq!(consumer.recv(&mut meta, &mut buf).unwrap(), None);

    // 3. Test view()
    producer.push(&20, b"world blob").unwrap();
    let viewed = consumer.view(|m, bytes| (*m, bytes.to_vec())).unwrap();
    assert_eq!(viewed, Some((20, b"world blob".to_vec())));

    let _ = std::fs::remove_file(&ring_path);
}

#[test]
fn test_blob_lapped_during_view_and_skip_overwritten() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_blob_lap_view_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);

    let mut producer = BlobProducer::<u32>::create(&ring_path, 8, 1024).unwrap();
    let mut consumer = BlobConsumer::<u32>::attach(&ring_path).unwrap();

    producer.push(&1, b"first payload").unwrap();

    // Lap the arena during view
    let path_clone = ring_path.clone();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path_clone)
            .unwrap();
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
        let header = unsafe { &*(mmap.as_ptr() as *const ringfire::header::RingHeader) };
        let arena_off = header.arena_offset as usize;
        let arena_header =
            unsafe { &*(mmap.as_ptr().add(arena_off) as *const ringfire::arena::ArenaHeader) };
        // Advance reserved past lap
        arena_header.reserved.fetch_add(4096, Ordering::SeqCst);
    });

    let res = consumer
        .view(|_meta, _bytes| {
            std::thread::sleep(Duration::from_millis(30));
            "done"
        })
        .unwrap();

    let _ = handle.join();
    assert_eq!(res, None); // Discarded due to is_lapped check!

    // Also test skip_overwritten returning 0 (lines 426, 475)
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&ring_path)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &*(mmap.as_ptr() as *const ringfire::header::RingHeader) };
    let slots_ptr = unsafe {
        mmap.as_mut_ptr().add(header.slots_offset as usize)
            as *mut ringfire::header::Slot<ringfire::blob::BlobPacket<u32>>
    };
    unsafe {
        let slot = &mut *slots_ptr.add((consumer.cursor() & 7) as usize);
        slot.seq.store(consumer.cursor() + 50, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);

    let mut meta = 0u32;
    let mut buf = vec![0u8; 64];
    assert_eq!(consumer.recv(&mut meta, &mut buf).unwrap(), None);

    let _ = std::fs::remove_file(&ring_path);
}

#[test]
fn test_blob_layout_disagreement_and_skip_hole() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_blob_mismatch_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);

    let _producer = BlobProducer::<u32>::create(&ring_path, 8, 1024).unwrap();

    // 1. Modify arena capacity to 512 with valid mask 511, leaving header.arena_size at 1024 -> triggers line 343 in blob.rs
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&ring_path)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ringfire::header::RingHeader) };
    let arena_ptr = unsafe { mmap.as_mut_ptr().add(header.arena_offset as usize) as *mut u64 };
    unsafe {
        *arena_ptr = 512;
        *arena_ptr.add(1) = 511;
    }
    drop(mmap);
    drop(file);

    assert!(matches!(
        BlobConsumer::<u32>::attach(&ring_path),
        Err(RingfireError::CorruptLayout(
            "arena header disagrees with ring header"
        ))
    ));
    let _ = std::fs::remove_file(&ring_path);

    // Test map failure on /dev/null -> covers blob.rs line 320
    assert!(BlobConsumer::<u32>::attach("/dev/null").is_err());

    // 2. Test skip_hole in BlobConsumer (sparse ring)
    let path = dir.join(format!("test_blob_sparse_skip_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let mut prod = BlobProducerBuilder::new(64, 4096)
        .max_readers(4)
        .build::<u32, _>(&path)
        .unwrap();
    prod.push(&1, b"first").unwrap();

    // Set FLAG_SPARSE in header
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ringfire::header::RingHeader) };
    header.flags |= ringfire::header::FLAG_SPARSE;
    drop(mmap);
    drop(file);

    let mut cons = BlobConsumer::<u32>::attach_with_name(&path, "sparse_blob_r").unwrap();

    let mut meta = 0u32;
    let mut buf = [0u8; 64];
    assert_eq!(cons.recv(&mut meta, &mut buf).unwrap(), Some(5));

    // Simulate hole: write_seq is 1, cursor is 2.
    // Advance write_seq to 5, write blob 5 into slot 5 with seq 5.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ringfire::header::RingHeader) };
    header.write_seq.store(5, Ordering::SeqCst);
    let slots_offset = header.slots_offset as usize;
    let arena_offset = header.arena_offset as usize;
    let slot_size = std::mem::size_of::<ringfire::header::Slot<ringfire::blob::BlobPacket<u32>>>();

    let arena = unsafe {
        ringfire::arena::PayloadArena::from_ptr(mmap.as_mut_ptr().add(arena_offset)).unwrap()
    };
    let blob_ref = arena.write_blob(b"item5", 0).unwrap();

    let slot5_ptr = unsafe {
        mmap.as_mut_ptr().add(slots_offset + 5 * slot_size)
            as *mut ringfire::header::Slot<ringfire::blob::BlobPacket<u32>>
    };
    unsafe {
        (*slot5_ptr).data = ringfire::blob::BlobPacket { meta: 55, blob_ref };
        (*slot5_ptr).seq.store(5, Ordering::SeqCst);
    }

    // Set slot 2 seq to SLOT_WRITING -> skip_hole sees SLOT_WRITING, returns 0 (line 396)
    let slot2_ptr = unsafe {
        mmap.as_mut_ptr().add(slots_offset + 2 * slot_size)
            as *mut ringfire::header::Slot<ringfire::blob::BlobPacket<u32>>
    };
    unsafe {
        (*slot2_ptr)
            .seq
            .store(ringfire::header::SLOT_WRITING, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);

    for _ in 0..64 {
        let _ = cons.recv(&mut meta, &mut buf);
    }

    // Now set slot 2 seq to 0 (hole) -> skip_hole finds seq 5, updates registry (line 411)
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ringfire::header::RingHeader) };
    let slots_offset = header.slots_offset as usize;
    let slot2_ptr = unsafe {
        mmap.as_mut_ptr().add(slots_offset + 2 * slot_size)
            as *mut ringfire::header::Slot<ringfire::blob::BlobPacket<u32>>
    };
    unsafe {
        (*slot2_ptr).seq.store(0, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);

    let mut got = None;
    for _ in 0..64 {
        if let Ok(Some(len)) = cons.recv(&mut meta, &mut buf) {
            got = Some(len);
            break;
        }
    }
    assert_eq!(got, Some(5));
    assert_eq!(meta, 55);
    assert_eq!(&buf[..5], b"item5");

    // Set write_seq to 0 so write_seq < cons.cursor() -> skip_hole returns 0 (line 390)
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ringfire::header::RingHeader) };
    header.write_seq.store(0, Ordering::SeqCst);
    drop(mmap);
    drop(file);
    for _ in 0..64 {
        let _ = cons.recv(&mut meta, &mut buf);
    }

    // 3. Test BufferTooSmall in recv() -> covers line 536
    let path_buf = dir.join(format!("test_blob_buf_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&path_buf);
    let mut prod_buf = BlobProducer::<u32>::create(&path_buf, 16, 1024).unwrap();
    prod_buf.push(&42, b"a rather large payload").unwrap();
    let mut cons_buf = BlobConsumer::<u32>::attach(&path_buf).unwrap();
    let mut tiny_buf = [0u8; 2];
    assert!(matches!(
        cons_buf.recv(&mut meta, &mut tiny_buf),
        Err(RingfireError::BufferTooSmall { .. })
    ));
    let _ = std::fs::remove_file(&path_buf);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_blob_overwritten_none() {
    let dir = std::env::temp_dir();
    let p_over = dir.join(format!("test_blob_over_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&p_over);
    let mut prod_over = BlobProducer::<u32>::create(&p_over, 16, 1024).unwrap();
    prod_over.push(&1, b"first").unwrap();
    prod_over.push(&2, b"second").unwrap();
    let mut cons_over = BlobConsumer::<u32>::attach(&p_over).unwrap();
    let mut m = 0u32;
    let mut b = [0u8; 32];
    assert_eq!(cons_over.recv(&mut m, &mut b).unwrap(), Some(5)); // cursor now 2
    // Set slot 1 (cursor 2) to seq 3 (ahead of cursor, but oldest is 1 <= cursor 2)
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p_over)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ringfire::header::RingHeader) };
    let slot_size = std::mem::size_of::<ringfire::header::Slot<ringfire::blob::BlobPacket<u32>>>();
    let slot2_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(header.slots_offset as usize + 2 * slot_size)
            as *mut ringfire::header::Slot<ringfire::blob::BlobPacket<u32>>
    };
    unsafe {
        (*slot2_ptr).seq.store(5, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);
    // recv sees Overwritten(5), skip_overwritten returns 0 (line 426), and recv returns Ok(None) (line 475)
    assert_eq!(cons_over.recv(&mut m, &mut b).unwrap(), None);
    let _ = std::fs::remove_file(&p_over);
}

#[test]
fn test_arena_concurrent_reserve_contention() {
    #[repr(align(64))]
    struct AlignedBuf([u8; 8192]);
    let mut buf = AlignedBuf([0u8; 8192]);
    let arena = std::sync::Arc::new(unsafe {
        ringfire::arena::PayloadArena::init(buf.0.as_mut_ptr(), 8192).unwrap()
    });

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let s1 = stop.clone();
    let a1 = arena.clone();
    let t1 = std::thread::spawn(move || {
        while !s1.load(Ordering::Relaxed) {
            let _ = a1.reserve(16, 0);
        }
    });
    let s2 = stop.clone();
    let a2 = arena.clone();
    let t2 = std::thread::spawn(move || {
        while !s2.load(Ordering::Relaxed) {
            let _ = a2.reserve(16, 0);
        }
    });

    std::thread::sleep(Duration::from_millis(15));
    stop.store(true, Ordering::Relaxed);
    let _ = t1.join();
    let _ = t2.join();
}
