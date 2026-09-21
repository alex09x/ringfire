use ringfire::{
    BlobConsumer, BlobProducer, BlobRecvStatus, CycleStamp, LayoutSignature, RingConsumer,
    RingProducer, RingfireError,
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

    let mut producer =
        BlobProducer::<OrderBookHeader>::create(&tmp_path, 128, 512 * 1024).unwrap();
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
    assert_eq!(producer.reader_lag(), 14); // 20 - 6 = 14 messages behind
    assert_eq!(producer.headroom(), 128 - 14);

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
