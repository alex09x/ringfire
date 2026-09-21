use ringfire::{CleanupMode, RingConsumer, RingProducer, RingProducerBuilder};
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct TradeMsg {
    id: u64,
    price: u64,
    qty: u32,
    side: u8,
}

#[test]
fn test_spmc_broadcast_to_multiple_consumers() {
    let tmp_path = std::env::temp_dir().join("test_spmc_broadcast.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let capacity = 4096;
    let mut producer = RingProducer::<TradeMsg>::create(&tmp_path, capacity).unwrap();

    let num_consumers = 4;
    let msg_count = 2000;

    let consumers: Vec<_> = (0..num_consumers)
        .map(|_| RingConsumer::<TradeMsg>::attach(&tmp_path).unwrap())
        .collect();

    // Spawn consumer threads
    let mut handles = Vec::new();
    for (cid, mut consumer) in consumers.into_iter().enumerate() {
        let handle = thread::spawn(move || {
            let mut received = 0;
            let mut last_id = 0;
            while received < msg_count {
                if let Some(msg) = consumer.try_recv() {
                    assert_eq!(msg.id, last_id + 1, "Consumer {} sequence discontinuity", cid);
                    assert_eq!(msg.price, (last_id + 1) * 100);
                    last_id = msg.id;
                    received += 1;
                } else {
                    core::hint::spin_loop();
                }
            }
            received
        });
        handles.push(handle);
    }

    // Publish messages
    for i in 1..=msg_count {
        let msg = TradeMsg {
            id: i,
            price: i * 100,
            qty: 10,
            side: if i % 2 == 0 { b'B' } else { b'S' },
        };
        producer.push(&msg);
    }

    for handle in handles {
        let count = handle.join().unwrap();
        assert_eq!(count, msg_count);
    }
}

#[test]
fn test_exclusive_producer_file_locking() {
    let tmp_path = std::env::temp_dir().join("test_spmc_flock.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let p1 = RingProducer::<u64>::create(&tmp_path, 1024).unwrap();

    // Attempting to create another producer on the same path must fail with ProducerAlreadyExists
    let p2_res = RingProducer::<u64>::create(&tmp_path, 1024);
    assert!(p2_res.is_err());

    drop(p1);

    // After p1 drops and releases flock, we can create p3
    let p3 = RingProducer::<u64>::create(&tmp_path, 1024);
    assert!(p3.is_ok());
}

#[test]
fn test_cleanup_mode_unlink_vs_persistent() {
    let tmp_unlink = std::env::temp_dir().join("test_cleanup_unlink.shm");
    let tmp_persist = std::env::temp_dir().join("test_cleanup_persist.shm");
    let _ = std::fs::remove_file(&tmp_unlink);
    let _ = std::fs::remove_file(&tmp_persist);

    {
        let _p = RingProducerBuilder::new(1024)
            .cleanup_mode(CleanupMode::UnlinkOnDrop)
            .build::<u64, _>(&tmp_unlink)
            .unwrap();
        assert!(tmp_unlink.exists());
    }
    // Should be unlinked after drop
    assert!(!tmp_unlink.exists());

    {
        let _p = RingProducerBuilder::new(1024)
            .cleanup_mode(CleanupMode::Persistent)
            .build::<u64, _>(&tmp_persist)
            .unwrap();
        assert!(tmp_persist.exists());
    }
    // Should persist after drop
    assert!(tmp_persist.exists());
    let _ = std::fs::remove_file(&tmp_persist);
}
