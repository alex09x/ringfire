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

#[test]
fn test_consumer_start_mode_latest_and_head() {
    let tmp_path = std::env::temp_dir().join(format!("test_spmc_modes_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&tmp_path);

    let mut producer = RingProducer::<u64>::create(&tmp_path, 1024).unwrap();
    // Publish 10 items
    for i in 1..=10 {
        producer.push(&i);
    }

    // 1. Latest mode: jumps directly to the latest published item (10), then reads subsequent items
    let mut cons_latest = RingConsumer::<u64>::attach_latest(&tmp_path).unwrap();
    assert_eq!(cons_latest.try_recv(), Some(10));
    assert_eq!(cons_latest.try_recv(), None);

    // 2. Head mode: strictly waits for future items (arriving after attach)
    let mut cons_head = RingConsumer::<u64>::builder()
        .start_from_head()
        .attach(&tmp_path)
        .unwrap();
    assert_eq!(cons_head.try_recv(), None);

    // Push new items 11 and 12
    producer.push(&11);
    producer.push(&12);

    assert_eq!(cons_latest.try_recv(), Some(11));
    assert_eq!(cons_latest.try_recv(), Some(12));

    assert_eq!(cons_head.try_recv(), Some(11));
    assert_eq!(cons_head.try_recv(), Some(12));

    let _ = std::fs::remove_file(&tmp_path);
}

#[test]
fn test_consumer_start_mode_oldest() {
    let tmp_path = std::env::temp_dir().join(format!("test_spmc_oldest_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&tmp_path);

    let mut producer = RingProducer::<u64>::create(&tmp_path, 1024).unwrap();
    for i in 1..=10 {
        producer.push(&i);
    }

    // Oldest mode (default for attach) replays from item 1
    let mut cons = RingConsumer::<u64>::attach(&tmp_path).unwrap();
    for i in 1..=10 {
        assert_eq!(cons.try_recv(), Some(i));
    }
    assert_eq!(cons.try_recv(), None);

    let _ = std::fs::remove_file(&tmp_path);
}

#[test]
fn test_shm_offset_checkpoint_crash_and_resume() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_spmc_resume_ring_{}.shm", std::process::id()));
    let offset_path = dir.join(format!("test_spmc_resume_offset_{}.offset", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);
    let _ = std::fs::remove_file(&offset_path);

    let mut producer = RingProducer::<u64>::create(&ring_path, 1024).unwrap();
    for i in 1..=50 {
        producer.push(&i);
    }

    // Consumer 1: processes items 1..=25 and commits offset in shared memory
    {
        let mut cons1 = RingConsumer::<u64>::builder()
            .offset_shm(&offset_path)
            .consumer_name("worker_a")
            .start_from_oldest()
            .attach(&ring_path)
            .unwrap();

        for i in 1..=25 {
            assert_eq!(cons1.try_recv(), Some(i));
        }
        assert_eq!(cons1.last_processed_sequence(), 25);
        cons1.commit_offset().unwrap();
        // cons1 dropped (simulating process exit/crash)
    }

    // Consumer 2: starts with same offset file. Must resume strictly from 26!
    {
        let mut cons2 = RingConsumer::<u64>::builder()
            .offset_shm(&offset_path)
            .consumer_name("worker_a")
            .attach(&ring_path)
            .unwrap();

        for i in 26..=50 {
            assert_eq!(cons2.try_recv(), Some(i), "Discontinuity during resume at {}", i);
        }
        assert_eq!(cons2.try_recv(), None);
        assert_eq!(cons2.last_processed_sequence(), 50);
        cons2.commit_offset().unwrap();
    }

    let _ = std::fs::remove_file(&ring_path);
    let _ = std::fs::remove_file(&offset_path);
}

#[test]
fn test_shm_offset_lapping_recovery() {
    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_spmc_lap_rec_ring_{}.shm", std::process::id()));
    let offset_path = dir.join(format!("test_spmc_lap_rec_offset_{}.offset", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);
    let _ = std::fs::remove_file(&offset_path);

    let capacity = 32;
    let mut producer = RingProducer::<u64>::create(&ring_path, capacity).unwrap();
    for i in 1..=10 {
        producer.push(&i);
    }

    // Commit offset at sequence 5
    {
        let mut cons1 = RingConsumer::<u64>::builder()
            .offset_shm(&offset_path)
            .consumer_name("slow_worker")
            .start_from_oldest()
            .attach(&ring_path)
            .unwrap();

        for i in 1..=5 {
            assert_eq!(cons1.try_recv(), Some(i));
        }
        cons1.commit_offset().unwrap();
    }

    // Producer laps consumer by writing up to 100 in capacity 32 buffer
    for i in 11..=100 {
        producer.push(&i);
    }

    // Surviving range in buffer is: 100 - 32 + 1 = 69..=100.
    // Consumer committed at 5, wants 6. But 6 was overwritten!
    // Consumer must detect lapping and catch up to oldest available (69)
    {
        let mut cons2 = RingConsumer::<u64>::builder()
            .offset_shm(&offset_path)
            .consumer_name("slow_worker")
            .attach(&ring_path)
            .unwrap();

        assert_eq!(cons2.lapped_count(), 69 - 6); // 63 messages skipped
        assert_eq!(cons2.try_recv(), Some(69));
        for i in 70..=100 {
            assert_eq!(cons2.try_recv(), Some(i));
        }
        assert_eq!(cons2.try_recv(), None);
    }

    let _ = std::fs::remove_file(&ring_path);
    let _ = std::fs::remove_file(&offset_path);
}

#[test]
fn test_flow_control_lossless_backpressure() {
    use ringfire::{FlowControl, RingfireError};

    let dir = std::env::temp_dir();
    let ring_path = dir.join(format!("test_spmc_backpressure_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&ring_path);

    let capacity = 16;
    let mut producer = RingProducer::<u64>::create_with_options(
        &ring_path,
        RingProducerBuilder::new(capacity)
            .flow_control(FlowControl::LosslessBackpressure)
            .max_readers(8),
    )
    .unwrap();

    assert_eq!(producer.flow_control(), FlowControl::LosslessBackpressure);
    assert_eq!(producer.headroom(), capacity);

    // Attach consumer
    let mut consumer = RingConsumer::<u64>::builder()
        .consumer_name("careful_reader")
        .start_from_oldest()
        .attach(&ring_path)
        .unwrap();

    assert!(consumer.registration().is_some());
    assert_eq!(producer.registry().unwrap().active_readers(0).len(), 1);

    // Push 16 items to fill the buffer exactly
    for i in 1..=16 {
        producer.push(&i);
    }

    assert_eq!(producer.headroom(), 0);
    assert_eq!(producer.reader_lag(), 16);

    // 17th item should fail with BackpressureBufferFull via try_push
    let res = producer.try_push(&17);
    match res {
        Err(RingfireError::BackpressureBufferFull) => {}
        other => panic!("Expected BackpressureBufferFull, got {:?}", other),
    }

    // Spawn thread to drain first 8 messages from consumer
    let drain_handle = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(20));
        let mut drained = Vec::new();
        for _ in 0..8 {
            if let Some(item) = consumer.try_recv() {
                drained.push(item);
            }
        }
        (consumer, drained)
    });

    // Blocking push(17) unblocks as soon as the reader drains its first item
    producer.push(&17);

    let (mut consumer, drained) = drain_handle.join().unwrap();
    // 8 drained, 17 published: 9 unread
    assert_eq!(producer.headroom(), 7);
    assert_eq!(drained.len(), 8);
    assert_eq!(drained, vec![1, 2, 3, 4, 5, 6, 7, 8]);

    // Drain remainder from consumer: 9..=17
    let mut rest = Vec::new();
    for _ in 9..=17 {
        if let Some(item) = consumer.try_recv() {
            rest.push(item);
        }
    }
    assert_eq!(rest, vec![9, 10, 11, 12, 13, 14, 15, 16, 17]);
    assert_eq!(consumer.lapped_count(), 0); // Zero dropped messages under backpressure

    // Drop consumer; dead reader slot is reclaimed and headroom returns to full capacity
    drop(consumer);
    assert_eq!(producer.headroom(), capacity);

    let _ = std::fs::remove_file(&ring_path);
}
