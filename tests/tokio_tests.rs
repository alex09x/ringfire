use ringfire::{AsyncRingConsumer, RingProducer};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct AsyncTrade {
    seq: u64,
    price: u64,
    qty: u32,
    side: u8,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tokio_multi_task_cooperative_concurrency() {
    let tmp_path = std::env::temp_dir().join("test_tokio_coop.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let capacity = 16384;
    let mut producer = RingProducer::<AsyncTrade>::create(&tmp_path, capacity).unwrap();
    let mut consumer = AsyncRingConsumer::<AsyncTrade>::attach(&tmp_path).unwrap();

    let total_messages = 20_000u64;

    // Background HTTP/WS simulation task on Tokio: ensures consumer doesn't block worker threads
    let other_tasks_processed = Arc::new(AtomicUsize::new(0));
    let other_tasks_clone = Arc::clone(&other_tasks_processed);

    let bg_handle = tokio::spawn(async move {
        for _ in 0..500 {
            tokio::time::sleep(Duration::from_micros(100)).await;
            other_tasks_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Reader task draining via async batch recv
    let reader_handle = tokio::spawn(async move {
        let mut total_received = 0u64;
        let mut batch_buf = [AsyncTrade {
            seq: 0,
            price: 0,
            qty: 0,
            side: 0,
        }; 64];

        let mut expected_seq = 1u64;
        while total_received < total_messages {
            let count = consumer.recv_batch(&mut batch_buf).await;
            for trade in batch_buf.iter().take(count) {
                assert_eq!(trade.seq, expected_seq);
                expected_seq += 1;
                total_received += 1;
            }
        }
        total_received
    });

    // Producer publishes in bursts
    for s in 1..=total_messages {
        let trade = AsyncTrade {
            seq: s,
            price: 8_500_000 + (s % 1000),
            qty: 10,
            side: if s % 2 == 0 { b'B' } else { b'S' },
        };
        producer.push(&trade);

        if s % 2000 == 0 {
            tokio::time::sleep(Duration::from_micros(200)).await;
        }
    }

    let received = reader_handle.await.unwrap();
    assert_eq!(received, total_messages);

    bg_handle.await.unwrap();
    let bg_count = other_tasks_processed.load(Ordering::SeqCst);
    println!("Simulated concurrent Tokio tasks completed: {}", bg_count);
    assert!(bg_count >= 500);
}
