//! # ringfire
//!
//! Ultra-low-latency, zero-copy lock-free Inter-Process Communication (IPC)
//! ring buffer and shared memory bus for Linux.
//!
//! Designed as the high-performance cross-process counterpart to `rapidfire`,
//! operating over memory-mapped `/dev/shm` files with 128-byte cache-line aligned headers,
//! atomic release/acquire synchronization, `LatestWins` lossy overflow handling,
//! configurable wait strategies (BusySpin, YieldBackoff, Futex 0% CPU idle),
//! and an O(1) seqlock-backed Blackboard state table.

pub mod blackboard;
pub mod error;
pub mod ffi;
pub mod header;
pub mod mpmc;
pub mod spmc;
pub mod wait;

#[cfg(feature = "tokio")]
pub mod async_ring;

// Re-export primary types
pub use blackboard::{BlackboardConsumer, BlackboardProducer};
pub use error::{Result, RingfireError};
pub use header::{
    BlackboardHeader, BlackboardSlot, RingHeader, Slot, BLACKBOARD_MAGIC, BLACKBOARD_VERSION,
    FLAG_MODE_MPMC, FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS, RINGFIRE_MAGIC, RINGFIRE_VERSION,
};
pub use mpmc::{MpmcProducer, MpmcQueueConsumer};
pub use spmc::{CleanupMode, RecvStatus, RingConsumer, RingProducer, RingProducerBuilder};
pub use wait::{BusySpin, FutexWait, WaitStrategy, YieldBackoff};

#[cfg(feature = "tokio")]
pub use async_ring::AsyncRingConsumer;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    struct TestTrade {
        time_ns: u64,
        px: u64,
        sz: u64,
        side: u8,
    }

    #[test]
    fn test_ring_buffer_push_and_recv() {
        let tmp_path = std::env::temp_dir().join("test_ringfire_spmc.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let mut producer = RingProducer::<TestTrade>::create(&tmp_path, 1024).unwrap();
        let mut consumer = RingConsumer::<TestTrade>::attach(&tmp_path).unwrap();

        assert_eq!(consumer.try_recv(), None);

        let t1 = TestTrade {
            time_ns: 100,
            px: 81200,
            sz: 15,
            side: b'B',
        };
        let t2 = TestTrade {
            time_ns: 200,
            px: 81205,
            sz: 20,
            side: b'S',
        };

        producer.push(&t1);
        producer.push(&t2);

        assert_eq!(consumer.try_recv(), Some(t1));
        assert_eq!(consumer.try_recv(), Some(t2));
        assert_eq!(consumer.try_recv(), None);
    }

    #[test]
    fn test_ring_buffer_batch_recv() {
        let tmp_path = std::env::temp_dir().join("test_ringfire_batch.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let mut producer = RingProducer::<u64>::create(&tmp_path, 1024).unwrap();
        let mut consumer = RingConsumer::<u64>::attach(&tmp_path).unwrap();

        let items: Vec<u64> = (1..=50).collect();
        producer.push_batch(&items);

        let mut buf = [0u64; 32];
        let n1 = consumer.recv_batch(&mut buf);
        assert_eq!(n1, 32);
        assert_eq!(&buf[..], &(1..=32).collect::<Vec<u64>>()[..]);

        let n2 = consumer.recv_batch(&mut buf);
        assert_eq!(n2, 18);
        assert_eq!(&buf[..18], &(33..=50).collect::<Vec<u64>>()[..]);

        assert_eq!(consumer.recv_batch(&mut buf), 0);
    }

    #[test]
    fn test_ring_buffer_latest_wins_lapping() {
        let tmp_path = std::env::temp_dir().join("test_ringfire_lap.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let capacity = 64;
        let mut producer = RingProducer::<u64>::create(&tmp_path, capacity).unwrap();
        let mut consumer = RingConsumer::<u64>::attach(&tmp_path).unwrap();

        // Write 150 items into a capacity 64 buffer (lapping consumer twice)
        for i in 1..=150 {
            producer.push(&i);
        }

        // Consumer reads: slot 1 (cursor 1 & 63 = 1) was overwritten with sequence 129 (1, 65, 129)
        let status = consumer.recv_status();
        match status {
            RecvStatus::Lapped { skipped, item } => {
                assert_eq!(skipped, 128);
                assert_eq!(item, 129);
            }
            _ => panic!("Expected Lapped status"),
        }

        assert_eq!(consumer.lapped_count(), 128);

        // Subsequent reads drain from 130 up to 150 in sequence
        for expected in 130..=150 {
            assert_eq!(consumer.try_recv(), Some(expected));
        }
        assert_eq!(consumer.try_recv(), None);
    }

    #[test]
    fn test_blackboard_read_write() {
        let tmp_path = std::env::temp_dir().join("test_ringfire_bb.shm");
        let _ = std::fs::remove_file(&tmp_path);

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[repr(C)]
        struct BboSnapshot {
            bid_px: u64,
            ask_px: u64,
            bid_sz: u64,
            ask_sz: u64,
        }

        let mut producer = BlackboardProducer::<BboSnapshot>::create(&tmp_path, 256).unwrap();
        let consumer = BlackboardConsumer::<BboSnapshot>::attach(&tmp_path).unwrap();

        assert_eq!(consumer.read(0).unwrap(), None);

        let bbo_btc = BboSnapshot {
            bid_px: 82100_00,
            ask_px: 82100_50,
            bid_sz: 500,
            ask_sz: 300,
        };

        producer.write(1, &bbo_btc).unwrap();

        let read_val = consumer.read(1).unwrap().unwrap();
        assert_eq!(read_val, bbo_btc);
    }

    #[test]
    fn test_wait_strategy_yield_and_futex() {
        let tmp_path = std::env::temp_dir().join("test_ringfire_wait.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let mut producer = RingProducer::<u64>::create(&tmp_path, 1024).unwrap();
        let mut consumer = RingConsumer::<u64>::attach(&tmp_path).unwrap();

        let handle = std::thread::spawn(move || {
            let mut wait = FutexWait::default();
            consumer.recv_blocking(&mut wait)
        });

        std::thread::sleep(std::time::Duration::from_millis(10));
        let val = 424242u64;
        producer.push(&val);

        let res = handle.join().unwrap();
        assert_eq!(res, val);
    }

    #[test]
    fn test_mpmc_producers() {
        let tmp_path = std::env::temp_dir().join("test_ringfire_mpmc.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let p1 = MpmcProducer::<u64>::create(&tmp_path, 1024).unwrap();
        let p2 = MpmcProducer::<u64>::attach(&tmp_path).unwrap();
        let mut consumer = RingConsumer::<u64>::attach(&tmp_path).unwrap();

        p1.push(&100);
        p2.push(&200);

        let v1 = consumer.try_recv().unwrap();
        let v2 = consumer.try_recv().unwrap();

        assert_eq!(v1 + v2, 300);
    }

    #[tokio::test]
    async fn test_tokio_async_consumer() {
        let tmp_path = std::env::temp_dir().join("test_tokio_async.shm");
        let _ = std::fs::remove_file(&tmp_path);

        let mut producer = RingProducer::<u64>::create(&tmp_path, 1024).unwrap();
        let mut async_consumer = AsyncRingConsumer::<u64>::attach(&tmp_path).unwrap();

        // Spawn a background cooperative counter task on the Tokio runtime
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c_clone = counter.clone();
        let bg_task = tokio::spawn(async move {
            for _ in 0..100 {
                c_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });

        // Spawn async reader task
        let reader_task = tokio::spawn(async move {
            let mut received = Vec::new();
            for _ in 0..5 {
                let msg = async_consumer.recv().await;
                received.push(msg);
            }
            received
        });

        // Push messages with small delay
        for val in 1..=5 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            producer.push(&val);
        }

        let msgs = reader_task.await.unwrap();
        assert_eq!(msgs, vec![1, 2, 3, 4, 5]);

        // Verify background task made continuous progress (not starved by the reader!)
        bg_task.await.unwrap();
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 100);
    }
}
