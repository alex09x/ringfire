use ringfire::{BlackboardConsumer, BlackboardProducer};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct OrderbookLevel {
    price: u64,
    size: u64,
    order_count: u32,
    _pad: u32,
    checksum: u64,
}

impl OrderbookLevel {
    fn new(price: u64, size: u64, order_count: u32) -> Self {
        let checksum = price ^ size ^ (order_count as u64);
        Self {
            price,
            size,
            order_count,
            _pad: 0,
            checksum,
        }
    }

    fn is_valid(&self) -> bool {
        self.checksum == (self.price ^ self.size ^ (self.order_count as u64))
    }
}

#[test]
fn test_blackboard_tear_free_concurrent_reads_and_writes() {
    let tmp_path = std::env::temp_dir().join("test_bb_tear_free.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let num_symbols = 16;
    let mut producer = BlackboardProducer::<OrderbookLevel>::create(&tmp_path, num_symbols).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let reads_done = Arc::new(AtomicU64::new(0));
    let num_readers = 4;
    let mut reader_handles = Vec::new();

    for rid in 0..num_readers {
        let p = tmp_path.clone();
        let r_flag = Arc::clone(&running);
        let r_count = Arc::clone(&reads_done);
        let handle = thread::spawn(move || {
            let consumer = BlackboardConsumer::<OrderbookLevel>::attach(&p).unwrap();
            let mut read_ops = 0u64;
            while r_flag.load(Ordering::Relaxed) {
                let symbol_id = (read_ops as usize + rid) % num_symbols;
                if let Some(level) = consumer.read(symbol_id).unwrap() {
                    assert!(
                        level.is_valid(),
                        "Torn read detected on symbol {}: {:?}",
                        symbol_id,
                        level
                    );
                }
                read_ops += 1;
                r_count.fetch_add(1, Ordering::Relaxed);
            }
            read_ops
        });
        reader_handles.push(handle);
    }

    // Heavy writes across all symbols; keep writing until the readers (which may start
    // late on a loaded machine) have overlapped with enough writes.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut iter = 0usize;
    while iter < 50_000 || (reads_done.load(Ordering::Relaxed) <= 50_000 && std::time::Instant::now() < deadline) {
        iter += 1;
        let symbol_id = iter % num_symbols;
        let level = OrderbookLevel::new(iter as u64 * 10, iter as u64 * 5, (iter % 100) as u32);
        producer.write(symbol_id, &level).unwrap();
    }

    running.store(false, Ordering::Relaxed);

    let mut total_reads = 0;
    for handle in reader_handles {
        total_reads += handle.join().unwrap();
    }

    println!("Verified {} concurrent tear-free blackboard reads", total_reads);
    assert!(total_reads > 50_000);
}
