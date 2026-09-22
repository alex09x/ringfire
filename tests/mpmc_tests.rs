use ringfire::{MpmcProducer, MpmcQueueConsumer, RingConsumer};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct OrderEvent {
    producer_id: u32,
    order_id: u64,
}

#[test]
fn test_mpmc_concurrent_producers() {
    let tmp_path = std::env::temp_dir().join("test_mpmc_prods.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let capacity = 32768;
    let _root_prod = MpmcProducer::<OrderEvent>::create(&tmp_path, capacity).unwrap();

    let num_producers = 4;
    let msgs_per_prod = 5000;
    let total_msgs = num_producers * msgs_per_prod;

    let path_clone = tmp_path.clone();
    let mut prod_handles = Vec::new();

    for pid in 0..num_producers {
        let p = path_clone.clone();
        let handle = thread::spawn(move || {
            let prod = MpmcProducer::<OrderEvent>::attach(&p).unwrap();
            for oid in 1..=msgs_per_prod {
                let ev = OrderEvent {
                    producer_id: pid as u32,
                    order_id: oid as u64,
                };
                prod.push(&ev);
            }
        });
        prod_handles.push(handle);
    }

    // Consumer reads from buffer
    let mut consumer = RingConsumer::<OrderEvent>::attach(&tmp_path).unwrap();
    let mut received_count = 0;
    let mut per_prod_seen: Vec<HashSet<u64>> = (0..num_producers).map(|_| HashSet::new()).collect();

    // Wait for producers to finish
    for handle in prod_handles {
        handle.join().unwrap();
    }

    while received_count < total_msgs {
        if let Some(ev) = consumer.try_recv() {
            let pid = ev.producer_id as usize;
            assert!(pid < num_producers);
            assert!(per_prod_seen[pid].insert(ev.order_id), "Duplicate order_id seen");
            received_count += 1;
        } else {
            core::hint::spin_loop();
        }
    }

    assert_eq!(received_count, total_msgs);
    for seen in per_prod_seen.iter().take(num_producers) {
        assert_eq!(seen.len(), msgs_per_prod);
    }
}

#[test]
fn test_mpmc_competing_worker_consumers() {
    let tmp_path = std::env::temp_dir().join("test_mpmc_workers.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let capacity = 8192;
    let producer = MpmcProducer::<u64>::create(&tmp_path, capacity).unwrap();

    let total_tasks = 4000u64;
    let num_workers = 4;

    let total_processed = Arc::new(AtomicUsize::new(0));
    let mut worker_handles = Vec::new();

    for _ in 0..num_workers {
        let p = tmp_path.clone();
        let counter = Arc::clone(&total_processed);
        let handle = thread::spawn(move || {
            let mut worker = MpmcQueueConsumer::<u64>::attach(&p).unwrap();
            let mut my_count = 0;
            loop {
                if let Some(_task) = worker.try_recv() {
                    counter.fetch_add(1, Ordering::Relaxed);
                    my_count += 1;
                } else {
                    if counter.load(Ordering::Relaxed) >= total_tasks as usize {
                        break;
                    }
                    core::hint::spin_loop();
                }
            }
            my_count
        });
        worker_handles.push(handle);
    }

    // Push tasks
    for task_id in 1..=total_tasks {
        producer.push(&task_id);
    }

    let mut sum_work = 0;
    for handle in worker_handles {
        let count = handle.join().unwrap();
        sum_work += count;
    }

    assert_eq!(sum_work, total_tasks as usize);
    assert_eq!(total_processed.load(Ordering::SeqCst), total_tasks as usize);
}
