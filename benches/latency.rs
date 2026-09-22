use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ringfire::{RingConsumer, RingProducer};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

#[derive(Clone, Copy)]
#[repr(C)]
struct LatencyPing {
    pub seq: u64,
}

fn bench_roundtrip_latency(c: &mut Criterion) {
    let tmp_path_fwd = std::env::temp_dir().join("bench_latency_fwd.shm");
    let tmp_path_rev = std::env::temp_dir().join("bench_latency_rev.shm");
    let _ = std::fs::remove_file(&tmp_path_fwd);
    let _ = std::fs::remove_file(&tmp_path_rev);

    let mut prod_fwd = RingProducer::<LatencyPing>::create(&tmp_path_fwd, 4096).unwrap();
    let mut prod_rev = RingProducer::<LatencyPing>::create(&tmp_path_rev, 4096).unwrap();

    let mut cons_fwd = RingConsumer::<LatencyPing>::attach(&tmp_path_fwd).unwrap();
    let mut cons_rev = RingConsumer::<LatencyPing>::attach(&tmp_path_rev).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let r_clone = Arc::clone(&running);

    // Echo thread: reads from fwd, writes to rev
    let echo_handle = thread::spawn(move || {
        while r_clone.load(Ordering::Relaxed) {
            if let Some(ping) = cons_fwd.try_recv() {
                prod_rev.push(&ping);
            } else {
                core::hint::spin_loop();
            }
        }
    });

    let mut group = c.benchmark_group("ringfire_latency");

    let mut ping_seq = 1u64;
    group.bench_function("ping_pong_rtt", |b| {
        b.iter(|| {
            let msg = LatencyPing { seq: ping_seq };
            prod_fwd.push(black_box(&msg));

            loop {
                if let Some(resp) = cons_rev.try_recv()
                    && resp.seq == ping_seq
                {
                    black_box(resp);
                    break;
                }
                core::hint::spin_loop();
            }
            ping_seq += 1;
        });
    });

    running.store(false, Ordering::Relaxed);
    let _ = echo_handle.join();

    group.finish();
    let _ = std::fs::remove_file(&tmp_path_fwd);
    let _ = std::fs::remove_file(&tmp_path_rev);
}

criterion_group!(benches, bench_roundtrip_latency);
criterion_main!(benches);
