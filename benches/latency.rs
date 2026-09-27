//! Round trip of an 8-byte ping between two threads over two rings, both sides busy
//! polling. One iteration = one round trip; exactly one ping is in flight, so every reply
//! must carry the sequence just sent. `cargo bench --bench latency`

mod support;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ringfire::{RingConsumer, RingProducer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use support::{AbortOnPanic, REPLY_TIMEOUT, SpinBound, TempShm};

#[derive(Clone, Copy)]
#[repr(C)]
struct LatencyPing {
    pub seq: u64,
}

fn bench_roundtrip_latency(c: &mut Criterion) {
    let _watchdog = support::Watchdog::start(
        "bench_roundtrip_latency",
        support::Watchdog::default_limit(),
    );
    let fwd = TempShm::new("latency_fwd");
    let rev = TempShm::new("latency_rev");

    let mut prod_fwd = RingProducer::<LatencyPing>::create(fwd.path(), 4096).unwrap();
    let mut prod_rev = RingProducer::<LatencyPing>::create(rev.path(), 4096).unwrap();

    let mut cons_fwd = RingConsumer::<LatencyPing>::attach(fwd.path()).unwrap();
    let mut cons_rev = RingConsumer::<LatencyPing>::attach(rev.path()).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let r_clone = Arc::clone(&running);

    // Echo thread: reads from fwd, writes to rev
    let echo_handle = thread::spawn(move || {
        let _abort = AbortOnPanic("latency echo");
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

            let mut wait = SpinBound::new("ping_pong_rtt reply", REPLY_TIMEOUT);
            let resp = loop {
                if let Some(resp) = cons_rev.try_recv() {
                    break resp;
                }
                wait.spin();
            };
            assert_eq!(resp.seq, ping_seq, "reply out of sequence");
            black_box(resp);
            ping_seq += 1;
        });
    });

    running.store(false, Ordering::Relaxed);
    echo_handle.join().unwrap();
    assert_eq!(cons_rev.lapped_count(), 0, "reply reader was lapped");

    group.finish();
}

criterion_group!(benches, bench_roundtrip_latency);
criterion_main!(benches);
