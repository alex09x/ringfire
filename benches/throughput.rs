//! Hot-path costs of one producer and one consumer on a 64-byte message.
//!
//! Receive benches time only the receives: the messages they read are pushed before the
//! timer starts (`support::timed_chunks`), each receive must return the next message in
//! sequence, and the ring never laps the reader. `cargo bench --bench throughput`

mod support;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use ringfire::{FlowControl, RingConsumer, RingConsumerBuilder, RingProducer, RingProducerBuilder};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use support::{AbortOnPanic, TempShm, timed_chunks};

#[derive(Clone, Copy)]
#[repr(C)]
struct Message64 {
    timestamp: u64,
    payload: [u8; 56],
}

/// The message pushed as sequence `seq`: its `timestamp` carries the sequence so the
/// receive side can check it got every message in order.
fn message(seq: u64) -> Message64 {
    Message64 {
        timestamp: seq,
        payload: [0xAA; 56],
    }
}

const CAPACITY: u64 = 65536;
/// Messages pushed per timed chunk: well inside the ring, so the reader is never lapped.
const CHUNK: u64 = 4096;
const BATCH: usize = 32;

fn bench_spmc_throughput(c: &mut Criterion) {
    let _watchdog =
        support::Watchdog::start("bench_spmc_throughput", support::Watchdog::default_limit());
    let mut group = c.benchmark_group("ringfire_throughput");

    {
        let shm = TempShm::new("push");
        let mut producer = RingProducer::<Message64>::create(shm.path(), CAPACITY).unwrap();
        let msg = message(1);
        group.throughput(Throughput::Elements(1));
        group.bench_function("spmc_push", |b| {
            b.iter(|| producer.push(black_box(&msg)));
        });
    }

    {
        let shm = TempShm::new("try_recv");
        let mut producer = RingProducer::<Message64>::create(shm.path(), CAPACITY).unwrap();
        let mut consumer = RingConsumer::<Message64>::attach(shm.path()).unwrap();
        let (mut pushed, mut expected) = (0u64, 1u64);
        group.throughput(Throughput::Elements(1));
        group.bench_function("spmc_try_recv", |b| {
            b.iter_custom(|iters| {
                timed_chunks(
                    iters,
                    CHUNK,
                    |n| {
                        for _ in 0..n {
                            pushed += 1;
                            producer.push(&message(pushed));
                        }
                    },
                    |n| {
                        let mut mismatch = 0u64;
                        for _ in 0..n {
                            match consumer.try_recv() {
                                Some(m) => {
                                    mismatch |= m.timestamp ^ expected;
                                    expected += 1;
                                    black_box(&m);
                                }
                                None => panic!("try_recv: no message at sequence {}", expected),
                            }
                        }
                        assert_eq!(mismatch, 0, "try_recv returned a message out of sequence");
                    },
                )
            });
        });
        assert_eq!(consumer.lapped_count(), 0, "try_recv reader was lapped");
    }

    {
        let shm = TempShm::new("recv_batch");
        let mut producer = RingProducer::<Message64>::create(shm.path(), CAPACITY).unwrap();
        let mut consumer = RingConsumer::<Message64>::attach(shm.path()).unwrap();
        let (mut pushed, mut expected) = (0u64, 1u64);
        let mut buf = [message(0); BATCH];
        // One iteration = one `recv_batch` call returning exactly BATCH messages.
        group.throughput(Throughput::Elements(BATCH as u64));
        group.bench_function(format!("spmc_batch_recv_{}", BATCH), |b| {
            b.iter_custom(|iters| {
                timed_chunks(
                    iters,
                    CHUNK / BATCH as u64,
                    |n| {
                        for _ in 0..n * BATCH as u64 {
                            pushed += 1;
                            producer.push(&message(pushed));
                        }
                    },
                    |n| {
                        let mut mismatch = 0u64;
                        for _ in 0..n {
                            let got = consumer.recv_batch(&mut buf);
                            if got != BATCH {
                                panic!(
                                    "recv_batch returned {} of {} at sequence {}",
                                    got, BATCH, expected
                                );
                            }
                            mismatch |= (buf[0].timestamp ^ expected)
                                | (buf[BATCH - 1].timestamp ^ (expected + BATCH as u64 - 1));
                            expected += BATCH as u64;
                            black_box(&buf);
                        }
                        assert_eq!(mismatch, 0, "recv_batch returned messages out of sequence");
                    },
                )
            });
        });
        assert_eq!(consumer.lapped_count(), 0, "recv_batch reader was lapped");
    }

    group.finish();
}

/// Push with a reader draining on another thread, lossy vs lossless: the difference is
/// the cost of the flow-control gate; the rest is cross-core cache-line traffic. After
/// the bench the reader drains what is left and every pushed message must be accounted
/// for as received or lapped (never lapped in lossless mode).
fn bench_push_with_reader(c: &mut Criterion) {
    let _watchdog =
        support::Watchdog::start("bench_push_with_reader", support::Watchdog::default_limit());
    let mut group = c.benchmark_group("ringfire_throughput");
    group.throughput(Throughput::Elements(1));
    for (name, flow) in [
        ("spmc_push_with_reader_lossy", FlowControl::LossyLatestWins),
        (
            "spmc_push_with_reader_lossless",
            FlowControl::LosslessBackpressure,
        ),
    ] {
        let shm = TempShm::new(name);

        let mut producer = RingProducerBuilder::new(CAPACITY)
            .flow_control(flow)
            .max_readers(4)
            .build::<Message64, _>(shm.path())
            .unwrap();
        let mut consumer = RingConsumerBuilder::<Message64>::new()
            .start_from_head()
            .attach(shm.path())
            .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_reader = stop.clone();
        let reader = std::thread::spawn(move || {
            let _abort = AbortOnPanic("push_with_reader reader");
            let mut buf = [message(0); 64];
            let mut received = 0u64;
            while !stop_reader.load(Ordering::Acquire) {
                let n = consumer.recv_batch(&mut buf);
                received += n as u64;
                black_box(&buf[..n]);
            }
            // Everything was published before `stop`: drain it.
            loop {
                let n = consumer.recv_batch(&mut buf);
                if n == 0 {
                    break;
                }
                received += n as u64;
            }
            (received, consumer.lapped_count())
        });

        let msg = message(1);
        group.bench_function(name, |b| {
            b.iter(|| producer.push(black_box(&msg)));
        });

        stop.store(true, Ordering::Release);
        let (received, lapped) = reader.join().unwrap();
        let pushed = producer.sequence();
        assert!(received > 0, "{}: the reader received nothing", name);
        assert_eq!(
            received + lapped,
            pushed,
            "{}: {} received + {} lapped != {} pushed",
            name,
            received,
            lapped,
            pushed
        );
        if flow == FlowControl::LosslessBackpressure {
            assert_eq!(lapped, 0, "{}: lossless reader was lapped", name);
        }
    }
    group.finish();
}

criterion_group!(benches, bench_spmc_throughput, bench_push_with_reader);
criterion_main!(benches);
