//! Fixed-size slots vs the variable-length payload arena, push and receive.
//!
//! Every receive bench has its own ring and times only the receives: the messages it
//! reads are pushed before the timer starts (`support::timed_chunks`), each receive must
//! return the next message (its sequence is in the payload's first 8 bytes), and neither
//! the ring nor the arena ever laps the reader. `cargo bench --bench arena_vs_fixed`

mod support;

use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use ringfire::{BlobConsumer, BlobProducer, RingConsumer, RingProducer};
use support::{TempShm, timed_chunks};

fn size_label(sz: usize) -> String {
    if sz < 1024 {
        format!("{}B", sz)
    } else {
        format!("{}KB", sz / 1024)
    }
}

/// Payload of `len` bytes whose first 8 bytes are `seq` (little endian).
fn stamp(buf: &mut [u8], seq: u64) {
    buf[..8].copy_from_slice(&seq.to_le_bytes());
}

fn seq_of(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().unwrap())
}

fn bench_fixed_push<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>, capacity: u64) {
    let shm = TempShm::new(&format!("fixed_push_{}", N));
    let mut producer = RingProducer::<[u8; N]>::create(shm.path(), capacity).unwrap();
    let payload = [0x5Au8; N];
    group.throughput(Throughput::Bytes(N as u64));
    group.bench_function(BenchmarkId::new("fixed_slot", size_label(N)), |b| {
        b.iter(|| producer.push(black_box(&payload)));
    });
}

fn bench_arena_push(group: &mut BenchmarkGroup<'_, WallTime>, sz: usize) {
    let shm = TempShm::new(&format!("arena_push_{}", sz));
    let arena_cap = 64 * 1024 * 1024;
    let mut producer = BlobProducer::<()>::create(shm.path(), 65536, arena_cap).unwrap();
    let payload = vec![0xAAu8; sz];
    group.throughput(Throughput::Bytes(sz as u64));
    group.bench_function(BenchmarkId::new("payload_arena", size_label(sz)), |b| {
        b.iter(|| producer.push_payload(black_box(&payload)).unwrap());
    });
}

fn bench_push(c: &mut Criterion) {
    let _watchdog = support::Watchdog::start("bench_push", support::Watchdog::default_limit());
    let mut group = c.benchmark_group("push_throughput");
    bench_fixed_push::<32>(&mut group, 65536);
    bench_fixed_push::<64>(&mut group, 65536);
    bench_fixed_push::<256>(&mut group, 65536);
    bench_fixed_push::<1024>(&mut group, 16384);
    for sz in [32, 64, 256, 1024, 8192, 65536] {
        bench_arena_push(&mut group, sz);
    }
    group.finish();
}

/// Messages per timed chunk: at most half the ring and half the arena.
const RING_CAPACITY: u64 = 65536;
const ARENA_CAPACITY: usize = 64 * 1024 * 1024;

fn chunk_for(payload: usize) -> u64 {
    (RING_CAPACITY / 2)
        .min((ARENA_CAPACITY / 2 / payload) as u64)
        .min(8192)
}

fn bench_fixed_recv(group: &mut BenchmarkGroup<'_, WallTime>) {
    const N: usize = 64;
    let shm = TempShm::new("fixed_recv_64");
    let mut producer = RingProducer::<[u8; N]>::create(shm.path(), RING_CAPACITY).unwrap();
    let mut consumer = RingConsumer::<[u8; N]>::attach(shm.path()).unwrap();
    let (mut pushed, mut expected) = (0u64, 1u64);
    let mut payload = [0x5Au8; N];
    group.throughput(Throughput::Bytes(N as u64));
    group.bench_function(BenchmarkId::new("fixed_slot", size_label(N)), |b| {
        b.iter_custom(|iters| {
            timed_chunks(
                iters,
                chunk_for(N),
                |n| {
                    for _ in 0..n {
                        pushed += 1;
                        stamp(&mut payload, pushed);
                        producer.push(&payload);
                    }
                },
                |n| {
                    let mut mismatch = 0u64;
                    for _ in 0..n {
                        match consumer.try_recv() {
                            Some(msg) => {
                                mismatch |= seq_of(&msg) ^ expected;
                                expected += 1;
                                black_box(&msg);
                            }
                            None => panic!("fixed try_recv: no message at sequence {}", expected),
                        }
                    }
                    assert_eq!(
                        mismatch, 0,
                        "fixed try_recv returned a message out of sequence"
                    );
                },
            )
        });
    });
    assert_eq!(consumer.lapped_count(), 0, "fixed reader was lapped");
}

/// Blob receive of `sz`-byte payloads, copying them out (`view == false`) or reading them
/// in place inside the arena (`view == true`).
fn bench_arena_recv(group: &mut BenchmarkGroup<'_, WallTime>, sz: usize, view: bool) {
    let shm = TempShm::new(&format!("arena_recv_{}_{}", sz, view));
    let mut producer =
        BlobProducer::<()>::create(shm.path(), RING_CAPACITY, ARENA_CAPACITY).unwrap();
    let mut consumer = BlobConsumer::<()>::attach(shm.path()).unwrap();
    let (mut pushed, mut expected) = (0u64, 1u64);
    let mut payload = vec![0x5Au8; sz];
    let mut out = vec![0u8; sz];
    let name = if view {
        "payload_arena_view_inplace"
    } else {
        "payload_arena_copy"
    };
    group.throughput(Throughput::Bytes(sz as u64));
    group.bench_function(BenchmarkId::new(name, size_label(sz)), |b| {
        b.iter_custom(|iters| {
            timed_chunks(
                iters,
                chunk_for(sz),
                |n| {
                    for _ in 0..n {
                        pushed += 1;
                        stamp(&mut payload, pushed);
                        producer.push_payload(&payload).unwrap();
                    }
                },
                |n| {
                    let mut mismatch = 0u64;
                    for _ in 0..n {
                        let got = if view {
                            // Reads the sequence out of the arena in place; the slice
                            // itself is handed to black_box so the access is not elided.
                            consumer.view(|_, bytes| {
                                black_box(bytes);
                                (seq_of(bytes), bytes.len())
                            })
                        } else {
                            consumer
                                .recv_payload(&mut out)
                                .map(|r| r.map(|len| (seq_of(black_box(&out)), len)))
                        };
                        match got {
                            Ok(Some((seq, len))) => {
                                mismatch |= (seq ^ expected) | (len ^ sz) as u64;
                                expected += 1;
                            }
                            other => {
                                panic!("{}: expected message {}, got {:?}", name, expected, other)
                            }
                        }
                    }
                    assert_eq!(
                        mismatch, 0,
                        "{}: message out of sequence or wrong length",
                        name
                    );
                },
            )
        });
    });
    assert_eq!(consumer.lapped_count(), 0, "{} reader was lapped", name);
}

fn bench_recv(c: &mut Criterion) {
    let _watchdog = support::Watchdog::start("bench_recv", support::Watchdog::default_limit());
    let mut group = c.benchmark_group("recv_throughput");
    bench_fixed_recv(&mut group);
    bench_arena_recv(&mut group, 64, false);
    bench_arena_recv(&mut group, 1024, false);
    bench_arena_recv(&mut group, 1024, true);
    group.finish();
}

criterion_group!(benches, bench_push, bench_recv);
criterion_main!(benches);
