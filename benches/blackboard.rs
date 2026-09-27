//! Uncontended O(1) blackboard read and write of one slot (no concurrent writer while
//! reading). `cargo bench --bench blackboard`

mod support;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ringfire::{BlackboardConsumer, BlackboardProducer};
use support::TempShm;

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C)]
struct SymbolBbo {
    symbol_id: u32,
    bid_px: u64,
    ask_px: u64,
    bid_sz: u64,
    ask_sz: u64,
}

fn bench_blackboard(c: &mut Criterion) {
    let shm = TempShm::new("blackboard");

    let slot_count = 1024;
    let mut producer = BlackboardProducer::<SymbolBbo>::create(shm.path(), slot_count).unwrap();
    let consumer = BlackboardConsumer::<SymbolBbo>::attach(shm.path()).unwrap();

    let bbo = SymbolBbo {
        symbol_id: 42,
        bid_px: 8_250_000,
        ask_px: 8_250_100,
        bid_sz: 1000,
        ask_sz: 800,
    };

    producer.write(42, &bbo).unwrap();
    // The read bench must measure a hit that returns the written value.
    assert_eq!(consumer.read(42).unwrap(), Some(bbo));

    let mut group = c.benchmark_group("ringfire_blackboard");

    group.bench_function("seqlock_read_o1", |b| {
        b.iter(|| {
            let val = consumer.read(black_box(42)).unwrap();
            black_box(val);
        });
    });

    group.bench_function("seqlock_write_o1", |b| {
        b.iter(|| {
            producer.write(black_box(42), black_box(&bbo)).unwrap();
        });
    });

    group.finish();
    assert_eq!(consumer.read(42).unwrap(), Some(bbo));
}

criterion_group!(benches, bench_blackboard);
criterion_main!(benches);
