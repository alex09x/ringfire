use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ringfire::{BlackboardConsumer, BlackboardProducer};

#[derive(Clone, Copy)]
#[repr(C)]
struct SymbolBbo {
    symbol_id: u32,
    bid_px: u64,
    ask_px: u64,
    bid_sz: u64,
    ask_sz: u64,
}

fn bench_blackboard(c: &mut Criterion) {
    let tmp_path = std::env::temp_dir().join("bench_ringfire_blackboard.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let slot_count = 1024;
    let mut producer = BlackboardProducer::<SymbolBbo>::create(&tmp_path, slot_count).unwrap();
    let consumer = BlackboardConsumer::<SymbolBbo>::attach(&tmp_path).unwrap();

    let bbo = SymbolBbo {
        symbol_id: 42,
        bid_px: 82500_00,
        ask_px: 82501_00,
        bid_sz: 1000,
        ask_sz: 800,
    };

    producer.write(42, &bbo).unwrap();

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
    let _ = std::fs::remove_file(&tmp_path);
}

criterion_group!(benches, bench_blackboard);
criterion_main!(benches);
