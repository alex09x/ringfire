use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ringfire::{RingConsumer, RingProducer};

#[derive(Clone, Copy)]
#[repr(C)]
struct Message64 {
    timestamp: u64,
    payload: [u8; 56],
}

fn bench_spmc_throughput(c: &mut Criterion) {
    let tmp_path = std::env::temp_dir().join("bench_ringfire_throughput.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let capacity = 65536;
    let mut producer = RingProducer::<Message64>::create(&tmp_path, capacity).unwrap();
    let mut consumer = RingConsumer::<Message64>::attach(&tmp_path).unwrap();

    let msg = Message64 {
        timestamp: 12345678,
        payload: [0xAA; 56],
    };

    let mut group = c.benchmark_group("ringfire_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("spmc_push", |b| {
        b.iter(|| {
            producer.push(black_box(&msg));
        });
    });

    group.bench_function("spmc_try_recv", |b| {
        // Pre-fill buffer
        for _ in 0..1024 {
            producer.push(&msg);
        }
        b.iter(|| {
            if let Some(m) = consumer.try_recv() {
                black_box(m);
            } else {
                producer.push(&msg);
            }
        });
    });

    group.bench_function("spmc_batch_recv_32", |b| {
        let mut batch_buf = [msg; 32];
        for _ in 0..1024 {
            producer.push(&msg);
        }
        b.iter(|| {
            let n = consumer.recv_batch(&mut batch_buf);
            black_box(n);
            if n == 0 {
                for _ in 0..32 {
                    producer.push(&msg);
                }
            }
        });
    });

    group.finish();
    let _ = std::fs::remove_file(&tmp_path);
}

criterion_group!(benches, bench_spmc_throughput);
criterion_main!(benches);
