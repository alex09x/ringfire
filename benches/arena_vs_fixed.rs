use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ringfire::{BlobConsumer, BlobProducer, RingConsumer, RingProducer};

fn bench_fixed_vs_arena(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_throughput");

    // 1. Fixed Slots: 32B, 64B, 256B, 1KB
    {
        let path = std::env::temp_dir().join("bench_fixed_32b.shm");
        let _ = std::fs::remove_file(&path);
        let mut producer = RingProducer::<[u8; 32]>::create(&path, 65536).unwrap();
        let payload = [0x5Au8; 32];

        group.throughput(Throughput::Bytes(32));
        group.bench_function(BenchmarkId::new("fixed_slot", "32B"), |b| {
            b.iter(|| {
                producer.push(black_box(&payload));
            });
        });
    }

    {
        let path = std::env::temp_dir().join("bench_fixed_64b.shm");
        let _ = std::fs::remove_file(&path);
        let mut producer = RingProducer::<[u8; 64]>::create(&path, 65536).unwrap();
        let payload = [0x5Au8; 64];

        group.throughput(Throughput::Bytes(64));
        group.bench_function(BenchmarkId::new("fixed_slot", "64B"), |b| {
            b.iter(|| {
                producer.push(black_box(&payload));
            });
        });
    }

    {
        let path = std::env::temp_dir().join("bench_fixed_256b.shm");
        let _ = std::fs::remove_file(&path);
        let mut producer = RingProducer::<[u8; 256]>::create(&path, 65536).unwrap();
        let payload = [0x5Au8; 256];

        group.throughput(Throughput::Bytes(256));
        group.bench_function(BenchmarkId::new("fixed_slot", "256B"), |b| {
            b.iter(|| {
                producer.push(black_box(&payload));
            });
        });
    }

    {
        let path = std::env::temp_dir().join("bench_fixed_1kb.shm");
        let _ = std::fs::remove_file(&path);
        let mut producer = RingProducer::<[u8; 1024]>::create(&path, 16384).unwrap();
        let payload = [0x5Au8; 1024];

        group.throughput(Throughput::Bytes(1024));
        group.bench_function(BenchmarkId::new("fixed_slot", "1KB"), |b| {
            b.iter(|| {
                producer.push(black_box(&payload));
            });
        });
    }

    // 2. PayloadArena Blobs: 32B, 64B, 256B, 1KB, 8KB, 64KB
    let sizes = [32, 64, 256, 1024, 8192, 65536];
    for &sz in &sizes {
        let path = std::env::temp_dir().join(format!("bench_arena_{}b.shm", sz));
        let _ = std::fs::remove_file(&path);

        let arena_cap = 64 * 1024 * 1024; // 64 MB arena
        let mut producer = BlobProducer::<()>::create(&path, 65536, arena_cap).unwrap();
        let payload = vec![0xAAu8; sz];

        let label = if sz < 1024 {
            format!("{}B", sz)
        } else {
            format!("{}KB", sz / 1024)
        };

        group.throughput(Throughput::Bytes(sz as u64));
        group.bench_function(BenchmarkId::new("payload_arena", label), |b| {
            b.iter(|| {
                producer.push_payload(black_box(&payload)).unwrap();
            });
        });
    }

    group.finish();

    // 3. Receive & Drain Throughput Comparison
    let mut recv_group = c.benchmark_group("recv_throughput");

    // Fixed 64B try_recv
    {
        let path = std::env::temp_dir().join("bench_recv_fixed_64b.shm");
        let _ = std::fs::remove_file(&path);
        let mut producer = RingProducer::<[u8; 64]>::create(&path, 65536).unwrap();
        let mut consumer = RingConsumer::<[u8; 64]>::attach(&path).unwrap();
        let payload = [0x5Au8; 64];

        for _ in 0..32768 {
            producer.push(&payload);
        }

        recv_group.throughput(Throughput::Bytes(64));
        recv_group.bench_function(BenchmarkId::new("fixed_slot", "64B"), |b| {
            b.iter(|| {
                if let Some(msg) = consumer.try_recv() {
                    black_box(msg);
                }
            });
        });
    }

    // Arena 64B recv
    {
        let path = std::env::temp_dir().join("bench_recv_arena_64b.shm");
        let _ = std::fs::remove_file(&path);
        let mut producer = BlobProducer::<()>::create(&path, 65536, 16 * 1024 * 1024).unwrap();
        let mut consumer = BlobConsumer::<()>::attach(&path).unwrap();
        let payload = [0x5Au8; 64];

        for _ in 0..32768 {
            producer.push_payload(&payload).unwrap();
        }

        let mut out = [0u8; 64];
        recv_group.throughput(Throughput::Bytes(64));
        recv_group.bench_function(BenchmarkId::new("payload_arena_copy", "64B"), |b| {
            b.iter(|| {
                if let Ok(Some(n)) = consumer.recv_payload(&mut out) {
                    black_box(n);
                }
            });
        });
    }

    // Arena 1KB recv vs view
    {
        let path = std::env::temp_dir().join("bench_recv_arena_1kb.shm");
        let _ = std::fs::remove_file(&path);
        let mut producer = BlobProducer::<()>::create(&path, 65536, 64 * 1024 * 1024).unwrap();
        let mut consumer = BlobConsumer::<()>::attach(&path).unwrap();
        let payload = vec![0x5Au8; 1024];

        for _ in 0..16384 {
            producer.push_payload(&payload).unwrap();
        }

        let mut out = vec![0u8; 1024];
        recv_group.throughput(Throughput::Bytes(1024));
        recv_group.bench_function(BenchmarkId::new("payload_arena_copy", "1KB"), |b| {
            b.iter(|| {
                if let Ok(Some(n)) = consumer.recv_payload(&mut out) {
                    black_box(n);
                }
            });
        });

        recv_group.bench_function(BenchmarkId::new("payload_arena_view_inplace", "1KB"), |b| {
            b.iter(|| {
                let _ = consumer.view(|_, slice| {
                    black_box(slice.len());
                });
            });
        });
    }

    recv_group.finish();
}

criterion_group!(benches, bench_fixed_vs_arena);
criterion_main!(benches);
