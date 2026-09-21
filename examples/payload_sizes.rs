use ringfire::{RingConsumer, RingProducer};
use std::time::Instant;

fn bench_size<const N: usize>(capacity: u64, iterations: u64) {
    let tmp_path = format!("/dev/shm/bench_payload_{}.shm", N);
    let _ = std::fs::remove_file(&tmp_path);

    #[derive(Clone, Copy)]
    #[repr(C)]
    struct Payload<const S: usize> {
        data: [u8; S],
    }

    let mut producer = RingProducer::<Payload<N>>::create(&tmp_path, capacity).unwrap();
    let mut consumer = RingConsumer::<Payload<N>>::attach(&tmp_path).unwrap();

    let sample = Payload { data: [0x5A; N] };

    // Warmup
    for _ in 0..10_000 {
        producer.push(&sample);
        let _ = consumer.try_recv();
    }

    // Benchmark Producer Push
    let start = Instant::now();
    for _ in 0..iterations {
        producer.push(&sample);
    }
    let elapsed = start.elapsed();
    let push_ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;
    let push_msg_per_sec = (iterations as f64 / elapsed.as_secs_f64()) / 1_000_000.0;
    let gb_per_sec = (iterations as f64 * N as f64) / (elapsed.as_secs_f64() * 1024.0 * 1024.0 * 1024.0);

    // Benchmark Consumer Drain
    let start = Instant::now();
    let mut recv_count = 0u64;
    while consumer.try_recv().is_some() {
        recv_count += 1;
    }
    let recv_elapsed = start.elapsed();
    let recv_ns_per_op = if recv_count > 0 {
        recv_elapsed.as_nanos() as f64 / recv_count as f64
    } else {
        0.0
    };

    let slot_size = std::mem::size_of::<ringfire::Slot<Payload<N>>>();
    let total_shm_mb = (128 + capacity as usize * slot_size) as f64 / (1024.0 * 1024.0);

    println!(
        "| {:>4} B  | {:>4} B   | {:>7} slots ({:>5.1} MB) | {:>6.2} ns ({:>6.1} M msg/s) | {:>6.2} GB/s | {:>6.2} ns |",
        N, slot_size, capacity, total_shm_mb, push_ns_per_op, push_msg_per_sec, gb_per_sec, recv_ns_per_op
    );

    let _ = std::fs::remove_file(&tmp_path);
}

fn main() {
    let capacity = 131_072; // 128K slots
    let iterations = 2_000_000;

    println!("==========================================================================================================");
    println!(" ringfire Payload Size Sweep Benchmark (AMD Ryzen 9 7950X, Linux /dev/shm)");
    println!(" Buffer Capacity: {} slots", capacity);
    println!("==========================================================================================================");
    println!("| Payload | Slot Size | Buffer RAM Size           | Push Latency & Rate          | Bandwidth   | Recv Latency |");
    println!("|:-------:|:---------:|:-------------------------:|:----------------------------:|:-----------:|:------------:|");

    bench_size::<32>(capacity, iterations);
    bench_size::<64>(capacity, iterations);
    bench_size::<128>(capacity, iterations);
    bench_size::<256>(capacity, iterations);
    bench_size::<512>(capacity, iterations);
    bench_size::<1024>(capacity, iterations);
    println!("==========================================================================================================");
}
