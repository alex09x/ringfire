//! One-way latency and throughput of ring replication over loopback.
//!
//! A producer pushes into a source ring; a `ReplicaServer` streams it to a `Mirror` in the
//! same process; a reader on the mirror ring timestamps arrival. Producer, server, mirror
//! and reader all busy-poll. Both ends share one clock, so the one-way figure is exact.
//!
//! ```text
//! cargo run --release --example replication_latency [-- --paced-us 100 --samples 20000]
//! ```

use std::thread;
use std::time::{Duration, Instant};

use ringfire::replication::{Mirror, MirrorStart, ReplicaServer};
use ringfire::{RingConsumer, RingProducer};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Msg {
    sent_ns: u64,
    seq: u64,
    _pad: [u8; 40],
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut paced_us = 100u64;
    let mut samples = 20_000usize;
    let mut burst = 2_000_000u64;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--paced-us" if i + 1 < args.len() => {
                paced_us = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--samples" if i + 1 < args.len() => {
                samples = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--burst" if i + 1 < args.len() => {
                burst = args[i + 1].parse().unwrap();
                i += 2;
            }
            _ => i += 1,
        }
    }

    let source =
        std::env::temp_dir().join(format!("ringfire_repl_lat_src_{}.shm", std::process::id()));
    let copy =
        std::env::temp_dir().join(format!("ringfire_repl_lat_dst_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Msg>::create(&source, 1 << 16).unwrap();
    let server = ReplicaServer::bind(&source, "127.0.0.1:0")
        .unwrap()
        .spin(true);
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Latest)
        .spin(true)
        .connect(addr, &copy)
        .unwrap();
    let handle = mirror.handle().unwrap();
    let mirror_thread = thread::spawn(move || {
        let _ = mirror.run();
        (mirror.sequence(), mirror.gaps())
    });
    let mut reader = RingConsumer::<Msg>::attach(&copy).unwrap();
    let epoch = Instant::now();
    let now_ns = move || epoch.elapsed().as_nanos() as u64;

    // Paced one-way latency: one message every `paced_us`, like a live feed.
    let pacer = {
        let period = Duration::from_micros(paced_us);
        thread::spawn(move || {
            let mut next = Instant::now();
            for seq in 1..=samples as u64 {
                while Instant::now() < next {
                    core::hint::spin_loop();
                }
                producer.push(&Msg {
                    sent_ns: now_ns(),
                    seq,
                    _pad: [0; 40],
                });
                next += period;
            }
            producer
        })
    };
    let mut lat = Vec::with_capacity(samples);
    while lat.len() < samples {
        if let Some(msg) = reader.try_recv() {
            lat.push(now_ns() - msg.sent_ns);
        } else {
            core::hint::spin_loop();
        }
    }
    let mut producer = pacer.join().unwrap();
    lat.sort_unstable();
    println!(
        "paced ({} us): one-way source ring -> mirror ring, {} samples, {} B slots",
        paced_us,
        samples,
        std::mem::size_of::<Msg>() + 8
    );
    println!(
        "  p50 {:.1} us  p90 {:.1} us  p99 {:.1} us  p99.9 {:.1} us  max {:.1} us",
        percentile(&lat, 0.50) as f64 / 1000.0,
        percentile(&lat, 0.90) as f64 / 1000.0,
        percentile(&lat, 0.99) as f64 / 1000.0,
        percentile(&lat, 0.999) as f64 / 1000.0,
        lat[lat.len() - 1] as f64 / 1000.0
    );

    // Burst throughput: push as fast as possible, measure what the mirror delivers.
    let start = Instant::now();
    let first_seq = samples as u64 + 1;
    let last_seq = first_seq + burst - 1;
    let pusher = thread::spawn(move || {
        for seq in first_seq..=last_seq {
            producer.push(&Msg {
                sent_ns: 0,
                seq,
                _pad: [0; 40],
            });
        }
        (producer, start.elapsed())
    });
    let mut delivered = 0u64;
    let mut last = 0u64;
    while last < last_seq {
        if let Some(msg) = reader.try_recv() {
            delivered += 1;
            last = msg.seq;
        } else {
            core::hint::spin_loop();
        }
    }
    let recv_elapsed = start.elapsed();
    let (_producer, push_elapsed) = pusher.join().unwrap();
    println!(
        "burst: {} pushed in {:.1} ms ({:.1} M/s); mirror delivered {} ({:.1}%) in {:.1} ms ({:.1} M/s), lapped {}",
        burst,
        push_elapsed.as_secs_f64() * 1e3,
        burst as f64 / push_elapsed.as_secs_f64() / 1e6,
        delivered,
        delivered as f64 * 100.0 / burst as f64,
        recv_elapsed.as_secs_f64() * 1e3,
        delivered as f64 / recv_elapsed.as_secs_f64() / 1e6,
        reader.lapped_count()
    );

    handle.shutdown().unwrap();
    let (seq, gaps) = mirror_thread.join().unwrap();
    println!("mirror: last seq {}, gaps {}", seq, gaps);
    let _ = std::fs::remove_file(&copy);
}
