//! Round-trip latency of ring replication between two hosts.
//!
//! Each side publishes a ring and mirrors the other side's ring, so the round trip is
//! two network hops plus four ring hand-offs. Only the pinger's clock is used, so the
//! figure is exact; one-way latency is about half of it.
//!
//! ```text
//! host B:  replication_pingpong --role ponger --bind 0.0.0.0:7401 --peer hostA:7400
//! host A:  replication_pingpong --role pinger --bind 0.0.0.0:7400 --peer hostB:7401 \
//!              [--samples 20000] [--paced-us 100] [--dir /dev/shm]
//! ```
//!
//! Start the ponger first; both sides retry the connection to their peer for a while.

use std::path::PathBuf;
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

fn connect_with_retry(peer: &str, path: &PathBuf) -> Mirror {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match Mirror::builder()
            .start(MirrorStart::Latest)
            .spin(true)
            .connect(peer, path)
        {
            Ok(mirror) => return mirror,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "could not connect to {}: {}",
                    peer,
                    e
                );
                thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut role = String::new();
    let mut bind = String::new();
    let mut peer = String::new();
    let mut samples = 20_000usize;
    let mut paced_us = 100u64;
    let mut dir = PathBuf::from("/dev/shm");
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--role" => role = args[i + 1].clone(),
            "--bind" => bind = args[i + 1].clone(),
            "--peer" => peer = args[i + 1].clone(),
            "--samples" => samples = args[i + 1].parse().unwrap(),
            "--paced-us" => paced_us = args[i + 1].parse().unwrap(),
            "--dir" => dir = PathBuf::from(&args[i + 1]),
            _ => {}
        }
        i += 2;
    }
    assert!(
        !role.is_empty() && !bind.is_empty() && !peer.is_empty(),
        "--role, --bind and --peer are required"
    );

    let out_path = dir.join(format!("ringfire_pingpong_{}_out.shm", role));
    let in_path = dir.join(format!("ringfire_pingpong_{}_in.shm", role));
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(&in_path);

    let mut out = RingProducer::<Msg>::create(&out_path, 1 << 16).unwrap();
    let server = ReplicaServer::bind(&out_path, bind.as_str())
        .unwrap()
        .spin(true);
    eprintln!(
        "{}: serving {} on {}",
        role,
        out_path.display(),
        server.local_addr().unwrap()
    );
    server.spawn().unwrap();

    let mut mirror = connect_with_retry(&peer, &in_path);
    eprintln!("{}: mirroring {} -> {}", role, peer, in_path.display());
    let handle = mirror.handle().unwrap();
    let mirror_thread = thread::spawn(move || {
        let _ = mirror.run();
        mirror.gaps()
    });
    let mut input = RingConsumer::<Msg>::attach(&in_path).unwrap();

    match role.as_str() {
        "ponger" => {
            // Echo every message back with its timestamp untouched, forever.
            let mut echoed = 0u64;
            loop {
                if let Some(msg) = input.try_recv() {
                    out.push(&msg);
                    echoed += 1;
                    if echoed.is_multiple_of(100_000) {
                        eprintln!("ponger: echoed {}", echoed);
                    }
                } else {
                    core::hint::spin_loop();
                }
            }
        }
        "pinger" => {
            let epoch = Instant::now();
            let now_ns = || epoch.elapsed().as_nanos() as u64;
            let period = Duration::from_micros(paced_us);
            // Warm up the path (connections, page faults) before measuring.
            let warmup = 2_000u64;
            let mut rtt = Vec::with_capacity(samples);
            let mut next = Instant::now();
            let mut seq = 0u64;
            let mut lost = 0u64;
            while rtt.len() < samples {
                while Instant::now() < next {
                    core::hint::spin_loop();
                }
                seq += 1;
                out.push(&Msg {
                    sent_ns: now_ns(),
                    seq,
                    _pad: [0; 40],
                });
                next += period;
                let deadline = Instant::now() + Duration::from_millis(500);
                loop {
                    if let Some(msg) = input.try_recv() {
                        if msg.seq == seq {
                            if seq > warmup {
                                rtt.push(now_ns() - msg.sent_ns);
                            }
                            break;
                        }
                    } else if Instant::now() > deadline {
                        lost += 1;
                        break;
                    } else {
                        core::hint::spin_loop();
                    }
                }
            }
            rtt.sort_unstable();
            println!(
                "pinger -> ponger -> pinger round trip, {} samples paced {} us, {} B slots, {} lost",
                rtt.len(),
                paced_us,
                std::mem::size_of::<Msg>() + 8,
                lost
            );
            println!(
                "  rtt p50 {:.1} us  p90 {:.1} us  p99 {:.1} us  p99.9 {:.1} us  max {:.1} us",
                percentile(&rtt, 0.50) as f64 / 1000.0,
                percentile(&rtt, 0.90) as f64 / 1000.0,
                percentile(&rtt, 0.99) as f64 / 1000.0,
                percentile(&rtt, 0.999) as f64 / 1000.0,
                rtt[rtt.len() - 1] as f64 / 1000.0
            );
            println!(
                "  one-way (rtt/2) p50 {:.1} us  p99 {:.1} us",
                percentile(&rtt, 0.50) as f64 / 2000.0,
                percentile(&rtt, 0.99) as f64 / 2000.0
            );
            handle.shutdown().unwrap();
            let gaps = mirror_thread.join().unwrap();
            println!("  mirror gaps {}", gaps);
        }
        other => panic!("unknown role {}", other),
    }
}
