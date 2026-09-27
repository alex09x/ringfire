//! Round-trip latency of ring replication between two hosts.
//!
//! Each side publishes a ring and mirrors the other side's ring, so the round trip is
//! two network hops plus four ring hand-offs. Only the pinger's monotonic clock is used,
//! so the figure is exact; one-way latency is about half of it.
//!
//! ```text
//! host B:  replication_pingpong --role ponger --bind 0.0.0.0:7401 --peer hostA:7400 \
//!              [--exit-on-close 1]
//! host A:  replication_pingpong --role pinger --bind 0.0.0.0:7400 --peer hostB:7401 \
//!              [--samples 20000] [--warmup 2000] [--paced-us 100] [--timeout-ms 500] [--dir /dev/shm]
//! ```
//!
//! Add `--multicast GROUP:PORT --iface LOCAL_ADDR` on each side (a different group or
//! port per side) to send live records by UDP multicast instead of TCP.
//!
//! Start the ponger first; both sides retry the connection to their peer for a while.
//! The pinger sends exactly `warmup + samples` pings, one at a time: a reply that does not
//! come within `--timeout-ms` is counted lost, and a reply that comes later is counted
//! late (not as a sample). The ponger echoes forever, or with `--exit-on-close 1` exits
//! once the pinger's source closes.

mod support;

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use ringfire::replication::{Mirror, MirrorStart, MulticastConfig, ReplicaServer};
use ringfire::{RingConsumer, RingProducer};
use support::{elapsed_ns, latency_line, parse_arg, parse_flag, percentile, Pacer};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Msg {
    sent_ns: u64,
    seq: u64,
    _pad: [u8; 40],
}

fn connect_with_retry(peer: &str, path: &Path, iface: Ipv4Addr) -> Mirror {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match Mirror::builder()
            .start(MirrorStart::Latest)
            .spin(true)
            .interface(iface)
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
    let mut samples = 20_000u64;
    let mut warmup = 2_000u64;
    let mut paced_us = 100u64;
    let mut timeout_ms = 500u64;
    let mut exit_on_close = false;
    let mut dir = PathBuf::from("/dev/shm");
    let mut multicast: Option<SocketAddrV4> = None;
    let mut iface = Ipv4Addr::UNSPECIFIED;
    let mut i = 1;
    while i + 1 < args.len() {
        let v = args[i + 1].as_str();
        match args[i].as_str() {
            "--role" => role = v.to_string(),
            "--bind" => bind = v.to_string(),
            "--peer" => peer = v.to_string(),
            "--samples" => samples = parse_arg("--samples", v),
            "--warmup" => warmup = parse_arg("--warmup", v),
            "--paced-us" => paced_us = parse_arg("--paced-us", v),
            "--timeout-ms" => timeout_ms = parse_arg("--timeout-ms", v),
            "--exit-on-close" => exit_on_close = parse_flag(v),
            "--dir" => dir = PathBuf::from(v),
            "--multicast" => multicast = Some(parse_arg("--multicast", v)),
            "--iface" => iface = parse_arg("--iface", v),
            _ => {}
        }
        i += 2;
    }
    assert!(
        !role.is_empty() && !bind.is_empty() && !peer.is_empty(),
        "--role, --bind and --peer are required"
    );

    let pid = std::process::id();
    let out_path = dir.join(format!("ringfire_pingpong_{}_{}_out.shm", role, pid));
    let in_path = dir.join(format!("ringfire_pingpong_{}_{}_in.shm", role, pid));
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(&in_path);

    let mut out = RingProducer::<Msg>::create(&out_path, 1 << 16).unwrap();
    let mut server = ReplicaServer::bind(&out_path, bind.as_str())
        .unwrap()
        .spin(true);
    if let Some(group) = multicast {
        server = server.multicast(MulticastConfig::new(*group.ip(), group.port()).interface(iface));
    }
    eprintln!(
        "{}: serving {} on {}{}",
        role,
        out_path.display(),
        server.local_addr().unwrap(),
        multicast.map_or(String::new(), |g| format!(", multicast {}", g))
    );
    server.spawn().unwrap();

    let mut mirror = connect_with_retry(&peer, &in_path, iface);
    eprintln!(
        "{}: mirroring {} -> {} ({})",
        role,
        peer,
        in_path.display(),
        if mirror.is_multicast() {
            "multicast"
        } else {
            "tcp"
        }
    );
    let handle = mirror.handle().unwrap();
    let mirror_thread = thread::spawn(move || {
        let result = mirror.run();
        (result, mirror.gaps(), mirror.naks(), mirror.retransmitted())
    });
    let mut input = RingConsumer::<Msg>::attach(&in_path).unwrap();

    let failed = match role.as_str() {
        "ponger" => {
            // Echo every message back with its timestamp untouched.
            let mut echoed = 0u64;
            let mut empty = 0u32;
            loop {
                if let Some(msg) = input.try_recv() {
                    out.push(&msg);
                    echoed += 1;
                    if echoed.is_multiple_of(100_000) {
                        eprintln!("ponger: echoed {}", echoed);
                    }
                } else {
                    empty = empty.wrapping_add(1);
                    // The mirror thread ends when the pinger's source closes.
                    if exit_on_close && empty.is_multiple_of(4096) && mirror_thread.is_finished() {
                        break;
                    }
                    core::hint::spin_loop();
                }
            }
            let (result, gaps, naks, retransmitted) = mirror_thread.join().unwrap();
            eprintln!(
                "ponger: source closed after echoing {}; reader lapped {}, mirror gaps {}, naks {}, retransmitted {}",
                echoed,
                input.lapped_count(),
                gaps,
                naks,
                retransmitted
            );
            report_mirror_error(result)
        }
        "pinger" => {
            let epoch = Instant::now();
            let now_ns = || epoch.elapsed().as_nanos() as u64;
            let timeout = Duration::from_millis(timeout_ms);
            let mut rtt: Vec<i64> = Vec::with_capacity(samples as usize);
            let (mut lost, mut late, mut unexpected) = (0u64, 0u64, 0u64);
            let mut pace = Pacer::every(Duration::from_micros(paced_us));
            // The first `warmup` pings warm up the path (connections, page faults) and are
            // not measured.
            for seq in 1..=warmup + samples {
                pace.wait_next();
                out.push(&Msg {
                    sent_ns: now_ns(),
                    seq,
                    _pad: [0; 40],
                });
                let deadline = Instant::now() + timeout;
                loop {
                    if let Some(msg) = input.try_recv() {
                        let arrived = now_ns();
                        if msg.seq == seq {
                            if seq > warmup {
                                rtt.push(elapsed_ns(msg.sent_ns, arrived));
                            }
                            break;
                        } else if msg.seq < seq {
                            late += 1;
                        } else {
                            unexpected += 1;
                        }
                    } else if Instant::now() > deadline {
                        lost += 1;
                        break;
                    } else {
                        core::hint::spin_loop();
                    }
                }
            }
            println!(
                "pinger -> ponger -> pinger round trip, {} pings after {} warm-up, paced {} us, {} B slots: {} lost (>{} ms), {} late, {} unexpected, reader lapped {}",
                samples,
                warmup,
                paced_us,
                std::mem::size_of::<Msg>() + 8,
                lost,
                timeout_ms,
                late,
                unexpected,
                input.lapped_count()
            );
            println!("  rtt {}", latency_line(&mut rtt));
            if let (Some(p50), Some(p99)) = (percentile(&rtt, 0.50), percentile(&rtt, 0.99)) {
                println!(
                    "  one-way (rtt/2) p50 {:.1} us  p99 {:.1} us",
                    p50 as f64 / 2000.0,
                    p99 as f64 / 2000.0
                );
            }
            handle.shutdown().unwrap();
            let (result, gaps, naks, retransmitted) = mirror_thread.join().unwrap();
            println!(
                "  mirror gaps {}, naks {}, retransmitted {}",
                gaps, naks, retransmitted
            );
            report_mirror_error(result) || rtt.is_empty()
        }
        other => panic!("unknown role {}", other),
    };
    drop(input);
    drop(out);
    let _ = std::fs::remove_file(&in_path);
    if failed {
        std::process::exit(1);
    }
}

/// Prints a mirror failure; returns whether there was one.
fn report_mirror_error(result: ringfire::Result<()>) -> bool {
    match result {
        Ok(()) => false,
        Err(e) => {
            eprintln!("mirror failed: {}", e);
            true
        }
    }
}
