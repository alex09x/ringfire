//! Open-loop stress test of ring replication between two hosts.
//!
//! The pinger publishes at a fixed rate for a fixed time and never waits; the ponger
//! echoes everything it receives. The pinger matches echoes by sequence and reports the
//! delivery ratio (distinct sequences, so a duplicate cannot hide a loss), duplicates,
//! reordering, round-trip percentiles and both mirrors' NAK, retransmission and gap
//! counters, so losses can be attributed: source ring lapping the sender, datagram loss
//! beyond retention, or a reader that fell behind.
//!
//! Round trips are measured from each record's actual push (the pinger's monotonic
//! clock stamps it immediately before `push`), not from its scheduled send time.
//!
//! ```text
//! host B:  replication_stress --role ponger --bind 0.0.0.0:7401 --peer hostA:7400 \
//!              [--multicast 239.255.1.2:7411 --iface B_ADDR] [--exit-on-close 1]
//! host A:  replication_stress --role pinger --bind 0.0.0.0:7400 --peer hostB:7401 \
//!              [--multicast 239.255.1.1:7410 --iface A_ADDR] --rate 100000 --seconds 5
//! ```
//!
//! The ponger reconnects whenever the pinger's source closes, or exits then with
//! `--exit-on-close 1`.

mod support;

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use ringfire::replication::{Mirror, MirrorStart, MulticastConfig, ReplicaServer};
use ringfire::{RingConsumer, RingProducer};
use support::{elapsed_ns, latency_line, parse_arg, parse_flag, Pacer, SeqTracker};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Msg {
    sent_ns: u64,
    seq: u64,
    _pad: [u8; 40],
}

fn connect_with_retry(peer: &str, path: &Path, iface: Ipv4Addr, unicast: bool) -> Mirror {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match Mirror::builder()
            .start(MirrorStart::Latest)
            .spin(true)
            .interface(iface)
            .unicast(unicast)
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
    let mut rate = 100_000u64;
    let mut seconds = 5u64;
    let mut dir = PathBuf::from("/dev/shm");
    let mut multicast: Option<SocketAddrV4> = None;
    let mut iface = Ipv4Addr::UNSPECIFIED;
    let mut linger_us: Option<u64> = None;
    let mut warmup_ms = 1500u64;
    let mut idle_ms = 1000u64;
    let mut burst = 1u64;
    let mut udp_port: Option<u16> = None;
    let mut unicast = false;
    let mut dup = 1u8;
    let mut exit_on_close = false;
    let mut i = 1;
    while i + 1 < args.len() {
        let v = args[i + 1].as_str();
        match args[i].as_str() {
            "--linger-us" => linger_us = Some(parse_arg("--linger-us", v)),
            "--burst" => burst = parse_arg::<u64>("--burst", v).max(1),
            "--udp" => udp_port = Some(parse_arg("--udp", v)),
            "--dup" => dup = parse_arg("--dup", v),
            "--unicast" => unicast = parse_flag(v),
            "--warmup-ms" => warmup_ms = parse_arg("--warmup-ms", v),
            "--idle-ms" => idle_ms = parse_arg("--idle-ms", v),
            "--exit-on-close" => exit_on_close = parse_flag(v),
            "--role" => role = v.to_string(),
            "--bind" => bind = v.to_string(),
            "--peer" => peer = v.to_string(),
            "--rate" => rate = parse_arg("--rate", v),
            "--seconds" => seconds = parse_arg("--seconds", v),
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
    assert!(rate > 0, "--rate must be positive");

    let pid = std::process::id();
    let out_path = dir.join(format!("ringfire_stress_{}_{}_out.shm", role, pid));
    let in_path = dir.join(format!("ringfire_stress_{}_{}_in.shm", role, pid));
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(&in_path);

    let mut out = RingProducer::<Msg>::create(&out_path, 1 << 18).unwrap();
    let mut server = ReplicaServer::bind(&out_path, bind.as_str())
        .unwrap()
        .spin(true)
        .linger(linger_us.map(Duration::from_micros));
    if let Some(group) = multicast {
        server = server.multicast(MulticastConfig::new(*group.ip(), group.port()).interface(iface));
    }
    if let Some(port) = udp_port {
        server = server.unicast(port, 1400);
    }
    server = server.duplicate(dup);
    server.spawn().unwrap();
    let mut mirror = connect_with_retry(&peer, &in_path, iface, unicast);
    eprintln!(
        "{}: serving {} on {}, mirroring {} ({})",
        role,
        out_path.display(),
        bind,
        peer,
        if mirror.is_multicast() {
            "multicast"
        } else {
            "tcp"
        }
    );
    let mut input = RingConsumer::<Msg>::attach(&in_path).unwrap();

    let failed = match role.as_str() {
        "ponger" => {
            // One thread: pump the mirror and echo, so the mirror's counters stay visible.
            let mut echoed = 0u64;
            let mut last_report = Instant::now();
            loop {
                let open = match mirror.step() {
                    Ok(open) => open,
                    Err(e) => {
                        eprintln!("ponger: mirror failed: {}", e);
                        break true;
                    }
                };
                if !open {
                    eprintln!(
                        "ponger: source closed after echoing {}; reader lapped {}, mirror datagrams {} naks {} retransmitted {} gaps {}{}",
                        echoed,
                        input.lapped_count(),
                        mirror.datagrams(),
                        mirror.naks(),
                        mirror.retransmitted(),
                        mirror.gaps(),
                        if exit_on_close { "" } else { "; reconnecting" }
                    );
                    if exit_on_close {
                        break false;
                    }
                    echoed = 0;
                    mirror = connect_with_retry(&peer, &in_path, iface, unicast);
                    input = RingConsumer::<Msg>::attach(&in_path).unwrap();
                }
                while let Some(msg) = input.try_recv() {
                    out.push(&msg);
                    echoed += 1;
                }
                if last_report.elapsed() >= Duration::from_secs(5) {
                    eprintln!(
                        "ponger: echoed {}, reader lapped {}, mirror datagrams {} naks {} retransmitted {} gaps {}",
                        echoed,
                        input.lapped_count(),
                        mirror.datagrams(),
                        mirror.naks(),
                        mirror.retransmitted(),
                        mirror.gaps()
                    );
                    last_report = Instant::now();
                }
            }
        }
        "pinger" => {
            let handle = mirror.handle().unwrap();
            let mirror_thread = thread::spawn(move || {
                let result = mirror.run();
                (
                    result,
                    mirror.datagrams(),
                    mirror.naks(),
                    mirror.retransmitted(),
                    mirror.gaps(),
                )
            });
            let epoch = Instant::now();
            let now_ns = move || epoch.elapsed().as_nanos() as u64;
            let total = rate * seconds;
            // Give the peer's mirror time to connect to our server (it retries every
            // 200 ms), or its `Latest` start would miss the first records, which would
            // then be reported as lost.
            thread::sleep(Duration::from_millis(warmup_ms));
            let mut pusher = Some(thread::spawn(move || {
                let start = Instant::now();
                // `rate` records per second, published `burst` at a time back to back.
                let mut pace = Pacer::per_second(rate, burst);
                for seq in 1..=total {
                    if (seq - 1) % burst == 0 {
                        pace.wait_next();
                    }
                    out.push(&Msg {
                        sent_ns: now_ns(),
                        seq,
                        _pad: [0; 40],
                    });
                }
                (out, start.elapsed())
            }));
            let mut rtt: Vec<i64> = Vec::with_capacity(total as usize);
            let mut echoes = SeqTracker::new(total);
            let idle = Duration::from_millis(idle_ms);
            let mut last_arrival = Instant::now();
            let mut pushed_done: Option<Duration> = None;
            let mut out_ring = None;
            let mut empty = 0u32;
            loop {
                if let Some(msg) = input.try_recv() {
                    let arrived = now_ns();
                    if echoes.record(msg.seq) {
                        rtt.push(elapsed_ns(msg.sent_ns, arrived));
                    }
                    empty = 0;
                    if pushed_done.is_some() {
                        last_arrival = Instant::now();
                    }
                } else {
                    empty = empty.wrapping_add(1);
                    if empty.is_multiple_of(256) {
                        if pushed_done.is_none() && pusher.as_ref().is_some_and(|p| p.is_finished()) {
                            let (ring, elapsed) = pusher.take().unwrap().join().unwrap();
                            out_ring = Some(ring);
                            pushed_done = Some(elapsed);
                            last_arrival = Instant::now();
                        }
                        if pushed_done.is_some()
                            && (echoes.unique() >= total || last_arrival.elapsed() > idle)
                        {
                            break;
                        }
                    }
                    core::hint::spin_loop();
                }
            }
            let push_elapsed = pushed_done.unwrap_or_default();
            drop(out_ring);
            handle.shutdown().unwrap();
            let (result, datagrams, naks, retransmitted, gaps) = mirror_thread.join().unwrap();
            println!(
                "rate {} msg/s (bursts of {}) x {} s: pushed {} in {:.2} s ({:.0} msg/s achieved), echoed {} ({:.3}%), lost {}, duplicates {}, disorder {}, reader lapped {}",
                rate,
                burst,
                seconds,
                total,
                push_elapsed.as_secs_f64(),
                total as f64 / push_elapsed.as_secs_f64().max(1e-9),
                echoes.unique(),
                echoes.unique() as f64 * 100.0 / total.max(1) as f64,
                echoes.missing_in(1, total),
                echoes.duplicates(),
                echoes.out_of_order(),
                input.lapped_count()
            );
            println!("  rtt {}", latency_line(&mut rtt));
            println!(
                "  pinger mirror: datagrams {}, naks {}, retransmitted {}, gaps {}",
                datagrams, naks, retransmitted, gaps
            );
            match result {
                Ok(()) => false,
                Err(e) => {
                    eprintln!("pinger: mirror failed: {}", e);
                    true
                }
            }
        }
        other => panic!("unknown role {}", other),
    };
    drop(input);
    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);
    if failed {
        std::process::exit(1);
    }
}
