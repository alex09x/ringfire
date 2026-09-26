//! Open-loop stress test of ring replication between two hosts.
//!
//! The pinger publishes at a fixed rate for a fixed time and never waits; the ponger
//! echoes everything it receives. The pinger matches echoes by sequence and reports the
//! delivery ratio, round-trip percentiles and both mirrors' NAK, retransmission and gap
//! counters, so losses can be attributed: source ring lapping the sender, datagram loss
//! beyond retention, or a reader that fell behind.
//!
//! ```text
//! host B:  replication_stress --role ponger --bind 0.0.0.0:7401 --peer hostA:7400 \
//!              [--multicast 239.255.1.2:7411 --iface B_ADDR]
//! host A:  replication_stress --role pinger --bind 0.0.0.0:7400 --peer hostB:7401 \
//!              [--multicast 239.255.1.1:7410 --iface A_ADDR] --rate 100000 --seconds 5
//! ```

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use ringfire::replication::{Mirror, MirrorStart, MulticastConfig, ReplicaServer};
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

fn connect_with_retry(peer: &str, path: &PathBuf, iface: Ipv4Addr, unicast: bool) -> Mirror {
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
    let mut burst = 1u64;
    let mut udp_port: Option<u16> = None;
    let mut unicast = false;
    let mut dup = 1u8;
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--linger-us" => linger_us = Some(args[i + 1].parse().unwrap()),
            "--burst" => burst = args[i + 1].parse::<u64>().unwrap().max(1),
            "--udp" => udp_port = Some(args[i + 1].parse().unwrap()),
            "--dup" => dup = args[i + 1].parse().unwrap(),
            "--unicast" => {
                unicast = args[i + 1] == "1" || args[i + 1] == "true";
            }
            "--warmup-ms" => warmup_ms = args[i + 1].parse().unwrap(),
            "--role" => role = args[i + 1].clone(),
            "--bind" => bind = args[i + 1].clone(),
            "--peer" => peer = args[i + 1].clone(),
            "--rate" => rate = args[i + 1].parse().unwrap(),
            "--seconds" => seconds = args[i + 1].parse().unwrap(),
            "--dir" => dir = PathBuf::from(&args[i + 1]),
            "--multicast" => multicast = Some(args[i + 1].parse().unwrap()),
            "--iface" => iface = args[i + 1].parse().unwrap(),
            _ => {}
        }
        i += 2;
    }
    assert!(
        !role.is_empty() && !bind.is_empty() && !peer.is_empty(),
        "--role, --bind and --peer are required"
    );

    let out_path = dir.join(format!("ringfire_stress_{}_out.shm", role));
    let in_path = dir.join(format!("ringfire_stress_{}_in.shm", role));
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

    match role.as_str() {
        "ponger" => {
            // One thread: pump the mirror and echo, so the mirror's counters stay visible.
            let mut echoed = 0u64;
            let mut last_report = Instant::now();
            loop {
                if !mirror.step().unwrap() {
                    eprintln!(
                        "ponger: source closed after echoing {}; reader lapped {}, mirror datagrams {} naks {} retransmitted {} gaps {}; reconnecting",
                        echoed,
                        input.lapped_count(),
                        mirror.datagrams(),
                        mirror.naks(),
                        mirror.retransmitted(),
                        mirror.gaps()
                    );
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
                let _ = mirror.run();
                (
                    mirror.datagrams(),
                    mirror.naks(),
                    mirror.retransmitted(),
                    mirror.gaps(),
                )
            });
            let epoch = Instant::now();
            let now_ns = move || epoch.elapsed().as_nanos() as u64;
            // `rate` records per second, published `burst` at a time back to back.
            let period = Duration::from_nanos(1_000_000_000 * burst / rate.max(1));
            let total = rate * seconds;
            // Give the peer's mirror time to connect to our server (it retries every
            // 200 ms), or its `Latest` start would miss the first records.
            thread::sleep(Duration::from_millis(warmup_ms));
            let mut pusher = Some(thread::spawn(move || {
                let start = Instant::now();
                let mut next = start;
                for seq in 1..=total {
                    if (seq - 1) % burst == 0 {
                        while Instant::now() < next {
                            core::hint::spin_loop();
                        }
                        next += period;
                    }
                    out.push(&Msg {
                        sent_ns: now_ns(),
                        seq,
                        _pad: [0; 40],
                    });
                }
                (out, start.elapsed())
            }));
            let mut rtt = Vec::with_capacity(total as usize);
            let mut received = 0u64;
            let mut last_seq = 0u64;
            let mut disorder = 0u64;
            let mut last_arrival = Instant::now();
            let mut pushed_done: Option<Duration> = None;
            let mut out_ring = None;
            loop {
                if let Some(msg) = input.try_recv() {
                    received += 1;
                    if msg.seq <= last_seq {
                        disorder += 1;
                    }
                    last_seq = msg.seq;
                    rtt.push(now_ns().saturating_sub(msg.sent_ns));
                    last_arrival = Instant::now();
                } else {
                    if pushed_done.is_none() && pusher.as_ref().is_some_and(|p| p.is_finished()) {
                        let (ring, elapsed) = pusher.take().unwrap().join().unwrap();
                        out_ring = Some(ring);
                        pushed_done = Some(elapsed);
                    }
                    if pushed_done.is_some()
                        && (received >= total || last_arrival.elapsed() > Duration::from_secs(1))
                    {
                        break;
                    }
                    core::hint::spin_loop();
                }
            }
            let push_elapsed = pushed_done.unwrap_or_default();
            drop(out_ring);
            rtt.sort_unstable();
            handle.shutdown().unwrap();
            let (datagrams, naks, retransmitted, gaps) = mirror_thread.join().unwrap();
            println!(
                "rate {} msg/s (bursts of {}) x {} s: pushed {} in {:.2} s ({:.0} msg/s achieved), echoed {} ({:.3}%), lost {}, disorder {}, reader lapped {}",
                rate,
                burst,
                seconds,
                total,
                push_elapsed.as_secs_f64(),
                total as f64 / push_elapsed.as_secs_f64().max(1e-9),
                received,
                received as f64 * 100.0 / total as f64,
                total - received.min(total),
                disorder,
                input.lapped_count()
            );
            if !rtt.is_empty() {
                println!(
                    "  rtt p50 {:.1} us  p90 {:.1} us  p99 {:.1} us  p99.9 {:.1} us  max {:.1} us",
                    percentile(&rtt, 0.50) as f64 / 1000.0,
                    percentile(&rtt, 0.90) as f64 / 1000.0,
                    percentile(&rtt, 0.99) as f64 / 1000.0,
                    percentile(&rtt, 0.999) as f64 / 1000.0,
                    rtt[rtt.len() - 1] as f64 / 1000.0
                );
            }
            println!(
                "  pinger mirror: datagrams {}, naks {}, retransmitted {}, gaps {}",
                datagrams, naks, retransmitted, gaps
            );
        }
        other => panic!("unknown role {}", other),
    }
}
