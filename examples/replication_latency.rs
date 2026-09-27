//! One-way latency and throughput of ring replication over loopback.
//!
//! A producer pushes into a source ring; a `ReplicaServer` streams it to a `Mirror` in the
//! same process; a reader on the mirror ring timestamps arrival. Producer, server, mirror
//! and reader all busy-poll. Both ends read one monotonic clock, so the one-way figure is
//! exact: each record is stamped immediately before its actual push (not its scheduled
//! time) and the reader stamps it as soon as `try_recv` returns it.
//!
//! Every phase ends when its last record arrives or after an idle timeout, so a lost
//! record is reported as a loss instead of hanging the run.
//!
//! ```text
//! cargo run --release --example replication_latency [-- --paced-us 100 --samples 20000]
//!     [--burst 2000000] [--idle-ms 2000] [--multicast 239.255.0.1:7401 --iface LOCAL_ADDR]
//! ```

mod support;

use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use std::time::{Duration, Instant};

use ringfire::replication::{Mirror, MirrorStart, MulticastConfig, ReplicaServer};
use ringfire::{RingConsumer, RingProducer};
use support::{latency_line, parse_arg, receive_until, elapsed_ns, Pacer, Received, SeqTracker};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Msg {
    sent_ns: u64,
    seq: u64,
    _pad: [u8; 40],
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut paced_us = 100u64;
    let mut samples = 20_000u64;
    let mut burst = 2_000_000u64;
    let mut idle_ms = 2_000u64;
    let mut multicast: Option<SocketAddrV4> = None;
    let mut iface = Ipv4Addr::UNSPECIFIED;
    let mut i = 1;
    while i < args.len() {
        let value = args.get(i + 1).map(String::as_str);
        match (args[i].as_str(), value) {
            ("--multicast", Some(v)) => multicast = Some(parse_arg("--multicast", v)),
            ("--iface", Some(v)) => iface = parse_arg("--iface", v),
            ("--paced-us", Some(v)) => paced_us = parse_arg("--paced-us", v),
            ("--samples", Some(v)) => samples = parse_arg("--samples", v),
            ("--burst", Some(v)) => burst = parse_arg("--burst", v),
            ("--idle-ms", Some(v)) => idle_ms = parse_arg("--idle-ms", v),
            _ => {
                i += 1;
                continue;
            }
        }
        i += 2;
    }
    assert!(samples > 0, "--samples must be positive");
    // Idle timeout: long against the pacing period, so only a stalled stream ends a phase.
    let idle = Duration::from_millis(idle_ms).max(Duration::from_micros(paced_us) * 10);
    let first_wait = idle.max(Duration::from_secs(10));

    let source =
        std::env::temp_dir().join(format!("ringfire_repl_lat_src_{}.shm", std::process::id()));
    let copy =
        std::env::temp_dir().join(format!("ringfire_repl_lat_dst_{}.shm", std::process::id()));
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Msg>::create(&source, 1 << 16).unwrap();
    let mut server = ReplicaServer::bind(&source, "127.0.0.1:0")
        .unwrap()
        .spin(true);
    if let Some(group) = multicast {
        server = server.multicast(MulticastConfig::new(*group.ip(), group.port()).interface(iface));
    }
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Latest)
        .spin(true)
        .interface(iface)
        .connect(addr, &copy)
        .unwrap();
    let transport = if mirror.is_multicast() {
        "multicast"
    } else {
        "tcp"
    };
    let handle = mirror.handle().unwrap();
    let mirror_thread = thread::spawn(move || {
        let result = mirror.run();
        (result, mirror.sequence(), mirror.gaps())
    });
    let mut reader = RingConsumer::<Msg>::attach(&copy).unwrap();
    let epoch = Instant::now();
    let now_ns = move || epoch.elapsed().as_nanos() as u64;

    // Paced one-way latency: one message every `paced_us`, like a live feed.
    let pacer = thread::spawn(move || {
        let mut pace = Pacer::every(Duration::from_micros(paced_us));
        for seq in 1..=samples {
            pace.wait_next();
            producer.push(&Msg {
                sent_ns: now_ns(),
                seq,
                _pad: [0; 40],
            });
        }
        producer
    });
    let mut lat: Vec<i64> = Vec::with_capacity(samples as usize);
    let mut paced = SeqTracker::new(samples);
    let paced_end = receive_until(
        || reader.try_recv(),
        |m| m.seq,
        samples,
        first_wait,
        idle,
        |m| {
            let arrived = now_ns();
            if paced.record(m.seq) {
                lat.push(elapsed_ns(m.sent_ns, arrived));
            }
        },
    );
    let producer = pacer.join().unwrap();
    println!(
        "paced ({} us, {}): one-way source ring -> mirror ring (actual push -> read), {} B slots",
        paced_us,
        transport,
        std::mem::size_of::<Msg>() + 8
    );
    println!("  {}", latency_line(&mut lat));
    println!(
        "  {}{}",
        paced.summary(1, samples),
        if paced_end == Received::Idle {
            ", ended by idle timeout"
        } else {
            ""
        }
    );

    // Burst throughput: push as fast as possible, measure what the mirror delivers.
    if burst > 0 {
        run_burst(&mut reader, producer, samples + 1, burst, first_wait, idle);
    }

    handle.shutdown().unwrap();
    let (result, seq, gaps) = mirror_thread.join().unwrap();
    println!("mirror: last seq {}, gaps {}", seq, gaps);
    let _ = std::fs::remove_file(&copy);
    if let Err(e) = result {
        eprintln!("mirror failed: {}", e);
        std::process::exit(1);
    }
}

fn run_burst(
    reader: &mut RingConsumer<Msg>,
    mut producer: RingProducer<Msg>,
    first_seq: u64,
    burst: u64,
    first_wait: Duration,
    idle: Duration,
) {
    let last_seq = first_seq + burst - 1;
    let lapped_before = reader.lapped_count();
    let start = Instant::now();
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
    let mut delivered = SeqTracker::new(last_seq);
    let mut last_arrival = start;
    let burst_end = receive_until(
        || reader.try_recv(),
        |m| m.seq,
        last_seq,
        first_wait,
        idle,
        |m| {
            // Late paced records still in flight are not part of the burst. The clock is
            // read on the last record and every 1,024th, to keep the reader fast.
            if m.seq >= first_seq
                && delivered.record(m.seq)
                && (m.seq >= last_seq || delivered.unique().is_multiple_of(1024))
            {
                last_arrival = Instant::now();
            }
        },
    );
    let recv_elapsed = last_arrival - start;
    let (_producer, push_elapsed) = pusher.join().unwrap();
    let got = delivered.unique();
    println!(
        "burst: {} pushed in {:.1} ms ({:.1} M/s); mirror delivered {} ({:.1}%) in {:.1} ms ({:.1} M/s), reader lapped {}{}",
        burst,
        push_elapsed.as_secs_f64() * 1e3,
        burst as f64 / push_elapsed.as_secs_f64().max(1e-9) / 1e6,
        got,
        got as f64 * 100.0 / burst.max(1) as f64,
        recv_elapsed.as_secs_f64() * 1e3,
        got as f64 / recv_elapsed.as_secs_f64().max(1e-9) / 1e6,
        reader.lapped_count() - lapped_before,
        if burst_end == Received::Idle {
            ", ended by idle timeout"
        } else {
            ""
        }
    );
    println!("  {}", delivered.summary(first_seq, last_seq));
}
