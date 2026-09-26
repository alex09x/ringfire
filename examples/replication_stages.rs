//! Per-stage latency of replication, measured on several hosts at once.
//!
//! The master stamps every record with its wall clock when it pushes it into the source
//! ring. A local consumer on the master reads the same ring and measures push → read on
//! the same host. Each slave mirrors the ring and a local consumer on the slave measures
//! push on the master → read on the slave, translating its own clock into the master's
//! with an offset estimated PTP-style (hundreds of probes, the minimum-round-trip sample
//! wins; the residual error is the path asymmetry, a few microseconds on a LAN).
//!
//! ```text
//! master:  replication_stages --role master --bind 0.0.0.0:7400 --clock 0.0.0.0:7402 \
//!              [--multicast 239.255.1.1:7410 --iface A] --rate 1000 --seconds 5
//! slave:   replication_stages --role slave --source A:7400 --clock A:7402 [--iface B] \
//!              --ring /dev/shm/stage_s1.shm --name s1
//! ```

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ringfire::replication::{Mirror, MirrorStart, MulticastConfig, ReplicaServer};
use ringfire::{RingConsumer, RingProducer};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Msg {
    sent_ns: u64,
    seq: u64,
    _pad: [u8; 40],
}

fn wall_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn percentile(sorted: &[i64], p: f64) -> i64 {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn report(label: &str, samples: &mut [i64], extra: &str) {
    if samples.is_empty() {
        println!("{}: no samples {}", label, extra);
        return;
    }
    samples.sort_unstable();
    println!(
        "{}: n={} p50 {:.1} us  p90 {:.1} us  p99 {:.1} us  p99.9 {:.1} us  max {:.1} us  min {:.1} us {}",
        label,
        samples.len(),
        percentile(samples, 0.50) as f64 / 1000.0,
        percentile(samples, 0.90) as f64 / 1000.0,
        percentile(samples, 0.99) as f64 / 1000.0,
        percentile(samples, 0.999) as f64 / 1000.0,
        samples[samples.len() - 1] as f64 / 1000.0,
        samples[0] as f64 / 1000.0,
        extra
    );
}

/// Answers clock probes: reads the probe's send time, replies with receive and send times.
fn clock_service(bind: String) {
    let listener = TcpListener::bind(bind).unwrap();
    for stream in listener.incoming().flatten() {
        thread::spawn(move || {
            let mut stream = stream;
            stream.set_nodelay(true).ok();
            let mut probe = [0u8; 8];
            while stream.read_exact(&mut probe).is_ok() {
                let t2 = wall_ns();
                let mut reply = [0u8; 24];
                reply[0..8].copy_from_slice(&probe);
                reply[8..16].copy_from_slice(&t2.to_le_bytes());
                reply[16..24].copy_from_slice(&wall_ns().to_le_bytes());
                if stream.write_all(&reply).is_err() {
                    break;
                }
            }
        });
    }
}

/// Estimates `master clock - slave clock` from the minimum-round-trip probe.
fn clock_offset(clock: &str, probes: usize) -> (i64, i64) {
    let mut stream = TcpStream::connect(clock).unwrap();
    stream.set_nodelay(true).unwrap();
    let mut best_rtt = i64::MAX;
    let mut best_offset = 0i64;
    let mut reply = [0u8; 24];
    for _ in 0..probes {
        let t1 = wall_ns();
        stream.write_all(&t1.to_le_bytes()).unwrap();
        stream.read_exact(&mut reply).unwrap();
        let t4 = wall_ns();
        let t2 = i64::from_le_bytes(reply[8..16].try_into().unwrap());
        let t3 = i64::from_le_bytes(reply[16..24].try_into().unwrap());
        let rtt = (t4 - t1) - (t3 - t2);
        if rtt < best_rtt {
            best_rtt = rtt;
            best_offset = ((t2 - t1) + (t3 - t4)) / 2;
        }
        thread::sleep(Duration::from_micros(200));
    }
    (best_offset, best_rtt)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut role = String::new();
    let mut bind = String::from("0.0.0.0:7400");
    let mut clock = String::from("0.0.0.0:7402");
    let mut source = String::new();
    let mut ring = PathBuf::from("/dev/shm/ringfire_stages.shm");
    let mut name = String::from("slave");
    let mut rate = 1000u64;
    let mut seconds = 5u64;
    let mut burst = 1u64;
    let mut warmup_ms = 3000u64;
    let mut multicast: Option<SocketAddrV4> = None;
    let mut iface = Ipv4Addr::UNSPECIFIED;
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--role" => role = args[i + 1].clone(),
            "--bind" => bind = args[i + 1].clone(),
            "--clock" => clock = args[i + 1].clone(),
            "--source" => source = args[i + 1].clone(),
            "--ring" => ring = PathBuf::from(&args[i + 1]),
            "--name" => name = args[i + 1].clone(),
            "--rate" => rate = args[i + 1].parse().unwrap(),
            "--seconds" => seconds = args[i + 1].parse().unwrap(),
            "--burst" => burst = args[i + 1].parse::<u64>().unwrap().max(1),
            "--warmup-ms" => warmup_ms = args[i + 1].parse().unwrap(),
            "--multicast" => multicast = Some(args[i + 1].parse().unwrap()),
            "--iface" => iface = args[i + 1].parse().unwrap(),
            _ => {}
        }
        i += 2;
    }

    match role.as_str() {
        "master" => {
            let _ = std::fs::remove_file(&ring);
            let mut producer = RingProducer::<Msg>::create(&ring, 1 << 18).unwrap();
            let mut server = ReplicaServer::bind(&ring, bind.as_str())
                .unwrap()
                .spin(true);
            if let Some(group) = multicast {
                server = server
                    .multicast(MulticastConfig::new(*group.ip(), group.port()).interface(iface));
            }
            server.spawn().unwrap();
            let clock_bind = clock.clone();
            thread::spawn(move || clock_service(clock_bind));
            eprintln!(
                "master: ring {} served on {}, clock on {}",
                ring.display(),
                bind,
                clock
            );

            // Local consumer: the same ring, the same host.
            let total = rate * seconds;
            let mut local = RingConsumer::<Msg>::attach(&ring).unwrap();
            let reader = thread::spawn(move || {
                let mut samples = Vec::with_capacity(total as usize);
                let mut last_seen = Instant::now();
                loop {
                    if let Some(msg) = local.try_recv() {
                        samples.push(wall_ns() - msg.sent_ns as i64);
                        last_seen = Instant::now();
                        if msg.seq == total {
                            break;
                        }
                    } else {
                        if last_seen.elapsed() > Duration::from_secs(30) {
                            break;
                        }
                        core::hint::spin_loop();
                    }
                }
                (samples, local.lapped_count())
            });

            thread::sleep(Duration::from_millis(warmup_ms));
            let period = Duration::from_nanos(1_000_000_000 * burst / rate.max(1));
            let mut next = Instant::now();
            for seq in 1..=total {
                if (seq - 1) % burst == 0 {
                    while Instant::now() < next {
                        core::hint::spin_loop();
                    }
                    next += period;
                }
                producer.push(&Msg {
                    sent_ns: wall_ns() as u64,
                    seq,
                    _pad: [0; 40],
                });
            }
            let (mut samples, lapped) = reader.join().unwrap();
            println!(
                "master pushed {} at {} msg/s (bursts of {}), {} B slots",
                total,
                rate,
                burst,
                std::mem::size_of::<Msg>() + 8
            );
            report(
                "stage master-local-consumer (push -> read, same host)",
                &mut samples,
                &format!("lapped={}", lapped),
            );
            // Keep serving until slaves have drained and reported.
            thread::sleep(Duration::from_secs(3));
        }
        "slave" => {
            let (offset, sync_rtt) = clock_offset(&clock, 400);
            let _ = std::fs::remove_file(&ring);
            let mut mirror = Mirror::builder()
                .start(MirrorStart::Latest)
                .spin(true)
                .interface(iface)
                .connect(source.as_str(), &ring)
                .unwrap();
            let transport = if mirror.is_multicast() {
                "multicast"
            } else {
                "tcp"
            };
            let handle = mirror.handle().unwrap();
            let mirror_thread = thread::spawn(move || {
                let _ = mirror.run();
                (
                    mirror.sequence(),
                    mirror.naks(),
                    mirror.retransmitted(),
                    mirror.gaps(),
                )
            });
            let mut consumer = RingConsumer::<Msg>::attach(&ring).unwrap();
            let mut samples: Vec<i64> = Vec::with_capacity(1 << 20);
            let mut first = None;
            let mut last_seen = Instant::now();
            let start = Instant::now();
            let mut last_seq = 0u64;
            loop {
                if let Some(msg) = consumer.try_recv() {
                    // Slave clock + offset = master clock.
                    samples.push(wall_ns() + offset - msg.sent_ns as i64);
                    last_seen = Instant::now();
                    last_seq = msg.seq;
                    first.get_or_insert(Instant::now());
                } else {
                    if (first.is_some() && last_seen.elapsed() > Duration::from_secs(2))
                        || start.elapsed() > Duration::from_secs(seconds + 40)
                    {
                        break;
                    }
                    core::hint::spin_loop();
                }
            }
            handle.shutdown().unwrap();
            let (seq, naks, retransmitted, gaps) = mirror_thread.join().unwrap();
            report(
                &format!(
                    "stage slave {} ({}, push on master -> read on slave)",
                    name, transport
                ),
                &mut samples,
                &format!(
                    "last_seq={} mirror_seq={} lapped={} naks={} retransmitted={} gaps={} clock_offset_us={:.1} sync_rtt_us={:.1}",
                    last_seq,
                    seq,
                    consumer.lapped_count(),
                    naks,
                    retransmitted,
                    gaps,
                    offset as f64 / 1000.0,
                    sync_rtt as f64 / 1000.0
                ),
            );
            let _ = std::fs::remove_file(&ring);
        }
        other => panic!("unknown role {}", other),
    }
}
