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
//!              [--multicast 239.255.1.1:7410 --iface A] [--udp 7403 --dup 2] --rate 1000 --seconds 5
//! slave:   replication_stages --role slave --source A:7400 --clock A:7402 [--iface B] \
//!              [--unicast 1] --ring /dev/shm/stage_s1.shm --name s1
//! ```
//!
//! Every slave also echoes each record it reads back over its clock connection; the
//! master stamps the echo's arrival with the same clock that stamped the push, so the
//! "round trip" line per slave is exact even across sites with unsynchronised clocks.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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

/// Round trips seen by the master: per slave (identified by its 8-byte name), the push →
/// echo-arrival latencies in the master's clock.
type Echoes = Arc<Mutex<Vec<(String, Vec<i64>)>>>;

/// Answers clock probes and collects echoes. Requests are 24 bytes: `kind u64, a u64,
/// b [u8; 8]`. kind 0 = probe (`a` = send time, replied with receive and send times),
/// kind 1 = echo (`a` = sequence read, `b` = slave name, no reply).
fn clock_service(bind: String, sent: Arc<Mutex<Vec<i64>>>, echoes: Echoes) {
    let listener = TcpListener::bind(bind).unwrap();
    for stream in listener.incoming().flatten() {
        let sent = sent.clone();
        let echoes = echoes.clone();
        thread::spawn(move || {
            let mut stream = stream;
            stream.set_nodelay(true).ok();
            let mut req = [0u8; 24];
            let mut mine: Vec<i64> = Vec::new();
            let mut name = String::new();
            while stream.read_exact(&mut req).is_ok() {
                let kind = u64::from_le_bytes(req[0..8].try_into().unwrap());
                let a = u64::from_le_bytes(req[8..16].try_into().unwrap());
                match kind {
                    0 => {
                        let t2 = wall_ns();
                        let mut reply = [0u8; 24];
                        reply[0..8].copy_from_slice(&a.to_le_bytes());
                        reply[8..16].copy_from_slice(&t2.to_le_bytes());
                        reply[16..24].copy_from_slice(&wall_ns().to_le_bytes());
                        if stream.write_all(&reply).is_err() {
                            break;
                        }
                    }
                    1 => {
                        let now = wall_ns();
                        if name.is_empty() {
                            name = String::from_utf8_lossy(&req[16..24])
                                .trim_end_matches('\0')
                                .to_string();
                        }
                        let sent = sent.lock().unwrap();
                        if let Some(&t) = sent.get(a as usize)
                            && t != 0
                        {
                            mine.push(now - t);
                        }
                    }
                    _ => {}
                }
            }
            if !mine.is_empty() {
                echoes.lock().unwrap().push((name, mine));
            }
        });
    }
}

/// Estimates `master clock - slave clock` from the minimum-round-trip probe.
fn clock_offset(clock: &str, probes: usize) -> (i64, i64, TcpStream) {
    let mut stream = TcpStream::connect(clock).unwrap();
    stream.set_nodelay(true).unwrap();
    let mut best_rtt = i64::MAX;
    let mut best_offset = 0i64;
    let mut reply = [0u8; 24];
    let mut req = [0u8; 24];
    // Bounded in time as well: across an ocean each probe is a 100 ms round trip.
    let budget = Instant::now() + Duration::from_secs(2);
    for _ in 0..probes {
        if Instant::now() > budget {
            break;
        }
        let t1 = wall_ns();
        req[8..16].copy_from_slice(&t1.to_le_bytes());
        stream.write_all(&req).unwrap();
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
    (best_offset, best_rtt, stream)
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
    let mut udp_port: Option<u16> = None;
    let mut dup = 1u8;
    let mut unicast = false;
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
            "--udp" => udp_port = Some(args[i + 1].parse().unwrap()),
            "--dup" => dup = args[i + 1].parse().unwrap(),
            "--unicast" => unicast = args[i + 1] == "1" || args[i + 1] == "true",
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
            if let Some(port) = udp_port {
                server = server.unicast(port, 1400);
            }
            server = server.duplicate(dup);
            server.spawn().unwrap();
            let sent: Arc<Mutex<Vec<i64>>> =
                Arc::new(Mutex::new(vec![0i64; (rate * seconds + 1) as usize]));
            let echoes: Echoes = Arc::new(Mutex::new(Vec::new()));
            let clock_bind = clock.clone();
            let (sent_c, echoes_c) = (sent.clone(), echoes.clone());
            thread::spawn(move || clock_service(clock_bind, sent_c, echoes_c));
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
                let now = wall_ns();
                sent.lock().unwrap()[seq as usize] = now;
                producer.push(&Msg {
                    sent_ns: now as u64,
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
            // Keep serving until slaves have drained, echoed and reported.
            thread::sleep(Duration::from_secs(4));
            let mut echoes = echoes.lock().unwrap();
            echoes.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, rtt) in echoes.iter_mut() {
                report(
                    &format!(
                        "stage round-trip via slave {} (push on master -> read on slave -> echo, master clock)",
                        name
                    ),
                    rtt,
                    "",
                );
            }
        }
        "slave" => {
            let (offset, sync_rtt, mut echo) = clock_offset(&clock, 400);
            let _ = std::fs::remove_file(&ring);
            let mut mirror = Mirror::builder()
                .start(MirrorStart::Latest)
                .spin(true)
                .interface(iface)
                .unicast(unicast)
                .connect(source.as_str(), &ring)
                .unwrap();
            let transport = if mirror.is_unicast() {
                "udp unicast"
            } else if mirror.is_multicast() {
                "multicast"
            } else {
                "tcp"
            };
            let mut echo_req = [0u8; 24];
            echo_req[0..8].copy_from_slice(&1u64.to_le_bytes());
            for (i, b) in name.bytes().take(8).enumerate() {
                echo_req[16 + i] = b;
            }
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
                    echo_req[8..16].copy_from_slice(&msg.seq.to_le_bytes());
                    let _ = echo.write_all(&echo_req);
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
