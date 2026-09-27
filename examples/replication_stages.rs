//! Per-stage latency of replication, measured on several hosts at once.
//!
//! The master stamps every record with its wall clock immediately before it pushes it
//! into the source ring (actual push time, not scheduled time). A local consumer on the
//! master reads the same ring and measures push → read on the same host. Each slave
//! mirrors the ring and a local consumer on the slave measures push on the master → read
//! on the slave, translating its own clock into the master's with an offset estimated
//! PTP-style (hundreds of probes, the minimum-round-trip sample wins; the residual error
//! is the path asymmetry, a few microseconds on a LAN). The slave estimates the offset
//! again after the run and reports the change, which bounds the clock drift during it.
//!
//! ```text
//! master:  replication_stages --role master --bind 0.0.0.0:7400 --clock 0.0.0.0:7402 \
//!              [--multicast 239.255.1.1:7410 --iface A] [--udp 7403 --dup 2] --rate 1000 --seconds 5 \
//!              [--drain-secs 30]
//! slave:   replication_stages --role slave --source A:7400 --clock A:7402 [--iface B] \
//!              [--unicast 1] --ring /dev/shm/stage_s1.shm --name s1
//! ```
//!
//! Every slave also echoes each record it reads back over its clock connection; the
//! master stamps the echo's arrival with the same clock that stamped the push, so the
//! "round trip" line per slave is exact even across sites with unsynchronised clocks.
//! After pushing, the master waits until every slave has disconnected (slaves leave 2 s
//! after their last record) or `--drain-secs` pass, then reports.

mod support;

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ringfire::replication::{Mirror, MirrorStart, MulticastConfig, ReplicaServer};
use ringfire::{RingConsumer, RingProducer};
use support::{latency_line, parse_arg, parse_flag, Pacer, SeqTracker};

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

fn report(label: &str, samples: &mut [i64], extra: &str) {
    println!("{}: {} {}", label, latency_line(samples), extra);
}

/// Round trips seen by the master: per slave (identified by its 8-byte name), the push →
/// echo-arrival latencies in the master's clock and the distinct sequences echoed.
type Echoes = Arc<Mutex<Vec<(String, Vec<i64>, SeqTracker)>>>;

/// Push timestamps by sequence, written by the pusher and read by the echo handlers
/// without a lock (a lock here could delay the push after the stamp was taken).
type SentTimes = Arc<Vec<AtomicI64>>;

/// Answers clock probes and collects echoes. Requests are 24 bytes: `kind u64, a u64,
/// b [u8; 8]`. kind 0 = probe (`a` = send time, replied with receive and send times),
/// kind 1 = echo (`a` = sequence read, `b` = slave name, no reply). `active` counts open
/// connections.
fn clock_service(listener: TcpListener, sent: SentTimes, echoes: Echoes, active: Arc<AtomicUsize>) {
    for stream in listener.incoming().flatten() {
        let sent = sent.clone();
        let echoes = echoes.clone();
        let active = active.clone();
        active.fetch_add(1, Ordering::SeqCst);
        thread::spawn(move || {
            let mut stream = stream;
            stream.set_nodelay(true).ok();
            let mut req = [0u8; 24];
            let mut mine: Vec<i64> = Vec::new();
            let mut seqs = SeqTracker::new(sent.len().saturating_sub(1) as u64);
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
                        if seqs.record(a) {
                            let t = sent[a as usize].load(Ordering::Acquire);
                            if t != 0 {
                                mine.push(now - t);
                            }
                        }
                    }
                    _ => {}
                }
            }
            if seqs.unique() + seqs.duplicates() + seqs.out_of_range() > 0 {
                echoes.lock().unwrap().push((name, mine, seqs));
            }
            active.fetch_sub(1, Ordering::SeqCst);
        });
    }
}

/// Connects to `addr` with a bounded connect time, and sets a bounded read/write timeout
/// on the resulting socket so a peer that accepts but then stops responding cannot hang a
/// later `read_exact`/`write_all` on it forever.
fn connect_bounded(addr: &str, timeout: Duration) -> std::io::Result<TcpStream> {
    let target = addr.to_socket_addrs()?.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, format!("no address for {}", addr))
    })?;
    let stream = TcpStream::connect_timeout(&target, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    Ok(stream)
}

/// Estimates `master clock - slave clock` from the minimum-round-trip probe on `stream`.
/// Returns the offset and that probe's round trip. `stream` is expected to already carry
/// a read/write timeout (see [`connect_bounded`]), so a probe whose peer stops responding
/// ends the loop early (returning the best estimate so far) instead of hanging: the
/// per-probe budget below only bounds how many *successful* probes are attempted, not a
/// stalled `read_exact`/`write_all`.
fn clock_offset(stream: &mut TcpStream, probes: usize) -> (i64, i64) {
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
        if stream.write_all(&req).is_err() || stream.read_exact(&mut reply).is_err() {
            break;
        }
        let t4 = wall_ns();
        let t2 = i64::from_le_bytes(reply[8..16].try_into().unwrap());
        let t3 = i64::from_le_bytes(reply[16..24].try_into().unwrap());
        let rtt = (t4 - t1) - (t3 - t2);
        if rtt < best_rtt {
            best_rtt = rtt;
            best_offset = ((t2 - t1) + (t3 - t4)) / 2;
        }
        // Spaces the probes out; not a readiness wait.
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
    let mut ring: Option<PathBuf> = None;
    let mut name = String::from("slave");
    let mut rate = 1000u64;
    let mut seconds = 5u64;
    let mut burst = 1u64;
    let mut warmup_ms = 3000u64;
    let mut drain_secs = 30u64;
    let mut multicast: Option<SocketAddrV4> = None;
    let mut iface = Ipv4Addr::UNSPECIFIED;
    let mut udp_port: Option<u16> = None;
    let mut dup = 1u8;
    let mut unicast = false;
    let mut i = 1;
    while i + 1 < args.len() {
        let v = args[i + 1].as_str();
        match args[i].as_str() {
            "--role" => role = v.to_string(),
            "--bind" => bind = v.to_string(),
            "--clock" => clock = v.to_string(),
            "--source" => source = v.to_string(),
            "--ring" => ring = Some(PathBuf::from(v)),
            "--name" => name = v.to_string(),
            "--rate" => rate = parse_arg("--rate", v),
            "--seconds" => seconds = parse_arg("--seconds", v),
            "--burst" => burst = parse_arg::<u64>("--burst", v).max(1),
            "--warmup-ms" => warmup_ms = parse_arg("--warmup-ms", v),
            "--drain-secs" => drain_secs = parse_arg("--drain-secs", v),
            "--multicast" => multicast = Some(parse_arg("--multicast", v)),
            "--iface" => iface = parse_arg("--iface", v),
            "--udp" => udp_port = Some(parse_arg("--udp", v)),
            "--dup" => dup = parse_arg("--dup", v),
            "--unicast" => unicast = parse_flag(v),
            _ => {}
        }
        i += 2;
    }
    // Default ring path unique per role and process, so a master and a slave on one host
    // (or two runs) never share a file by accident.
    let ring = ring.unwrap_or_else(|| {
        PathBuf::from(format!(
            "/dev/shm/ringfire_stages_{}_{}.shm",
            role,
            std::process::id()
        ))
    });

    match role.as_str() {
        "master" => {
            assert!(rate > 0, "--rate must be positive");
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
            let total = rate * seconds;
            let sent: SentTimes = Arc::new((0..=total).map(|_| AtomicI64::new(0)).collect());
            let echoes: Echoes = Arc::new(Mutex::new(Vec::new()));
            let active = Arc::new(AtomicUsize::new(0));
            let listener = TcpListener::bind(clock.as_str()).unwrap();
            let (sent_c, echoes_c, active_c) = (sent.clone(), echoes.clone(), active.clone());
            thread::spawn(move || clock_service(listener, sent_c, echoes_c, active_c));
            eprintln!(
                "master: ring {} served on {}, clock on {}",
                ring.display(),
                bind,
                clock
            );

            // Local consumer: the same ring, the same host.
            let mut local = RingConsumer::<Msg>::attach(&ring).unwrap();
            let reader = thread::spawn(move || {
                let mut samples = Vec::with_capacity(total as usize);
                let mut seqs = SeqTracker::new(total);
                let mut last_seen = Instant::now();
                loop {
                    if let Some(msg) = local.try_recv() {
                        let now = wall_ns();
                        if seqs.record(msg.seq) {
                            samples.push(now - msg.sent_ns as i64);
                        }
                        last_seen = Instant::now();
                        if msg.seq >= total {
                            break;
                        }
                    } else {
                        if last_seen.elapsed() > Duration::from_secs(30) {
                            break;
                        }
                        core::hint::spin_loop();
                    }
                }
                (samples, seqs, local.lapped_count())
            });

            // Lets slaves connect and mirror before the first record (`Latest` start).
            thread::sleep(Duration::from_millis(warmup_ms));
            let mut pace = Pacer::per_second(rate, burst);
            for seq in 1..=total {
                if (seq - 1) % burst == 0 {
                    pace.wait_next();
                }
                let now = wall_ns();
                sent[seq as usize].store(now, Ordering::Release);
                producer.push(&Msg {
                    sent_ns: now as u64,
                    seq,
                    _pad: [0; 40],
                });
            }
            let (mut samples, seqs, lapped) = reader.join().unwrap();
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
                &format!("lapped={} {}", lapped, seqs.summary(1, total)),
            );
            // Wait for every slave to drain, echo and disconnect, bounded.
            let deadline = Instant::now() + Duration::from_secs(drain_secs);
            while active.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            let still_connected = active.load(Ordering::SeqCst);
            if still_connected > 0 {
                eprintln!(
                    "master: {} slave connection(s) still open after {} s; their round trips are not reported",
                    still_connected, drain_secs
                );
            }
            let mut echoes = echoes.lock().unwrap();
            echoes.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, rtt, seqs) in echoes.iter_mut() {
                report(
                    &format!(
                        "stage round-trip via slave {} (push on master -> read on slave -> echo, master clock)",
                        name
                    ),
                    rtt,
                    &seqs.summary(1, total),
                );
            }
            drop(producer);
            let _ = std::fs::remove_file(&ring);
        }
        "slave" => {
            let mut echo = connect_bounded(clock.as_str(), Duration::from_secs(10)).unwrap();
            echo.set_nodelay(true).unwrap();
            let (offset, sync_rtt) = clock_offset(&mut echo, 400);
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
                let result = mirror.run();
                (
                    result,
                    mirror.sequence(),
                    mirror.naks(),
                    mirror.retransmitted(),
                    mirror.gaps(),
                )
            });
            let mut consumer = RingConsumer::<Msg>::attach(&ring).unwrap();
            let mut samples: Vec<i64> = Vec::with_capacity(1 << 20);
            // Sequences above 2^28 are counted out of range rather than tracked.
            let mut seqs = SeqTracker::new(1 << 28);
            let mut echo_errors = 0u64;
            let mut first = None;
            let mut last_seen = Instant::now();
            let start = Instant::now();
            loop {
                if let Some(msg) = consumer.try_recv() {
                    // Slave clock + offset = master clock.
                    let now = wall_ns();
                    if seqs.record(msg.seq) {
                        samples.push(now + offset - msg.sent_ns as i64);
                    }
                    last_seen = Instant::now();
                    first.get_or_insert(last_seen);
                    echo_req[8..16].copy_from_slice(&msg.seq.to_le_bytes());
                    if echo.write_all(&echo_req).is_err() {
                        echo_errors += 1;
                    }
                } else {
                    if (first.is_some() && last_seen.elapsed() > Duration::from_secs(2))
                        || start.elapsed() > Duration::from_secs(seconds + 40)
                    {
                        break;
                    }
                    core::hint::spin_loop();
                }
            }
            // Offset again: its change over the run bounds the clock drift in the samples.
            let (offset_end, sync_rtt_end) = if echo_errors == 0 {
                clock_offset(&mut echo, 400)
            } else {
                (offset, sync_rtt)
            };
            handle.shutdown().unwrap();
            let (result, seq, naks, retransmitted, gaps) = mirror_thread.join().unwrap();
            report(
                &format!(
                    "stage slave {} ({}, push on master -> read on slave)",
                    name, transport
                ),
                &mut samples,
                &format!(
                    "{} (between first and last seen) last_seq={} mirror_seq={} lapped={} naks={} retransmitted={} gaps={} echo_errors={} clock_offset_us={:.1} sync_rtt_us={:.1} offset_change_us={:.1} (sync_rtt_end_us={:.1})",
                    seqs.summary(seqs.lowest(), seqs.highest()),
                    seqs.highest(),
                    seq,
                    consumer.lapped_count(),
                    naks,
                    retransmitted,
                    gaps,
                    echo_errors,
                    offset as f64 / 1000.0,
                    sync_rtt as f64 / 1000.0,
                    (offset_end - offset) as f64 / 1000.0,
                    sync_rtt_end as f64 / 1000.0
                ),
            );
            drop(consumer);
            let _ = std::fs::remove_file(&ring);
            if let Err(e) = result {
                eprintln!("slave {}: mirror failed: {}", name, e);
                std::process::exit(1);
            }
        }
        other => panic!("unknown role {}", other),
    }
}
