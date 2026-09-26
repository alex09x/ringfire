use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use ringfire::replication::{Mirror, MirrorStart, MulticastConfig, ReplicaServer};
use ringfire::{BlobConsumer, BlobProducer, BlobRecvStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
struct Meta {
    seq: u64,
    kind: u32,
}

/// Deterministic payload for record `seq`: length and bytes both derive from it.
fn payload_len(seq: u64, max: usize) -> usize {
    1 + ((seq * 7919) % max as u64) as usize
}

fn payload(seq: u64, max: usize) -> Vec<u8> {
    (0..payload_len(seq, max))
        .map(|i| (seq.wrapping_mul(31) + i as u64) as u8)
        .collect()
}

fn temp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ringfire_blob_{}_{}.shm", name, std::process::id()))
}

fn multicast(index: u16) -> MulticastConfig {
    let base = 44_000 + (std::process::id() % 2_000) as u16 * 8;
    MulticastConfig::new(Ipv4Addr::new(239, 255, 44, 1 + index as u8), base + index)
}

fn start_server(path: &PathBuf, cfg: Option<MulticastConfig>) -> SocketAddr {
    let mut server = ReplicaServer::bind(path, "127.0.0.1:0").unwrap();
    if let Some(cfg) = cfg {
        server = server.multicast(cfg);
    }
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();
    addr
}

/// Reads records `expected` in order, checking metadata and every payload byte.
fn collect(
    consumer: &mut BlobConsumer<Meta>,
    expected: std::ops::RangeInclusive<u64>,
    max: usize,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    let mut out = vec![0u8; max + 1];
    let mut meta = Meta::default();
    for seq in expected {
        loop {
            match consumer.recv_status(&mut meta, &mut out).unwrap() {
                BlobRecvStatus::Ok { payload_len: n }
                | BlobRecvStatus::Lapped { payload_len: n, .. } => {
                    assert_eq!(
                        meta,
                        Meta {
                            seq,
                            kind: (seq % 5) as u32
                        },
                        "metadata of record {}",
                        seq
                    );
                    assert_eq!(
                        &out[..n],
                        &payload(seq, max)[..],
                        "payload of record {}",
                        seq
                    );
                    break;
                }
                BlobRecvStatus::Empty => {
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for record {}",
                        seq
                    );
                    std::hint::spin_loop();
                }
            }
        }
    }
}

fn push(producer: &mut BlobProducer<Meta>, seq: u64, max: usize) {
    producer
        .push(
            &Meta {
                seq,
                kind: (seq % 5) as u32,
            },
            &payload(seq, max),
        )
        .unwrap();
}

#[test]
fn blob_ring_is_mirrored_over_tcp() {
    let source = temp("tcp_src");
    let copy = temp("tcp_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let max = 3000;
    let mut producer = BlobProducer::<Meta>::create(&source, 4096, 1 << 24).unwrap();
    push(&mut producer, 1, max);
    let addr = start_server(&source, None);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &copy)
        .unwrap();
    assert!(mirror.geometry().has_arena());
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (mirror.sequence(), mirror.gaps())
    });
    let mut consumer = BlobConsumer::<Meta>::attach(&copy).unwrap();
    for seq in 2..=3000 {
        push(&mut producer, seq, max);
    }
    collect(&mut consumer, 1..=3000, max, Duration::from_secs(20));
    assert_eq!(consumer.lapped_count(), 0);
    handle.shutdown().unwrap();
    assert_eq!(runner.join().unwrap(), (3000, 0));
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn blob_ring_is_mirrored_by_multicast_with_payloads_beyond_a_datagram() {
    let source = temp("mc_src");
    let copy = temp("mc_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    // Payloads up to 70,000 bytes: some exceed the MTU (IP fragmentation), a few exceed
    // what one datagram can carry at all and must come back over TCP.
    let max = 70_000;
    let mut producer = BlobProducer::<Meta>::create(&source, 1024, 1 << 26).unwrap();
    let addr = start_server(&source, Some(multicast(0)));
    let mut mirror = match Mirror::builder()
        .start(MirrorStart::Latest)
        .connect(addr, &copy)
    {
        Ok(mirror) => mirror,
        Err(e) => {
            eprintln!("skipping multicast test: {}", e);
            return;
        }
    };
    assert!(mirror.is_multicast());
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (
            mirror.sequence(),
            mirror.naks(),
            mirror.retransmitted(),
            mirror.gaps(),
        )
    });
    let mut consumer = BlobConsumer::<Meta>::attach(&copy).unwrap();
    let n = 600u64;
    for seq in 1..=n {
        push(&mut producer, seq, max);
        thread::sleep(Duration::from_micros(300));
    }
    collect(&mut consumer, 1..=n, max, Duration::from_secs(30));
    assert_eq!(consumer.lapped_count(), 0);
    handle.shutdown().unwrap();
    let (seq, naks, retransmitted, gaps) = runner.join().unwrap();
    assert_eq!(seq, n);
    assert!(
        retransmitted > 0,
        "records larger than a datagram must arrive over TCP"
    );
    assert!(naks > 0);
    assert_eq!(gaps, 0);
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn blob_source_arena_lapping_never_corrupts_the_mirror() {
    let source = temp("lap_src");
    let copy = temp("lap_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    // A 256-slot ring over a 64 KiB arena, flooded before the mirror connects: the oldest
    // retained descriptors name payloads the arena has long overwritten. Those records
    // must turn into gaps, everything else must arrive intact and strictly in order, and
    // the live stream that follows must be complete.
    let max = 1500;
    let mut producer = BlobProducer::<Meta>::create(&source, 256, 1 << 16).unwrap();
    let flood = 50_000u64;
    for seq in 1..=flood {
        push(&mut producer, seq, max);
    }
    let addr = start_server(&source, None);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &copy)
        .unwrap();
    assert!(mirror.first_sequence() <= flood);
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (mirror.sequence(), mirror.gaps())
    });
    let mut consumer = BlobConsumer::<Meta>::attach(&copy).unwrap();
    let live = flood + 2_000;
    for seq in flood + 1..=live {
        push(&mut producer, seq, max);
        if seq.is_multiple_of(50) {
            thread::sleep(Duration::from_micros(200));
        }
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut out = vec![0u8; max + 1];
    let mut meta = Meta::default();
    let mut last = 0u64;
    let mut delivered = 0u64;
    while last < live {
        match consumer.recv_status(&mut meta, &mut out).unwrap() {
            BlobRecvStatus::Ok { payload_len: len }
            | BlobRecvStatus::Lapped {
                payload_len: len, ..
            } => {
                assert!(
                    meta.seq > last,
                    "sequence went backwards: {} after {}",
                    meta.seq,
                    last
                );
                assert_eq!(
                    &out[..len],
                    &payload(meta.seq, max)[..],
                    "torn payload of record {}",
                    meta.seq
                );
                last = meta.seq;
                delivered += 1;
            }
            BlobRecvStatus::Empty => {
                assert!(Instant::now() < deadline, "stalled at {} of {}", last, live);
                std::hint::spin_loop();
            }
        }
    }
    assert!(
        delivered < 256 + 2_000,
        "the flooded arena cannot have kept every retained payload"
    );
    handle.shutdown().unwrap();
    let (seq, gaps) = runner.join().unwrap();
    assert_eq!(seq, live);
    assert!(gaps > 0, "overwritten payloads must be reported as gaps");
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn blob_mirror_readers_skip_the_hole_of_a_late_join() {
    let source = temp("hole_src");
    let copy = temp("hole_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let max = 500;
    let mut producer = BlobProducer::<Meta>::create(&source, 1024, 1 << 22).unwrap();
    for seq in 1..=100 {
        push(&mut producer, seq, max);
    }
    let addr = start_server(&source, None);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Latest)
        .connect(addr, &copy)
        .unwrap();
    assert_eq!(mirror.first_sequence(), 101);
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || mirror.run());
    // Attached to an empty mirror: its cursor starts at 1, which this ring never gets.
    let mut consumer = BlobConsumer::<Meta>::attach(&copy).unwrap();
    for seq in 101..=160 {
        push(&mut producer, seq, max);
    }
    collect(&mut consumer, 101..=160, max, Duration::from_secs(20));
    assert_eq!(
        consumer.lapped_count(),
        100,
        "the hole 1..=100 is reported as skipped"
    );
    handle.shutdown().unwrap();
    runner.join().unwrap().unwrap();
    let _ = std::fs::remove_file(&copy);
}
