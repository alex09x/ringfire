use std::net::SocketAddr;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use ringfire::header::FLAG_SPARSE;
use ringfire::replication::{Mirror, MirrorStart, ReplicaServer};
use ringfire::{RingConsumer, RingProducer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct Tick {
    seq: u64,
    px: u64,
    qty: u32,
    side: u8,
}

impl Tick {
    fn nth(seq: u64) -> Self {
        Tick {
            seq,
            px: seq * 3,
            qty: seq as u32,
            side: (seq % 2) as u8,
        }
    }
}

fn temp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ringfire_repl_{}_{}.shm", name, std::process::id()))
}

fn start_server(path: &PathBuf) -> SocketAddr {
    let server = ReplicaServer::bind(path, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();
    addr
}

/// Reads `expected.len()` ticks in order from `consumer`, failing after `timeout`.
fn collect(
    consumer: &mut RingConsumer<Tick>,
    expected: std::ops::RangeInclusive<u64>,
    timeout: Duration,
) -> Vec<Tick> {
    let deadline = Instant::now() + timeout;
    let mut got = Vec::new();
    for seq in expected {
        loop {
            if let Some(tick) = consumer.try_recv() {
                assert_eq!(tick, Tick::nth(seq), "record {} does not match", seq);
                got.push(tick);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for record {} (got {})",
                seq,
                got.len()
            );
            std::hint::spin_loop();
        }
    }
    got
}

/// `RingHeader` fields read from the file bytes (a `Vec<u8>` is not 128-byte aligned).
fn header_flags(path: &PathBuf) -> u32 {
    let bytes = std::fs::read(path).unwrap();
    u32::from_le_bytes(bytes[48..52].try_into().unwrap())
}

fn header_slots_offset(bytes: &[u8]) -> usize {
    u64::from_le_bytes(bytes[104..112].try_into().unwrap()) as usize
}

#[test]
fn mirror_replicates_every_record_in_order_and_byte_for_byte() {
    let source = temp("exact_src");
    let copy = temp("exact_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    // More slots than records: the server-side reader can never be lapped, so the copy
    // must be complete and gap-free whatever the scheduling.
    let mut producer = RingProducer::<Tick>::create(&source, 8192).unwrap();
    let addr = start_server(&source);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &copy)
        .unwrap();
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        let result = mirror.run();
        (result, mirror.sequence(), mirror.gaps())
    });

    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    assert_eq!(header_flags(&copy) & FLAG_SPARSE, FLAG_SPARSE);
    let n = 5000u64;
    for seq in 1..=n {
        producer.push(&Tick::nth(seq));
    }
    let got = collect(&mut consumer, 1..=n, Duration::from_secs(10));
    assert_eq!(got.len(), n as usize);
    assert_eq!(consumer.lapped_count(), 0);

    // Every retained slot of the mirror equals the source's, including sequence words.
    let src = std::fs::read(&source).unwrap();
    let dst = std::fs::read(&copy).unwrap();
    let slots = header_slots_offset(&src);
    assert_eq!(src.len(), dst.len());
    assert_eq!(&src[slots..], &dst[slots..], "slot regions differ");

    handle.shutdown().unwrap();
    let (result, seq, gaps) = runner.join().unwrap();
    result.unwrap();
    assert_eq!(seq, n);
    assert_eq!(gaps, 0);
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn late_mirror_starts_from_oldest_retained() {
    let source = temp("oldest_src");
    let copy = temp("oldest_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 256).unwrap();
    for seq in 1..=100 {
        producer.push(&Tick::nth(seq));
    }
    let addr = start_server(&source);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &copy)
        .unwrap();
    assert_eq!(mirror.first_sequence(), 1);
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || mirror.run());

    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    collect(&mut consumer, 1..=100, Duration::from_secs(10));
    for seq in 101..=120 {
        producer.push(&Tick::nth(seq));
    }
    collect(&mut consumer, 101..=120, Duration::from_secs(10));
    assert_eq!(consumer.lapped_count(), 0);

    handle.shutdown().unwrap();
    runner.join().unwrap().unwrap();
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn latest_mirror_skips_history_and_readers_skip_the_hole() {
    let source = temp("latest_src");
    let copy = temp("latest_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 1024).unwrap();
    for seq in 1..=100 {
        producer.push(&Tick::nth(seq));
    }
    let addr = start_server(&source);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Latest)
        .connect(addr, &copy)
        .unwrap();
    assert_eq!(mirror.first_sequence(), 101);
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || mirror.run());

    // Attached to an empty mirror: its cursor starts at 1, a sequence this ring never gets.
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    for seq in 101..=150 {
        producer.push(&Tick::nth(seq));
    }
    let got = collect(&mut consumer, 101..=150, Duration::from_secs(10));
    assert_eq!(got.first().unwrap().seq, 101);
    assert_eq!(
        consumer.lapped_count(),
        100,
        "the hole 1..=100 is reported as skipped"
    );

    handle.shutdown().unwrap();
    runner.join().unwrap().unwrap();
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn resume_continues_after_reconnect_without_duplicates() {
    let source = temp("resume_src");
    let copy = temp("resume_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 1024).unwrap();
    let addr = start_server(&source);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &copy)
        .unwrap();
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        mirror.sequence()
    });
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    for seq in 1..=100 {
        producer.push(&Tick::nth(seq));
    }
    collect(&mut consumer, 1..=100, Duration::from_secs(10));
    handle.shutdown().unwrap();
    assert_eq!(runner.join().unwrap(), 100);

    // Messages published while the mirror was down are recovered from the source ring.
    for seq in 101..=200 {
        producer.push(&Tick::nth(seq));
    }
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Resume)
        .connect(addr, &copy)
        .unwrap();
    assert!(mirror.resumed());
    assert_eq!(mirror.first_sequence(), 101);
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (mirror.sequence(), mirror.gaps())
    });
    for seq in 201..=250 {
        producer.push(&Tick::nth(seq));
    }
    collect(&mut consumer, 101..=250, Duration::from_secs(10));
    assert_eq!(consumer.lapped_count(), 0);
    handle.shutdown().unwrap();
    assert_eq!(runner.join().unwrap(), (250, 0));
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn resume_ahead_of_a_restarted_source_starts_over() {
    let source = temp("restart_src");
    let copy = temp("restart_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 256).unwrap();
    let addr = start_server(&source);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &copy)
        .unwrap();
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || mirror.run());
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    for seq in 1..=50 {
        producer.push(&Tick::nth(seq));
    }
    collect(&mut consumer, 1..=50, Duration::from_secs(10));
    handle.shutdown().unwrap();
    runner.join().unwrap().unwrap();

    // The source ring is recreated from sequence 1: the mirror must not wait for 51.
    drop(producer);
    let _ = std::fs::remove_file(&source);
    let mut producer = RingProducer::<Tick>::create(&source, 256).unwrap();
    let addr = start_server(&source);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Resume)
        .connect(addr, &copy)
        .unwrap();
    assert!(!mirror.resumed());
    assert_eq!(mirror.first_sequence(), 1);
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || mirror.run());
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    for seq in 1..=20 {
        producer.push(&Tick::nth(seq));
    }
    collect(&mut consumer, 1..=20, Duration::from_secs(10));
    handle.shutdown().unwrap();
    runner.join().unwrap().unwrap();
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn lapped_source_resynchronizes_without_torn_records() {
    let source = temp("lap_src");
    let copy = temp("lap_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    // A tiny ring and a producer that never waits: the server-side reader is lapped
    // constantly. Every record the mirror delivers must still be intact and in order.
    let mut producer = RingProducer::<Tick>::create(&source, 16).unwrap();
    let addr = start_server(&source);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &copy)
        .unwrap();
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (mirror.sequence(), mirror.gaps())
    });
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    let n = 200_000u64;
    let pusher = thread::spawn(move || {
        for seq in 1..=n {
            producer.push(&Tick::nth(seq));
        }
        producer
    });

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = 0u64;
    let mut received = 0u64;
    while last < n {
        if let Some(tick) = consumer.try_recv() {
            assert!(
                tick.seq > last,
                "sequence went backwards: {} after {}",
                tick.seq,
                last
            );
            assert_eq!(tick, Tick::nth(tick.seq), "torn record");
            last = tick.seq;
            received += 1;
        } else {
            assert!(Instant::now() < deadline, "stalled at {} of {}", last, n);
            std::hint::spin_loop();
        }
    }
    let _producer = pusher.join().unwrap();
    assert!(received <= n);
    handle.shutdown().unwrap();
    let (seq, _gaps) = runner.join().unwrap();
    assert_eq!(seq, n);
    let _ = std::fs::remove_file(&copy);
}
