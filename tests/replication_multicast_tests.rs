use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use ringfire::replication::{Mirror, MirrorStart, MulticastConfig, ReplicaServer};
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
    std::env::temp_dir().join(format!("ringfire_mc_{}_{}.shm", name, std::process::id()))
}

/// Each test gets its own group and port: on Linux a socket bound to a port receives every
/// group the host joined on that port, so tests must not share ports.
fn multicast(index: u16) -> MulticastConfig {
    let base = 42_000 + (std::process::id() % 2_000) as u16 * 8;
    MulticastConfig::new(Ipv4Addr::new(239, 255, 42, 1 + index as u8), base + index)
}

fn start_server(path: &PathBuf, cfg: MulticastConfig) -> SocketAddr {
    let server = ReplicaServer::bind(path, "127.0.0.1:0")
        .unwrap()
        .multicast(cfg);
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();
    addr
}

/// Reads records `expected` in order from `consumer`, failing after `timeout`.
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

/// Pushes `range` with a short pause every few records so the multicast sender is never
/// lapped by the producer (loss is injected deliberately where a test wants it).
fn push_paced(producer: &mut RingProducer<Tick>, range: std::ops::RangeInclusive<u64>) {
    for seq in range {
        producer.push(&Tick::nth(seq));
        if seq.is_multiple_of(64) {
            thread::sleep(Duration::from_micros(200));
        }
    }
}

/// Multicast needs a multicast-capable route; skip (not fail) where there is none.
fn connect_or_skip(addr: SocketAddr, copy: &PathBuf, start: MirrorStart) -> Option<Mirror> {
    match Mirror::builder().start(start).connect(addr, copy) {
        Ok(mirror) => {
            assert!(mirror.is_multicast());
            Some(mirror)
        }
        Err(e) => {
            eprintln!("skipping multicast test: {}", e);
            None
        }
    }
}

#[test]
fn multicast_delivers_live_records_in_order() {
    let source = temp("live_src");
    let copy = temp("live_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 1 << 16).unwrap();
    let addr = start_server(&source, multicast(0));
    let Some(mut mirror) = connect_or_skip(addr, &copy, MirrorStart::Latest) else {
        return;
    };
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (
            mirror.sequence(),
            mirror.datagrams(),
            mirror.naks(),
            mirror.gaps(),
        )
    });
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    let n = 20_000u64;
    push_paced(&mut producer, 1..=n);
    collect(&mut consumer, 1..=n, Duration::from_secs(20));
    assert_eq!(consumer.lapped_count(), 0);
    handle.shutdown().unwrap();
    let (seq, datagrams, _naks, gaps) = runner.join().unwrap();
    assert_eq!(seq, n);
    assert!(datagrams > 0, "no multicast datagrams arrived");
    assert_eq!(gaps, 0);
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn multicast_history_is_fetched_over_tcp_then_live_continues() {
    let source = temp("history_src");
    let copy = temp("history_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 1 << 16).unwrap();
    push_paced(&mut producer, 1..=5_000);
    let addr = start_server(&source, multicast(1));
    let Some(mut mirror) = connect_or_skip(addr, &copy, MirrorStart::Oldest) else {
        return;
    };
    assert_eq!(mirror.first_sequence(), 1);
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
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    // History arrives by NAK over TCP (there is nothing on the wire yet), live by multicast.
    push_paced(&mut producer, 5_001..=10_000);
    collect(&mut consumer, 1..=10_000, Duration::from_secs(20));
    assert_eq!(consumer.lapped_count(), 0);
    handle.shutdown().unwrap();
    let (seq, naks, retransmitted, gaps) = runner.join().unwrap();
    assert_eq!(seq, 10_000);
    assert!(naks >= 1, "history must be requested");
    assert!(
        retransmitted >= 5_000,
        "history must arrive over TCP, got {}",
        retransmitted
    );
    assert_eq!(gaps, 0);
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn multicast_recovers_dropped_datagrams_in_order() {
    let source = temp("loss_src");
    let copy = temp("loss_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 1 << 16).unwrap();
    // Every third datagram is dropped on purpose; the mirror must NAK and still deliver
    // everything in order.
    let addr = start_server(&source, multicast(2).drop_every(3));
    let Some(mut mirror) = connect_or_skip(addr, &copy, MirrorStart::Latest) else {
        return;
    };
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
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    let n = 20_000u64;
    push_paced(&mut producer, 1..=n);
    collect(&mut consumer, 1..=n, Duration::from_secs(30));
    assert_eq!(consumer.lapped_count(), 0);
    handle.shutdown().unwrap();
    let (seq, naks, retransmitted, gaps) = runner.join().unwrap();
    assert_eq!(seq, n);
    assert!(naks > 0, "losses must be requested");
    assert!(retransmitted > 0, "losses must be retransmitted");
    assert_eq!(gaps, 0);
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn multicast_last_datagram_loss_is_recovered_by_heartbeat() {
    let source = temp("tail_src");
    let copy = temp("tail_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 1 << 12).unwrap();
    // Drop every second datagram: with one record per push and pauses in between, the
    // lost datagram is regularly the last one, so only the heartbeat can reveal it.
    let addr = start_server(&source, multicast(3).drop_every(2));
    let Some(mut mirror) = connect_or_skip(addr, &copy, MirrorStart::Latest) else {
        return;
    };
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (mirror.sequence(), mirror.naks())
    });
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    for seq in 1..=200u64 {
        producer.push(&Tick::nth(seq));
        thread::sleep(Duration::from_millis(3));
    }
    collect(&mut consumer, 1..=200, Duration::from_secs(20));
    handle.shutdown().unwrap();
    let (seq, naks) = runner.join().unwrap();
    assert_eq!(seq, 200);
    assert!(naks > 0);
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn multicast_lapped_source_yields_gaps_but_never_disorder() {
    let source = temp("mclap_src");
    let copy = temp("mclap_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    // A tiny ring flooded without pauses: the multicast sender is lapped constantly, so
    // mirrors see jumps, NAK old ranges and get GAP for what is gone. Every delivered
    // record must still be intact and strictly increasing.
    let mut producer = RingProducer::<Tick>::create(&source, 16).unwrap();
    let addr = start_server(&source, multicast(4));
    let Some(mut mirror) = connect_or_skip(addr, &copy, MirrorStart::Oldest) else {
        return;
    };
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (mirror.sequence(), mirror.gaps(), mirror.naks())
    });
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    let n = 100_000u64;
    let pusher = thread::spawn(move || {
        for seq in 1..=n {
            producer.push(&Tick::nth(seq));
        }
        // Keep the ring alive (and quiet) so the last records can still be fetched.
        thread::sleep(Duration::from_secs(2));
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
    let (seq, gaps, naks) = runner.join().unwrap();
    assert_eq!(seq, n);
    assert!(gaps > 0, "a 16-slot ring at full speed must lose history");
    assert!(naks > 0);
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn multicast_ignores_datagrams_from_another_session() {
    let source = temp("session_src");
    let copy = temp("session_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 1024).unwrap();
    let cfg = multicast(5);
    let addr = start_server(&source, cfg);
    let Some(mut mirror) = connect_or_skip(addr, &copy, MirrorStart::Latest) else {
        return;
    };
    let session = mirror.session();
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();

    // Stray DATA datagrams on the same group and port, stamped with another session
    // (a restarted source, or another stream): a far-ahead sequence that must neither
    // be written nor be asked for. The mirror is driven from this thread so its
    // counters can be checked directly.
    let stray = UdpSocket::bind("0.0.0.0:0").unwrap();
    stray.set_multicast_loop_v4(true).unwrap();
    let payload_len = std::mem::size_of::<Tick>();
    let mut frame = vec![0u8; 16 + payload_len];
    frame[0] = 3; // DATA
    frame[1] = session.wrapping_add(1);
    frame[2..4].copy_from_slice(&1u16.to_le_bytes());
    frame[4..8].copy_from_slice(&(payload_len as u32).to_le_bytes());
    frame[8..16].copy_from_slice(&999_999u64.to_le_bytes());
    let until = Instant::now() + Duration::from_millis(300);
    let mut sent = 0;
    while Instant::now() < until {
        if sent < 20 {
            stray.send_to(&frame, (cfg.group, cfg.port)).unwrap();
            sent += 1;
        }
        assert!(mirror.step().unwrap());
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        mirror.datagrams() > 0,
        "the mirror should have seen the stray datagrams"
    );
    assert_eq!(mirror.naks(), 0, "a foreign session must not cause NAKs");
    assert_eq!(mirror.sequence(), 0);
    assert_eq!(
        consumer.try_recv(),
        None,
        "stray record must not be written"
    );

    // The real source still works and the stray sequence left no trace.
    for seq in 1..=100 {
        producer.push(&Tick::nth(seq));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while mirror.sequence() < 100 {
        assert!(
            Instant::now() < deadline,
            "records did not arrive: {:?}",
            mirror
        );
        assert!(mirror.step().unwrap());
    }
    collect(&mut consumer, 1..=100, Duration::from_secs(10));
    assert_eq!(mirror.gaps(), 0);
    assert_eq!(
        mirror.naks(),
        0,
        "no NAK expected on an intact live stream: {:?}, last nak {:?}, datagrams {}, retransmitted {}",
        mirror,
        mirror.last_nak(),
        mirror.datagrams(),
        mirror.retransmitted()
    );
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn multicast_reordered_datagrams_are_written_in_order() {
    let source = temp("swap_src");
    let copy = temp("swap_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    // Every fourth datagram goes out after the one that follows it, so the mirror keeps
    // seeing a later sequence before an earlier one. The ring must still be written
    // strictly in order and no record may be lost.
    let mut producer = RingProducer::<Tick>::create(&source, 1 << 16).unwrap();
    let addr = start_server(&source, multicast(6).swap_every(4));
    let Some(mut mirror) = connect_or_skip(addr, &copy, MirrorStart::Latest) else {
        return;
    };
    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        mirror.run().unwrap();
        (mirror.sequence(), mirror.datagrams(), mirror.gaps())
    });
    let mut consumer = RingConsumer::<Tick>::attach(&copy).unwrap();
    let n = 20_000u64;
    push_paced(&mut producer, 1..=n);
    // `collect` asserts every record arrives exactly in sequence order.
    collect(&mut consumer, 1..=n, Duration::from_secs(30));
    assert_eq!(consumer.lapped_count(), 0);
    handle.shutdown().unwrap();
    let (seq, datagrams, gaps) = runner.join().unwrap();
    assert_eq!(seq, n);
    assert!(datagrams > 0);
    assert_eq!(gaps, 0);
    let _ = std::fs::remove_file(&copy);
}
