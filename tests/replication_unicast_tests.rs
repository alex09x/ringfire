use std::net::SocketAddr;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

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
    std::env::temp_dir().join(format!("ringfire_uc_{}_{}.shm", name, std::process::id()))
}

fn udp_port(index: u16) -> u16 {
    46_000 + (std::process::id() % 2_000) as u16 * 4 + index
}

fn start_server(path: &PathBuf, port: u16, duplicate: u8) -> SocketAddr {
    let server = ReplicaServer::bind(path, "127.0.0.1:0")
        .unwrap()
        .unicast(port, 1472)
        .duplicate(duplicate);
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();
    addr
}

fn collect(
    consumer: &mut RingConsumer<Tick>,
    expected: std::ops::RangeInclusive<u64>,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    for seq in expected {
        loop {
            if let Some(tick) = consumer.try_recv() {
                assert_eq!(tick, Tick::nth(seq), "record {} does not match", seq);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for record {}",
                seq
            );
            std::hint::spin_loop();
        }
    }
}

fn push_paced(producer: &mut RingProducer<Tick>, range: std::ops::RangeInclusive<u64>) {
    for seq in range {
        producer.push(&Tick::nth(seq));
        if seq.is_multiple_of(64) {
            thread::sleep(Duration::from_micros(200));
        }
    }
}

#[test]
fn unicast_delivers_in_order_and_drops_duplicates() {
    let source = temp("dup_src");
    let copy = temp("dup_dst");
    let _ = std::fs::remove_file(&source);
    let _ = std::fs::remove_file(&copy);

    let mut producer = RingProducer::<Tick>::create(&source, 1 << 16).unwrap();
    // Every datagram is sent twice; the mirror must write each record exactly once.
    let addr = start_server(&source, udp_port(0), 2);
    let mut mirror = Mirror::builder()
        .start(MirrorStart::Latest)
        .unicast(true)
        .connect(addr, &copy)
        .unwrap();
    assert!(mirror.is_unicast());
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
    // The punch takes up to 200 ms to reach the source; records before that arrive by NAK.
    let n = 20_000u64;
    push_paced(&mut producer, 1..=n);
    collect(&mut consumer, 1..=n, Duration::from_secs(20));
    assert_eq!(consumer.lapped_count(), 0);
    handle.shutdown().unwrap();
    let (seq, datagrams, _naks, gaps) = runner.join().unwrap();
    assert_eq!(seq, n);
    assert!(datagrams > 0, "no unicast datagrams arrived");
    assert_eq!(gaps, 0);
    let _ = std::fs::remove_file(&copy);
}

#[test]
fn a_mirror_can_be_served_again_as_a_source() {
    // master ring -> unicast -> hub mirror -> served again -> leaf mirror: the leaf must
    // hold the same records under the same sequence numbers as the master.
    let source = temp("cascade_src");
    let hub = temp("cascade_hub");
    let leaf = temp("cascade_leaf");
    for p in [&source, &hub, &leaf] {
        let _ = std::fs::remove_file(p);
    }

    let mut producer = RingProducer::<Tick>::create(&source, 1 << 14).unwrap();
    push_paced(&mut producer, 1..=500);
    let master = start_server(&source, udp_port(1), 1);
    let mut hub_mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .unicast(true)
        .connect(master, &hub)
        .unwrap();
    let hub_handle = hub_mirror.handle().unwrap();
    let hub_runner = thread::spawn(move || {
        hub_mirror.run().unwrap();
        hub_mirror.sequence()
    });
    // The hub serves its mirror ring like any ring.
    let hub_server = start_server(&hub, udp_port(2), 1);
    let mut leaf_mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .unicast(true)
        .connect(hub_server, &leaf)
        .unwrap();
    assert!(leaf_mirror.is_unicast());
    let leaf_handle = leaf_mirror.handle().unwrap();
    let leaf_runner = thread::spawn(move || {
        leaf_mirror.run().unwrap();
        (leaf_mirror.sequence(), leaf_mirror.gaps())
    });
    let mut consumer = RingConsumer::<Tick>::attach(&leaf).unwrap();
    push_paced(&mut producer, 501..=4_000);
    collect(&mut consumer, 1..=4_000, Duration::from_secs(30));
    assert_eq!(consumer.lapped_count(), 0);
    leaf_handle.shutdown().unwrap();
    let (leaf_seq, leaf_gaps) = leaf_runner.join().unwrap();
    hub_handle.shutdown().unwrap();
    let hub_seq = hub_runner.join().unwrap();
    assert_eq!((hub_seq, leaf_seq, leaf_gaps), (4_000, 4_000, 0));
    for p in [&hub, &leaf] {
        let _ = std::fs::remove_file(p);
    }
}
