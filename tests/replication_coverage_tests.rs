#[path = "support/deadline.rs"]
mod deadline;

use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use ringfire::error::RingfireError;
use ringfire::header::{FLAG_SPARSE, FLAG_WITH_ARENA, FLAG_WITH_REGISTRY};
use ringfire::replication::{
    Mirror, MirrorStart, MulticastConfig, REPLICATION_MAGIC, REPLICATION_VERSION, ReplicaServer,
};
use ringfire::{BlobConsumer, BlobProducer, RingConsumer, RingConsumerBuilder, RingProducer};

const FRAME_HEADER_LEN: usize = 16;
const HELLO_LEN: usize = 16;
const GEOMETRY_LEN: usize = 48;
const MULTICAST_LEN: usize = 16;

const KIND_HELLO: u8 = 1;
const KIND_GEOMETRY: u8 = 2;
const KIND_DATA: u8 = 3;
const KIND_GAP: u8 = 4;
const KIND_HEARTBEAT: u8 = 5;
const KIND_NAK: u8 = 6;
const KIND_MULTICAST: u8 = 7;
const KIND_PUNCH: u8 = 8;

const GEOMETRY_RESET: u8 = 0x01;
const GEOMETRY_MULTICAST: u8 = 0x02;

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
            px: seq * 10,
            qty: seq as u32,
            side: (seq % 2) as u8,
        }
    }
}

fn temp(name: &str) -> PathBuf {
    static CTR: AtomicU64 = AtomicU64::new(1);
    let id = CTR.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ringfire_cov_{}_{}_{}.shm",
        name,
        std::process::id(),
        id
    ))
}

fn encode_frame(kind: u8, flags: u8, count: u16, len: u32, seq: u64) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0] = kind;
    b[1] = flags;
    b[2..4].copy_from_slice(&count.to_le_bytes());
    b[4..8].copy_from_slice(&len.to_le_bytes());
    b[8..16].copy_from_slice(&seq.to_le_bytes());
    b
}

fn valid_geometry_payload(capacity: u64, element_size: u32) -> [u8; GEOMETRY_LEN] {
    let mut b = [0u8; GEOMETRY_LEN];
    let flags = 0u32;
    let schema_sig = 0u64; // mock wire records are untyped bytes
    let registry_count = 0u32;
    let slots_offset = 128u32;
    let arena_offset = 0u64;
    let arena_size = 0u64;

    b[0..8].copy_from_slice(&capacity.to_le_bytes());
    b[8..12].copy_from_slice(&element_size.to_le_bytes());
    b[12..16].copy_from_slice(&flags.to_le_bytes());
    b[16..24].copy_from_slice(&schema_sig.to_le_bytes());
    b[24..28].copy_from_slice(&registry_count.to_le_bytes());
    b[28..32].copy_from_slice(&slots_offset.to_le_bytes());
    b[32..40].copy_from_slice(&arena_offset.to_le_bytes());
    b[40..48].copy_from_slice(&arena_size.to_le_bytes());
    b
}

#[test]
fn test_server_bind_missing_fails_early() {
    let _deadline = deadline::Deadline::new();
    let p = temp("non_existent_server");
    assert!(ReplicaServer::bind(&p, "127.0.0.1:0").is_err());
}

#[test]
fn test_server_builder_options() {
    let _deadline = deadline::Deadline::new();
    let p = temp("server_opts");
    let _prod = RingProducer::<Tick>::create(&p, 64).unwrap();
    let server = ReplicaServer::bind(&p, "127.0.0.1:0")
        .unwrap()
        .batch(128)
        .spin(true)
        .duplicate(2)
        .linger(Some(Duration::ZERO))
        .unicast(48123, 1400);

    assert_eq!(server.ring_path(), &p);
    assert!(server.local_addr().is_ok());
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_mirror_builder_options_and_getters() {
    let _deadline = deadline::Deadline::new();
    let src = temp("mirror_opts_src");
    let dst = temp("mirror_opts_dst");
    let mut prod = RingProducer::<Tick>::create(&src, 128).unwrap();
    prod.push(&Tick::nth(1));

    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mirror = Mirror::builder()
        .spin(false)
        .file_mode(0o600)
        .interface(Ipv4Addr::LOCALHOST)
        .rcvbuf(65536)
        .nak_timeout(Duration::from_millis(15))
        .start(MirrorStart::Latest)
        .connect(addr, &dst)
        .unwrap();

    assert_eq!(mirror.geometry().capacity, 128);
    assert_eq!(mirror.path(), &dst);
    assert!(mirror.peer_addr().is_ok());
    assert!(!mirror.is_multicast());
    assert!(!mirror.is_unicast());
    assert_eq!(mirror.first_sequence(), 2);
    assert!(!mirror.resumed());
    assert_eq!(mirror.sequence(), 1);
    assert_eq!(mirror.source_sequence(), 1);
    assert_eq!(mirror.gaps(), 0);
    assert_eq!(mirror.frames(), 0);
    assert_eq!(mirror.datagrams(), 0);
    assert_eq!(mirror.naks(), 0);
    assert_eq!(mirror.retransmitted(), 0);

    let debug_str = format!("{:?}", mirror);
    assert!(debug_str.contains("Mirror"));
    assert!(debug_str.contains("multicast: false"));

    let handle = mirror.handle().unwrap();
    handle.shutdown().unwrap();

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_server_serve_one_happy_path() {
    let _deadline = deadline::Deadline::new();
    let src = temp("serve_one_src");
    let dst = temp("serve_one_dst");
    let mut prod = RingProducer::<Tick>::create(&src, 128).unwrap();

    let server = ReplicaServer::bind(&src, "127.0.0.1:0")
        .unwrap()
        .batch(10)
        .linger(Some(Duration::ZERO));
    let addr = server.local_addr().unwrap();

    let server_handle = thread::spawn(move || {
        let result = server.serve_one();
        assert!(
            matches!(result, Err(RingfireError::Io(e)) if matches!(e.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::UnexpectedEof))
        );
    });

    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &dst)
        .unwrap();
    let mirror_handle = mirror.handle().unwrap();

    let dst_clone = dst.clone();
    let runner = thread::spawn(move || {
        while mirror.sequence() < 5 {
            mirror.step().unwrap();
        }
        mirror_handle.shutdown().unwrap();
        mirror.run().unwrap();
        assert_eq!(mirror.path(), dst_clone.as_path());
        assert!(mirror.peer_addr().is_ok());
        let _ = mirror.session();
        let _ = mirror.last_nak();
        let _ = mirror.source_sequence();
        let _ = mirror.frames();
        let _ = mirror.datagrams();
        let _ = mirror.naks();
        let _ = mirror.retransmitted();
        let _ = mirror.gaps();
        let _ = mirror.first_sequence();
        let _ = mirror.resumed();
        let _ = mirror.is_multicast();
        let _ = mirror.is_unicast();
        let _ = mirror.geometry();
        mirror.sequence()
    });

    assert!(ReplicaServer::bind(&src, "invalid:addr:999").is_err());

    for seq in 1..=5 {
        prod.push(&Tick::nth(seq));
    }

    assert_eq!(runner.join().unwrap(), 5);
    server_handle.join().unwrap();

    let mut consumer = RingConsumer::<Tick>::attach(&dst).unwrap();
    for seq in 1..=5 {
        assert_eq!(consumer.try_recv(), Some(Tick::nth(seq)));
    }

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_tcp_handshake_rejects_bad_magic() {
    let _deadline = deadline::Deadline::new();
    let src = temp("hs_bad_magic");
    let _prod = RingProducer::<Tick>::create(&src, 64).unwrap();
    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mut stream = TcpStream::connect(addr).unwrap();
    let hdr = encode_frame(KIND_HELLO, 0, 0, HELLO_LEN as u32, 0);
    stream.write_all(&hdr).unwrap();

    let mut body = [0u8; HELLO_LEN];
    body[0..8].copy_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
    body[8..12].copy_from_slice(&REPLICATION_VERSION.to_le_bytes());
    stream.write_all(&body).unwrap();

    let mut resp = [0u8; 16];
    assert!(matches!(
        stream.read_exact(&mut resp),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof
            || e.kind() == std::io::ErrorKind::ConnectionReset
    ));
    let _ = std::fs::remove_file(&src);
}

#[test]
fn test_tcp_handshake_rejects_bad_version() {
    let _deadline = deadline::Deadline::new();
    let src = temp("hs_bad_ver");
    let _prod = RingProducer::<Tick>::create(&src, 64).unwrap();
    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mut stream = TcpStream::connect(addr).unwrap();
    let hdr = encode_frame(KIND_HELLO, 0, 0, HELLO_LEN as u32, 0);
    stream.write_all(&hdr).unwrap();

    let mut body = [0u8; HELLO_LEN];
    body[0..8].copy_from_slice(&REPLICATION_MAGIC.to_le_bytes());
    body[8..12].copy_from_slice(&999u32.to_le_bytes());
    stream.write_all(&body).unwrap();

    let mut resp = [0u8; 16];
    assert!(matches!(
        stream.read_exact(&mut resp),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof
            || e.kind() == std::io::ErrorKind::ConnectionReset
    ));
    let _ = std::fs::remove_file(&src);
}

#[test]
fn test_tcp_handshake_rejects_wrong_kind() {
    let _deadline = deadline::Deadline::new();
    let src = temp("hs_wrong_kind");
    let _prod = RingProducer::<Tick>::create(&src, 64).unwrap();
    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mut stream = TcpStream::connect(addr).unwrap();
    let hdr = encode_frame(KIND_DATA, 0, 0, 0, 0);
    stream.write_all(&hdr).unwrap();

    let mut resp = [0u8; 16];
    assert!(matches!(
        stream.read_exact(&mut resp),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof
            || e.kind() == std::io::ErrorKind::ConnectionReset
    ));
    let _ = std::fs::remove_file(&src);
}

#[test]
fn test_tcp_handshake_wanted_ahead_sets_reset_flag() {
    let _deadline = deadline::Deadline::new();
    let src = temp("hs_wanted_ahead");
    let _prod = RingProducer::<Tick>::create(&src, 64).unwrap();
    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mut stream = TcpStream::connect(addr).unwrap();
    let hdr = encode_frame(KIND_HELLO, 0, 0, HELLO_LEN as u32, 9999);
    stream.write_all(&hdr).unwrap();

    let mut body = [0u8; HELLO_LEN];
    body[0..8].copy_from_slice(&REPLICATION_MAGIC.to_le_bytes());
    body[8..12].copy_from_slice(&REPLICATION_VERSION.to_le_bytes());
    stream.write_all(&body).unwrap();

    let mut resp_hdr = [0u8; 16];
    stream.read_exact(&mut resp_hdr).unwrap();
    assert_eq!(resp_hdr[0], KIND_GEOMETRY);
    assert_ne!(resp_hdr[1] & GEOMETRY_RESET, 0);
    let _ = std::fs::remove_file(&src);
}

#[test]
fn test_mirror_connect_rejects_bad_geometry_frame() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mirror_bad_geom");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();
        let hdr = encode_frame(KIND_DATA, 0, 0, 0, 0);
        client.write_all(&hdr).unwrap();
    });

    let res = Mirror::builder().connect(addr, &dst);
    assert!(matches!(
        res,
        Err(RingfireError::Protocol("expected GEOMETRY"))
    ));
    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_connect_rejects_corrupted_geometry() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mirror_corrupt_geom");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();
        let hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&hdr).unwrap();
        let body = valid_geometry_payload(1000, 32);
        client.write_all(&body).unwrap();
    });

    let res = Mirror::builder().connect(addr, &dst);
    assert!(matches!(
        res,
        Err(RingfireError::Protocol(
            "geometry capacity is not a power of two"
        ))
    ));
    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_connect_multicast_flag_requires_multicast_frame() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mirror_mc_flag_missing");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();
        let hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&hdr).unwrap();
        let body = valid_geometry_payload(64, 32);
        client.write_all(&body).unwrap();
        let mc_hdr = encode_frame(KIND_DATA, 0, 0, 0, 0);
        client.write_all(&mc_hdr).unwrap();
    });

    let res = Mirror::builder().connect(addr, &dst);
    assert!(matches!(
        res,
        Err(RingfireError::Protocol("expected MULTICAST"))
    ));
    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_connect_multicast_flag_bad_multicast_address() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mirror_mc_bad_addr");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();
        let hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&hdr).unwrap();
        let body = valid_geometry_payload(64, 32);
        client.write_all(&body).unwrap();
        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        client.write_all(&mc_hdr).unwrap();
        let mut mc_info = [0u8; MULTICAST_LEN];
        mc_info[0..4].copy_from_slice(&[127, 0, 0, 1]);
        client.write_all(&mc_info).unwrap();
    });

    let res = Mirror::builder().connect(addr, &dst);
    assert!(matches!(
        res,
        Err(RingfireError::Protocol(
            "MULTICAST group is not a multicast address"
        ))
    ));
    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_tcp_step_validates_data_payload_size() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mirror_tcp_data_val");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();
        let hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&hdr).unwrap();
        let body = valid_geometry_payload(64, 32);
        client.write_all(&body).unwrap();

        let d1 = encode_frame(KIND_DATA, 0, 0, 24, 1);
        client.write_all(&d1).unwrap();

        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    let res = mirror.step();
    assert!(matches!(
        res,
        Err(RingfireError::Protocol(
            "DATA length does not match its record count"
        ))
    ));
    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_tcp_step_handles_gap_and_heartbeat() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mirror_tcp_gap_hb");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();
        let hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&hdr).unwrap();
        let body = valid_geometry_payload(64, 32);
        client.write_all(&body).unwrap();

        client
            .write_all(&encode_frame(KIND_GAP, 0, 0, 0, 100))
            .unwrap();

        client
            .write_all(&encode_frame(KIND_GAP, 0, 0, 0, 50))
            .unwrap();

        client
            .write_all(&encode_frame(KIND_HEARTBEAT, 0, 0, 0, 200))
            .unwrap();

        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    assert!(mirror.step().unwrap());
    assert_eq!(mirror.gaps(), 1);
    assert_eq!(mirror.sequence(), 99);

    assert!(mirror.step().unwrap());
    assert_eq!(mirror.gaps(), 1);

    assert!(mirror.step().unwrap());
    assert_eq!(mirror.source_sequence(), 200);

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_tcp_step_handles_source_restart_and_jump() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mirror_tcp_restart_jump");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();
        let hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 10);
        client.write_all(&hdr).unwrap();
        let body = valid_geometry_payload(64, 32);
        client.write_all(&body).unwrap();

        let d10 = encode_frame(KIND_DATA, 0, 1, 24, 10);
        client.write_all(&d10).unwrap();
        client.write_all(&[42u8; 24]).unwrap();

        let d20 = encode_frame(KIND_DATA, 0, 1, 24, 20);
        client.write_all(&d20).unwrap();
        client.write_all(&[43u8; 24]).unwrap();

        let d1 = encode_frame(KIND_DATA, 0, 1, 24, 1);
        client.write_all(&d1).unwrap();
        client.write_all(&[44u8; 24]).unwrap();

        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    assert!(mirror.step().unwrap());
    assert_eq!(mirror.sequence(), 10);
    assert_eq!(mirror.gaps(), 0);

    assert!(mirror.step().unwrap());
    assert_eq!(mirror.sequence(), 20);
    assert_eq!(mirror.gaps(), 1);

    assert!(mirror.step().unwrap());
    assert_eq!(mirror.sequence(), 1);
    assert_eq!(mirror.gaps(), 2);

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_multicast_control_rejects_non_nak() {
    let _deadline = deadline::Deadline::new();
    let src = temp("mc_ctrl_non_nak_src");
    let _prod = RingProducer::<Tick>::create(&src, 64).unwrap();
    let cfg = MulticastConfig::new(Ipv4Addr::new(239, 255, 42, 99), 42999);
    let server = ReplicaServer::bind(&src, "127.0.0.1:0")
        .unwrap()
        .multicast(cfg);
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mut stream = TcpStream::connect(addr).unwrap();
    let hdr = encode_frame(KIND_HELLO, 0, 0, HELLO_LEN as u32, 0);
    stream.write_all(&hdr).unwrap();
    let mut body = [0u8; HELLO_LEN];
    body[0..8].copy_from_slice(&REPLICATION_MAGIC.to_le_bytes());
    body[8..12].copy_from_slice(&REPLICATION_VERSION.to_le_bytes());
    stream.write_all(&body).unwrap();

    let mut resp = [0u8; FRAME_HEADER_LEN + GEOMETRY_LEN + FRAME_HEADER_LEN + MULTICAST_LEN];
    stream.read_exact(&mut resp).unwrap();

    let non_nak = encode_frame(KIND_DATA, 0, 0, 0, 0);
    stream.write_all(&non_nak).unwrap();

    let mut chk = [0u8; 1];
    assert!(matches!(stream.read(&mut chk), Ok(0) | Err(_)));
    let _ = std::fs::remove_file(&src);
}

#[test]
fn test_mirror_start_sequence() {
    let _deadline = deadline::Deadline::new();
    let src = temp("start_seq_src");
    let dst = temp("start_seq_dst");
    let mut prod = RingProducer::<Tick>::create(&src, 256).unwrap();
    for seq in 1..=100 {
        prod.push(&Tick::nth(seq));
    }

    let server = ReplicaServer::bind(&src, "127.0.0.1:0")
        .unwrap()
        .batch(10)
        .linger(Some(Duration::ZERO));
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mut mirror = Mirror::builder()
        .start(MirrorStart::Sequence(51))
        .connect(addr, &dst)
        .unwrap();
    assert_eq!(mirror.first_sequence(), 51);

    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        while mirror.sequence() < 100 {
            mirror.step().unwrap();
        }
        handle.shutdown().unwrap();
        let _ = mirror.run();
    });

    runner.join().unwrap();
    let mut consumer = RingConsumerBuilder::<Tick>::new()
        .start_from_sequence(51)
        .attach(&dst)
        .unwrap();
    for seq in 51..=100 {
        assert_eq!(consumer.try_recv(), Some(Tick::nth(seq)));
    }
    assert_eq!(consumer.try_recv(), None);

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_blob_zero_length_replication() {
    let _deadline = deadline::Deadline::new();
    let src = temp("blob_zero_src");
    let dst = temp("blob_zero_dst");
    let mut prod = BlobProducer::<Tick>::create(&src, 64, 4096).unwrap();

    prod.push(&Tick::nth(1), &[]).unwrap();
    prod.push(&Tick::nth(2), b"hello world").unwrap();

    let server = ReplicaServer::bind(&src, "127.0.0.1:0")
        .unwrap()
        .linger(Some(Duration::ZERO));
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &dst)
        .unwrap();
    let handle = mirror.handle().unwrap();

    let runner = thread::spawn(move || {
        while mirror.sequence() < 2 {
            mirror.step().unwrap();
        }
        handle.shutdown().unwrap();
        let _ = mirror.run();
    });

    runner.join().unwrap();
    let mut consumer = BlobConsumer::<Tick>::attach(&dst).unwrap();

    let mut meta1 = Tick::nth(0);
    let mut buf1 = [0u8; 64];
    match consumer.recv_status(&mut meta1, &mut buf1).unwrap() {
        ringfire::blob::BlobRecvStatus::Ok { payload_len } => {
            assert_eq!(meta1, Tick::nth(1));
            assert_eq!(payload_len, 0);
        }
        _ => panic!("expected blob 1"),
    }

    let mut meta2 = Tick::nth(0);
    let mut buf2 = [0u8; 64];
    match consumer.recv_status(&mut meta2, &mut buf2).unwrap() {
        ringfire::blob::BlobRecvStatus::Ok { payload_len } => {
            assert_eq!(meta2, Tick::nth(2));
            assert_eq!(&buf2[..payload_len], b"hello world");
        }
        _ => panic!("expected blob 2"),
    }

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_sparse_ring_with_reader_registry() {
    let _deadline = deadline::Deadline::new();
    let src = temp("registry_src");
    let dst = temp("registry_dst");

    let _prod = BlobProducer::<Tick>::create(&src, 64, 4096).unwrap();

    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mirror = Mirror::builder()
        .start(MirrorStart::Latest)
        .connect(addr, &dst)
        .unwrap();

    assert_eq!(mirror.geometry().registry_count, 32);
    let dst_bytes = std::fs::read(&dst).unwrap();
    let flags = u32::from_le_bytes(dst_bytes[48..52].try_into().unwrap());
    assert_ne!(flags & FLAG_WITH_REGISTRY, 0);
    assert_ne!(flags & FLAG_SPARSE, 0);

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_unicast_mock_protocol_recovery_and_edge_cases() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("uc_mock_dst");
    let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap();
    let udp_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let udp_port = udp_sock.local_addr().unwrap().port();

    let session = 42u8;
    let token = 101u32;

    let srv = thread::spawn(move || {
        let (mut tcp_stream, _) = tcp_listener.accept().unwrap();
        let mut hello_buf = [0u8; 32];
        tcp_stream.read_exact(&mut hello_buf).unwrap();

        let geom_hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        tcp_stream.write_all(&geom_hdr).unwrap();
        let geom_body = valid_geometry_payload(64, 32);
        tcp_stream.write_all(&geom_body).unwrap();

        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        tcp_stream.write_all(&mc_hdr).unwrap();
        let mut mc_info = [0u8; MULTICAST_LEN];
        mc_info[0..4].copy_from_slice(&[0, 0, 0, 0]);
        mc_info[4..6].copy_from_slice(&udp_port.to_le_bytes());
        mc_info[6..8].copy_from_slice(&1472u16.to_le_bytes());
        mc_info[9] = session;
        mc_info[10..14].copy_from_slice(&token.to_le_bytes());
        tcp_stream.write_all(&mc_info).unwrap();

        let mut punch_buf = [0u8; 16];
        let (n, mirror_addr) = udp_sock.recv_from(&mut punch_buf).unwrap();
        assert_eq!(n, 16);
        assert_eq!(punch_buf[0], KIND_PUNCH);
        assert_eq!(punch_buf[1], session);
        assert_eq!(
            u64::from_le_bytes(punch_buf[8..16].try_into().unwrap()),
            token as u64
        );

        udp_sock.send_to(b"tiny", mirror_addr).unwrap();

        let bad_session_dgram = encode_frame(KIND_DATA, session.wrapping_add(1), 1, 24, 1);
        let mut bad_pkt = bad_session_dgram.to_vec();
        bad_pkt.extend_from_slice(&[1u8; 24]);
        udp_sock.send_to(&bad_pkt, mirror_addr).unwrap();

        let dgram3 = encode_frame(KIND_DATA, session, 1, 24, 3);
        let mut pkt3 = dgram3.to_vec();
        pkt3.extend_from_slice(&[3u8; 24]);
        udp_sock.send_to(&pkt3, mirror_addr).unwrap();

        let mut nak1 = [0u8; 24];
        tcp_stream.read_exact(&mut nak1).unwrap();
        assert_eq!(nak1[0], KIND_NAK);
        let from1 = u64::from_le_bytes(nak1[8..16].try_into().unwrap());
        let to1 = u64::from_le_bytes(nak1[16..24].try_into().unwrap());
        assert_eq!(from1, 1);
        assert_eq!(to1, 2);

        thread::sleep(Duration::from_millis(25));

        let mut nak2 = [0u8; 24];
        tcp_stream.read_exact(&mut nak2).unwrap();
        assert_eq!(nak2[0], KIND_NAK);

        let dgram2 = encode_frame(KIND_DATA, session, 1, 24, 2);
        let mut pkt2 = dgram2.to_vec();
        pkt2.extend_from_slice(&[2u8; 24]);
        udp_sock.send_to(&pkt2, mirror_addr).unwrap();

        let dgram1 = encode_frame(KIND_DATA, session, 1, 24, 1);
        let mut pkt1 = dgram1.to_vec();
        pkt1.extend_from_slice(&[1u8; 24]);
        udp_sock.send_to(&pkt1, mirror_addr).unwrap();

        udp_sock.send_to(&pkt2, mirror_addr).unwrap();

        (tcp_stream, udp_sock, mirror_addr)
    });

    let mut mirror = Mirror::builder()
        .start(MirrorStart::Latest)
        .nak_timeout(Duration::from_millis(10))
        .unicast(true)
        .spin(true)
        .connect(tcp_addr, &dst)
        .unwrap();

    assert!(mirror.is_unicast());

    while mirror.naks() == 0 {
        mirror.step().unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(mirror.naks(), 1);

    thread::sleep(Duration::from_millis(30));
    mirror.step().unwrap();
    assert_eq!(mirror.naks(), 2);

    let deadline = Instant::now() + Duration::from_secs(5);
    while mirror.sequence() < 3 && Instant::now() < deadline {
        mirror.step().unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(mirror.sequence(), 3);

    mirror.step().unwrap();
    assert_eq!(mirror.sequence(), 3);
    let mut reader = RingConsumer::<[u8; 24]>::attach(&dst).unwrap();
    for byte in 1..=3 {
        assert_eq!(reader.try_recv(), Some([byte; 24]));
    }
    assert_eq!(reader.try_recv(), None);
    assert_eq!(reader.lapped_count(), 0);

    let (mut tcp_stream, udp_sock, mirror_addr) = srv.join().unwrap();

    // Datagram KIND_HEARTBEAT
    let hb_dgram = encode_frame(KIND_HEARTBEAT, session, 0, 0, 5);
    udp_sock.send_to(&hb_dgram, mirror_addr).unwrap();

    // Datagram KIND_DATA bad size (truncated payload)
    let bad_data = encode_frame(KIND_DATA, session, 1, 24, 6);
    udp_sock.send_to(&bad_data, mirror_addr).unwrap();

    // Datagram non-DATA non-HEARTBEAT (line 2442)
    let other_dgram = encode_frame(KIND_GAP, session, 0, 0, 0);
    udp_sock.send_to(&other_dgram, mirror_addr).unwrap();

    // Datagram KIND_DATA ahead of sequence (enters pending queue)
    let ahead_data = encode_frame(KIND_DATA, session, 1, 24, 50);
    let mut ahead_pkt = ahead_data.to_vec();
    ahead_pkt.extend_from_slice(&[50u8; 24]);
    udp_sock.send_to(&ahead_pkt, mirror_addr).unwrap();

    // Control frame KIND_MULTICAST
    let mc_ctrl = encode_frame(KIND_MULTICAST, 0, 0, 0, 0);
    tcp_stream.write_all(&mc_ctrl).unwrap();

    let gap_hdr = encode_frame(KIND_GAP, 0, 0, 0, 10);
    tcp_stream.write_all(&gap_hdr[..5]).unwrap();
    thread::sleep(Duration::from_millis(5));
    tcp_stream.write_all(&gap_hdr[5..]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while mirror.gaps() < 1 && Instant::now() < deadline {
        mirror.step().unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(mirror.sequence(), 9);
    assert_eq!(mirror.gaps(), 1);

    // Control frame KIND_GAP with seq <= mirror.sequence() (ignored)
    let gap_old = encode_frame(KIND_GAP, 0, 0, 0, 5);
    tcp_stream.write_all(&gap_old).unwrap();
    mirror.step().unwrap();

    let hb_hdr = encode_frame(KIND_HEARTBEAT, 0, 0, 0, 20);
    tcp_stream.write_all(&hb_hdr).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while mirror.source_sequence() < 20 && Instant::now() < deadline {
        mirror.step().unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(mirror.source_sequence(), 20);
    assert!(mirror.naks() >= 3);

    let unexp = encode_frame(KIND_HELLO, 0, 0, 0, 0);
    tcp_stream.write_all(&unexp).unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut err = None;
    while Instant::now() < deadline {
        match mirror.step() {
            Ok(_) => thread::sleep(Duration::from_millis(1)),
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    assert!(matches!(
        err,
        Some(RingfireError::Protocol("unexpected frame kind"))
    ));

    drop(tcp_stream);
    assert!(!mirror.step().unwrap());

    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_lapped_arena_payload_generates_gap() {
    let _deadline = deadline::Deadline::new();
    let src = temp("lap_arena_src");
    let dst = temp("lap_arena_dst");
    let mut prod = BlobProducer::<Tick>::create(&src, 1024, 64).unwrap();

    let server = ReplicaServer::bind(&src, "127.0.0.1:0")
        .unwrap()
        .linger(Some(Duration::ZERO));
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    for seq in 1..=50 {
        prod.push(&Tick::nth(seq), &[seq as u8; 32]).unwrap();
    }

    let mut mirror = Mirror::builder()
        .start(MirrorStart::Oldest)
        .connect(addr, &dst)
        .unwrap();

    let handle = mirror.handle().unwrap();
    let runner = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        while mirror.sequence() < 50 && Instant::now() < deadline {
            let _ = mirror.step();
        }
        handle.shutdown().unwrap();
        let _ = mirror.run();
        (mirror.sequence(), mirror.gaps())
    });

    let (seq, gaps) = runner.join().unwrap();
    assert_eq!(seq, 50);
    assert!(gaps > 0, "lapped arena payload must generate gaps");

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_resume_mismatched_layout_recreates_ring() {
    let _deadline = deadline::Deadline::new();
    let src = temp("res_mismatch_src");
    let dst = temp("res_mismatch_dst");

    let prod_old = ringfire::RingProducerBuilder::new(64)
        .cleanup_mode(ringfire::CleanupMode::Persistent)
        .build::<Tick, _>(&dst)
        .unwrap();
    drop(prod_old);
    assert!(dst.exists(), "resume must inspect an existing ring");

    let mut prod_src = RingProducer::<Tick>::create(&src, 256).unwrap();
    for seq in 1..=10 {
        prod_src.push(&Tick::nth(seq));
    }
    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mirror = Mirror::builder()
        .start(MirrorStart::Resume)
        .connect(addr, &dst)
        .unwrap();
    assert!(!mirror.resumed());
    assert_eq!(mirror.geometry().capacity, 256);

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_builder_default() {
    let _deadline = deadline::Deadline::new();
    let b = ringfire::replication::MirrorBuilder::default();
    assert_eq!(format!("{:?}", b), format!("{:?}", Mirror::builder()));
}

#[test]
fn test_mirror_convenience_connect_and_last_nak() {
    let _deadline = deadline::Deadline::new();
    let src = temp("conv_connect_src");
    let dst = temp("conv_connect_dst");
    let _prod = RingProducer::<Tick>::create(&src, 64).unwrap();
    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mirror = Mirror::connect(addr, &dst).unwrap();
    assert_eq!(mirror.last_nak(), None);
    assert_eq!(mirror.path(), dst.as_path());
    assert_eq!(mirror.first_sequence(), 1);

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_write_records_arena_blob_size_validations() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("arena_blob_size_dst");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();

        let geom_hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&geom_hdr).unwrap();

        let capacity = 64u64;
        let element_size = 32u32;
        let flags = ringfire::header::FLAG_WITH_ARENA;
        let registry_count = 0u32;
        let slots_offset = 128u32;
        let arena_offset = 128 + 64 * 32u64;
        let arena_size = 1024u64;

        let mut geom_body = [0u8; GEOMETRY_LEN];
        geom_body[0..8].copy_from_slice(&capacity.to_le_bytes());
        geom_body[8..12].copy_from_slice(&element_size.to_le_bytes());
        geom_body[12..16].copy_from_slice(&flags.to_le_bytes());
        geom_body[16..24].copy_from_slice(&123u64.to_le_bytes());
        geom_body[24..28].copy_from_slice(&registry_count.to_le_bytes());
        geom_body[28..32].copy_from_slice(&slots_offset.to_le_bytes());
        geom_body[32..40].copy_from_slice(&arena_offset.to_le_bytes());
        geom_body[40..48].copy_from_slice(&arena_size.to_le_bytes());
        client.write_all(&geom_body).unwrap();

        let mut rec = vec![0u8; 24];
        rec[16..20].copy_from_slice(&100u32.to_le_bytes()); // blob len = 100
        rec.extend_from_slice(&[1u8; 10]); // only 10 bytes provided

        let d1 = encode_frame(KIND_DATA, 0, 1, rec.len() as u32, 1);
        client.write_all(&d1).unwrap();
        client.write_all(&rec).unwrap();

        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    let err = mirror.step().unwrap_err();
    assert!(matches!(
        err,
        RingfireError::Protocol("DATA frame shorter than its blobs")
    ));

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_write_records_arena_blob_longer_than_records() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("arena_blob_longer_dst");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();

        let geom_hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&geom_hdr).unwrap();

        let capacity = 64u64;
        let element_size = 32u32;
        let flags = ringfire::header::FLAG_WITH_ARENA;
        let registry_count = 0u32;
        let slots_offset = 128u32;
        let arena_offset = 128 + 64 * 32u64;
        let arena_size = 1024u64;

        let mut geom_body = [0u8; GEOMETRY_LEN];
        geom_body[0..8].copy_from_slice(&capacity.to_le_bytes());
        geom_body[8..12].copy_from_slice(&element_size.to_le_bytes());
        geom_body[12..16].copy_from_slice(&flags.to_le_bytes());
        geom_body[16..24].copy_from_slice(&123u64.to_le_bytes());
        geom_body[24..28].copy_from_slice(&registry_count.to_le_bytes());
        geom_body[28..32].copy_from_slice(&slots_offset.to_le_bytes());
        geom_body[32..40].copy_from_slice(&arena_offset.to_le_bytes());
        geom_body[40..48].copy_from_slice(&arena_size.to_le_bytes());
        client.write_all(&geom_body).unwrap();

        let mut rec = vec![0u8; 24];
        rec[16..20].copy_from_slice(&5u32.to_le_bytes());
        rec.extend_from_slice(&[1u8; 15]); // 5 blob bytes + 10 extra trailing bytes

        let d1 = encode_frame(KIND_DATA, 0, 1, rec.len() as u32, 1);
        client.write_all(&d1).unwrap();
        client.write_all(&rec).unwrap();

        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    let err = mirror.step().unwrap_err();
    assert!(matches!(
        err,
        RingfireError::Protocol("DATA frame longer than its records")
    ));

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_unicast_rejects_ipv6_source() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("ipv6_unicast_dst");
    let listener = TcpListener::bind("[::1]:0").expect("IPv6 loopback is required for this test");
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        let _ = client.read_exact(&mut buf);

        let geom_hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        let _ = client.write_all(&geom_hdr);
        let _ = client.write_all(&valid_geometry_payload(64, 32));

        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        let _ = client.write_all(&mc_hdr);
        let mut mc_info = [0u8; MULTICAST_LEN];
        mc_info[0..4].copy_from_slice(&[0, 0, 0, 0]);
        mc_info[4..6].copy_from_slice(&55555u16.to_le_bytes());
        mc_info[6..8].copy_from_slice(&1472u16.to_le_bytes());
        mc_info[9] = 1;
        mc_info[10..14].copy_from_slice(&123u32.to_le_bytes());
        let _ = client.write_all(&mc_info);
    });

    let res = Mirror::builder().unicast(true).connect(addr, &dst);
    assert!(matches!(
        res,
        Err(RingfireError::Unsupported(
            "unicast delivery needs an IPv4 source"
        ))
    ));

    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_datagram_shorter_than_records_error() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("dgram_short_rec_dst");
    let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap();
    let udp_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let udp_port = udp_sock.local_addr().unwrap().port();

    let session = 42u8;
    let srv = thread::spawn(move || {
        let (mut tcp_stream, _) = tcp_listener.accept().unwrap();
        let mut hello_buf = [0u8; 32];
        tcp_stream.read_exact(&mut hello_buf).unwrap();

        let geom_hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        tcp_stream.write_all(&geom_hdr).unwrap();

        let capacity = 64u64;
        let element_size = 32u32;
        let flags = ringfire::header::FLAG_WITH_ARENA;
        let registry_count = 0u32;
        let slots_offset = 128u32;
        let arena_offset = 128 + 64 * 32u64;
        let arena_size = 1024u64;

        let mut geom_body = [0u8; GEOMETRY_LEN];
        geom_body[0..8].copy_from_slice(&capacity.to_le_bytes());
        geom_body[8..12].copy_from_slice(&element_size.to_le_bytes());
        geom_body[12..16].copy_from_slice(&flags.to_le_bytes());
        geom_body[16..24].copy_from_slice(&123u64.to_le_bytes());
        geom_body[24..28].copy_from_slice(&registry_count.to_le_bytes());
        geom_body[28..32].copy_from_slice(&slots_offset.to_le_bytes());
        geom_body[32..40].copy_from_slice(&arena_offset.to_le_bytes());
        geom_body[40..48].copy_from_slice(&arena_size.to_le_bytes());
        tcp_stream.write_all(&geom_body).unwrap();

        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        tcp_stream.write_all(&mc_hdr).unwrap();
        let mut mc_info = [0u8; MULTICAST_LEN];
        mc_info[0..4].copy_from_slice(&[0, 0, 0, 0]);
        mc_info[4..6].copy_from_slice(&udp_port.to_le_bytes());
        mc_info[6..8].copy_from_slice(&1472u16.to_le_bytes());
        mc_info[9] = session;
        mc_info[10..14].copy_from_slice(&1u32.to_le_bytes());
        tcp_stream.write_all(&mc_info).unwrap();

        let mut punch = [0u8; 16];
        let (_, mirror_addr) = udp_sock.recv_from(&mut punch).unwrap();

        let dgram = encode_frame(KIND_DATA, session, 2, 50, 1);
        let mut pkt = dgram.to_vec();
        let mut r1 = vec![0u8; 24];
        r1[16..20].copy_from_slice(&20u32.to_le_bytes());
        pkt.extend_from_slice(&r1);
        pkt.extend_from_slice(&[1u8; 20]);
        pkt.extend_from_slice(&[2u8; 6]);
        udp_sock.send_to(&pkt, mirror_addr).unwrap();

        tcp_stream
    });

    let mut mirror = Mirror::builder()
        .start(MirrorStart::Latest)
        .unicast(true)
        .connect(tcp_addr, &dst)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut err = None;
    while Instant::now() < deadline {
        match mirror.step() {
            Ok(_) => thread::sleep(Duration::from_millis(1)),
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    assert!(matches!(
        err,
        Some(RingfireError::Protocol(
            "DATA frame shorter than its records"
        ))
    ));

    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_tcp_unexpected_frame_kind() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("tcp_unexp_dst");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();

        let geom_hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&geom_hdr).unwrap();
        client.write_all(&valid_geometry_payload(64, 32)).unwrap();

        let unexp = encode_frame(99, 0, 0, 0, 0);
        client.write_all(&unexp).unwrap();
        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    let err = mirror.step().unwrap_err();
    assert!(matches!(
        err,
        RingfireError::Protocol("unexpected frame kind")
    ));

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_tcp_data_eof_returns_false() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("tcp_eof_dst");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();

        let geom_hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&geom_hdr).unwrap();
        client.write_all(&valid_geometry_payload(64, 32)).unwrap();

        let d1 = encode_frame(KIND_DATA, 0, 1, 24, 1);
        client.write_all(&d1).unwrap();
        drop(client);
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    let res = mirror.step().unwrap();
    assert!(!res);

    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_multicast_tcp_data_eof_returns_false() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mc_data_eof_dst");
    let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap();
    let udp_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let udp_port = udp_sock.local_addr().unwrap().port();

    let srv = thread::spawn(move || {
        let (mut tcp_stream, _) = tcp_listener.accept().unwrap();
        let mut hello_buf = [0u8; 32];
        tcp_stream.read_exact(&mut hello_buf).unwrap();

        let geom_hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        tcp_stream.write_all(&geom_hdr).unwrap();
        tcp_stream
            .write_all(&valid_geometry_payload(64, 32))
            .unwrap();

        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        tcp_stream.write_all(&mc_hdr).unwrap();
        let mut mc_info = [0u8; MULTICAST_LEN];
        mc_info[0..4].copy_from_slice(&[0, 0, 0, 0]);
        mc_info[4..6].copy_from_slice(&udp_port.to_le_bytes());
        mc_info[6..8].copy_from_slice(&1472u16.to_le_bytes());
        mc_info[9] = 1;
        mc_info[10..14].copy_from_slice(&1u32.to_le_bytes());
        tcp_stream.write_all(&mc_info).unwrap();

        let d1 = encode_frame(KIND_DATA, 0, 1, 24, 1);
        tcp_stream.write_all(&d1).unwrap();
        drop(tcp_stream);
    });

    let mut mirror = Mirror::builder()
        .unicast(true)
        .connect(tcp_addr, &dst)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut ok_false = false;
    while Instant::now() < deadline {
        match mirror.step() {
            Ok(false) => {
                ok_false = true;
                break;
            }
            Ok(true) => thread::sleep(Duration::from_millis(1)),
            Err(_) => break,
        }
    }
    assert!(ok_false);

    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_server_spin_mode_mirror_connection() {
    let _deadline = deadline::Deadline::new();
    let src = temp("spin_mode_src");
    let _prod = RingProducer::<Tick>::create(&src, 64).unwrap();
    let dst = temp("spin_mode_dst");

    let server = ReplicaServer::bind(&src, "127.0.0.1:0").unwrap().spin(true);
    let addr = server.local_addr().unwrap();
    server.spawn().unwrap();

    let mirror = Mirror::builder().connect(addr, &dst).unwrap();
    thread::sleep(Duration::from_millis(20));
    drop(mirror);

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_connect_rejects_occupied_udp_port() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mc_busy_port_dst");
    let occupied = UdpSocket::bind("0.0.0.0:0").unwrap();
    let occupied_port = occupied.local_addr().unwrap().port();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        let _ = client.read_exact(&mut buf);

        let geom_hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        let _ = client.write_all(&geom_hdr);
        let _ = client.write_all(&valid_geometry_payload(64, 32));

        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        let _ = client.write_all(&mc_hdr);
        let mut mc_info = [0u8; MULTICAST_LEN];
        mc_info[0..4].copy_from_slice(&[239, 255, 1, 1]);
        mc_info[4..6].copy_from_slice(&occupied_port.to_le_bytes());
        mc_info[6..8].copy_from_slice(&1472u16.to_le_bytes());
        let _ = client.write_all(&mc_info);
    });

    let res = Mirror::builder().connect(addr, &dst);
    assert!(res.is_err());

    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_partial_frame_header_read_full() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("partial_hdr_dst");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        let _ = client.read_exact(&mut buf);

        let geom_hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        let _ = client.write_all(&geom_hdr);
        let _ = client.write_all(&valid_geometry_payload(64, 32));

        // Send a frame header in two parts with a small pause
        let d = encode_frame(KIND_HEARTBEAT, 0, 0, 0, 5);
        let _ = client.write_all(&d[..4]);
        let _ = client.flush();
        thread::sleep(Duration::from_millis(20));
        let _ = client.write_all(&d[4..]);
        let _ = client.flush();
        thread::sleep(Duration::from_millis(20));
        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    assert!(mirror.step().unwrap());
    assert_eq!(mirror.source_sequence(), 5);

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_multicast_out_of_order_datagram_pending() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mc_pending_dst");
    let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap();
    let udp_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let udp_port = udp_sock.local_addr().unwrap().port();

    let srv = thread::spawn(move || {
        let (mut tcp_stream, _) = tcp_listener.accept().unwrap();
        let mut hello_buf = [0u8; 32];
        let _ = tcp_stream.read_exact(&mut hello_buf);

        let geom_hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        let _ = tcp_stream.write_all(&geom_hdr);
        let _ = tcp_stream.write_all(&valid_geometry_payload(64, 32));

        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        let _ = tcp_stream.write_all(&mc_hdr);
        let mut mc_info = [0u8; MULTICAST_LEN];
        mc_info[0..4].copy_from_slice(&[0, 0, 0, 0]);
        mc_info[4..6].copy_from_slice(&udp_port.to_le_bytes());
        mc_info[6..8].copy_from_slice(&1472u16.to_le_bytes());
        mc_info[9] = 1;
        mc_info[10..14].copy_from_slice(&1u32.to_le_bytes());
        let _ = tcp_stream.write_all(&mc_info);

        // Read punch from client
        let mut punch_buf = [0u8; 64];
        let (_, peer) = udp_sock.recv_from(&mut punch_buf).unwrap();

        // Send datagram with seq = 2 (skipping 1) -> triggers pending entry
        let mut dgram = Vec::new();
        dgram.extend_from_slice(&encode_frame(KIND_DATA, 1, 1, 24, 2));
        dgram.extend_from_slice(&[42u8; 24]);
        let _ = udp_sock.send_to(&dgram, peer);

        // Expect NAK over TCP
        let mut nak_hdr = [0u8; FRAME_HEADER_LEN];
        let _ = tcp_stream.read_exact(&mut nak_hdr);

        // Send missing record 1 over TCP
        let mut data1 = Vec::new();
        data1.extend_from_slice(&encode_frame(KIND_DATA, 0, 1, 24, 1));
        data1.extend_from_slice(&[11u8; 24]);
        let _ = tcp_stream.write_all(&data1);

        tcp_stream
    });

    let mut mirror = Mirror::builder()
        .nak_timeout(Duration::from_millis(50))
        .connect(tcp_addr, &dst)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while mirror.sequence() < 2 && Instant::now() < deadline {
        let _ = mirror.step();
    }
    assert_eq!(mirror.sequence(), 2);
    let mut reader = RingConsumer::<[u8; 24]>::attach(&dst).unwrap();
    assert_eq!(reader.try_recv(), Some([11; 24]));
    assert_eq!(reader.try_recv(), Some([42; 24]));
    assert_eq!(reader.try_recv(), None);
    assert_eq!(reader.lapped_count(), 0);

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_multicast_peer_gone_on_nak() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mc_peer_gone_dst");
    let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap();
    let udp_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let udp_port = udp_sock.local_addr().unwrap().port();

    let srv = thread::spawn(move || {
        let (mut tcp_stream, _) = tcp_listener.accept().unwrap();
        let mut hello_buf = [0u8; 32];
        let _ = tcp_stream.read_exact(&mut hello_buf);

        let geom_hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        let _ = tcp_stream.write_all(&geom_hdr);
        let _ = tcp_stream.write_all(&valid_geometry_payload(64, 32));

        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        let _ = tcp_stream.write_all(&mc_hdr);
        let mut mc_info = [0u8; MULTICAST_LEN];
        mc_info[0..4].copy_from_slice(&[0, 0, 0, 0]);
        mc_info[4..6].copy_from_slice(&udp_port.to_le_bytes());
        mc_info[6..8].copy_from_slice(&1472u16.to_le_bytes());
        mc_info[9] = 1;
        mc_info[10..14].copy_from_slice(&1u32.to_le_bytes());
        let _ = tcp_stream.write_all(&mc_info);

        let mut punch_buf = [0u8; 64];
        let (_, peer) = udp_sock.recv_from(&mut punch_buf).unwrap();

        // Close TCP stream before client sends NAK
        drop(tcp_stream);
        thread::sleep(Duration::from_millis(20));

        // Now send datagram with seq = 5 -> client will try to NAK on broken TCP stream
        let mut dgram = Vec::new();
        dgram.extend_from_slice(&encode_frame(KIND_DATA, 1, 1, 24, 5));
        dgram.extend_from_slice(&[42u8; 24]);
        let _ = udp_sock.send_to(&dgram, peer);
    });

    let mut mirror = Mirror::builder()
        .nak_timeout(Duration::ZERO)
        .connect(tcp_addr, &dst)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut ended = false;
    while Instant::now() < deadline {
        match mirror.step().expect("peer closure must be a clean end") {
            false => {
                ended = true;
                break;
            }
            true => thread::yield_now(),
        }
    }
    assert!(ended, "mirror never reported the closed peer");

    srv.join().unwrap();
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_duplicate_arena_frame() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("dup_arena_dst");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        let _ = client.read_exact(&mut buf);

        let geom_hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        let _ = client.write_all(&geom_hdr);
        let mut geom_bytes = valid_geometry_payload(64, 32);
        geom_bytes[12..16].copy_from_slice(&FLAG_WITH_ARENA.to_le_bytes());
        geom_bytes[32..40].copy_from_slice(&(128u64 + 64 * 32).to_le_bytes());
        geom_bytes[40..48].copy_from_slice(&4096u64.to_le_bytes());
        let _ = client.write_all(&geom_bytes);

        // Send record 1
        let mut d1 = Vec::new();
        d1.extend_from_slice(&encode_frame(KIND_DATA, 0, 1, 24 + 10, 1));
        let mut desc1 = [0u8; 24];
        desc1[16..20].copy_from_slice(&10u32.to_le_bytes()); // len 10
        d1.extend_from_slice(&desc1);
        d1.extend_from_slice(&[42u8; 10]);
        let _ = client.write_all(&d1);

        // Send record 1 again (s < next) -> exercises false branch of s >= next for arena
        let _ = client.write_all(&d1);
        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    assert!(mirror.step().unwrap());
    assert_eq!(mirror.sequence(), 1);
    assert!(mirror.step().unwrap());
    assert_eq!(mirror.sequence(), 1);
    let mut reader = BlobConsumer::<u64>::attach(&dst).unwrap();
    let mut meta = 99;
    let mut blob = [0; 10];
    assert_eq!(reader.recv(&mut meta, &mut blob).unwrap(), Some(10));
    assert_eq!(meta, 0);
    assert_eq!(blob, [42; 10]);
    assert_eq!(reader.recv(&mut meta, &mut blob).unwrap(), None);
    assert_eq!(reader.lapped_count(), 0);

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_mirror_data_frame_trailing_bytes() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("trailing_bytes_dst");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        let _ = client.read_exact(&mut buf);

        let geom_hdr = encode_frame(KIND_GEOMETRY, 0, 0, GEOMETRY_LEN as u32, 1);
        let _ = client.write_all(&geom_hdr);
        let mut geom_bytes = valid_geometry_payload(64, 32);
        geom_bytes[12..16].copy_from_slice(&FLAG_WITH_ARENA.to_le_bytes());
        geom_bytes[32..40].copy_from_slice(&(128u64 + 64 * 32).to_le_bytes());
        geom_bytes[40..48].copy_from_slice(&4096u64.to_le_bytes());
        let _ = client.write_all(&geom_bytes);

        // Send record 1 with extra trailing bytes: count 1, but len = 24 + 10 + 5
        let mut d1 = Vec::new();
        d1.extend_from_slice(&encode_frame(KIND_DATA, 0, 1, 24 + 10 + 5, 1));
        let mut desc1 = [0u8; 24];
        desc1[16..20].copy_from_slice(&10u32.to_le_bytes()); // len 10
        d1.extend_from_slice(&desc1);
        d1.extend_from_slice(&[42u8; 10]);
        d1.extend_from_slice(&[99u8; 5]); // 5 trailing bytes!
        let _ = client.write_all(&d1);
        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    let err = mirror.step().unwrap_err();
    assert!(
        err.to_string()
            .contains("DATA frame longer than its records")
    );

    // Test frame_sizes_ok overflow -> line 2237
    assert!(!mirror.frame_sizes_ok(usize::MAX, 100));

    // Test after_advance when pending entry has end <= next -> line 2487
    mirror.pending.insert(1, (1, vec![0; 32]));
    assert!(mirror.after_advance().is_ok());

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_server_spin_idle_and_punch_unexpected_size() {
    let _deadline = deadline::Deadline::new();
    let src = temp("server_spin_idle_src");
    let _prod = RingProducer::<Tick>::create(&src, 64).unwrap();

    let udp_port = UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let server = ReplicaServer::bind(&src, "127.0.0.1:0")
        .unwrap()
        .unicast(udp_port, 1400)
        .spin(true);
    let _ = server.spawn().unwrap();

    // Send a 1-byte datagram to server UDP port -> hits line 1438 (Ok(_) => {})
    let cl_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    cl_sock
        .send_to(b"x", format!("127.0.0.1:{}", udp_port))
        .unwrap();

    // Let the server spin in its idle loop for 10ms -> hits lines 1497-1498 (core::hint::spin_loop())
    std::thread::sleep(Duration::from_millis(15));
    let _ = std::fs::remove_file(&src);
}

#[test]
fn test_source_ring_read_slot_races() {
    let _deadline = deadline::Deadline::new();
    let src = temp("src_ring_races");
    let mut prod = ringfire::spmc::RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&src)
        .unwrap();
    prod.push(&100);

    let ring = ringfire::replication::SourceRing::open(&src).unwrap();
    let mut buf = vec![0u8; 8];
    assert_eq!(ring.read(1, &mut buf), ringfire::replication::RawRead::Item);

    // Modify slot 1 seq concurrently while reading to test line 819
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&src)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let slots_offset = std::mem::size_of::<ringfire::header::RingHeader>();
    let slot1_ptr = unsafe { mmap.as_mut_ptr().add(slots_offset + 16) as *mut AtomicU64 };

    // When s2 is SLOT_WRITING -> Overwritten(0)
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = stop.clone();
    let slot_addr = slot1_ptr as usize;
    let handle = std::thread::spawn(move || {
        let ptr = slot_addr as *mut AtomicU64;
        while !stop_clone.load(Ordering::Relaxed) {
            unsafe {
                (*ptr).store(ringfire::header::SLOT_WRITING, Ordering::Relaxed);
                (*ptr).store(1, Ordering::Relaxed);
            }
        }
    });

    for _ in 0..10_000 {
        let _ = ring.read(1, &mut buf);
    }
    stop.store(true, Ordering::Relaxed);
    let _ = handle.join();

    // When s2 is want + 5 -> Overwritten(want + 5)
    let stop2 = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2_clone = stop2.clone();
    let handle2 = std::thread::spawn(move || {
        let ptr = slot_addr as *mut AtomicU64;
        while !stop2_clone.load(Ordering::Relaxed) {
            unsafe {
                (*ptr).store(15, Ordering::Relaxed);
                (*ptr).store(1, Ordering::Relaxed);
            }
        }
    });

    for _ in 0..10_000 {
        let _ = ring.read(1, &mut buf);
    }
    stop2.store(true, Ordering::Relaxed);
    let _ = handle2.join();

    let _ = std::fs::remove_file(&src);
}

#[test]
fn test_mirror_unicast_punch_and_step_coverage() {
    let _deadline = deadline::Deadline::new();
    let dst = temp("mirror_unicast_step_dst");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let punch_udp = UdpSocket::bind("127.0.0.1:0").unwrap();
    let punch_port = punch_udp.local_addr().unwrap().port();

    let srv = thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        let mut buf = [0u8; 32];
        client.read_exact(&mut buf).unwrap();
        let hdr = encode_frame(KIND_GEOMETRY, GEOMETRY_MULTICAST, 0, GEOMETRY_LEN as u32, 1);
        client.write_all(&hdr).unwrap();
        let body = valid_geometry_payload(64, 32);
        client.write_all(&body).unwrap();
        let mc_hdr = encode_frame(KIND_MULTICAST, 0, 0, MULTICAST_LEN as u32, 0);
        client.write_all(&mc_hdr).unwrap();
        let mut mc_info = [0u8; MULTICAST_LEN];
        // 0.0.0.0 (unspecified) announces unicast
        mc_info[0..4].copy_from_slice(&[0, 0, 0, 0]);
        mc_info[4..6].copy_from_slice(&punch_port.to_le_bytes());
        mc_info[6..8].copy_from_slice(&1400u16.to_le_bytes()); // MTU
        mc_info[8] = 1; // ttl
        mc_info[9] = 7; // session
        mc_info[10..14].copy_from_slice(&42u32.to_le_bytes()); // token
        client.write_all(&mc_info).unwrap();
        client
    });

    let mut mirror = Mirror::builder().connect(addr, &dst).unwrap();
    assert!(mirror.is_unicast());
    // mirror.step() should send the punch datagram and return Ok(true)
    let progressed = mirror.step().unwrap();
    assert!(progressed);

    // Verify punch datagram arrived at punch_udp
    let mut dgram = [0u8; 64];
    punch_udp
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let (n, from) = punch_udp.recv_from(&mut dgram).unwrap();
    assert!(n >= 16);
    assert_eq!(dgram[0], KIND_PUNCH);

    // Now test sending a datagram back to mirror's ephemeral UDP port:
    // 1. Short datagram (< 16 bytes) -> Ok(())
    punch_udp.send_to(b"short", from).unwrap();
    // 2. Unexpected session datagram -> Ok(())
    let bad_session = encode_frame(KIND_DATA, 99, 1, 0, 1);
    punch_udp.send_to(&bad_session, from).unwrap();
    // 3. Heartbeat frame from correct session -> updates heartbeat
    let hb = encode_frame(KIND_HEARTBEAT, 7, 0, 0, 1);
    punch_udp.send_to(&hb, from).unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let _ = mirror.step();

    drop(srv.join().unwrap());
    let _ = std::fs::remove_file(&dst);
}
