#[path = "../support/deadline.rs"]
mod deadline;

use std::io;
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::*;
use crate::arena::{ArenaHeader, BlobRef};
use crate::header::{
    FLAG_MODE_SPMC, FLAG_POLICY_LATEST_WINS, FLAG_SPARSE, FLAG_WITH_ARENA, FLAG_WITH_REGISTRY,
    RingHeader, RingLayout,
};
use crate::{BlobProducer, RingProducer};

fn temp(name: &str) -> PathBuf {
    static CTR: AtomicU64 = AtomicU64::new(1);
    let id = CTR.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ringfire_repl_u_{}_{}_{}.shm",
        name,
        std::process::id(),
        id
    ))
}

fn valid_geometry_bytes(arena: bool) -> [u8; GEOMETRY_LEN] {
    let mut b = [0u8; GEOMETRY_LEN];
    let capacity = 1024u64;
    let element_size = 64u32;
    let flags = 0u32;
    let schema_sig = 0x1234_5678u64;
    let registry_count = 0u32;
    let slots_offset = 128u32;
    let arena_offset = if arena { 128 + 1024 * 64 } else { 0u64 };
    let arena_size = if arena { 65536u64 } else { 0u64 };

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
fn test_geometry_decode_success() {
    let _deadline = deadline::Deadline::new();
    let b_no_arena = valid_geometry_bytes(false);
    let g_no = Geometry::decode(&b_no_arena).unwrap();
    assert_eq!(g_no.capacity, 1024);
    assert_eq!(g_no.element_size, 64);
    assert!(!g_no.has_arena());
    assert_eq!(g_no.payload_len(), 56);
    assert_eq!(g_no.total_len(), 128 + 1024 * 64);

    let b_arena = valid_geometry_bytes(true);
    let g_arena = Geometry::decode(&b_arena).unwrap();
    assert!(g_arena.has_arena());
    assert_eq!(g_arena.arena_size, 65536);
    assert_eq!(
        g_arena.total_len(),
        (128 + 1024 * 64) + std::mem::size_of::<ArenaHeader>() + 65536
    );
}

#[test]
fn test_geometry_decode_errors() {
    let _deadline = deadline::Deadline::new();
    // Capacity not power of two
    let mut b = valid_geometry_bytes(false);
    b[0..8].copy_from_slice(&1000u64.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry capacity is not a power of two"
        ))
    ));

    // Element size < 8
    let mut b = valid_geometry_bytes(false);
    b[8..12].copy_from_slice(&4u32.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry element size is not a multiple of 8"
        ))
    ));

    // Element size not multiple of 8
    let mut b = valid_geometry_bytes(false);
    b[8..12].copy_from_slice(&12u32.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry element size is not a multiple of 8"
        ))
    ));

    // Slots offset too small
    let mut b = valid_geometry_bytes(false);
    b[28..32].copy_from_slice(&64u32.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry slots offset is misplaced"
        ))
    ));

    // Slots offset not multiple of 64
    let mut b = valid_geometry_bytes(false);
    b[28..32].copy_from_slice(&130u32.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry slots offset is misplaced"
        ))
    ));

    // Slots end overflow
    let mut b = valid_geometry_bytes(false);
    b[0..8].copy_from_slice(&(1u64 << 62).to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol("geometry does not fit in memory"))
    ));

    // Arena offset before slots_end
    let mut b = valid_geometry_bytes(true);
    b[32..40].copy_from_slice(&128u64.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry arena offset is misplaced"
        ))
    ));

    // Arena offset not multiple of 64
    let mut b = valid_geometry_bytes(true);
    let misplaced = (128 + 1024 * 64) + 1;
    b[32..40].copy_from_slice(&(misplaced as u64).to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry arena offset is misplaced"
        ))
    ));

    // Arena size not power of two
    let mut b = valid_geometry_bytes(true);
    b[40..48].copy_from_slice(&1000u64.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry arena size is not a power of two"
        ))
    ));

    // Arena size < 64
    let mut b = valid_geometry_bytes(true);
    b[40..48].copy_from_slice(&32u64.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry arena size is not a power of two"
        ))
    ));

    // Arena ring has no blob descriptor (payload_len < 16)
    let mut b = valid_geometry_bytes(true);
    b[8..12].copy_from_slice(&16u32.to_le_bytes()); // payload_len = 8 < 16
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry arena ring has no blob descriptor"
        ))
    ));

    // Arena total size overflow
    let mut b = valid_geometry_bytes(true);
    let overflow_off = (usize::MAX - 100) as u64;
    let aligned_overflow = overflow_off & !63;
    b[32..40].copy_from_slice(&aligned_overflow.to_le_bytes());
    b[40..48].copy_from_slice(&1024u64.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol("geometry does not fit in memory"))
    ));

    // Arena size without arena offset
    let mut b = valid_geometry_bytes(false);
    b[40..48].copy_from_slice(&1024u64.to_le_bytes());
    assert!(matches!(
        Geometry::decode(&b),
        Err(RingfireError::Protocol(
            "geometry arena size without an arena"
        ))
    ));
}

#[test]
fn test_geometry_encode_roundtrip() {
    let _deadline = deadline::Deadline::new();
    let b = valid_geometry_bytes(true);
    let g1 = Geometry::decode(&b).unwrap();
    let mut out = [0u8; GEOMETRY_LEN];
    g1.encode(&mut out);
    let g2 = Geometry::decode(&out).unwrap();
    assert_eq!(g1, g2);
}

#[test]
fn test_geometry_same_layout() {
    let _deadline = deadline::Deadline::new();
    let g1 = Geometry::decode(&valid_geometry_bytes(true)).unwrap();
    let g2 = g1;
    assert!(g1.same_layout(&g2));

    let mut g_diff = g1;
    g_diff.capacity = 2048;
    assert!(!g1.same_layout(&g_diff));

    let mut g_diff = g1;
    g_diff.element_size = 128;
    assert!(!g1.same_layout(&g_diff));

    let mut g_diff = g1;
    g_diff.schema_sig = 0x9999;
    assert!(!g1.same_layout(&g_diff));

    let mut g_diff = g1;
    g_diff.registry_count = 8;
    assert!(!g1.same_layout(&g_diff));

    let mut g_diff = g1;
    g_diff.slots_offset = 192;
    assert!(!g1.same_layout(&g_diff));

    let mut g_diff = g1;
    g_diff.arena_offset = 0;
    assert!(!g1.same_layout(&g_diff));

    let mut g_diff = g1;
    g_diff.arena_size = 0;
    assert!(!g1.same_layout(&g_diff));
}

#[test]
fn test_geometry_mirror_flags() {
    let _deadline = deadline::Deadline::new();
    let mut g = Geometry::decode(&valid_geometry_bytes(false)).unwrap();
    assert_eq!(
        g.mirror_flags(),
        FLAG_MODE_SPMC | FLAG_POLICY_LATEST_WINS | FLAG_SPARSE
    );

    g.registry_count = 4;
    assert_eq!(
        g.mirror_flags(),
        FLAG_MODE_SPMC | FLAG_POLICY_LATEST_WINS | FLAG_SPARSE | FLAG_WITH_REGISTRY
    );

    let g_arena = Geometry::decode(&valid_geometry_bytes(true)).unwrap();
    assert_eq!(
        g_arena.mirror_flags(),
        FLAG_MODE_SPMC | FLAG_POLICY_LATEST_WINS | FLAG_SPARSE | FLAG_WITH_ARENA
    );
}

#[test]
fn test_multicast_info_decode() {
    let _deadline = deadline::Deadline::new();
    let mut buf = [0u8; MULTICAST_LEN];
    encode_udp_info(
        Ipv4Addr::new(239, 255, 42, 1),
        42000,
        1472,
        2,
        42,
        100,
        &mut buf,
    );
    let info = MulticastInfo::decode(&buf).unwrap();
    assert_eq!(info.group, Ipv4Addr::new(239, 255, 42, 1));
    assert_eq!(info.port, 42000);
    assert_eq!(info.session, 42);
    assert_eq!(info.token, 100);

    // Unspecified group (unicast)
    encode_udp_info(Ipv4Addr::UNSPECIFIED, 42000, 1472, 1, 10, 200, &mut buf);
    let info = MulticastInfo::decode(&buf).unwrap();
    assert!(info.group.is_unspecified());
    assert_eq!(info.token, 200);

    // Non-multicast non-unspecified group: e.g. 127.0.0.1
    encode_udp_info(
        Ipv4Addr::new(127, 0, 0, 1),
        42000,
        1472,
        1,
        10,
        200,
        &mut buf,
    );
    assert!(matches!(
        MulticastInfo::decode(&buf),
        Err(RingfireError::Protocol(
            "MULTICAST group is not a multicast address"
        ))
    ));
}

#[test]
fn test_multicast_config_methods() {
    let _deadline = deadline::Deadline::new();
    let cfg = MulticastConfig::new(Ipv4Addr::new(239, 255, 1, 2), 43000)
        .interface(Ipv4Addr::LOCALHOST)
        .mtu(9000)
        .ttl(4)
        .heartbeat(Duration::from_millis(50))
        .drop_every(3)
        .swap_every(5);
    assert_eq!(cfg.group, Ipv4Addr::new(239, 255, 1, 2));
    assert_eq!(cfg.port, 43000);
    assert_eq!(cfg.interface, Ipv4Addr::LOCALHOST);
    assert_eq!(cfg.mtu, 9000);
    assert_eq!(cfg.ttl, 4);
    assert_eq!(cfg.heartbeat, Duration::from_millis(50));
    assert_eq!(cfg.drop_every, 3);
    assert_eq!(cfg.swap_every, 5);

    let mut buf = [0u8; MULTICAST_LEN];
    cfg.encode(9, &mut buf);
    let info = MulticastInfo::decode(&buf).unwrap();
    assert_eq!(info.group, cfg.group);
    assert_eq!(info.port, cfg.port);
    assert_eq!(info.session, 9);
}

#[test]
fn test_frame_control_and_roundtrip() {
    let _deadline = deadline::Deadline::new();
    let frame = Frame::control(KIND_GAP, 12345);
    assert_eq!(frame.kind, KIND_GAP);
    assert_eq!(frame.flags, 0);
    assert_eq!(frame.count, 0);
    assert_eq!(frame.len, 0);
    assert_eq!(frame.seq, 12345);

    let encoded = frame.encode();
    let decoded = Frame::decode(&encoded);
    assert_eq!(frame, decoded);
}

#[test]
fn test_peer_gone() {
    let _deadline = deadline::Deadline::new();
    assert!(peer_gone(&io::Error::from(io::ErrorKind::UnexpectedEof)));
    assert!(peer_gone(&io::Error::from(io::ErrorKind::ConnectionReset)));
    assert!(peer_gone(&io::Error::from(
        io::ErrorKind::ConnectionAborted
    )));
    assert!(peer_gone(&io::Error::from(io::ErrorKind::BrokenPipe)));
    assert!(!peer_gone(&io::Error::from(io::ErrorKind::Other)));
    assert!(!peer_gone(&io::Error::from(io::ErrorKind::TimedOut)));
    assert!(!peer_gone(&io::Error::from(io::ErrorKind::WouldBlock)));
}

#[test]
fn test_blob_ref_helpers() {
    let _deadline = deadline::Deadline::new();
    let mut payload = vec![0u8; 32];
    let r = BlobRef {
        offset: 123456,
        len: 789,
        flags: 42,
    };
    put_blob_ref(&mut payload, r);
    let retrieved = blob_ref_at(&payload);
    assert_eq!(r.offset, retrieved.offset);
    assert_eq!(r.len, retrieved.len);
    assert_eq!(r.flags, retrieved.flags);
}

#[test]
fn test_arena_view_checks() {
    let _deadline = deadline::Deadline::new();
    let total = std::mem::size_of::<ArenaHeader>() + 1024 + 128;
    let mut mem = vec![0u8; total];
    let offset = (64 - (mem.as_ptr() as usize % 64)) % 64;
    let base = unsafe { mem.as_mut_ptr().add(offset) };

    let header = base.cast::<ArenaHeader>();
    unsafe {
        (*header).capacity = 1024;
        (*header).mask = 1023;
        (*header).reserved.store(0, Ordering::Relaxed);
    }
    let geom = Geometry {
        capacity: 64,
        element_size: 64,
        flags: 0,
        schema_sig: 0,
        registry_count: 0,
        slots_offset: 128,
        arena_offset: 0,
        arena_size: 1024,
    };

    let view = unsafe { ArenaView::open(base, &geom).unwrap() };
    assert!(view.in_bounds(BlobRef {
        offset: 100,
        len: 200,
        flags: 0
    }));
    assert!(!view.in_bounds(BlobRef {
        offset: 900,
        len: 200,
        flags: 0
    }));
    assert!(!view.is_lapped(BlobRef {
        offset: 0,
        len: 50,
        flags: 0
    }));

    unsafe { (*header).reserved.store(2000, Ordering::Relaxed) };
    assert!(view.is_lapped(BlobRef {
        offset: 0,
        len: 50,
        flags: 0
    }));

    // Data copy test
    unsafe {
        let data = base.add(std::mem::size_of::<ArenaHeader>());
        std::ptr::copy_nonoverlapping(b"test data".as_ptr(), data.add(10), 9);
    }
    let mut copied = Vec::new();
    view.copy_into(
        BlobRef {
            offset: 10,
            len: 9,
            flags: 0,
        },
        &mut copied,
    );
    assert_eq!(&copied, b"test data");

    // Corrupt layout
    unsafe { (*header).capacity = 512 };
    assert!(matches!(
        unsafe { ArenaView::open(base, &geom) },
        Err(RingfireError::CorruptLayout(_))
    ));
}

#[test]
fn test_source_ring_and_read_states() {
    let _deadline = deadline::Deadline::new();
    assert!(SourceRing::open(&PathBuf::from("/non/existent/path/for/ring.shm")).is_err());

    let p = temp("src_ring");
    let mut prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let source = SourceRing::open(&p).unwrap();
    assert_eq!(source.write_seq(), 0);

    let mut buf = vec![0u8; source.geometry.payload_len()];
    assert!(matches!(source.read(1, &mut buf), RawRead::Pending));

    for i in 1..=5 {
        prod.push(&i);
    }
    assert_eq!(source.write_seq(), 5);
    assert!(matches!(source.read(1, &mut buf), RawRead::Item));
    assert!(matches!(source.read(6, &mut buf), RawRead::Pending));

    // Overwrite slot 1
    for i in 6..=200 {
        prod.push(&i);
    }
    assert!(matches!(source.read(1, &mut buf), RawRead::Overwritten(_)));

    // Collect with max_bytes limit
    let mut wire = vec![0u8; FRAME_HEADER_LEN];
    let collected = source.collect(190, 10, FRAME_HEADER_LEN + 16, &mut wire);
    assert!(collected.count < 10);

    // Collect lingering with ZERO duration
    let mut wire2 = vec![0u8; FRAME_HEADER_LEN];
    let coll_lingering = source.collect_lingering(190, 5, 1000, &mut wire2, Duration::ZERO);
    assert_eq!(coll_lingering.count, 5);

    // Resync
    assert_eq!(source.resync(1), oldest_retained(200, 64));
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_source_ring_arena_read_record() {
    let _deadline = deadline::Deadline::new();
    let p = temp("src_arena_ring");
    let mut prod = BlobProducer::<u64>::create(&p, 64, 4096).unwrap();
    prod.push(&1u64, &[]).unwrap();
    prod.push(&2u64, b"hello blob").unwrap();

    let source = SourceRing::open(&p).unwrap();
    let mut out = Vec::new();
    assert!(matches!(source.read_record(1, &mut out), RawRead::Item));

    out.clear();
    assert!(matches!(source.read_record(2, &mut out), RawRead::Item));
    assert!(out.ends_with(b"hello blob"));

    // Corrupt blob ref offset in file to test out_of_bounds / RawRead::Lost
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        let slot_off = source.geometry.slots_offset as u64
            + (2 & (source.geometry.capacity - 1)) * (source.geometry.element_size as u64);
        f.seek(SeekFrom::Start(slot_off + 24)).unwrap();
        f.write_all(&100_000u32.to_le_bytes()).unwrap();
    }
    out.clear();
    assert!(matches!(source.read_record(2, &mut out), RawRead::Lost));

    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_source_ring_open_arena_without_descriptor() {
    let _deadline = deadline::Deadline::new();
    let p = temp("arena_no_desc");
    let file = crate::shm::create_backing_file(&p, 0o660, true, 4096).unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let base = mmap.as_mut_ptr();
    unsafe {
        RingHeader::initialize(
            base.cast(),
            &RingLayout {
                capacity: 64,
                slot_size: 16,
                flags: crate::header::FLAG_MODE_SPMC
                    | crate::header::FLAG_POLICY_LATEST_WINS
                    | FLAG_WITH_ARENA,
                schema_sig: 0,
                claim_seq: 0,
                read_seq: 0,
                registry_offset: 0,
                registry_count: 0,
                slots_offset: 128,
                arena_offset: 128 + 64 * 16,
                arena_size: 1024,
            },
        );
        RingHeader::publish(base.cast());
    }
    assert!(matches!(
        SourceRing::open(&p),
        Err(RingfireError::Unsupported(
            "arena ring without a blob descriptor"
        ))
    ));
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_mirror_ring_operations() {
    let _deadline = deadline::Deadline::new();
    let p = temp("mirror_ring");
    let geom = Geometry {
        capacity: 64,
        element_size: 16,
        flags: 0,
        schema_sig: 42,
        registry_count: 4,
        slots_offset: 128 + 4 * 64,
        arena_offset: 0,
        arena_size: 0,
    };

    let mut ring = MirrorRing::create(&p, &geom, 0o660).unwrap();
    ring.write(1, &[42u8; 8]);
    ring.publish();
    ring.skip_to(10);
    assert_eq!(ring.next_seq, 10);
    drop(ring);

    let adopted = MirrorRing::adopt(&p).unwrap().unwrap();
    assert_eq!(adopted.next_seq, 2);
    drop(adopted);

    // Adopt non-existent
    let non_exist = temp("non_existent");
    assert!(MirrorRing::adopt(&non_exist).unwrap().is_none());

    // Adopt corrupt file
    let p_corrupt = temp("adopt_corrupt");
    std::fs::write(&p_corrupt, b"0123456789abcdef").unwrap();
    assert!(MirrorRing::adopt(&p_corrupt).unwrap().is_none());
    let _ = std::fs::remove_file(&p_corrupt);

    // Adopt flock
    let f = OpenOptions::new().read(true).write(true).open(&p).unwrap();
    assert_eq!(
        unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    assert!(matches!(
        MirrorRing::adopt(&p),
        Err(RingfireError::ProducerAlreadyExists)
    ));
    unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_UN) };
    let _ = std::fs::remove_file(&p);

    // Adopt a directory (EISDIR)
    let dir = temp("mirror_dir");
    std::fs::create_dir(&dir).unwrap();
    assert!(MirrorRing::adopt(&dir).is_err());
    let _ = std::fs::remove_dir(&dir);

    // Adopt a 0-byte file (empty mmap returns None)
    let empty_f = temp("empty_file");
    std::fs::File::create(&empty_f).unwrap();
    assert!(MirrorRing::adopt(&empty_f).unwrap().is_none());
    let _ = std::fs::remove_file(&empty_f);

    // Arena mirror ring create & adopt
    let p_arena = temp("adopt_arena");
    let geom_arena = Geometry {
        capacity: 64,
        element_size: 32,
        flags: FLAG_WITH_ARENA,
        schema_sig: 42,
        registry_count: 0,
        slots_offset: 128,
        arena_offset: 128 + 64 * 32,
        arena_size: 1024,
    };
    let mut r_arena = MirrorRing::create(&p_arena, &geom_arena, 0o660).unwrap();
    r_arena.write_blob(1, &[42u8; 24], b"hello blob").unwrap();
    r_arena.publish();
    drop(r_arena);

    let adopted_arena = MirrorRing::adopt(&p_arena).unwrap().unwrap();
    assert_eq!(adopted_arena.next_seq, 2);
    assert!(adopted_arena.arena.is_some());
    drop(adopted_arena);

    // Corrupt arena capacity to test adopt returning None (line 1063)
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f_arena = std::fs::OpenOptions::new()
            .write(true)
            .open(&p_arena)
            .unwrap();
        f_arena
            .seek(SeekFrom::Start(geom_arena.arena_offset))
            .unwrap();
        f_arena.write_all(&999999u64.to_le_bytes()).unwrap();
    }
    assert!(MirrorRing::adopt(&p_arena).unwrap().is_none());
    let _ = std::fs::remove_file(&p_arena);

    // Arena mirror ring create failure with invalid arena size (line 1017)
    let p_bad_arena = temp("bad_arena");
    let geom_bad_arena = Geometry {
        capacity: 64,
        element_size: 32,
        flags: FLAG_WITH_ARENA,
        schema_sig: 42,
        registry_count: 0,
        slots_offset: 128,
        arena_offset: 128 + 64 * 32,
        arena_size: 50,
    };
    assert!(MirrorRing::create(&p_bad_arena, &geom_bad_arena, 0o660).is_err());
    let _ = std::fs::remove_file(&p_bad_arena);
}

#[test]
fn test_linger_struct() {
    let _deadline = deadline::Deadline::new();
    let mut l_fixed = Linger::new(Some(Duration::from_millis(10)));
    assert_eq!(l_fixed.current(), Duration::from_millis(10));
    l_fixed.sent();

    let mut l_adaptive = Linger::new(None);
    l_adaptive.sent();
    assert!(l_adaptive.current() <= ADAPTIVE_PACE);
}

#[test]
fn test_sockets_and_io_helpers() {
    let _deadline = deadline::Deadline::new();
    let sock = udp_sender(0, None).unwrap();
    assert!(sock.local_addr().is_ok());

    let cfg =
        MulticastConfig::new(Ipv4Addr::new(239, 255, 10, 1), 45000).interface(Ipv4Addr::LOCALHOST);
    let sock_mc = udp_sender(0, Some(&cfg)).unwrap();
    assert!(sock_mc.local_addr().is_ok());

    let rx1 = multicast_receiver(Ipv4Addr::UNSPECIFIED, 0, Ipv4Addr::UNSPECIFIED, 0).unwrap();
    let rx2 = multicast_receiver(Ipv4Addr::UNSPECIFIED, 0, Ipv4Addr::UNSPECIFIED, 65536).unwrap();
    wait_readable(
        &[rx1.as_raw_fd(), rx2.as_raw_fd()],
        Duration::from_millis(1),
    );

    // read_full and write_full
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).unwrap();
    let (mut server, _) = listener.accept().unwrap();

    write_full(&mut client, b"hello network").unwrap();
    let mut buf = [0u8; 13];
    read_full(&mut server, &mut buf, false).unwrap();
    assert_eq!(&buf, b"hello network");

    drop(client);
    let mut buf2 = [0u8; 1];
    assert!(matches!(
        read_full(&mut server, &mut buf2, true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof
    ));

    // set_sockopt error on invalid fd
    assert!(set_sockopt(-1, libc::SOL_SOCKET, libc::SO_REUSEADDR, &1i32).is_err());

    // Deterministic bind conflict, independent of privilege/capabilities.
    let occupied = UdpSocket::bind("0.0.0.0:0").unwrap();
    assert!(
        multicast_receiver(
            Ipv4Addr::UNSPECIFIED,
            occupied.local_addr().unwrap().port(),
            Ipv4Addr::UNSPECIFIED,
            0
        )
        .is_err()
    );

    // send_datagram
    let u1 = UdpSocket::bind("127.0.0.1:0").unwrap();
    let u2 = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr2 = match u2.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    send_datagram(&u1, addr2, b"hello udp").unwrap();

    // read_full with spin: true and WouldBlock
    let l_sp = TcpListener::bind("127.0.0.1:0").unwrap();
    let a_sp = l_sp.local_addr().unwrap();
    let mut cl_sp = TcpStream::connect(a_sp).unwrap();
    let (mut srv_sp, _) = l_sp.accept().unwrap();
    cl_sp.set_nonblocking(true).unwrap();

    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        srv_sp.write_all(b"spin-read").unwrap();
    });
    let mut sp_buf = [0u8; 9];
    read_full(&mut cl_sp, &mut sp_buf, true).unwrap();
    assert_eq!(&sp_buf, b"spin-read");
    writer.join().unwrap();

    // write_full on nonblocking socket with would_block
    let l_nb = TcpListener::bind("127.0.0.1:0").unwrap();
    let a_nb = l_nb.local_addr().unwrap();
    let mut cl_nb = TcpStream::connect(a_nb).unwrap();
    let (mut srv_nb, _) = l_nb.accept().unwrap();
    cl_nb.set_nonblocking(true).unwrap();
    set_sockopt(
        cl_nb.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_SNDBUF,
        &4096i32,
    )
    .unwrap();
    set_sockopt(
        srv_nb.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_RCVBUF,
        &4096i32,
    )
    .unwrap();

    let drainer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        let mut b = vec![0u8; 4096];
        let mut total = 0;
        while total < 64 * 1024 {
            if let Ok(n) = srv_nb.read(&mut b) {
                if n == 0 {
                    break;
                }
                assert!(b[..n].iter().all(|byte| *byte == 42));
                total += n;
            }
        }
        assert_eq!(total, 64 * 1024);
    });
    let big = vec![42u8; 64 * 1024];
    write_full(&mut cl_nb, &big).unwrap();
    drainer.join().unwrap();

    // test write_full on broken pipe (hits line 565)
    let l_bp = TcpListener::bind("127.0.0.1:0").unwrap();
    let a_bp = l_bp.local_addr().unwrap();
    let mut cl_bp = TcpStream::connect(a_bp).unwrap();
    let (srv_bp, _) = l_bp.accept().unwrap();
    cl_bp.shutdown(std::net::Shutdown::Write).unwrap();
    assert!(write_full(&mut cl_bp, &[1u8; 100]).is_err());
    drop(srv_bp);

    // test send_datagram on oversized payload (hits line 661)
    let u_big = UdpSocket::bind("127.0.0.1:0").unwrap();
    let dest = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9999);
    let oversized = vec![0u8; 70_000];
    assert_eq!(
        send_datagram(&u_big, dest, &oversized)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EMSGSIZE)
    );

    // multicast interface invalid IP -> EADDRNOTAVAIL (hits line 605)
    let cfg_bad_if = MulticastConfig::new(Ipv4Addr::new(239, 255, 10, 1), 45000)
        .interface(Ipv4Addr::new(192, 0, 2, 250));
    assert!(udp_sender(0, Some(&cfg_bad_if)).is_err());
}

#[test]
fn test_serve_range_paths() {
    let _deadline = deadline::Deadline::new();
    let p = temp("serve_range_paths");
    let mut prod = BlobProducer::<u64>::create(&p, 64, 4096).unwrap();
    for seq in 1..=80 {
        prod.push(&(seq as u64), format!("item {}", seq).as_bytes())
            .unwrap();
    }

    let source = SourceRing::open(&p).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).unwrap();
    let (mut server, _) = listener.accept().unwrap();

    let mut wire = Vec::new();

    // 1. cursor < oldest -> sends GAP oldest
    serve_range(&source, &mut server, 1, 20, 5, &mut wire).unwrap();

    // 2. got.count == 0 (break when to > write_seq)
    serve_range(&source, &mut server, 81, 100, 5, &mut wire).unwrap();

    // 3. got.lost -> sends GAP
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        let slot_off = source.geometry.slots_offset as u64
            + (80 & (source.geometry.capacity - 1)) * (source.geometry.element_size as u64);
        f.seek(SeekFrom::Start(slot_off + 24)).unwrap();
        f.write_all(&100_000u32.to_le_bytes()).unwrap();
    }
    serve_range(&source, &mut server, 80, 80, 5, &mut wire).unwrap();

    // 4. got.lapped -> sends GAP resync
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        let slot_off = source.geometry.slots_offset as u64
            + (50 & (source.geometry.capacity - 1)) * (source.geometry.element_size as u64);
        f.seek(SeekFrom::Start(slot_off)).unwrap();
        f.write_all(&120u64.to_le_bytes()).unwrap();
    }
    serve_range(&source, &mut server, 50, 60, 5, &mut wire).unwrap();

    drop(server);
    let mut in_buf = Vec::new();
    client.read_to_end(&mut in_buf).unwrap();
    let mut frames = Vec::new();
    let mut at = 0;
    while at < in_buf.len() {
        let header: &[u8; FRAME_HEADER_LEN] = in_buf[at..at + FRAME_HEADER_LEN].try_into().unwrap();
        let frame = Frame::decode(header);
        frames.push((frame.kind, frame.seq, frame.count));
        at += FRAME_HEADER_LEN + frame.len as usize;
    }
    assert_eq!(at, in_buf.len());
    assert_eq!(
        frames,
        vec![
            (KIND_GAP, 17, 0),
            (KIND_DATA, 17, 4),
            (KIND_GAP, 81, 0),
            (KIND_GAP, 57, 0),
            (KIND_DATA, 57, 4)
        ]
    );

    let _ = std::fs::remove_file(&p);
}
