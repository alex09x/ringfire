//! Behavioral coverage for public IPC controls and malformed shared-memory layouts.
use memmap2::MmapMut;
use ringfire::*;
use std::error::Error;
use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "ringfire-core-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn map(path: &Path) -> MmapMut {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    unsafe { MmapMut::map_mut(&f).unwrap() }
}

#[test]
fn cursor_controls_and_checkpoints_preserve_replay_position() {
    let d = Dir::new();
    let path = d.path("ring");
    let offset = d.path("offset");
    let mut p = RingProducerBuilder::new(4)
        .max_readers(4)
        .file_mode(0o600)
        .cleanup_mode(CleanupMode::Persistent)
        .build::<u64, _>(&path)
        .unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        (
            p.capacity(),
            p.sequence(),
            p.next_sequence(),
            p.published_sequence()
        ),
        (4, 0, 1, 0)
    );
    assert_eq!(p.path(), path);
    assert_eq!(p.flow_control(), FlowControl::LossyLatestWins);
    assert!(format!("{p:?}").contains("RingProducer"));
    let mut c = RingConsumerBuilder::<u64>::default()
        .start_mode(ConsumerStartMode::Head)
        .offset_shm(&offset)
        .attach(&path)
        .unwrap();
    assert_eq!(c.offset_path(), Some(offset.as_path()));
    assert_eq!(c.checkpoint().unwrap().load(), None);
    assert_eq!(c.registry().unwrap().capacity(), 4);
    assert!(c.registration().is_some());
    assert_eq!((c.capacity(), c.cursor(), c.lag()), (4, 1, 0));
    assert_eq!(c.jump_to_latest(), 0);
    assert_eq!(c.jump_to_oldest(), 0);
    p.push_batch(&[1, 2, 3, 4, 5, 6]);
    assert_eq!(
        (p.sequence(), p.next_sequence(), p.published_sequence()),
        (6, 7, 6)
    );
    assert_eq!(c.jump_to_oldest(), 2);
    assert_eq!(c.cursor(), 3);
    assert_eq!(c.try_recv(), Some(3));
    assert_eq!(c.lag(), 3);
    assert_eq!(c.jump_to_latest(), 2);
    assert_eq!(c.try_recv(), Some(6));
    assert_eq!(c.seek(0), 2);
    assert_eq!(c.cursor(), 3);
    assert_eq!(c.seek(5), 0);
    assert_eq!(c.try_recv(), Some(5));
    c.commit_offset().unwrap();
    assert_eq!(c.checkpoint().unwrap().load(), Some(5));
    c.commit_offset_to(d.path("second-offset"), "backup")
        .unwrap();
    assert_eq!(
        OffsetCheckpoint::open_or_create(d.path("second-offset"), "backup")
            .unwrap()
            .load(),
        Some(5)
    );
    assert!(format!("{c:?}").contains("cursor"));
    drop(c);
    let mut resumed = RingConsumer::<u64>::attach_from_offset(&path, &offset).unwrap();
    assert_eq!(resumed.try_recv(), Some(6));
    assert_eq!(resumed.try_recv(), None);
    assert_eq!(p.reader_lag(), 0);
    assert_eq!(p.headroom(), 4);
    drop(resumed);
    drop(p);
    assert!(path.exists());
}

#[test]
fn ring_options_and_unregistered_reader_fallback() {
    let d = Dir::new();
    let path = d.path("ring");
    assert!(matches!(
        RingProducer::<u64>::create(&path, 3),
        Err(RingfireError::InvalidCapacity(3))
    ));
    let mut p = RingProducerBuilder::new(4)
        .exclusive_lock(false)
        .max_readers(1)
        .build::<u64, _>(&path)
        .unwrap();
    let first = RingConsumer::<u64>::attach(&path).unwrap();
    let mut second = RingConsumer::<u64>::attach(&path).unwrap();
    assert!(first.registration().is_some());
    assert!(second.registration().is_none());
    assert!(matches!(
        second.commit_offset(),
        Err(RingfireError::NoOffsetFileConfigured)
    ));
    assert!(second.offset_path().is_none());
    assert!(second.checkpoint().is_none());
    assert!(
        second
            .commit_offset_to(d.path("missing/offset"), "second")
            .is_ok()
    );
    p.try_push(&42).unwrap();
    assert_eq!(p.published_sequence(), 1);
    assert_eq!(second.try_recv(), Some(42));
    drop(first);
    drop(second);
    drop(p);
    assert!(!path.exists());
    let p = RingProducerBuilder::new(4)
        .flow_control(FlowControl::LosslessBackpressure)
        .max_readers(0)
        .build::<u64, _>(&path)
        .unwrap();
    assert_eq!(p.registry().unwrap().capacity(), DEFAULT_MAX_READERS);
    assert_eq!(p.headroom(), 4);
}

#[test]
fn checkpoint_metadata_and_corrupt_files_are_reported() {
    let d = Dir::new();
    let path = d.path("offset");
    let cp = OffsetCheckpoint::open_or_create(&path, "  desk  ").unwrap();
    assert_eq!(cp.name(), "desk");
    assert_eq!(cp.pid(), std::process::id());
    assert!(cp.updated_nanos() > 0);
    assert!(format!("{cp:?}").contains("desk"));
    cp.save(99);
    drop(cp);
    let mut m = map(&path);
    m[..8].fill(0);
    drop(m);
    let e = OffsetCheckpoint::open_or_create(&path, "desk").unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
    assert!(e.to_string().contains("magic"));
    std::fs::remove_file(&path).unwrap();
    drop(OffsetCheckpoint::open_or_create(&path, "desk").unwrap());
    let mut m = map(&path);
    m[8..12].copy_from_slice(&99u32.to_ne_bytes());
    drop(m);
    assert!(
        OffsetCheckpoint::open_or_create(&path, "desk")
            .unwrap_err()
            .to_string()
            .contains("Version")
    );
    let cp = OffsetCheckpoint::for_consumer(d.path("feed.shm"), "../desk/a").unwrap();
    assert_eq!(cp.path().parent(), Some(d.0.as_path()));
    assert!(
        cp.path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("___desk_a")
    );
}

#[test]
fn blackboard_copy_bounds_and_validation() {
    let d = Dir::new();
    let path = d.path("board");
    let mut p = BlackboardProducer::<u64>::create(&path, 2).unwrap();
    p.set_cleanup_mode(CleanupMode::Persistent);
    assert_eq!(p.slot_count(), 2);
    let c = BlackboardConsumer::<u64>::attach(&path).unwrap();
    assert_eq!(c.slot_count(), 2);
    let mut out = 777;
    assert!(!c.read_copy(0, &mut out).unwrap());
    assert_eq!(out, 777);
    p.write(0, &42).unwrap();
    assert!(c.read_copy(0, &mut out).unwrap());
    assert_eq!(out, 42);
    assert!(matches!(
        p.write(2, &1),
        Err(RingfireError::KeyOutOfRange {
            key: 2,
            capacity: 2
        })
    ));
    assert!(matches!(
        c.read_copy(2, &mut out),
        Err(RingfireError::KeyOutOfRange { .. })
    ));
    assert!(matches!(
        BlackboardConsumer::<u32>::attach(&path),
        Err(RingfireError::ValueSizeMismatch { .. })
    ));
    drop(c);
    drop(p);
    assert!(path.exists());
    let pristine = std::fs::read(&path).unwrap();
    for (offset, value, expected) in [
        (0, 0u64.to_ne_bytes().to_vec(), "magic"),
        (8, 99u32.to_ne_bytes().to_vec(), "Version"),
        (16, 1u32.to_ne_bytes().to_vec(), "stride"),
        (20, 999u32.to_ne_bytes().to_vec(), "past"),
    ] {
        let mut bytes = pristine.clone();
        bytes[offset..offset + value.len()].copy_from_slice(&value);
        std::fs::write(&path, bytes).unwrap();
        let e = BlackboardConsumer::<u64>::attach(&path).err().unwrap();
        assert!(e.to_string().contains(expected), "{e}");
    }
    std::fs::write(&path, [0u8; 16]).unwrap();
    assert!(matches!(
        BlackboardConsumer::<u64>::attach(&path),
        Err(RingfireError::CorruptLayout(_))
    ));
    assert!(matches!(
        BlackboardProducer::<u64>::create(d.path("huge"), u32::MAX as usize + 1),
        Err(RingfireError::InvalidCapacity(_))
    ));
}

#[test]
fn malformed_ring_regions_never_attach() {
    let d = Dir::new();
    let path = d.path("ring");
    let p = RingProducerBuilder::new(4)
        .max_readers(2)
        .cleanup_mode(CleanupMode::Persistent)
        .build::<u64, _>(&path)
        .unwrap();
    drop(p);
    let pristine = std::fs::read(&path).unwrap();
    let mutations: &[fn(&mut RingHeader)] = &[
        |h| h.magic = 0,
        |h| h.version = 99,
        |h| h.element_size = 1,
        |h| h.capacity = 3,
        |h| h.mask = 0,
        |h| h.slots_offset = 1,
        |h| h.slots_offset = 1 << 30,
        |h| h.reader_registry_offset = 129,
        |h| h.reader_registry_count = u32::MAX,
        |h| h.arena_offset = 129,
        |h| {
            h.arena_offset = 128;
            h.arena_size = u64::MAX;
        },
    ];
    for mutate in mutations {
        std::fs::write(&path, &pristine).unwrap();
        let mut m = map(&path);
        mutate(unsafe { &mut *m.as_mut_ptr().cast::<RingHeader>() });
        let e = unsafe { header::validate_ring(m.as_ptr(), m.len(), None, 64) }.unwrap_err();
        assert!(matches!(
            e,
            RingfireError::CorruptLayout(_)
                | RingfireError::InvalidMagic { .. }
                | RingfireError::VersionMismatch { .. }
        ));
    }
}

#[test]
fn arena_empty_bounds_and_overwrite_contract() {
    let mut m = MmapMut::map_anon(64 + 256).unwrap();
    let arena = unsafe { PayloadArena::init(m.as_mut_ptr(), 256) }.unwrap();
    assert_eq!(arena.capacity(), 256);
    assert_eq!(arena.write_blob(&[], 9).unwrap(), BlobRef::EMPTY);
    assert_eq!(
        arena.write_blob_with(0, 0, |b| b.len()).unwrap(),
        (BlobRef::EMPTY, 0)
    );
    assert_eq!(arena.read_blob(BlobRef::EMPTY, &mut []).unwrap(), 0);
    assert_eq!(arena.view_blob(BlobRef::EMPTY, |b| b.len()), 0);
    assert!(matches!(
        arena.reserve(257, 0),
        Err(RingfireError::ArenaPayloadTooLarge {
            len: 257,
            max_capacity: 256
        })
    ));
    let r = arena.write_blob(b"abcdef", 3).unwrap();
    assert_eq!(r.flags, 3);
    assert!(!arena.is_lapped(r));
    assert!(matches!(
        arena.read_blob(r, &mut [0; 2]),
        Err(RingfireError::BufferTooSmall {
            required: 6,
            provided: 2
        })
    ));
    let invalid = BlobRef {
        offset: 255,
        len: 2,
        flags: 0,
    };
    assert!(!arena.is_in_bounds(invalid));
    assert!(matches!(
        arena.read_blob(invalid, &mut [0; 2]),
        Err(RingfireError::CorruptLayout(_))
    ));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || arena.view_blob(invalid, |_| ())
        ))
        .is_err()
    );
    arena.write_blob(&[1; 256], 0).unwrap();
    assert!(arena.is_lapped(r));
    let h = unsafe { &mut *m.as_mut_ptr().cast::<ArenaHeader>() };
    h.mask = 1;
    assert!(matches!(
        unsafe { PayloadArena::from_ptr(m.as_mut_ptr()) },
        Err(RingfireError::CorruptLayout(_))
    ));
    unsafe {
        (*m.as_mut_ptr().cast::<ArenaHeader>()).capacity = 3;
    }
    assert!(matches!(
        unsafe { PayloadArena::from_ptr(m.as_mut_ptr()) },
        Err(RingfireError::InvalidCapacity(3))
    ));
    assert!(matches!(
        unsafe { PayloadArena::init(m.as_mut_ptr(), 3) },
        Err(RingfireError::InvalidCapacity(3))
    ));
}

#[test]
fn blob_options_empty_payload_and_reader_metrics() {
    let d = Dir::new();
    let path = d.path("blob");
    assert!(matches!(
        BlobProducer::<u64>::create(&path, 3, 256),
        Err(RingfireError::InvalidCapacity(3))
    ));
    assert!(matches!(
        BlobProducer::<u64>::create(&path, 4, 3),
        Err(RingfireError::InvalidCapacity(3))
    ));
    let mut p = BlobProducerBuilder::new(4, 256)
        .max_readers(1)
        .file_mode(0o600)
        .exclusive_lock(false)
        .cleanup_mode(CleanupMode::Persistent)
        .build::<u64, _>(&path)
        .unwrap();
    let mut c = BlobConsumer::<u64>::attach_with_name(&path, "test").unwrap();
    assert_eq!(c.cursor(), 1);
    assert_eq!(c.lag(), 0);
    assert_eq!(p.min_reader_seq(), Some(1));
    assert_eq!(c.registry().unwrap().capacity(), 1);
    let mut extra = BlobConsumer::<u64>::attach(&path).unwrap();
    assert_eq!(p.push_with(&7, 0, |b| b.len()).unwrap(), (1, 0));
    assert_eq!(p.sequence(), 1);
    assert_eq!(p.reader_lag(), 1);
    assert_eq!(p.headroom(), 3);
    assert_eq!(p.active_readers().len(), 1);
    assert_eq!(c.lag(), 1);
    assert_eq!(c.view(|m, b| (*m, b.len())).unwrap(), Some((7, 0)));
    assert_eq!(extra.recv(&mut 0, &mut []).unwrap(), Some(0));
    assert_eq!(c.cursor(), 2);
    assert_eq!(p.reader_lag(), 0);
    assert!(format!("{p:?}").contains("arena_capacity"));
    assert!(format!("{c:?}").contains("lapped_total"));
    drop(c);
    drop(extra);
    assert_eq!(p.min_reader_seq(), None);
    drop(p);
    assert!(path.exists());
}

#[test]
fn multiplexer_controls_and_fairness() {
    let d = Dir::new();
    let a = d.path("a");
    let b = d.path("b");
    let mut pa = RingProducer::<u64>::create(&a, 8).unwrap();
    let mut pb = RingProducer::<u64>::create(&b, 8).unwrap();
    let mut mux = RingMultiplexer::<u64>::default();
    assert!(mux.is_empty());
    assert_eq!(mux.try_recv_any(), None);
    assert_eq!(mux.attach(&a).unwrap(), 0);
    assert_eq!(mux.attach_named("prices", &b).unwrap(), 1);
    assert!(!mux.is_empty());
    assert_eq!(mux.channel_name(0), Some("channel_0"));
    assert_eq!(mux.channel_name(1), Some("prices"));
    assert_eq!(mux.channel_name(2), None);
    assert!(mux.consumer(2).is_none());
    assert!(mux.consumer_mut(2).is_none());
    assert_eq!(mux.consumer(0).unwrap().capacity(), 8);
    pa.push_batch(&[10, 11]);
    pb.push_batch(&[20, 21]);
    assert_eq!(mux.recv_batch_any(3), vec![(0, 10), (1, 20), (0, 11)]);
    assert_eq!(mux.try_recv_priority(), Some((1, 21)));
    assert_eq!(mux.try_recv_priority(), None);
    assert!(mux.recv_batch_any(8).is_empty());
    assert!(mux.recv_batch_any(0).is_empty());
    assert_eq!(mux.consumer_mut(0).unwrap().seek(1), 0);
    assert_eq!(mux.try_recv_any(), Some((0, 10)));
    assert!(mux.attach(d.path("missing")).is_err());
}

#[test]
fn mpmc_blocking_receive_and_stalled_ticket_recovery() {
    let d = Dir::new();
    let path = d.path("mpmc");
    assert!(matches!(
        MpmcProducer::<u64>::create(&path, 3),
        Err(RingfireError::InvalidCapacity(3))
    ));
    let mut p = MpmcProducer::<u64>::create(&path, 4).unwrap();
    p.set_cleanup_mode(CleanupMode::Persistent);
    assert_eq!(p.capacity(), 4);
    let mut c = MpmcQueueConsumer::<u64>::attach(&path).unwrap();
    struct Publish<'a>(&'a MpmcProducer<u64>, usize);
    impl WaitStrategy for Publish<'_> {
        fn wait(&mut self, _: &RingHeader, _: u64) {
            self.1 += 1;
            self.0.push(&42);
        }
        fn reset(&mut self) {
            self.1 += 10;
        }
    }
    let mut wait = Publish(&p, 0);
    assert_eq!(c.recv_blocking(&mut wait), 42);
    assert_eq!(wait.1, 11);
    drop(c);
    let mut m = map(&path);
    let h = unsafe { &mut *m.as_mut_ptr().cast::<RingHeader>() };
    // Simulate a producer dying after claiming ticket 2 but before publishing it.
    h.claim_seq.store(3, Ordering::Release);
    let mut c = MpmcQueueConsumer::<u64>::attach(&path).unwrap();
    assert_eq!(c.try_recv(), None);
    for n in 3..=7 {
        assert_eq!(p.push(&n), n);
    }
    assert_eq!(c.try_recv(), Some(4));
    assert_eq!(c.dropped_count(), 2);
    assert_eq!(c.try_recv(), Some(5));
    assert_eq!(c.try_recv(), Some(6));
    assert_eq!(c.try_recv(), Some(7));
    assert_eq!(c.try_recv(), None);
    drop(p);
    assert!(path.exists());
}

#[test]
fn wait_strategies_return_and_reset_on_empty_and_ready_headers() {
    let d = Dir::new();
    let path = d.path("ring");
    let mut p = RingProducer::<u64>::create(&path, 4).unwrap();
    let m = map(&path);
    let h = unsafe { &*m.as_ptr().cast::<RingHeader>() };
    let mut spin = BusySpin::new();
    spin.wait(h, 1);
    spin.reset();
    let mut backoff = YieldBackoff::new(1);
    backoff.wait(h, 1);
    backoff.wait(h, 1);
    backoff.reset();
    backoff.wait(h, 1);
    let mut default_backoff = YieldBackoff::default();
    default_backoff.wait(h, 1);
    default_backoff.reset();
    let mut futex = FutexWait::new(0, Some(Duration::from_millis(1)));
    let start = Instant::now();
    futex.wait(h, 1);
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(h.waiting_consumers.load(Ordering::Acquire), 0);
    p.push(&7);
    futex.wait(h, 1);
    futex.reset();
    let before = CycleStamp::now();
    let after = CycleStamp::now();
    assert_eq!(
        after.diff_cycles(&before),
        after.tsc.saturating_sub(before.tsc)
    );
    assert!(before.elapsed_cycles() >= after.diff_cycles(&before));
    assert_eq!(CycleStamp::cycles_to_ns(900, 3.0), 300.0);
    #[cfg(target_arch = "aarch64")]
    assert!(CycleStamp::counter_frequency_hz().unwrap() > 0);
}

#[test]
fn errors_keep_actionable_context_and_io_sources() {
    let cases: Vec<(RingfireError, &str)> = vec![
        (RingfireError::InvalidCapacity(3), "Capacity 3"),
        (
            RingfireError::InvalidMagic {
                expected: 1,
                actual: 2,
            },
            "0x0000000000000002",
        ),
        (
            RingfireError::VersionMismatch {
                expected: 2,
                actual: 1,
            },
            "expected 2, got 1",
        ),
        (
            RingfireError::ElementSizeMismatch {
                expected: 16,
                actual: 8,
            },
            "16 bytes, got 8",
        ),
        (RingfireError::ProducerAlreadyExists, "Exclusive producer"),
        (
            RingfireError::KeyOutOfRange {
                key: 8,
                capacity: 4,
            },
            "key 8",
        ),
        (
            RingfireError::ValueSizeMismatch {
                expected: 8,
                actual: 4,
            },
            "8 bytes, got 4",
        ),
        (
            RingfireError::SchemaMismatch {
                expected: 1,
                actual: 2,
                type_name: "Tick",
            },
            "Tick",
        ),
        (
            RingfireError::ArenaPayloadTooLarge {
                len: 10,
                max_capacity: 8,
            },
            "10 bytes",
        ),
        (
            RingfireError::BufferTooSmall {
                required: 8,
                provided: 4,
            },
            "provided 4",
        ),
        (RingfireError::NoAvailableReaderSlots, "no free slots"),
        (RingfireError::NoOffsetFileConfigured, "No offset file"),
        (
            RingfireError::BackpressureBufferFull,
            "slowest active reader",
        ),
        (RingfireError::CorruptLayout("bad slot"), "bad slot"),
        (RingfireError::WriterStalled { key: 9 }, "key 9"),
        (RingfireError::Unsupported("mode"), "mode"),
        (RingfireError::Protocol("frame"), "frame"),
    ];
    for (e, message) in cases {
        assert!(e.to_string().contains(message), "{e}");
        assert!(e.source().is_none());
    }
    let e: RingfireError =
        std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied").into();
    assert_eq!(e.source().unwrap().to_string(), "denied");
    assert_eq!(e.to_string(), "I/O error: denied");
}

#[test]
fn lossless_try_push_recovers_dead_reader_and_updates_batch_cursor() {
    let d = Dir::new();
    let path = d.path("lossless");
    let mut producer = RingProducerBuilder::new(4)
        .flow_control(FlowControl::LosslessBackpressure)
        .max_readers(1)
        .build::<u64, _>(&path)
        .unwrap();
    let mut reader = RingConsumer::<u64>::attach(&path).unwrap();
    producer.push_batch(&[]);
    producer.push_batch(&[1, 2, 3, 4]);
    assert!(matches!(
        producer.try_push(&5),
        Err(RingfireError::BackpressureBufferFull)
    ));
    let mut batch = [0; 2];
    assert_eq!(reader.recv_batch(&mut batch), 2);
    assert_eq!(batch, [1, 2]);
    assert_eq!(producer.headroom(), 2);
    producer.try_push(&5).unwrap();
    producer.try_push(&6).unwrap();
    // Replace the registration's PID with an actual reaped PID to simulate a crashed
    // reader without leaking a child or depending on a guessed unused process number.
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let pid = child.id();
    assert!(child.wait().unwrap().success());
    let m = map(&path);
    let h = unsafe { &*m.as_ptr().cast::<RingHeader>() };
    let slot = unsafe {
        &*m.as_ptr()
            .add(h.reader_registry_offset as usize)
            .cast::<ReaderSlot>()
    };
    slot.pid.store(pid, Ordering::Release);
    let mut accepted = false;
    for _ in 0..1024 {
        match producer.try_push(&7) {
            Ok(()) => {
                accepted = true;
                break;
            }
            Err(RingfireError::BackpressureBufferFull) => {}
            Err(e) => panic!("unexpected push failure: {e}"),
        }
    }
    assert!(accepted, "try_push did not reclaim a dead reader");
    assert_eq!(producer.published_sequence(), 7);
    assert!(producer.registry().unwrap().active_readers(7).is_empty());
    let mut late = RingConsumer::<u64>::attach(&path).unwrap();
    let mut out = [0; 4];
    assert_eq!(late.recv_batch(&mut out), 4);
    assert_eq!(out, [4, 5, 6, 7]);
}

#[test]
fn attachments_reject_wrong_types_and_missing_resources() {
    let d = Dir::new();
    let path = d.path("ring");
    let producer = RingProducer::<u64>::create(&path, 4).unwrap();
    assert_eq!(producer.headroom(), 4);
    assert_eq!(producer.reader_lag(), 0);
    assert!(matches!(
        RingConsumer::<[u8; 128]>::attach(&path),
        Err(RingfireError::ElementSizeMismatch { .. })
    ));
    assert!(matches!(
        RingConsumer::<i64>::attach(&path),
        Err(RingfireError::SchemaMismatch { .. })
    ));
    assert!(RingConsumer::<u64>::attach(d.path("missing")).is_err());
    assert!(MpmcProducer::<u64>::attach(d.path("missing")).is_err());
    assert!(MpmcQueueConsumer::<u64>::attach(d.path("missing")).is_err());
    assert!(BlobConsumer::<u64>::attach(d.path("missing")).is_err());
    assert!(BlackboardConsumer::<u64>::attach(d.path("missing")).is_err());
    assert!(RingProducer::<u64>::create(d.path("missing/ring"), 4).is_err());
    assert!(BlackboardProducer::<u64>::create(d.path("missing/board"), 4).is_err());
    assert!(BlobProducer::<u64>::create(d.path("missing/blob"), 4, 256).is_err());
    let plain_path = d.path("plain-packet-ring");
    let _plain = RingProducer::<BlobPacket<u64>>::create(&plain_path, 4).unwrap();
    let mut mmap = map(&plain_path);
    // A plain ring of the same slot stride still cannot be opened as a blob ring.
    unsafe {
        (*mmap.as_mut_ptr().cast::<RingHeader>()).schema_sig = 0;
    }
    assert!(matches!(
        BlobConsumer::<u64>::attach(&plain_path),
        Err(RingfireError::CorruptLayout(_))
    ));
}
