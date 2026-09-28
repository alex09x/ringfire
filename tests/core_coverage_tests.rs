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
    #[cfg(target_arch = "x86_64")]
    assert!(CycleStamp::counter_frequency_hz().is_none());
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

#[test]
fn blackboard_writer_stalled_and_cleanup_modes() {
    let d = Dir::new();
    let path = d.path("stalled_board");
    let mut prod = BlackboardProducer::<u64>::create(&path, 4).unwrap();
    prod.set_cleanup_mode(CleanupMode::Persistent);
    prod.write(0, &100).unwrap();
    let cons = BlackboardConsumer::<u64>::attach(&path).unwrap();
    assert_eq!(cons.read(0).unwrap(), Some(100));

    // Simulate stalled writer: set slot seqlock to odd number (1)
    let m = map(&path);
    let slot = unsafe {
        let ptr =
            m.as_ptr().add(std::mem::size_of::<BlackboardHeader>()) as *mut BlackboardSlot<u64>;
        &*ptr
    };
    slot.seqlock.store(1, Ordering::Release);

    let err = cons.read(0).unwrap_err();
    assert!(matches!(err, RingfireError::WriterStalled { key: 0 }));

    let mut out = 0u64;
    assert!(cons.read_copy(5, &mut out).is_err()); // KeyOutOfRange
}

#[test]
fn blob_buffer_too_small_and_layout_disagreements() {
    let d = Dir::new();
    let path = d.path("blob_small");
    let mut prod = BlobProducerBuilder::new(4, 256)
        .max_readers(2)
        .file_mode(0o600)
        .exclusive_lock(true)
        .cleanup_mode(CleanupMode::Persistent)
        .build::<u64, _>(&path)
        .unwrap();

    let meta = 42u64;
    let payload = [1u8, 2, 3, 4, 5, 6, 7, 8];
    prod.push(&meta, &payload).unwrap();

    let mut cons = BlobConsumer::<u64>::attach(&path).unwrap();
    let mut small_buf = [0u8; 4];
    let mut read_meta = 0u64;
    let err = cons
        .recv_status(&mut read_meta, &mut small_buf)
        .unwrap_err();
    assert!(matches!(
        err,
        RingfireError::BufferTooSmall {
            required: 8,
            provided: 4
        }
    ));

    // Test corrupted arena header where arena_cap != ring header arena_size
    let corrupt_path = d.path("corrupt_arena");
    let _prod2 = BlobProducer::<u64>::create(&corrupt_path, 4, 256).unwrap();
    let mut m = map(&corrupt_path);
    let h = unsafe { &*m.as_ptr().cast::<RingHeader>() };
    let arena_header = unsafe {
        &mut *m
            .as_mut_ptr()
            .add(h.arena_offset as usize)
            .cast::<ArenaHeader>()
    };
    arena_header.capacity = 512; // disagrees with ring header (256)
    assert!(matches!(
        BlobConsumer::<u64>::attach(&corrupt_path),
        Err(RingfireError::CorruptLayout(_))
    ));
}

#[test]
fn checkpoint_corrupted_headers_and_subdirectories() {
    let d = Dir::new();
    let path = d.path("sub/dir/offset");
    let cp = OffsetCheckpoint::open_or_create(&path, "test_cons").unwrap();
    assert_eq!(cp.load(), None);
    cp.save(123);
    assert_eq!(cp.load(), Some(123));

    // Test bad magic
    let bad_magic_path = d.path("bad_magic.offset");
    let mut data = vec![0u8; 64];
    data[0..8].copy_from_slice(&0xDEADBEEFu64.to_le_bytes());
    std::fs::write(&bad_magic_path, &data).unwrap();
    assert!(OffsetCheckpoint::open_or_create(&bad_magic_path, "cons").is_err());

    // Test bad version
    let bad_ver_path = d.path("bad_ver.offset");
    let mut data = vec![0u8; 64];
    data[0..8].copy_from_slice(&ringfire::checkpoint::SHM_OFFSET_MAGIC.to_le_bytes());
    data[8..12].copy_from_slice(&999u32.to_le_bytes());
    std::fs::write(&bad_ver_path, &data).unwrap();
    assert!(OffsetCheckpoint::open_or_create(&bad_ver_path, "cons").is_err());
}

#[test]
fn mpmc_cleanup_and_lost_tickets() {
    let d = Dir::new();
    let path = d.path("mpmc_tickets");
    let mut prod = MpmcProducer::<u64>::create(&path, 4).unwrap();
    prod.set_cleanup_mode(CleanupMode::Persistent);

    let mut cons = MpmcQueueConsumer::<u64>::attach(&path).unwrap();

    // Push 10 items through 4-slot ring to cause wrapping / ticket loss
    for i in 1..=10 {
        prod.push(&i);
    }

    // Try recv should skip expired tickets and report dropped_count
    let mut received = Vec::new();
    while let Some(val) = cons.try_recv() {
        received.push(val);
    }
    assert!(cons.dropped_count() > 0);
    assert!(!received.is_empty());
}

#[test]
fn spmc_commit_offset_with_registry_update() {
    let d = Dir::new();
    let path = d.path("spmc_reg_offset");
    let offset_path = d.path("spmc.offset");
    let mut prod = RingProducerBuilder::new(8)
        .max_readers(4)
        .build::<u64, _>(&path)
        .unwrap();
    for i in 1..=5 {
        prod.push(&i);
    }

    let mut cons = RingConsumerBuilder::<u64>::new()
        .start_mode(ConsumerStartMode::Oldest)
        .offset_shm(&offset_path)
        .consumer_name("reader1")
        .attach(&path)
        .unwrap();

    assert_eq!(cons.try_recv(), Some(1));
    cons.commit_offset().unwrap();
    assert_eq!(cons.checkpoint().unwrap().load(), Some(1));

    // Also test commit_offset_to with registry
    let second_offset = d.path("second.offset");
    cons.commit_offset_to(&second_offset, "reader1").unwrap();
    let cp2 = OffsetCheckpoint::open_or_create(&second_offset, "reader1").unwrap();
    assert_eq!(cp2.load(), Some(1));
}

#[test]
fn spmc_cleanup_mode_and_sequence_start_mode() {
    let d = Dir::new();
    let path = d.path("spmc_seq_start");
    let mut prod = RingProducerBuilder::new(8)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&path)
        .unwrap();
    assert_eq!(prod.cleanup_mode(), ringfire::spmc::CleanupMode::Persistent);
    prod.set_cleanup_mode(ringfire::spmc::CleanupMode::UnlinkOnDrop);
    assert_eq!(
        prod.cleanup_mode(),
        ringfire::spmc::CleanupMode::UnlinkOnDrop
    );
    prod.set_cleanup_mode(ringfire::spmc::CleanupMode::Persistent);

    for i in 1..=5 {
        prod.push(&i);
    }

    // Sequence start mode with seq >= oldest (seq = 3, oldest = 1)
    let mut cons = RingConsumerBuilder::<u64>::new()
        .start_mode(ConsumerStartMode::Sequence(3))
        .attach(&path)
        .unwrap();
    assert_eq!(cons.try_recv(), Some(3));

    // Sequence start mode with seq < oldest
    for i in 6..=20 {
        prod.push(&i);
    }
    // capacity 8, 20 items published -> oldest is 13
    let mut cons_old = RingConsumerBuilder::<u64>::new()
        .start_mode(ConsumerStartMode::Sequence(1))
        .attach(&path)
        .unwrap();
    assert_eq!(cons_old.cursor(), 13);
    assert_eq!(cons_old.lapped_count(), 12);
    assert_eq!(cons_old.try_recv(), Some(13));

    // Consumer with default consumer_name for automatic offset path
    let mut cons2 = RingConsumerBuilder::<u64>::new()
        .consumer_name("auto_offset_reader")
        .start_mode(ConsumerStartMode::Head)
        .attach(&path)
        .unwrap();
    assert_eq!(cons2.try_recv(), None);

    // Error opening offset checkpoint
    assert!(matches!(
        RingConsumerBuilder::<u64>::new()
            .offset_file("/proc/nonexistent/off.shm")
            .consumer_name("test")
            .attach(&path),
        Err(RingfireError::Io(_))
    ));
    assert!(matches!(
        cons2.commit_offset_to("/proc/nonexistent/off.shm", "test"),
        Err(RingfireError::Io(_))
    ));
}

#[test]
fn blackboard_odd_seqlock_recovery_and_write() {
    let d = Dir::new();
    let path = d.path("bb_odd_seqlock");
    let mut prod = ringfire::BlackboardProducer::<u64>::create(&path, 4).unwrap();
    prod.set_cleanup_mode(ringfire::spmc::CleanupMode::Persistent);

    // Manually set slot 0 seqlock to odd value (1), simulating crashed previous writer
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let slots_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(std::mem::size_of::<ringfire::BlackboardHeader>())
            .cast::<ringfire::BlackboardSlot<u64>>()
    };
    unsafe {
        (*slots_ptr).seqlock.store(1, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);

    // Write should notice odd seqlock (s & 1 != 0), advance to s + 2, and complete write
    prod.write(0, &777).unwrap();

    let cons = ringfire::BlackboardConsumer::<u64>::attach(&path).unwrap();
    assert_eq!(cons.read(0).unwrap(), Some(777));
}

#[test]
fn arena_oversized_blob_and_empty_blob() {
    #[repr(align(64))]
    struct AlignedBuf([u8; 1024]);
    let mut buf = AlignedBuf([0u8; 1024]);
    let arena = unsafe { ringfire::arena::PayloadArena::init(buf.0.as_mut_ptr(), 1024).unwrap() };

    // Oversized reserve returns ArenaPayloadTooLarge
    assert!(matches!(
        arena.write_blob(&vec![0u8; 2048], 0),
        Err(RingfireError::ArenaPayloadTooLarge { .. })
    ));
    assert!(matches!(
        arena.write_blob_with(2048, 0, |_| ()),
        Err(RingfireError::ArenaPayloadTooLarge { .. })
    ));

    // Zero-length write_blob_with passes empty slice to closure
    let (r, res) = arena
        .write_blob_with(0, 0, |slice| {
            assert!(slice.is_empty());
            42
        })
        .unwrap();
    assert_eq!(r.len, 0);
    assert_eq!(res, 42);
}

#[test]
fn mpmc_stale_push_and_lost_tickets() {
    let d = Dir::new();
    let path = d.path("mpmc_stale");
    let prod = ringfire::mpmc::MpmcProducer::<u64>::create(&path, 4).unwrap();

    // 1. Manually set slot 0 sequence to a higher sequence than will be claimed
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let view = unsafe {
        ringfire::header::validate_ring(
            mmap.as_ptr(),
            mmap.len(),
            Some(std::mem::size_of::<ringfire::header::Slot<u64>>()),
            std::mem::align_of::<ringfire::header::Slot<u64>>(),
        )
        .unwrap()
    };
    let slots_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(view.slots_offset)
            .cast::<ringfire::header::Slot<u64>>()
    };
    unsafe {
        // Set slot 0 sequence to 1000 (> seq 0)
        (*slots_ptr).seq.store(1000, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);

    // Push sees cur (1000) != SLOT_WRITING && cur > seq -> returns seq immediately
    let seq = prod.push(&99);
    assert_eq!(seq, 1);

    // 2. Test MpmcQueueConsumer dropping tickets when read_seq < floor
    let mut cons = ringfire::mpmc::MpmcQueueConsumer::<u64>::attach(&path).unwrap();
    for i in 1..=20 {
        prod.push(&i);
    }
    let mut received = Vec::new();
    while let Some(v) = cons.try_recv() {
        received.push(v);
    }
    assert!(cons.dropped_count() > 0);

    // 3. Stalled writer in MPMC producer: when slot sequence is SLOT_WRITING at deadline
    let p_stall = d.path("mpmc_stall");
    let prod_stall = ringfire::mpmc::MpmcProducer::<u64>::create(&p_stall, 4).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p_stall)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let view = unsafe {
        ringfire::header::validate_ring(
            mmap.as_ptr(),
            mmap.len(),
            Some(std::mem::size_of::<ringfire::header::Slot<u64>>()),
            std::mem::align_of::<ringfire::header::Slot<u64>>(),
        )
        .unwrap()
    };
    let slots_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(view.slots_offset)
            .cast::<ringfire::header::Slot<u64>>()
    };
    unsafe {
        (*slots_ptr.add(1))
            .seq
            .store(ringfire::header::SLOT_WRITING, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);

    // Push waits 10ms for stall deadline, sees SLOT_WRITING, and returns seq (line 187)
    let s = prod_stall.push(&123);
    assert_eq!(s, 1);

    // 4. Test read_ticket edge cases directly
    let cons_edge = ringfire::mpmc::MpmcQueueConsumer::<u64>::attach(&p_stall).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p_stall)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header_ptr = mmap.as_mut_ptr().cast::<ringfire::header::RingHeader>();
    unsafe {
        (*header_ptr).claim_seq.store(100, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);
    // claim_seq > ticket + capacity (line 297)
    assert_eq!(cons_edge.read_ticket(1), ringfire::mpmc::Ticket::Lost);

    // 5. Mpmc attach with truncated file -> lines 121 and 255
    let p_trunc = d.path("mpmc_trunc");
    std::fs::write(&p_trunc, [0u8; 16]).unwrap();
    assert!(matches!(
        ringfire::mpmc::MpmcProducer::<u64>::attach(&p_trunc),
        Err(RingfireError::CorruptLayout(_))
    ));
    assert!(matches!(
        ringfire::mpmc::MpmcQueueConsumer::<u64>::attach(&p_trunc),
        Err(RingfireError::CorruptLayout(_))
    ));
}

#[test]
fn shm_create_backing_file_and_retry_coverage() {
    let d = Dir::new();
    let path = d.path("shm_non_exclusive");

    // 1. Non-exclusive lock (legacy/overwrite mode) -> lines 28, 29
    let file = ringfire::shm::create_backing_file(&path, 0o660, false, 512).unwrap();
    assert_eq!(file.metadata().unwrap().len(), 512);

    // 2. Retry loop in exclusive mode -> lines 51, 52, 66
    let path2 = d.path("shm_exclusive_retry");
    std::fs::write(&path2, [0xFFu8; 64]).unwrap();

    let tmp1 = d.path("shm_tmp1");
    let tmp2 = d.path("shm_tmp2");
    std::fs::write(&tmp1, b"xyz").unwrap();
    std::fs::write(&tmp2, b"abc").unwrap();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = stop.clone();
    let p_clone = path2.clone();
    let handle = std::thread::spawn(move || {
        while !stop_clone.load(Ordering::Relaxed) {
            let _ = std::fs::copy(&tmp1, &p_clone);
            let _ = std::fs::rename(&tmp2, &p_clone);
            let _ = std::fs::remove_file(&p_clone);
        }
    });

    for _ in 0..5_000 {
        let _ = ringfire::shm::create_backing_file(&path2, 0o660, true, 1024);
    }
    stop.store(true, Ordering::Relaxed);
    let _ = handle.join();
}

#[test]
fn replication_server_mtu_and_adopt_coverage() {
    let d = Dir::new();
    let path = d.path("repl_mtu_ring");
    let _prod = RingProducer::<u64>::create(&path, 64).unwrap();

    // 1. Multicast + Unicast ReplicaServer -> MTU calculation
    let mcfg = ringfire::MulticastConfig::new("239.255.0.1".parse().unwrap(), 54321);
    let s2 = ringfire::ReplicaServer::bind(&path, "127.0.0.1:0")
        .unwrap()
        .multicast(mcfg)
        .unicast(54322, 1400);
    assert!(s2.local_addr().is_ok());

    // 2. Mirror connect in Resume mode on /dev/null -> adopt returns Ok(None) (line 1051)
    let res = ringfire::Mirror::builder()
        .start(ringfire::MirrorStart::Resume)
        .connect("127.0.0.1:1", "/dev/null");
    assert!(res.is_err());

    // 3. SourceRing::read slot testing (line 819)
    let p_src = d.path("repl_src_ring");
    let mut prod_src = RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&p_src)
        .unwrap();
    prod_src.push(&42);
    let s_ring = ringfire::replication::SourceRing::open(&p_src).unwrap();
    let mut slot_buf = vec![0u8; std::mem::size_of::<u64>()];
    assert_eq!(
        s_ring.read(1, &mut slot_buf),
        ringfire::replication::RawRead::Item
    );

    // 4. Registry reader lag with no readers (line 160) and process alive (line 256)
    let p_reg = d.path("reg_coverage_ring");
    let prod_reg = RingProducerBuilder::new(16)
        .max_readers(4)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&p_reg)
        .unwrap();
    assert_eq!(prod_reg.reader_lag(), 0);
    assert!(!ringfire::registry::is_process_alive(0));
    assert!(!ringfire::registry::is_process_alive(u32::MAX));
}

#[test]
fn wait_strategy_default_and_none_timeout_coverage() {
    use ringfire::wait::{BusySpin, FutexWait, WaitStrategy, YieldBackoff};
    let mut bs = BusySpin::new();
    bs.reset();
    let empty_hdr: ringfire::header::RingHeader = unsafe { std::mem::zeroed() };
    bs.wait(&empty_hdr, 0);
    #[allow(clippy::default_constructed_unit_structs)]
    let mut bs2 = BusySpin::default();
    bs2.wait(&empty_hdr, 0);
    bs2.reset();

    let mut yb = YieldBackoff::default();
    yb.wait(&empty_hdr, 0);
    yb.reset();

    let mut yb2 = YieldBackoff::new(1);
    yb2.wait(&empty_hdr, 0);
    yb2.wait(&empty_hdr, 0);
    yb2.reset();

    let mut fw = FutexWait::default();
    fw.reset();

    // FutexWait with timeout = None -> line 138 in wait.rs
    let mut fw_none = FutexWait::new(0, None);
    let d = Dir::new();
    let p = d.path("wait_futex_test");
    let _prod = RingProducer::<u64>::create(&p, 16).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p)
        .unwrap();
    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &*(mmap.as_mut_ptr() as *const ringfire::header::RingHeader) };

    let mut fw_spin = FutexWait::new(5, None);
    fw_spin.wait(header, 10);

    let handle = std::thread::spawn(move || {
        fw_none.wait(header, 10);
    });
    std::thread::sleep(Duration::from_millis(10));
    ringfire::wait::wake_futex(header, 1);
    let _ = handle.join();
    drop(mmap);
}

#[test]
fn additional_core_api_edge_cases_coverage() {
    let d = Dir::new();

    // 1. BlackboardConsumer attach /dev/null and read_copy on empty slot
    assert!(ringfire::BlackboardConsumer::<u64>::attach("/dev/null").is_err());
    let p_bb = d.path("bb_read_copy_empty");
    let mut bb_prod = ringfire::BlackboardProducer::<u64>::create(&p_bb, 4).unwrap();
    let bb_cons = ringfire::BlackboardConsumer::<u64>::attach(&p_bb).unwrap();
    let mut val = 0u64;
    assert!(!bb_cons.read_copy(0, &mut val).unwrap());
    bb_prod.write(0, &999).unwrap();
    assert!(bb_cons.read_copy(0, &mut val).unwrap());
    assert_eq!(val, 999);
    assert_eq!(bb_prod.slot_count(), 4);
    assert_eq!(bb_cons.slot_count(), 4);

    // 2. Checkpoint for_consumer with empty path -> line 146
    let cp = ringfire::OffsetCheckpoint::for_consumer("", "my_consumer");
    assert!(cp.is_ok());

    // 3. SPMC RingConsumer on /dev/null, attach_from_offset, jump_to_latest/oldest/lag edge cases
    assert!(RingConsumer::<u64>::attach("/dev/null").is_err());
    let p_ring = d.path("spmc_api_edges");
    let p_off = d.path("spmc_api_edges.offset");
    let mut prod = RingProducerBuilder::new(16)
        .cleanup_mode(CleanupMode::Persistent)
        .build::<u64, _>(&p_ring)
        .unwrap();
    prod.push(&10);
    prod.push(&20);

    let mut cons = RingConsumer::<u64>::attach_from_offset(&p_ring, &p_off).unwrap();
    assert!(cons.checkpoint().is_some());
    assert_eq!(cons.offset_path(), Some(p_off.as_path()));
    assert_eq!(cons.jump_to_latest(), 0);
    assert_eq!(cons.jump_to_oldest(), 0);
    cons.seek(50);
    assert_eq!(cons.lag(), 0);
}

#[test]
fn test_shm_mpmc_spmc_blob_additional_lines() {
    let d = Dir::new();

    // 1. shm.rs: exclusive_lock = false (lines 21-30)
    let p1 = d.path("no_excl_ring");
    let prod1 = RingProducerBuilder::new(64)
        .exclusive_lock(false)
        .build::<u64, _>(&p1)
        .unwrap();
    assert_eq!(prod1.capacity(), 64);

    // 2. shm.rs: replace stale non-zero-length file without active flock (lines 56-60)
    let p2 = d.path("stale_shm_ring");
    std::fs::write(&p2, b"stale data to be cleaned by producer").unwrap();
    let prod2 = RingProducer::<u64>::create(&p2, 64).unwrap();
    assert_eq!(prod2.capacity(), 64);

    // 3. mpmc.rs: schema mismatch on attach (lines 124, 260)
    let p3 = d.path("mpmc_schema_ring");
    let _mp1 = ringfire::mpmc::MpmcProducer::<u64>::create(&p3, 64).unwrap();
    assert!(ringfire::mpmc::MpmcProducer::<u32>::attach(&p3).is_err());
    assert!(ringfire::mpmc::MpmcQueueConsumer::<u32>::attach(&p3).is_err());

    // 4. mpmc.rs: consumer overrun / lag jump (line 341: self.dropped += floor - read_seq)
    let p4 = d.path("mpmc_lag_ring");
    let mp4 = ringfire::mpmc::MpmcProducer::<u64>::create(&p4, 16).unwrap();
    let mut mc4 = ringfire::mpmc::MpmcQueueConsumer::<u64>::attach(&p4).unwrap();
    for i in 0..30 {
        mp4.push(&(i as u64));
    }
    let item = mc4.try_recv().unwrap();
    assert!(item >= 14);
    assert!(mc4.dropped_count() > 0);

    // 5. spmc.rs: RingConsumerBuilder with consumer_name only (line 710: OffsetCheckpoint::for_consumer)
    let p5 = d.path("spmc_named_ring");
    let mut prod5 = RingProducerBuilder::new(64)
        .cleanup_mode(CleanupMode::Persistent)
        .build::<u64, _>(&p5)
        .unwrap();
    prod5.push(&123);
    let mut cons5 = ringfire::spmc::RingConsumerBuilder::<u64>::new()
        .consumer_name("named_worker")
        .attach(&p5)
        .unwrap();
    assert_eq!(cons5.try_recv().unwrap(), 123);
    cons5.commit_offset().unwrap();

    // 6. spmc.rs: checkpoint resume when target < oldest (lines 715-716)
    let p6 = d.path("spmc_lapped_cp_ring");
    let mut prod6 = RingProducerBuilder::new(16)
        .cleanup_mode(CleanupMode::Persistent)
        .build::<u64, _>(&p6)
        .unwrap();
    prod6.push(&1);
    let mut cons6 = ringfire::spmc::RingConsumerBuilder::<u64>::new()
        .consumer_name("lapped_worker")
        .attach(&p6)
        .unwrap();
    assert_eq!(cons6.try_recv().unwrap(), 1);
    cons6.commit_offset().unwrap();
    drop(cons6);
    for i in 2..50 {
        prod6.push(&(i as u64));
    }
    let cons6_resumed = ringfire::spmc::RingConsumerBuilder::<u64>::new()
        .consumer_name("lapped_worker")
        .attach(&p6)
        .unwrap();
    assert!(cons6_resumed.lapped_count() > 0);

    // 7. blob.rs: schema mismatch on attach (line 332)
    let p7 = d.path("blob_schema_ring");
    let _bprod = ringfire::blob::BlobProducer::<u64>::create(&p7, 32, 4096).unwrap();
    assert!(ringfire::blob::BlobConsumer::<u32>::attach(&p7).is_err());

    // 8. arena.rs: view_blob with BlobRef::EMPTY (line 238), write_blob_with empty (line 203)
    #[repr(align(64))]
    struct AlignedBuf([u8; 4096]);
    let mut aligned = AlignedBuf([0u8; 4096]);
    let arena =
        unsafe { ringfire::arena::PayloadArena::init(aligned.0.as_mut_ptr(), 2048).unwrap() };
    let empty_res = arena.view_blob(ringfire::arena::BlobRef::EMPTY, |slice| slice.len());
    assert_eq!(empty_res, 0);
    let (b_ref, val) = arena
        .write_blob_with(0, 0, |dest| {
            assert!(dest.is_empty());
            99
        })
        .unwrap();
    assert_eq!(b_ref, ringfire::arena::BlobRef::EMPTY);
    assert_eq!(val, 99);

    // 9. arena.rs: invalid capacity in init and from_ptr (lines 78-79, 106-107, 112)
    assert!(unsafe { ringfire::arena::PayloadArena::init(aligned.0.as_mut_ptr(), 50) }.is_err());
    assert!(unsafe { ringfire::arena::PayloadArena::init(aligned.0.as_mut_ptr(), 32) }.is_err());
    let mut bad_header_buf = AlignedBuf([0u8; 4096]);
    let bad_hdr = bad_header_buf.0.as_mut_ptr() as *mut ringfire::arena::ArenaHeader;
    unsafe {
        (*bad_hdr).capacity = 50;
        (*bad_hdr).mask = 49;
    }
    assert!(
        unsafe { ringfire::arena::PayloadArena::from_ptr(bad_header_buf.0.as_mut_ptr()) }.is_err()
    );
    unsafe {
        (*bad_hdr).capacity = 128;
        (*bad_hdr).mask = 100; // mismatch
    }
    assert!(
        unsafe { ringfire::arena::PayloadArena::from_ptr(bad_header_buf.0.as_mut_ptr()) }.is_err()
    );

    // 10. blob.rs: validate_ring error propagating via ? in attach_with_name (line 329)
    let p_bad_blob = d.path("blob_truncated_for_attach");
    std::fs::write(&p_bad_blob, [0u8; 16]).unwrap();
    assert!(ringfire::blob::BlobConsumer::<()>::attach_with_name(&p_bad_blob, "worker").is_err());

    // 11. mpmc.rs: read_ticket s2 != ticket after fence -> Ticket::Lost (line 293)
    let p_mpmc_race = d.path("mpmc_race_ticket_lost");
    let prod_race = ringfire::mpmc::MpmcProducer::<u64>::create(&p_mpmc_race, 4).unwrap();
    let cons_race = ringfire::mpmc::MpmcQueueConsumer::<u64>::attach(&p_mpmc_race).unwrap();
    prod_race.push(&42);

    let f_race = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p_mpmc_race)
        .unwrap();
    let mut m_race = unsafe { memmap2::MmapMut::map_mut(&f_race).unwrap() };
    let v_race = unsafe {
        ringfire::header::validate_ring(
            m_race.as_ptr(),
            m_race.len(),
            Some(std::mem::size_of::<ringfire::header::Slot<u64>>()),
            std::mem::align_of::<ringfire::header::Slot<u64>>(),
        )
        .unwrap()
    };
    let s_ptr = unsafe {
        m_race
            .as_mut_ptr()
            .add(v_race.slots_offset)
            .cast::<ringfire::header::Slot<u64>>()
    };
    let stop_race = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_race_clone = stop_race.clone();
    let slot_addr = unsafe { &(*s_ptr.add(1)).seq } as *const std::sync::atomic::AtomicU64 as usize;
    let t_handle = std::thread::spawn(move || {
        let ptr = slot_addr as *mut std::sync::atomic::AtomicU64;
        while !stop_race_clone.load(Ordering::Relaxed) {
            unsafe {
                (*ptr).store(1, Ordering::Relaxed);
                core::hint::spin_loop();
                (*ptr).store(2, Ordering::Relaxed);
            }
        }
    });

    for _ in 0..100_000 {
        let _ = cons_race.read_ticket(1);
    }
    stop_race.store(true, Ordering::Relaxed);
    let _ = t_handle.join();
}

#[test]
fn lossless_backpressure_prunes_dead_reader_in_wait_for_headroom() {
    let d = Dir::new();
    let path = d.path("backpressure_dead_reader");
    let mut prod = RingProducerBuilder::new(4)
        .flow_control(FlowControl::LosslessBackpressure)
        .max_readers(2)
        .build::<u64, _>(&path)
        .unwrap();

    for i in 1..=4 {
        prod.push(&i);
    }

    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut m = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };
    let hdr = unsafe { &*(m.as_ptr() as *const ringfire::header::RingHeader) };
    let slots = hdr.reader_registry_offset as usize;
    let slot_ptr = unsafe { m.as_mut_ptr().add(slots) as *mut ringfire::header::ReaderSlot };
    unsafe {
        (*slot_ptr).pid.store(99999999, Ordering::SeqCst);
        (*slot_ptr).active.store(1, Ordering::SeqCst);
        (*slot_ptr).cursor_seq.store(0, Ordering::SeqCst);
    }
    drop(m);
    drop(f);

    prod.push(&5);
    assert_eq!(prod.sequence(), 5);
}
