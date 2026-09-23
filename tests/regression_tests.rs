//! Regression tests for defects found in the v0.3.0 review (R1..R8) and the follow-up
//! hardening. Each test asserts the correct behaviour.

use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ringfire::{
    BlackboardConsumer, BlackboardProducer, BlobConsumer, BlobProducer, FlowControl,
    MpmcProducer, MpmcQueueConsumer, RingConsumer, RingConsumerBuilder, RingProducer,
    RingProducerBuilder, RingfireError,
};

fn tmp(name: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("regress_{}_{}.shm", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

/// Runs `f` on a thread; returns its result, or None if it did not finish within `limit`.
fn within<R: Send + 'static>(limit: Duration, f: impl FnOnce() -> R + Send + 'static) -> Option<R> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(limit).ok()
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Wide([u64; 32]);

impl Wide {
    fn is_torn(&self) -> bool {
        self.0.iter().any(|&x| x != self.0[0])
    }
}

/// R1: a consumer must never return a torn record, even when lapped continuously.
#[test]
fn r1_spmc_no_torn_reads() {
    let path = tmp("torn");
    let mut producer = RingProducer::<Wide>::create(&path, 4).unwrap();
    let mut consumer = RingConsumer::<Wide>::attach(&path).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let writer = std::thread::spawn(move || {
        let mut i = 1u64;
        while !s.load(Ordering::Relaxed) {
            producer.push(&Wide([i; 32]));
            i += 1;
        }
    });
    let (mut torn, mut reads, mut last) = (0u64, 0u64, 0u64);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        if let Some(w) = consumer.try_recv() {
            reads += 1;
            if w.is_torn() {
                torn += 1;
            }
            assert!(w.0[0] > last, "sequence went backwards: {} after {}", w.0[0], last);
            last = w.0[0];
        }
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    eprintln!("r1: reads={} torn={} lapped={}", reads, torn, consumer.lapped_count());
    assert!(reads > 0);
    assert_eq!(torn, 0, "torn records returned to the application");
}

/// R2: BlobConsumer skips messages whose arena bytes were overwritten instead of hanging.
#[test]
fn r2_blob_consumer_skips_arena_lap() {
    let path = tmp("bloblap");
    let mut producer = BlobProducer::<u64>::create(&path, 64, 4096).unwrap();
    let mut consumer = BlobConsumer::<u64>::attach(&path).unwrap();
    for i in 0..20u64 {
        producer.push(&i, &[i as u8; 1000]).unwrap();
    }
    let got = within(Duration::from_secs(3), move || {
        let mut meta = 0u64;
        let mut out = [0u8; 2048];
        let mut seen = Vec::new();
        while let Some(len) = consumer.recv(&mut meta, &mut out).unwrap() {
            assert!(out[..len].iter().all(|&b| b == meta as u8), "payload of {} corrupted", meta);
            seen.push(meta);
        }
        (seen, consumer.lapped_count(), producer)
    })
    .expect("BlobConsumer::recv hung on a lapped arena");
    // 4 KiB arena holds the last four 1 KiB blobs.
    assert_eq!(got.0, vec![16, 17, 18, 19]);
    assert_eq!(got.1, 16);
}

/// R3: a failed second create must not truncate the live producer's file.
#[test]
fn r3_second_create_leaves_live_ring_alone() {
    let path = tmp("trunc");
    let mut producer = RingProducer::<u64>::create(&path, 1024).unwrap();
    let mut consumer = RingConsumer::<u64>::attach(&path).unwrap();
    let before = std::fs::metadata(&path).unwrap().len();
    assert!(matches!(
        RingProducer::<u64>::create(&path, 1024),
        Err(RingfireError::ProducerAlreadyExists)
    ));
    assert!(matches!(
        BlobProducer::<u64>::create(&path, 64, 4096),
        Err(RingfireError::ProducerAlreadyExists)
    ));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), before);
    producer.push(&7);
    assert_eq!(consumer.try_recv(), Some(7));
}

/// R3b: a stale file left by a dead producer is replaced by a fresh inode, so readers
/// still attached to the old ring are not corrupted.
#[test]
fn r3b_stale_ring_replaced_not_truncated() {
    let path = tmp("stale");
    let mut old = RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::CleanupMode::Persistent)
        .build::<u64, _>(&path)
        .unwrap();
    old.push(&1);
    let mut orphan = RingConsumer::<u64>::attach(&path).unwrap();
    drop(old);

    let mut fresh = RingProducer::<u64>::create(&path, 16).unwrap();
    fresh.push(&100);
    assert_eq!(orphan.try_recv(), Some(1));
    assert_eq!(orphan.try_recv(), None);
    let mut new_reader = RingConsumer::<u64>::attach(&path).unwrap();
    assert_eq!(new_reader.try_recv(), Some(100));
}

/// R4: push_batch honours LosslessBackpressure: a concurrent reader sees every message.
#[test]
fn r4_push_batch_respects_backpressure() {
    let path = tmp("batchbp");
    let mut producer = RingProducerBuilder::new(8)
        .flow_control(FlowControl::LosslessBackpressure)
        .build::<u64, _>(&path)
        .unwrap();
    let mut consumer = RingConsumerBuilder::<u64>::new()
        .start_from_head()
        .attach(&path)
        .unwrap();
    let reader = std::thread::spawn(move || {
        let mut got = Vec::new();
        while got.len() < 1000 {
            if let Some(v) = consumer.try_recv() {
                got.push(v);
            }
        }
        (got, consumer.lapped_count())
    });
    let items: Vec<u64> = (1..=1000).collect();
    for chunk in items.chunks(37) {
        producer.push_batch(chunk);
    }
    let (got, lapped) = within(Duration::from_secs(10), move || reader.join().unwrap())
        .expect("reader starved: producer and reader deadlocked");
    assert_eq!(lapped, 0);
    assert_eq!(got, items);
}

/// R5: the MPMC queue consumer drops overrun tickets instead of spinning forever.
#[test]
fn r5_mpmc_queue_consumer_survives_overrun() {
    let path = tmp("mpmc");
    let producer = MpmcProducer::<u64>::create(&path, 8).unwrap();
    let mut consumer = MpmcQueueConsumer::<u64>::attach(&path).unwrap();
    for i in 0..20 {
        producer.push(&i);
    }
    let (got, dropped, _producer) = within(Duration::from_secs(2), move || {
        let mut got = Vec::new();
        while let Some(v) = consumer.try_recv() {
            got.push(v);
        }
        (got, consumer.dropped_count(), producer)
    })
    .expect("MpmcQueueConsumer::try_recv spins forever on an overwritten ticket");
    assert_eq!(got, (12..20).collect::<Vec<u64>>());
    assert_eq!(dropped, 12);
}

/// R5b: concurrent MPMC producers never interleave payloads in one slot.
#[test]
fn r5b_mpmc_producers_no_torn_slots() {
    let path = tmp("mpmc_torn");
    let p1 = Arc::new(MpmcProducer::<Wide>::create(&path, 4).unwrap());
    let mut consumer = RingConsumer::<Wide>::attach(&path).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..3u64)
        .map(|w| {
            let p = if w == 0 { p1.clone() } else { Arc::new(MpmcProducer::<Wide>::attach(&path).unwrap()) };
            let s = stop.clone();
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !s.load(Ordering::Relaxed) {
                    p.push(&Wide([(w << 56) | i; 32]));
                    i += 1;
                }
            })
        })
        .collect();
    let (mut torn, mut reads) = (0u64, 0u64);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        if let Some(w) = consumer.try_recv() {
            reads += 1;
            torn += w.is_torn() as u64;
        }
    }
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    eprintln!("r5b: reads={} torn={}", reads, torn);
    assert!(reads > 0);
    assert_eq!(torn, 0);
}

fn dump_rows(path: &std::path::Path, tail: usize) -> Vec<Vec<String>> {
    let out = Command::new(env!("CARGO_BIN_EXE_ringfire"))
        .args(["dump", path.to_str().unwrap(), "--tail", &tail.to_string()])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.chars().next().is_some_and(|c| c.is_ascii_digit()))
        .map(|l| l.split_whitespace().map(str::to_string).collect())
        .collect()
}

/// R6: `ringfire dump` reads real slots for plain rings and rings with a registry.
#[test]
fn r6_cli_dump_reads_real_slots() {
    let plain = tmp("dump_plain");
    let mut p1 = RingProducer::<u64>::create(&plain, 16).unwrap();
    let odd = tmp("dump_registry");
    let mut p2 = RingProducerBuilder::new(16)
        .max_readers(3)
        .build::<u64, _>(&odd)
        .unwrap();
    for i in 1..=5u64 {
        p1.push(&(i * 111));
        p2.push(&(i * 111));
    }
    for path in [&plain, &odd] {
        let rows = dump_rows(path, 5);
        assert_eq!(rows.len(), 5);
        for row in rows {
            assert_eq!(row[0], row[2], "dump row shows the wrong slot: {:?}", row);
        }
    }
}

/// R7: Python BlobConsumer returns the right payload for every blob.
#[test]
fn r7_python_blob_consumer_payloads() {
    let path = tmp("pyblob");
    let mut producer = BlobProducer::<u64>::create(&path, 64, 65536).unwrap();
    producer.push(&1, &[0xAA; 100]).unwrap(); // offset 0, reserves 128
    producer.push(&2, &[0xBB; 100]).unwrap(); // offset 128, reserved = 256
    producer.push(&3, &[0xCC; 300]).unwrap(); // offset 256
    let script = format!(
        r#"
import sys, ctypes
sys.path.insert(0, "{py}")
from ringfire.ring import BlobConsumer
class M(ctypes.Structure):
    _fields_ = [("v", ctypes.c_uint64)]
c = BlobConsumer("{path}", M)
for v, b, n in [(1, 0xAA, 100), (2, 0xBB, 100), (3, 0xCC, 300)]:
    m, p = c.try_recv_copy() if v == 3 else c.try_recv()
    assert m.v == v, m.v
    assert bytes(p) == bytes([b]) * n, (v, bytes(p[:8]).hex())
assert c.try_recv() is None
print("OK")
"#,
        py = concat!(env!("CARGO_MANIFEST_DIR"), "/python"),
        path = path.display()
    );
    let out = Command::new("python3").arg("-c").arg(&script).output().unwrap();
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// R7b: Python BlobConsumer skips blobs whose arena bytes were overwritten.
#[test]
fn r7b_python_blob_consumer_skips_lapped_arena() {
    let path = tmp("pyblob_lap");
    let mut producer = BlobProducer::<u64>::create(&path, 64, 4096).unwrap();
    for i in 0..20u64 {
        producer.push(&i, &[i as u8; 1000]).unwrap();
    }
    let script = format!(
        r#"
import sys, ctypes
sys.path.insert(0, "{py}")
from ringfire.ring import BlobConsumer
class M(ctypes.Structure):
    _fields_ = [("v", ctypes.c_uint64)]
c = BlobConsumer("{path}", M)
seen = []
while True:
    r = c.try_recv()
    if r is None:
        break
    m, p = r
    assert bytes(p) == bytes([m.v]) * 1000
    seen.append(m.v)
assert seen == [16, 17, 18, 19], seen
assert c.lapped_count == 16, c.lapped_count
print("OK")
"#,
        py = concat!(env!("CARGO_MANIFEST_DIR"), "/python"),
        path = path.display()
    );
    let out = Command::new("python3").arg("-c").arg(&script).output().unwrap();
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn push_recv_ns(mut producer: RingProducer<u64>, mut consumer: RingConsumer<u64>) -> f64 {
    let n = 200_000u64;
    let start = Instant::now();
    for i in 0..n {
        producer.push(&i);
        assert_eq!(consumer.try_recv(), Some(i));
    }
    start.elapsed().as_nanos() as f64 / n as f64
}

/// R8: lossless flow control costs no syscall per push: within a small factor of lossy.
#[test]
fn r8_lossless_push_cost() {
    let lossy_path = tmp("lossy_cost");
    let lossy = push_recv_ns(
        RingProducer::<u64>::create(&lossy_path, 1 << 16).unwrap(),
        RingConsumerBuilder::<u64>::new().start_from_head().attach(&lossy_path).unwrap(),
    );
    let path = tmp("lossless_cost");
    let lossless = push_recv_ns(
        RingProducerBuilder::new(1 << 16)
            .flow_control(FlowControl::LosslessBackpressure)
            .build::<u64, _>(&path)
            .unwrap(),
        RingConsumerBuilder::<u64>::new().start_from_head().attach(&path).unwrap(),
    );
    eprintln!("r8: push+recv lossy={:.1} ns lossless={:.1} ns", lossy, lossless);
    assert!(lossless < lossy * 3.0 + 20.0, "lossless {:.0} ns vs lossy {:.0} ns", lossless, lossy);
}

/// R9: the FFI consumer honours `slots_offset` (rings with a reader registry).
#[test]
fn r9_ffi_consumer_on_registry_ring() {
    use ringfire::ffi::{ringfire_consumer_attach, ringfire_consumer_close, ringfire_consumer_try_recv};
    let path = tmp("ffi_registry");
    let mut producer = RingProducerBuilder::new(16)
        .flow_control(FlowControl::LosslessBackpressure)
        .build::<u64, _>(&path)
        .unwrap();
    for v in [11u64, 22, 33] {
        producer.push(&v);
    }
    let c_path = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    unsafe {
        let cons = ringfire_consumer_attach(c_path.as_ptr(), 8);
        assert!(!cons.is_null());
        let mut out = 0u64;
        for v in [11u64, 22, 33] {
            assert_eq!(ringfire_consumer_try_recv(cons, (&mut out as *mut u64).cast()), 1);
            assert_eq!(out, v);
        }
        assert_eq!(ringfire_consumer_try_recv(cons, (&mut out as *mut u64).cast()), 0);
        ringfire_consumer_close(cons);
    }
}

/// R10: the header-only C consumer compiles and reads v2 rings (lapping included).
#[test]
fn r10_c_header_consumer() {
    let Ok(cc) = Command::new("cc").arg("--version").output() else {
        eprintln!("r10: no C compiler, skipped");
        return;
    };
    assert!(cc.status.success());
    let dir = std::env::temp_dir().join(format!("regress_c_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let src = dir.join("consumer.c");
    std::fs::write(
        &src,
        r#"
#include "ringfire.h"
#include <stdio.h>
int main(int argc, char** argv) {
    ringfire_c_consumer_t c;
    int rc = ringfire_c_consumer_attach(&c, argv[1], 8);
    if (rc != 0) { printf("attach=%d\n", rc); return 1; }
    uint64_t v, sum = 0, n = 0;
    while (ringfire_c_consumer_try_recv(&c, &v)) { sum += v; n++; }
    printf("n=%llu sum=%llu lapped=%llu\n", (unsigned long long)n,
           (unsigned long long)sum, (unsigned long long)c.lapped_count);
    ringfire_c_consumer_detach(&c);
    return 0;
}
"#,
    )
    .unwrap();
    let bin = dir.join("consumer");
    let build = Command::new("cc")
        .args(["-std=c11", "-Wall", "-Werror", "-O2", "-I", concat!(env!("CARGO_MANIFEST_DIR"), "/include")])
        .arg(&src)
        .arg("-o")
        .arg(&bin)
        .output()
        .unwrap();
    assert!(build.status.success(), "{}", String::from_utf8_lossy(&build.stderr));

    let path = tmp("c_header");
    let mut producer = RingProducerBuilder::new(16).max_readers(3).build::<u64, _>(&path).unwrap();
    for v in 1..=40u64 {
        producer.push(&v);
    }
    let out = Command::new(&bin).arg(&path).output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Attaches at the oldest retained message: 25..=40.
    assert_eq!(stdout.trim(), format!("n=16 sum={} lapped=0", (25..=40u64).sum::<u64>()));
}

/// R11: a blackboard reader reports a writer that died mid-update instead of hanging.
#[test]
fn r11_blackboard_stalled_writer() {
    let path = tmp("bb_stall");
    let mut producer = BlackboardProducer::<u64>::create(&path, 4).unwrap();
    producer.write(0, &5).unwrap();
    let consumer = BlackboardConsumer::<u64>::attach(&path).unwrap();
    // Simulate a writer that crashed after marking slot 0 odd.
    let file = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
    let mut map = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };
    map[128..136].copy_from_slice(&3u64.to_le_bytes());
    let res = within(Duration::from_secs(2), move || consumer.read(0).map_err(|e| e.to_string()))
        .expect("blackboard read hung on a stalled slot");
    assert!(res.unwrap_err().contains("mid-write"));
    drop(producer);
}

/// R12: on a lossless ring, a reader that cannot register is refused, not silently unprotected.
#[test]
fn r12_lossless_registry_full_is_an_error() {
    let path = tmp("reg_full");
    let _producer = RingProducerBuilder::new(16)
        .flow_control(FlowControl::LosslessBackpressure)
        .max_readers(1)
        .build::<u64, _>(&path)
        .unwrap();
    let _first = RingConsumer::<u64>::attach(&path).unwrap();
    assert!(matches!(
        RingConsumer::<u64>::attach(&path),
        Err(RingfireError::NoAvailableReaderSlots)
    ));
}

/// R13: attaching to truncated or foreign files fails cleanly instead of reading out of bounds.
#[test]
fn r13_attach_validates_layout() {
    let path = tmp("layout");
    let producer = RingProducerBuilder::new(1024)
        .cleanup_mode(ringfire::CleanupMode::Persistent)
        .build::<u64, _>(&path)
        .unwrap();
    drop(producer);
    let full = std::fs::metadata(&path).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(full / 2).unwrap();
    assert!(matches!(RingConsumer::<u64>::attach(&path), Err(RingfireError::CorruptLayout(_))));

    std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(16).unwrap();
    assert!(matches!(RingConsumer::<u64>::attach(&path), Err(RingfireError::CorruptLayout(_))));
    let _ = std::fs::remove_file(&path);
}

/// R14: `Sequence(0)` starts at the first message without a phantom lapped message.
#[test]
fn r14_sequence_zero_is_not_lapped() {
    let path = tmp("seq0");
    let mut producer = RingProducer::<u64>::create(&path, 16).unwrap();
    producer.push(&9);
    let mut consumer = RingConsumerBuilder::<u64>::new()
        .start_from_sequence(0)
        .attach(&path)
        .unwrap();
    assert_eq!(consumer.try_recv(), Some(9));
    assert_eq!(consumer.lapped_count(), 0);
}

/// R15: an idle `Stream` consumer backs off to timer sleeps instead of busy-polling.
#[cfg(feature = "tokio")]
#[tokio::test(flavor = "current_thread")]
async fn r15_idle_stream_does_not_busy_poll() {
    use futures_core::Stream;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll, Wake, Waker};

    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let path = tmp("stream");
    let _producer = RingProducer::<u64>::create(&path, 16).unwrap();
    let mut stream = ringfire::AsyncRingConsumer::<u64>::attach(&path)
        .unwrap()
        .with_idle_sleep(Duration::from_millis(50));
    let counter = Arc::new(Counter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&waker);

    // Poll until the stream stops self-waking (it has armed its idle timer).
    let mut polls = 0;
    loop {
        let before = counter.0.load(Ordering::Relaxed);
        assert!(matches!(Pin::new(&mut stream).poll_next(&mut cx), Poll::Pending));
        polls += 1;
        if counter.0.load(Ordering::Relaxed) == before {
            break;
        }
        assert!(polls < 100, "stream keeps waking itself");
    }
    // While the timer is pending, nothing wakes the task.
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(counter.0.load(Ordering::Relaxed), polls - 1);
}
