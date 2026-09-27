//! Checks of the measurement helpers the benches and replication examples report with:
//! loss/duplicate accounting, percentiles, drift-free pacing, receive loops that end on
//! loss instead of hanging, and the bench chunking that keeps setup out of the timings.

#[path = "../examples/support/mod.rs"]
mod support;

#[path = "../benches/support/mod.rs"]
mod bench_support;

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bench_support::{timed_chunks, SpinBound, TempShm};
use support::{latency_line, percentile, receive_until, Pacer, Received, SeqTracker};

#[test]
fn percentile_of_empty_is_none_and_line_says_so() {
    assert_eq!(percentile(&[], 0.5), None);
    assert_eq!(latency_line(&mut []), "n=0 (no samples)");
}

#[test]
fn percentile_uses_rounded_rank_on_sorted_samples() {
    let sorted: Vec<i64> = (1..=101).collect();
    assert_eq!(percentile(&sorted, 0.0), Some(1));
    assert_eq!(percentile(&sorted, 0.5), Some(51));
    assert_eq!(percentile(&sorted, 0.99), Some(100));
    assert_eq!(percentile(&sorted, 1.0), Some(101));
}

#[test]
fn latency_line_sorts_and_reports_extremes_in_microseconds() {
    let mut samples = vec![3_000, -500, 1_000, 2_000];
    let line = latency_line(&mut samples);
    assert_eq!(samples, vec![-500, 1_000, 2_000, 3_000]);
    assert!(line.starts_with("n=4 "), "{}", line);
    assert!(line.contains("max 3.0 us"), "{}", line);
    assert!(line.contains("min -0.5 us"), "{}", line);
}

#[test]
fn seq_tracker_separates_loss_duplicates_reordering_and_garbage() {
    let mut t = SeqTracker::new(10);
    for seq in [1, 2, 4, 3, 4, 0, 11, 10] {
        t.record(seq);
    }
    assert_eq!(t.unique(), 5);
    assert_eq!(t.duplicates(), 1);
    assert_eq!(t.out_of_order(), 1); // 3 after 4
    assert_eq!(t.out_of_range(), 2); // 0 and 11
    assert_eq!((t.lowest(), t.highest()), (1, 10));
    assert_eq!(t.missing_in(1, 10), 5); // 5..=9
    assert_eq!(t.missing_in(5, 4), 0);
    assert_eq!(
        t.summary(1, 10),
        "received 5 of 10 (lost 5), duplicates 1, out of order 1, out of range 2"
    );
}

#[test]
fn duplicates_do_not_hide_losses() {
    let mut t = SeqTracker::new(4);
    for seq in [1, 1, 2, 2] {
        t.record(seq);
    }
    // Four arrivals for four records, but two records were never delivered.
    assert_eq!(t.unique(), 2);
    assert_eq!(t.missing_in(1, 4), 2);
}

#[test]
fn pacer_deadlines_do_not_accumulate_rounding() {
    // 1e9 / 300,000 is not a whole number of nanoseconds.
    let p = Pacer::per_second(300_000, 1);
    assert_eq!(p.offset(1), Duration::from_nanos(3_333));
    assert_eq!(p.offset(300_000), Duration::from_secs(1));
    let bursts = Pacer::per_second(4_000, 4);
    assert_eq!(bursts.offset(1_000), Duration::from_secs(1));
    let every = Pacer::every(Duration::from_micros(100));
    assert_eq!(every.offset(20_000), Duration::from_secs(2));
}

#[test]
fn pacer_waits_until_each_deadline() {
    let start = Instant::now();
    let mut p = Pacer::every(Duration::from_millis(2));
    for _ in 0..4 {
        p.wait_next();
    }
    // Ticks 0..=3: the last one is due 6 ms after the pacer was created.
    assert!(start.elapsed() >= Duration::from_millis(6));
}

/// A source that yields `records` in order, then nothing.
fn source(records: &[u64]) -> impl FnMut() -> Option<u64> {
    let mut queue: VecDeque<u64> = records.iter().copied().collect();
    move || queue.pop_front()
}

#[test]
fn receive_until_completes_on_the_last_sequence() {
    let mut seen = Vec::new();
    let end = receive_until(
        source(&[1, 2, 3, 4, 5, 6]),
        |s| *s,
        5,
        Duration::from_secs(5),
        Duration::from_secs(5),
        |s| seen.push(*s),
    );
    assert_eq!(end, Received::Complete);
    assert_eq!(seen, vec![1, 2, 3, 4, 5]);
}

#[test]
fn receive_until_ends_by_idle_timeout_when_the_last_record_is_lost() {
    let mut tracker = SeqTracker::new(10);
    let start = Instant::now();
    let end = receive_until(
        source(&[1, 2, 3, 5, 6, 7, 8, 9]),
        |s| *s,
        10,
        Duration::from_secs(5),
        Duration::from_millis(20),
        |s| {
            tracker.record(*s);
        },
    );
    assert_eq!(end, Received::Idle);
    assert!(start.elapsed() < Duration::from_secs(5), "idle timeout not honoured");
    assert_eq!(tracker.unique(), 8);
    assert_eq!(tracker.missing_in(1, 10), 2);
}

#[test]
fn receive_until_gives_up_when_nothing_arrives() {
    let start = Instant::now();
    let end = receive_until(
        source(&[]),
        |s| *s,
        1,
        Duration::from_millis(20),
        Duration::from_secs(60),
        |_| panic!("no record expected"),
    );
    assert_eq!(end, Received::Idle);
    let waited = start.elapsed();
    assert!(waited >= Duration::from_millis(20) && waited < Duration::from_secs(30));
}

#[test]
fn timed_chunks_prepares_every_operation_and_times_only_runs() {
    let mut prepared = Vec::new();
    let mut ran = Vec::new();
    let total = timed_chunks(
        10,
        4,
        |n| {
            prepared.push(n);
            // Setup cost that must not show up in the result.
            std::thread::sleep(Duration::from_millis(30));
        },
        |n| ran.push(n),
    );
    assert_eq!(prepared, vec![4, 4, 2]);
    assert_eq!(ran, vec![4, 4, 2]);
    assert!(total < Duration::from_millis(30), "setup was timed: {:?}", total);
}

#[test]
#[should_panic(expected = "stalled wait did not complete")]
fn spin_bound_turns_a_stalled_wait_into_a_panic() {
    let mut bound = SpinBound::new("stalled wait", Duration::from_millis(1));
    loop {
        bound.spin();
    }
}

#[test]
fn temp_shm_paths_are_unique_and_removed_on_drop() {
    let a = TempShm::new("harness_check");
    let b = TempShm::new("harness_check");
    assert_ne!(a.path(), b.path());
    std::fs::write(a.path(), b"x").unwrap();
    let path = a.path().to_path_buf();
    drop(a);
    assert!(!path.exists());
}
