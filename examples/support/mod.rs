//! Measurement helpers shared by the replication examples: percentiles, sequence
//! accounting (loss, duplicates, reordering), a drift-free pacer and a receive loop that
//! always terminates. Not an example target (Cargo only discovers `examples/*.rs` and
//! `examples/*/main.rs`); `tests/harness_support_tests.rs` checks it.
//! See `docs/benchmarking.md` for how the examples use it.

#![allow(dead_code)]

use std::time::{Duration, Instant};

/// Value at quantile `p` of `sorted` (index `round((n - 1) * p)`), or `None` if empty.
pub fn percentile(sorted: &[i64], p: f64) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p.clamp(0.0, 1.0)).round() as usize;
    Some(sorted[idx])
}

/// Sorts `samples` (nanoseconds) and formats `n`, p50, p90, p99, p99.9, max and min in
/// microseconds, or `n=0 (no samples)`.
pub fn latency_line(samples: &mut [i64]) -> String {
    if samples.is_empty() {
        return "n=0 (no samples)".to_string();
    }
    samples.sort_unstable();
    let us = |p: f64| percentile(samples, p).unwrap() as f64 / 1000.0;
    format!(
        "n={} p50 {:.1} us  p90 {:.1} us  p99 {:.1} us  p99.9 {:.1} us  max {:.1} us  min {:.1} us",
        samples.len(),
        us(0.50),
        us(0.90),
        us(0.99),
        us(0.999),
        samples[samples.len() - 1] as f64 / 1000.0,
        samples[0] as f64 / 1000.0
    )
}

/// `later - earlier` in nanoseconds as a signed sample (a negative value exposes a clock
/// problem instead of wrapping around).
pub fn elapsed_ns(earlier: u64, later: u64) -> i64 {
    later as i64 - earlier as i64
}

/// Accounts for the sequence numbers of received records: new ones, duplicates, ones
/// that arrived below the highest seen so far, and ones outside `1..=max_seq` (which are
/// not tracked, so a corrupt sequence cannot make the bitmap grow without bound).
#[derive(Debug, Clone)]
pub struct SeqTracker {
    seen: Vec<u64>,
    max_seq: u64,
    unique: u64,
    duplicates: u64,
    out_of_order: u64,
    out_of_range: u64,
    lowest: u64,
    highest: u64,
}

impl SeqTracker {
    pub fn new(max_seq: u64) -> Self {
        Self {
            seen: Vec::new(),
            max_seq,
            unique: 0,
            duplicates: 0,
            out_of_order: 0,
            out_of_range: 0,
            lowest: 0,
            highest: 0,
        }
    }

    /// Records one arrival. Returns whether `seq` is a valid sequence seen for the first time.
    pub fn record(&mut self, seq: u64) -> bool {
        if seq == 0 || seq > self.max_seq {
            self.out_of_range += 1;
            return false;
        }
        let (word, bit) = ((seq / 64) as usize, 1u64 << (seq % 64));
        if self.seen.len() <= word {
            self.seen.resize(word + 1, 0);
        }
        if self.seen[word] & bit != 0 {
            self.duplicates += 1;
            return false;
        }
        self.seen[word] |= bit;
        self.unique += 1;
        if seq < self.highest {
            self.out_of_order += 1;
        }
        self.highest = self.highest.max(seq);
        self.lowest = if self.lowest == 0 { seq } else { self.lowest.min(seq) };
        true
    }

    pub fn contains(&self, seq: u64) -> bool {
        let word = (seq / 64) as usize;
        word < self.seen.len() && self.seen[word] & (1u64 << (seq % 64)) != 0
    }

    /// Sequences in `lo..=hi` never recorded (0 for an empty range).
    pub fn missing_in(&self, lo: u64, hi: u64) -> u64 {
        if lo > hi {
            return 0;
        }
        (lo..=hi).filter(|&s| !self.contains(s)).count() as u64
    }

    pub fn unique(&self) -> u64 {
        self.unique
    }
    pub fn duplicates(&self) -> u64 {
        self.duplicates
    }
    pub fn out_of_order(&self) -> u64 {
        self.out_of_order
    }
    pub fn out_of_range(&self) -> u64 {
        self.out_of_range
    }
    /// Lowest valid sequence recorded (0 if none).
    pub fn lowest(&self) -> u64 {
        self.lowest
    }
    /// Highest valid sequence recorded (0 if none).
    pub fn highest(&self) -> u64 {
        self.highest
    }

    /// One-line summary against the expected range `lo..=hi`.
    pub fn summary(&self, lo: u64, hi: u64) -> String {
        format!(
            "received {} of {} (lost {}), duplicates {}, out of order {}, out of range {}",
            self.unique,
            hi.saturating_sub(lo) + u64::from(hi >= lo),
            self.missing_in(lo, hi),
            self.duplicates,
            self.out_of_order,
            self.out_of_range
        )
    }
}

/// Drift-free schedule: deadline `k` is exactly `start + k * num / den` nanoseconds, so
/// a period that is not a whole number of nanoseconds does not accumulate rounding error.
#[derive(Debug, Clone)]
pub struct Pacer {
    start: Instant,
    num: u128,
    den: u128,
    k: u64,
}

impl Pacer {
    /// One tick every `period`.
    pub fn every(period: Duration) -> Self {
        Self::with_ratio(period.as_nanos(), 1)
    }

    /// `rate` records per second, released `burst` at a time: one tick per burst.
    pub fn per_second(rate: u64, burst: u64) -> Self {
        Self::with_ratio(burst.max(1) as u128 * 1_000_000_000, rate.max(1) as u128)
    }

    fn with_ratio(num: u128, den: u128) -> Self {
        Self {
            start: Instant::now(),
            num,
            den,
            k: 0,
        }
    }

    /// Offset of deadline `k` from the start.
    pub fn offset(&self, k: u64) -> Duration {
        let ns = k as u128 * self.num / self.den;
        Duration::from_nanos(ns.min(u64::MAX as u128) as u64)
    }

    /// Busy-waits until the next deadline (returns at once if it already passed: a late
    /// tick is not skipped, the schedule stays anchored to the start).
    pub fn wait_next(&mut self) {
        let deadline = self.start + self.offset(self.k);
        while Instant::now() < deadline {
            core::hint::spin_loop();
        }
        self.k += 1;
    }
}

/// How [`receive_until`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received {
    /// A record with sequence `>= last` arrived.
    Complete,
    /// Nothing arrived for the idle timeout (before the first record: `first_wait`).
    Idle,
}

/// Polls `poll` until it yields a record whose sequence is at least `last`, or until no
/// *new* highest sequence has arrived for `idle` (`first_wait` before the first record).
/// `on_record` is called for every record right after `poll` returns it, so it can take
/// the arrival timestamp. The clock is read only every 256 polls, whether or not `poll`
/// returned a record. Never waits forever: a lost record ends the run with a loss count
/// instead of a hang, and neither a flood of duplicates/stale sequences nor a `poll` that
/// never returns `None` can keep resetting the idle timer without real progress.
pub fn receive_until<T>(
    mut poll: impl FnMut() -> Option<T>,
    seq_of: impl Fn(&T) -> u64,
    last: u64,
    first_wait: Duration,
    idle: Duration,
    mut on_record: impl FnMut(&T),
) -> Received {
    let mut last_arrival = Instant::now();
    let mut any = false;
    let mut highest = 0u64;
    let mut polls = 0u32;
    loop {
        match poll() {
            Some(record) => {
                on_record(&record);
                any = true;
                let seq = seq_of(&record);
                if seq >= last {
                    return Received::Complete;
                }
                // Only a new highest sequence counts as progress: a duplicate or a
                // record below the highest seen so far must not be able to stall the
                // idle timeout forever.
                if seq > highest {
                    highest = seq;
                    last_arrival = Instant::now();
                }
            }
            None => core::hint::spin_loop(),
        }
        polls = polls.wrapping_add(1);
        if polls.is_multiple_of(256) {
            let limit = if any { idle } else { first_wait };
            if last_arrival.elapsed() > limit {
                return Received::Idle;
            }
        }
    }
}

/// Parses `value` for option `name`, exiting with a message instead of a panic trace.
pub fn parse_arg<T: std::str::FromStr>(name: &str, value: &str) -> T
where
    T::Err: std::fmt::Display,
{
    value.parse().unwrap_or_else(|e| {
        eprintln!("invalid value {:?} for {}: {}", value, name, e);
        std::process::exit(2)
    })
}

/// `true` for `1`/`true`/`yes`.
pub fn parse_flag(value: &str) -> bool {
    matches!(value, "1" | "true" | "yes")
}
