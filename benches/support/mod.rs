//! Helpers shared by the Criterion benches. Not a bench target: Cargo only discovers
//! `benches/*.rs` and `benches/*/main.rs`. See `docs/benchmarking.md` for the methodology.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Directory for bench rings: `RINGFIRE_BENCH_DIR` if set, else `/dev/shm` when it
/// exists (the tmpfs the published figures use), else the system temp dir.
pub fn bench_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("RINGFIRE_BENCH_DIR") {
        return PathBuf::from(dir);
    }
    let shm = Path::new("/dev/shm");
    if shm.is_dir() {
        shm.to_path_buf()
    } else {
        std::env::temp_dir()
    }
}

/// A ring path unique to this process and call, removed when dropped (also on unwind).
pub struct TempShm(PathBuf);

impl TempShm {
    pub fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = bench_dir().join(format!(
            "ringfire_bench_{}_{}_{}.shm",
            name,
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_file(&path);
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempShm {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Aborts the process if the thread holding it unwinds. A bench thread waiting for a
/// reply from a helper thread would otherwise wait forever after the helper panicked.
pub struct AbortOnPanic(pub &'static str);

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("bench helper thread `{}` panicked; aborting", self.0);
            std::process::abort();
        }
    }
}

/// Bounded busy wait. Creating one reads no clock, and the clock is read only once per
/// 65,536 empty polls, so the bound stays off the measured path; a wait that has not
/// finished `limit` after the first clock read panics instead of hanging the bench.
pub struct SpinBound {
    polls: u32,
    since: Option<Instant>,
    limit: Duration,
    what: &'static str,
}

impl SpinBound {
    pub const fn new(what: &'static str, limit: Duration) -> Self {
        Self {
            polls: 0,
            since: None,
            limit,
            what,
        }
    }

    #[inline(always)]
    pub fn spin(&mut self) {
        core::hint::spin_loop();
        self.polls = self.polls.wrapping_add(1);
        if self.polls & 0xFFFF == 0 {
            self.check();
        }
    }

    #[cold]
    #[inline(never)]
    fn check(&mut self) {
        let now = Instant::now();
        match self.since {
            None => self.since = Some(now),
            Some(since) if now - since > self.limit => {
                panic!("{} did not complete within {:?}", self.what, self.limit)
            }
            Some(_) => {}
        }
    }
}

/// Default bound for waiting on a reply from another thread.
pub const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// Times `iters` operations in chunks of at most `chunk`: `prepare(n)` sets up `n`
/// operations untimed (for example, pushes the messages a receive bench will read), then
/// only `run(n)` is timed. Returns the summed time of the `run` calls, for
/// `Bencher::iter_custom`.
pub fn timed_chunks(
    iters: u64,
    chunk: u64,
    prepare: impl FnMut(u64),
    run: impl FnMut(u64),
) -> Duration {
    timed_chunks_with_clock(iters, chunk, prepare, run, Instant::now, Instant::elapsed)
}

/// Like [`timed_chunks`], but `now`/`elapsed` stand in for `Instant::now`/`Instant::elapsed`
/// so a test can supply a fake clock instead of asserting on real scheduling delays (a
/// real sleep timed against a wall-clock upper bound is flaky under load).
pub fn timed_chunks_with_clock<C>(
    iters: u64,
    chunk: u64,
    mut prepare: impl FnMut(u64),
    mut run: impl FnMut(u64),
    mut now: impl FnMut() -> C,
    mut elapsed: impl FnMut(&C) -> Duration,
) -> Duration {
    assert!(chunk > 0);
    let mut total = Duration::ZERO;
    let mut left = iters;
    while left > 0 {
        let n = left.min(chunk);
        prepare(n);
        let start = now();
        run(n);
        total += elapsed(&start);
        left -= n;
    }
    total
}

/// Per-process watchdog for a blocking wait that has no built-in timeout (a blocking recv
/// on another thread, or a plain socket/pipe read) so a peer that stays alive but stops
/// making progress cannot hang the bench forever. The hot path calls only
/// [`Watchdog::heartbeat`], a relaxed atomic increment with no clock read, so it stays
/// cheap enough for operations measured in nanoseconds; a background thread reads the
/// clock on its own schedule and aborts the process if the heartbeat count has not moved
/// for `limit`. This is a real liveness check, not just a panic guard: unlike
/// [`AbortOnPanic`], it also catches a peer that is merely stuck, not just one that
/// panicked.
pub struct Watchdog {
    ticks: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Watchdog {
    /// Starts watching. `what` names the wait in the abort message; `limit` is how long
    /// the heartbeat count may stay unchanged before the process aborts.
    pub fn start(what: impl Into<String>, limit: Duration) -> Self {
        let what = what.into();
        let ticks = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let poll_every = (limit / 8).max(Duration::from_millis(1));
        let (watch_ticks, watch_stop) = (ticks.clone(), stop.clone());
        let thread = thread::spawn(move || {
            let mut last = watch_ticks.load(Ordering::Relaxed);
            let mut since = Instant::now();
            while !watch_stop.load(Ordering::Relaxed) {
                thread::sleep(poll_every);
                let now_ticks = watch_ticks.load(Ordering::Relaxed);
                if now_ticks != last {
                    last = now_ticks;
                    since = Instant::now();
                } else if since.elapsed() > limit {
                    eprintln!("{} made no progress within {:?}; aborting", what, limit);
                    std::process::abort();
                }
            }
        });
        Self {
            ticks,
            stop,
            thread: Some(thread),
        }
    }

    /// Default deadline for a wait that should normally complete in well under a second:
    /// `RINGFIRE_BENCH_WATCHDOG_SECS` if set, else 30 s.
    pub fn default_limit() -> Duration {
        std::env::var("RINGFIRE_BENCH_WATCHDOG_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30))
    }

    /// Records that the watched wait made progress. Cheap enough to call every iteration.
    #[inline(always)]
    pub fn heartbeat(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
