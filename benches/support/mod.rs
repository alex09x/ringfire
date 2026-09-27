//! Helpers shared by the Criterion benches. Not a bench target: Cargo only discovers
//! `benches/*.rs` and `benches/*/main.rs`. See `docs/benchmarking.md` for the methodology.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
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

/// Whole-case deadline, including Criterion warm-up, sampling and helper-thread teardown.
/// The only clock lives in a sleeping watchdog; no per-operation counter or clock is
/// added to the measured loop. Configure longer experiments via the environment.
pub struct Watchdog {
    cancel: Option<mpsc::Sender<()>>,
    worker: Option<thread::JoinHandle<()>>,
}
impl Watchdog {
    pub fn start(what: impl Into<String>, limit: Duration) -> Self {
        let what = what.into();
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            if matches!(rx.recv_timeout(limit), Err(mpsc::RecvTimeoutError::Timeout)) {
                eprintln!(
                    "{} exceeded the whole-case deadline {:?}; aborting",
                    what, limit
                );
                std::process::abort();
            }
        });
        Self {
            cancel: Some(tx),
            worker: Some(worker),
        }
    }
    /// Total time for a case: RINGFIRE_BENCH_WATCHDOG_SECS, default 900 seconds.
    pub fn default_limit() -> Duration {
        let seconds = std::env::var("RINGFIRE_BENCH_WATCHDOG_SECS")
            .map(|s| {
                s.parse::<u64>()
                    .expect("RINGFIRE_BENCH_WATCHDOG_SECS must be a positive integer")
            })
            .unwrap_or(900);
        assert!(seconds > 0, "watchdog deadline must be positive");
        Duration::from_secs(seconds)
    }
}
impl Drop for Watchdog {
    fn drop(&mut self) {
        drop(self.cancel.take());
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
