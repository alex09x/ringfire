use std::sync::atomic::{compiler_fence, AtomicU32, Ordering};
use std::time::Duration;
use crate::header::RingHeader;

/// Strategy used by consumers to wait when the ring buffer has no new messages.
pub trait WaitStrategy: Send {
    /// Wait for a new message to become available.
    fn wait(&mut self, header: &RingHeader, cursor: u64);
    /// Reset internal state after a message was successfully received.
    fn reset(&mut self);
}

/// Ultra-low-latency spin polling using `core::hint::spin_loop()`.
/// Achieves steady-state latency < 20 ns at the cost of 100% CPU usage on the core.
#[derive(Debug, Default, Clone, Copy)]
pub struct BusySpin;

impl BusySpin {
    pub fn new() -> Self {
        Self
    }
}

impl WaitStrategy for BusySpin {
    #[inline(always)]
    fn wait(&mut self, _header: &RingHeader, _cursor: u64) {
        core::hint::spin_loop();
    }

    #[inline(always)]
    fn reset(&mut self) {}
}

/// Adaptive backoff strategy: spins for a configurable number of iterations,
/// then yields the CPU time-slice to other OS threads.
#[derive(Debug, Clone, Copy)]
pub struct YieldBackoff {
    spins: u32,
    max_spins: u32,
}

impl Default for YieldBackoff {
    fn default() -> Self {
        Self::new(64)
    }
}

impl YieldBackoff {
    pub fn new(max_spins: u32) -> Self {
        Self {
            spins: 0,
            max_spins,
        }
    }
}

impl WaitStrategy for YieldBackoff {
    #[inline]
    fn wait(&mut self, _header: &RingHeader, _cursor: u64) {
        if self.spins < self.max_spins {
            self.spins += 1;
            core::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }

    #[inline]
    fn reset(&mut self) {
        self.spins = 0;
    }
}

/// Futex-based wait strategy for 0% CPU consumption when idle.
/// Spins briefly (adaptive fast-path) before putting the calling thread to sleep in the kernel.
///
/// Lost wake-ups are prevented with an asymmetric barrier: producers keep a barrier-free
/// hot path, and a consumer about to sleep issues `membarrier(GLOBAL_EXPEDITED)` (Linux
/// 4.16+), which forces a full barrier on every producer process. Producers that cannot
/// register (Python, kernels without `membarrier`, seccomp-restricted containers) fall back
/// to bounded sleeps: `timeout: None` means [`FutexWait::SAFETY_TIMEOUT`].
#[derive(Debug, Clone, Copy)]
pub struct FutexWait {
    spins: u32,
    spin_limit: u32,
    timeout: Option<Duration>,
}

impl Default for FutexWait {
    fn default() -> Self {
        Self::new(32, Some(Duration::from_millis(50)))
    }
}

impl FutexWait {
    /// Upper bound on a single sleep when no explicit timeout is configured.
    pub const SAFETY_TIMEOUT: Duration = Duration::from_millis(10);

    pub fn new(spin_limit: u32, timeout: Option<Duration>) -> Self {
        Self {
            spins: 0,
            spin_limit,
            timeout,
        }
    }
}

impl WaitStrategy for FutexWait {
    #[inline]
    fn wait(&mut self, header: &RingHeader, cursor: u64) {
        if self.spins < self.spin_limit {
            self.spins += 1;
            core::hint::spin_loop();
            return;
        }

        // Fast check before sleeping: did writer publish while we spun?
        if header.write_seq.load(Ordering::Acquire) >= cursor {
            return;
        }

        let futex_val = header.futex_word.load(Ordering::Acquire);
        header.waiting_consumers.fetch_add(1, Ordering::SeqCst);

        // Either the producer's last publish is visible after this barrier, or its next
        // check of `waiting_consumers` sees our registration and wakes us.
        producer_barrier();

        // Re-check write_seq after registering as waiting to avoid lost wakeups
        if header.write_seq.load(Ordering::Acquire) >= cursor {
            header.waiting_consumers.fetch_sub(1, Ordering::SeqCst);
            return;
        }

        sys_futex_wait(
            &header.futex_word,
            futex_val,
            Some(self.timeout.unwrap_or(Self::SAFETY_TIMEOUT)),
        );
        header.waiting_consumers.fetch_sub(1, Ordering::SeqCst);
    }

    #[inline]
    fn reset(&mut self) {
        self.spins = 0;
    }
}

/// Wakes sleeping consumers on the futex word.
/// Called by the producer after publishing a message if `waiting_consumers > 0`.
#[inline]
pub fn wake_futex(header: &RingHeader, count: i32) {
    // Light side of the asymmetric barrier: keep the compiler from hoisting this load above
    // the publish; the CPU-level ordering comes from the consumer's `membarrier`.
    compiler_fence(Ordering::SeqCst);
    if header.waiting_consumers.load(Ordering::Relaxed) > 0 {
        header.futex_word.fetch_add(1, Ordering::Release);
        sys_futex_wake(&header.futex_word, count);
    }
}

#[cfg(target_os = "linux")]
fn sys_futex_wait(addr: &AtomicU32, val: u32, timeout: Option<Duration>) {
    let timespec = timeout.map(|d| libc::timespec {
        tv_sec: d.as_secs() as libc::time_t,
        tv_nsec: d.subsec_nanos() as libc::c_long,
    });
    let ts_ptr = timespec
        .as_ref()
        .map_or(std::ptr::null(), |ts| ts as *const _);

    unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr as *const AtomicU32 as *const u32,
            libc::FUTEX_WAIT,
            val,
            ts_ptr,
            std::ptr::null::<u32>(),
            0u32,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn sys_futex_wait(_addr: &AtomicU32, _val: u32, timeout: Option<Duration>) {
    // Portable fallback for macOS and BSDs
    match timeout {
        Some(d) => std::thread::sleep(d.min(Duration::from_millis(5))),
        None => std::thread::yield_now(),
    }
}

#[cfg(target_os = "linux")]
fn sys_futex_wake(addr: &AtomicU32, count: i32) {
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr as *const AtomicU32 as *const u32,
            libc::FUTEX_WAKE,
            count,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn sys_futex_wake(_addr: &AtomicU32, _count: i32) {
    // No-op on fallback platform; sleeping threads wake on timeout
}

#[cfg(target_os = "linux")]
const MEMBARRIER_CMD_GLOBAL_EXPEDITED: libc::c_long = 1 << 1;
#[cfg(target_os = "linux")]
const MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED: libc::c_long = 1 << 2;

/// Registers this process as a producer for the consumers' asymmetric barrier.
/// Idempotent; failures (old kernel, seccomp) leave consumers on bounded sleeps.
pub(crate) fn register_producer_barrier() {
    #[cfg(target_os = "linux")]
    {
        static REGISTER: std::sync::Once = std::sync::Once::new();
        REGISTER.call_once(|| unsafe {
            libc::syscall(libc::SYS_membarrier, MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED, 0, 0);
        });
    }
}

/// Heavy side of the asymmetric barrier: a full memory barrier on every running thread of
/// every registered producer process.
#[inline]
fn producer_barrier() {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::syscall(libc::SYS_membarrier, MEMBARRIER_CMD_GLOBAL_EXPEDITED, 0, 0);
    }
}
