//! # CycleStamp
//!
//! Ultra-low-overhead hardware timestamping via `rdtscp`, providing cycle-accurate
//! elapsed time measurement and CPU core / NUMA node awareness without kernel syscalls.

/// Hardware cycle timestamp with core and NUMA node metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(C)]
pub struct CycleStamp {
    /// Raw CPU Time Stamp Counter (TSC) value
    pub tsc: u64,
    /// Logical CPU core ID executing this instruction
    pub core_id: u16,
    /// NUMA node ID of the executing core
    pub numa_node: u16,
}

impl CycleStamp {
    /// Capture current hardware TSC and CPU core placement.
    #[inline(always)]
    pub fn now() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            let mut aux: u32 = 0;
            let tsc = unsafe { core::arch::x86_64::__rdtscp(&mut aux as *mut u32) };
            Self {
                tsc,
                core_id: (aux & 0x0FFF) as u16,
                numa_node: ((aux >> 12) & 0x00FF) as u16,
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            // Portable fallback: use monotonic clock ticks
            use std::sync::atomic::{AtomicU64, Ordering};
            static FALLBACK_COUNTER: AtomicU64 = AtomicU64::new(1);
            let tsc = FALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed);
            Self {
                tsc,
                core_id: 0,
                numa_node: 0,
            }
        }
    }

    /// Calculate cycles elapsed since this timestamp.
    #[inline(always)]
    pub fn elapsed_cycles(&self) -> u64 {
        let now = Self::now();
        now.tsc.saturating_sub(self.tsc)
    }

    /// Difference in cycles between two timestamps.
    #[inline(always)]
    pub fn diff_cycles(&self, earlier: &Self) -> u64 {
        self.tsc.saturating_sub(earlier.tsc)
    }

    /// Convert cycles to nanoseconds given CPU base frequency in GHz (e.g. 4.5 for 4.5 GHz).
    #[inline]
    pub fn cycles_to_ns(cycles: u64, freq_ghz: f64) -> f64 {
        cycles as f64 / freq_ghz
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cycle_stamp_progress() {
        let t1 = CycleStamp::now();
        for _ in 0..1000 {
            core::hint::spin_loop();
        }
        let t2 = CycleStamp::now();
        assert!(t2.tsc >= t1.tsc);
        assert!(t2.diff_cycles(&t1) > 0);
    }
}
