use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use futures_core::Stream;
use tokio::time::Sleep;

use crate::error::Result;
use crate::spmc::{RecvStatus, RingConsumer};

/// Asynchronous wrapper around `RingConsumer<T>` for the Tokio runtime.
///
/// Designed for high-frequency trading and market-data streaming in async bots:
/// - **Zero-Latency Fast Path**: If a message is published, `recv()` returns in < 20 ns without async overhead.
/// - **Adaptive Spinning**: Spins briefly (`core::hint::spin_loop`) to catch incoming bursts without context switches.
/// - **Cooperative Task Switching**: Calls `tokio::task::yield_now().await` when no traffic is present so other Tokio tasks (websockets, order management, REST APIs) are never starved.
/// - **Zero-CPU Idle Sleep**: Drops into async `tokio::time::sleep` during long quiet periods so CPU usage stays at 0%.
pub struct AsyncRingConsumer<T: Copy + 'static> {
    inner: RingConsumer<T>,
    max_spins: u32,
    max_yields: u32,
    idle_sleep: Duration,
    /// `Stream` backoff state: cooperative yields so far and the pending idle timer.
    stream_yields: u32,
    stream_sleep: Option<Pin<Box<Sleep>>>,
}

impl<T: Copy + 'static> AsyncRingConsumer<T> {
    /// Attaches to a shared memory ring buffer at `path`.
    pub fn attach<P: AsRef<Path>>(path: P) -> Result<Self> {
        let consumer = RingConsumer::attach(path)?;
        Ok(Self::from_consumer(consumer))
    }

    /// Wraps an existing `RingConsumer`.
    pub fn from_consumer(inner: RingConsumer<T>) -> Self {
        Self {
            inner,
            max_spins: 32,
            max_yields: 8,
            idle_sleep: Duration::from_micros(50),
            stream_yields: 0,
            stream_sleep: None,
        }
    }

    /// Sets the maximum spin loop count before cooperatively yielding to Tokio.
    pub fn with_spins(mut self, spins: u32) -> Self {
        self.max_spins = spins;
        self
    }

    /// Sets the maximum cooperative `yield_now()` count before entering async sleep.
    pub fn with_yields(mut self, yields: u32) -> Self {
        self.max_yields = yields;
        self
    }

    /// Sets the idle sleep duration for long quiet periods.
    pub fn with_idle_sleep(mut self, sleep: Duration) -> Self {
        self.idle_sleep = sleep;
        self
    }

    /// Non-blocking try-recv returning `Some(T)` or `None`.
    #[inline(always)]
    pub fn try_recv(&mut self) -> Option<T> {
        self.inner.try_recv()
    }

    /// Non-blocking receive with explicit `RecvStatus` (Ok, Empty, or Lapped).
    #[inline(always)]
    pub fn recv_status(&mut self) -> RecvStatus<T> {
        self.inner.recv_status()
    }

    /// Asynchronously receives the next message, cooperatively yielding to Tokio when waiting.
    pub async fn recv(&mut self) -> T {
        let mut spins = 0;
        let mut yields = 0;
        loop {
            if let Some(item) = self.inner.try_recv() {
                return item;
            }

            if spins < self.max_spins {
                spins += 1;
                core::hint::spin_loop();
                continue;
            }

            if yields < self.max_yields {
                yields += 1;
                tokio::task::yield_now().await;
                continue;
            }

            tokio::time::sleep(self.idle_sleep).await;
            spins = 0;
            yields = 0;
        }
    }

    /// Asynchronously receives the next message with explicit status.
    pub async fn recv_detailed(&mut self) -> (T, u64) {
        let mut spins = 0;
        let mut yields = 0;
        loop {
            match self.inner.recv_status() {
                RecvStatus::Ok(item) => return (item, 0),
                RecvStatus::Lapped { skipped, item } => return (item, skipped),
                RecvStatus::Empty => {}
            }

            if spins < self.max_spins {
                spins += 1;
                core::hint::spin_loop();
                continue;
            }

            if yields < self.max_yields {
                yields += 1;
                tokio::task::yield_now().await;
                continue;
            }

            tokio::time::sleep(self.idle_sleep).await;
            spins = 0;
            yields = 0;
        }
    }

    /// Asynchronously drains up to `buf.len()` messages in a batch.
    pub async fn recv_batch(&mut self, buf: &mut [T]) -> usize {
        if buf.is_empty() {
            return 0;
        }

        let mut spins = 0;
        let mut yields = 0;
        loop {
            let count = self.inner.recv_batch(buf);
            if count > 0 {
                return count;
            }

            if spins < self.max_spins {
                spins += 1;
                core::hint::spin_loop();
                continue;
            }

            if yields < self.max_yields {
                yields += 1;
                tokio::task::yield_now().await;
                continue;
            }

            tokio::time::sleep(self.idle_sleep).await;
            spins = 0;
            yields = 0;
        }
    }

    /// Jumps cursor directly to the latest published sequence, skipping any backlog.
    pub fn jump_to_latest(&mut self) -> u64 {
        self.inner.jump_to_latest()
    }

    /// Jumps cursor to the oldest message still surviving in the buffer.
    pub fn jump_to_oldest(&mut self) -> u64 {
        self.inner.jump_to_oldest()
    }

    /// Total count of messages skipped due to writer lapping.
    #[inline]
    pub fn lapped_count(&self) -> u64 {
        self.inner.lapped_count()
    }

    /// Current cursor sequence number.
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.inner.cursor()
    }

    /// Buffer capacity.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.inner.capacity()
    }

    /// Lag behind the producer.
    #[inline]
    pub fn lag(&self) -> u64 {
        self.inner.lag()
    }
}

/// The stream never ends. While idle it follows the same backoff as [`AsyncRingConsumer::recv`]:
/// a short spin, a few cooperative yields, then `idle_sleep` timer waits, so an idle
/// stream costs no CPU and spawns no tasks.
impl<T: Copy + Unpin + 'static> Stream for AsyncRingConsumer<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(sleep) = this.stream_sleep.as_mut() {
                if sleep.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.stream_sleep = None;
                this.stream_yields = 0;
            }

            for _ in 0..=this.max_spins {
                if let Some(item) = this.inner.try_recv() {
                    this.stream_yields = 0;
                    return Poll::Ready(Some(item));
                }
                core::hint::spin_loop();
            }

            if this.stream_yields < this.max_yields {
                this.stream_yields += 1;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            this.stream_sleep = Some(Box::pin(tokio::time::sleep(this.idle_sleep)));
        }
    }
}
