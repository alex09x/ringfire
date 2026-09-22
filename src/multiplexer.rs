//! # RingMultiplexer & AsyncRingMultiplexer
//!
//! High-performance channel multiplexer for polling across multiple independent
//! shared-memory ring buffers with fair Round-Robin or Priority scheduling.
//!
//! Optimized for trading engines, order routing gateways, and multi-symbol
//! ingestion buses (e.g. streaming hundreds of order books concurrently
//! without dedicating a thread per book).

use std::path::Path;
use crate::error::Result;
use crate::signature::LayoutSignature;
use crate::spmc::RingConsumer;

/// Synchronous channel multiplexer across multiple `RingConsumer<T>` instances.
pub struct RingMultiplexer<T: Copy + LayoutSignature + 'static> {
    consumers: Vec<RingConsumer<T>>,
    names: Vec<String>,
    next_idx: usize,
}

impl<T: Copy + LayoutSignature + 'static> Default for RingMultiplexer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy + LayoutSignature + 'static> RingMultiplexer<T> {
    /// Creates a new empty `RingMultiplexer`.
    pub fn new() -> Self {
        Self {
            consumers: Vec::new(),
            names: Vec::new(),
            next_idx: 0,
        }
    }

    /// Adds an existing `RingConsumer` to the multiplexer. Returns its channel index.
    pub fn add(&mut self, consumer: RingConsumer<T>) -> usize {
        let idx = self.consumers.len();
        self.names.push(format!("channel_{}", idx));
        self.consumers.push(consumer);
        idx
    }

    /// Adds an existing `RingConsumer` with a human-readable channel name.
    pub fn add_named<S: Into<String>>(&mut self, name: S, consumer: RingConsumer<T>) -> usize {
        let idx = self.consumers.len();
        self.names.push(name.into());
        self.consumers.push(consumer);
        idx
    }

    /// Attaches to a ring buffer at `path` and adds it to the multiplexer.
    pub fn attach<P: AsRef<Path>>(&mut self, path: P) -> Result<usize> {
        let consumer = RingConsumer::attach(path)?;
        Ok(self.add(consumer))
    }

    /// Attaches to a ring buffer at `path` with a custom channel name.
    pub fn attach_named<P: AsRef<Path>, S: Into<String>>(&mut self, name: S, path: P) -> Result<usize> {
        let consumer = RingConsumer::attach(path)?;
        Ok(self.add_named(name, consumer))
    }

    /// Total number of multiplexed ring buffer channels.
    #[inline]
    pub fn len(&self) -> usize {
        self.consumers.len()
    }

    /// Returns `true` if no channels have been added yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    /// Name of the channel at index `idx`.
    #[inline]
    pub fn channel_name(&self, idx: usize) -> Option<&str> {
        self.names.get(idx).map(|s| s.as_str())
    }

    /// Reference to the underlying consumer at index `idx`.
    #[inline]
    pub fn consumer(&self, idx: usize) -> Option<&RingConsumer<T>> {
        self.consumers.get(idx)
    }

    /// Mutable reference to the underlying consumer at index `idx`.
    #[inline]
    pub fn consumer_mut(&mut self, idx: usize) -> Option<&mut RingConsumer<T>> {
        self.consumers.get_mut(idx)
    }

    /// Polls all channels using fair Round-Robin scheduling.
    ///
    /// Returns `Some((channel_index, message))` from the next available channel,
    /// or `None` if all channels are currently empty.
    #[inline]
    pub fn try_recv_any(&mut self) -> Option<(usize, T)> {
        let n = self.consumers.len();
        if n == 0 {
            return None;
        }

        let start = self.next_idx;
        for i in 0..n {
            let idx = (start + i) % n;
            if let Some(msg) = self.consumers[idx].try_recv() {
                self.next_idx = (idx + 1) % n;
                return Some((idx, msg));
            }
        }

        None
    }

    /// Polls channels strictly in priority order (0, 1, 2, ..., N-1).
    ///
    /// Favors higher-priority channels (e.g. primary liquidity, risk cancels).
    #[inline]
    pub fn try_recv_priority(&mut self) -> Option<(usize, T)> {
        for (idx, consumer) in self.consumers.iter_mut().enumerate() {
            if let Some(msg) = consumer.try_recv() {
                return Some((idx, msg));
            }
        }
        None
    }

    /// Drains up to `max_items` across all channels in a single batch.
    pub fn recv_batch_any(&mut self, max_items: usize) -> Vec<(usize, T)> {
        let mut batch = Vec::with_capacity(max_items.min(64));
        while batch.len() < max_items {
            if let Some((idx, msg)) = self.try_recv_any() {
                batch.push((idx, msg));
            } else {
                break;
            }
        }
        batch
    }
}

#[cfg(feature = "tokio")]
pub use async_impl::AsyncRingMultiplexer;

#[cfg(feature = "tokio")]
mod async_impl {
    use super::*;
    use crate::async_ring::AsyncRingConsumer;

    /// Asynchronous channel multiplexer for Tokio.
    pub struct AsyncRingMultiplexer<T: Copy + LayoutSignature + 'static> {
        consumers: Vec<AsyncRingConsumer<T>>,
        names: Vec<String>,
        next_idx: usize,
    }

    impl<T: Copy + LayoutSignature + 'static> Default for AsyncRingMultiplexer<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<T: Copy + LayoutSignature + 'static> AsyncRingMultiplexer<T> {
        pub fn new() -> Self {
            Self {
                consumers: Vec::new(),
                names: Vec::new(),
                next_idx: 0,
            }
        }

        pub fn add(&mut self, consumer: AsyncRingConsumer<T>) -> usize {
            let idx = self.consumers.len();
            self.names.push(format!("channel_{}", idx));
            self.consumers.push(consumer);
            idx
        }

        pub fn add_named<S: Into<String>>(&mut self, name: S, consumer: AsyncRingConsumer<T>) -> usize {
            let idx = self.consumers.len();
            self.names.push(name.into());
            self.consumers.push(consumer);
            idx
        }

        pub fn attach<P: AsRef<Path>>(&mut self, path: P) -> Result<usize> {
            let consumer = AsyncRingConsumer::attach(path)?;
            Ok(self.add(consumer))
        }

        pub fn attach_named<P: AsRef<Path>, S: Into<String>>(&mut self, name: S, path: P) -> Result<usize> {
            let consumer = AsyncRingConsumer::attach(path)?;
            Ok(self.add_named(name, consumer))
        }

        #[inline]
        pub fn len(&self) -> usize {
            self.consumers.len()
        }

        #[inline]
        pub fn is_empty(&self) -> bool {
            self.consumers.is_empty()
        }

        #[inline]
        pub fn channel_name(&self, idx: usize) -> Option<&str> {
            self.names.get(idx).map(|s| s.as_str())
        }

        #[inline]
        pub fn try_recv_any(&mut self) -> Option<(usize, T)> {
            let n = self.consumers.len();
            if n == 0 {
                return None;
            }

            let start = self.next_idx;
            for i in 0..n {
                let idx = (start + i) % n;
                if let Some(msg) = self.consumers[idx].try_recv() {
                    self.next_idx = (idx + 1) % n;
                    return Some((idx, msg));
                }
            }

            None
        }

        /// Asynchronously waits for a message on any multiplexed channel.
        ///
        /// Features adaptive fast-path spinning, cooperative `tokio::task::yield_now().await`,
        /// and gentle sleep backoff to eliminate worker thread starvation.
        pub async fn recv_any(&mut self) -> (usize, T) {
            let mut spin_count = 0u32;
            let mut yield_count = 0u32;

            loop {
                if let Some((idx, msg)) = self.try_recv_any() {
                    return (idx, msg);
                }

                if spin_count < 64 {
                    core::hint::spin_loop();
                    spin_count += 1;
                } else if yield_count < 16 {
                    tokio::task::yield_now().await;
                    yield_count += 1;
                } else {
                    tokio::time::sleep(std::time::Duration::from_micros(10)).await;
                    spin_count = 0;
                    yield_count = 0;
                }
            }
        }
    }
}
