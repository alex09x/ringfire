#![cfg(feature = "tokio")]
use futures_core::Stream;
use ringfire::*;
use std::future::{Future, poll_fn};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "ringfire-async-{}-{}",
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

#[tokio::test]
async fn async_receive_backoff_and_status_preserve_values() {
    let d = Dir::new();
    let path = d.path("ring");
    let mut p = RingProducer::<u64>::create(&path, 4).unwrap();
    let mut c = AsyncRingConsumer::<u64>::attach(&path)
        .unwrap()
        .with_spins(2)
        .with_yields(1)
        .with_idle_sleep(Duration::from_millis(1));
    assert_eq!(
        (c.capacity(), c.cursor(), c.lag(), c.lapped_count()),
        (4, 1, 0, 0)
    );
    assert!(matches!(c.recv_status(), RecvStatus::Empty));
    let mut cx = Context::from_waker(Waker::noop());
    let mut recv = Box::pin(c.recv());
    assert!(recv.as_mut().poll(&mut cx).is_pending()); // yield reached after spins
    assert!(recv.as_mut().poll(&mut cx).is_pending()); // idle timer reached
    p.push(&41);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), recv)
            .await
            .unwrap(),
        41
    );
    let mut recv = Box::pin(c.recv_detailed());
    assert!(recv.as_mut().poll(&mut cx).is_pending());
    assert!(recv.as_mut().poll(&mut cx).is_pending());
    p.push(&42);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), recv)
            .await
            .unwrap(),
        (42, 0)
    );
    let mut buf = [0; 4];
    let mut recv = Box::pin(c.recv_batch(&mut buf));
    assert!(recv.as_mut().poll(&mut cx).is_pending());
    assert!(recv.as_mut().poll(&mut cx).is_pending());
    p.push_batch(&[43, 44]);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), recv)
            .await
            .unwrap(),
        2
    );
    assert_eq!(&buf[..2], &[43, 44]);
    assert_eq!(c.recv_batch(&mut []).await, 0);
    p.push_batch(&[45, 46, 47, 48, 49, 50]);
    assert_eq!(c.recv_detailed().await, (47, 2));
    assert_eq!(c.lapped_count(), 2);
    assert_eq!(c.jump_to_latest(), 2);
    assert_eq!(c.try_recv(), Some(50));
    p.push_batch(&[51, 52, 53, 54, 55, 56]);
    assert_eq!(c.jump_to_oldest(), 2);
    assert_eq!(c.cursor(), 13);
    assert_eq!(c.lag(), 4);
    assert_eq!(c.recv().await, 53);
    assert_eq!(c.lapped_count(), 6);
    assert!(AsyncRingConsumer::<u64>::attach(d.path("missing")).is_err());
}

#[tokio::test]
async fn stream_wakes_from_idle_timer_and_returns_to_fast_path() {
    let d = Dir::new();
    let path = d.path("ring");
    let mut p = RingProducer::<u64>::create(&path, 4).unwrap();
    let mut c = AsyncRingConsumer::<u64>::attach(&path)
        .unwrap()
        .with_spins(1)
        .with_yields(1)
        .with_idle_sleep(Duration::from_millis(1));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut c).poll_next(&mut cx).is_pending());
    assert!(Pin::new(&mut c).poll_next(&mut cx).is_pending());
    p.push(&7);
    let value = tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| Pin::new(&mut c).poll_next(cx)),
    )
    .await
    .unwrap();
    assert_eq!(value, Some(7));
    p.push(&8);
    assert_eq!(Pin::new(&mut c).poll_next(&mut cx), Poll::Ready(Some(8)));
}

#[tokio::test]
async fn async_multiplexer_empty_backoff_names_and_round_robin() {
    let d = Dir::new();
    let a = d.path("a");
    let b = d.path("b");
    let mut pa = RingProducer::<u64>::create(&a, 8).unwrap();
    let mut pb = RingProducer::<u64>::create(&b, 8).unwrap();
    let mut mux = AsyncRingMultiplexer::<u64>::default();
    assert!(mux.is_empty());
    assert_eq!(mux.try_recv_any(), None);
    assert_eq!(mux.attach(&a).unwrap(), 0);
    assert_eq!(mux.attach_named("b", &b).unwrap(), 1);
    assert_eq!(mux.len(), 2);
    assert!(!mux.is_empty());
    assert_eq!(mux.channel_name(0), Some("channel_0"));
    assert_eq!(mux.channel_name(1), Some("b"));
    assert_eq!(mux.channel_name(2), None);
    assert!(mux.attach(d.path("missing")).is_err());
    assert!(mux.attach_named("bad", d.path("missing")).is_err());
    let mut future = Box::pin(mux.recv_any());
    let mut cx = Context::from_waker(Waker::noop());
    // Drive through all 16 cooperative yields and into the idle timer before publishing.
    for _ in 0..17 {
        assert!(future.as_mut().poll(&mut cx).is_pending());
    }
    pb.push(&20);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), future)
            .await
            .unwrap(),
        (1, 20)
    );
    pa.push_batch(&[10, 11]);
    pb.push(&21);
    assert_eq!(mux.recv_any().await, (0, 10));
    assert_eq!(mux.try_recv_any(), Some((1, 21)));
    assert_eq!(mux.try_recv_any(), Some((0, 11)));
    assert_eq!(mux.try_recv_any(), None);
}
