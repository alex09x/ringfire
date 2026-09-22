use ringfire::{RingMultiplexer, RingProducer};
#[cfg(feature = "tokio")]
use ringfire::AsyncRingMultiplexer;
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct OrderEvent {
    order_id: u64,
    symbol_id: u32,
    price: u64,
}

#[test]
fn test_multiplexer_round_robin_fairness() {
    let dir = std::env::temp_dir();
    let path_btc = dir.join(format!("test_mux_btc_{}.shm", std::process::id()));
    let path_eth = dir.join(format!("test_mux_eth_{}.shm", std::process::id()));
    let path_sol = dir.join(format!("test_mux_sol_{}.shm", std::process::id()));

    let _ = std::fs::remove_file(&path_btc);
    let _ = std::fs::remove_file(&path_eth);
    let _ = std::fs::remove_file(&path_sol);

    let mut prod_btc = RingProducer::<OrderEvent>::create(&path_btc, 64).unwrap();
    let mut prod_eth = RingProducer::<OrderEvent>::create(&path_eth, 64).unwrap();
    let mut prod_sol = RingProducer::<OrderEvent>::create(&path_sol, 64).unwrap();

    let mut mux = RingMultiplexer::<OrderEvent>::new();
    let idx_btc = mux.attach_named("BTC", &path_btc).unwrap();
    let idx_eth = mux.attach_named("ETH", &path_eth).unwrap();
    let idx_sol = mux.attach_named("SOL", &path_sol).unwrap();

    assert_eq!(mux.len(), 3);
    assert_eq!(mux.channel_name(idx_btc), Some("BTC"));
    assert_eq!(mux.channel_name(idx_eth), Some("ETH"));
    assert_eq!(mux.channel_name(idx_sol), Some("SOL"));

    // Push items into each ring
    prod_btc.push(&OrderEvent { order_id: 1, symbol_id: 1, price: 65000 });
    prod_btc.push(&OrderEvent { order_id: 2, symbol_id: 1, price: 65010 });

    prod_eth.push(&OrderEvent { order_id: 10, symbol_id: 2, price: 3500 });
    prod_eth.push(&OrderEvent { order_id: 20, symbol_id: 2, price: 3510 });

    prod_sol.push(&OrderEvent { order_id: 100, symbol_id: 3, price: 150 });

    // Round-robin polling must alternate fairly across channels
    let (ch1, msg1) = mux.try_recv_any().unwrap();
    assert_eq!(ch1, idx_btc);
    assert_eq!(msg1.order_id, 1);

    let (ch2, msg2) = mux.try_recv_any().unwrap();
    assert_eq!(ch2, idx_eth);
    assert_eq!(msg2.order_id, 10);

    let (ch3, msg3) = mux.try_recv_any().unwrap();
    assert_eq!(ch3, idx_sol);
    assert_eq!(msg3.order_id, 100);

    // Loops back to BTC
    let (ch4, msg4) = mux.try_recv_any().unwrap();
    assert_eq!(ch4, idx_btc);
    assert_eq!(msg4.order_id, 2);

    // Then ETH
    let (ch5, msg5) = mux.try_recv_any().unwrap();
    assert_eq!(ch5, idx_eth);
    assert_eq!(msg5.order_id, 20);

    // All drained
    assert_eq!(mux.try_recv_any(), None);

    // Test batch drain across multiple channels
    prod_btc.push(&OrderEvent { order_id: 3, symbol_id: 1, price: 65020 });
    prod_sol.push(&OrderEvent { order_id: 101, symbol_id: 3, price: 151 });
    let batch = mux.recv_batch_any(10);
    assert_eq!(batch.len(), 2);

    let _ = std::fs::remove_file(&path_btc);
    let _ = std::fs::remove_file(&path_eth);
    let _ = std::fs::remove_file(&path_sol);
}

#[test]
fn test_multiplexer_priority_ordering() {
    let dir = std::env::temp_dir();
    let p_high = dir.join(format!("test_mux_high_{}.shm", std::process::id()));
    let p_low = dir.join(format!("test_mux_low_{}.shm", std::process::id()));

    let _ = std::fs::remove_file(&p_high);
    let _ = std::fs::remove_file(&p_low);

    let mut prod_high = RingProducer::<u64>::create(&p_high, 64).unwrap();
    let mut prod_low = RingProducer::<u64>::create(&p_low, 64).unwrap();

    let mut mux = RingMultiplexer::<u64>::new();
    let idx_high = mux.attach_named("HIGH_PRIO", &p_high).unwrap();
    let idx_low = mux.attach_named("LOW_PRIO", &p_low).unwrap();

    // Push to both channels
    for i in 1..=5 {
        prod_low.push(&i);
    }
    for i in 101..=103 {
        prod_high.push(&i);
    }

    // Priority polling should completely drain high-priority channel before low
    for expected in 101..=103 {
        let (ch, msg) = mux.try_recv_priority().unwrap();
        assert_eq!(ch, idx_high);
        assert_eq!(msg, expected);
    }

    // Once high is empty, low is serviced
    let (ch, msg) = mux.try_recv_priority().unwrap();
    assert_eq!(ch, idx_low);
    assert_eq!(msg, 1);

    let _ = std::fs::remove_file(&p_high);
    let _ = std::fs::remove_file(&p_low);
}

#[tokio::test]
async fn test_async_multiplexer_tokio() {
    let dir = std::env::temp_dir();
    let p1 = dir.join(format!("test_async_mux1_{}.shm", std::process::id()));
    let p2 = dir.join(format!("test_async_mux2_{}.shm", std::process::id()));

    let _ = std::fs::remove_file(&p1);
    let _ = std::fs::remove_file(&p2);

    let mut prod1 = RingProducer::<u64>::create(&p1, 1024).unwrap();
    let mut prod2 = RingProducer::<u64>::create(&p2, 1024).unwrap();

    let mut mux = AsyncRingMultiplexer::<u64>::new();
    mux.attach_named("STREAM_1", &p1).unwrap();
    mux.attach_named("STREAM_2", &p2).unwrap();

    // Spawn producer background thread
    let p_handle = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(10));
        for i in 1..=50 {
            prod1.push(&i);
            prod2.push(&(i * 100));
        }
    });

    let mut received_count = 0;
    let mut sum_ch1 = 0u64;
    let mut sum_ch2 = 0u64;

    while received_count < 100 {
        let (ch, val) = mux.recv_any().await;
        if ch == 0 {
            sum_ch1 += val;
        } else {
            sum_ch2 += val;
        }
        received_count += 1;
    }

    p_handle.join().unwrap();

    assert_eq!(received_count, 100);
    assert_eq!(sum_ch1, (1..=50).sum::<u64>());
    assert_eq!(sum_ch2, (1..=50).map(|x| x * 100).sum::<u64>());

    let _ = std::fs::remove_file(&p1);
    let _ = std::fs::remove_file(&p2);
}
