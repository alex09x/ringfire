//! Runnable versions of the README quickstart snippets (kept in sync by CI building examples).
//!
//! `cargo run --example quickstart`

use ringfire::{
    BlackboardConsumer, BlackboardProducer, BlobConsumer, BlobProducerBuilder, BusySpin,
    ConsumerStartMode, FlowControl, RingConsumer, RingConsumerBuilder, RingMultiplexer,
    RingProducer, RingProducerBuilder, RingfireError,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MarketTicker {
    asset_id: u32,
    bid_px: u64,
    ask_px: u64,
    timestamp_ns: u64,
}

const TICKER: MarketTicker = MarketTicker {
    asset_id: 42,
    bid_px: 64_250_000_000,
    ask_px: 64_250_500_000,
    timestamp_ns: 1_726_870_000_000_000,
};

fn shm(name: &str) -> std::path::PathBuf {
    let dir = if cfg!(target_os = "linux") { "/dev/shm".into() } else { std::env::temp_dir() };
    dir.join(format!("ringfire_quickstart_{}_{}", name, std::process::id()))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1 + 3. Producer and synchronous consumer
    let path = shm("ticker");
    let mut producer = RingProducer::<MarketTicker>::create(&path, 65536)?;
    let mut consumer = RingConsumer::<MarketTicker>::attach(&path)?;
    producer.push(&TICKER);
    let mut wait = BusySpin::new();
    assert_eq!(consumer.recv_blocking(&mut wait), TICKER);

    // 4. Blackboard
    let bb_path = shm("state");
    let mut bb_prod = BlackboardProducer::<MarketTicker>::create(&bb_path, 1024)?;
    bb_prod.write(42, &TICKER)?;
    let bb_cons = BlackboardConsumer::<MarketTicker>::attach(&bb_path)?;
    if let Some(state) = bb_cons.read(42)? {
        println!("BBO for asset 42: bid={}, ask={}", state.bid_px, state.ask_px);
    }

    // 5. Raw bytes, decoded without assuming alignment
    let raw_path = shm("raw");
    let mut raw_prod = RingProducer::<[u8; 64]>::create(&raw_path, 1024)?;
    let mut raw_cons = RingConsumer::<[u8; 64]>::attach(&raw_path)?;
    let mut packet = [0u8; 64];
    unsafe { std::ptr::write_unaligned(packet.as_mut_ptr().cast::<MarketTicker>(), TICKER) };
    raw_prod.push(&packet);
    let bytes = raw_cons.try_recv().expect("published above");
    let ticker: MarketTicker = unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast()) };
    assert_eq!(ticker, TICKER);

    // 6. Variable-length payloads
    let blob_path = shm("book");
    let mut blob_prod = BlobProducerBuilder::new(65536, 16 * 1024 * 1024).build::<u32, _>(&blob_path)?;
    let raw_json = br#"{"event":"snapshot","bids":[[82000.5,1.2]],"asks":[[82001.0,0.8]]}"#;
    blob_prod.push(&42, raw_json)?;
    let mut blob_cons = BlobConsumer::<u32>::attach(&blob_path)?;
    let mut meta = 0u32;
    let mut scratch = vec![0u8; 4096];
    if let Some(len) = blob_cons.recv(&mut meta, &mut scratch)? {
        println!("snapshot for symbol {}: {} bytes", meta, len);
    }
    // Or inspect in place; the closure result is discarded if the arena laps during it.
    blob_prod.push(&43, b"in-place")?;
    let seen = blob_cons.view(|symbol, bytes| (*symbol, bytes.len()))?;
    assert_eq!(seen, Some((43, 8)));

    // 7. Start modes and SHM offset checkpoints
    let mut live = RingConsumerBuilder::<MarketTicker>::new()
        .start_mode(ConsumerStartMode::Latest)
        .attach(&path)?;
    assert_eq!(live.try_recv(), Some(TICKER));
    let offset_path = shm("ticker_worker1.offset");
    let persistent = RingConsumerBuilder::<MarketTicker>::new()
        .offset_file(&offset_path)
        .attach(&path)?;
    persistent.commit_offset()?;

    // 8. Lossless backpressure
    let reliable = shm("reliable");
    let mut lossless = RingProducerBuilder::new(4096)
        .flow_control(FlowControl::LosslessBackpressure)
        .build::<MarketTicker, _>(&reliable)?;
    let mut careful = RingConsumerBuilder::<MarketTicker>::new()
        .consumer_name("careful")
        .start_from_head()
        .attach(&reliable)?;
    lossless.push(&TICKER);
    match lossless.try_push(&TICKER) {
        Ok(()) => {}
        Err(RingfireError::BackpressureBufferFull) => println!("slow reader: backpressure applied"),
        Err(e) => return Err(e.into()),
    }
    assert_eq!(careful.try_recv(), Some(TICKER));

    // 9. Multiplexing
    let btc = shm("stream_btc");
    let eth = shm("stream_eth");
    let mut p_btc = RingProducer::<MarketTicker>::create(&btc, 1024)?;
    let _p_eth = RingProducer::<MarketTicker>::create(&eth, 1024)?;
    let mut mux = RingMultiplexer::new();
    mux.add(RingConsumer::<MarketTicker>::attach(&btc)?);
    mux.add(RingConsumer::<MarketTicker>::attach(&eth)?);
    p_btc.push(&TICKER);
    if let Some((channel_idx, ticker)) = mux.try_recv_any() {
        println!("channel {} received ticker {}", channel_idx, ticker.asset_id);
    }

    for p in [careful.offset_path().map(|p| p.to_path_buf()), Some(offset_path)].into_iter().flatten() {
        let _ = std::fs::remove_file(p);
    }
    println!("quickstart OK");
    Ok(())
}
