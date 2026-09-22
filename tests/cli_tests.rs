use std::process::Command;
use ringfire::{BlackboardProducer, RingConsumer, RingProducer};

#[test]
fn test_cli_stat_and_dump_spmc() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_cli_spmc.shm");
    let _ = std::fs::remove_file(&tmp_path);

    // Create a ring buffer and write some messages
    let mut producer = RingProducer::<u64>::create(&tmp_path, 1024).unwrap();
    let mut consumer = RingConsumer::<u64>::attach(&tmp_path).unwrap();

    for i in 1..=50 {
        producer.push(&i);
    }
    // Read 20 items so consumer has cursor at 21, lag 30
    for _ in 0..20 {
        let _ = consumer.try_recv();
    }

    let bin_path = env!("CARGO_BIN_EXE_ringfire");

    // 1. Test `stat` (human readable)
    let output = Command::new(bin_path)
        .arg("stat")
        .arg(&tmp_path)
        .output()
        .expect("failed to run ringfire stat");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ringfire Shared Memory Ring Buffer Status"));
    assert!(stdout.contains("Capacity (Slots):    1024"));
    assert!(stdout.contains("Write Sequence:    50"));
    assert!(stdout.contains("Registered Readers"));

    // 2. Test `stat --json`
    let output_json = Command::new(bin_path)
        .arg("stat")
        .arg(&tmp_path)
        .arg("--json")
        .output()
        .expect("failed to run ringfire stat --json");
    assert!(output_json.status.success());
    let stdout_json = String::from_utf8_lossy(&output_json.stdout);
    assert!(stdout_json.contains("\"capacity\": 1024"));
    assert!(stdout_json.contains("\"write_seq\": 50"));
    assert!(stdout_json.contains("\"readers\": ["));

    // 3. Test `dump`
    let output_dump = Command::new(bin_path)
        .arg("dump")
        .arg(&tmp_path)
        .arg("--tail")
        .arg("5")
        .output()
        .expect("failed to run ringfire dump");
    assert!(output_dump.status.success());
    let stdout_dump = String::from_utf8_lossy(&output_dump.stdout);
    assert!(stdout_dump.contains("Dumping slots from seq 46 to seq 50"));

    // 4. Test `dump --hex`
    let output_hex = Command::new(bin_path)
        .arg("dump")
        .arg(&tmp_path)
        .arg("--tail")
        .arg("3")
        .arg("--hex")
        .output()
        .expect("failed to run ringfire dump --hex");
    assert!(output_hex.status.success());
    let stdout_hex = String::from_utf8_lossy(&output_hex.stdout);
    assert!(stdout_hex.contains("hex: ["));

    // 5. Test `prune`
    let output_prune = Command::new(bin_path)
        .arg("prune")
        .arg(&tmp_path)
        .output()
        .expect("failed to run ringfire prune");
    assert!(output_prune.status.success());

    let _ = std::fs::remove_file(&tmp_path);
}

#[test]
fn test_cli_stat_blackboard() {
    let tmp_path = std::env::temp_dir().join("test_ringfire_cli_bb.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let _bb = BlackboardProducer::<[u8; 32]>::create(&tmp_path, 128).unwrap();
    let bin_path = env!("CARGO_BIN_EXE_ringfire");

    // Human readable
    let output = Command::new(bin_path)
        .arg("stat")
        .arg(&tmp_path)
        .output()
        .expect("failed to run ringfire stat");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Blackboard"));
    assert!(stdout.contains("Slots Count:  128"));

    // JSON
    let output_json = Command::new(bin_path)
        .arg("stat")
        .arg(&tmp_path)
        .arg("--json")
        .output()
        .expect("failed to run ringfire stat --json");
    assert!(output_json.status.success());
    let stdout_json = String::from_utf8_lossy(&output_json.stdout);
    assert!(stdout_json.contains("\"type\": \"Blackboard\""));
    assert!(stdout_json.contains("\"slot_count\": 128"));

    let _ = std::fs::remove_file(&tmp_path);
}
