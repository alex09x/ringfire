use ringfire::{RingConsumer, RingProducer};
use std::env;
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct StressRecord {
    pub seq: u64,
    pub payload: [u64; 7],
}

#[test]
fn test_multiprocess_1_writer_8_readers() {
    let args: Vec<String> = env::args().collect();

    // Check if running in child worker mode
    if let Some(pos) = args.iter().position(|a| a == "--worker-consumer") {
        let shm_path = &args[pos + 1];
        let total_msgs: u64 = args[pos + 2].parse().unwrap();

        let mut consumer = RingConsumer::<StressRecord>::attach(shm_path)
            .expect("Failed to attach consumer in child process");

        let mut received = 0u64;
        let mut expected_seq = 1u64;

        while received < total_msgs {
            if let Some(rec) = consumer.try_recv() {
                // In non-lossy high-capacity test, sequences must match
                assert_eq!(rec.seq, expected_seq, "Worker observed seq gap");
                assert_eq!(rec.payload[0], rec.seq * 11);
                expected_seq += 1;
                received += 1;
            } else {
                core::hint::spin_loop();
            }
        }
        return;
    }

    // Master test process
    let tmp_path = std::env::temp_dir().join("test_multiprocess_stress.shm");
    let _ = std::fs::remove_file(&tmp_path);

    let capacity = 131072; // 128k slots buffer
    let mut producer = RingProducer::<StressRecord>::create(&tmp_path, capacity)
        .expect("Failed to create producer");

    let num_readers = 8;
    let total_msgs = 100_000u64;

    let current_exe = env::current_exe().expect("Failed to get current_exe");

    // Spawn 8 reader processes
    let mut child_processes = Vec::new();
    for _ in 0..num_readers {
        let child = Command::new(&current_exe)
            .arg("--")
            .arg("--worker-consumer")
            .arg(&tmp_path)
            .arg(total_msgs.to_string())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("Failed to spawn child reader process");
        child_processes.push(child);
    }

    // Give children a few milliseconds to attach
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Producer publishes total_msgs
    for s in 1..=total_msgs {
        let rec = StressRecord {
            seq: s,
            payload: [s * 11, s * 13, s * 17, s * 19, s * 23, s * 29, s * 31],
        };
        producer.push(&rec);
    }

    // Wait for all 8 reader processes to finish
    for (i, mut child) in child_processes.into_iter().enumerate() {
        let status = child.wait().expect("Failed to wait on child process");
        assert!(status.success(), "Child reader process {} failed", i);
    }

    println!(
        "Successfully verified 1 writer + {} reader processes with {} messages",
        num_readers, total_msgs
    );
}
