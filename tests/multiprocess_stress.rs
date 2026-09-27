use ringfire::{RingConsumer, RingProducer};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct StressRecord {
    seq: u64,
    payload: [u64; 7],
}

// Kill and reap children on assertion failure too: a stalled reader must not outlive the test.
struct Readers(Vec<Child>);
impl Drop for Readers {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn test_multiprocess_1_writer_8_readers() {
    const TOTAL: u64 = 100_000;
    const READERS: usize = 8;
    const LIMIT: Duration = Duration::from_secs(30);
    if let Some(dir) = std::env::var_os("RINGFIRE_STRESS_CHILD_DIR") {
        let dir = PathBuf::from(dir);
        let id: usize = std::env::var("RINGFIRE_STRESS_CHILD_ID")
            .unwrap()
            .parse()
            .unwrap();
        let mut consumer = RingConsumer::<StressRecord>::attach(dir.join("ring.shm")).unwrap();
        // This file is created only after attach succeeds, proving the child test executed.
        std::fs::write(dir.join(format!("ready-{id}")), b"attached").unwrap();
        let deadline = Instant::now() + LIMIT;
        let mut received = 0;
        while received < TOTAL {
            if let Some(record) = consumer.try_recv() {
                received += 1;
                assert_eq!(record.seq, received);
                assert_eq!(
                    record.payload,
                    [
                        received * 11,
                        received * 13,
                        received * 17,
                        received * 19,
                        received * 23,
                        received * 29,
                        received * 31
                    ]
                );
            } else {
                assert!(
                    Instant::now() < deadline,
                    "reader {id} stopped at {received}/{TOTAL}"
                );
                std::thread::yield_now();
            }
        }
        assert_eq!(consumer.lapped_count(), 0);
        std::fs::write(dir.join(format!("done-{id}")), received.to_string()).unwrap();
        return;
    }

    let dir = Directory(
        std::env::temp_dir().join(format!("ringfire-multiprocess-{}", std::process::id())),
    );
    std::fs::create_dir(&dir.0).unwrap();
    let mut producer =
        RingProducer::<StressRecord>::create(dir.0.join("ring.shm"), 131_072).unwrap();
    let mut children = Readers(Vec::new());
    for id in 0..READERS {
        children.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "test_multiprocess_1_writer_8_readers",
                    "--nocapture",
                ])
                .env("RINGFIRE_STRESS_CHILD_DIR", &dir.0)
                .env("RINGFIRE_STRESS_CHILD_ID", id.to_string())
                .spawn()
                .unwrap(),
        );
    }
    let deadline = Instant::now() + LIMIT;
    while !(0..READERS).all(|id| dir.0.join(format!("ready-{id}")).exists()) {
        for (id, child) in children.0.iter_mut().enumerate() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "reader {id} exited before readiness"
            );
        }
        assert!(Instant::now() < deadline, "readers did not attach");
        std::thread::sleep(Duration::from_millis(1));
    }
    for seq in 1..=TOTAL {
        producer.push(&StressRecord {
            seq,
            payload: [
                seq * 11,
                seq * 13,
                seq * 17,
                seq * 19,
                seq * 23,
                seq * 29,
                seq * 31,
            ],
        });
    }
    for (id, child) in children.0.iter_mut().enumerate() {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success(), "reader {id} failed: {status}");
                break;
            }
            assert!(Instant::now() < deadline, "reader {id} did not finish");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            std::fs::read_to_string(dir.0.join(format!("done-{id}"))).unwrap(),
            TOTAL.to_string()
        );
    }
    println!("Verified {READERS} reader processes, each receiving {TOTAL} complete records");
}
