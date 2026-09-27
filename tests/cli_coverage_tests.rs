use std::fs::OpenOptions;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use memmap2::MmapMut;
use ringfire::blob::BlobProducer;
use ringfire::header::{
    ReaderSlot, RingHeader, SLOT_WRITING, validate_ring,
};
use ringfire::mpmc::MpmcProducer;
use ringfire::spmc::{FlowControl, RingConsumer, RingProducer, RingProducerBuilder};

fn temp_file(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("rf_cli_cov_{}_{}.shm", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_ringfire")
}

fn run_cli(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(bin_path())
        .args(args)
        .output()
        .expect("failed to execute ringfire CLI");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn test_cli_help_version_and_unknown() {
    let (ok, _, err) = run_cli(&[]);
    assert!(!ok);
    assert!(err.contains("USAGE:"));

    for h in &["-h", "--help", "help"] {
        let (ok, _, err) = run_cli(&[h]);
        assert!(ok);
        assert!(err.contains("USAGE:"));
    }

    for v in &["-V", "--version", "version"] {
        let (ok, out, _) = run_cli(&[v]);
        assert!(ok);
        assert!(out.contains("ringfire "));
    }

    let (ok, _, err) = run_cli(&["unknown_subcmd"]);
    assert!(!ok);
    assert!(err.contains("Unknown subcommand: 'unknown_subcmd'"));
}

#[test]
fn test_cli_stat_arg_and_file_errors() {
    let (ok, _, err) = run_cli(&["stat"]);
    assert!(!ok);
    assert!(err.contains("requires a path"));

    let (ok, _, err) = run_cli(&["stat", "/nonexistent_path_xyz.shm"]);
    assert!(!ok);
    assert!(err.contains("Error:"));

    let p = temp_file("short");
    std::fs::write(&p, [0u8; 16]).unwrap();
    let (ok, _, err) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(!ok);
    assert!(err.contains("File too small"));
    let _ = std::fs::remove_file(&p);

    let p = temp_file("bad_magic");
    std::fs::write(&p, [0x55u8; 256]).unwrap();
    let (ok, _, err) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(!ok);
    assert!(err.contains("Invalid magic signature"));
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_stat_spmc_and_lossless() {
    let p = temp_file("spmc_empty");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let (ok, out, _) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("SPMC Broadcast"));
    assert!(out.contains("(empty: nothing published yet)"));

    let (ok, out_json, _) = run_cli(&["stat", p.to_str().unwrap(), "--json"]);
    assert!(ok);
    assert!(out_json.contains("\"mode\": \"SPMC\""));
    assert!(out_json.contains("\"oldest_seq\": 0"));
    assert!(out_json.contains("\"readers\": ["));
    let _ = std::fs::remove_file(&p);

    let p = temp_file("spmc_lossless");
    let _prod = RingProducerBuilder::new(64)
        .flow_control(FlowControl::LosslessBackpressure)
        .build::<u64, _>(&p)
        .unwrap();
    let (ok, out, _) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("LosslessBackpressure"));

    let (ok, out_json, _) = run_cli(&["stat", p.to_str().unwrap(), "--json"]);
    assert!(ok);
    assert!(out_json.contains("\"flow_control\": \"LosslessBackpressure\""));
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_stat_mpmc_arena_and_readers() {
    let p = temp_file("mpmc_stat");
    let prod = MpmcProducer::<u64>::create(&p, 64).unwrap();
    prod.push(&123u64);
    let (ok, out, _) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("MPMC Queue"));
    assert!(out.contains("Claim Sequence:"));
    let (ok, out_json, _) = run_cli(&["stat", p.to_str().unwrap(), "--json"]);
    assert!(ok);
    assert!(out_json.contains("\"mode\": \"MPMC\""));
    let _ = std::fs::remove_file(&p);

    let p = temp_file("arena_stat");
    let mut prod = BlobProducer::<()>::create(&p, 64, 1024 * 1024).unwrap();
    prod.push(&(), b"hello world payload").unwrap();
    let (ok, out, _) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("Variable-Length Payload Arena:"));
    assert!(out.contains("Capacity:"));
    assert!(out.contains("Reserved Bytes:"));
    let (ok, out_json, _) = run_cli(&["stat", p.to_str().unwrap(), "--json"]);
    assert!(ok);
    assert!(out_json.contains("\"has_arena\": true"));
    assert!(out_json.contains("\"arena_capacity\":"));
    let _ = std::fs::remove_file(&p);

    let p = temp_file("readers_stat");
    let mut prod = RingProducerBuilder::new(64)
        .max_readers(4)
        .build::<u64, _>(&p)
        .unwrap();
    prod.push(&100u64);
    let mut cons = RingConsumer::<u64>::builder()
        .consumer_name("alive_reader\n\"\\test")
        .attach(&p)
        .unwrap();
    let _ = cons.try_recv();

    let file = OpenOptions::new().read(true).write(true).open(&p).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    let base_ptr =
        unsafe { mmap.as_mut_ptr().add(header.reader_registry_offset as usize) as *mut ReaderSlot };
    unsafe {
        let slot1 = &mut *base_ptr.add(1);
        slot1.active.store(1, Ordering::SeqCst);
        slot1.pid.store(99999999, Ordering::SeqCst);
        slot1.cursor_seq.store(50, Ordering::SeqCst);
        let name = b"dead_reader\0";
        slot1.name[..name.len()].copy_from_slice(name);
    };

    let (ok, out, _) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("Registered Readers (2 active"));
    assert!(out.contains("ALIVE"));
    assert!(out.contains("DEAD"));

    let (ok, out_json, _) = run_cli(&["stat", p.to_str().unwrap(), "--json"]);
    assert!(ok);
    assert!(out_json.contains("\"readers\": ["));
    assert!(out_json.contains("\"alive\": true"));
    assert!(out_json.contains("\"alive\": false"));
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_dump_options() {
    let (ok, _, err) = run_cli(&["dump"]);
    assert!(!ok);
    assert!(err.contains("requires a path"));

    let (ok, _, err) = run_cli(&["dump", "/nonexistent.shm"]);
    assert!(!ok);
    assert!(err.contains("Error:"));

    let p = temp_file("dump_empty");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let (ok, out, _) = run_cli(&["dump", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("Ring is empty: nothing published yet"));
    let _ = std::fs::remove_file(&p);

    let p = temp_file("dump_msgs");
    let mut prod = RingProducer::<[u8; 32]>::create(&p, 64).unwrap();
    for i in 1..=20u8 {
        let mut buf = [0u8; 32];
        buf[0] = b'A' + (i % 26);
        buf[1] = b'B';
        prod.push(&buf);
    }
    let (ok, out, _) = run_cli(&["dump", p.to_str().unwrap(), "--tail", "5"]);
    assert!(ok);
    assert!(out.contains("Dumping slots from seq 16 to seq 20"));

    let (ok, out_hex, _) = run_cli(&["dump", p.to_str().unwrap(), "--tail", "3", "--hex"]);
    assert!(ok);
    assert!(out_hex.contains("hex: ["));

    let file = OpenOptions::new().read(true).write(true).open(&p).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let view = unsafe { validate_ring(mmap.as_ptr(), mmap.len(), None, 8).unwrap() };
    let slot_byte_offset = view.slots_offset + (20 & view.mask) as usize * view.slot_size;
    unsafe {
        (*(mmap.as_mut_ptr().add(slot_byte_offset) as *mut AtomicU64))
            .store(SLOT_WRITING, Ordering::SeqCst)
    };
    let (ok, out_wr, _) = run_cli(&["dump", p.to_str().unwrap(), "--tail", "1"]);
    assert!(ok);
    assert!(out_wr.contains("WRITING"));
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_prune_options() {
    let (ok, _, err) = run_cli(&["prune"]);
    assert!(!ok);
    assert!(err.contains("requires a path"));

    let (ok, _, err) = run_cli(&["prune", "/nonexistent.shm"]);
    assert!(!ok);
    assert!(err.contains("Error:"));

    let p = temp_file("prune_no_reg");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let (ok, out, _) = run_cli(&["prune", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("No ReaderRegistry configured"));
    let _ = std::fs::remove_file(&p);

    let p = temp_file("prune_reg");
    let _prod = RingProducerBuilder::new(64)
        .max_readers(4)
        .build::<u64, _>(&p)
        .unwrap();
    let mut cons = RingConsumer::<u64>::builder()
        .consumer_name("alive_prune")
        .attach(&p)
        .unwrap();
    let _ = cons.try_recv();

    let file = OpenOptions::new().read(true).write(true).open(&p).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    let base_ptr =
        unsafe { mmap.as_mut_ptr().add(header.reader_registry_offset as usize) as *mut ReaderSlot };
    unsafe {
        let slot1 = &mut *base_ptr.add(1);
        slot1.active.store(1, Ordering::SeqCst);
        slot1.pid.store(99999999, Ordering::SeqCst);
        let name = b"dead_reader\0";
        slot1.name[..name.len()].copy_from_slice(name);
    };

    let (ok, out, _) = run_cli(&["prune", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("Pruning dead reader slot 1: PID 99999999"));
    assert!(out.contains("Successfully pruned 1 dead reader slot(s)."));

    let (ok, out2, _) = run_cli(&["prune", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out2.contains("Successfully pruned 0 dead reader slot(s)."));
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_top_options_and_signals() {
    let (ok, _, err) = run_cli(&["top"]);
    assert!(!ok);
    assert!(err.contains("requires a path"));

    let (ok, _, err) = run_cli(&["top", "/nonexistent.shm"]);
    assert!(!ok);
    assert!(err.contains("Error:"));

    let p = temp_file("top_empty");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let (ok, out, _) = run_cli(&[
        "top",
        p.to_str().unwrap(),
        "--interval-ms",
        "10",
        "--iterations",
        "1",
    ]);
    assert!(ok);
    assert!(out.contains("ringfire top"));
    assert!(out.contains("(No active readers registered)"));
    let _ = std::fs::remove_file(&p);

    let p = temp_file("top_active");
    let mut prod = RingProducerBuilder::new(64)
        .max_readers(2)
        .flow_control(FlowControl::LosslessBackpressure)
        .build::<u64, _>(&p)
        .unwrap();
    prod.push(&42u64);
    let mut cons = RingConsumer::<u64>::builder()
        .consumer_name("top_reader")
        .attach(&p)
        .unwrap();
    let _ = cons.try_recv();
    let (ok, out, _) = run_cli(&[
        "top",
        p.to_str().unwrap(),
        "--interval-ms",
        "10",
        "--iterations",
        "1",
    ]);
    assert!(ok);
    assert!(out.contains("Backpressure"));
    assert!(out.contains("top_reader"));
    drop(cons);
    let _ = std::fs::remove_file(&p);

    let p = temp_file("top_sig");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let mut child = Command::new(bin_path())
        .args(&["top", p.to_str().unwrap(), "--interval-ms", "10"])
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    let status = child.wait().unwrap();
    assert!(status.success());
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_serve_and_mirror_errors() {
    let (ok, _, err) = run_cli(&["serve"]);
    assert!(!ok);
    assert!(err.contains("requires a path"));

    let (ok, _, err) = run_cli(&["serve", "/some/path"]);
    assert!(!ok);
    assert!(err.contains("requires --bind"));

    let (ok, _, err) = run_cli(&[
        "serve",
        "/some/path",
        "--bind",
        "127.0.0.1:0",
        "--multicast",
        "127.0.0.1:5000",
    ]);
    assert!(!ok);
    assert!(err.contains("multicast expects <GROUP:PORT>"));

    let (ok, _, err) = run_cli(&["serve", "/nonexistent.shm", "--bind", "127.0.0.1:0"]);
    assert!(!ok);
    assert!(err.contains("Error:"));

    let (ok, _, err) = run_cli(&["mirror"]);
    assert!(!ok);
    assert!(err.contains("requires <SOURCE_ADDR>"));

    let (ok, _, err) = run_cli(&["mirror", "127.0.0.1:7400"]);
    assert!(!ok);
    assert!(err.contains("requires <SOURCE_ADDR>"));

    let (ok, _, err) = run_cli(&["mirror", "127.0.0.1:7400", "/some/dst", "--from", "invalid"]);
    assert!(!ok);
    assert!(err.contains("--from expects"));

    let (ok, _, _) = run_cli(&["mirror", "127.0.0.1:1", "/some/dst", "--once"]);
    assert!(!ok);
}

#[test]
fn test_cli_serve_signal_termination() {
    let p = temp_file("serve_sig");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let mut child = Command::new(bin_path())
        .args(&["serve", p.to_str().unwrap(), "--bind", "127.0.0.1:0"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    let status = child.wait().unwrap();
    assert!(status.success());
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_serve_and_mirror_full_replication() {
    let src = temp_file("repl_src");
    let dst = temp_file("repl_dst");
    let mut prod = RingProducer::<u64>::create(&src, 128).unwrap();
    for i in 1..=50u64 {
        prod.push(&i);
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mut s_child = Command::new(bin_path())
        .args(&[
            "serve",
            src.to_str().unwrap(),
            "--bind",
            &addr.to_string(),
            "--batch",
            "64",
            "--spin",
            "--linger-us",
            "10",
        ])
        .spawn()
        .unwrap();

    std::thread::sleep(Duration::from_millis(50));

    let mut m_child = Command::new(bin_path())
        .args(&[
            "mirror",
            &addr.to_string(),
            dst.to_str().unwrap(),
            "--from",
            "oldest",
            "--once",
            "--spin",
            "--unicast",
        ])
        .spawn()
        .unwrap();

    // Wait until mirror replicates all 50 records into dst
    let start = std::time::Instant::now();
    loop {
        if let Ok(mut cons) = RingConsumer::<u64>::attach(&dst) {
            let mut count = 0;
            while let Some(_) = cons.try_recv() {
                count += 1;
            }
            if count == 50 {
                break;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timeout waiting for mirror to replicate 50 records"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // Terminate server with SIGINT.
    // Mirror has --once, so closing the server makes mirror finish cleanly with Ok(())!
    unsafe {
        libc::kill(s_child.id() as libc::pid_t, libc::SIGINT);
    }

    let s_status = s_child.wait().unwrap();
    assert!(s_status.success());

    let m_status = m_child.wait().unwrap();
    assert!(m_status.success());

    let mut cons = RingConsumer::<u64>::attach(&dst).unwrap();
    for i in 1..=50u64 {
        let val = cons.try_recv().expect("missing message");
        assert_eq!(val, i);
    }
    assert_eq!(cons.try_recv(), None);

    let dst2 = temp_file("repl_dst2");
    let (ok, _, _) = run_cli(&[
        "mirror",
        "127.0.0.1:1",
        dst2.to_str().unwrap(),
        "--from",
        "latest",
        "--once",
    ]);
    assert!(!ok);
    let (ok, _, _) = run_cli(&[
        "mirror",
        "127.0.0.1:1",
        dst2.to_str().unwrap(),
        "--from",
        "resume",
        "--once",
    ]);
    assert!(!ok);
    let (ok, _, _) = run_cli(&[
        "mirror",
        "127.0.0.1:1",
        dst2.to_str().unwrap(),
        "--from",
        "20",
        "--once",
    ]);
    assert!(!ok);

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
    let _ = std::fs::remove_file(&dst2);
}

#[test]
fn test_cli_mirror_reconnect_sigterm() {
    let dst = temp_file("mirror_sig");
    let mut child = Command::new(bin_path())
        .args(&[
            "mirror",
            "127.0.0.1:1",
            dst.to_str().unwrap(),
            "--reconnect-ms",
            "10",
        ])
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    let status = child.wait().unwrap();
    assert!(status.success());
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_cli_unrecognized_options_and_flags() {
    let p = temp_file("unrec_opts");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let (ok, _, _) = run_cli(&["top", p.to_str().unwrap(), "--unknown-flag", "--iterations", "1"]);
    assert!(ok);
    let (ok, _, _) = run_cli(&["dump", p.to_str().unwrap(), "--unknown-flag", "--tail", "1"]);
    assert!(ok);
    let mut child = Command::new(bin_path())
        .args(&["serve", p.to_str().unwrap(), "--bind", "127.0.0.1:0", "--unknown-flag", "--once"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT); }
    let _ = child.wait();
    let (ok, _, _) = run_cli(&["mirror", "127.0.0.1:1", "/tmp/nonexistent_mirror", "--iface", "127.0.0.1", "--unknown-flag", "--once"]);
    assert!(!ok);
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_serve_all_network_flags() {
    let p = temp_file("serve_net_flags");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let mut child = Command::new(bin_path())
        .args(&[
            "serve",
            p.to_str().unwrap(),
            "--bind",
            "127.0.0.1:0",
            "--multicast",
            "239.255.0.1:7401",
            "--iface",
            "127.0.0.1",
            "--mtu",
            "1400",
            "--ttl",
            "1",
            "--udp",
            "7402",
            "--dup",
            "2",
            "--batch",
            "128",
            "--spin",
            "--linger-us",
            "100",
            "--once",
        ])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT); }
    let status = child.wait().unwrap();
    assert!(status.success());
    let _ = std::fs::remove_file(&p);
}

#[test]
fn test_cli_stat_truncated_and_mpmc_top() {
    // Corrupt truncated layout
    let p_reg = temp_file("trunc_reg");
    let _prod = RingProducerBuilder::new(64).max_readers(4).build::<u64, _>(&p_reg).unwrap();
    let file = OpenOptions::new().write(true).open(&p_reg).unwrap();
    file.set_len(128).unwrap();
    let (ok, _, err) = run_cli(&["stat", p_reg.to_str().unwrap()]);
    assert!(!ok);
    assert!(err.contains("Error:"));
    let _ = std::fs::remove_file(&p_reg);

    // MPMC in top
    let p_mpmc = temp_file("mpmc_top");
    let prod = MpmcProducer::<u64>::create(&p_mpmc, 64).unwrap();
    prod.push(&1u64);
    let (ok, out, _) = run_cli(&["top", p_mpmc.to_str().unwrap(), "--interval-ms", "10", "--iterations", "1"]);
    assert!(ok);
    assert!(out.contains("MPMC"));
    let _ = std::fs::remove_file(&p_mpmc);
}
