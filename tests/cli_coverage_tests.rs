#[path = "support/deadline.rs"]
mod deadline;

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read};
use std::net::UdpSocket;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use memmap2::MmapMut;
use ringfire::blob::BlobProducer;
use ringfire::header::{ReaderSlot, RingHeader, SLOT_WRITING, validate_ring};
use ringfire::mpmc::MpmcProducer;
use ringfire::spmc::{FlowControl, RingConsumer, RingProducer, RingProducerBuilder};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique path per call: process id plus a monotonic counter, so parallel test threads
/// and repeated invocations never collide even when they share a `name`.
fn temp_file(name: &str) -> PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!(
        "rf_cli_cov_{}_{}_{}.shm",
        name,
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_file(&p);
    p
}

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_ringfire")
}

/// A free UDP port on loopback, picked by the OS rather than a hardcoded literal.
fn ephemeral_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Owns a spawned child and guarantees it is killed and reaped even if a test panics
/// (e.g. on a failed assertion) while the guard is still in scope, so a broken assertion
/// never leaks a subprocess.
struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn spawn(cmd: &mut Command) -> Self {
        ChildGuard(Some(
            cmd.spawn().expect("failed to spawn ringfire subprocess"),
        ))
    }

    fn id(&self) -> u32 {
        self.0.as_ref().expect("child already reaped").id()
    }

    fn stdout(&mut self) -> std::process::ChildStdout {
        self.0
            .as_mut()
            .expect("child already reaped")
            .stdout
            .take()
            .expect("child has no piped stdout")
    }

    fn stderr(&mut self) -> std::process::ChildStderr {
        self.0
            .as_mut()
            .expect("child already reaped")
            .stderr
            .take()
            .expect("child has no piped stderr")
    }

    fn signal(&mut self, sig: libc::c_int) {
        unsafe { libc::kill(self.id() as libc::pid_t, sig) };
    }

    /// Blocks until the child exits, bounded by `timeout`; on timeout it is killed and
    /// reaped so the wait can never hang forever.
    fn wait_bounded(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            let child = self.0.as_mut().expect("child already reaped");
            if let Some(status) = child.try_wait().expect("try_wait failed") {
                self.0 = None;
                return status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let status = child.wait().expect("wait after kill failed");
                self.0 = None;
                return status;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Streams lines from a child's piped stdout/stderr on a background thread, so a test can
/// wait for a specific line with a bounded deadline instead of sleeping blindly.
struct LineWatcher {
    rx: mpsc::Receiver<String>,
}

impl LineWatcher {
    fn spawn<R: Read + Send + 'static>(reader: R) -> Self {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let buffered = BufReader::new(reader);
            for line in buffered.lines() {
                match line {
                    Ok(l) => {
                        if tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        LineWatcher { rx }
    }

    /// Waits until a line matching `predicate` arrives, or `timeout` elapses.
    fn wait_for(
        &self,
        timeout: Duration,
        mut predicate: impl FnMut(&str) -> bool,
    ) -> Option<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.checked_duration_since(Instant::now())?;
            match self.rx.recv_timeout(remaining) {
                Ok(line) => {
                    if predicate(&line) {
                        return Some(line);
                    }
                }
                Err(_) => return None,
            }
        }
    }
}

/// Extracts the address from ringfire's `ringfire serve: <path> on <addr> (...)` line.
fn parse_serve_addr(line: &str) -> String {
    let after = line
        .split(" on ")
        .nth(1)
        .expect("no bind address in serve announcement line");
    after.split(" (").next().unwrap().to_string()
}

/// Runs a finite ringfire invocation. Bounded: if the process does not exit within the
/// timeout (a sign the wrong, long-running mode was invoked by mistake) it is killed and
/// the test fails with a clear message instead of hanging the suite forever.
fn run_cli(args: &[&str]) -> (bool, String, String) {
    let mut child = ChildGuard::spawn(
        Command::new(bin_path())
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    );
    let mut out = child.stdout();
    let mut err = child.stderr();
    let stdout = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        out.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let stderr = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        err.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let status = child.wait_bounded(Duration::from_secs(15));
    let out = String::from_utf8(stdout.join().unwrap()).unwrap();
    let err = String::from_utf8(stderr.join().unwrap()).unwrap();
    assert!(
        status.signal().is_none(),
        "finite CLI invocation was killed or timed out: {args:?}: {err}"
    );
    if status.success() && args.contains(&"--json") {
        let json: serde_json::Value =
            serde_json::from_str(&out).expect("stat must emit valid JSON");
        assert!(json.is_object());
        assert!(json["capacity"].is_u64());
        assert!(json["readers"].is_array());
    }
    (status.success(), out, err)
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
    let base_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(header.reader_registry_offset as usize) as *mut ReaderSlot
    };
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
    let base_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(header.reader_registry_offset as usize) as *mut ReaderSlot
    };
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
fn test_cli_top_options() {
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
}

#[test]
fn test_cli_top_rejects_invalid_iterations() {
    let p = temp_file("top_bad_iter");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();

    let (ok, _, err) = run_cli(&["top", p.to_str().unwrap(), "--iterations", "0"]);
    assert!(!ok);
    assert!(err.contains("--iterations expects a positive integer"));

    let (ok, _, err) = run_cli(&["top", p.to_str().unwrap(), "--iterations", "not-a-number"]);
    assert!(!ok);
    assert!(err.contains("--iterations expects a positive integer"));

    let _ = std::fs::remove_file(&p);
}

/// `top` with no `--iterations` runs forever by design (it is a live dashboard); the only
/// way to end it is a signal. Since ringfire installs no signal handler, the default Unix
/// disposition applies: the process is terminated by the signal, not exited successfully.
/// This is verified with real `ExitStatusExt` semantics, not treated as a coverage source.
#[test]
fn test_cli_top_default_signal_termination() {
    let p = temp_file("top_sig");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();

    let mut guard = ChildGuard::spawn(
        Command::new(bin_path())
            .args(["top", p.to_str().unwrap(), "--interval-ms", "10"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null()),
    );
    let watcher = LineWatcher::spawn(guard.stdout());
    let frame = watcher.wait_for(Duration::from_secs(5), |l| {
        l.contains("Press Ctrl+C to exit.")
    });
    assert!(
        frame.is_some(),
        "top did not render a frame within the deadline"
    );

    guard.signal(libc::SIGTERM);
    let status = guard.wait_bounded(Duration::from_secs(5));
    assert!(!status.success());
    assert_eq!(status.signal(), Some(libc::SIGTERM));

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

    let dst = temp_file("errors_mirror_dst");
    let (ok, _, err) = run_cli(&[
        "mirror",
        "127.0.0.1:7400",
        dst.to_str().unwrap(),
        "--from",
        "invalid",
    ]);
    assert!(!ok);
    assert!(err.contains("--from expects"));

    let (ok, _, _) = run_cli(&["mirror", "127.0.0.1:0", dst.to_str().unwrap(), "--once"]);
    assert!(!ok);
}

#[test]
fn test_cli_serve_and_mirror_full_replication() {
    let src = temp_file("repl_src");
    let dst = temp_file("repl_dst");
    let mut prod = RingProducer::<u64>::create(&src, 128).unwrap();
    for i in 1..=50u64 {
        prod.push(&i);
    }

    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                src.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--batch",
                "64",
                "--spin",
                "--linger-us",
                "10",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    let server_stderr = LineWatcher::spawn(server.stderr());
    let announce = server_stderr
        .wait_for(Duration::from_secs(5), |l| l.starts_with("ringfire serve:"))
        .expect("serve did not announce its bind address within the deadline");
    let addr = parse_serve_addr(&announce);

    let mut mirror = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "mirror",
                &addr,
                dst.to_str().unwrap(),
                "--from",
                "oldest",
                "--once",
                "--spin",
                "--unicast",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    // Bounded wait, synchronized on actual ring content: not "done" until 50 records have
    // really landed in `dst`.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut cons) = RingConsumer::<u64>::attach(&dst) {
            let mut count = 0;
            while cons.try_recv().is_some() {
                count += 1;
            }
            if count == 50 {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timeout waiting for mirror to replicate 50 records"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // The mirror has `--once`: closing the server is what makes it see `peer_gone` and
    // finish cleanly on its own (no signal to the mirror itself). Without ringfire's own
    // signal handler, the killed server is terminated by the signal rather than exiting
    // successfully -- verified with real ExitStatusExt semantics.
    server.signal(libc::SIGTERM);
    let server_status = server.wait_bounded(Duration::from_secs(5));
    assert!(!server_status.success());
    assert_eq!(server_status.signal(), Some(libc::SIGTERM));

    let mirror_status = mirror.wait_bounded(Duration::from_secs(5));
    assert!(mirror_status.success());

    let mut cons = RingConsumer::<u64>::attach(&dst).unwrap();
    for i in 1..=50u64 {
        let val = cons.try_recv().expect("missing message");
        assert_eq!(val, i);
    }
    assert_eq!(cons.try_recv(), None);

    let dst2 = temp_file("repl_dst2");
    let (ok, _, _) = run_cli(&[
        "mirror",
        "127.0.0.1:0",
        dst2.to_str().unwrap(),
        "--from",
        "latest",
        "--once",
    ]);
    assert!(!ok);
    let (ok, _, _) = run_cli(&[
        "mirror",
        "127.0.0.1:0",
        dst2.to_str().unwrap(),
        "--from",
        "resume",
        "--once",
    ]);
    assert!(!ok);
    let (ok, _, _) = run_cli(&[
        "mirror",
        "127.0.0.1:0",
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

/// `mirror` without `--once` reconnects forever on a schedule; the only way to end it is a
/// signal, verified with real Unix semantics rather than assumed to be a clean exit.
#[test]
fn test_cli_mirror_reconnect_default_signal_termination() {
    let dst = temp_file("mirror_sig");
    let mut mirror = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "mirror",
                "127.0.0.1:0",
                dst.to_str().unwrap(),
                "--reconnect-ms",
                "10",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    let stderr_watcher = LineWatcher::spawn(mirror.stderr());
    let failed = stderr_watcher.wait_for(Duration::from_secs(5), |l| l.contains("connect failed"));
    assert!(
        failed.is_some(),
        "mirror did not report a connect failure within the deadline"
    );

    mirror.signal(libc::SIGTERM);
    let status = mirror.wait_bounded(Duration::from_secs(5));
    assert!(!status.success());
    assert_eq!(status.signal(), Some(libc::SIGTERM));

    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_cli_unrecognized_options_and_flags() {
    let p = temp_file("unrec_opts");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();

    let (ok, _, _) = run_cli(&[
        "top",
        p.to_str().unwrap(),
        "--unknown-flag",
        "--iterations",
        "1",
    ]);
    assert!(ok);
    let (ok, _, _) = run_cli(&["dump", p.to_str().unwrap(), "--unknown-flag", "--tail", "1"]);
    assert!(ok);

    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                p.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--unknown-flag",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    let server_stderr = LineWatcher::spawn(server.stderr());
    let announce = server_stderr
        .wait_for(Duration::from_secs(5), |l| l.starts_with("ringfire serve:"))
        .expect("serve with an unknown flag did not start up within the deadline");
    assert!(announce.contains("127.0.0.1"));
    server.signal(libc::SIGTERM);
    let status = server.wait_bounded(Duration::from_secs(5));
    assert!(!status.success());
    assert_eq!(status.signal(), Some(libc::SIGTERM));

    let unrec_dst = temp_file("unrec_mirror_dst");
    let (ok, _, _) = run_cli(&[
        "mirror",
        "127.0.0.1:0",
        unrec_dst.to_str().unwrap(),
        "--iface",
        "127.0.0.1",
        "--unknown-flag",
        "--once",
    ]);
    assert!(!ok);
    let _ = std::fs::remove_file(&p);
}

/// Exercises every serve network flag with a real mirror handshake and content transfer
/// rather than a blind launch/sleep/kill: confirms the
/// startup announcement reflects the configured flags, and asserts the exact replicated
/// contents.
#[test]
fn test_cli_serve_all_network_flags() {
    let src = temp_file("net_flags_src");
    let dst = temp_file("net_flags_dst");
    let mut prod = RingProducer::<u64>::create(&src, 64).unwrap();
    for i in 1..=10u64 {
        prod.push(&i);
    }

    let udp_port = ephemeral_udp_port();
    let mcast_port = 30_000 + (std::process::id() % 5_000) as u16;
    let multicast = format!(
        "239.255.77.{}:{}",
        1 + (std::process::id() % 250) as u8,
        mcast_port
    );

    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                src.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--multicast",
                &multicast,
                "--iface",
                "127.0.0.1",
                "--mtu",
                "1400",
                "--ttl",
                "1",
                "--udp",
                &udp_port.to_string(),
                "--dup",
                "2",
                "--batch",
                "128",
                "--spin",
                "--linger-us",
                "100",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    let server_stderr = LineWatcher::spawn(server.stderr());
    let announce = server_stderr
        .wait_for(Duration::from_secs(5), |l| l.starts_with("ringfire serve:"))
        .expect("serve did not announce its bind address within the deadline");
    assert!(announce.contains("multicast 239.255.77"));
    assert!(announce.contains("mtu 1400"));
    assert!(announce.contains("ttl 1"));
    let addr = parse_serve_addr(&announce);
    let udp_announce = server_stderr
        .wait_for(Duration::from_secs(5), |l| {
            l.contains("unicast from udp port")
        })
        .expect("serve did not announce its unicast udp port within the deadline");
    assert!(udp_announce.contains(&udp_port.to_string()));

    let mut mirror = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "mirror",
                &addr,
                dst.to_str().unwrap(),
                "--from",
                "oldest",
                "--once",
                "--unicast",
                "--iface",
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut cons) = RingConsumer::<u64>::attach(&dst) {
            let mut count = 0;
            while cons.try_recv().is_some() {
                count += 1;
            }
            if count == 10 {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timeout waiting for mirror to replicate 10 records over serve --once"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    prod.push(&11);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut live = RingConsumer::<u64>::builder()
        .start_from_sequence(11)
        .attach(&dst)
        .unwrap();
    loop {
        if let Some(value) = live.try_recv() {
            assert_eq!(value, 11);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "live unicast record not delivered"
        );
        std::thread::yield_now();
    }
    // Stop the source after verifying live UDP delivery; the mirror exits on EOF.
    server.signal(libc::SIGTERM);
    let server_status = server.wait_bounded(Duration::from_secs(5));
    assert!(!server_status.success());
    assert_eq!(server_status.signal(), Some(libc::SIGTERM));

    let mirror_status = mirror.wait_bounded(Duration::from_secs(5));
    assert!(mirror_status.success());

    let mut cons = RingConsumer::<u64>::attach(&dst).unwrap();
    for i in 1..=11u64 {
        assert_eq!(cons.try_recv(), Some(i));
    }
    assert_eq!(cons.try_recv(), None);

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_cli_stat_truncated_and_mpmc_top() {
    // Corrupt truncated layout
    let p_reg = temp_file("trunc_reg");
    let _prod = RingProducerBuilder::new(64)
        .max_readers(4)
        .build::<u64, _>(&p_reg)
        .unwrap();
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
    let (ok, out, _) = run_cli(&[
        "top",
        p_mpmc.to_str().unwrap(),
        "--interval-ms",
        "10",
        "--iterations",
        "1",
    ]);
    assert!(ok);
    assert!(out.contains("MPMC"));
    let _ = std::fs::remove_file(&p_mpmc);
}

#[test]
fn test_cli_finite_tcp_server_and_missing_iterations() {
    let _deadline = deadline::Deadline::new();
    let src = temp_file("finite_server");
    let dst = temp_file("finite_mirror");
    let mut producer = RingProducer::<u64>::create(&src, 64).unwrap();
    producer.push(&123);
    let (ok, _, error) = run_cli(&["top", src.to_str().unwrap(), "--iterations"]);
    assert!(!ok);
    assert!(error.contains("positive integer"));
    for flag in [["--udp", "7403"], ["--multicast", "239.255.0.1:7401"]] {
        let (ok, _, error) = run_cli(&[
            "serve",
            src.to_str().unwrap(),
            "--bind",
            "127.0.0.1:0",
            "--once",
            flag[0],
            flag[1],
        ]);
        assert!(!ok);
        assert!(error.contains("supports TCP only"));
    }
    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                src.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--once",
                "--batch",
                "32",
                "--spin",
                "--linger-us",
                "0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    let watcher = LineWatcher::spawn(server.stderr());
    let line = watcher
        .wait_for(Duration::from_secs(5), |line| {
            line.starts_with("ringfire serve:")
        })
        .expect("server readiness announcement");
    let mut mirror = ringfire::Mirror::builder()
        .start(ringfire::MirrorStart::Oldest)
        .connect(parse_serve_addr(&line), &dst)
        .unwrap();
    while mirror.sequence() < 1 {
        assert!(mirror.step().unwrap());
    }
    let mut reader = RingConsumer::<u64>::attach(&dst).unwrap();
    assert_eq!(reader.try_recv(), Some(123));
    assert_eq!(reader.try_recv(), None);
    drop(mirror); // finite server observes peer closure, returns, and exits normally
    assert!(server.wait_bounded(Duration::from_secs(5)).success());
    std::fs::remove_file(dst).unwrap();
}
