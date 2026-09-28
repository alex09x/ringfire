#[path = "support/deadline.rs"]
mod deadline;

use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read};
use std::net::UdpSocket;
use std::os::unix::fs::PermissionsExt;
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

    // Out of bounds reader registry offset
    let p = temp_file("bad_reg_offset");
    let _prod = RingProducerBuilder::new(64)
        .max_readers(4)
        .build::<u64, _>(&p)
        .unwrap();
    let file = OpenOptions::new().read(true).write(true).open(&p).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut RingHeader) };
    header.reader_registry_offset = 1_000_000;
    drop(mmap);
    drop(file);
    let (ok, _, err) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(!ok);
    assert!(err.contains("reader registry outside the mapping"));
    let _ = std::fs::remove_file(&p);

    // Out of bounds arena offset
    let p = temp_file("bad_arena_offset");
    let _prod = RingProducer::<u64>::create(&p, 64).unwrap();
    let file = OpenOptions::new().read(true).write(true).open(&p).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut RingHeader) };
    header.arena_offset = 1_000_000;
    header.arena_size = 1000;
    drop(mmap);
    drop(file);
    let (ok, _, err) = run_cli(&["stat", p.to_str().unwrap()]);
    assert!(!ok);
    assert!(err.contains("payload arena outside the mapping"));
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

    let p_bad = temp_file("bad_prune");
    std::fs::write(&p_bad, [0x11u8; 256]).unwrap();
    let (ok, _, err) = run_cli(&["prune", p_bad.to_str().unwrap()]);
    assert!(!ok);
    assert!(err.contains("Error:"));
    let _ = std::fs::remove_file(&p_bad);

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
                "--spin",
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

    // A peer can disappear before sending HELLO (for example a TCP health probe).
    // Match the long-running server's treatment of that EOF as a normal departure.
    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                src.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--once",
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
    drop(std::net::TcpStream::connect(parse_serve_addr(&line)).unwrap());
    assert!(server.wait_bounded(Duration::from_secs(5)).success());
}

#[test]
fn test_cli_clean_subcommand() {
    let dir = std::env::temp_dir().join(format!("rf_clean_test_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let dead_ring = dir.join("dead.shm");
    let dead_board = dir.join("dead_board.shm");
    let unrelated = dir.join("not_a_ring.txt");

    // 1. Create a persistent ring and blackboard, then drop producer so they become orphaned
    {
        let _p = RingProducerBuilder::new(4)
            .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
            .build::<u64, _>(&dead_ring)
            .unwrap();
        let mut b = ringfire::BlackboardProducer::<u64>::create(&dead_board, 4).unwrap();
        b.set_cleanup_mode(ringfire::spmc::CleanupMode::Persistent);
    }
    std::fs::write(&unrelated, b"hello").unwrap();
    let _ = std::fs::create_dir(dir.join("clean_sub_directory"));

    // 2. Run clean --dry-run
    let out = Command::new(bin_path())
        .args(["clean", dir.to_str().unwrap(), "--dry-run"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("[DRY RUN] Orphaned shared memory file:"));
    assert!(stdout.contains("Summary: Found 2 orphaned file(s)"));
    assert!(dead_ring.exists());
    assert!(dead_board.exists());
    assert!(unrelated.exists());

    // 3. Keep an active producer running on active_ring
    let active_ring = dir.join("active.shm");
    let _active_p = RingProducerBuilder::new(4)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&active_ring)
        .unwrap();

    // 4. Run clean for real
    let out = Command::new(bin_path())
        .args(["clean", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Cleaned orphaned shared memory file:"));
    assert!(!dead_ring.exists());
    assert!(!dead_board.exists());
    assert!(active_ring.exists(), "active ring should not be removed");
    assert!(unrelated.exists(), "unrelated file should not be removed");

    // 5. Test nonexistent directory
    let out = Command::new(bin_path())
        .args(["clean", "/path/to/nowhere/does_not_exist"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("does not exist or is not accessible"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_serve_and_mirror_additional_flags() {
    let src = temp_file("src_flags");
    let mut prod = RingProducerBuilder::new(4)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&src)
        .unwrap();
    prod.push(&999);

    let udp_port = ephemeral_udp_port();
    let mcast_port = ephemeral_udp_port();
    let mcast_addr = format!("224.0.0.1:{}", mcast_port);

    // Test serve with multicast, iface, mtu, ttl, udp, dup and --once (which rejects and exits 1 after parsing)
    let (ok, _, err) = run_cli(&[
        "serve",
        src.to_str().unwrap(),
        "--bind",
        "127.0.0.1:0",
        "--multicast",
        &mcast_addr,
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
        "--linger-us",
        "50",
        "--unknown-extra",
        "--once",
    ]);
    assert!(!ok);
    assert!(err.contains("supports TCP only"));

    // Test mirror with reconnect-ms and invalid from
    let (ok, _, err) = run_cli(&[
        "mirror",
        "127.0.0.1:9999",
        "/tmp/nonexistent_mirror.shm",
        "--reconnect-ms",
        "100",
        "--from",
        "invalid_from_mode",
    ]);
    assert!(!ok);
    assert!(err.contains("--from expects"));

    // Spawn server with all multicast + unicast + dup + linger flags
    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                src.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--multicast",
                &mcast_addr,
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
                "--linger-us",
                "50",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    let watcher = LineWatcher::spawn(server.stderr());
    let announce = watcher
        .wait_for(Duration::from_secs(5), |line| {
            line.starts_with("ringfire serve:")
        })
        .expect("serve announcement");
    assert!(announce.contains("multicast"));
    assert!(announce.contains("linger 50 us"));

    server.signal(libc::SIGTERM);
    let _ = server.wait_bounded(Duration::from_secs(5));

    let _ = std::fs::remove_file(src);
}

#[test]
fn test_cli_stat_unknown_magic_and_top_dead_reader() {
    let p_unknown = temp_file("unknown_magic");
    let prod = RingProducerBuilder::new(8)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&p_unknown)
        .unwrap();
    drop(prod);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p_unknown)
        .unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut RingHeader) };
    header.flags = 0; // clear MPMC and SPMC flags to test UNKNOWN mode
    drop(mmap);
    drop(file);

    let (ok, out, _) = run_cli(&["stat", p_unknown.to_str().unwrap(), "--json"]);
    assert!(ok);
    assert!(out.contains("\"mode\": \"UNKNOWN\""));
    let _ = std::fs::remove_file(&p_unknown);

    let p_ring = temp_file("top_dead");
    let mut prod = RingProducerBuilder::new(8)
        .max_readers(2)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&p_ring)
        .unwrap();
    prod.push(&100);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p_ring)
        .unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    let base_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(header.reader_registry_offset as usize) as *mut ReaderSlot
    };
    unsafe {
        let slot = &mut *base_ptr.add(0);
        slot.active.store(1, Ordering::SeqCst);
        slot.pid.store(99999999, Ordering::SeqCst);
        let name = b"dead_top_r\0";
        slot.name[..name.len()].copy_from_slice(name);
    }
    drop(mmap);
    drop(file);

    let (ok, out, _) = run_cli(&[
        "top",
        p_ring.to_str().unwrap(),
        "--interval-ms",
        "10",
        "--iterations",
        "1",
    ]);
    assert!(ok);
    assert!(out.contains("DEAD"));

    // Also run top with 2 iterations and active publishing to exercise rate calculation
    let p_rate = p_ring.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = stop.clone();
    let handle = std::thread::spawn(move || {
        let mut i = 200u64;
        while !stop_clone.load(Ordering::Relaxed) {
            prod.push(&i);
            i += 1;
            std::thread::sleep(Duration::from_millis(5));
        }
    });

    let (ok, _out, _) = run_cli(&[
        "top",
        p_rate.to_str().unwrap(),
        "--interval-ms",
        "50",
        "--iterations",
        "2",
    ]);
    assert!(ok);
    stop.store(true, Ordering::Relaxed);
    let _ = handle.join();
    let _ = std::fs::remove_file(&p_ring);
}

#[test]
fn test_cli_clean_single_file_and_readers() {
    let p = temp_file("clean_single");
    {
        let mut prod = RingProducerBuilder::new(8)
            .max_readers(2)
            .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
            .build::<u64, _>(&p)
            .unwrap();
        prod.push(&1);
    }

    // Set an active reader with current process ID
    let file = OpenOptions::new().read(true).write(true).open(&p).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    let base_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(header.reader_registry_offset as usize) as *mut ReaderSlot
    };
    unsafe {
        let slot = &mut *base_ptr.add(0);
        slot.active.store(1, Ordering::SeqCst);
        slot.pid.store(std::process::id(), Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);

    // Try dry-run on single file with active reader
    let (ok, out, _) = run_cli(&["clean", p.to_str().unwrap(), "--dry-run"]);
    assert!(ok);
    assert!(out.contains("Found 0 orphaned file(s)"));
    assert!(p.exists());

    // Try real clean on single file with active reader -> skipped
    let (ok, out, _) = run_cli(&["clean", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("Cleaned 0 orphaned file(s)"));
    assert!(p.exists());

    // Change reader PID to a dead PID
    let file = OpenOptions::new().read(true).write(true).open(&p).unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &*(mmap.as_ptr() as *const RingHeader) };
    let base_ptr = unsafe {
        mmap.as_mut_ptr()
            .add(header.reader_registry_offset as usize) as *mut ReaderSlot
    };
    unsafe {
        let slot = &mut *base_ptr.add(0);
        slot.pid.store(99999999, Ordering::SeqCst);
    }
    drop(mmap);
    drop(file);

    // Single file dry run with dead reader
    let (ok, out, _) = run_cli(&["clean", p.to_str().unwrap(), "--dry-run"]);
    assert!(ok);
    assert!(out.contains("[DRY RUN] Orphaned shared memory file:"));
    assert!(p.exists());

    // Single file real clean
    let (ok, out, _) = run_cli(&["clean", p.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("Cleaned orphaned shared memory file:"));
    assert!(!p.exists());

    // Test clean with extra positional arg -> line 211
    let (ok, _, _) = run_cli(&["clean", "/dev/shm", "extra_arg"]);
    assert!(ok);

    // Clean directory containing non-ring file and unreadable file -> lines 951 & 966
    let clean_dir = std::env::temp_dir().join(format!("rf_clean_test_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&clean_dir);
    let bad_magic_f = clean_dir.join("bad_magic.shm");
    std::fs::write(&bad_magic_f, [0xEEu8; 256]).unwrap();
    let ro_f = clean_dir.join("ro_file.shm");
    std::fs::write(&ro_f, [0xEEu8; 256]).unwrap();
    let mut perms = std::fs::metadata(&ro_f).unwrap().permissions();
    perms.set_readonly(true);
    let _ = std::fs::set_permissions(&ro_f, perms);
    let (ok, _, _) = run_cli(&["clean", clean_dir.to_str().unwrap()]);
    assert!(ok);
    let mut restore_f = std::fs::metadata(&ro_f).unwrap().permissions();
    restore_f.set_mode(0o644);
    let _ = std::fs::set_permissions(&ro_f, restore_f);
    let _ = std::fs::remove_dir_all(&clean_dir);

    // Clean directory that is read-only -> line 1016
    let ro_dir = std::env::temp_dir().join(format!("rf_clean_ro_dir_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&ro_dir);
    let orphan_f = ro_dir.join("orphan.shm");
    let prod = RingProducerBuilder::new(64)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&orphan_f)
        .unwrap();
    drop(prod);
    let mut dir_perms = std::fs::metadata(&ro_dir).unwrap().permissions();
    dir_perms.set_mode(0o555);
    let _ = std::fs::set_permissions(&ro_dir, dir_perms);
    let (ok, _, _) = run_cli(&["clean", ro_dir.to_str().unwrap()]);
    assert!(ok);
    let mut restore_perms = std::fs::metadata(&ro_dir).unwrap().permissions();
    restore_perms.set_mode(0o777);
    let _ = std::fs::set_permissions(&ro_dir, restore_perms);
    let _ = std::fs::remove_dir_all(&ro_dir);

    // Clean directory that cannot be read (read_dir fails) -> lines 923, 924
    let unreadable_dir =
        std::env::temp_dir().join(format!("rf_clean_unreadable_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&unreadable_dir);
    let mut unread_perms = std::fs::metadata(&unreadable_dir).unwrap().permissions();
    unread_perms.set_mode(0o000);
    let _ = std::fs::set_permissions(&unreadable_dir, unread_perms);
    let (ok, _, _) = run_cli(&["clean", unreadable_dir.to_str().unwrap()]);
    assert!(ok);
    let mut restore_unread = std::fs::metadata(&unreadable_dir).unwrap().permissions();
    restore_unread.set_mode(0o777);
    let _ = std::fs::set_permissions(&unreadable_dir, restore_unread);
    let _ = std::fs::remove_dir_all(&unreadable_dir);

    // Clean ring with corrupt registry bounds (end > mmap.len()) -> lines 978, 979
    let corrupt_reg_p = temp_file("clean_corrupt_reg");
    let prod_cr = RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&corrupt_reg_p)
        .unwrap();
    drop(prod_cr);
    let f_cr = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&corrupt_reg_p)
        .unwrap();
    let mut m_cr = unsafe { MmapMut::map_mut(&f_cr).unwrap() };
    let hdr_cr = unsafe { &mut *(m_cr.as_mut_ptr() as *mut RingHeader) };
    hdr_cr.reader_registry_offset = 128;
    hdr_cr.reader_registry_count = 1_000_000;
    drop(m_cr);
    drop(f_cr);
    let (ok, _, _) = run_cli(&["clean", corrupt_reg_p.to_str().unwrap()]);
    assert!(ok);
    let _ = std::fs::remove_file(&corrupt_reg_p);
}

#[test]
fn test_cli_mirror_resume_mode_and_clean_exit() {
    let src = temp_file("mirror_res_src");
    let dst = temp_file("mirror_res_dst");
    let mut prod = RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&src)
        .unwrap();
    prod.push(&100);

    // Pre-create the mirror ring with identical layout so mirror.resumed() is true!
    let dst_prod = RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&dst)
        .unwrap();
    drop(dst_prod);

    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                src.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--once",
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

    let mut mirror = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "mirror",
                &parse_serve_addr(&line),
                dst.to_str().unwrap(),
                "--from",
                "resume",
                "--once",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    let m_watcher = LineWatcher::spawn(mirror.stderr());
    let m_line = m_watcher
        .wait_for(Duration::from_secs(5), |line| line.contains("resumed"))
        .expect("mirror announced resumed mode");
    assert!(m_line.contains("resumed"));

    server.signal(libc::SIGTERM);
    let _ = server.wait_bounded(Duration::from_secs(5));
    assert!(mirror.wait_bounded(Duration::from_secs(5)).success());
    let _ = std::fs::remove_file(src);
    let _ = std::fs::remove_file(dst);

    // Test mirror --once to non-existent server (hits connect fail branch in once mode)
    let (ok, _, err) = run_cli(&[
        "mirror",
        "127.0.0.1:1",
        "/tmp/rf_nonexistent_mirror.shm",
        "--once",
    ]);
    assert!(!ok);
    assert!(err.contains("Error:"));
}

#[test]
fn test_cli_serve_and_mirror_once_complete_coverage() {
    let src = temp_file("sm_once_src");
    let dst = temp_file("sm_once_dst");
    let mut prod = RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&src)
        .unwrap();
    prod.push(&100);

    // Verify serve --once error when --udp or --multicast is supplied
    let (ok, _, err) = run_cli(&[
        "serve",
        src.to_str().unwrap(),
        "--bind",
        "127.0.0.1:0",
        "--once",
        "--udp",
        "54321",
    ]);
    assert!(!ok);
    assert!(err.contains("serve --once supports TCP only"));

    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                src.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--once",
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

    let mut mirror = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "mirror",
                &parse_serve_addr(&line),
                dst.to_str().unwrap(),
                "--from",
                "oldest",
                "--spin",
                "--once",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if RingConsumer::<u64>::attach(&dst).is_ok_and(|mut cons| cons.try_recv().is_some()) {
            break;
        }
        assert!(Instant::now() < deadline, "timeout waiting for mirror");
        std::thread::sleep(Duration::from_millis(20));
    }

    server.signal(libc::SIGTERM);
    let mirror_status = mirror.wait_bounded(Duration::from_secs(5));
    assert!(mirror_status.success());

    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
}

#[test]
fn test_cli_serve_once_client_disconnect_clean_exit() {
    let _deadline = deadline::Deadline::new();
    let src = temp_file("serve_once_eof");
    let _prod = RingProducer::<u64>::create(&src, 16).unwrap();

    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                src.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--once",
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

    let addr = parse_serve_addr(&line);
    // Connect and immediately close stream without sending HELLO
    {
        let stream = std::net::TcpStream::connect(addr).unwrap();
        drop(stream);
    }

    // Server should handle UnexpectedEof/ConnectionReset, return Ok(()), and exit 0
    let status = server.wait_bounded(Duration::from_secs(5));
    assert!(status.success());
    let _ = std::fs::remove_file(&src);
}

#[test]
fn test_cli_stat_arena_and_registry_edge_cases() {
    // Arena capacity 0 -> stat prints util = 0.0
    let p_zero = temp_file("stat_zero_arena");
    let _prod = BlobProducer::<u32>::create(&p_zero, 8, 1024).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p_zero)
        .unwrap();
    let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
    let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut ringfire::header::RingHeader) };
    let arena_ptr = unsafe { mmap.as_mut_ptr().add(header.arena_offset as usize) as *mut u64 };
    unsafe {
        *arena_ptr = 0; // arena_cap = 0
    }
    drop(mmap);
    drop(file);
    let (ok, out, _) = run_cli(&["stat", p_zero.to_str().unwrap()]);
    assert!(ok);
    assert!(out.contains("0.0%"));
    let _ = std::fs::remove_file(&p_zero);
}

#[test]
fn test_cli_additional_prune_clean_serve_mirror_coverage() {
    let _deadline = deadline::Deadline::new();

    // 1. clean default (/dev/shm) and clean on directory with files
    let (ok_clean_def, _, _) = run_cli(&["clean", "--dry-run"]);
    assert!(ok_clean_def);

    let d_clean = temp_file("clean_dir_test");
    std::fs::create_dir_all(&d_clean).unwrap();
    let f1 = d_clean.join("test_file_1");
    std::fs::write(&f1, b"arbitrary non-shm contents").unwrap();
    let (ok_clean_dir, _, _) = run_cli(&["clean", d_clean.to_str().unwrap(), "--dry-run"]);
    assert!(ok_clean_dir);
    let _ = std::fs::remove_dir_all(&d_clean);

    // 2. prune with an alive reader in reader registry (lines 974-985)
    let p_prune = temp_file("prune_alive_slot");
    let mut prod_prune = RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .max_readers(2)
        .build::<u64, _>(&p_prune)
        .unwrap();
    prod_prune.push(&100u64);
    // Attach consumer so it registers current pid
    let mut cons_prune = RingConsumer::<u64>::builder()
        .consumer_name("prune_alive")
        .attach(&p_prune)
        .unwrap();
    let _ = cons_prune.try_recv();

    // Drop producer so file is not locked by producer, but reader is still alive!
    drop(prod_prune);

    let (ok_clean_alive, out_clean_alive, _) =
        run_cli(&["clean", p_prune.to_str().unwrap(), "--dry-run"]);
    assert!(ok_clean_alive);
    assert!(out_clean_alive.contains("Summary: Found 0 orphaned file(s)"));

    let (ok_prune_alive, out_prune_alive, _) = run_cli(&["prune", p_prune.to_str().unwrap()]);
    assert!(ok_prune_alive);
    assert!(out_prune_alive.contains("Successfully pruned 0 dead reader slot(s)."));
    drop(cons_prune);
    let _ = std::fs::remove_file(&p_prune);

    // 3. serve with --udp, --dup 2, --spin (lines 1039, 1050, 1060-1063, 1079)
    let p_serve = temp_file("serve_cov_ring");
    let mut prod_serve = RingProducerBuilder::new(16)
        .cleanup_mode(ringfire::spmc::CleanupMode::Persistent)
        .build::<u64, _>(&p_serve)
        .unwrap();
    prod_serve.push(&42u64);

    let udp_port = ephemeral_udp_port();
    let mut server = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "serve",
                p_serve.to_str().unwrap(),
                "--bind",
                "127.0.0.1:0",
                "--udp",
                &udp_port.to_string(),
                "--dup",
                "2",
                "--spin",
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
    assert!(line.contains("busy-poll"));

    let line2 = watcher
        .wait_for(Duration::from_secs(5), |line| {
            line.contains("unicast from udp port")
        })
        .expect("unicast announcement");
    assert!(line2.contains("2 copies per datagram"));

    server.signal(libc::SIGTERM);
    let _ = server.wait_bounded(Duration::from_secs(5));
    let _ = std::fs::remove_file(&p_serve);

    // 4. mirror connect failure with reconnect (lines 1150, 1154, 1155)
    let dst_mirror = temp_file("mirror_reconnect_test");
    let mut mirror = ChildGuard::spawn(
        Command::new(bin_path())
            .args([
                "mirror",
                "127.0.0.1:1", // guaranteed connection refused
                dst_mirror.to_str().unwrap(),
                "--reconnect",
                "10",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    std::thread::sleep(Duration::from_millis(60));
    mirror.signal(libc::SIGTERM);
    let _ = mirror.wait_bounded(Duration::from_secs(5));
    let _ = std::fs::remove_file(&dst_mirror);

    // 5. clean corrupt reader registry bounds (line 979)
    let p_corrupt_reg = temp_file("clean_corrupt_reg");
    let _prod_cr = RingProducer::<u64>::create(&p_corrupt_reg, 64).unwrap();
    let file_cr = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&p_corrupt_reg)
        .unwrap();
    let mut mmap_cr = unsafe { MmapMut::map_mut(&file_cr).unwrap() };
    let hdr_cr = unsafe { &mut *(mmap_cr.as_mut_ptr() as *mut RingHeader) };
    hdr_cr.reader_registry_offset = 128;
    hdr_cr.reader_registry_count = 100_000;
    drop(mmap_cr);
    drop(file_cr);
    let (ok_cr, _, _) = run_cli(&["clean", p_corrupt_reg.to_str().unwrap(), "--dry-run"]);
    assert!(ok_cr);
    let _ = std::fs::remove_file(&p_corrupt_reg);

    // 6. mirror connect failure with --once (lines 1147-1148)
    let (ok_m_fail, _, _) = run_cli(&["mirror", "127.0.0.1:1", "/dev/null", "--once"]);
    assert!(!ok_m_fail);
}
