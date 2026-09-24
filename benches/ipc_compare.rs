//! Round-trip latency of a 64-byte message between two threads over different IPC
//! mechanisms, measured the same way: the bench thread sends a ping and waits for the
//! echo thread's reply. One iteration = one round trip.
//!
//! `cargo bench --bench ipc_compare`

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ringfire::{FutexWait, RingConsumer, RingProducer};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::FromRawFd;
use std::os::unix::net::UnixStream;
use std::thread;

#[derive(Clone, Copy)]
#[repr(C)]
struct Msg64 {
    seq: u64,
    payload: [u8; 56],
}

const STOP: u64 = u64::MAX;

fn msg(seq: u64) -> Msg64 {
    Msg64 { seq, payload: [0xAB; 56] }
}

/// Ping-pong over two rings. `blocking` selects `FutexWait` (sleeps in the kernel when
/// idle, like a socket read) instead of busy polling on both sides.
fn bench_ringfire(c: &mut Criterion, name: &str, blocking: bool) {
    let dir = std::env::temp_dir();
    let fwd = dir.join(format!("bench_cmp_fwd_{}.shm", name));
    let rev = dir.join(format!("bench_cmp_rev_{}.shm", name));
    let _ = std::fs::remove_file(&fwd);
    let _ = std::fs::remove_file(&rev);

    let mut prod_fwd = RingProducer::<Msg64>::create(&fwd, 4096).unwrap();
    let mut prod_rev = RingProducer::<Msg64>::create(&rev, 4096).unwrap();
    let mut cons_fwd = RingConsumer::<Msg64>::attach(&fwd).unwrap();
    let mut cons_rev = RingConsumer::<Msg64>::attach(&rev).unwrap();

    let echo = thread::spawn(move || {
        let mut wait = FutexWait::default();
        loop {
            let ping = if blocking {
                cons_fwd.recv_blocking(&mut wait)
            } else {
                loop {
                    if let Some(p) = cons_fwd.try_recv() {
                        break p;
                    }
                    core::hint::spin_loop();
                }
            };
            if ping.seq == STOP {
                break;
            }
            prod_rev.push(&ping);
        }
    });

    let mut seq = 1u64;
    let mut wait = FutexWait::default();
    c.bench_function(&format!("ipc_rtt_64B/{}", name), |b| {
        b.iter(|| {
            prod_fwd.push(black_box(&msg(seq)));
            loop {
                let resp = if blocking {
                    cons_rev.recv_blocking(&mut wait)
                } else {
                    match cons_rev.try_recv() {
                        Some(r) => r,
                        None => {
                            core::hint::spin_loop();
                            continue;
                        }
                    }
                };
                if resp.seq == seq {
                    black_box(resp);
                    break;
                }
            }
            seq += 1;
        });
    });

    prod_fwd.push(&msg(STOP));
    echo.join().unwrap();
}

fn as_bytes(m: &Msg64) -> &[u8] {
    unsafe { std::slice::from_raw_parts((m as *const Msg64).cast::<u8>(), 64) }
}

/// Ping-pong over a byte stream (socket or pipe pair). The echo side exits on EOF.
fn bench_stream<R, W>(c: &mut Criterion, name: &str, mut tx: W, mut rx: R, echo: impl FnOnce() + Send + 'static)
where
    R: Read,
    W: Write,
{
    let handle = thread::spawn(echo);
    let mut seq = 1u64;
    let mut buf = [0u8; 64];
    c.bench_function(&format!("ipc_rtt_64B/{}", name), |b| {
        b.iter(|| {
            tx.write_all(as_bytes(&msg(seq))).unwrap();
            rx.read_exact(&mut buf).unwrap();
            black_box(&buf);
            seq += 1;
        });
    });
    drop(tx);
    drop(rx);
    handle.join().unwrap();
}

fn echo_loop(mut rx: impl Read, mut tx: impl Write) {
    let mut buf = [0u8; 64];
    while rx.read_exact(&mut buf).is_ok() {
        if tx.write_all(&buf).is_err() {
            break;
        }
    }
}

fn pipe_pair() -> (std::fs::File, std::fs::File) {
    let mut fds = [0i32; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    unsafe { (std::fs::File::from_raw_fd(fds[0]), std::fs::File::from_raw_fd(fds[1])) }
}

fn bench_ipc_compare(c: &mut Criterion) {
    bench_ringfire(c, "ringfire_busy_spin", false);
    bench_ringfire(c, "ringfire_futex_wait", true);

    let (a, b) = UnixStream::pair().unwrap();
    let (a_rx, b_rx) = (a.try_clone().unwrap(), b.try_clone().unwrap());
    bench_stream(c, "unix_domain_socket", a, a_rx, move || echo_loop(b_rx, b));

    let (fwd_rx, fwd_tx) = pipe_pair();
    let (rev_rx, rev_tx) = pipe_pair();
    bench_stream(c, "pipe", fwd_tx, rev_rx, move || echo_loop(fwd_rx, rev_tx));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    client.set_nodelay(true).unwrap();
    server.set_nodelay(true).unwrap();
    let (client_rx, server_rx) = (client.try_clone().unwrap(), server.try_clone().unwrap());
    bench_stream(c, "tcp_loopback", client, client_rx, move || echo_loop(server_rx, server));
}

criterion_group!(benches, bench_ipc_compare);
criterion_main!(benches);
