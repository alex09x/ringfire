//! Round-trip latency of a 64-byte message between two threads over different IPC
//! mechanisms, measured the same way: the bench thread sends a ping and waits for the
//! echo thread's reply. One iteration = one round trip; exactly one ping is in flight, so
//! every reply must carry the sequence and payload just sent, unmodified — checked in
//! full for every mechanism, not just the sequence, so a corruption in the other 56 bytes
//! cannot pass unnoticed on one transport but not another.
//!
//! Every wait here is bounded: a per-mechanism [`Watchdog`] aborts the process when the whole case exceeds its deadline, so a peer that stays alive but stops making progress cannot hang the
//! bench (unlike [`AbortOnPanic`], which only covers a peer that panics).
//!
//! `cargo bench --bench ipc_compare`

mod support;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ringfire::{FutexWait, RingConsumer, RingProducer};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::FromRawFd;
use std::os::unix::net::UnixStream;
use std::thread;
use support::{AbortOnPanic, REPLY_TIMEOUT, SpinBound, TempShm, Watchdog};

#[derive(Clone, Copy)]
#[repr(C)]
struct Msg64 {
    seq: u64,
    payload: [u8; 56],
}

const STOP: u64 = u64::MAX;

fn msg(seq: u64) -> Msg64 {
    Msg64 {
        seq,
        payload: [0xAB; 56],
    }
}

/// Ping-pong over two rings. `blocking` selects `FutexWait` (sleeps in the kernel when
/// idle, like a socket read) instead of busy polling on both sides.
fn bench_ringfire(c: &mut Criterion, name: &str, blocking: bool) {
    let fwd = TempShm::new(&format!("cmp_fwd_{}", name));
    let rev = TempShm::new(&format!("cmp_rev_{}", name));

    let mut prod_fwd = RingProducer::<Msg64>::create(fwd.path(), 4096).unwrap();
    let mut prod_rev = RingProducer::<Msg64>::create(rev.path(), 4096).unwrap();
    let mut cons_fwd = RingConsumer::<Msg64>::attach(fwd.path()).unwrap();
    let mut cons_rev = RingConsumer::<Msg64>::attach(rev.path()).unwrap();

    let echo = thread::spawn(move || {
        let _abort = AbortOnPanic("ringfire echo");
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
    // `recv_blocking` is a real kernel wait with no built-in timeout, so `SpinBound`
    // (which only bounds the busy-poll loop) cannot cover it. The watchdog is an
    // independent guard against the echo thread staying alive but stuck; `AbortOnPanic`
    // on that thread only covers it panicking outright.
    let _watchdog = Watchdog::start(name, Watchdog::default_limit());
    c.bench_function(&format!("ipc_rtt_64B/{}", name), |b| {
        b.iter(|| {
            let ping = msg(seq);
            prod_fwd.push(black_box(&ping));
            let resp = if blocking {
                cons_rev.recv_blocking(&mut wait)
            } else {
                let mut bound = SpinBound::new("ringfire reply", REPLY_TIMEOUT);
                loop {
                    if let Some(r) = cons_rev.try_recv() {
                        break r;
                    }
                    bound.spin();
                }
            };
            assert_eq!(resp.seq, seq, "{}: reply out of sequence", name);
            assert_eq!(
                resp.payload, ping.payload,
                "{}: reply payload corrupted",
                name
            );
            black_box(resp);
            seq += 1;
        });
    });

    prod_fwd.push(&msg(STOP));
    echo.join().unwrap();
    assert_eq!(
        cons_rev.lapped_count(),
        0,
        "{}: reply reader was lapped",
        name
    );
}

fn as_bytes(m: &Msg64) -> &[u8] {
    unsafe { std::slice::from_raw_parts((m as *const Msg64).cast::<u8>(), 64) }
}

/// Ping-pong over a byte stream (socket or pipe pair). The echo side exits on EOF, and a
/// failed echo closes its end, so the bench's `read_exact` fails instead of blocking. A
/// peer that stays open but stalls (no EOF, no bytes) has no such signal, so it is bounded
/// by the watchdog instead.
fn bench_stream<R, W>(
    c: &mut Criterion,
    name: &str,
    mut tx: W,
    mut rx: R,
    echo: impl FnOnce() + Send + 'static,
) where
    R: Read,
    W: Write,
{
    let handle = thread::spawn(echo);
    let mut seq = 1u64;
    let mut buf = [0u8; 64];
    let _watchdog = Watchdog::start(name, Watchdog::default_limit());
    c.bench_function(&format!("ipc_rtt_64B/{}", name), |b| {
        b.iter(|| {
            let ping = msg(seq);
            tx.write_all(as_bytes(&ping)).unwrap();
            rx.read_exact(&mut buf).unwrap();
            let got_seq = u64::from_ne_bytes(buf[..8].try_into().unwrap());
            assert_eq!(got_seq, seq, "{}: reply out of sequence", name);
            assert_eq!(
                &buf[..],
                as_bytes(&ping),
                "{}: reply payload corrupted",
                name
            );
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
    unsafe {
        (
            std::fs::File::from_raw_fd(fds[0]),
            std::fs::File::from_raw_fd(fds[1]),
        )
    }
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
    bench_stream(c, "tcp_loopback", client, client_rx, move || {
        echo_loop(server_rx, server)
    });
}

criterion_group!(benches, bench_ipc_compare);
criterion_main!(benches);
