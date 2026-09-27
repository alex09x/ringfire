# Benchmarking methodology

This document covers what every benchmark in `benches/` and every replication
measurement example in `examples/` measures, the checks that make a run valid, how to
reproduce a measurement on an isolated host, and the status of each figure published in
the README and `docs/replication.md`.

## Harnesses at a glance

| Harness | One iteration / sample | Clock | Validity checks |
| :--- | :--- | :--- | :--- |
| `benches/throughput.rs` | one `push`, one successful `try_recv`, or one `recv_batch` of 32 | Criterion (monotonic) | every receive returns the next sequence; reader never lapped; with-reader runs account for every push |
| `benches/arena_vs_fixed.rs` | one push or one successful receive of an N-byte payload | Criterion | every receive returns the next sequence and the full length; ring and arena never lap the reader |
| `benches/blackboard.rs` | one uncontended read hit or write of one slot | Criterion | the read returns the written value before and after |
| `benches/latency.rs` | one ring round trip between two threads, 8-byte ping | Criterion | reply carries the sequence just sent; bounded wait |
| `benches/ipc_compare.rs` | one 64-byte round trip per transport | Criterion | reply carries the sequence just sent; bounded or failing waits |
| `examples/replication_latency.rs` | actual push → read on a loopback mirror; burst delivery rate | one process, monotonic | distinct-sequence loss, duplicates, reordering; idle timeouts |
| `examples/replication_pingpong.rs` | closed-loop round trip over two mirrors | pinger, monotonic | fixed ping count; lost, late and unexpected replies counted |
| `examples/replication_stress.rs` | open-loop round trip at a fixed rate | pinger, monotonic | distinct-sequence loss, duplicates, reordering; achieved push rate |
| `examples/replication_stages.rs` | push → read per stage on several hosts | wall clock, offset corrected | per-stage loss accounting; clock offset change over the run |

## Criterion benches

All benches create their rings in `RINGFIRE_BENCH_DIR`, else `/dev/shm` when it exists,
else the temp dir, under names unique to the process (`ringfire_bench_<name>_<pid>_<n>.shm`).
Each file is removed when its bench finishes, during normal completion and Rust unwinding (`benches/support/mod.rs`). An abort or
external kill bypasses destructors; the smoke runner owns a private directory and removes
it even when a child fails.

### Definitions

- **`ringfire_throughput/spmc_push`**: one `push` of a 64-byte message with no reader,
  single thread, warm cache. Rate is messages per second.
- **`ringfire_throughput/spmc_try_recv`**: one `try_recv` that returns the next message.
  Messages are pushed in chunks of 4,096 *before* the timer starts
  (`support::timed_chunks` with `Bencher::iter_custom`), so refill is excluded. The timed loop includes receives, `black_box` and sequence checks. The
  producer runs on the same thread, so the slots are warm in this core's cache; this is
  a warm-cache validated receive loop, not a cross-core figure or an isolated instruction cost. Every message carries its
  sequence and is checked; the run fails if a receive comes back empty or out of order,
  or if the reader was lapped.
- **`ringfire_throughput/spmc_batch_recv_32`**: one `recv_batch` call that must return
  exactly 32 messages. The reported time is per call; Criterion's throughput is set to 32
  elements per iteration, so its `thrpt` is messages per second. Per-message cost is the
  time divided by 32.
- **`ringfire_throughput/spmc_push_with_reader_{lossy,lossless}`**: one `push` while a
  reader thread drains the ring with `recv_batch`. This is the realistic cross-core push
  cost. After the bench the reader drains what is left, then
  `received + lapped == pushed` must hold, the reader must have received something, and a
  lossless reader must never have been lapped.
- **`push_throughput/{fixed_slot,payload_arena}/<size>`**: one push of a payload of the
  given size. Throughput is the payload bytes of one push.
- **`recv_throughput/fixed_slot/64B`, `payload_arena_copy/<size>`**: one successful
  receive, which copies the payload out, with the same chunked untimed refill as
  `spmc_try_recv`. Chunks are at most half the ring and half the arena, so nothing is
  lapped. Sequence and length are checked on every receive.
- **`recv_throughput/payload_arena_view_inplace/1KB`**: one successful `view`. The
  closure reads the 8-byte sequence in place and hands the slice to `black_box`; it does
  not read all 1,024 bytes. It measures in-place access, not a full copy, and is not a
  like-for-like replacement for `payload_arena_copy`.
- **`ringfire_blackboard/seqlock_{read,write}_o1`**: one read hit or one write of one
  slot with no concurrent writer.
- **`ringfire_latency/ping_pong_rtt`**: one round trip of an 8-byte ping over two rings
  with a busy-polling echo thread. Exactly one ping is in flight, so a reply with any other
  sequence fails the run. The wait is bounded (`support::SpinBound`: 10 s, with the clock
  read only once per 65,536 empty polls), and the echo thread aborts the process if it
  panics.
- **`ipc_rtt_64B/<transport>`**: one round trip of a 64-byte message, bench thread to
  echo thread, over ringfire (busy spin or `FutexWait`), a Unix domain socket, a pipe
  pair and TCP loopback with `TCP_NODELAY`. Replies are checked for sequence and all 64 bytes on every transport. A failing
  stream echo closes its end; a whole-case watchdog also covers live but stalled peers
  and helper-thread teardown, without per-operation counters or clock reads.

`black_box` wraps every message or payload a bench receives, and every input a bench
pushes. Sequence validation and loop overhead are included in reported receive costs;
batch validation checks its length and endpoint sequences. A watchdog bounds each
Criterion group/case, including warm-up, sampling and teardown, to 900 seconds by default
(`RINGFIRE_BENCH_WATCHDOG_SECS` overrides it). Cancellation wakes the watchdog immediately.

### Running

Full run (the bench profile inherits `[profile.release]`: `opt-level = 3`, fat LTO, one
codegen unit):

```text
cargo bench --locked --bench throughput
cargo bench --locked --bench arena_vs_fixed
cargo bench --locked --bench latency
cargo bench --locked --bench blackboard
cargo bench --locked --bench ipc_compare
```

Compare against a saved baseline with `-- --save-baseline NAME` and
`-- --baseline NAME`. Run one bench with a filter: `-- recv_throughput`.

## Replication examples

Build with `cargo build --release --examples`. Ring files carry the process ID in their
names and are removed at the end of a run. Finite measurement roles have receive deadlines; pongers normally serve until stopped,
or exit on peer close with `--exit-on-close 1`. Wrap external network experiments in
`timeout` as well. A mirror failure makes the measurement process exit with status 1.

### What is timed

All four examples time from each record's **actual push**: the producer stamps the record
immediately before `push`, after the pacer has released it. They do not time from the
record's scheduled send time, so they do not include how late the pacer released a record
(the delay a coordinated-omission-corrected figure would add). That is deliberate: the
figures are labeled push → read and round trip from push. The open-loop stress prints the
achieved push rate next to the requested one, so a sender that fell behind is visible.

Pacing is drift-free (`support::Pacer`): deadline *k* is exactly `start + k · period`, so
a period that is not a whole number of nanoseconds does not accumulate rounding error, and
a late tick is not skipped.

### Loss accounting

Received records are counted by distinct sequence (`support::SeqTracker`), so a duplicate
cannot hide a loss. Latency and stress examples report distinct delivery, loss, duplicates and reordering.
Ping-pong matches one outstanding sequence and reports missing, late and unexpected
replies; warm-up counters are separate. Stage slaves can count holes only between the
first and last sequence seen; compare their sample count with the master's published
total, and use the master's full-range echo accounting to detect missing prefixes/tails.
Samples are taken from the first arrival of each sequence only.

### Clock domains

- **`replication_latency`**: source, mirror and reader share one process and one
  monotonic `Instant` epoch; the one-way figure has no clock error. Each phase ends on its
  last sequence or after `--idle-ms` (default 2 s, and at least ten pacing periods) with no
  record; the first record may take 10 s. A lost record is therefore reported as a loss.
  Before this revision the loop waited for a fixed number of samples and hung on any loss.
- **`replication_pingpong`**: only the pinger's monotonic clock is used, so the round trip
  has no cross-host clock offset; half of it is only a one-way estimate assuming a symmetric path. The pinger sends exactly
  `--warmup + --samples` pings. A reply that misses `--timeout-ms` is lost; if it arrives
  later it is counted late, not as a sample.
- **`replication_stress`**: the pinger's monotonic clock stamps pushes and echo arrivals.
- **`replication_stages`**: records carry the master's wall clock (`SystemTime`) because
  slaves on other hosts must read the same stamp.
  - The master-local stage and echo round trips use the master's monotonic `Instant` epoch.
  - A slave converts its clock with an offset estimated before the run (minimum round-trip
    probe). The residual error is the path asymmetry plus drift during the run. The slave
    estimates the offset again after the run and prints `offset_change_us`: a change that
    is not small next to the latencies invalidates that slave's figures. Endpoint probes
    cannot bound transient drift or wall-clock steps during the run; retain RTT results
    and treat negative/unstable corrected samples as invalid.
  - The round trip via a slave uses the master's monotonic clock, so it needs no offset
    and is unaffected by wall-clock adjustments. Failed clock probes terminate the slave
    measurement; no fabricated zero offset is published.
  - Push stamps are published through a lock-free table. Before this revision the master
    took a mutex between stamping and pushing, and that mutex was shared with the echo
    handlers, so contention could delay a push after its stamp.
  - After pushing, the master waits until every slave connection has closed, or
    `--drain-secs` pass (default 30), instead of sleeping 4 s.

## Audit of the previous harnesses

These defects were found and fixed:

1. `arena_vs_fixed` receive benches pre-filled 32,768 or 16,384 messages once, then ran
   millions of iterations. After the backlog drained, almost every iteration timed an
   empty poll. `payload_arena_view_inplace` reused the consumer the copy bench had
   already drained, so it timed empty polls from the start.
2. `throughput/spmc_try_recv` and `spmc_batch_recv_32` pushed (inside the timed loop)
   whenever the reader had caught up, so they averaged receives with empty polls and
   pushes. `spmc_batch_recv_32` also declared one element per iteration while a
   successful iteration receives 32.
3. The benches shared fixed file names in the temp dir (collisions between concurrent
   runs; `/tmp` may be disk-backed). Some also did not check that a receive returned data
   or the expected message. The ping-pong benches silently skipped replies with an
   unexpected sequence, and their waits were unbounded if the echo thread died.
4. `replication_latency` waited for a fixed number of samples and for the burst's last
   sequence: one lost record hung the run. The percentile helper also panicked on an empty
   sample set.
5. `replication_pingpong` looped until it had enough samples, so a dead ponger meant an
   endless run of 500 ms timeouts. Late replies were dropped without being counted.
6. `replication_stress` counted every arrival, duplicates included, so a duplicate could
   mask a loss. The published runs report no reordering, and that counter also counted
   duplicates, so they are unaffected. Pacing truncated the period to whole nanoseconds, which drifts at
   rates that do not divide 10⁹ evenly.
7. `replication_stages` had the stamp-then-mutex defect and the fixed 4 s drain sleep
   described above. It ignored echo write errors and did not check clock drift.
8. Mirror errors were discarded (`let _ = mirror.run()`) in every example.

`tests/harness_support_tests.rs` checks the shared helpers: sequence accounting,
percentiles, pacing arithmetic, receive loops that end on loss, chunked timing that
excludes setup, bounded spin waits, and unique, self-removing ring paths.

## Status of published figures

| Figure | Where | Status |
| :--- | :--- | :--- |
| `try_recv` 6.4 ns, `recv_batch(32)` 2.3 ns/msg (74.2 ns) | README | **Withdrawn**: defect 2. Pending re-measurement. |
| SPMC recv 83.74 M msg/s (11.94 ns) | ROADMAP (v0.1 history) | **Withdrawn**: same harness. |
| `push` 1.88 ns, push with a reader 41 / 42 ns, blackboard 2.1 / 1.1 ns, ping-pong 249.6 ns, `ipc_compare` round trips | README | Historical x86-64 results; validation cost in IPC now includes the full payload. Do not compare directly with corrected ARM results. |
| Replication round trips, stress and WAN tables | `docs/replication.md`, README | Definitions unchanged; not re-measured in this revision. The stress runs are published as having no loss or reordering, and the old reordering counter also counted duplicates, so none were hidden. |
| Per-stage push → read (0.1 µs, 3.8 µs, 30–32 µs, …) | README, `docs/replication.md` | Taken with the stamp-then-mutex harness (defect 7). That lock could only add delay to a sample, and only when an echo handler held it. Not re-measured yet. |

Smoke runs validate behavior; their latencies are not treated as performance measurements.

## Reproducing a measurement

The automated local reproduction is `bash scripts/bench-measure.sh A,B`. It records
three runs (1 s warm-up, 3 s measurement, 50 samples by default), raw Criterion JSON and
CPU/kernel/compiler metadata under `${CARGO_TARGET_DIR:-target}/measurements/`.
`BENCH_RUNS`, `BENCH_WARMUP`, `BENCH_MEASUREMENT` and `BENCH_SAMPLES` override those settings.

On the measurement host:

1. Stop other work. Set the CPU governor to `performance`, and disable turbo/boost if you
   need run-to-run stability. Record CPU model, kernel (`uname -r`), `rustc -V`, the
   commit, and whether SMT is on.
2. For thread-pair benches (`latency`, `ipc_compare`, the with-reader benches), run under
   `taskset -c A,B` with two physical cores on one CCX/socket, and note which cores you
   used.
3. Keep rings on tmpfs: `/dev/shm` is used automatically. Set `RINGFIRE_BENCH_DIR` only to
   point at another tmpfs.
4. Run each bench in full (commands above), three times, and publish all three Criterion medians with their range and sample settings.
   Do not pick the fastest run or compare results from different hosts as a regression.
5. For replication, run `replication_stages` and `replication_pingpong` as documented in
   their headers on two hosts, giving each busy-polling process at least two cores (one
   core per mirror process quantizes latencies to scheduler ticks). Report the loss line
   with every latency line, and for slaves the `offset_change_us`.

## Smoke check

`bash scripts/bench-smoke.sh` runs all five Criterion targets in test mode, then
runs the four replication examples as local processes over TCP, multicast and
UDP unicast (where supported). It validates every measured delivery count, sample
count, sequence-loss/duplicate/reordering counter, and process exit. There are no
performance thresholds: smoke latencies are not published benchmark results.

Each command has a timeout. A private ring directory prevents collisions with
concurrent tests; a trap kills and reaps owned children and removes that directory
on failure. Logs remain under `${CARGO_TARGET_DIR:-target}/bench-smoke/` and CI
uploads them. `scripts/check-bench-smoke.py` rejects partial output and nonzero
measured losses. Warm-up loss is printed separately from measured ping-pong loss.
