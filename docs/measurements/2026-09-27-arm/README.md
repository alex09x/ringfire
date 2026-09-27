# Corrected benchmark measurements — Linux AArch64, 2026-09-27 UTC

These are three repeated measurements, with every run retained. They are not a
regression comparison with the historical Ryzen or WAN results.

- CPU: ARM Neoverse-N1, 128 physical cores, no SMT; affinity restricted to CPUs 32,33
  on NUMA node 1. Threads can migrate within that two-core set; they are not pinned
  individually. Linux 6.8.0-137, rustc 1.97.1 / LLVM 22.1.6.
- Both selected cores reported `performance`; boost reported disabled. No host
  configuration was changed. The node was shared, not isolated; unrelated work and
  validation restricted to CPUs 0–15 were running. The repeat ranges expose this noise.
- tmpfs `/dev/shm`, release profile, optimization 3, fat LTO, one codegen unit.
- Each case: 1 s warm-up, 3 s measurement, 50 Criterion samples, repeated three times.
  Reported values below are **Criterion sample medians**, in ns per iteration. They are
  not individual-message p50/p99 latency distributions. Some cases include validation
  and timer/loop overhead, as defined in [benchmarking.md](../../benchmarking.md).
- The original 27 cases use benchmark sources at `7fde3b1`. The additional zero-spin
  case uses `f323a2b`; earlier case bodies are unchanged. The last original IPC launch
  stopped before measurement during a dev-dependency lockfile update and was resumed
  once; no completed samples were discarded. Details are in [environment.txt](environment.txt).

## All runs

One batch iteration receives **32 messages**; divide that row by 32 for ns/message.
Arena view reads the sequence in place and exposes the slice to `black_box`; it does
not copy the full payload. Default futex waiting spins up to 32 times before sleeping;
zero-spin waiting removes that spin budget but can still receive an already-ready reply.

| Benchmark | Run 1, ns | Run 2, ns | Run 3, ns | Median of runs, ns | Run range, ns |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `ipc_rtt_64B/pipe` | 10141.324 | 9607.456 | 10224.723 | 10141.324 | 9607.456–10224.723 |
| `ipc_rtt_64B/ringfire_busy_spin` | 295.189 | 289.797 | 286.454 | 289.797 | 286.454–295.189 |
| `ipc_rtt_64B/ringfire_futex_no_spin` | 4427.731 | 4456.910 | 6080.025 | 4456.910 | 4427.731–6080.025 |
| `ipc_rtt_64B/ringfire_futex_wait` | 421.146 | 440.779 | 357.279 | 421.146 | 357.279–440.779 |
| `ipc_rtt_64B/tcp_loopback` | 21514.745 | 21673.752 | 21482.762 | 21514.745 | 21482.762–21673.752 |
| `ipc_rtt_64B/unix_domain_socket` | 6551.724 | 7158.693 | 6542.894 | 6551.724 | 6542.894–7158.693 |
| `push_throughput/fixed_slot/1KB` | 186.847 | 165.983 | 164.440 | 165.983 | 164.440–186.847 |
| `push_throughput/fixed_slot/256B` | 57.193 | 40.759 | 42.640 | 42.640 | 40.759–57.193 |
| `push_throughput/fixed_slot/32B` | 8.419 | 8.554 | 8.627 | 8.554 | 8.419–8.627 |
| `push_throughput/fixed_slot/64B` | 10.467 | 10.596 | 11.354 | 10.596 | 10.467–11.354 |
| `push_throughput/payload_arena/1KB` | 177.873 | 191.747 | 197.153 | 191.747 | 177.873–197.153 |
| `push_throughput/payload_arena/256B` | 49.215 | 52.096 | 51.326 | 51.326 | 49.215–52.096 |
| `push_throughput/payload_arena/32B` | 21.190 | 21.233 | 47.042 | 21.233 | 21.190–47.042 |
| `push_throughput/payload_arena/64B` | 23.107 | 21.941 | 24.575 | 23.107 | 21.941–24.575 |
| `push_throughput/payload_arena/64KB` | 4318.152 | 4030.971 | 4128.779 | 4128.779 | 4030.971–4318.152 |
| `push_throughput/payload_arena/8KB` | 1259.117 | 1190.962 | 1082.254 | 1190.962 | 1082.254–1259.117 |
| `recv_throughput/fixed_slot/64B` | 23.851 | 22.289 | 22.327 | 22.327 | 22.289–23.851 |
| `recv_throughput/payload_arena_copy/1KB` | 133.893 | 104.484 | 109.216 | 109.216 | 104.484–133.893 |
| `recv_throughput/payload_arena_copy/64B` | 17.866 | 17.814 | 17.840 | 17.840 | 17.814–17.866 |
| `recv_throughput/payload_arena_view_inplace/1KB` | 27.042 | 26.339 | 27.248 | 27.042 | 26.339–27.248 |
| `ringfire_blackboard/seqlock_read_o1` | 11.134 | 11.096 | 11.082 | 11.096 | 11.082–11.134 |
| `ringfire_blackboard/seqlock_write_o1` | 7.071 | 7.061 | 7.060 | 7.061 | 7.060–7.071 |
| `ringfire_latency/ping_pong_rtt` | 117.131 | 117.155 | 117.089 | 117.131 | 117.089–117.155 |
| `ringfire_throughput/spmc_batch_recv_32` | 364.850 | 363.100 | 361.577 | 363.100 | 361.577–364.850 |
| `ringfire_throughput/spmc_push` | 11.324 | 11.079 | 10.258 | 11.079 | 10.258–11.324 |
| `ringfire_throughput/spmc_push_with_reader_lossless` | 35.247 | 36.383 | 36.547 | 36.383 | 35.247–36.547 |
| `ringfire_throughput/spmc_push_with_reader_lossy` | 35.461 | 35.015 | 36.491 | 35.461 | 35.015–36.491 |
| `ringfire_throughput/spmc_try_recv` | 10.910 | 10.833 | 10.835 | 10.835 | 10.833–10.910 |

## Evidence and reproduction

- [CSV medians](medians.csv), per-run full estimates and confidence intervals:
  [run 1](estimates-1.json), [run 2](estimates-2.json), [run 3](estimates-3.json).
- [Raw Criterion samples, logs and environment archive](criterion-raw.tar.gz).
- [Production coverage summary](coverage-summary.json): 96.67% lines in the measured
  all-features Linux ARM build. Coverage does not include test bodies or benchmark code.

```sh
bash scripts/bench-measure.sh 32,33  # select suitable physical CPU IDs on your host
bash scripts/bench-smoke.sh       # behavioral check; not performance evidence
bash scripts/coverage.sh          # >=95% of production Rust lines
```

The revised network examples passed local TCP/multicast/unicast smoke validation.
No fresh cross-host LAN/WAN performance claim is made in this report.
