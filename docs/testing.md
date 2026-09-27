# Tests and coverage

Run `cargo test --all-features`, `cargo test --release --all-features` and
`cargo test --no-default-features` on Linux. The CI matrix exercises x86-64 and
AArch64. C interoperability requires `cc`; Python interoperability requires
Python 3. Network tests use loopback TCP/UDP and a multicast-capable local route.
Missing required prerequisites must not be reported as successful validation.

## Coverage contract

Install `cargo-llvm-cov` 0.9.1 and the Rust `llvm-tools-preview` component, then run:

```sh
bash scripts/coverage.sh
```

The gate requires **at least 95% line coverage** of production Rust source under
`src/`, including the CLI, FFI, optional Tokio wrapper and replication. It runs a
clean instrumented `--all-features --tests` build; test bodies live under `tests/`
and are excluded along with examples and benchmark harnesses. No production
module or error branch is removed from the denominator. The script writes raw
LLVM JSON and an HTML report to `target/coverage/`, uploaded by CI.

This is line coverage on the measured Linux architecture, not a claim of 95%
branch coverage, C/Python source coverage or exhaustive concurrency interleavings.
Doctests and the no-default-features/release configurations run separately; their
profiles are not merged into the coverage percentage. Keeping one configuration
avoids incompatible profile data obscuring the result.

## Measured result

The Linux AArch64 all-features run at `9461d90` covered **4,961 / 5,132 production
lines (96.67%)**, **438 / 439 functions (99.77%)**, and **7,299 / 7,685 regions
(94.98%)**. The gate is on lines. CLI: 94.05%; FFI: 97.41%; replication: 97.13%;
Tokio wrapper: 100%. See the [per-file JSON](measurements/2026-09-27-arm/coverage-summary.json).
CI independently measures its x86-64 build, so its count can differ.

LLVM 22's HTML renderer reports four hash-zero mapping mismatches for inline copies
of `CycleStamp::now`, `CycleStamp::diff_cycles`, `OffsetCheckpoint::save` and
`BusySpin::reset`. Their executed variants and counts are present in the JSON report
(5, 3, 7 and 1 respectively); none of these functions is excluded. The JSON threshold
check passes independently of HTML rendering.

## Test integrity

The multiprocess stress test invokes the exact test in each child with an explicit
worker environment. The parent waits for all eight attach acknowledgements, then
publishes 100,000 records. Each child validates sequence and the full payload and
writes its final receive count. Every wait has a deadline, and a guard kills and
reaps children when an assertion fails. An exit status alone is not delivery proof.

Behavioral tests check replay/checkpoint positions, registry/headroom semantics,
malformed shared-memory layouts, blob bounds and overwrite handling, async idle
wake-up and fairness, CLI commands, FFI round trips, and replication recovery.
Use unique temporary paths and observable readiness; a timed sleep is not a
handshake. Network failures after readiness must fail their tests.

Benchmark methodology and reproduction commands are in [benchmarking.md](benchmarking.md).

The CLI supports finite operation without changing signal semantics:
`ringfire top PATH --iterations 3` prints three refreshes, and
`ringfire serve PATH --bind 127.0.0.1:7400 --once` serves one TCP mirror until it
disconnects. The latter rejects multicast/UDP options. Normal invocations retain their
existing long-running behavior; no signal handler is installed to manufacture coverage.
