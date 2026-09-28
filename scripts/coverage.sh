#!/usr/bin/env bash
# Measure production Rust source, including CLI and FFI. Test bodies live under tests/.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/coverage
cargo llvm-cov --all-features --tests --locked --no-fail-fast \
  --ignore-filename-regex '(^|/)(tests|examples|benches)/' \
  --fail-under-lines 98 --json --output-path target/coverage/coverage.json \
  -- --test-threads=1
cargo llvm-cov report --html --output-dir target/coverage \
  --ignore-filename-regex '(^|/)(tests|examples|benches)/'
