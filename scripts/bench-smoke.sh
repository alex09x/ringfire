#!/usr/bin/env bash
# Smoke check of every benchmark harness on one Linux host: each Criterion bench runs once
# in test mode, then the replication examples run briefly over loopback (both roles of the
# two-host examples as local processes). Checks that the harnesses complete, validate
# their data and clean up; the figures it prints are not measurements.
# See docs/benchmarking.md. Usage: scripts/bench-smoke.sh
set -euo pipefail

cd "$(dirname "$0")/.."
LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/ringfire-smoke.XXXXXX")
trap 'rm -rf "$LOG_DIR"' EXIT
STAMP="$LOG_DIR/start.stamp"
touch "$STAMP"
# Ports derived from the PID so parallel runs on one host do not collide.
BASE=$((20000 + ($$ % 2000) * 10))
TIMEOUT=${SMOKE_TIMEOUT:-120}

echo "== criterion benches, test mode"
cargo bench --locked -- --test

echo "== build examples"
cargo build --locked --release --examples
EX=target/release/examples

# Waits (bounded) until file $1 contains text $2.
wait_for_line() {
    local deadline=$((SECONDS + 30))
    until grep -q "$2" "$1" 2>/dev/null; do
        if ((SECONDS > deadline)); then
            echo "timed out waiting for '$2' in $1" >&2
            cat "$1" >&2 || true
            return 1
        fi
        sleep 0.05
    done
}

echo "== replication_latency (loopback, TCP)"
timeout "$TIMEOUT" "$EX/replication_latency" --samples 5000 --paced-us 100 --burst 200000

echo "== replication_pingpong (two local processes)"
timeout "$TIMEOUT" "$EX/replication_pingpong" --role ponger --bind 127.0.0.1:$((BASE + 1)) \
    --peer 127.0.0.1:$((BASE + 0)) --exit-on-close 1 2>"$LOG_DIR/ponger.err" &
PONGER=$!
timeout "$TIMEOUT" "$EX/replication_pingpong" --role pinger --bind 127.0.0.1:$((BASE + 0)) \
    --peer 127.0.0.1:$((BASE + 1)) --samples 2000 --warmup 200 --paced-us 100
wait "$PONGER"
cat "$LOG_DIR/ponger.err"

echo "== replication_stress (two local processes)"
timeout "$TIMEOUT" "$EX/replication_stress" --role ponger --bind 127.0.0.1:$((BASE + 3)) \
    --peer 127.0.0.1:$((BASE + 2)) --exit-on-close 1 2>"$LOG_DIR/stress_ponger.err" &
PONGER=$!
timeout "$TIMEOUT" "$EX/replication_stress" --role pinger --bind 127.0.0.1:$((BASE + 2)) \
    --peer 127.0.0.1:$((BASE + 3)) --rate 20000 --seconds 2 --warmup-ms 1500
wait "$PONGER"
cat "$LOG_DIR/stress_ponger.err"

echo "== replication_stages (master + one slave, local)"
timeout "$TIMEOUT" "$EX/replication_stages" --role master --bind 127.0.0.1:$((BASE + 4)) \
    --clock 127.0.0.1:$((BASE + 5)) --rate 1000 --seconds 2 --warmup-ms 1500 \
    --drain-secs 20 2>"$LOG_DIR/master.err" >"$LOG_DIR/master.out" &
MASTER=$!
wait_for_line "$LOG_DIR/master.err" "served on"
timeout "$TIMEOUT" "$EX/replication_stages" --role slave --source 127.0.0.1:$((BASE + 4)) \
    --clock 127.0.0.1:$((BASE + 5)) --ring "/dev/shm/ringfire_smoke_slave_$$.shm" --name s1
wait "$MASTER"
cat "$LOG_DIR/master.err" "$LOG_DIR/master.out"

leftover=$(find /dev/shm "${TMPDIR:-/tmp}" -maxdepth 1 -name 'ringfire_*' -newer "$STAMP" 2>/dev/null | wc -l)
echo "== leftover ringfire files in /dev/shm and the temp dir since the start: $leftover"
test "$leftover" -eq 0
echo "== smoke OK"
