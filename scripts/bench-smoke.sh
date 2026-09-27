#!/usr/bin/env bash
# Exercise all harnesses and supported replication transports; these are not measurements.
set -euo pipefail
cd "$(dirname "$0")/.."
TARGET_DIR=${CARGO_TARGET_DIR:-target}
mkdir -p "$TARGET_DIR/bench-smoke"
LOG_DIR=$(mktemp -d "$TARGET_DIR/bench-smoke/run.XXXXXX")
LOG_DIR=$(cd "$LOG_DIR" && pwd)
SHM_BASE=${TMPDIR:-/tmp}
if [[ -d /dev/shm && -w /dev/shm ]]; then SHM_BASE=/dev/shm; fi
RING_DIR=$(mktemp -d "$SHM_BASE/ringfire-smoke.XXXXXX")
export TMPDIR="$RING_DIR"
export RINGFIRE_BENCH_DIR="$RING_DIR"
PIDS=()
cleanup() {
    local result=$?
    trap - EXIT INT TERM
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
    rm -rf "$RING_DIR"
    echo "Smoke logs: $LOG_DIR"
    if (( result != 0 )); then
        for log in "$LOG_DIR"/*.log; do
            [[ -f "$log" ]] && { echo "--- $log"; tail -n 40 "$log"; }
        done
    fi
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
BASE=${SMOKE_PORT_BASE:-$((20000 + ($$ % 1000) * 30))}
TIMEOUT=${SMOKE_TIMEOUT:-120}
# Compilation is bounded separately; the execution deadline must not depend on a cold cache.
timeout 600 cargo bench --locked --no-run
timeout "$TIMEOUT" cargo bench --locked -- --test >"$LOG_DIR/criterion.log" 2>&1
timeout 600 cargo build --locked --release --examples
EX="$TARGET_DIR/release/examples"
run() {
    local name=$1; shift
    timeout --kill-after=5 "$TIMEOUT" "$@" >"$LOG_DIR/$name.log" 2>&1
    cat "$LOG_DIR/$name.log"
}
wait_for_line() {
    local file=$1 text=$2 pid=$3 deadline=$((SECONDS + 30))
    until grep -q "$text" "$file" 2>/dev/null; do
        kill -0 "$pid" 2>/dev/null || { cat "$file"; return 1; }
        (( SECONDS < deadline )) || { cat "$file"; return 1; }
        sleep 0.05
    done
}
for transport in tcp multicast; do
    extra=()
    if [[ $transport == multicast ]]; then extra=(--multicast "239.255.91.1:$((BASE+10))" --iface 127.0.0.1); fi
    run "latency-$transport" "$EX/replication_latency" --samples 1000 --paced-us 100 --burst 10000 "${extra[@]}"
done
for transport in tcp multicast unicast; do
    pinger_extra=() ponger_extra=() master_extra=() slave_extra=()
    if [[ $transport == multicast ]]; then
        pinger_extra=(--multicast "239.255.91.1:$((BASE+10))" --iface 127.0.0.1)
        ponger_extra=(--multicast "239.255.91.2:$((BASE+11))" --iface 127.0.0.1)
        master_extra=("${pinger_extra[@]}")
        slave_extra=(--iface 127.0.0.1)
    elif [[ $transport == unicast ]]; then
        pinger_extra=(--udp "$((BASE+10))" --unicast 1 --dup 2)
        ponger_extra=(--udp "$((BASE+11))" --unicast 1 --dup 2)
        master_extra=(--udp "$((BASE+10))" --dup 2)
        slave_extra=(--unicast 1)
    fi
    if [[ $transport != unicast ]]; then
        timeout --kill-after=5 "$TIMEOUT" "$EX/replication_pingpong" --role ponger \
            --bind "127.0.0.1:$((BASE+1))" --peer "127.0.0.1:$BASE" --dir "$RING_DIR" \
            --exit-on-close 1 "${ponger_extra[@]}" >"$LOG_DIR/pong-$transport.log" 2>&1 &
        PIDS=("$!")
        run "ping-$transport" "$EX/replication_pingpong" --role pinger --bind "127.0.0.1:$BASE" \
            --peer "127.0.0.1:$((BASE+1))" --dir "$RING_DIR" --samples 500 --warmup 100 \
            --paced-us 100 "${pinger_extra[@]}"
        wait "${PIDS[0]}"; PIDS=()
    fi
    timeout --kill-after=5 "$TIMEOUT" "$EX/replication_stress" --role ponger \
        --bind "127.0.0.1:$((BASE+3))" --peer "127.0.0.1:$((BASE+2))" --dir "$RING_DIR" \
        --exit-on-close 1 "${ponger_extra[@]}" >"$LOG_DIR/stress-ponger-$transport.log" 2>&1 &
    PIDS=("$!")
    run "stress-$transport" "$EX/replication_stress" --role pinger --bind "127.0.0.1:$((BASE+2))" \
        --peer "127.0.0.1:$((BASE+3))" --dir "$RING_DIR" --rate 10000 --seconds 1 \
        --warmup-ms 1500 "${pinger_extra[@]}"
    wait "${PIDS[0]}"; PIDS=()
    timeout --kill-after=5 "$TIMEOUT" "$EX/replication_stages" --role master \
        --bind "127.0.0.1:$((BASE+4))" --clock "127.0.0.1:$((BASE+5))" --ring "$RING_DIR/master.shm" \
        --rate 1000 --seconds 1 --warmup-ms 3000 --drain-secs 20 "${master_extra[@]}" \
        >"$LOG_DIR/master-$transport.log" 2>&1 &
    PIDS=("$!")
    wait_for_line "$LOG_DIR/master-$transport.log" 'served on' "${PIDS[0]}"
    run "slave-$transport" "$EX/replication_stages" --role slave --source "127.0.0.1:$((BASE+4))" \
        --clock "127.0.0.1:$((BASE+5))" --ring "$RING_DIR/slave.shm" --name s1 "${slave_extra[@]}"
    wait "${PIDS[0]}"; PIDS=()
    cat "$LOG_DIR/master-$transport.log"
    BASE=$((BASE + 30))
done
python3 scripts/check-bench-smoke.py "$LOG_DIR"
# Only examine this run's private files: unrelated concurrent tests are not leaks.
if [[ -n $(find "$RING_DIR" -type f -print -quit) ]]; then
    echo 'harness leaked ring files:' >&2
    find "$RING_DIR" -type f >&2
    exit 1
fi
echo 'smoke OK: Criterion and TCP/multicast/unicast examples'
