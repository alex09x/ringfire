#!/usr/bin/env bash
# Three reproducible Criterion runs. Choose two physical cores on the same NUMA node.
# Usage: bash scripts/bench-measure.sh 32,33
set -euo pipefail
cd "$(dirname "$0")/.."
CORES=${1:?provide two physical CPU IDs, e.g. 32,33}
TARGET_DIR=${CARGO_TARGET_DIR:-target}
RUNS=${BENCH_RUNS:-3}
WARMUP=${BENCH_WARMUP:-1}
MEASUREMENT=${BENCH_MEASUREMENT:-3}
SAMPLES=${BENCH_SAMPLES:-50}
mkdir -p "$TARGET_DIR/measurements"
OUTPUT=$(mktemp -d "$TARGET_DIR/measurements/run.XXXXXX")
OUTPUT=$(cd "$OUTPUT" && pwd)
printf 'Reports: %s\n' "$OUTPUT"
{
    if [[ -n ${BENCH_COMMIT:-} ]]; then
        printf 'commit=%s\ntree=%s\n' "$BENCH_COMMIT" "${BENCH_TREE_STATE:-unknown}"
    else
        git rev-parse HEAD
        git status --short
    fi
    uname -srmo
    rustc -Vv
    lscpu
    uptime
    printf 'affinity=%s runs=%s warmup_seconds=%s measurement_seconds=%s samples=%s\n' \
        "$CORES" "$RUNS" "$WARMUP" "$MEASUREMENT" "$SAMPLES"
    for cpu in ${CORES//,/ }; do
        cat "/sys/devices/system/cpu/cpu$cpu/cpufreq/scaling_governor" 2>/dev/null || true
    done
} >"$OUTPUT/environment.txt"
# Compilation is outside measurement time. No configuration is changed on the host.
timeout --kill-after=5 600 cargo bench --locked --no-run >"$OUTPUT/build.log" 2>&1
for ((pass=1; pass<=RUNS; pass++)); do
    for bench in throughput arena_vs_fixed blackboard latency ipc_compare; do
        echo "Run $pass/$RUNS: $bench"
        timeout --kill-after=5 900 taskset -c "$CORES" cargo bench --locked --bench "$bench" -- \
            --warm-up-time "$WARMUP" --measurement-time "$MEASUREMENT" \
            --sample-size "$SAMPLES" --noplot --save-baseline "repeat-$pass" \
            >"$OUTPUT/$pass-$bench.log" 2>&1
    done
    # Keep raw samples/estimates, not only rounded console numbers.
    python3 - "$TARGET_DIR/criterion" "$OUTPUT" "$pass" <<'PY'
import json, pathlib, shutil, sys
source, dest, run = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
rows = []
for estimate in sorted(source.glob('**/repeat-' + run + '/estimates.json')):
    parent = estimate.parent
    benchmark = json.loads((parent / 'benchmark.json').read_text())
    values = json.loads(estimate.read_text())
    rows.append({'benchmark': benchmark, 'estimates': values})
    output = dest / ('raw-' + run) / parent.relative_to(source)
    output.mkdir(parents=True, exist_ok=True)
    for file in parent.glob('*.json'):
        shutil.copyfile(file, output / file.name)
assert rows, 'no Criterion results found'
(dest / ('estimates-' + run + '.json')).write_text(json.dumps(rows, indent=2) + '\n')
print(f'Preserved {len(rows)} benchmark estimates for run {run}')
PY
done
uptime >>"$OUTPUT/environment.txt"
echo "Measurements complete: $OUTPUT"
