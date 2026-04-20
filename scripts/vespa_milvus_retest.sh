#!/usr/bin/env bash
#
# Re-run Vespa and Milvus search-only (2 passes each) to verify whether the
# surprisingly low QPS numbers from the 2026-04-09 sweep are reproducible or
# were first-run warmup artifacts.
#
# Context:
#   - Old m5d.8xlarge (32c): Milvus 242 QPS, Vespa 222 QPS (baseline)
#   - New r7g.4xlarge (16c): Milvus 102 QPS (-58%!), Vespa 200 QPS (-10%)
#   - Halving cores should give roughly -15% to -30% at 1 client for I/O-bound
#     workloads. Vespa matches expectations. Milvus does NOT.
#
# Both boxes still have their existing collections/indices from the sweep, so
# we use `search-only` (no re-ingestion).
#
# Usage:
#   bash scripts/vespa_milvus_retest.sh
#
set -u
set -o pipefail

VESPA_HOST="${VESPA_HOST:-10.0.139.8}"
VESPA_PORT="${VESPA_PORT:-8080}"
MILVUS_HOST="${MILVUS_HOST:-10.0.138.142}"
MILVUS_PORT="${MILVUS_PORT:-19530}"
WORKLOAD_PATH="${WORKLOAD_PATH:-/home/ec2-user/opensearch-benchmark-workloads/vectorsearch}"
VESPA_PARAMS="${VESPA_PARAMS:-$HOME/params_vespa_1m.json}"
MILVUS_PARAMS="${MILVUS_PARAMS:-$HOME/params_milvus_1m.json}"
LOG_DIR="${LOG_DIR:-$HOME/vm-retest-$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$LOG_DIR"

C_BLUE=$'\033[1;34m'
C_GREEN=$'\033[1;32m'
C_RED=$'\033[1;31m'
C_RESET=$'\033[0m'

log() { printf '%s[retest %s]%s %s\n' "$C_BLUE" "$(date -u +%H:%M:%S)" "$C_RESET" "$*" | tee -a "$LOG_DIR/retest.log"; }
ok()  { printf '  %s✅%s %s\n' "$C_GREEN" "$C_RESET" "$*" | tee -a "$LOG_DIR/retest.log"; }
bad() { printf '  %s❌%s %s\n' "$C_RED" "$C_RESET" "$*" | tee -a "$LOG_DIR/retest.log" >&2; }

declare -a RESULTS

run_one() {
    # $1: label  $2: target  $3: params  $4: db_type  $5+: extra args
    local label=$1 target=$2 params=$3 db_type=$4
    shift 4
    local run_id="${label}-$(date -u +%Y%m%d-%H%M%S)"
    local logfile="$LOG_DIR/${label}.log"

    log "=== $label ==="
    log "  target:  $target"
    log "  run-id:  $run_id"
    log "  log:     $logfile"

    : > "$HOME/.benchmark/logs/benchmark.log" 2>/dev/null || true

    local cmd=(
        opensearch-benchmark run
            --pipeline=benchmark-only
            --workload-path="$WORKLOAD_PATH"
            --workload-params="$params"
            --test-procedure=search-only
            --target-hosts="$target"
            --database-type="$db_type"
            --test-run-id="$run_id"
            --kill-running-processes
            "$@"
    )

    local start_sec=$SECONDS
    if "${cmd[@]}" 2>&1 | tee "$logfile"; then
        local elapsed=$((SECONDS - start_sec))
        ok "$label completed in ${elapsed}s"

        # Extract mean throughput from the log for the summary
        local qps
        qps=$(grep -E "Mean Throughput" "$logfile" | grep "prod-queries" | awk '{print $(NF-1)}' | head -1)
        RESULTS+=("$label|PASS|${qps:-?} ops/s|$run_id")
    else
        bad "$label FAILED"
        RESULTS+=("$label|FAIL|—|$run_id")
    fi
}

# ============================================================================
# Pre-flight
# ============================================================================
log "Pre-flight checks..."
if ! command -v opensearch-benchmark >/dev/null; then
    bad "opensearch-benchmark not on PATH"
    exit 1
fi
if ! curl -sf "http://$VESPA_HOST:$VESPA_PORT/ApplicationStatus" >/dev/null 2>&1; then
    bad "Vespa unreachable at $VESPA_HOST:$VESPA_PORT"
    exit 1
fi
if ! timeout 3 bash -c "</dev/tcp/$MILVUS_HOST/$MILVUS_PORT" 2>/dev/null; then
    bad "Milvus unreachable at $MILVUS_HOST:$MILVUS_PORT"
    exit 1
fi
ok "Both targets reachable"

# ============================================================================
# Milvus — 2 warm search-only passes (index already exists from sweep)
# ============================================================================
log ""
log "##############################"
log "# Milvus — 2 search-only passes"
log "##############################"

for pass in 1 2; do
    run_one "milvus-retest-pass${pass}" \
        "${MILVUS_HOST}:${MILVUS_PORT}" \
        "$MILVUS_PARAMS" \
        "milvus" \
        --client-options="hnsw_ef_search:256,space_type:innerproduct"
done

# ============================================================================
# Vespa — 2 warm search-only passes (index already exists from sweep)
# ============================================================================
log ""
log "##############################"
log "# Vespa — 2 search-only passes"
log "##############################"

for pass in 1 2; do
    run_one "vespa-retest-pass${pass}" \
        "${VESPA_HOST}:${VESPA_PORT}" \
        "$VESPA_PARAMS" \
        "vespa" \
        --client-options="hnsw_ef_search:256"
done

# ============================================================================
# Summary
# ============================================================================
log ""
log "=================================================================="
log "Retest complete."
log "=================================================================="
printf '\n%-30s %-6s %-20s %s\n' "label" "status" "mean QPS" "test-run-id"
printf '%-30s %-6s %-20s %s\n' "------------------------------" "------" "--------------------" "-----------"
for r in "${RESULTS[@]}"; do
    IFS='|' read -r label status qps run_id <<< "$r"
    printf '%-30s %-6s %-20s %s\n' "$label" "$status" "$qps" "$run_id"
done
echo

log "Compare against sweep baselines:"
log "  Milvus sweep pass2: 101.6 ops/s  (if retest is also ~100, the regression is real)"
log "  Vespa sweep pass2:  199.7 ops/s  (for reference)"
log ""
log "Detailed metrics are in the test_run.json files:"
log "  ~/.osb/benchmarks/test-runs/<run-id>/test_run.json"
