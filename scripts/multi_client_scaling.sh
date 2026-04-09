#!/usr/bin/env bash
#
# Multi-client scaling test for comparative vectorsearch benchmark.
#
# Runs search-only at 1, 8, 16, and 32 clients against all three engines
# (OpenSearch, Vespa, Milvus) to measure QPS scaling behavior.
#
# Prerequisites:
#   - All three engines have cohere-1m indexed (from a prior comparative_sweep.sh run)
#   - Params files exist at $HOME/params_{os,vespa,milvus}_1m.json
#   - opensearch-benchmark is installed and on PATH
#
# Usage:
#   bash scripts/multi_client_scaling.sh
#
# Override defaults:
#   OS_HOST=10.0.131.57 VESPA_HOST=10.0.139.8 MILVUS_HOST=10.0.138.142 \
#     bash scripts/multi_client_scaling.sh
#
set -u
set -o pipefail

# === Configuration ===
OS_HOST="${OS_HOST:-10.0.131.57}"
OS_PORT="${OS_PORT:-9200}"
VESPA_HOST="${VESPA_HOST:-10.0.139.8}"
VESPA_PORT="${VESPA_PORT:-8080}"
MILVUS_HOST="${MILVUS_HOST:-10.0.138.142}"
MILVUS_PORT="${MILVUS_PORT:-19530}"

WORKLOAD_PATH="${WORKLOAD_PATH:-/home/ec2-user/opensearch-benchmark-workloads/vectorsearch}"
OS_PARAMS="${OS_PARAMS:-$HOME/params_os_1m.json}"
VESPA_PARAMS="${VESPA_PARAMS:-$HOME/params_vespa_1m.json}"
MILVUS_PARAMS="${MILVUS_PARAMS:-$HOME/params_milvus_1m.json}"

CLIENT_COUNTS="${CLIENT_COUNTS:-1 8 16 32}"
# Two passes per config: pass1 = warmup, pass2 = measurement
PASSES_PER_CONFIG=2

LOG_DIR="${LOG_DIR:-$HOME/scaling-test-$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$LOG_DIR"

# === Formatting ===
C_BLUE=$'\033[1;34m'
C_GREEN=$'\033[1;32m'
C_RED=$'\033[1;31m'
C_YELLOW=$'\033[1;33m'
C_RESET=$'\033[0m'

log()  { printf '%s[scaling %s]%s %s\n' "$C_BLUE" "$(date -u +%H:%M:%S)" "$C_RESET" "$*" | tee -a "$LOG_DIR/scaling.log"; }
ok()   { printf '  %s✅%s %s\n' "$C_GREEN" "$C_RESET" "$*" | tee -a "$LOG_DIR/scaling.log"; }
bad()  { printf '  %s❌%s %s\n' "$C_RED" "$C_RESET" "$*" | tee -a "$LOG_DIR/scaling.log" >&2; }
warn() { printf '  %s⚠️%s  %s\n' "$C_YELLOW" "$C_RESET" "$*" | tee -a "$LOG_DIR/scaling.log"; }

declare -a RESULTS

# === Run a single benchmark ===
run_one() {
    local label=$1 target=$2 params=$3 db_type=$4 clients=$5
    shift 5
    local run_id="${label}-$(date -u +%Y%m%d-%H%M%S)"
    local logfile="$LOG_DIR/${label}.log"

    log "  Starting $label (clients=$clients)"
    log "    target:  $target"
    log "    run-id:  $run_id"

    local cmd=(
        opensearch-benchmark run
            --pipeline=benchmark-only
            --workload-path="$WORKLOAD_PATH"
            --workload-params="$params"
            --test-procedure=search-only
            --target-hosts="$target"
            --test-run-id="$run_id"
            --kill-running-processes
    )

    # Add database type for non-OS engines
    if [[ "$db_type" != "opensearch" ]]; then
        cmd+=(--database-type="$db_type")
    fi

    # Add any extra args (e.g., --client-options)
    cmd+=("$@")

    local start_sec=$SECONDS
    if "${cmd[@]}" 2>&1 | tee "$logfile"; then
        local elapsed=$((SECONDS - start_sec))
        ok "$label completed in ${elapsed}s"

        # Extract mean throughput and recall from log
        local qps recall
        qps=$(grep -E "Mean Throughput" "$logfile" | grep "prod-queries" | awk '{print $(NF-1)}' | head -1)
        recall=$(grep -E "recall@k" "$logfile" | grep "prod-queries" | awk '{print $(NF-1)}' | head -1)
        RESULTS+=("$label|PASS|${qps:-?}|${recall:-?}|${elapsed}s|$run_id")
    else
        local elapsed=$((SECONDS - start_sec))
        bad "$label FAILED after ${elapsed}s"
        RESULTS+=("$label|FAIL|—|—|${elapsed}s|$run_id")
    fi
}

# === Pre-flight checks ===
log "Multi-client scaling test"
log "  LOG_DIR=$LOG_DIR"
log "  CLIENT_COUNTS=$CLIENT_COUNTS"
log ""
log "Pre-flight checks..."

preflight_ok=true

# OpenSearch
if curl -sf "http://${OS_HOST}:${OS_PORT}/_cluster/health" >/dev/null 2>&1; then
    ok "OpenSearch reachable at ${OS_HOST}:${OS_PORT}"
else
    bad "OpenSearch unreachable at ${OS_HOST}:${OS_PORT}"
    preflight_ok=false
fi

# Vespa
if curl -sf "http://${VESPA_HOST}:${VESPA_PORT}/ApplicationStatus" >/dev/null 2>&1; then
    ok "Vespa reachable at ${VESPA_HOST}:${VESPA_PORT}"
else
    bad "Vespa unreachable at ${VESPA_HOST}:${VESPA_PORT}"
    preflight_ok=false
fi

# Milvus
if timeout 3 bash -c "</dev/tcp/${MILVUS_HOST}/${MILVUS_PORT}" 2>/dev/null; then
    ok "Milvus reachable at ${MILVUS_HOST}:${MILVUS_PORT}"
else
    bad "Milvus unreachable at ${MILVUS_HOST}:${MILVUS_PORT}"
    preflight_ok=false
fi

if [[ "$preflight_ok" != "true" ]]; then
    log "Pre-flight failed. Aborting."
    exit 1
fi

# === Params files ===
for f in "$OS_PARAMS" "$VESPA_PARAMS" "$MILVUS_PARAMS"; do
    if [[ ! -f "$f" ]]; then
        bad "Params file not found: $f"
        exit 1
    fi
done
ok "All params files found"

log ""

# === Engine definitions ===
# Each engine: name|target|params_file|db_type|extra_client_options
declare -a ENGINES=(
    "os|${OS_HOST}:${OS_PORT}|${OS_PARAMS}|opensearch|"
    "vespa|${VESPA_HOST}:${VESPA_PORT}|${VESPA_PARAMS}|vespa|--client-options=hnsw_ef_search:256"
    "milvus|${MILVUS_HOST}:${MILVUS_PORT}|${MILVUS_PARAMS}|milvus|--client-options=hnsw_ef_search:256,space_type:innerproduct"
)

# === Main loop ===
for engine_spec in "${ENGINES[@]}"; do
    IFS='|' read -r engine_name target params_file db_type extra_opts <<< "$engine_spec"

    log "#############################################################"
    log "# $engine_name — scaling test (search-only, clients: $CLIENT_COUNTS)"
    log "#############################################################"

    for clients in $CLIENT_COUNTS; do
        log ""
        log "### $engine_name @ $clients client(s) ###"

        # Create a temp params file with search_clients overridden.
        # OSB only accepts one --workload-params value (file OR inline, not both).
        # Passing it twice causes the second to clobber the first.
        tmp_params="$LOG_DIR/.params-${engine_name}-${clients}c.json"
        if command -v jq >/dev/null 2>&1; then
            jq --argjson c "$clients" '.search_clients = $c' "$params_file" > "$tmp_params"
        else
            # Fallback: sed replace (works if search_clients is on its own line)
            sed "s/\"search_clients\":[[:space:]]*[0-9]*/\"search_clients\": $clients/" "$params_file" > "$tmp_params"
        fi

        for pass in $(seq 1 $PASSES_PER_CONFIG); do
            label="${engine_name}-${clients}c-pass${pass}"

            run_args=()
            # Add extra client options if present
            if [[ -n "$extra_opts" ]]; then
                run_args+=($extra_opts)
            fi

            run_one "$label" "$target" "$tmp_params" "$db_type" "$clients" "${run_args[@]}"
        done
    done

    log ""
done

# === Summary ===
log ""
log "=================================================================="
log "Scaling test complete."
log "=================================================================="
printf '\n%-30s %-6s %-15s %-10s %-10s %s\n' "label" "status" "mean QPS" "recall@k" "time" "test-run-id"
printf '%-30s %-6s %-15s %-10s %-10s %s\n' "------------------------------" "------" "---------------" "----------" "----------" "-----------"
for r in "${RESULTS[@]}"; do
    IFS='|' read -r label status qps recall elapsed run_id <<< "$r"
    printf '%-30s %-6s %-15s %-10s %-10s %s\n' "$label" "$status" "$qps" "$recall" "$elapsed" "$run_id"
done | tee -a "$LOG_DIR/scaling.log"

echo ""
log "Detailed metrics: ~/.osb/benchmarks/test-runs/<run-id>/test_run.json"
log "Logs: $LOG_DIR"
