#!/usr/bin/env bash
#
# Comparative vectorsearch benchmark sweep across all 3 engines.
#
# Ingest strategy — reuse the index across runs wherever possible. We only
# re-ingest when forced to by the optimization under test:
#
#   - OpenSearch index is ingested ONCE at the start (Run A pass1). All 8
#     subsequent OS passes reuse it:
#       * Runs B and D (MOS on) toggle MOS in-place via close/set/open
#       * Runs C and D (gRPC) use a new `grpc-search-only` test procedure
#         against the same Lucene index (gRPC is just a transport — the
#         underlying data is identical)
#   - Milvus ingests ONCE (run 1 pass1). Run 1 pass2 is search-only.
#   - Vespa ingests ONCE (run 1 pass1). Run 1 pass2 is search-only.
#
# Runs:
#   OpenSearch:
#     A1  REST baseline,    full ingest + search            (cold)
#     A2  REST baseline,    search-only                     (warm)
#     B1  REST + MOS,       search-only (MOS just enabled)  (cold MOS cache)
#     B2  REST + MOS,       search-only                     (warm MOS cache)
#     C1  gRPC only,        grpc-search-only                (cold gRPC JIT)
#     C2  gRPC only,        grpc-search-only                (warm gRPC JIT)
#     D1  gRPC + MOS,       grpc-search-only (MOS on)       (cold)
#     D2  gRPC + MOS,       grpc-search-only                (warm)
#   Milvus:
#     M1  full ingest + search                              (cold)
#     M2  search-only                                       (warm)
#   Vespa:
#     V1  full ingest + search                              (cold)
#     V2  search-only                                       (warm)
#
# 12 total passes. Every pass gets a unique --test-run-id.
#
# The script is error-resilient — if any one pass fails, the others still
# execute, and a pass/fail summary prints at the end. If a pre-ingest fails
# (e.g. A1 for OpenSearch) the dependent runs are skipped automatically.
#
# Usage:
#   bash ~/opensearch-benchmark/scripts/os_4way_sweep.sh
#
# Optional env vars:
#   OS_HOST, OS_REST_PORT, OS_GRPC_PORT
#   VESPA_HOST, VESPA_PORT
#   MILVUS_HOST, MILVUS_PORT
#   WORKLOAD_PATH, OS_PARAMS, VESPA_PARAMS, MILVUS_PARAMS
#   OS_INDEX_NAME, LOG_DIR
#   SKIP_OS / SKIP_VESPA / SKIP_MILVUS (set to "1" to skip that engine)

set -u
set -o pipefail
# intentionally NO `set -e` — we want to continue past individual failures

# ============================================================================
# Config — override via env vars if your setup differs
# ============================================================================
OS_HOST="${OS_HOST:-10.0.131.57}"
OS_REST_PORT="${OS_REST_PORT:-9200}"
OS_GRPC_PORT="${OS_GRPC_PORT:-9400}"

VESPA_HOST="${VESPA_HOST:-10.0.139.8}"
VESPA_PORT="${VESPA_PORT:-8080}"

MILVUS_HOST="${MILVUS_HOST:-10.0.138.142}"
MILVUS_PORT="${MILVUS_PORT:-19530}"

WORKLOAD_PATH="${WORKLOAD_PATH:-/home/ec2-user/opensearch-benchmark-workloads/vectorsearch}"
OS_PARAMS="${OS_PARAMS:-$HOME/params_os_1m.json}"
VESPA_PARAMS="${VESPA_PARAMS:-$HOME/params_vespa_1m.json}"
MILVUS_PARAMS="${MILVUS_PARAMS:-$HOME/params_milvus_1m.json}"
OS_INDEX_NAME="${OS_INDEX_NAME:-vector_1m}"

SKIP_OS="${SKIP_OS:-0}"
SKIP_VESPA="${SKIP_VESPA:-0}"
SKIP_MILVUS="${SKIP_MILVUS:-0}"

LOG_DIR="${LOG_DIR:-$HOME/comparative-sweep-$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$LOG_DIR"

OS_REST_URL="http://${OS_HOST}:${OS_REST_PORT}"
TEST_PROCS_DEFAULT="${WORKLOAD_PATH}/test_procedures/default.json"
TEST_PROCS_BACKUP="/tmp/default.json.bak.$$"

# ============================================================================
# Logging helpers
# ============================================================================
log()  { printf '\033[1;34m[sweep %s]\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG_DIR/sweep.log"; }
warn() { printf '\033[1;33m[sweep %s WARN]\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG_DIR/sweep.log"; }
err()  { printf '\033[1;31m[sweep %s ERROR]\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG_DIR/sweep.log" >&2; }

declare -a RUN_NAMES
declare -a RUN_STATUS
declare -a RUN_IDS
declare -a RUN_LOGS

record_run() {
    RUN_NAMES+=("$1")
    RUN_STATUS+=("$2")
    RUN_IDS+=("$3")
    RUN_LOGS+=("$4")
}

# Look up a previous run's status by label (exact match). Prints PASS/FAIL or NOTFOUND.
run_status_of() {
    local wanted=$1
    for i in "${!RUN_NAMES[@]}"; do
        if [[ "${RUN_NAMES[$i]}" == "$wanted" ]]; then
            echo "${RUN_STATUS[$i]}"
            return
        fi
    done
    echo "NOTFOUND"
}

# ============================================================================
# OpenSearch helpers
# ============================================================================
wait_green() {
    local timeout=${1:-120}
    local deadline=$((SECONDS + timeout))
    while (( SECONDS < deadline )); do
        local status
        status=$(curl -s "$OS_REST_URL/_cluster/health" 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status","?"))' 2>/dev/null || echo "?")
        if [[ "$status" == "green" ]]; then
            return 0
        fi
        sleep 2
    done
    warn "Cluster did not turn green within ${timeout}s"
    return 1
}

toggle_mos() {
    # $1 = "true" or "false"
    local enable=$1
    log "Toggling MOS=$enable on $OS_INDEX_NAME"
    if ! curl -s -f -X POST "$OS_REST_URL/$OS_INDEX_NAME/_close" > /dev/null; then
        warn "Failed to close index (maybe it doesn't exist yet?) — continuing"
        return 1
    fi
    curl -s -f -X PUT "$OS_REST_URL/$OS_INDEX_NAME/_settings" \
        -H 'Content-Type: application/json' \
        -d "{\"index.knn.memory_optimized_search\": ${enable}}" > /dev/null \
        || warn "Failed to set MOS=$enable"
    curl -s -f -X POST "$OS_REST_URL/$OS_INDEX_NAME/_open" > /dev/null \
        || warn "Failed to reopen index"
    wait_green 60

    local actual
    actual=$(curl -s "$OS_REST_URL/$OS_INDEX_NAME/_settings" 2>/dev/null \
        | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("'"$OS_INDEX_NAME"'",{}).get("settings",{}).get("index",{}).get("knn",{}).get("memory_optimized_search","unset"))' 2>/dev/null)
    log "  MOS setting now reports: $actual"
}

verify_grpc_listener() {
    log "Checking gRPC listener on ${OS_HOST}:${OS_GRPC_PORT}..."
    if timeout 3 bash -c "</dev/tcp/${OS_HOST}/${OS_GRPC_PORT}" 2>/dev/null; then
        log "  gRPC port reachable."
        return 0
    fi
    err "gRPC port ${OS_GRPC_PORT} NOT listening on ${OS_HOST}."
    return 1
}

# ============================================================================
# Workload patching — add a grpc-search-only test procedure
# ============================================================================
# The vectorsearch workload ships test_procedures/grpc/search-only-schedule.json
# but doesn't expose it as a standalone test procedure — it's only used inside
# grpc-no-train-test (combined with index + force-merge phases). We add a
# `grpc-search-only` test procedure that uses JUST the search schedule so we
# can run pure gRPC searches against an already-ingested index.
patch_workload_add_grpc_search_only() {
    if ! grep -q '"name": "grpc-search-only"' "$TEST_PROCS_DEFAULT"; then
        log "Patching vectorsearch workload: adding grpc-search-only test procedure"
        cp "$TEST_PROCS_DEFAULT" "$TEST_PROCS_BACKUP"
        python3 - <<PY
import re
path = "$TEST_PROCS_DEFAULT"
with open(path) as f:
    content = f.read()

# The vectorsearch workload's default.json is a JSON array of test procedure
# dicts wrapped inside a Jinja-templated file — NOT pure JSON. We insert a
# new test procedure entry right after the existing "grpc-no-train-test"
# definition so the file remains valid.

new_proc = """{
    "name": "grpc-search-only",
    "description": "gRPC-only search phase; assumes the index was already created and populated (e.g. via a prior REST run). Mirrors 'search-only' but uses proto-vector-search instead of vector-search.",
    "default": false,
    "schedule": [
       {{ benchmark.collect(parts="grpc/search-only-schedule.json") }}
    ]
},
"""

# Find the line after grpc-no-train-test's closing brace and insert there
pattern = r'(\{\s*"name":\s*"grpc-no-train-test"[\s\S]*?\n\s*\},\n)'
match = re.search(pattern, content)
if not match:
    raise SystemExit("Could not find grpc-no-train-test block in default.json — aborting patch")
insert_point = match.end()
patched = content[:insert_point] + "    " + new_proc + content[insert_point:]
with open(path, "w") as f:
    f.write(patched)
print("  grpc-search-only added")
PY
    else
        log "grpc-search-only already present in default.json — no patch needed"
    fi
}

restore_workload() {
    if [[ -f "$TEST_PROCS_BACKUP" ]]; then
        log "Restoring original test_procedures/default.json"
        mv "$TEST_PROCS_BACKUP" "$TEST_PROCS_DEFAULT"
    fi
}

trap 'restore_workload' EXIT INT TERM

# ============================================================================
# Single OSB run wrapper
# ============================================================================
run_osb() {
    # $1: label  $2: test procedure  $3: target host:port  $4: params file  $5: database type ("" for OS)
    # $6+: extra args (may include --user-tag=...)
    #
    # If SWEEP_TAGS env var is set (e.g. "run_type:nightly,sweep_id:abc123"), those tags are
    # merged with any per-run tags inferred from the label (engine + config) and appended
    # as --user-tag to the opensearch-benchmark command. Labels map to config tags as:
    #   os-run-a-*   → engine:opensearch,config:rest-baseline
    #   os-run-b-*   → engine:opensearch,config:rest-mos
    #   os-run-c-*   → engine:opensearch,config:grpc-no-mos
    #   os-run-d-*   → engine:opensearch,config:grpc-mos
    #   milvus-*     → engine:milvus,config:default
    #   vespa-*      → engine:vespa,config:default
    local label=$1 test_proc=$2 target=$3 params=$4 db_type=$5
    shift 5
    local extra_args=("$@")

    # Derive per-run tags from the label
    local label_tags=""
    case "$label" in
        os-run-a-*)  label_tags="engine:opensearch,config:rest-baseline" ;;
        os-run-b-*)  label_tags="engine:opensearch,config:rest-mos" ;;
        os-run-c-*)  label_tags="engine:opensearch,config:grpc-no-mos" ;;
        os-run-d-*)  label_tags="engine:opensearch,config:grpc-mos" ;;
        milvus-*)    label_tags="engine:milvus,config:default" ;;
        vespa-*)     label_tags="engine:vespa,config:default" ;;
    esac

    # Combine SWEEP_TAGS (from caller/cron) with label-derived tags
    local combined_tags="$label_tags"
    if [[ -n "${SWEEP_TAGS:-}" ]]; then
        if [[ -n "$combined_tags" ]]; then
            combined_tags="${combined_tags},${SWEEP_TAGS}"
        else
            combined_tags="$SWEEP_TAGS"
        fi
    fi
    if [[ -n "$combined_tags" ]]; then
        extra_args+=(--user-tag="$combined_tags")
    fi
    local run_id="${label}-$(date -u +%Y%m%d-%H%M%S)"
    local logfile="$LOG_DIR/${label}.log"

    log "=================================================================="
    log "Starting $label"
    log "  procedure: $test_proc  target: $target  params: $(basename "$params")"
    log "  run-id:    $run_id"
    log "  log:       $logfile"
    log "=================================================================="

    : > "$HOME/.benchmark/logs/benchmark.log" 2>/dev/null || true

    local cmd=(
        opensearch-benchmark run
            --pipeline=benchmark-only
            --workload-path="$WORKLOAD_PATH"
            --workload-params="$params"
            --test-procedure="$test_proc"
            --target-hosts="$target"
            --test-run-id="$run_id"
            --kill-running-processes
    )
    if [[ -n "$db_type" ]]; then
        cmd+=(--database-type="$db_type")
    fi
    if (( ${#extra_args[@]} > 0 )); then
        cmd+=("${extra_args[@]}")
    fi

    log "Command: ${cmd[*]}"
    local start_sec=$SECONDS
    if "${cmd[@]}" 2>&1 | tee "$logfile"; then
        local elapsed=$((SECONDS - start_sec))
        log "✅ $label completed in ${elapsed}s"
        record_run "$label" "PASS" "$run_id" "$logfile"
    else
        local elapsed=$((SECONDS - start_sec))
        err "❌ $label FAILED after ${elapsed}s (see $logfile)"
        record_run "$label" "FAIL" "$run_id" "$logfile"
    fi
    if [[ -f "$HOME/.benchmark/logs/benchmark.log" ]]; then
        grep -E "TIMING" "$HOME/.benchmark/logs/benchmark.log" 2>/dev/null \
            > "$LOG_DIR/${label}.timing.log" || true
    fi
}

# ============================================================================
# Pre-flight checks
# ============================================================================
log "Starting comparative vectorsearch sweep"
log "  LOG_DIR=$LOG_DIR"

if ! command -v opensearch-benchmark >/dev/null; then
    err "opensearch-benchmark not found on PATH. Activate the right venv first."
    exit 1
fi

# OS pre-flight
RUN_OS=true
if [[ "$SKIP_OS" == "1" ]]; then
    log "SKIP_OS=1 — skipping OpenSearch runs"
    RUN_OS=false
elif ! curl -sf "$OS_REST_URL/" >/dev/null 2>&1; then
    err "OpenSearch unreachable at $OS_REST_URL — skipping all OS runs"
    RUN_OS=false
else
    log "OpenSearch reachable at $OS_REST_URL"
fi

# gRPC pre-flight (non-fatal)
GRPC_AVAILABLE=true
if $RUN_OS; then
    if ! verify_grpc_listener; then
        GRPC_AVAILABLE=false
        warn "gRPC unavailable — will skip OS runs C and D"
    fi
fi

# Vespa pre-flight
RUN_VESPA=true
if [[ "$SKIP_VESPA" == "1" ]]; then
    log "SKIP_VESPA=1 — skipping Vespa runs"
    RUN_VESPA=false
elif ! curl -sf "http://${VESPA_HOST}:${VESPA_PORT}/ApplicationStatus" >/dev/null 2>&1; then
    err "Vespa unreachable at ${VESPA_HOST}:${VESPA_PORT} — skipping Vespa runs"
    RUN_VESPA=false
else
    log "Vespa reachable at ${VESPA_HOST}:${VESPA_PORT}"
fi

# Milvus pre-flight
RUN_MILVUS=true
if [[ "$SKIP_MILVUS" == "1" ]]; then
    log "SKIP_MILVUS=1 — skipping Milvus runs"
    RUN_MILVUS=false
elif ! timeout 3 bash -c "</dev/tcp/${MILVUS_HOST}/${MILVUS_PORT}" 2>/dev/null; then
    err "Milvus unreachable at ${MILVUS_HOST}:${MILVUS_PORT} — skipping Milvus runs"
    RUN_MILVUS=false
else
    log "Milvus reachable at ${MILVUS_HOST}:${MILVUS_PORT}"
fi

# Verify params files
for pf_var in OS_PARAMS VESPA_PARAMS MILVUS_PARAMS; do
    pf="${!pf_var}"
    if [[ ! -f "$pf" ]]; then
        warn "Params file not found: $pf ($pf_var)"
    fi
done

# Patch workload if we're running gRPC
if $RUN_OS && $GRPC_AVAILABLE; then
    patch_workload_add_grpc_search_only
fi

# ============================================================================
# OpenSearch runs
# ============================================================================
if $RUN_OS; then
    log ""
    log "#############################################################"
    log "# OpenSearch sweep (MOS × gRPC, single ingest, 8 search passes)"
    log "#############################################################"

    # --- Run A: REST baseline ---
    log ""
    log "### OS Run A: REST baseline ###"
    # A1 does the ONE AND ONLY full ingest. Everything else reuses this index.
    run_osb "os-run-a-rest-baseline-pass1" "no-train-test" \
        "${OS_HOST}:${OS_REST_PORT}" "$OS_PARAMS" ""

    if [[ "$(run_status_of os-run-a-rest-baseline-pass1)" == "PASS" ]]; then
        run_osb "os-run-a-rest-baseline-pass2" "search-only" \
            "${OS_HOST}:${OS_REST_PORT}" "$OS_PARAMS" ""

        # --- Run B: REST + MOS ---
        log ""
        log "### OS Run B: REST + MOS (toggle MOS on existing index) ###"
        toggle_mos true
        run_osb "os-run-b-rest-mos-pass1" "search-only" \
            "${OS_HOST}:${OS_REST_PORT}" "$OS_PARAMS" ""
        run_osb "os-run-b-rest-mos-pass2" "search-only" \
            "${OS_HOST}:${OS_REST_PORT}" "$OS_PARAMS" ""
    else
        warn "Skipping OS Runs A-pass2 and B because Run A pass1 (ingest) failed"
    fi

    # --- Run C: gRPC only, no MOS ---
    if $GRPC_AVAILABLE && [[ "$(run_status_of os-run-a-rest-baseline-pass1)" == "PASS" ]]; then
        log ""
        log "### OS Run C: gRPC + no MOS (grpc-search-only, reusing same index) ###"
        toggle_mos false
        run_osb "os-run-c-grpc-no-mos-pass1" "grpc-search-only" \
            "${OS_HOST}:${OS_REST_PORT}" "$OS_PARAMS" "" \
            --grpc-target-hosts="${OS_HOST}:${OS_GRPC_PORT}"
        run_osb "os-run-c-grpc-no-mos-pass2" "grpc-search-only" \
            "${OS_HOST}:${OS_REST_PORT}" "$OS_PARAMS" "" \
            --grpc-target-hosts="${OS_HOST}:${OS_GRPC_PORT}"

        # --- Run D: gRPC + MOS ---
        log ""
        log "### OS Run D: gRPC + MOS (toggle MOS back on, same index) ###"
        toggle_mos true
        run_osb "os-run-d-grpc-mos-pass1" "grpc-search-only" \
            "${OS_HOST}:${OS_REST_PORT}" "$OS_PARAMS" "" \
            --grpc-target-hosts="${OS_HOST}:${OS_GRPC_PORT}"
        run_osb "os-run-d-grpc-mos-pass2" "grpc-search-only" \
            "${OS_HOST}:${OS_REST_PORT}" "$OS_PARAMS" "" \
            --grpc-target-hosts="${OS_HOST}:${OS_GRPC_PORT}"
    else
        warn "Skipping OS Runs C and D (gRPC unavailable or Run A ingest failed)"
    fi
fi

# ============================================================================
# Milvus runs
# ============================================================================
if $RUN_MILVUS; then
    log ""
    log "#############################################################"
    log "# Milvus sweep (1 ingest, 2 search passes)"
    log "#############################################################"
    # Pass 1 does the full pipeline including ingest. Pass 2 is search-only
    # against the same collection — warm JVM / warm grpc.aio code path.
    run_osb "milvus-pass1-full" "no-train-test" \
        "${MILVUS_HOST}:${MILVUS_PORT}" "$MILVUS_PARAMS" "milvus" \
        --client-options="hnsw_ef_search:256,space_type:innerproduct"

    if [[ "$(run_status_of milvus-pass1-full)" == "PASS" ]]; then
        run_osb "milvus-pass2-search-only" "search-only" \
            "${MILVUS_HOST}:${MILVUS_PORT}" "$MILVUS_PARAMS" "milvus" \
            --client-options="hnsw_ef_search:256,space_type:innerproduct"
    else
        warn "Skipping milvus pass2 because pass1 (ingest) failed"
    fi
fi

# ============================================================================
# Vespa runs
# ============================================================================
if $RUN_VESPA; then
    log ""
    log "#############################################################"
    log "# Vespa sweep (1 ingest, 2 search passes)"
    log "#############################################################"
    run_osb "vespa-pass1-full" "no-train-test" \
        "${VESPA_HOST}:${VESPA_PORT}" "$VESPA_PARAMS" "vespa" \
        --client-options="hnsw_ef_search:256"

    if [[ "$(run_status_of vespa-pass1-full)" == "PASS" ]]; then
        run_osb "vespa-pass2-search-only" "search-only" \
            "${VESPA_HOST}:${VESPA_PORT}" "$VESPA_PARAMS" "vespa" \
            --client-options="hnsw_ef_search:256"
    else
        warn "Skipping vespa pass2 because pass1 (ingest) failed"
    fi
fi

# ============================================================================
# Summary
# ============================================================================
log ""
log "=================================================================="
log "Sweep complete. Results:"
log "=================================================================="
passed=0
failed=0
for i in "${!RUN_NAMES[@]}"; do
    status="${RUN_STATUS[$i]}"
    name="${RUN_NAMES[$i]}"
    run_id="${RUN_IDS[$i]}"
    if [[ "$status" == "PASS" ]]; then
        log "  ✅ $name  (test-run-id: $run_id)"
        passed=$((passed + 1))
    else
        log "  ❌ $name  (test-run-id: $run_id)  see ${RUN_LOGS[$i]}"
        failed=$((failed + 1))
    fi
done
log ""
log "Total: $passed passed, $failed failed"
log "Logs: $LOG_DIR"

(( failed == 0 )) || exit 1
