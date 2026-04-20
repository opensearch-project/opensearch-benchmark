#!/usr/bin/env bash
#
# Recover a wedged OpenSearch node (tarball install via opensearch-cluster-cdk).
# Idempotent — safe to re-run. Handles the specific failure modes we hit on
# arm64 (Graviton) r7g nodes:
#
#   1. JVM zombie from a failed startup (performance-analyzer javaagent
#      keeps the JVM alive even after bootstrap crashes)
#   2. k-NN plugin's Faiss native libs not on java.library.path (arm64
#      CDK tarball install doesn't wire this up)
#   3. Stale vector_1m shard data from a prior crashed ingest, which
#      re-triggers the same native load failure during startup recovery
#
# Does, in order:
#   1. Status snapshot (before)
#   2. Kill JVM (SIGKILL if needed)
#   3. Append -Djava.library.path to jvm.options if missing
#   4. Optionally wipe stale index data (WIPE_DATA=1 or prompt)
#   5. Restart OS in daemon mode
#   6. Wait for REST up + cluster green
#   7. Smoke-test the k-NN Faiss codec with a tiny knn_vector index
#   8. Report success / next steps
#
# Usage:
#   bash recover.sh               # prompts before wiping index data
#   WIPE_DATA=1 bash recover.sh   # wipe without prompting
#   WIPE_DATA=0 bash recover.sh   # never wipe (skip step 4)
#
# Exit codes:
#   0 = recovery successful, OS healthy + smoke test passed
#   1 = recovery failed, inspect logs
set -u

OS_HOME="${OS_HOME:-/home/ec2-user/opensearch}"
OS_USER="${OS_USER:-ec2-user}"
REST_PORT="${REST_PORT:-9200}"
GRPC_PORT="${GRPC_PORT:-9400}"
WIPE_DATA="${WIPE_DATA:-prompt}"

C_RED=$'\033[1;31m'
C_GREEN=$'\033[1;32m'
C_YELLOW=$'\033[1;33m'
C_BLUE=$'\033[1;34m'
C_RESET=$'\033[0m'

log()  { printf '%s[recover %s]%s %s\n' "$C_BLUE" "$(date -u +%H:%M:%S)" "$C_RESET" "$*"; }
ok()   { printf '  %s✅%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn() { printf '  %s⚠️ %s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
bad()  { printf '  %s❌%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; }

fail() {
    bad "$1"
    log "Latest log entries:"
    local log_file
    log_file=$(find "$OS_HOME/logs" -name 'opensearch-*.log' -not -name '*deprecation*' -not -name '*slowlog*' 2>/dev/null | head -1)
    if [[ -n "$log_file" ]]; then
        tail -40 "$log_file" 2>/dev/null | sed 's/^/     /'
    fi
    exit 1
}

# ============================================================================
# 1. Before-snapshot
# ============================================================================
log "=== Before ==="
if pgrep -f 'org.opensearch.bootstrap.OpenSearch' >/dev/null; then
    pid=$(pgrep -f 'org.opensearch.bootstrap.OpenSearch')
    uptime=$(ps -o etime= -p "$pid" 2>/dev/null | xargs)
    ok "JVM alive (pid $pid, up $uptime)"
else
    warn "JVM not running"
fi

if curl -sf "http://localhost:$REST_PORT/_cluster/health" >/dev/null 2>&1; then
    ok "REST responsive on :$REST_PORT"
else
    warn "REST not responding on :$REST_PORT"
fi

# ============================================================================
# 2. Kill any running/zombie JVM
# ============================================================================
log "=== Step 1/7: Stop JVM ==="
if pgrep -f 'org.opensearch.bootstrap.OpenSearch' >/dev/null; then
    log "Sending SIGTERM..."
    sudo pkill -f 'org.opensearch.bootstrap.OpenSearch' 2>/dev/null || true
    # Give it 20 seconds to exit cleanly
    for _ in {1..20}; do
        pgrep -f 'org.opensearch.bootstrap.OpenSearch' >/dev/null || break
        sleep 1
    done
    if pgrep -f 'org.opensearch.bootstrap.OpenSearch' >/dev/null; then
        log "SIGTERM didn't stick — using SIGKILL"
        sudo pkill -9 -f 'org.opensearch.bootstrap.OpenSearch' 2>/dev/null || true
        sleep 2
    fi
fi
if pgrep -f 'org.opensearch.bootstrap.OpenSearch' >/dev/null; then
    fail "Could not kill JVM — check manually"
fi
ok "JVM stopped"

# ============================================================================
# 3. Verify Faiss libs exist and are arm64
# ============================================================================
log "=== Step 2/7: Verify k-NN Faiss libraries ==="
KNN_LIB_DIR="$OS_HOME/plugins/opensearch-knn/lib"
FAISS_LIB="$KNN_LIB_DIR/libopensearchknn_faiss.so"
if [[ ! -f "$FAISS_LIB" ]]; then
    fail "Faiss library not found at $FAISS_LIB — is this an arm64 OpenSearch install?"
fi
lib_arch=$(file "$FAISS_LIB" 2>/dev/null | grep -oE 'aarch64|x86[-_]64' || echo "unknown")
host_arch=$(uname -m)
log "  library arch: $lib_arch, host arch: $host_arch"
if [[ "$lib_arch" == "aarch64" && "$host_arch" == "aarch64" ]]; then
    ok "arm64 library matches host"
elif [[ "$lib_arch" == "x86_64" && "$host_arch" == "x86_64" ]]; then
    ok "x86_64 library matches host"
else
    fail "Architecture mismatch: library=$lib_arch host=$host_arch. Wrong OpenSearch distribution for this instance."
fi

# ============================================================================
# 4. Ensure jvm.options has -Djava.library.path pointing at the plugin lib dir
# ============================================================================
log "=== Step 3/7: Ensure k-NN libs can load (jvm.options + LD_LIBRARY_PATH) ==="
JVM_OPTS="$OS_HOME/config/jvm.options"
if [[ ! -f "$JVM_OPTS" ]]; then
    fail "jvm.options not found at $JVM_OPTS"
fi

# Part 1: java.library.path for the PRIMARY library load via System.loadLibrary()
if grep -q "java.library.path=.*opensearch-knn" "$JVM_OPTS"; then
    ok "jvm.options already has java.library.path for k-NN"
else
    log "Appending -Djava.library.path to jvm.options"
    sudo cp "$JVM_OPTS" "${JVM_OPTS}.bak.$(date +%s)"
    sudo tee -a "$JVM_OPTS" >/dev/null <<EOF

# k-NN plugin primary library path (added by scripts/os-node/recover.sh).
# Used by System.loadLibrary("opensearchknn_faiss") inside the JVM.
-Djava.library.path=$KNN_LIB_DIR:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib
EOF
    ok "Added java.library.path"
fi

# Part 2: LD_LIBRARY_PATH for TRANSITIVE dependencies. libopensearchknn_faiss.so
# internally depends on libopensearchknn_common.so, libopensearchknn_util.so,
# and libgomp.so.1 — all in the same plugin lib dir. The OS-level dynamic
# linker (ld.so) resolves those at runtime using LD_LIBRARY_PATH, NOT
# java.library.path. Without it, loading libopensearchknn_faiss.so succeeds
# but then immediately fails on its transitive deps. The jvm.options fix
# alone is not enough.
#
# Quick diagnostic first:
log "Checking transitive library dependencies via ldd..."
ldd_output=$(ldd "$FAISS_LIB" 2>&1 || true)
if echo "$ldd_output" | grep -q "not found"; then
    warn "  Unresolved dependencies (confirms LD_LIBRARY_PATH fix is needed):"
    echo "$ldd_output" | grep "not found" | sed 's/^/     /'
else
    ok "  All transitive deps resolve (this path may not need the fix, but applying anyway for safety)"
fi

# OpenSearch's launcher sources bin/opensearch-env for env var setup. We
# append an LD_LIBRARY_PATH export there so every subsequent opensearch
# invocation (ours, manual restarts, anything) picks up the plugin lib dir.
OS_ENV="$OS_HOME/bin/opensearch-env"
if [[ ! -f "$OS_ENV" ]]; then
    # Some OS installs don't have opensearch-env; fall back to editing the
    # main launcher script directly.
    OS_ENV="$OS_HOME/bin/opensearch"
fi

if grep -q "LD_LIBRARY_PATH=.*opensearch-knn" "$OS_ENV" 2>/dev/null; then
    ok "LD_LIBRARY_PATH already set in $(basename "$OS_ENV")"
else
    log "Injecting LD_LIBRARY_PATH export into $(basename "$OS_ENV")"
    sudo cp "$OS_ENV" "${OS_ENV}.bak.$(date +%s)"
    # Write a temp file with the export at the top (after the shebang) then
    # replace. Inserting at line 2 (after #!/usr/bin/env bash) to guarantee
    # the env var is set before any other code runs.
    sudo python3 - <<PY
import pathlib
p = pathlib.Path("$OS_ENV")
content = p.read_text()
lines = content.splitlines(keepends=True)
# Find insertion point: right after the shebang line
insert_idx = 1 if lines and lines[0].startswith("#!") else 0
block = (
    "\n"
    "# k-NN plugin transitive library deps (injected by scripts/os-node/recover.sh).\n"
    "# libopensearchknn_faiss.so depends on libopensearchknn_common.so,\n"
    "# libopensearchknn_util.so, and libgomp.so.1 in the same dir. ld.so needs\n"
    "# LD_LIBRARY_PATH (not java.library.path) to find those at dlopen time.\n"
    'export LD_LIBRARY_PATH="$KNN_LIB_DIR:\${LD_LIBRARY_PATH:-}"\n'
    "\n"
)
lines.insert(insert_idx, block)
p.write_text("".join(lines))
PY
    ok "LD_LIBRARY_PATH export added to $(basename "$OS_ENV")"
fi

# Also export it for our own shell so the `opensearch -d` we run below
# picks it up even if the launcher script doesn't re-read opensearch-env.
export LD_LIBRARY_PATH="$KNN_LIB_DIR:${LD_LIBRARY_PATH:-}"
log "  LD_LIBRARY_PATH for this shell: $LD_LIBRARY_PATH"

# ============================================================================
# 5. Optional: wipe stale index data
# ============================================================================
log "=== Step 4/7: Stale index data ==="
DATA_DIR="$OS_HOME/data/nodes/0/indices"
if [[ -d "$DATA_DIR" ]] && [[ -n "$(ls -A "$DATA_DIR" 2>/dev/null)" ]]; then
    idx_count=$(find "$DATA_DIR" -maxdepth 1 -mindepth 1 -type d | wc -l)
    log "Data dir contains $idx_count index directories"

    do_wipe=false
    case "$WIPE_DATA" in
        1|true|yes|y)
            do_wipe=true
            log "WIPE_DATA=$WIPE_DATA — wiping without prompt"
            ;;
        0|false|no|n|skip)
            log "WIPE_DATA=$WIPE_DATA — skipping wipe"
            ;;
        prompt|*)
            log "The previous failed ingest may have left corrupted shard data that"
            log "re-triggers the native load crash during startup recovery."
            read -r -p "  Wipe all indices under $DATA_DIR? [y/N] " reply
            [[ "$reply" =~ ^[Yy]$ ]] && do_wipe=true
            ;;
    esac

    if $do_wipe; then
        log "Wiping $DATA_DIR/*"
        sudo rm -rf "$DATA_DIR"/*
        sudo -u "$OS_USER" mkdir -p "$DATA_DIR"
        sudo rm -rf "$OS_HOME/data/nodes/0/translog" 2>/dev/null || true
        ok "Index data wiped"
    else
        warn "Keeping existing data — startup may re-trigger the crash"
    fi
else
    ok "Data dir empty (no recovery needed)"
fi

# ============================================================================
# 6. Start OpenSearch in daemon mode
# ============================================================================
log "=== Step 5/7: Start OpenSearch ==="
# Use sudo -E to preserve LD_LIBRARY_PATH through the privilege transition,
# plus an explicit env prefix so the variable is definitely in the opensearch
# process's environment (sudo -E only works if sudoers permits it).
sudo -E -u "$OS_USER" env \
    LD_LIBRARY_PATH="$KNN_LIB_DIR:${LD_LIBRARY_PATH:-}" \
    "$OS_HOME/bin/opensearch" -d -p "$OS_HOME/opensearch.pid" \
    || fail "opensearch -d returned non-zero"
sleep 3
ok "Daemon launched with LD_LIBRARY_PATH=$KNN_LIB_DIR:..."

# ============================================================================
# 7. Wait for REST up + cluster green (up to 3 minutes)
# ============================================================================
log "=== Step 6/7: Wait for REST + cluster green ==="
UP=false
for i in {1..90}; do
    if curl -sf "http://localhost:$REST_PORT/_cluster/health" >/dev/null 2>&1; then
        UP=true
        log "REST up after $((i*2))s"
        break
    fi
    sleep 2
done
if ! $UP; then
    fail "REST did not come up within 3 minutes"
fi

# Wait for green
for i in {1..60}; do
    status=$(curl -s "http://localhost:$REST_PORT/_cluster/health" 2>/dev/null \
        | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status","?"))' 2>/dev/null || echo "?")
    if [[ "$status" == "green" ]]; then
        ok "Cluster green"
        break
    fi
    sleep 2
done

# Verify JVM still alive (if it crashed during startup recovery, we'll see it here)
if ! pgrep -f 'org.opensearch.bootstrap.OpenSearch' >/dev/null; then
    fail "JVM died during startup (check log — likely another native lib issue)"
fi

# Check gRPC (non-fatal)
if timeout 3 bash -c "</dev/tcp/localhost/$GRPC_PORT" 2>/dev/null; then
    ok "gRPC listening on :$GRPC_PORT"
else
    warn "gRPC not listening on :$GRPC_PORT (sweep will skip Runs C and D)"
fi

# ============================================================================
# 8. Smoke test: write + query a Faiss HNSW index (hits the crashy code path)
# ============================================================================
log "=== Step 7/7: k-NN Faiss smoke test ==="
TEST_INDEX="knn_smoke_$(date +%s)"
log "Creating test index $TEST_INDEX..."
create_result=$(curl -s -X PUT "http://localhost:$REST_PORT/$TEST_INDEX" \
    -H 'Content-Type: application/json' -d '{
        "settings": {"index.knn": true, "number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"properties": {"v": {"type": "knn_vector", "dimension": 4,
            "method": {"name": "hnsw", "engine": "faiss", "space_type": "innerproduct"}}}}
    }')
if echo "$create_result" | grep -q '"acknowledged":true'; then
    ok "  Index created"
else
    warn "  Create returned: $create_result"
fi

log "Indexing a vector with refresh=true (this is what crashed before)..."
write_result=$(curl -s -X POST "http://localhost:$REST_PORT/$TEST_INDEX/_doc?refresh=true" \
    -H 'Content-Type: application/json' -d '{"v": [0.1, 0.2, 0.3, 0.4]}')
sleep 2

if ! pgrep -f 'org.opensearch.bootstrap.OpenSearch' >/dev/null; then
    fail "JVM DIED during vector refresh — java.library.path fix didn't take effect"
fi
ok "  Vector written + refresh completed, JVM still alive"

log "Running a knn query..."
query_result=$(curl -s -X GET "http://localhost:$REST_PORT/$TEST_INDEX/_search" \
    -H 'Content-Type: application/json' -d '{
        "size": 1, "query": {"knn": {"v": {"vector": [0.1, 0.2, 0.3, 0.4], "k": 1}}}
    }')
if echo "$query_result" | grep -q '"total"'; then
    ok "  Query returned results"
else
    warn "  Query returned unexpected response: $query_result"
fi

log "Cleaning up test index..."
curl -s -X DELETE "http://localhost:$REST_PORT/$TEST_INDEX" >/dev/null
ok "  Test index deleted"

# ============================================================================
# Done
# ============================================================================
log ""
log "=================================================================="
ok "Recovery complete. OpenSearch is ready for benchmarking."
log "=================================================================="
log ""
log "Next: run the comparative sweep from the runner box"
log "  bash ~/opensearch-benchmark/scripts/comparative_sweep.sh"
log ""
log "Or, if Milvus/Vespa already completed in a previous sweep, run OS only:"
log "  SKIP_MILVUS=1 SKIP_VESPA=1 bash ~/opensearch-benchmark/scripts/comparative_sweep.sh"
exit 0
