#!/usr/bin/env bash
#
# Read-only status check for an OpenSearch node deployed via opensearch-cluster-cdk's
# tarball install (NOT the RPM install — no systemd involved). Runs on the OS node
# itself. Safe to invoke any time.
#
# Reports:
#   - JVM liveness, uptime, RSS, CPU
#   - REST (:9200) + gRPC (:9400) reachability
#   - Cluster health (if REST is up)
#   - Installed k-NN plugin Faiss libraries (architecture + location)
#   - Latest 15 log lines for context
#
# Usage:
#   bash status.sh
#
# Exit code:
#   0 = OS fully healthy (REST + JVM alive)
#   1 = something's wrong
set -u

OS_HOME="${OS_HOME:-/home/ec2-user/opensearch}"
REST_PORT="${REST_PORT:-9200}"
GRPC_PORT="${GRPC_PORT:-9400}"

C_RED=$'\033[1;31m'
C_GREEN=$'\033[1;32m'
C_YELLOW=$'\033[1;33m'
C_BLUE=$'\033[1;34m'
C_RESET=$'\033[0m'

header()  { printf '\n%s=== %s ===%s\n' "$C_BLUE" "$*" "$C_RESET"; }
ok()      { printf '  %s✅%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn()    { printf '  %s⚠️ %s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
bad()     { printf '  %s❌%s %s\n' "$C_RED" "$C_RESET" "$*"; }
info()    { printf '     %s\n' "$*"; }

EXIT_CODE=0

# ============================================================================
# 1. JVM state
# ============================================================================
header "JVM state"
JVM_PID=$(pgrep -f 'org.opensearch.bootstrap.OpenSearch' || true)
if [[ -n "$JVM_PID" ]]; then
    ok "JVM alive (pid $JVM_PID)"
    info "$(ps -o etime,state,rss,pcpu,cmd -p "$JVM_PID" 2>/dev/null | tail -1 | awk '{printf "uptime=%s state=%s rss=%sKB cpu=%s%%", $1, $2, $3, $4}')"
else
    bad "JVM NOT running"
    EXIT_CODE=1
fi

# ============================================================================
# 2. Listening ports
# ============================================================================
header "Listening ports"
PORTS=$(sudo ss -tlnp 2>/dev/null | grep -E ":($REST_PORT|9300|$GRPC_PORT|94[0-9]{2})" || true)
if [[ -n "$PORTS" ]]; then
    echo "$PORTS" | while read -r line; do
        info "$line"
    done
else
    bad "No OS ports ($REST_PORT, 9300, $GRPC_PORT) listening"
    EXIT_CODE=1
fi

# ============================================================================
# 3. REST health
# ============================================================================
header "REST endpoint :$REST_PORT"
if curl -sf "http://localhost:$REST_PORT/_cluster/health" >/dev/null 2>&1; then
    HEALTH=$(curl -s "http://localhost:$REST_PORT/_cluster/health" 2>/dev/null)
    STATUS=$(echo "$HEALTH" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status","?"))' 2>/dev/null || echo "?")
    case "$STATUS" in
        green)  ok "cluster status: green" ;;
        yellow) warn "cluster status: yellow" ;;
        red)    bad "cluster status: red"; EXIT_CODE=1 ;;
        *)      bad "cluster status: $STATUS"; EXIT_CODE=1 ;;
    esac
    info "$(echo "$HEALTH" | python3 -m json.tool 2>/dev/null | head -20)"
else
    bad "REST not responding on :$REST_PORT"
    EXIT_CODE=1
fi

# ============================================================================
# 4. gRPC listener
# ============================================================================
header "gRPC endpoint :$GRPC_PORT"
if timeout 3 bash -c "</dev/tcp/localhost/$GRPC_PORT" 2>/dev/null; then
    ok "gRPC TCP port $GRPC_PORT accepting connections"
else
    warn "gRPC TCP port $GRPC_PORT not listening"
    warn "  (this only matters for Run C and Run D of the 4-way sweep)"
fi

# ============================================================================
# 5. k-NN Faiss native libraries
# ============================================================================
header "k-NN plugin Faiss libraries"
KNN_LIB_DIR="$OS_HOME/plugins/opensearch-knn/lib"
if [[ -d "$KNN_LIB_DIR" ]]; then
    ok "plugin lib dir exists: $KNN_LIB_DIR"
    for lib in libopensearchknn_faiss.so libopensearchknn_common.so libopensearchknn_util.so; do
        if [[ -f "$KNN_LIB_DIR/$lib" ]]; then
            arch=$(file "$KNN_LIB_DIR/$lib" 2>/dev/null | awk -F', ' '{for(i=1;i<=NF;i++) if($i ~ /aarch64|x86[-_]64/) {print $i; exit}}')
            info "  $lib — $arch"
        else
            warn "  $lib MISSING"
        fi
    done
    # Check transitive dependency resolution — this is the failure mode that
    # -Djava.library.path doesn't fix. ldd uses LD_LIBRARY_PATH.
    if [[ -f "$KNN_LIB_DIR/libopensearchknn_faiss.so" ]]; then
        unresolved=$(ldd "$KNN_LIB_DIR/libopensearchknn_faiss.so" 2>&1 | grep "not found" || true)
        if [[ -z "$unresolved" ]]; then
            ok "Faiss lib's transitive deps all resolve (from current shell env)"
        else
            warn "Faiss lib has unresolved deps when loaded from current shell:"
            echo "$unresolved" | sed 's/^/     /'
            warn "  The running OpenSearch JVM likely has the same problem unless"
            warn "  LD_LIBRARY_PATH was set before launch (see recover.sh Step 3)"
        fi
    fi
else
    bad "k-NN plugin lib dir not found at $KNN_LIB_DIR"
    EXIT_CODE=1
fi

# ============================================================================
# 6. jvm.options library path
# ============================================================================
header "java.library.path in jvm.options"
JVM_OPTS="$OS_HOME/config/jvm.options"
if grep -q "java.library.path=.*opensearch-knn" "$JVM_OPTS" 2>/dev/null; then
    ok "jvm.options has k-NN library path directive"
    info "$(grep 'java.library.path' "$JVM_OPTS" | tail -1)"
else
    warn "jvm.options does NOT include k-NN lib dir in java.library.path"
    warn "  run recover.sh to add it"
fi

# ============================================================================
# 7. Latest log lines
# ============================================================================
header "Latest log lines"
LOG_FILE=$(find "$OS_HOME/logs" -name 'opensearch-*.log' -not -name '*deprecation*' -not -name '*slowlog*' 2>/dev/null | head -1)
if [[ -n "$LOG_FILE" ]]; then
    info "source: $LOG_FILE"
    tail -15 "$LOG_FILE" 2>/dev/null | sed 's/^/     /'
else
    warn "no log file found"
fi

# ============================================================================
# Summary
# ============================================================================
header "Summary"
if (( EXIT_CODE == 0 )); then
    ok "OpenSearch is healthy and ready for benchmarking"
else
    bad "OpenSearch is NOT healthy. Run scripts/os-node/recover.sh to fix."
fi

exit $EXIT_CODE
