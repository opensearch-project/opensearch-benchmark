#!/usr/bin/env bash
#
# Nightly comparative sweep for cron. Runs full OS/Vespa/Milvus ingest + search
# pipeline and pushes metrics to the configured OSB datastore.
#
# What this does:
#   1. Self-updates the OSB install from origin/feature/multi-engine (so the runner
#      always picks up the latest pushed code). Logs the SHA that actually ran.
#   2. Updates the workloads checkout to origin/main.
#   3. Runs scripts/comparative_sweep.sh with SWEEP_TAGS="run_type:nightly"
#      so every run is tagged in the datastore. The sweep itself adds per-run
#      tags like engine:opensearch,config:rest-mos.
#   4. Rotates logs (keeps last 30 days).
#
# All metrics land in the datastore configured in ~/.benchmark/benchmark.ini.
# That file must exist on the runner with datastore credentials (mode 600).
#
# Cron entry (2am PST = 10am UTC, no DST handling):
#   0 10 * * * /bin/bash -l /home/ec2-user/opensearch-benchmark/scripts/nightly_sweep.sh >> /home/ec2-user/nightly-logs/cron.log 2>&1
#
# Or use TZ to auto-handle DST:
#   TZ=America/Los_Angeles
#   0 2 * * * /bin/bash -l /home/ec2-user/opensearch-benchmark/scripts/nightly_sweep.sh >> /home/ec2-user/nightly-logs/cron.log 2>&1

set -u
set -o pipefail

# === Paths ===
OSB_DIR="${OSB_DIR:-$HOME/opensearch-benchmark}"
WORKLOADS_DIR="${WORKLOADS_DIR:-$HOME/opensearch-benchmark-workloads}"
NIGHTLY_LOG_DIR="${NIGHTLY_LOG_DIR:-$HOME/nightly-logs}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-30}"

SWEEP_DATE=$(date -u +%Y%m%d-%H%M%S)
SWEEP_LOG="$NIGHTLY_LOG_DIR/nightly-$SWEEP_DATE.log"
mkdir -p "$NIGHTLY_LOG_DIR"

log() { printf '[nightly %s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$SWEEP_LOG"; }
err() { printf '[nightly %s ERR] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$SWEEP_LOG" >&2; }

log "====================================================================="
log "Nightly comparative sweep starting"
log "  OSB_DIR=$OSB_DIR"
log "  WORKLOADS_DIR=$WORKLOADS_DIR"
log "  SWEEP_LOG=$SWEEP_LOG"
log "====================================================================="

# === Pre-flight: datastore config must exist ===
if [[ ! -f "$HOME/.benchmark/benchmark.ini" ]]; then
    err "Missing $HOME/.benchmark/benchmark.ini — datastore config required for nightly runs."
    err "Metrics would not be pushed. Aborting."
    exit 1
fi

# === Step 1: self-update OSB ===
log ""
log "Updating OSB to latest origin/feature/multi-engine..."
if ! cd "$OSB_DIR"; then
    err "Cannot cd to $OSB_DIR"
    exit 1
fi
git fetch origin feature/multi-engine 2>&1 | tee -a "$SWEEP_LOG"
# reset --hard discards any local changes on the runner. This is deliberate
# — nightly runs are reproducible; any manual fiddling on the runner should
# have been committed and pushed by now.
git reset --hard origin/feature/multi-engine 2>&1 | tee -a "$SWEEP_LOG"

# Reinstall editable via python3.11 from within the OSB dir. Editable installs
# pick up code changes without reinstalling, but we still run pip install -e .
# to catch any setup.py / dependency changes. This is the same command that
# works in manual setup — it resolves deps from the existing environment rather
# than from PyPI for private packages like opensearch-protobufs.
if ! python3.11 -m pip install -e . 2>&1 | tee -a "$SWEEP_LOG"; then
    err "pip install failed. Check dependencies."
    exit 1
fi

OSB_SHA=$(git rev-parse --short HEAD)
log "  OSB at $OSB_SHA"

# === Step 2: update workloads ===
log ""
log "Updating workloads to latest origin/main..."
if ! cd "$WORKLOADS_DIR"; then
    err "Cannot cd to $WORKLOADS_DIR"
    exit 1
fi
git fetch origin main 2>&1 | tee -a "$SWEEP_LOG"
git reset --hard origin/main 2>&1 | tee -a "$SWEEP_LOG"
WORKLOADS_SHA=$(git rev-parse --short HEAD)
log "  workloads at $WORKLOADS_SHA"

# === Step 3: run the sweep ===
log ""
log "Launching comparative sweep (SWEEP_TAGS='run_type:nightly,osb_sha:$OSB_SHA')..."
log "---------------------------------------------------------------------"

export SWEEP_TAGS="run_type:nightly,osb_sha:$OSB_SHA"

# The sweep script has per-engine isolation internally — if Vespa dies, OS and
# Milvus still run. We capture its exit code but don't gate on it (we want the
# log rotation and summary to always run).
bash "$OSB_DIR/scripts/comparative_sweep.sh" 2>&1 | tee -a "$SWEEP_LOG"
sweep_exit=${PIPESTATUS[0]}

log "---------------------------------------------------------------------"
if [[ $sweep_exit -eq 0 ]]; then
    log "Comparative sweep completed (exit=0)"
else
    err "Comparative sweep exited with code $sweep_exit (check per-engine results)"
fi

# === Step 4: log rotation ===
log ""
log "Rotating logs (keeping last $LOG_RETENTION_DAYS days)..."
find "$NIGHTLY_LOG_DIR" -name "nightly-*.log" -mtime +$LOG_RETENTION_DAYS -delete 2>&1 | tee -a "$SWEEP_LOG" || true
find "$HOME" -maxdepth 1 -name "comparative-sweep-*" -type d -mtime +$LOG_RETENTION_DAYS -exec rm -rf {} \; 2>/dev/null || true

log ""
log "====================================================================="
log "Nightly sweep done. Exit code: $sweep_exit"
log "Metrics pushed to datastore per ~/.benchmark/benchmark.ini"
log "Filter in OS Dashboards: user-tags.run_type:nightly"
log "====================================================================="

exit $sweep_exit
