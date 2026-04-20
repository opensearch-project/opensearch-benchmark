#!/usr/bin/env bash
#
# Bootstrap a fresh EC2 instance (AL2023 arm64) into a working Milvus
# standalone node for the comparative benchmark. Idempotent — safe to re-run.
#
# Usage (from the new Milvus EC2 box):
#   1. scp this script onto the box
#      scp ~/workplace/opensearch-benchmark/docker/milvus/bootstrap.sh \
#          ec2-user@<ip>:~/
#   2. ssh into the box
#      ssh ec2-user@<ip>
#   3. bash ~/bootstrap.sh
#
# After completion:
#   - Docker installed + running
#   - 'milvus-standalone' container running (embedded etcd, no MinIO — local storage)
#   - Port 19530 (gRPC) and 9091 (REST + healthz) bound
#   - Ready for OSB — no pre-deployed schema, OSB's MilvusCreateIndex runner
#     builds the CollectionSchema + IndexParams at workload startup
#
# Environment variables (optional):
#   MILVUS_VERSION   Milvus Docker image tag to pull. Default: v2.6.13 (matches
#                    the pinned version in Vectorsearch Results.md).
#   WORKDIR          Where to keep Milvus's data + config files.
#                    Default: ~/milvus
#
set -euo pipefail

MILVUS_VERSION="${MILVUS_VERSION:-v2.6.13}"
WORKDIR="${WORKDIR:-$HOME/milvus}"

log() { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[bootstrap ERROR]\033[0m %s\n' "$*" >&2; }

log "Milvus version: $MILVUS_VERSION"
log "Working dir:    $WORKDIR"

# ----- 1. Install Docker (AL2023 arm64) -----
if ! command -v docker >/dev/null 2>&1; then
    log "Installing Docker..."
    sudo dnf install -y docker
    sudo systemctl enable --now docker
    sudo usermod -aG docker ec2-user
    log "Docker installed. NOTE: if this is your first run, log out and back in for group membership to take effect, or use 'sudo docker' for the remaining commands."
fi

if docker ps >/dev/null 2>&1; then
    DOCKER="docker"
else
    log "Current user not in docker group yet — using sudo for this run"
    DOCKER="sudo docker"
fi

# ----- 2. Download standalone_embed.sh (pinned to MILVUS_VERSION) -----
mkdir -p "$WORKDIR"
cd "$WORKDIR"

EMBED_URL="https://raw.githubusercontent.com/milvus-io/milvus/${MILVUS_VERSION}/scripts/standalone_embed.sh"
if [[ ! -f standalone_embed.sh ]]; then
    log "Downloading standalone_embed.sh from Milvus release $MILVUS_VERSION..."
    if ! curl -sfL "$EMBED_URL" -o standalone_embed.sh; then
        err "Failed to download standalone_embed.sh from $EMBED_URL"
        err "Is '$MILVUS_VERSION' a valid git tag in milvus-io/milvus?"
        exit 1
    fi
    chmod +x standalone_embed.sh
else
    log "standalone_embed.sh already present — reusing"
fi

# The script hardcodes the Milvus image tag inside it. If we pinned a specific
# version above, force the script to use that exact image tag (it defaults to
# whatever tag the script author embedded).
if grep -q "milvusdb/milvus:v" standalone_embed.sh; then
    log "Pinning image tag inside standalone_embed.sh to $MILVUS_VERSION"
    sed -i.bak -E "s|milvusdb/milvus:v[0-9.]+(-[a-z0-9]+)?|milvusdb/milvus:${MILVUS_VERSION}|g" standalone_embed.sh
fi

# ----- 3. Start Milvus (or restart if stopped) -----
if $DOCKER ps --format '{{.Names}}' | grep -q '^milvus-standalone$'; then
    log "milvus-standalone container already running — leaving it alone"
elif $DOCKER ps -a --format '{{.Names}}' | grep -q '^milvus-standalone$'; then
    log "milvus-standalone container exists but is stopped — starting it"
    $DOCKER start milvus-standalone
else
    log "Starting fresh milvus-standalone container (this will pull the image on first run)..."
    # standalone_embed.sh uses plain `docker run` (embedded etcd, local storage
    # — no docker-compose dependency), so we just run it. It respects the
    # current user's docker group membership; override by wrapping in sudo if
    # needed.
    if [[ "$DOCKER" == "sudo docker" ]]; then
        sudo bash standalone_embed.sh start
    else
        bash standalone_embed.sh start
    fi
fi

# ----- 4. Wait for healthz endpoint -----
log "Waiting for Milvus health endpoint on :9091..."
for i in {1..60}; do
    code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:9091/healthz 2>/dev/null || echo 000)
    if [[ "$code" == "200" ]]; then
        log "Milvus is healthy."
        break
    fi
    if (( i == 60 )); then
        err "Milvus did not become healthy within 2 minutes (last HTTP code: $code)."
        $DOCKER logs --tail 80 milvus-standalone 2>&1 || true
        exit 1
    fi
    sleep 2
done
sleep 3  # extra settle for gRPC listener

# ----- 5. Sanity-check gRPC port (19530) -----
log "Sanity-checking gRPC port 19530..."
if ! timeout 3 bash -c '</dev/tcp/localhost/19530' 2>/dev/null; then
    err "Port 19530 not accepting TCP connections"
    exit 1
fi
log "Port 19530 is open."

# ----- 6. Optional: verify via pymilvus if installed -----
if python3 -c 'import pymilvus' 2>/dev/null; then
    log "Running a pymilvus sanity check..."
    python3 <<'PY'
from pymilvus import MilvusClient
c = MilvusClient(uri="http://localhost:19530")
try:
    cols = c.list_collections()
    print(f"  pymilvus reports server reachable. Existing collections: {cols}")
finally:
    c.close()
PY
else
    log "pymilvus not installed on this box — skipping SDK sanity check."
    log "That's fine: OSB on the runner box will talk to us over gRPC."
fi

# ----- 7. Report readiness -----
IP_ADDR=$(hostname -I 2>/dev/null | awk '{print $1}')
[[ -z "$IP_ADDR" ]] && IP_ADDR="<this-box-ip>"

log ""
log "Bootstrap complete. Milvus is ready for benchmarking."
log ""
log "From the runner box, point your OSB command at:"
log "  --target-hosts=${IP_ADDR}:19530"
log "  --database-type=milvus"
log ""
log "No schema deploy needed — OSB's MilvusCreateIndex runner creates the"
log "collection (schema + HNSW index) at the start of each benchmark run"
log "using params from params_milvus_1m.json."
log ""
log "Useful commands:"
log "  $DOCKER logs -f milvus-standalone             # follow server logs"
log "  $DOCKER exec milvus-standalone ls /var/lib/milvus  # inspect data dir"
log "  cd $WORKDIR && bash standalone_embed.sh stop  # stop Milvus"
log "  cd $WORKDIR && bash standalone_embed.sh start # restart Milvus"
