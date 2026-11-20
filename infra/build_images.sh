#!/usr/bin/env bash
# =============================================================================
# build_images.sh — Build container images for SRB and import into K3s containerd
#
# Usage:
#   ./infra/build_images.sh [--distribute]
# =============================================================================

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

cd "${REPO_ROOT}"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'
log() { echo -e "${GREEN}[SRB BUILD]${NC} $*"; }

SERVICES=("gt" "rmc" "cancel" "persistence")

log "1. Building service images with docker..."
for s in "${SERVICES[@]}"; do
    log "Building srb-${s}:latest from services/${s}/Dockerfile..."
    docker build -t "srb-${s}:latest" -f "services/${s}/Dockerfile" .
done

log "Building srb-client:latest..."
docker build -t "srb-client:latest" -f "clients/Dockerfile" .

log "Building srb-seed:latest..."
docker build -t "srb-seed:latest" -f "seed/Dockerfile" .

log "2. Exporting images and importing into K3s containerd on this machine..."
IMAGES=("srb-gt:latest" "srb-rmc:latest" "srb-cancel:latest" "srb-persistence:latest" "srb-client:latest" "srb-seed:latest")

mkdir -p /tmp/srb-images
for img in "${IMAGES[@]}"; do
    tar_name="/tmp/srb-images/$(echo "$img" | tr ':' '_').tar"
    log "Saving $img -> $tar_name..."
    docker save "$img" -o "$tar_name"
    log "Importing $tar_name into K3s containerd (k8s.io namespace)..."
    sudo k3s ctr images import "$tar_name" || sudo ctr -n k8s.io images import "$tar_name"
done

log "All images built and imported into local K3s containerd successfully!"

if [[ "${1:-}" == "--distribute" ]]; then
    log "Distributing images to worker nodes (PC2, PC3, PC4)..."
    WORKERS=("10.43.100.175" "10.43.99.199" "10.43.99.206")
    for w in "${WORKERS[@]}"; do
        log "Copying images to estudiante@${w}..."
        for img in "${IMAGES[@]}"; do
            tar_name="/tmp/srb-images/$(echo "$img" | tr ':' '_').tar"
            scp -o StrictHostKeyChecking=no "$tar_name" "estudiante@${w}:/tmp/"
            ssh -o StrictHostKeyChecking=no "estudiante@${w}" "sudo k3s ctr images import $tar_name && rm -f $tar_name"
        done
    done
    log "Image distribution to all nodes complete!"
fi
