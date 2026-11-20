#!/usr/bin/env bash
# =============================================================================
# install_k3s.sh — K3s cluster setup for the SRB system (4 Ubuntu 24.04 VMs)
#
# Topology:
#   PC1: 10.43.99.216  (Server / Control-plane, Client pods, Locust host)
#   PC2: 10.43.100.175 (Agent: GT, Cancel service, CockroachDB node 1)
#   PC3: 10.43.99.199  (Agent: RMC workers, CockroachDB node 2)
#   PC4: 10.43.99.206  (Agent: Persistence service, CockroachDB node 3)
#
# Usage:
#   On PC1 (control-plane):
#     ./install_k3s.sh server
#
#   On PC2 (business):
#     ./install_k3s.sh agent 10.43.99.216 <TOKEN> pc2 business
#
#   On PC3 (business / rmc):
#     ./install_k3s.sh agent 10.43.99.216 <TOKEN> pc3 business
#
#   On PC4 (storage):
#     ./install_k3s.sh agent 10.43.99.216 <TOKEN> pc4 storage
# =============================================================================

set -euo pipefail

ROLE="${1:-}"
SERVER_IP="${2:-10.43.99.216}"
TOKEN="${3:-}"
NODE_NAME="${4:-}"
SRB_ROLE="${5:-}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()   { echo -e "${GREEN}[SRB]${NC} $*"; }
warn()  { echo -e "${YELLOW}[SRB WARN]${NC} $*"; }
error() { echo -e "${RED}[SRB ERROR]${NC} $*" >&2; exit 1; }

# Detect primary interface and IP
detect_network() {
    IFACE=$(ip route get 10.43.99.216 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev") print $(i+1)}' | head -1)
    if [[ -z "$IFACE" ]]; then
        IFACE=$(ip route | grep default | awk '{print $5}' | head -1)
    fi
    LOCAL_IP=$(ip -4 addr show "$IFACE" | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -1)
    log "Detected network interface: ${IFACE}, IP: ${LOCAL_IP}"
}

# ── Prerequisites ─────────────────────────────────────────────────────────
install_prerequisites() {
    log "Updating package lists..."
    sudo apt-get update -y

    log "Installing system prerequisites..."
    sudo apt-get install -y \
        curl \
        wget \
        apt-transport-https \
        ca-certificates \
        software-properties-common \
        open-iscsi \
        nfs-common \
        iptables \
        iproute2

    # Disable swap (Kubernetes requirement)
    log "Disabling swap..."
    sudo swapoff -a || true
    sudo sed -i '/ swap / s/^/#/' /etc/fstab || true

    # Enable IP forwarding and bridge netfilter
    log "Enabling IP forwarding and bridge netfilter..."
    sudo modprobe br_netfilter || true
    sudo modprobe overlay || true
    echo 'net.ipv4.ip_forward = 1' | sudo tee /etc/sysctl.d/99-k3s.conf
    echo 'net.bridge.bridge-nf-call-iptables = 1' | sudo tee -a /etc/sysctl.d/99-k3s.conf
    echo 'net.bridge.bridge-nf-call-ip6tables = 1' | sudo tee -a /etc/sysctl.d/99-k3s.conf
    sudo sysctl --system

    detect_network
    log "Prerequisites configured successfully."
}

# ── K3s Server (PC1) ─────────────────────────────────────────────────────
install_server() {
    detect_network
    NODE_IP="${LOCAL_IP}"
    log "Installing K3s server on PC1 (${NODE_IP})..."

    curl -sfL https://get.k3s.io | sh -s - server \
        --write-kubeconfig-mode 644 \
        --disable traefik \
        --disable servicelb \
        --node-name pc1 \
        --node-ip "${NODE_IP}" \
        --advertise-address "${NODE_IP}" \
        --flannel-iface "${IFACE}" \
        --node-label srb-role=control-plane

    log "Waiting for K3s server to become Ready..."
    sleep 8
    mkdir -p "$HOME/.kube"
    sudo cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"
    sudo chown "$(id -u):$(id -g)" "$HOME/.kube/config"
    export KUBECONFIG="$HOME/.kube/config"

    kubectl get nodes

    TOKEN=$(sudo cat /var/lib/rancher/k3s/server/node-token)

    echo ""
    log "================================================================"
    log "K3s SERVER READY ON PC1!"
    log "Server IP:    ${NODE_IP}"
    log "Node Token:   ${TOKEN}"
    log ""
    log "Run these commands on the worker nodes:"
    log "  PC2 (10.43.100.175): ./install_k3s.sh agent ${NODE_IP} ${TOKEN} pc2 business"
    log "  PC3 (10.43.99.199):  ./install_k3s.sh agent ${NODE_IP} ${TOKEN} pc3 business"
    log "  PC4 (10.43.99.206):  ./install_k3s.sh agent ${NODE_IP} ${TOKEN} pc4 storage"
    log "================================================================"
}

# ── K3s Agent (PC2, PC3, PC4) ────────────────────────────────────────────
install_agent() {
    if [[ -z "$SERVER_IP" ]] || [[ -z "$TOKEN" ]]; then
        error "Usage: $0 agent <SERVER_IP> <TOKEN> [NODE_NAME] [SRB_ROLE]"
    fi

    detect_network
    NODE_IP="${LOCAL_IP}"
    NAME="${NODE_NAME:-$(hostname -s)}"
    ROLE_LABEL="${SRB_ROLE:-worker}"

    log "Installing K3s agent (Node: ${NAME}, IP: ${NODE_IP}, Role: ${ROLE_LABEL})..."

    curl -sfL https://get.k3s.io | K3S_URL="https://${SERVER_IP}:6443" \
        K3S_TOKEN="${TOKEN}" \
        sh -s - agent \
        --node-name "${NAME}" \
        --node-ip "${NODE_IP}" \
        --flannel-iface "${IFACE}" \
        --node-label "srb-role=${ROLE_LABEL}"

    log "K3s agent installed and joined cluster!"
    log "Check on PC1: kubectl get nodes -o wide"
}

# ── Label helper ─────────────────────────────────────────────────────────
label_nodes() {
    log "Applying SRB node labels on PC1..."
    kubectl label node pc1 srb-role=control-plane --overwrite || true
    kubectl label node pc2 srb-role=business --overwrite || true
    kubectl label node pc3 srb-role=business --overwrite || true
    kubectl label node pc3 srb-role=rmc --overwrite || true
    kubectl label node pc4 srb-role=storage --overwrite || true
    kubectl get nodes --show-labels
}

# ── Main ──────────────────────────────────────────────────────────────────
case "${ROLE}" in
    server)
        install_prerequisites
        install_server
        ;;
    agent)
        install_prerequisites
        install_agent
        ;;
    labels)
        label_nodes
        ;;
    *)
        echo "Usage: $0 {server | agent <SERVER_IP> <TOKEN> [NODE_NAME] [ROLE] | labels}"
        echo ""
        echo "  server                         Install K3s control-plane (run on PC1)"
        echo "  agent IP TOKEN [NAME] [ROLE]   Install K3s agent and join cluster"
        echo "  labels                         Label all cluster nodes (run on PC1)"
        exit 1
        ;;
esac
