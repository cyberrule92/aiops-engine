#!/usr/bin/env bash
# =============================================================================
#  AIOps Intelligence Engine — Kubernetes Bootstrap & Deploy
# =============================================================================
#  Provisions a complete Kubernetes environment and all prerequisites, then
#  builds and deploys the AIOps engine via Helm.
#
#  Handles:
#    - Tooling install (kubectl, helm, k3s/kind, container runtime)
#    - Cluster creation (k3s single-node OR kind multi-node OR use existing)
#    - In-cluster image build & import (no external registry required)
#    - Prometheus + Alertmanager (kube-prometheus-stack)
#    - OpenTelemetry Collector
#    - Ollama (CPU or GPU) + model pull
#    - Namespace, RBAC, storage class checks
#    - Helm install of the AIOps engine
#    - Post-deploy verification & access instructions
#
#  Idempotent: safe to re-run. Detects existing components and skips/upgrades.
#
#  Usage:
#    ./setup-k8s.sh [options]
#
#  Options:
#    --cluster-type   k3s | kind | existing      (default: k3s)
#    --namespace      <ns>                        (default: observability)
#    --release        <name>                      (default: aiops)
#    --image-tag      <tag>                       (default: 1.0.0)
#    --gpu            enable GPU for Ollama        (default: off)
#    --skip-monitoring  skip Prometheus stack install
#    --skip-otel        skip OTEL collector install
#    --skip-build       skip building the backend image
#    --uninstall        tear everything down
#    --dry-run          print actions without executing
#    -h | --help
# =============================================================================

set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Configuration & defaults
# ---------------------------------------------------------------------------
CLUSTER_TYPE="k3s"
NAMESPACE="observability"
MONITORING_NS="monitoring"
RELEASE="aiops"
IMAGE_REPO="aiops-engine"
IMAGE_TAG="1.0.0"
ENABLE_GPU="false"
SKIP_MONITORING="false"
SKIP_OTEL="false"
SKIP_BUILD="false"
UNINSTALL="false"
DRY_RUN="false"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
HELM_CHART_DIR="${PROJECT_ROOT}/helm"
BACKEND_DIR="${PROJECT_ROOT}/backend"

KIND_CLUSTER_NAME="aiops"
K3S_VERSION="v1.30.5+k3s1"
KIND_NODE_IMAGE="kindest/node:v1.30.4"
OLLAMA_MODEL="llama3"

# ---------------------------------------------------------------------------
# Colors & logging
# ---------------------------------------------------------------------------
if [[ -t 1 ]]; then
  C_RESET=$'\033[0m'; C_RED=$'\033[31m'; C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'; C_BLUE=$'\033[34m'; C_CYAN=$'\033[36m'; C_BOLD=$'\033[1m'
else
  C_RESET=""; C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_CYAN=""; C_BOLD=""
fi

log()    { echo "${C_CYAN}[$(date +%H:%M:%S)]${C_RESET} $*"; }
info()   { echo "${C_BLUE}${C_BOLD}▶${C_RESET} $*"; }
ok()     { echo "${C_GREEN}✓${C_RESET} $*"; }
warn()   { echo "${C_YELLOW}⚠${C_RESET} $*" >&2; }
err()    { echo "${C_RED}✗ $*${C_RESET}" >&2; }
fatal()  { err "$*"; exit 1; }
step()   { echo; echo "${C_BOLD}${C_CYAN}━━━ $* ━━━${C_RESET}"; }

run() {
  # Execute (or print in dry-run) a command
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "  ${C_YELLOW}[dry-run]${C_RESET} $*"
  else
    "$@"
  fi
}

trap 'err "Failed at line $LINENO. Command: ${BASH_COMMAND}"' ERR

# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster-type)    CLUSTER_TYPE="$2"; shift 2 ;;
    --namespace)       NAMESPACE="$2"; shift 2 ;;
    --release)         RELEASE="$2"; shift 2 ;;
    --image-tag)       IMAGE_TAG="$2"; shift 2 ;;
    --gpu)             ENABLE_GPU="true"; shift ;;
    --skip-monitoring) SKIP_MONITORING="true"; shift ;;
    --skip-otel)       SKIP_OTEL="true"; shift ;;
    --skip-build)      SKIP_BUILD="true"; shift ;;
    --uninstall)       UNINSTALL="true"; shift ;;
    --dry-run)         DRY_RUN="true"; shift ;;
    -h|--help)         usage ;;
    *) fatal "Unknown option: $1 (use --help)" ;;
  esac
done

IMAGE_FULL="${IMAGE_REPO}:${IMAGE_TAG}"

# ---------------------------------------------------------------------------
# OS / arch detection
# ---------------------------------------------------------------------------
detect_platform() {
  OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
  ARCH="$(uname -m)"
  case "${ARCH}" in
    x86_64|amd64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) fatal "Unsupported architecture: ${ARCH}" ;;
  esac
  log "Platform: ${OS}/${ARCH}"

  SUDO=""
  if [[ "${EUID}" -ne 0 ]] && command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  fi
}

have() { command -v "$1" >/dev/null 2>&1; }

# ---------------------------------------------------------------------------
# Prerequisite installers
# ---------------------------------------------------------------------------
install_kubectl() {
  if have kubectl; then ok "kubectl present ($(kubectl version --client -o yaml 2>/dev/null | grep -m1 gitVersion | awk '{print $2}'))"; return; fi
  info "Installing kubectl..."
  local ver; ver="$(curl -fsSL https://dl.k8s.io/release/stable.txt)"
  run curl -fsSLo /tmp/kubectl "https://dl.k8s.io/release/${ver}/bin/${OS}/${ARCH}/kubectl"
  run chmod +x /tmp/kubectl
  run ${SUDO} mv /tmp/kubectl /usr/local/bin/kubectl
  ok "kubectl ${ver} installed"
}

install_helm() {
  if have helm; then ok "helm present ($(helm version --short 2>/dev/null))"; return; fi
  info "Installing helm..."
  run bash -c 'curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash'
  ok "helm installed"
}

install_kind() {
  if have kind; then ok "kind present"; return; fi
  info "Installing kind..."
  run curl -fsSLo /tmp/kind "https://kind.sigs.k8s.io/dl/v0.24.0/kind-${OS}-${ARCH}"
  run chmod +x /tmp/kind
  run ${SUDO} mv /tmp/kind /usr/local/bin/kind
  ok "kind installed"
}

ensure_docker() {
  if have docker && docker info >/dev/null 2>&1; then ok "Docker available"; return; fi
  warn "Docker not available — required for 'kind' cluster type and image builds."
  if [[ "${OS}" == "linux" ]]; then
    info "Installing Docker via get.docker.com..."
    run bash -c 'curl -fsSL https://get.docker.com | sh'
    run ${SUDO} systemctl enable --now docker || true
    if [[ -n "${SUDO}" ]]; then
      run ${SUDO} usermod -aG docker "${USER}" || true
      warn "Added ${USER} to docker group — you may need to re-login for it to take effect."
    fi
  else
    fatal "Please install Docker Desktop manually on ${OS}."
  fi
}

# ---------------------------------------------------------------------------
# Cluster provisioning
# ---------------------------------------------------------------------------
provision_k3s() {
  if have k3s && ${SUDO} k3s kubectl get nodes >/dev/null 2>&1; then
    ok "k3s already running"
  else
    info "Installing k3s (${K3S_VERSION})..."
    # --disable traefik so we can use our own ingress if desired; keep local-path storage
    run bash -c "curl -fsSL https://get.k3s.io | INSTALL_K3S_VERSION='${K3S_VERSION}' sh -s - \
      --write-kubeconfig-mode 644 \
      --disable traefik"
    ok "k3s installed"
  fi
  # Wire kubeconfig
  export KUBECONFIG="${HOME}/.kube/config"
  run mkdir -p "${HOME}/.kube"
  if [[ "${DRY_RUN}" != "true" ]]; then
    ${SUDO} cp /etc/rancher/k3s/k3s.yaml "${KUBECONFIG}"
    ${SUDO} chown "$(id -u):$(id -g)" "${KUBECONFIG}"
    chmod 600 "${KUBECONFIG}"
  fi
  ok "kubeconfig at ${KUBECONFIG}"
}

provision_kind() {
  ensure_docker
  install_kind
  if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
    ok "kind cluster '${KIND_CLUSTER_NAME}' exists"
    return
  fi
  info "Creating kind cluster '${KIND_CLUSTER_NAME}'..."
  local cfg="/tmp/kind-aiops.yaml"
  if [[ "${DRY_RUN}" != "true" ]]; then
    cat > "${cfg}" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      - containerPort: 30080
        hostPort: 8080
        protocol: TCP
  - role: worker
  - role: worker
EOF
  fi
  run kind create cluster --name "${KIND_CLUSTER_NAME}" --image "${KIND_NODE_IMAGE}" --config "${cfg}"
  ok "kind cluster created"
}

verify_existing() {
  kubectl cluster-info >/dev/null 2>&1 || fatal "No reachable cluster. Set KUBECONFIG or use --cluster-type k3s|kind."
  ok "Using existing cluster: $(kubectl config current-context)"
}

provision_cluster() {
  step "PROVISION CLUSTER (${CLUSTER_TYPE})"
  case "${CLUSTER_TYPE}" in
    k3s)      provision_k3s ;;
    kind)     provision_kind ;;
    existing) verify_existing ;;
    *) fatal "Invalid --cluster-type: ${CLUSTER_TYPE}" ;;
  esac

  info "Waiting for nodes to become Ready..."
  run kubectl wait --for=condition=Ready nodes --all --timeout=180s || warn "Node readiness wait timed out"
  kubectl get nodes -o wide 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Image build & load into cluster (registry-free)
# ---------------------------------------------------------------------------
build_and_load_image() {
  [[ "${SKIP_BUILD}" == "true" ]] && { warn "Skipping image build (--skip-build)"; return; }
  step "BUILD BACKEND IMAGE"
  [[ -f "${BACKEND_DIR}/Dockerfile" ]] || fatal "Dockerfile not found at ${BACKEND_DIR}"

  # Pick a builder: docker or nerdctl
  local builder=""
  if have docker && docker info >/dev/null 2>&1; then builder="docker"
  elif have nerdctl; then builder="nerdctl"; fi
  # In dry-run, assume docker so all steps are visible
  if [[ "${DRY_RUN}" == "true" && -z "${builder}" ]]; then builder="docker"; fi

  case "${CLUSTER_TYPE}" in
    kind)
      [[ -n "${builder}" ]] || fatal "Docker required to build for kind"
      info "Building ${IMAGE_FULL}..."
      run ${builder} build -t "${IMAGE_FULL}" "${BACKEND_DIR}"
      info "Loading image into kind..."
      run kind load docker-image "${IMAGE_FULL}" --name "${KIND_CLUSTER_NAME}"
      ok "Image loaded into kind"
      ;;
    k3s)
      # k3s uses containerd; build then import via 'k3s ctr images import'
      if [[ -n "${builder}" ]]; then
        info "Building ${IMAGE_FULL} with ${builder}..."
        run ${builder} build -t "${IMAGE_FULL}" "${BACKEND_DIR}"
        info "Importing image into k3s containerd..."
        if [[ "${DRY_RUN}" != "true" ]]; then
          ${builder} save "${IMAGE_FULL}" -o /tmp/aiops-image.tar
          ${SUDO} k3s ctr images import /tmp/aiops-image.tar
          rm -f /tmp/aiops-image.tar
        else
          echo "  ${C_YELLOW}[dry-run]${C_RESET} ${builder} save + k3s ctr images import"
        fi
        ok "Image imported into k3s"
      else
        warn "No docker/nerdctl found. Trying to build directly with k3s ctr is not supported."
        fatal "Install Docker, or push '${IMAGE_FULL}' to a registry and set image.repository in values.yaml"
      fi
      ;;
    existing)
      [[ -n "${builder}" ]] || fatal "Docker required to build"
      info "Building ${IMAGE_FULL}..."
      run ${builder} build -t "${IMAGE_FULL}" "${BACKEND_DIR}"
      warn "Existing cluster: ensure '${IMAGE_FULL}' is reachable by your nodes."
      warn "Push to your registry and override image.repository at install time if needed."
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------------------
ensure_namespaces() {
  step "NAMESPACES"
  for ns in "${NAMESPACE}" "${MONITORING_NS}"; do
    if kubectl get ns "${ns}" >/dev/null 2>&1; then
      ok "namespace '${ns}' exists"
    else
      info "Creating namespace '${ns}'..."
      run kubectl create namespace "${ns}"
    fi
  done
}

# ---------------------------------------------------------------------------
# Helm repos
# ---------------------------------------------------------------------------
add_helm_repos() {
  step "HELM REPOSITORIES"
  declare -A repos=(
    [prometheus-community]="https://prometheus-community.github.io/helm-charts"
    [open-telemetry]="https://open-telemetry.github.io/opentelemetry-helm-charts"
  )
  for name in "${!repos[@]}"; do
    if helm repo list 2>/dev/null | awk '{print $1}' | grep -qx "${name}"; then
      ok "repo '${name}' present"
    else
      info "Adding repo '${name}'..."
      run helm repo add "${name}" "${repos[$name]}"
    fi
  done
  info "Updating repos..."
  run helm repo update
}

# ---------------------------------------------------------------------------
# Prometheus + Alertmanager (kube-prometheus-stack)
# ---------------------------------------------------------------------------
install_monitoring() {
  [[ "${SKIP_MONITORING}" == "true" ]] && { warn "Skipping monitoring stack (--skip-monitoring)"; return; }
  step "PROMETHEUS + ALERTMANAGER"

  local values="/tmp/kps-values.yaml"
  if [[ "${DRY_RUN}" != "true" ]]; then
    cat > "${values}" <<EOF
# Lean kube-prometheus-stack for the AIOps engine
grafana:
  enabled: false
prometheus:
  prometheusSpec:
    retention: 6h
    resources:
      requests: { cpu: 200m, memory: 512Mi }
      limits:   { cpu: "1", memory: 1Gi }
    # Allow Prometheus to scrape all ServiceMonitors regardless of labels
    serviceMonitorSelectorNilUsesHelmValues: false
    podMonitorSelectorNilUsesHelmValues: false
alertmanager:
  alertmanagerSpec:
    resources:
      requests: { cpu: 50m, memory: 128Mi }
  config:
    global:
      resolve_timeout: 5m
    route:
      group_by: ['alertname','namespace']
      group_wait: 10s
      group_interval: 1m
      repeat_interval: 4h
      receiver: 'aiops'
      routes:
        - receiver: 'aiops'
          matchers:
            - severity =~ "critical|warning"
    receivers:
      - name: 'aiops'
        webhook_configs:
          - url: 'http://${RELEASE}-aiops-backend.${NAMESPACE}.svc.cluster.local:8000/api/v1/alerts/webhook/prometheus'
            send_resolved: true
nodeExporter:
  enabled: true
kubeStateMetrics:
  enabled: true
EOF
  fi

  info "Installing/upgrading kube-prometheus-stack..."
  run helm upgrade --install kube-prometheus-stack \
    prometheus-community/kube-prometheus-stack \
    --namespace "${MONITORING_NS}" \
    --values "${values}" \
    --wait --timeout 10m
  ok "Monitoring stack ready"

  info "Prometheus svc: prometheus-operated.${MONITORING_NS}:9090"
  info "Alertmanager svc: alertmanager-operated.${MONITORING_NS}:9093"
}

# ---------------------------------------------------------------------------
# OpenTelemetry Collector
# ---------------------------------------------------------------------------
install_otel() {
  [[ "${SKIP_OTEL}" == "true" ]] && { warn "Skipping OTEL collector (--skip-otel)"; return; }
  step "OPENTELEMETRY COLLECTOR"

  local values="/tmp/otel-values.yaml"
  if [[ "${DRY_RUN}" != "true" ]]; then
    cat > "${values}" <<EOF
mode: deployment
image:
  repository: otel/opentelemetry-collector-contrib
config:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317
        http:
          endpoint: 0.0.0.0:4318
  processors:
    batch: {}
    memory_limiter:
      check_interval: 5s
      limit_percentage: 80
      spike_limit_percentage: 25
  exporters:
    # Forward WARN/ERROR logs to the AIOps engine webhook
    otlphttp/aiops:
      logs_endpoint: http://${RELEASE}-aiops-backend.${NAMESPACE}.svc.cluster.local:8000/api/v1/alerts/webhook/otel
    debug:
      verbosity: basic
  service:
    pipelines:
      logs:
        receivers: [otlp]
        processors: [memory_limiter, batch]
        exporters: [otlphttp/aiops, debug]
ports:
  otlp:
    enabled: true
    containerPort: 4317
    servicePort: 4317
  otlp-http:
    enabled: true
    containerPort: 4318
    servicePort: 4318
resources:
  requests: { cpu: 100m, memory: 128Mi }
  limits:   { cpu: 500m, memory: 512Mi }
EOF
  fi

  info "Installing/upgrading OTEL collector..."
  run helm upgrade --install otel-collector \
    open-telemetry/opentelemetry-collector \
    --namespace "${NAMESPACE}" \
    --values "${values}" \
    --wait --timeout 5m
  ok "OTEL collector ready (otlp http: otel-collector-opentelemetry-collector.${NAMESPACE}:4318)"
}

# ---------------------------------------------------------------------------
# GPU operator (only when --gpu)
# ---------------------------------------------------------------------------
install_gpu_support() {
  [[ "${ENABLE_GPU}" != "true" ]] && return
  step "GPU SUPPORT (NVIDIA)"
  if kubectl get nodes -o jsonpath='{.items[*].status.allocatable}' 2>/dev/null | grep -q 'nvidia.com/gpu'; then
    ok "GPU resources already advertised by nodes"
    return
  fi
  warn "No 'nvidia.com/gpu' resources detected on nodes."
  info "Installing NVIDIA device plugin (assumes NVIDIA drivers + container toolkit on nodes)..."
  run kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.16.2/deployments/static/nvidia-device-plugin.yml
  ok "NVIDIA device plugin applied"
}

# ---------------------------------------------------------------------------
# Deploy AIOps engine
# ---------------------------------------------------------------------------
deploy_aiops() {
  step "DEPLOY AIOPS ENGINE"
  [[ -d "${HELM_CHART_DIR}" ]] || fatal "Helm chart not found at ${HELM_CHART_DIR}"

  # Lint first
  info "Linting Helm chart..."
  run helm lint "${HELM_CHART_DIR}" || warn "helm lint reported issues (continuing)"

  # Build --set overrides
  local sets=(
    --set "image.repository=${IMAGE_REPO}"
    --set "image.tag=${IMAGE_TAG}"
    --set "image.pullPolicy=IfNotPresent"
    --set "backend.env.PROMETHEUS_URL=http://prometheus-operated.${MONITORING_NS}:9090"
    --set "backend.env.ALERTMANAGER_URL=http://alertmanager-operated.${MONITORING_NS}:9093"
    --set "backend.env.OTEL_HTTP_ENDPOINT=http://otel-collector-opentelemetry-collector.${NAMESPACE}:4318"
    --set "ollama.model=${OLLAMA_MODEL}"
  )

  if [[ "${ENABLE_GPU}" == "true" ]]; then
    sets+=(
      --set "ollama.nodeSelector.accelerator=nvidia"
      --set "ollama.resources.limits.nvidia\.com/gpu=1"
    )
  fi

  # k3s default storage class is "local-path"; kind uses "standard"
  local sc=""
  sc="$(kubectl get sc -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${sc}" ]]; then
    ok "Default StorageClass: ${sc}"
  else
    warn "No default StorageClass detected — PVCs may stay Pending."
  fi

  info "Installing/upgrading release '${RELEASE}'..."
  run helm upgrade --install "${RELEASE}" "${HELM_CHART_DIR}" \
    --namespace "${NAMESPACE}" \
    "${sets[@]}" \
    --wait --timeout 12m || warn "helm wait timed out — Ollama model pull can take a while; verify below."

  ok "Helm release applied"
}

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
verify_deployment() {
  step "VERIFY"
  [[ "${DRY_RUN}" == "true" ]] && { warn "Dry-run: skipping verification"; return; }

  info "Pods in ${NAMESPACE}:"
  kubectl get pods -n "${NAMESPACE}" -o wide || true
  echo
  info "Services in ${NAMESPACE}:"
  kubectl get svc -n "${NAMESPACE}" || true
  echo

  info "Waiting for backend rollout..."
  kubectl rollout status "deployment/${RELEASE}-aiops-backend" -n "${NAMESPACE}" --timeout=180s || warn "backend not ready yet"
  kubectl rollout status "deployment/${RELEASE}-aiops-frontend" -n "${NAMESPACE}" --timeout=120s || warn "frontend not ready yet"

  # Ollama model pull can be slow
  if kubectl get deploy "${RELEASE}-aiops-ollama" -n "${NAMESPACE}" >/dev/null 2>&1; then
    info "Ollama deployment present (model '${OLLAMA_MODEL}' pulls on first start — may take several minutes)."
  fi

  # Quick health probe via a throwaway pod
  info "Probing backend /api/v1/health ..."
  kubectl run aiops-healthcheck --rm -i --restart=Never \
    --image=curlimages/curl:8.10.1 -n "${NAMESPACE}" -- \
    curl -fsS "http://${RELEASE}-aiops-backend:8000/api/v1/health" 2>/dev/null \
    && ok "Backend healthy" || warn "Backend health probe failed (it may still be starting)"
}

print_access() {
  [[ "${DRY_RUN}" == "true" ]] && return
  step "ACCESS"
  cat <<EOF
${C_GREEN}${C_BOLD}AIOps Engine deployed.${C_RESET}

  ${C_BOLD}UI (port-forward):${C_RESET}
    kubectl port-forward -n ${NAMESPACE} svc/${RELEASE}-aiops-frontend 8080:80
    open http://localhost:8080

  ${C_BOLD}API (port-forward):${C_RESET}
    kubectl port-forward -n ${NAMESPACE} svc/${RELEASE}-aiops-backend 8000:8000
    open http://localhost:8000/api/docs

  ${C_BOLD}Wiring (already configured):${C_RESET}
    Alertmanager → ${RELEASE}-aiops-backend:8000/api/v1/alerts/webhook/prometheus
    OTEL logs    → ${RELEASE}-aiops-backend:8000/api/v1/alerts/webhook/otel
    Prometheus   → prometheus-operated.${MONITORING_NS}:9090

  ${C_BOLD}Logs:${C_RESET}
    kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/component=backend -f
    kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/component=ollama  -f

  ${C_BOLD}Pull Ollama model manually (if needed):${C_RESET}
    kubectl exec -n ${NAMESPACE} deploy/${RELEASE}-aiops-ollama -- ollama pull ${OLLAMA_MODEL}
EOF
}

# ---------------------------------------------------------------------------
# Uninstall
# ---------------------------------------------------------------------------
uninstall_all() {
  step "UNINSTALL"
  warn "Tearing down release '${RELEASE}' and supporting components..."
  run helm uninstall "${RELEASE}" -n "${NAMESPACE}" || true
  run helm uninstall otel-collector -n "${NAMESPACE}" || true
  run helm uninstall kube-prometheus-stack -n "${MONITORING_NS}" || true
  # PVCs are retained by Helm; remove explicitly
  run kubectl delete pvc -n "${NAMESPACE}" -l "app.kubernetes.io/instance=${RELEASE}" || true
  warn "Namespaces '${NAMESPACE}' and '${MONITORING_NS}' left intact (delete manually if desired)."

  case "${CLUSTER_TYPE}" in
    kind)
      read -r -p "Delete kind cluster '${KIND_CLUSTER_NAME}'? [y/N] " ans
      [[ "${ans,,}" == "y" ]] && run kind delete cluster --name "${KIND_CLUSTER_NAME}"
      ;;
    k3s)
      warn "To fully remove k3s: sudo /usr/local/bin/k3s-uninstall.sh"
      ;;
  esac
  ok "Uninstall complete"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  echo "${C_BOLD}${C_CYAN}"
  echo "  ╔═══════════════════════════════════════════════╗"
  echo "  ║        AIOps Engine — K8s Bootstrap           ║"
  echo "  ╚═══════════════════════════════════════════════╝"
  echo "${C_RESET}"
  log "cluster=${CLUSTER_TYPE} ns=${NAMESPACE} release=${RELEASE} image=${IMAGE_FULL} gpu=${ENABLE_GPU} dry-run=${DRY_RUN}"

  detect_platform

  if [[ "${UNINSTALL}" == "true" ]]; then
    install_kubectl; install_helm
    uninstall_all
    exit 0
  fi

  step "INSTALL PREREQUISITES"
  install_kubectl
  install_helm

  provision_cluster
  build_and_load_image
  ensure_namespaces
  add_helm_repos
  install_monitoring
  install_otel
  install_gpu_support
  deploy_aiops
  verify_deployment
  print_access

  echo
  ok "${C_BOLD}All done.${C_RESET}"
}

main "$@"
