#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define color codes for pretty output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================${NC}"
echo -e "${BLUE}    Distributed Web Crawler Kubernetes Destroyer   ${NC}"
echo -e "${BLUE}====================================================${NC}"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT" || exit 1

echo -e "\n${YELLOW}[*] Checking prerequisites...${NC}"
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}[ERROR] kubectl is not installed or not in PATH.${NC}"
    exit 1
fi
if ! command -v helm &> /dev/null; then
    echo -e "${RED}[ERROR] helm is not installed or not in PATH.${NC}"
    exit 1
fi
echo -e "${GREEN}[OK] All prerequisites found.${NC}"

# Define a function to delete Helm release gracefully
uninstall_helm() {
    local release=$1
    local ns=$2
    if helm status "$release" -n "$ns" &> /dev/null; then
        echo -e "${YELLOW}[*] Uninstalling Helm release: $release from namespace: $ns...${NC}"
        helm uninstall "$release" -n "$ns"
    else
        echo -e "${BLUE}[*] Helm release '$release' not found in namespace '$ns'. Skipping...${NC}"
    fi
}

echo -e "\n${YELLOW}[Step 1] Tearing down Observability Stack (Helm)...${NC}"
uninstall_helm "promtail" "monitoring"
uninstall_helm "loki" "monitoring"
uninstall_helm "prometheus" "monitoring"

echo -e "\n${YELLOW}[Step 2] Tearing down Stateful Infrastructure (Helm)...${NC}"
uninstall_helm "mongodb" "crawler"
uninstall_helm "rabbitmq" "crawler"
uninstall_helm "my-redis" "crawler"

echo -e "\n${YELLOW}[Step 3] Deleting custom resource definitions and monitors...${NC}"
kubectl delete -f k8s/service-monitor/service-monitor.yaml --ignore-not-found=true || true
kubectl delete -f k8s/services/loki-datasource.yaml --ignore-not-found=true || true

# List of Prometheus Operator CRDs
CRD_URLS=(
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_alertmanagers.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_prometheuses.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_thanosrulers.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_probes.yaml"
)

echo -e "${YELLOW}[*] Deleting Prometheus Operator CRDs...${NC}"
for url in "${CRD_URLS[@]}"; do
  kubectl delete -f "$url" --ignore-not-found=true || true
done

echo -e "\n${YELLOW}[Step 4] Deleting Namespaces (this will clean up all remaining resources)...${NC}"

if kubectl get namespace crawler &> /dev/null; then
    echo -e "${YELLOW}[*] Deleting namespace 'crawler'...${NC}"
    kubectl delete namespace crawler --timeout=60s || kubectl delete namespace crawler --grace-period=0 --force
else
    echo -e "${BLUE}[*] Namespace 'crawler' not found. Skipping...${NC}"
fi

if kubectl get namespace monitoring &> /dev/null; then
    echo -e "${YELLOW}[*] Deleting namespace 'monitoring'...${NC}"
    kubectl delete namespace monitoring --timeout=60s || kubectl delete namespace monitoring --grace-period=0 --force
else
    echo -e "${BLUE}[*] Namespace 'monitoring' not found. Skipping...${NC}"
fi

echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}      Teardown / Cleanup Completed Successfully!    ${NC}"
echo -e "${GREEN}====================================================${NC}"
