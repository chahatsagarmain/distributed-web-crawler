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
echo -e "${BLUE}     Distributed Web Crawler Kubernetes Deployer    ${NC}"
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

echo -e "\n${YELLOW}[Step 1] Deploying Base Environment (Namespace & ConfigMap)...${NC}"
kubectl apply -f k8s/namespaces/crawler.yaml
kubectl apply -f k8s/configMap/envcm.yaml
echo -e "${GREEN}[OK] Base environment applied.${NC}"

echo -e "\n${YELLOW}[Step 2] Setting up Helm repositories...${NC}"
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add redis-stack https://redis-stack.github.io/helm-redis-stack/
helm repo update

echo -e "\n${YELLOW}[Step 2] Deploying Stateful Infrastructure (MongoDB, RabbitMQ, Redis)...${NC}"

echo -e "${BLUE}[*] Deploying MongoDB...${NC}"
helm upgrade --install mongodb bitnami/mongodb \
  -f k8s/values/mongodb-values.yaml \
  -n crawler

echo -e "${BLUE}[*] Deploying RabbitMQ...${NC}"
helm upgrade --install rabbitmq oci://registry-1.docker.io/cloudpirates/rabbitmq \
  -f k8s/values/rabbitmq-values.yaml \
  -n crawler

echo -e "${BLUE}[*] Deploying Redis (using redis-stack)...${NC}"
helm upgrade --install my-redis redis-stack/redis-stack \
  --values k8s/values/redis-values.yaml \
  -n crawler

echo -e "${YELLOW}[*] Waiting 15s for pods to initialize...${NC}"
sleep 15

echo -e "${YELLOW}[*] Waiting for stateful services to be ready (up to 3 minutes)...${NC}"
kubectl wait --namespace crawler \
  --for=condition=ready pod \
  --all \
  --timeout=180s || {
    echo -e "${RED}[WARNING] Stateful pods are taking longer than expected to start.${NC}"
    echo -e "${YELLOW}Continuing with application deployment anyway...${NC}"
}

echo -e "${GREEN}[OK] Stateful infrastructure is ready.${NC}"

# 4. Step 3: Application Deployment
echo -e "\n${YELLOW}[Step 3] Deploying Application Microservices (Scheduler & Worker)...${NC}"
kubectl apply -f k8s/deployments/scheduler.yaml
kubectl apply -f k8s/deployments/worker.yaml
kubectl apply -f k8s/services/scheduler-svc.yaml
kubectl apply -f k8s/services/worker-svc.yaml
echo -e "${GREEN}[OK] Application layer deployed.${NC}"

# 5. Step 4: Observability (Prometheus, Loki, Promtail)
echo -e "\n${YELLOW}[Step 4] Setting up Observability Stack (Prometheus, Loki, Promtail & ServiceMonitor)...${NC}"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

echo -e "${BLUE}[*] Pre-installing Prometheus Operator CRDs...${NC}"
CRD_URLS=(
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_alertmanagers.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_prometheuses.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_thanosrulers.yaml"
  "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/example/prometheus-operator-crd/monitoring.coreos.com_probes.yaml"
)

for url in "${CRD_URLS[@]}"; do
  kubectl apply --server-side -f "$url"
done

echo -e "${BLUE}[*] Deploying Prometheus (kube-prometheus-stack) in monitoring namespace...${NC}"
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
  -f k8s/values/prometheus-values.yaml \
  -n monitoring --create-namespace

echo -e "${BLUE}[*] Deploying Loki in monitoring namespace...${NC}"
helm upgrade --install loki grafana/loki \
  -f k8s/values/loki-values.yaml \
  -n monitoring

echo -e "${BLUE}[*] Deploying Promtail in monitoring namespace...${NC}"
helm upgrade --install promtail grafana/promtail \
  --set config.clients[0].url=http://loki.monitoring.svc.cluster.local:3100/loki/api/v1/push \
  -n monitoring

echo -e "${BLUE}[*] Provisioning Loki datasource in Grafana...${NC}"
kubectl apply -f k8s/services/loki-datasource.yaml

echo -e "${BLUE}[*] Applying ServiceMonitor...${NC}"
kubectl apply -f k8s/service-monitor/service-monitor.yaml
echo -e "${GREEN}[OK] Observability stack deployed.${NC}"

# 6. Summary and Instructions
echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}      Deployment Completed Successfully!            ${NC}"
echo -e "${GREEN}====================================================${NC}"
echo -e "\nTo test the deployment, use the following commands:"
echo -e "${YELLOW}1. Port-forward the Scheduler API:${NC}"
echo -e "   kubectl port-forward svc/scheduler-svc 8080:8080 -n crawler"
echo -e "${YELLOW}2. Trigger a crawl job (in another terminal):${NC}"
echo -e "   curl -X POST http://localhost:8080/crawl -H \"Content-Type: application/json\" -d '{\"url\":\"https://example.com\",\"depth\":2}'"
echo -e "${YELLOW}3. Port-forward Prometheus (optional):${NC}"
echo -e "   kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090 -n monitoring"
echo -e "${YELLOW}4. Port-forward Grafana (optional, admin/admin):${NC}"
echo -e "   kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring"
echo -e "${YELLOW}5. Port-forward MongoDB (optional):${NC}"
echo -e "   kubectl port-forward svc/mongodb 27017:27017 -n crawler"
echo -e "${YELLOW}6. Port-forward RabbitMQ Management UI (optional, guest/guest):${NC}"
echo -e "   kubectl port-forward svc/rabbitmq 15672:15672 -n crawler"
