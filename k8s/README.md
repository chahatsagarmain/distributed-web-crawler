# Distributed Web Crawler: Kubernetes Deployment Guide

This directory contains all the manifests, Helm configurations, and instructions required to deploy the Distributed Web Crawler natively into a Kubernetes cluster. 

The architecture is divided into three distinct layers:
1. **Infrastructure/Stateful Layer**: Database systems (MongoDB, Redis, RabbitMQ) deployed via Helm charts.
2. **Application Layer**: Custom microservices (Scheduler, Worker) deployed via raw Kubernetes YAML.
3. **Observability Layer**: Prometheus and Service Monitors to gather real-time metrics.

---

## 📂 Directory Structure & File Breakdowns

### `namespaces/`
- **`crawler.yaml`**: Defines the isolated `crawler` namespace. All application and infrastructure components are deployed here to avoid polluting the `default` namespace.

### `configMap/`
- **`envcm.yaml`**: The central configuration hub. It contains all environment variables required by the Scheduler and Worker pods (e.g., `MONGO_URI`, `RABBITMQ_URL`, `REDIS_ADDR`). This is mounted directly into the deployments using `envFrom`.

### `values/` (Helm Configurations)
Instead of manually maintaining complex stateful sets, we use industry-standard Bitnami and Prometheus-Community Helm charts. These `.yaml` files override the default chart configurations to match our crawler's specific needs.
- **`mongodb-values.yaml`**: Configures MongoDB in a `standalone` architecture with custom admin credentials (`admin` / `password`), avoiding the overhead of a full ReplicaSet for local deployment.
- **`rabbitmq-values.yaml`**: Configures RabbitMQ and dynamically installs required plugins (`rabbitmq_consistent_hash_exchange`, `rabbitmq_prometheus`) at boot time.
- **`redis-values.yaml`**: Configures a standalone, passwordless Redis instance for the scheduler's Bloom Filter and job locking mechanisms.
- **`prometheus-values.yaml`**: Strips down the Prometheus installation (disables Alertmanager/NodeExporter) to save local resources, provisions persistent storage, and explicitly configures scraping for RabbitMQ.

### `deployments/`
- **`scheduler.yaml`**: Deploys the central brain of the crawler (1 replica). It listens for new crawl jobs, deduplicates links via Redis, pushes links to RabbitMQ, and saves finished payloads to MongoDB.
- **`worker.yaml`**: Deploys the scalable muscle of the crawler. Workers pull URLs from RabbitMQ, scrape HTML, evaluate `robots.txt`, extract links, and pass data back to the scheduler.
- **`redis-stack.yaml`**: *(Alternative)* A raw deployment manifest deploying `redis-stack-server` which contains the necessary `RedisBloom` modules (for `BF.RESERVE`) if you choose not to use the vanilla Bitnami Helm chart.

### `services/`
- **`scheduler-svc.yaml`**: A `ClusterIP` (or `LoadBalancer`) service that exposes the scheduler's HTTP REST API on port `8080`, allowing users to `POST /crawl` and `POST /stop`. It is labeled with `monitor: enabled`.
- **`worker-svc.yaml`**: A standard `ClusterIP` service routing to the worker pods. Since workers only consume from queues, this service is exclusively used to expose their `:8081` metrics endpoints to Prometheus. It is also labeled with `monitor: enabled`.

### `service-monitor/`
- **`service-monitor.yaml`**: A custom resource (requires Prometheus Operator) that automatically discovers any service with the `monitor: enabled` label and instructs Prometheus to scrape its `metrics` port every 15 seconds. This elegantly links the `scheduler-svc` and `worker-svc` to the observability stack.

---

## 🚀 Deployment Order & Instructions

To ensure all dependencies are met, resources must be applied in a specific order. Ensure you have `kubectl` and `helm` installed.

### Step 1: Base Environment
Create the namespace and inject the central configuration map first.
```bash
kubectl apply -f k8s/namespaces/crawler.yaml
kubectl apply -f k8s/configMap/envcm.yaml
```

### Step 2: Stateful Infrastructure (Helm)
Deploy the databases and message broker. The application pods will crash if these are not running first.
```bash
# Add required Helm repos
helm repo add bitnami https://charts.bitnami.com/bitnami
repo add redis-stack https://redis-stack.github.io/helm-redis-stack/      
helm repo update

# Deploy MongoDB
helm install mongodb bitnami/mongodb -f k8s/values/mongodb-values.yaml -n crawler

# Deploy RabbitMQ
helm upgrade --install rabbitmq oci://registry-1.docker.io/cloudpirates/rabbitmq  -f ./k8s/values/rabbitmq-values.yaml  -n crawler                    

# Deploy Redis (Using Helm)
helm install my-redis redis-stack/redis-stack --values ./k8s/values/redis-values.yaml -n crawler   
```
*Wait a minute or two for these pods to reach the `Running` state before proceeding.*

### Step 3: Application Deployment
Spin up the scheduler, the workers, and their internal networking routes.
```bash
kubectl apply -f k8s/deployments/scheduler.yaml
kubectl apply -f k8s/deployments/worker.yaml
kubectl apply -f k8s/services/scheduler-svc.yaml
kubectl apply -f k8s/services/worker-svc.yaml
```

### Step 4: Observability (Prometheus)
Deploy Prometheus to monitor the queue depths, scraping speeds, and pod health.
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install Prometheus
helm install prometheus prometheus-community/prometheus -f k8s/values/prometheus-values.yaml -n crawler

# Apply the dynamic ServiceMonitor
kubectl apply -f k8s/service-monitor/service-monitor.yaml
```

---

## 🕹️ Operations & Testing

**Starting a Crawl Job:**
Because the scheduler is deployed as an internal `ClusterIP` service, you must port-forward traffic to it to trigger a crawl from your local machine:
```bash
# Open a tunnel to the scheduler
kubectl port-forward svc/scheduler-svc 8080:8080 -n crawler

# In a separate terminal, start the job
curl -X POST http://localhost:8080/crawl \
     -H "Content-Type: application/json" \
     -d '{"url":"https://example.com","depth":2}'
```

**Stopping a Crawl Job:**
```bash
curl -X POST http://localhost:8080/stop
```

**Viewing Metrics:**
```bash
# Open a tunnel to the Prometheus server
kubectl port-forward svc/prometheus-server 9090:80 -n crawler
```
*Navigate to `http://localhost:9090` in your browser to view the live dashboard and metrics targets.*
