# Distributed Web Crawler

A highly scalable, concurrent, and distributed web crawling engine written in Go that can scale to scrape Millions of URLs on Kubernetes. This system is designed to traverse the web rapidly, extract data, respect site boundaries (`robots.txt` and domain politeness), and handle massive backlogs of URLs by utilizing a microservice architecture communicating over a message broker (RabbitMQ).

-> Read the blog here : [link](https://medium.com/@chahatsagar2003/handling-millions-of-urls-designing-and-implementing-a-highly-concurrent-distributed-web-crawler-0890357fcfce)

> [!NOTE]
> To deploy and run the crawler using Kubernetes (K8s), please refer to the dedicated [Kubernetes Deployment Guide (k8s/README.md)](file:///D:/distributed-crawler/distributed-web-crawler/k8s/README.md).

---

## 📖 Key Documentation

To make this repository clean and maintainable, detailed topics have been moved to separate documentation files:

*   👉 **[Architecture Guide (ARCHITECTURE.md)](file:///D:/distributed-crawler/distributed-web-crawler/ARCHITECTURE.md)**: Detailed master-worker sequence flow, RedisBloom deduplication engine details, local IP-isolated politeness rate-limiting queue, RabbitMQ consistent hashing topology, and the graceful stop protocol.
*   👉 **[API Reference (API_REFERENCE.md)](file:///D:/distributed-crawler/distributed-web-crawler/API_REFERENCE.md)**: Endpoints specification for `/crawl`, `/stop`, `/ping`, and `/metrics` including request schemas, payload examples, and status codes.
*   👉 **[Kubernetes Deployment Guide (k8s/README.md)](file:///D:/distributed-crawler/distributed-web-crawler/k8s/README.md)**: Operational guide to deploying the entire stateful stack onto Kubernetes using Helm charts.
*   👉 **[Distributed Agents & Operations (AGENTS.md)](file:///D:/distributed-crawler/distributed-web-crawler/AGENTS.md)**: Overview of the scheduler and worker roles, package structure, and basic commands for the stack.

---

## 🏗️ System Architecture & Sequence Flow

The crawler employs a master-worker (Scheduler-Worker) architecture to ensure horizontal scalability, fault tolerance, and data ingestion reliability.

### Architectural Diagram
<img width="4013" height="2742" alt="crawler" src="https://github.com/user-attachments/assets/2aa8ea81-1739-47ea-8da0-8bf00e219c78" />

### 🛡️ Local Politeness & Rotating IP Vision
In production, each worker container can route its requests through rotating proxy networks or have its own public egress IP address. Therefore, instead of throttling hosts globally across all workers (which would limit performance), rate limits are enforced **locally per worker container**. 

This local enforcement uses a lock-free mutex reservation queue. Goroutines query `robots.txt` for `Crawl-delay` (defaulting to 3s), book a scheduled slot in an in-memory map, and sleep in a non-blocking way using Go channels. A background sweeper runs every 1 minute to delete host records that have been inactive for more than 5 minutes, preventing memory growth.

For technical details, see the [Architecture Guide (ARCHITECTURE.md)](file:///D:/distributed-crawler/distributed-web-crawler/ARCHITECTURE.md).

---

## 📂 Precise Project & File Structure

Below is an tree of all files in the project workspace, specifying their individual roles:

```text
distributed-web-crawler/
├── .github/
│   └── workflows/
│       └── test-build-push.yaml      # CI/CD pipeline running Go tests and pushing images to Docker Hub
├── common/                           # SHARED LIBRARY (Go Module: github.com/chahatsagarmain/distributed-web-crawler/common)
│   ├── common.go                     # Handles shared client connection initialization, health checks, and connection monitoring
│   ├── config.go                     # Loads system configuration values and parses environment files via Viper
│   ├── config_test.go                # Unit tests checking environment loading configurations
│   ├── constants.go                  # Contains shared queue names, exchange types, and routing key definitions
│   ├── go.mod                        # Go module specifications for the common library
│   ├── go.sum                        # Cryptographic checksums of dependencies for the common library
│   ├── logger.go                     # Global wrapper setting up structured logging (slog)
│   └── models.go                     # Defines global data structures (CrawlMessage, UrlData, CrawlDocument)
├── grafana/                          # GRAPHICAL OBSERVABILITY PROVISIONING
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboard.yml         # Defines dashboard provisioning providers for local configurations
│       └── datasources/
│           └── datasource.yml        # Automatically binds Prometheus as the default data source
├── k8s/                              # KUBERNETES MANIFESTS & HELM OVERRIDES
│   ├── configMap/
│   │   └── envcm.yaml                # Key-value map mounting environment variables to scheduler and worker pods
│   ├── deployments/
│   │   ├── scheduler.yaml            # Configures deployment for the scheduler pod (singleton brain)
│   │   └── worker.yaml               # Configures scaling replica sets for worker microservice pods
│   ├── namespaces/
│   │   └── crawler.yaml              # Creates the isolated "crawler" Namespace
│   ├── service-monitor/
│   │   └── service-monitor.yaml      # CRD allowing Prometheus Operator to discover and scrape metrics from pods
│   ├── services/
│   │   ├── scheduler-svc.yaml        # Service exposing scheduler APIs (REST endpoints) on port 8080
│   │   └── worker-svc.yaml           # Service exposing worker Prometheus metrics internally on port 8081
│   ├── values/
│   │   ├── mongodb-values.yaml       # Helm configuration overriding MongoDB database settings
│   │   ├── prometheus-values.yaml    # Helm configuration setting Prometheus scraping configurations
│   │   ├── rabbitmq-values.yaml      # Helm configuration enabling consistent hash exchange and plugins
│   │   └── redis-values.yaml         # Helm configuration running Redis Stack to support RedisBloom modules
│   ├── .env                          # Local deployment environmental variable values for Kubernetes
│   ├── .env.example                  # Template configuration file for Kubernetes deployment variables
│   └── README.md                     # Exhaustive operational manual detailing Helm installation steps
├── scheduler/                        # SCHEDULER CONTROLLER (Go Module: github.com/chahatsagarmain/distributed-web-crawler/scheduler)
│   ├── cmd/
│   │   └── scheduler/
│   │       └── main.go               # Scheduler entrypoint starting Web service, result consumer, and watchdog
│   ├── internal/
│   │   ├── bloom/
│   │   │   └── bloom.go              # Wrapper around RedisBloom (BF.RESERVE, BF.EXISTS, BF.ADD) under package cache
│   │   ├── broker/
│   │   │   ├── broker.go             # Declares queues/exchanges, consumes worker responses, and checks job timeouts
│   │   │   └── broker_test.go        # Unit tests verifying broker queues and consistent hash routing bindings
│   │   ├── db/
│   │   │   ├── db.go                 # Buffers scraped documents and does thread-safe InsertMany batch writes to MongoDB
│   │   │   └── redis.go              # Implements active job locks, max depth parameters, and activity tracking
│   │   ├── handlers/
│   │   │   ├── crawl.go              # REST endpoint handler for starting jobs (POST /crawl)
│   │   │   └── stop.go               # REST endpoint handler for stopping jobs gracefully (POST /stop)
│   │   ├── metrics/
│   │   │   └── metrics.go            # Initializes Prometheus counters for URLs queued and scheduling errors
│   │   └── router/
│   │       └── router.go             # Sets up routing table mapping paths to Gin and promhttp handlers
│   ├── dockerfile                    # Multi-stage Dockerfile packaging the Scheduler service
│   ├── go.mod                        # Go module specifications for the scheduler service
│   ├── go.sum                        # Cryptographic checksums of dependencies for the scheduler service
│   ├── scheduler.exe                 # Local built executable binary of the scheduler (for development test)
│   └── cmd.exe                       # Utility executable helper
├── worker/                           # WORKER SCRAPER (Go Module: github.com/chahatsagarmain/distributed-web-crawler/worker)
│   ├── cmd/
│   │   └── main.go                   # Worker entrypoint initiating task consumers and metrics server on port 8081
│   ├── internal/
│   │   ├── broker/
│   │   │   └── broker.go             # Consumes messages from RabbitMQ queues, processes URLs, and publishes results
│   │   ├── crawler/
│   │   │   ├── crawler.go            # Domain scraper resolving absolute paths and cleaning URLs via purell and goquery
│   │   │   └── politeness.go         # Enforces thread-safe rate limits locally per host with background sweeper
│   │   ├── metrics/
│   │   │   └── metrics.go            # Worker metrics counters (pages processed, bytes downloaded, response latency)
│   │   └── robots/
│   │       ├── robots.go             # Handles robots.txt cache validation, user-agent rules, and host checks
│   │       └── robots_test.go        # Unit tests evaluating robots.txt rule matching compliance
│   ├── public/                       # Directory placeholder for public distribution files (empty)
│   ├── dockerfile                    # Multi-stage Dockerfile packaging the Worker service
│   ├── go.mod                        # Go module specifications for the worker service
│   └── go.sum                        # Cryptographic checksums of dependencies for the worker service
├── .env                              # Main workspace runtime variables for database, delay, and queue configurations
├── .env.example                      # Example workspace environment variables template
├── .gitignore                        # Standard file for excluding credentials, temporary builds, and local binaries
├── AGENTS.md                         # Quick architecture overview and summary sheet
├── compose.yaml                      # Multi-container orchestration YAML configuring the full local stack
├── Dockerfile                        # Custom Dockerfile enabling consistent hash exchange plugin for RabbitMQ
├── go.work                           # Go workspace configuration referencing common, scheduler, and worker modules
├── go.work.sum                       # Go workspace checksum dependency resolution registry
├── LICENSE                           # Licensing details (Apache 2.0 / MIT)
├── main.exe                          # Root test compiled executable
└── prometheus.yml                    # Local Prometheus configuration establishing target endpoints and DNS-SD settings
```

---

## 🧪 Step-by-Step Workflow

Follow this workflow to trigger a crawl and query the resulting database records.

### Step 1: Start the services
Ensure all containers are running and healthy:
```bash
docker compose up -d
```

### Step 2: Trigger a Crawl Request
Submit a crawl job starting at `https://example.com` up to a maximum depth of `1`:
```bash
curl -X POST http://localhost:8080/crawl \
     -H "Content-Type: application/json" \
     -d '{"url":"https://example.com","depth":1}'
```
You should receive a `200 OK` response with the body: `job started`.

### Step 3: Inspect Logs
Monitor the workers to see tasks being consumed from RabbitMQ and pages being processed:
```bash
docker compose logs -f worker
```
Wait a few seconds for the Scheduler's database batcher (flushes every 5 seconds or 100 documents) to commit the results.

### Step 4: Query MongoDB
Access the running MongoDB container to inspect the crawled documents in the database:
```bash
# Connect and list crawled URLs and their depths
docker exec -it mongodb mongosh -u admin -p password --eval "use url_db; db.crawled_urls.find({}, {url: 1, depth: 1, has_robots: 1}).pretty()"
```
This should output the crawled documents:
```json
[
  {
    "_id": {"$oid": "6668db86df935c102beab91a"},
    "url": "https://example.com",
    "depth": 0,
    "has_robots": false
  },
  {
    "_id": {"$oid": "6668db8bdf935c102beab91b"},
    "url": "https://example.com/about",
    "depth": 1,
    "has_robots": false
  }
]
```

To fetch the raw scraped HTML content of a specific page, run:
```bash
docker exec -it mongodb mongosh -u admin -p password --eval "use url_db; db.crawled_urls.find({url: 'https://example.com'}, {raw_html: 1}).pretty()"
```

### Step 5: Visual Verification via MongoDB Compass (GUI)
If you prefer a graphical user interface instead of querying the terminal:
1. Open **MongoDB Compass**.
2. Connect using the connection string:
   ```text
   mongodb://admin:password@localhost:27017/?authSource=admin
   ```
3. Once connected, select the **`url_db`** database and the **`crawled_urls`** collection to browse, search, and view crawled web pages and raw HTML.

---

## 📊 Observability & Metrics Setup

The crawler exposes metrics for monitoring performance:

### 1. Exposed Metrics
*   **Scheduler**:
    *   `crawler_urls_queued_total` (Counter): Total URLs successfully pushed to RabbitMQ.
    *   `crawler_scheduling_errors_total` (Counter): Number of scheduling failures.
*   **Worker**:
    *   `crawler_pages_processed_total` (Counter, labeled by status): Total processed pages.
    *   `crawler_http_status_codes_total` (Counter, labeled by code, domain): Frequency of HTTP status codes.
    *   `crawler_page_processing_duration_seconds` (Histogram): Latency of HTTP fetches and DOM parsing.
    *   `crawler_bytes_downloaded_total` (Counter): Volume of raw data scraped.

### 2. Prometheus Target Discovery
In [prometheus.yml](file:///D:/distributed-crawler/distributed-web-crawler/prometheus.yml), we configure target endpoints. For worker discovery, we use DNS Service Discovery (`dns_sd_configs`):
```yaml
  - job_name: 'worker'
    dns_sd_configs:
      - names:
          - 'worker' # Resolves container IP addresses in the Docker Compose network
        type: 'A'
        port: 8081
```

---

## 🚀 Modes of Deployment

This crawler is container-native and designed to run entirely locally or be deployed to production clusters.

### 1. Docker Compose (Local & Quick Start)

The simplest way to run the stack is via Docker Compose. The [compose.yaml](file:///D:/distributed-crawler/distributed-web-crawler/compose.yaml) file natively spins up the databases, the broker, the scheduler, 3 worker replicas, and a full observability stack (Prometheus + Grafana).

**Prerequisites:**
*   Docker & Docker Compose installed.

**Running the Stack:**
```bash
# Start the full stack
docker compose up -d --build
```

**Scale up workers dynamically:**
```bash
docker compose up -d --scale worker=8
```

**Using the Crawler:**
Once the containers are healthy, you can trigger a job via curl:
```bash
curl -X POST http://localhost:8080/crawl \
     -H "Content-Type: application/json" \
     -d '{"url":"https://example.com","depth":2}'
```

To view live metrics of the scrape job, navigate to Grafana at `http://localhost:3000` (Login: `admin` / `admin`).

**Tearing Down:**
```bash
docker compose down -v
```

### 2. Kubernetes (Production & Native)

For robust, production-grade deployments, the system is fully configured for Kubernetes. We utilize Helm charts for the complex stateful databases (MongoDB, RabbitMQ, Redis) and native Kubernetes objects for the microservices.

A dedicated and highly detailed guide for deploying, operating, and configuring the Kubernetes environment is located in the `k8s` directory. You can deploy the entire stack automatically using the script:
```bash
./k8s/deploy.sh
```

👉 **[Read the Kubernetes Deployment Guide here.](file:///D:/distributed-crawler/distributed-web-crawler/k8s/README.md)**

---

## 💻 Native Local Development Setup

To run the codebase locally without containerizing the Go applications:

1.  **Launch Dependencies**: Start MongoDB, Redis (using Redis Stack to ensure Bloom filter support), and RabbitMQ:
    ```bash
    # Run dependencies in background
    docker run -d --name local-mongo -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=password mongo:latest
    docker run -d --name local-redis -p 6379:6379 redis/redis-stack-server:latest
    docker run -d --name local-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
    ```
2.  **Enable RabbitMQ Plugins**:
    ```bash
    docker exec -it local-rabbitmq rabbitmq-plugins enable rabbitmq_consistent_hash_exchange rabbitmq_prometheus
    ```
3.  **Configure Environment**:
    Create a local `.env` file in the project root:
    ```env
    MONGO_URI=mongodb://admin:password@localhost:27017
    REDIS_ADDR=localhost:6379
    REDIS_PASSWORD=
    REDIS_DB=0
    RABBITMQ_URI=amqp://guest:guest@localhost:5672/
    TIME_DELAY=1000
    TTL_JOB=12
    DEFAULT_POLITENESS_DELAY=3000
    ```
4.  **Run Go tests**:
    Ensure the Go compiler (v1.25+) is installed. Run:
    ```bash
    go work sync
    go test ./worker/... ./common/... ./scheduler/...
    ```
5.  **Start the Scheduler**:
    ```bash
    go run ./scheduler/cmd/scheduler/main.go
    ```
6.  **Start the Worker(s)**:
    Set the specific queue name if you want to bind a worker to a single queue, or leave it empty to listen to all queues:
    ```bash
    # Terminal 1: Consume queue_1 tasks
    $env:QUEUE_NAME="queue_1"; go run ./worker/cmd/main.go
    
    # Terminal 2: Consume queue_2 tasks
    $env:QUEUE_NAME="queue_2"; go run ./worker/cmd/main.go
    ```
7.  **Trigger a Crawl**:
    ```bash
    curl -X POST http://localhost:8080/crawl -H "Content-Type: application/json" -d '{"url":"https://example.com","depth":2}'
    ```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps to contribute:

1.  **Fork the repository** and create a feature branch (`git checkout -b feature/amazing-feature`).
2.  **Ensure workspace synchronization**: After modifying `go.mod` files or adding new modules, run:
    ```bash
    go work sync
    ```
3.  **Adhere to Go standards**: Format your code using `go fmt ./...` and lint before submitting.
4.  **Run tests**: Make sure all tests pass:
    ```bash
    go test ./worker/... ./common/... ./scheduler/...
    ```
5.  **Submit a Pull Request** with a detailed description of your changes, reference any open issues, and verify that CI workflows complete successfully.
