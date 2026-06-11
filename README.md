# Distributed Web Crawler

A highly scalable, concurrent, and distributed web crawling engine written in Go. This system is designed to traverse the web rapidly, extract data, respect site boundaries (`robots.txt`), and handle massive backlogs of URLs by utilizing a microservice architecture communicating over a message broker (RabbitMQ).

> [!NOTE]
> To deploy and run the crawler using Kubernetes (K8s), please refer to the dedicated [Kubernetes Deployment Guide (k8s/README.md)](file:///D:/distributed-crawler/distributed-web-crawler/k8s/README.md).

---

## 🏗️ System Architecture & Sequence Flow

The crawler employs a master-worker (Scheduler-Worker) architecture to ensure horizontal scalability, fault tolerance, and data ingestion reliability.

```mermaid
sequenceDiagram
    autonumber
    actor User as Client / User
    participant Router as router.go (Gin)
    participant HC as handlers/crawl.go
    participant Redis as Redis Stack (Bloom Filter & Lock)
    participant Broker as broker.go (Publisher)
    participant RMQ as RabbitMQ Hashed Exchange
    participant Worker as worker/cmd/main.go
    participant Robots as robots/robots.go
    participant Crawler as crawler/crawler.go
    participant ResultQ as RabbitMQ Result Queue
    participant Consumer as broker.go (Result Consumer)
    participant Batcher as db/db.go (Batcher)
    participant Mongo as MongoDB (Raw HTML)

    User->>Router: POST /crawl {url, depth}
    Router->>HC: Route handler
    HC->>Redis: IsJobActive()
    alt Job is Active
        Redis-->>HC: True
        HC-->>User: 429 Too Many Requests
    else Job is Free
        Redis-->>HC: False
        HC->>Redis: StartJob() [Lock key & Max Depth]
        HC->>Redis: AddToBloom(seedURL)
        HC->>Broker: InsertMessage(seedURL, depth=0)
        Broker->>RMQ: Publish message
        HC-->>User: 200 Job Started
    end

    RMQ->>Worker: Consume message (via Consistent Hash Routing)
    Worker->>Robots: IsAllowed(URL)
    alt Cached or Allowed
        Robots-->>Worker: Allowed (True)
    else Fetch & Parse
        Robots->>User: HTTP GET robots.txt
        User-->>Robots: Parse robots.txt
        Robots-->>Worker: Cache & return status
    end

    Worker->>Crawler: CrawlUrl(URL)
    Crawler->>User: HTTP GET Target URL
    User-->>Crawler: Return HTML Document
    Crawler->>Crawler: goquery & purell normalization
    Crawler-->>Worker: Return UrlData (HTML, Extracted URLs)
    Worker->>ResultQ: Publish Result JSON
    ResultQ->>Consumer: Consume result payload
    Consumer->>Batcher: Buffer result (dbchan)
    alt depth < maxDepth
        loop Extracted URLs
            Consumer->>Redis: CheckUrlDuplicate(nextURL)
            alt Duplicate
                Redis-->>Consumer: True (Skip)
            else Unique
                Consumer->>Redis: AddToBloom(nextURL)
                Consumer->>Broker: InsertMessage(nextURL, depth+1)
                Broker->>RMQ: Publish child message
            end
        end
    end

    loop Every 5s or 100 Docs
        Batcher->>Mongo: InsertMany(Batch)
    end
```

---

## 📂 Precise Project & File Structure

Below is an exhaustive, complete tree of all files in the project workspace, specifying their individual roles:

```text
distributed-web-crawler/
├── .github/
│   └── workflows/
│       └── test-build-push.yaml      # CI/CD pipeline running Go tests and pushing images to Docker Hub
├── common/                           # SHARED LIBRARY (Go Module: github.com/chahatsagarmain/distributed-web-crawler/common)
│   ├── common.go                     # Handles shared client connection initialization and health pings
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
│   │   │   └── crawler.go            # Domain scraper resolving absolute paths and cleaning URLs via purell and goquery
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

## 🔍 Deep Dive into Components & Code Logic

### 1. Scheduler (The "Brain")
The **Scheduler** coordinates the state of the crawler, monitors timeouts, enforces deduplication, and handles data persistence.
*   **Web Server ([router.go](file:///D:/distributed-crawler/distributed-web-crawler/scheduler/internal/router/router.go))**: Sets up Gin-Gonic routes.
    *   `POST /crawl` triggers a new crawl. It validates input parameters, checks if a job is already in progress, sets locks in Redis, registers the seed URL in the Bloom Filter, and publishes the first message to RabbitMQ.
    *   `POST /stop` halts an active crawl job, deletes the keys in Redis, and purges all RabbitMQ queues.
    *   `GET /metrics` exposes system-level metrics, and `GET /ping` validates downstream database health.
*   **Deduplication Engine ([bloom.go](file:///D:/distributed-crawler/distributed-web-crawler/scheduler/internal/bloom/bloom.go))**: Utilizes Redis's `RedisBloom` module (`BF.RESERVE`, `BF.EXISTS`, `BF.ADD`). Standard Redis images lack these commands, necessitating the `redis/redis-stack-server` image.
    *   During startup, it executes `BF.RESERVE url_bloom 0.01 1000000` to establish a Bloom Filter with a 1% false positive probability and a capacity of 1,000,000 items.
    *   `CheckUrlDuplicate` uses `BF.EXISTS` to check if a URL was previously crawled in O(1) time without querying a persistent disk.
*   **Database Ingestion ([db.go](file:///D:/distributed-crawler/distributed-web-crawler/scheduler/internal/db/db.go))**: To optimize MongoDB write I/O, raw HTML files are processed through a concurrent thread-safe buffering pipeline:
    *   `BatchInsert` reads scraped payloads from a Go channel `dbchan`.
    *   If the buffered data reaches `BatchSize (100)`, a background goroutine is spawned to execute `InsertMany` to bulk-insert records into the `crawled_urls` collection.
    *   An independent `time.NewTicker (5s)` guarantees that even during slow crawls, data is flushed to MongoDB.
    *   To conserve storage space, `NextUrls` is set to `nil` before database insertion.
*   **Inactivity Watchdog ([broker.go](file:///D:/distributed-crawler/distributed-web-crawler/scheduler/internal/broker/broker.go))**:
    *   A background daemon runs every 10 seconds.
    *   It passive-checks all queues (`queue_1`, `queue_2`, `queue_3`, and `result_queue`) via `QueueDeclarePassive` to see if their message count is zero.
    *   If all queues are empty, it checks if `time.Now().Unix() - last_activity_time > idleTimeout (300 seconds)`.
    *   If both conditions are met, it triggers `ForceCleanupJob` to release the job lock.

### 2. Worker (The "Muscle")
The **Worker** is stateless and scales horizontally. It pulls URLs from RabbitMQ queues, scrapes HTML documents, extracts links, and reports back.
*   **Task Consumption ([broker.go](file:///D:/distributed-crawler/distributed-web-crawler/worker/internal/broker/broker.go))**:
    *   Workers consume from queues with manual acknowledgements (`autoAck: false`) to implement backpressure.
    *   `ch.Qos(5, 0, false)` configures the channel prefetch count, limiting the worker to fetching a maximum of 5 unacknowledged messages at a time.
    *   For each queue consumed, 5 concurrent worker goroutines are spawned to process messages in parallel.
    *   Once processing completes, `d.Ack(false)` is invoked to remove the message from the queue.
*   **Scraping and Link Normalization ([crawler.go](file:///D:/distributed-crawler/distributed-web-crawler/worker/internal/crawler/crawler.go))**:
    *   Exposes a customized `http.Client` optimized for connection reuse:
        ```go
        transport := &http.Transport{
            MaxIdleConns:        1000,
            MaxIdleConnsPerHost: 100,
            MaxConnsPerHost:     100,
            IdleConnTimeout:     90 * time.Second,
        }
        ```
    *   Parses the HTML response using `goquery` and extracts all links.
    *   Extracted paths are resolved into absolute URLs relative to the parent URL and normalized using the `purell` library. Normalization rules include:
        *   Removing browser fragments (`#`).
        *   Alphabetically sorting query string parameters.
        *   Removing duplicate slashes (`//`).
*   **Robots.txt Compliance ([robots.go](file:///D:/distributed-crawler/distributed-web-crawler/worker/internal/robots/robots.go))**:
    *   Before fetching a URL, the worker checks compliance against the host's `robots.txt` rules using `temoto/robotstxt`.
    *   To avoid overloading target servers, parsed rules are cached in a thread-safe map (`map[string]*robotstxt.Group` protected by a `sync.RWMutex`).
    *   If a request to a host is disallowed, the worker logs the exception and updates the database record with `HasRobots: true` without downloading the page body.

### 3. Common Package (Shared Layer)
*   **Unified Connection Manager ([common.go](file:///D:/distributed-crawler/distributed-web-crawler/common/common.go))**: Centralizes configuration, initialization, connection pooling, and cleanup logic for MongoDB, Redis, and RabbitMQ.
*   **Viper Configuration ([config.go](file:///D:/distributed-crawler/distributed-web-crawler/common/config.go))**: Loads environment variables with defaults. It recursively searches parent directories to locate a `.env` file, enabling the application to find configuration files regardless of the directory from which it is executed.

---

## 📡 API Reference

The Scheduler exposes the following HTTP endpoints on port `8080`:

### 1. Start Crawl Job (`POST /crawl`)
Starts a new, asynchronous crawling process starting from the specified seed URL.

*   **Content-Type**: `application/json`
*   **Request Body Schema**:
    ```json
    {
      "url": "string",
      "depth": "integer"
    }
    ```
    *   `url` (string, **required**): The absolute HTTP/HTTPS URL of the seed page.
    *   `depth` (integer, optional): The maximum traversal depth limit. A depth of `0` scrapes only the seed page.
*   **Example Payload**:
    ```json
    {
      "url": "https://example.com",
      "depth": 2
    }
    ```
*   **Response Codes**:
    *   `200 OK`: The seed URL has been successfully locked, registered in the Bloom Filter, and published. Returns text: `"job started"`.
    *   `400 Bad Request`: The request payload is malformed or the `url` parameter is missing.
    *   `429 Too Many Requests`: Another crawling job is already running in the cluster.
    *   `500 Internal Server Error`: Failed to communicate with Redis/RabbitMQ.

### 2. Stop Crawl Job (`POST /stop`)
Forcefully cancels the running job by purging all queue backlogs and removing database state locks.

*   **Request Body**: None (Empty body)
*   **Response Codes**:
    *   `200 OK`: The active job state was released and all queues successfully flushed.
    *   `400 Bad Request`: No active crawl job was currently running.
    *   `500 Internal Server Error`: Failed to clear active flags in Redis or purge RabbitMQ.

### 3. Service Health Check (`GET /ping`)
Triggers connection checks to the MongoDB, Redis, and RabbitMQ state layers.

*   **Response Codes**:
    *   `200 OK`: All system connections are healthy. Returns JSON: `{"message": "pinged"}`.
    *   `500 Internal Server Error`: One or more underlying datastores are unreachable.

### 4. Metrics Export (`GET /metrics`)
Exposes system-level Prometheus metrics.

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

## 🔀 Queue Topology & Consistent Hashing

Standard load balancing (like round-robin) distributes URLs across workers arbitrarily. This reduces `robots.txt` cache hit rates, as different workers keep fetching the same domain, forcing them to fetch `robots.txt` repeatedly.

To address this, we configure RabbitMQ with the **Consistent Hashing Exchange** plugin (`x-consistent-hash`):

1.  The Scheduler declares a consistent hashing exchange named `consistent_hashing`.
2.  Three queues (`queue_1`, `queue_2`, `queue_3`) are bound to this exchange with a weight routing key of `"1"`.
3.  When publishing, the Scheduler sets the target **URL string** (e.g., `https://example.com/page-1`) as the routing key.
4.  RabbitMQ hashes the routing key and routes the message to the corresponding queue.
5.  Because URLs from the same domain hash to the same queue, a single worker (consuming from that queue) processes all URLs for that domain. This maximizes memory cache hits for `robots.txt` and DNS lookups.

---

## 🛑 The Graceful Stop Protocol

If a user starts a crawl at depth 10 on a large site, millions of URLs can flood the queues. The crawler provides a stop mechanism to halt a running job immediately:

1.  The user sends an HTTP request: `POST http://localhost:8080/stop`.
2.  `handlers/stop.go` executes `db.ForceCleanupJob(rdb)`. This deletes the active job key (`crawler:active_job`) and the max depth key (`crawler:max_depth`) from Redis.
3.  The handler calls `broker.PurgeQueues(ch)`.
4.  The system calls RabbitMQ's `QueuePurge` on `queue_1`, `queue_2`, `queue_3`, and `result_queue`.
5.  This empties the queues. Workers currently processing pages finish their active tasks, but subsequent tasks are discarded as the queues are now empty.
6.  The Scheduler watchdog detects that the queues are empty and terminates the job tracking session.

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

---

### 2. Kubernetes (Production & Native)

For robust, production-grade deployments, the system is fully configured for Kubernetes. We utilize Helm charts for the complex stateful databases (MongoDB, RabbitMQ, Redis) and native Kubernetes objects for the microservices.

A dedicated and highly detailed guide for deploying, operating, and configuring the Kubernetes environment is located in the `k8s` directory.

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