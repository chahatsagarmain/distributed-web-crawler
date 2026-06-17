# HTTP API Reference

The Scheduler controller exposes several HTTP endpoints on port `8080` (or the configured REST port) to initiate, manage, and monitor crawl jobs.

---

## Endpoint Summary

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/crawl` | Starts a new asynchronous crawl job from a seed URL |
| `POST` | `/stop` | Gracefully halts the active crawl job and clears backlogs |
| `GET` | `/ping` | Validates health of downstream datastores and services |
| `GET` | `/metrics` | Exports Prometheus metrics |

---

## 1. Start Crawl Job (`POST /crawl`)

Triggers a new crawl process traversing the web starting from the specified seed URL up to a maximum depth.

*   **Content-Type**: `application/json`
*   **Request Parameters**:
    *   `url` (string, **required**): The absolute, fully qualified URL of the page to start crawling from.
    *   `depth` (integer, optional): The maximum depth to traverse. A depth of `0` scrapes only the seed page.
*   **Example Request Body**:
    ```json
    {
      "url": "https://example.com",
      "depth": 2
    }
    ```
*   **Success Response**:
    *   **Code**: `200 OK`
    *   **Content**: `job started` (plain text)
*   **Error Responses**:
    *   **Code**: `400 Bad Request`
        *   **Reason**: Request payload is malformed or the `url` parameter is missing.
    *   **Code**: `429 Too Many Requests`
        *   **Reason**: Another crawling job is already running in the cluster. Only one active job is allowed at a time.
    *   **Code**: `500 Internal Server Error`
        *   **Reason**: Failed to communicate with Redis or RabbitMQ during job lock and queue initialization.

---

## 2. Stop Crawl Job (`POST /stop`)

Forcefully cancels the active crawl job. It deletes the locks in Redis and purges all RabbitMQ queues.

*   **Request Body**: None (Empty)
*   **Success Response**:
    *   **Code**: `200 OK`
    *   **Content**: `{"message": "crawl job stopped successfully"}` (JSON)
*   **Error Responses**:
    *   **Code**: `400 Bad Request`
        *   **Reason**: No active crawl job is currently running to stop.
    *   **Code**: `500 Internal Server Error`
        *   **Reason**: Failed to clear the lock in Redis or failed to purge the RabbitMQ queues.

---

## 3. Service Health Check (`GET /ping`)

Validates the end-to-end connectivity between the Scheduler and its state layers (MongoDB, Redis).

*   **Request Body**: None
*   **Success Response**:
    *   **Code**: `200 OK`
    *   **Content**: `{"message": "pinged"}` (JSON)
*   **Error Responses**:
    *   **Code**: `500 Internal Server Error`
        *   **Reason**: One or more underlying databases (MongoDB or Redis) are unreachable or responding with errors.
        *   **Content**: `{"error": "cant connect to services : <err_msg>"}` (JSON)

---

## 4. Metrics Export (`GET /metrics`)

Exposes standard Prometheus metrics in the native format.

*   **Request Body**: None
*   **Format**: Plain text Prometheus exposition format.
*   **Port**:
    *   Scheduler metrics are served on `:8080/metrics`.
    *   Worker metrics are served on `:8081/metrics`.
