# 🏗️ Event-Driven DDD Microservices Platform with E2E Resilience & Observability

![Microservices](https://img.shields.io/badge/Services-12%20Microservices-blue)
![FastAPI](https://img.shields.io/badge/Framework-FastAPI%20%7C%20FastMCP-009688)
![Kafka](https://img.shields.io/badge/Broker-Apache%20Kafka%20(KRaft)-231F20)
![Observability](https://img.shields.io/badge/Observability-OTel%20%7C%20Prometheus%20%7C%20Grafana%20%7C%20Loki%20%7C%20Jaeger-F46800)
![Updated](https://img.shields.io/badge/Last%20Updated-August%202026-brightgreen)

A production-grade, highly available, resilient, and observable E-Commerce platform built using Python **FastAPI**, **Domain-Driven Design (DDD)**, and **Apache Kafka (KRaft mode)**. The system is designed with a decentralized choreographed Saga pattern and robust traffic protection layers, including high-availability gateway routing, rate-limiting, dual-scope circuit breakers, API idempotency, and a complete distributed observability stack.

---

## 🗺️ 1. Architecture Overview

```mermaid
graph TD
    %% Node Definitions
    Client["Client / Locust Load Tester"]
    Keepalived{"Keepalived Master/Backup VRRP"}
    PartnerWebhook["Partner Store Webhook Endpoint"]

    subgraph GatewayRouting ["API Gateway Routing (Traefik)"]
        Traefik["Traefik Master Load Balancer"]
        TraefikBackup["Traefik Backup Load Balancer"]
        Router["Path-Based Prefix Router"]
    end

    subgraph BoundedContexts ["DDD Bounded Contexts (FastAPI & FastMCP)"]
        UserServ["user-service:8001"]
        ProdServ["product-service:8002"]
        OrdServ["order-service:8003"]
        PayServ["payment-service:8004"]
        RepServ["reporting-service:8005"]
        WebhookServ["webhook-service:8006"]
        SupportServ["support-service:8007 (Agentic RAG)"]
        MCPServ["mcp-service:8008 (Model Context Protocol)"]
        DiscoveryServ["discovery-service:8009 (Product Discovery & Bundles)"]
        CopilotServ["merchant-copilot-service:8010 (ClickHouse Text-to-SQL)"]
        GraphRAGServ["knowledge-graph-rag-service:8011 (GraphRAG & Community Detection)"]
        DisputeServ["dispute-resolution-service:8012 (Multi-Agent Debate & Claims)"]
    end

    subgraph DataCaching ["Data, Vector & Caching Tier"]
        UserDB[("user_db: PostgreSQL")]
        ProdDB[("product_db: PostgreSQL")]
        OrdDB[("order_db: PostgreSQL")]
        PayDB[("payment_db: PostgreSQL")]
        RepDB[("reporting_db: PostgreSQL")]
        WebhookDB[("webhook_db: PostgreSQL")]
        ClickHouseDB[("clickhouse: Columnar OLAP")]
        QdrantDB[("qdrant: Vector Database")]
        Redis[("redis: Redis 7")]
    end


    subgraph EventBroker ["Asynchronous Event Broker"]
        Kafka[("Apache Kafka KRaft Broker")]
    end

    subgraph Observability ["Distributed Observability Stack"]
        OTel["OTel Collector:4317"]
        JG["Jaeger UI:16686"]
        PR["Prometheus:9090"]
        LK["Loki:3100"]
        GF["Grafana:3000"]
        AM["Alertmanager:9093"]
    end

    %% Gateway Routing Connections
    Client -->|"Port 80 (Virtual IP)"| Keepalived
    Client -.->|"Direct Checkouts"| Keepalived
    Keepalived -->|"Active Ingress"| Traefik
    Keepalived -.->|"Failover Ingress"| TraefikBackup
    Traefik -->|"Rate Limiting & Load Shedding"| Router
    TraefikBackup -->|"Rate Limiting & Load Shedding"| Router

    %% Bounded Contexts Connections
    Router -->|"/users/*"| UserServ
    Router -->|"/products/*"| ProdServ
    Router -->|"/orders/*"| OrdServ
    Router -->|"/payments/*"| PayServ
    Router -->|"/reporting/*"| RepServ
    Router -->|"/webhooks/*"| WebhookServ
    Router -->|"/support/*"| SupportServ
    Router -->|"/mcp/*"| MCPServ
    Router -->|"/discovery/*"| DiscoveryServ
    Router -->|"/copilot/*"| CopilotServ
    Router -->|"/graphrag/*"| GraphRAGServ
    Router -->|"/disputes/*"| DisputeServ

    %% Database, OLAP & Vector Connections
    UserServ -->|"db_breaker"| UserDB
    ProdServ -->|"db_breaker"| ProdDB
    OrdServ -->|"db_breaker"| OrdDB
    PayServ -->|"db_breaker"| PayDB
    RepServ -->|"db_breaker"| RepDB
    WebhookServ -->|"db_breaker"| WebhookDB
    SupportServ -->|"qdrant_breaker"| QdrantDB
    DiscoveryServ -->|"qdrant_breaker"| QdrantDB
    CopilotServ -->|"qdrant_breaker"| QdrantDB
    CopilotServ -->|"MicroBatcher Insert"| ClickHouseDB
    GraphRAGServ -->|"qdrant_breaker"| QdrantDB
    GraphRAGServ -->|"In-Memory Graph"| NetworkXGraph[("NetworkX MultiDiGraph")]
    DisputeServ -->|"qdrant_breaker"| QdrantDB
    DisputeServ -->|"Resilient Breaker"| GraphRAGServ
    DisputeServ -->|"Fraud Analytics"| ClickHouseDB

    %% Idempotency Cache Connections
    UserServ -->|"Idempotency Cache"| Redis
    ProdServ -->|"Idempotency Cache"| Redis
    OrdServ -->|"Idempotency Cache"| Redis
    DisputeServ -->|"Idempotency Cache"| Redis
    PayServ -->|"Idempotency Cache"| Redis
    RepServ -->|"Idempotency Cache"| Redis
    WebhookServ -->|"Idempotency Cache"| Redis

    %% Kafka Event Broker Connections
    UserServ -.->|"kafka_breaker"| Kafka
    ProdServ -.->|"kafka_breaker"| Kafka
    OrdServ -.->|"kafka_breaker"| Kafka
    PayServ -.->|"kafka_breaker"| Kafka
    RepServ -.->|"kafka_breaker"| Kafka
    WebhookServ -.->|"kafka_breaker"| Kafka

    Kafka -.->|"Event Subscription / Inbox Pattern"| UserServ
    Kafka -.->|"Event Subscription / Inbox Pattern"| ProdServ
    Kafka -.->|"Event Subscription / Inbox Pattern"| OrdServ
    Kafka -.->|"Event Subscription / Inbox Pattern"| PayServ
    Kafka -.->|"Event Subscription / Inbox Pattern"| RepServ
    Kafka -.->|"Event Subscription / Inbox Pattern"| WebhookServ
    Kafka -.->|"Real-Time Embedding Sync"| DiscoveryServ
    Kafka -.->|"Micro-Batcher Stream Ingestion"| CopilotServ
    Kafka -.->|"Dynamic Entity Hydration"| GraphRAGServ

    %% Webhook Outbound Connection
    WebhookServ -->|"POST Resilient Webhook Dispatch"| PartnerWebhook

    %% Observability Connections
    UserServ -->|"OTel Traces & Metrics"| OTel
    ProdServ -->|"OTel Traces & Metrics"| OTel
    OrdServ -->|"OTel Traces & Metrics"| OTel
    PayServ -->|"OTel Traces & Metrics"| OTel
    RepServ -->|"OTel Traces & Metrics"| OTel
    WebhookServ -->|"OTel Traces & Metrics"| OTel
    SupportServ -->|"OTel Traces & Metrics"| OTel
    MCPServ -->|"OTel Traces & Metrics"| OTel
    DiscoveryServ -->|"OTel Traces & Metrics"| OTel
    CopilotServ -->|"OTel Traces & Metrics"| OTel
    GraphRAGServ -->|"OTel Traces & Metrics"| OTel
    Traefik -->|"OTel Traces & Metrics"| OTel

    OTel -->|"Traces"| JG
    OTel -->|"Metrics"| PR
    OTel -->|"Logs"| LK
    PR -->|"Dashboards"| GF
    PR -->|"Alerts"| AM
```

---

## 🛡️ 2. Resilience, Protection & Reliability Features

The system implements multi-layered protection to guarantee extreme reliability, operational stability, and self-healing behavior under stress or service degradation:

### A. Ingress Gateway Protection (Traefik)
- **High Availability**: Programmed with active-passive **Keepalived VRRP** clustering (Virtual IP `172.20.0.100`), ensuring instantaneous, zero-downtime failover between `traefik-master` and `traefik-backup`.
- **Rate-Limiting**: Limits average traffic to 30 requests/sec with a burst tolerance of 50 requests. Sudden volume spikes are throttled instantly, returning a `429 Too Many Requests` response to safeguard downstream services.
- **Load-Shedding**: Capped at 50 concurrent in-flight requests. Excess traffic is shed gracefully, preventing resource exhaustion.

### B. Dual-Scope & Database Circuit Breakers
Implemented via a custom async-native `AsyncCircuitBreaker` wrapper (`shared/common/resilience.py`) for complete operational safety:
1. **`db_breaker` (Internal Persistence Protection)**: Applied to every database interaction. If a PostgreSQL instance encounters three consecutive failures (socket timeout, database offline, lock contention), the circuit trips to `OPEN`. Downstream transactions immediately fast-fail with a custom `503 Service Unavailable`, preventing connection-pool deadlock.
2. **`kafka_breaker` (Broker Connectivity Protection)**: Protects event publishing. Prevents background threads from blocking if the Kafka broker experiences transient disconnects or partition rebalances.
3. **Resilient HTTP Client**: Built-in HTTP client wrapper (`shared/common/http_client.py`) automatically wraps external REST calls in a dedicated circuit breaker with exponential backoff retries.
4. **Self-Healing Recovery State Machine**: When the circuit trips, the breaker enters `OPEN` state. After a 15-second cooling period, the next transaction probe transitions the state to `HALF-OPEN`. A successful interaction fully closes the circuit back to `CLOSED`; any failure trips it back to `OPEN`.

### C. End-to-End REST API Idempotency (Redis-Backed)
- Implemented via a high-performance `@idempotent_api` decorator (`shared/common/idempotency.py`).
- Requires mutating requests (`POST`) to contain a unique `X-Idempotency-Key` header.
- Upon receiving a request, the service checks Redis. If the key exists, it returns the cached response instantly, skipping database persistence and business execution. If not, it executes the operation, caches the result in Redis with a TTL, and returns.

### D. Asynchronous Event Consumer Idempotency (Inbox Pattern)
- Event-driven platforms are susceptible to duplicate events due to broker partition rebalancing or network retries.
- Consumers verify and record event IDs in a single atomic transaction. Duplicate events are silently discarded, guaranteeing that stock levels and order statuses are updated exactly once.

### E. Transactional Outbox Pattern (Event-Publishing Resilience)
- **The Issue**: In standard event-driven systems, writing to the database and publishing to Kafka are separate operations. If Kafka goes down or a network timeout occurs right after the database transaction commits, the event is lost. Conversely, if you publish the event first and the DB commit fails, you dispatch a "ghost" event.
- **The Solution**: We implement the **Transactional Outbox Pattern**. When a service updates its database (e.g., creating an order or reserving stock), it writes the event payload into a local `outbox_messages` table within the **same atomic database transaction** (implemented in `shared/common/outbox.py`).
- **Self-Healing & Decoupled Uptime**: A background `OutboxPublisher` task continuously polls the local database table, publishes pending messages to Kafka, and deletes them upon success. If Kafka goes down (tripping the `kafka_breaker`), the API write still succeeds instantly, buffering the messages in PostgreSQL. Once Kafka recovers, the background publisher drains the queue automatically.

### F. Read Fallbacks (Redis Cache-Aside) for Degraded Reads
- **Resilient Fallback**: To preserve system readability during database outages, we implement a Redis-backed cache-aside fallback system. 
- **`@cache_fallback` Decorator**: Applied to `GET` endpoints (e.g. `/products/{id}`, `/users/{id}`). Upon a database lookup failure (or if the database circuit breaker is in `OPEN` state), the decorator intercepts the call, reads the cached DTO from Redis (populated with a 5-minute TTL on prior successful DB reads), and returns it to the client with `200 OK`. 
- **Write Fail-Fast Validation**: While `GET` read endpoints bypass database circuit breaker checks via cache fallbacks, mutating write endpoints (`POST`, `PUT`, `DELETE`) continue to fail-fast immediately if the database breaker is open.

### G. Consumer Retry Loop & Dead Letter Queue (DLQ) Pipeline
- **Transient vs. Non-Transient Failures**: In the background event consumers, exceptions are evaluated by a robust classification function (`is_retriable_exception` in `shared/common/resilience.py`).
  - **Retriable Failures**: Transient issues like network dropouts, database socket timeouts, name resolution errors, or downstream HTTP `5xx` status codes. These are safely retried locally up to 3 times with exponential backoff (e.g. 1s, 2s, 4s).
  - **Non-Retriable Failures**: Programming bugs, schema validation errors (`ValidationError`), database constraint violations (`IntegrityError`), and HTTP `4xx` client errors. These bypass retries entirely to avoid head-of-line partition blocking.
- **Dead Letter Queue (DLQ) Routing**: If all retries for a retriable failure are exhausted, or if a non-retriable failure occurs, the consumer wraps the message in a diagnostic envelope containing metadata (original topic, consumer group, failure timestamp, exception class, error message, and complete traceback stack trace) and routes it to a `.deadletter` topic (e.g. `order.created.deadletter`), then commits the partition offset to keep the stream moving.

### H. DLQ Replay CLI Utility
- **Replay Capabilities**: A dedicated command-line tool `shared/bin/replay_dlq.py` is included to recover from dead-letter failures.
- **Watermark Boundaries**: When executed, it scans DLQ topics, reads partition high watermarks to determine the batch boundaries (preventing infinite tail-chasing loops), reserializes the original payload, and republishes the event back into its original Kafka topic. It then commits the replay consumer offsets, advancing the DLQ pointer.

### I. Observability & Dashboard Metrics
- **Prometheus Metrics**: The consumer retry and DLQ pipelines are instrumented with dedicated metrics:
  - `messaging_consumer_retries_total` (counter, labels: `topic`, `consumer_group`, `attempt`): Tracks individual consumer retry attempts.
  - `messaging_dlq_routed_total` (counter, labels: `original_topic`, `consumer_group`, `error_class`): Tracks message counts directed to DLQs.
  - `messaging_process_duration_seconds` (histogram, labels: `topic`): Tracks consumer execution latency.
- **Enhanced Grafana Dashboard**: We provisioned new telemetry panels in the **"Consumer Retries & Dead Letter Queues (DLQ)"** row of the "Transactional Outbox & Read Resiliency" dashboard:
  - **DLQ Routed Messages Rate**: Real-time rate of messages arriving in DLQs.
  - **Consumer Callback Retry Attempts Rate**: Active retry rate per topic and attempt sequence.
  - **Unreplayed DLQ Backlog (Messages)**: Monitored via consumer group lag (`kafka_consumergroup_lag`) of the replay group on `.deadletter` topics. This value automatically drops to 0 when replayed.
  - **Consumer Callback Execution Time (Avg)**: Computes the average execution time of consumer callbacks to detect lag bottlenecks.

### J. Resilient Webhook Delivery Service (Store Webhooks)
- **Zero-Lookup Materialized View**: The `webhook-service` subscribes to `store.registered` events and materializes store webhook configurations locally in a PostgreSQL read model database (`materialized_stores`). When processing `order.confirmed` events, it resolves target webhook destinations without making synchronous API gateway requests to `product-service`.
- **Two-Tiered Partition Isolation (Famous vs. Small Stores)**:
  To provide absolute isolation between different stores, we provision **8 partitions** on the `order.confirmed` topic and segment traffic by store popularity:
  * **Famous Stores (`is_famous = True`)**: Routed dynamically to dedicated partitions `0 to 3` using `store_id % 4`.
  * **Small Stores (`is_famous = False`)**: Routed dynamically to shared partitions `4 to 7` using `4 + (store_id % 4)`.
- **Per-Store Circuit Breaker & Outage Routing Scenarios**:
  Each store is assigned an isolated in-memory circuit breaker. If a webhook target fails continuously (e.g., HTTP `5xx` or timeouts), the breaker trips to `OPEN`, triggering one of two partition scenarios:
  * **Famous Store Outage Scenario (Dedicated Partitions)**:
    - The Kafka consumer **pauses** the specific dedicated partition (`TopicPartition`) assigned to the store.
    - Because the partition is dedicated, pausing it applies backpressure to the broker for this store's events only, leaving all other famous and small stores completely unaffected.
    - A background health probe pings the store's webhook. Once healthy, the breaker resets to `CLOSED`, the consumer **resumes** partition polling, and the failing messages (safely seeked back to their original offsets) are re-read and delivered.
  * **Small Store Outage Scenario (Shared Partitions)**:
    - The Kafka consumer **does not pause** the partition (since pausing it would cause head-of-line blocking for other healthy small stores sharing the partition).
    - Instead, the consumer fast-fails the event directly to the `webhook.deadletter` DLQ and commits the partition offset, keeping the shared pipeline flowing.
- **Dead Letter Queue (DLQ) Routing**: If a webhook target throws a non-retriable exception (e.g., `400 Bad Request` or `404 Not Found`), the message skips retries and circuit breaking, and is immediately archived in the `webhook.deadletter` Kafka topic.

#### Webhook Service Resilient Flow Diagram:

```mermaid
graph TD
    OC_Event["order.confirmed Event"] --> Consumer["Kafka Consumer Loop"]
    Consumer --> InboxCheck{"Inbox Deduplication?<br/>(idempotent_consumers)"}
    InboxCheck -- "Yes (Duplicate)" --> Ack["Commit Offset & Skip"]
    InboxCheck -- "No" --> StoreFetch["Query Local Materialized Store Table<br/>(materialized_stores)"]
    
    StoreFetch --> StoreExist{"Store Webhook Configured?"}
    StoreExist -- "No" --> DLQ["Send to DLQ<br/>(webhook.deadletter)"] --> Ack
    StoreExist -- "Yes" --> BreakerCheck{"Circuit Breaker State?"}
    
    BreakerCheck -- "OPEN" --> FamousCheckOpen{"Is Famous Store?"}
    FamousCheckOpen -- "Yes" --> PausePart["Pause Kafka Partition<br/>Start Health Probe"] --> SeekBack["Seek Back to Message Offset"]
    FamousCheckOpen -- "No" --> DLQ
    
    BreakerCheck -- "CLOSED / HALF-OPEN" --> Dispatch["POST Dispatch to Store Webhook"]
    
    Dispatch --> DispatchSuccess{"HTTP Status < 400?"}
    DispatchSuccess -- "Yes (2xx)" --> LogSuccess["Log Success in DB"] --> Ack
    DispatchSuccess -- "No (5xx / Timeout)" --> RecordFail["Log Attempt Fail in DB"] --> BreakerTrip{"Threshold Met?<br/>(3 failures)"}
    
    BreakerTrip -- "Yes" --> FamousCheckTrip{"Is Famous Store?"}
    FamousCheckTrip -- "Yes" --> TripOpen["Trip Breaker to OPEN"] --> PausePart
    FamousCheckTrip -- "No" --> TripOpenSmall["Trip Breaker to OPEN"] --> DLQ
    
    BreakerTrip -- "No" --> SleepRetry["Sleep 2s & Seek Back"]
    
    DispatchSuccess -- "No (4xx Client Error)" --> RecordFailNonRetriable["Log Non-Retriable Fail in DB"] --> DLQ
```

### K. Bloom Filter Cache Penetration & Identity Protection
- **The Threat (Cache Penetration)**: Malicious scrapers or bots querying high volumes of non-existent entity IDs (e.g. `GET /products/999999` or `GET /users/888888`) completely bypass the Redis cache, forcing repeated, expensive SQL full-index scans against PostgreSQL.
- **The Solution**: We implement mathematically optimized **Bloom Filters** (`shared/common/bloom.py` and `algorithms/bloom_filter.py`) backed by in-memory bit arrays and distributed Redis bitsets:
  - **`product-service`**: Maintains `product_bloom_filter` pre-warmed on boot. When non-existent IDs are requested, the `@bloom_guard` decorator intercepts the request at the edge in **$< 0.01\text{ms}$**, immediately returning `404 Not Found` without touching PostgreSQL or Redis.
  - **`user-service`**: Employs `user_id_bloom_filter` for profile lookup protection and `user_identity_bloom_filter` for $O(1)$ email/username uniqueness validation during user registration (`POST /api/v1/users`).

### L. Log-Structured Merge-Tree (LSM) Storage Engine & Event Deduplication
- **The Write-Heavy Challenge**: Webhook delivery audit logs (`webhook-service`) produce tens of thousands of writes per second (request payload, response body, latency, status code). Writing these directly to PostgreSQL causes table bloat, index lock contention, and heavy vacuuming overhead.
- **The LSM Engine Solution**: We implement a complete **LSM-Tree Storage Engine** (`algorithms/lsm_tree.py` and `services/webhook-service/src/infrastructure/lsm_storage.py`):
  - **Sequential Writes**: Appends logs sequentially to `wal.log` on disk and buffers them in RAM (`MemTable`), absorbing writes in **$< 0.05\text{ms}$**.
  - **Immutable SSTables with Embedded Bloom Filters**: When `MemTable` fills up, it flushes sorted, immutable `SSTable` files to disk, each embedding its own Bloom filter to skip disk blocks during point lookups (`GET /webhooks/lsm-logs/{key}`).
  - **Background Merge Compaction**: Periodically merge-sorts overlapping SSTables, cleans up tombstones, and reclaims disk space.
  - **Event Deduplication**: Fast-path `dedup_bloom` filter suppresses duplicate Kafka events in RAM before issuing SQL queries.

---

## 🏬 3. Multi-Tenancy Architecture (Schema-per-Tenant Model)

The platform implements a production-ready **Multi-Tenant Architecture** using PostgreSQL **Schema-per-Tenant isolation** (Marketplace Model). Each store/tenant operates inside its own isolated database schema (e.g. `store_tech`, `store_alpha`), while global resources (like universal user accounts) and connection pools are safely shared.

```
Incoming Request (HTTP / REST)
       │  Header: X-Tenant-ID: store_tech
       ▼
[TenantMiddleware] ──(Check/Cache)──► [TenantRegistry (public.tenants)]
       │
       ├─► Sets Request State & ContextVar (TenantContext: slug="store_tech")
       │
       ├─► [Database Session]: SET LOCAL search_path TO store_tech, public
       │     └─► Executes SQL inside tenant's isolated tables
       │
       ├─► [ResilientHTTPClient]: Injects X-Tenant-ID into downstream calls
       │     └─► Propagates tenant context to user-service & product-service
       │
       └─► [Redis / Cache]: Namespaces keys -> tenant:store_tech:idem:...
```

### Key Multi-Tenancy Components

| Component | File Location | Responsibility |
|---|---|---|
| **Tenant Context** | `shared/common/tenant.py` | Immutable `TenantContext` data class and `ContextVar` management for async task scoping. |
| **Tenant Middleware** | `shared/common/tenant_middleware.py` | Validates `X-Tenant-ID` header against `TenantRegistry`, rejecting missing/invalid tenants with `400`/`404`. |
| **Tenant Registry** | `shared/common/tenant_registry.py` | Maintains metadata in `public.tenants` with thread-safe async cache-miss database lookups. |
| **Dynamic Provisioner** | `shared/common/tenant_provisioner.py` | Programmatically executes Alembic migrations (`run_migrations_for_schema`) on-demand when new tenants are created (`POST /admin/tenants`). |
| **Connection Pool Safety** | `shared/common/database.py` | Executes `SET LOCAL search_path TO <slug>, public` inside transactions so PostgreSQL connection pools are reused without cross-tenant schema leakage. |
| **Inter-Service Propagation** | `shared/common/http_client.py` | Automatically injects `X-Tenant-ID` and W3C `traceparent` headers into all downstream REST calls. |
| **Redis Namespacing** | `shared/common/idempotency.py` | Automatically prefixes idempotency keys and cache keys with `tenant:<slug>:...`. |

### Provisioning a New Tenant Dynamically

To create and migrate a brand-new tenant on the fly:
```bash
curl -X POST http://localhost/products/admin/tenants \
     -H "Content-Type: application/json" \
     -d '{
           "slug": "store_gaming",
           "name": "Gaming Superstore",
           "owner_email": "owner@gaming.com"
         }'
```
This automatically creates the `store_gaming` schema and applies all database migrations instantly.

---

## 🏢 4. DDD Bounded Contexts & Clean Architecture

Each microservice is a self-contained bounded context strictly isolating its domain, Ubiquitous Language, and database schema, conforming to clean architecture standards:

```
[Presentation Layer]  <-- API routers, Request/Response schemas
       │
       ▼
[Application Layer]   <-- Use Cases, Commands, Handlers, Application Services
       │
       ▼
  [Domain Layer]      <-- Aggregate Roots, Entities, Value Objects, Domain Events
       ▲
       │
[Infrastructure Layer]<-- ORM Models, DB migrations, Settings & Config
       ▲
       │
 [Adapter Layer]      <-- Repositories (SQLAlchemy), Event Pub/Sub adapters
```

### The 5 Layers in Detail

| Layer | Responsibility | Key Component Examples |
| :--- | :--- | :--- |
| **Domain** | Contains the enterprise business logic, entities, aggregates, validation rules, and domain events. Zero external dependencies. | `User` Entity, `Product` Aggregate, `Order` Aggregate, `DomainException` |
| **Application** | Orchestrates the domain objects to execute specific use cases. Translates external inputs into commands. | `OrderApplicationService`, `ConfirmOrderCommand` |
| **Infrastructure** | Integrates databases, framework components (FastAPI setup), configuration settings, and OpenTelemetry. | `db_setup.py`, `config.py`, SQLAlchemy tables |
| **Presentation** | Exposes HTTP routes, handles JSON serialization/deserialization, and maps HTTP requests to Pydantic schemas. | `api.py` (FastAPI Routers), `RegisterUserRequest` Schema |
| **Adapter** | Implements domain repository interfaces (SQLAlchemy persistence) and maps message broker events (Kafka). | `SQLAlchemyOrderRepository`, `OrderMessagingPublisher` |

### Bounded Context Directory Layout
```
system_design/
├── docker-compose.yml                  # Core cluster (databases, Kafka KRaft, microservices)
├── docker-compose.observability.yml    # Telemetry cluster (Prometheus, Grafana, Jaeger, Loki, OTel)
├── shared/                             # Common code packages shared between services
│   ├── contracts/
│   │   └── events.py                   # Pydantic models for shared integration events
│   └── common/
│       ├── database.py                 # Async SQLAlchemy DB connection helper
│       ├── messaging.py                # Resilient async aiokafka Kafka manager wrapper
│       ├── resilience.py               # Central AsyncCircuitBreaker definition
│       ├── idempotency.py              # Redis API idempotency & SQL inbox deduplication
│       └── http_client.py              # Resilient service-to-service HTTP client
├── services/                           # Microservice Bounded Contexts
│   ├── user-service/
│   │   ├── Dockerfile                  # Multi-stage container build
│   │   ├── requirements.txt            # Python dependencies (includes email-validator & aiokafka)
│   │   └── src/                        # DDD 5-layer codebase
│   ├── product-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── order-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── payment-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── reporting-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── webhook-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── support-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── mcp-server/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── discovery-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── merchant-copilot-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   ├── knowledge-graph-rag-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/
│   └── dispute-resolution-service/
│       ├── Dockerfile                  # FastEmbed cached layer & multi-stage build
│       ├── requirements.txt            # LangGraph, Qdrant, ClickHouse, FastEmbed, Redis
│       └── src/                        # 5-stage Multi-Agent Negotiation & Self-RAG
└── otel-collector-config.yaml          # OpenTelemetry central metrics/trace pipeline router
```

---

## 🔄 4. Asynchronous Event-Driven Saga Pattern & CQRS Materialized State

To maintain transactional consistency across our isolated databases without resorting to slow distributed locks or blocking two-phase commits (2PC), we implement an **asynchronous choreographed Saga pattern** powered by **CQRS Materialized Local Views**.

### CQRS Temporal Decoupling: Eliminating Sync Service-to-Service Lookups
In typical choreographed saga microservice architectures, when a consumer receives an event (e.g. `PaymentService` receiving `inventory.reserved`), it often needs context from other domains (e.g. the order's price). Making synchronous HTTP REST requests (e.g. `GET /orders/{id}`) back to the originating service creates **temporal coupling**: if the order service goes down mid-transaction, the entire Saga compensation flow fails.

To solve this, both `payment-service` and `product-service` subscribe to `order.created` events and **materialize the order metadata locally** (read models) under single atomic transactions protected by the **Inbox Pattern**. When the next steps or compensations in the Saga trigger, they query their **local database tables** with **zero HTTP requests**. If a cache miss occurs (e.g. out-of-order Kafka message), they gracefully fall back to a resilient, circuit-breaker-protected HTTP request before failing.

### How Our Implementation Maps to CQRS (Write vs. Read Models)

Our microservice architecture cleanly separates state mutation (Commands) from state querying (Reads) using decentralized read-only projections:

```mermaid
flowchart TD
    %% Node Definitions
    KafkaOrderCreated[("Topic: order.created")]
    KafkaOrderConfirmed[("Topic: order.confirmed")]
    KafkaStoreRegistered[("Topic: store.registered")]

    subgraph Order Context [Order Bounded Context]
        CMD[1. POST /orders] -->|Write Model| OrderDB[("Order DB: PostgreSQL")]
        ConfirmCMD[Saga Confirm] -->|Update status: CONFIRMED| OrderDB
    end

    subgraph Product Context [Product Bounded Context]
        StoreCMD[2. POST /products/stores] -->|Write Model| ProductDB[("Product DB: PostgreSQL")]
    end

    subgraph Payment Context [Payment Bounded Context]
        Sub[Kafka Consumer] -->|Write local projection| ReadTable[("materialized_orders Read Model")]
        
        InvEvent[Event: inventory.reserved] -->|Consume| PayProc[process_payment]
        PayProc -->|3. Local DB Query| ReadTable
    end

    subgraph Webhook Context [Webhook Bounded Context]
        WebSubStore[Kafka Consumer] -->|Write store webhook config| WebhookReadTable[("materialized_stores Read Model")]
        
        ConfirmEvent[Event: order.confirmed] -->|Consume| WebhookProc[dispatch_webhook]
        WebhookProc -->|4. Local DB Query| WebhookReadTable
        WebhookProc -->|POST Resilient Dispatch| PartnerAPI["Partner Store Webhook Endpoint"]
    end

    subgraph Reporting Context [Reporting Bounded Context]
        RepSub[Kafka Consumer] -->|Write Materialized Views| RepTable[("Profiles, Orders, & Payments Read Model")]
        
        QueryCMD[5. GET /reporting/customers/.../dashboard] -->|Read Model Query| RepTable
        QueryStoreCMD[6. GET /reporting/stores/.../dashboard] -->|Read Model Query| RepTable
    end

    %% Flow Connections
    OrderDB -->|Publish Event| KafkaOrderCreated
    OrderDB -->|Publish Event| KafkaOrderConfirmed
    ProductDB -->|Publish Event| KafkaStoreRegistered
    
    KafkaOrderCreated -->|Asynchronous Sync| Sub
    KafkaOrderCreated -->|Asynchronous Sync| RepSub
    KafkaStoreRegistered -->|Asynchronous Sync| WebSubStore
    KafkaOrderConfirmed -->|Asynchronous Sync| ConfirmEvent
```

1. **The Write Model (Command Side)**: Exclusively managed by `order-service`. Mutating operations (e.g. creating an order) write directly to the `order_db` source of truth.
2. **The Read Model (Query Side)**: Projections such as `materialized_orders` inside `payment-service` and `materialized_reservations` inside `product-service`. These exist purely to answer local reads fast, without hitting the primary Write database.
3. **Eventual Consistency**: Kafka events asynchronously synchronize mutations from the Command side to the local Read projections within milliseconds.

### Saga Flow Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant OrderService as Order Service
    participant Kafka as Apache Kafka Broker
    participant ProductService as Product Service
    participant PaymentService as Payment Service
    participant ReportingService as Reporting Service
    participant WebhookService as Webhook Service
    participant PartnerStore as Partner Store Webhook

    Client->>OrderService: POST /orders
    activate OrderService
    OrderService->>ProductService: GET /products/{id} (Verify product & resolve store_id)
    ProductService-->>OrderService: Return Product Details (with store_id)
    OrderService->>OrderService: Write Order (with store_id) & Outbox Message (Atomic DB Transaction)
    OrderService-->>Client: Returns Order DTO (PENDING)
    deactivate OrderService
    OrderService->>Kafka: Outbox Publisher dispatches "order.created" Event

    Note over Client, OrderService: Client establishes SSE connection (GET /orders/{id}/status-stream)
    Client->>OrderService: Establish SSE Stream Connection
    activate OrderService
    OrderService-->>Client: Stream Push: status: PENDING

    par Parallel Event Deliveries (CQRS Materialization)
        Kafka->>PaymentService: Deliver "order.created" Event
        activate PaymentService
        PaymentService->>PaymentService: Materialize order locally in DB (total_price, qty)
        deactivate PaymentService
    and Product Reservation & Materialization
        Kafka->>ProductService: Deliver "order.created" Event
        activate ProductService
        alt Stock is Available
            ProductService->>ProductService: Decrement stock, save reservation & Outbox (Atomic DB Transaction)
            ProductService->>Kafka: Outbox Publisher dispatches "inventory.reserved" Event
        else Insufficient Stock
            ProductService->>ProductService: Write Outbox Message (Atomic DB Transaction)
            ProductService->>Kafka: Outbox Publisher dispatches "inventory.failed" Event
        end
        deactivate ProductService
    and Reporting Service Materialization
        Kafka->>ReportingService: Deliver "order.created" Event
        activate ReportingService
        ReportingService->>ReportingService: Materialize order locally in DB (status: PENDING)
        deactivate ReportingService
    end

    alt Stock Reservation Succeeded
        Kafka->>PaymentService: Deliver "inventory.reserved" Event
        activate PaymentService
        PaymentService->>PaymentService: Query local DB for materialized order (Zero HTTP calls)
        PaymentService->>PaymentService: Execute simulated gateway transaction
        alt Payment Succeeded (Success Path)
            PaymentService->>PaymentService: Save Payment & Outbox Message (Atomic DB Transaction)
            PaymentService->>Kafka: Outbox Publisher dispatches "payment.succeeded" Event
            par Saga Success Action
                Kafka->>OrderService: Deliver "payment.succeeded" Event
                OrderService->>OrderService: Update Order (status: CONFIRMED) & Outbox Message (Atomic DB Transaction)
                OrderService-->>Client: Stream Push: status: CONFIRMED
                OrderService->>Kafka: Outbox Publisher dispatches "order.confirmed" Event
            and Reporting Service Success Capture
                Kafka->>ReportingService: Deliver "payment.succeeded" Event
                activate ReportingService
                ReportingService->>ReportingService: Materialize Payment & Update Order status to CONFIRMED
                deactivate ReportingService
            end

            par Webhook Resilient Dispatch
                Kafka->>WebhookService: Deliver "order.confirmed" Event
                activate WebhookService
                WebhookService->>WebhookService: Query local DB for materialized store (Zero HTTP calls)
                WebhookService->>PartnerStore: POST Resilient Webhook Dispatch
                activate PartnerStore
                PartnerStore-->>WebhookService: Return 200 OK / Success
                deactivate PartnerStore
                deactivate WebhookService
            end
        else Payment Failed (Compensating Saga Rollback)
            PaymentService->>PaymentService: Save Payment & Outbox Message (Atomic DB Transaction)
            PaymentService->>Kafka: Outbox Publisher dispatches "payment.failed" Event
            par Saga Compensating Action
                Kafka->>OrderService: Deliver "payment.failed" Event
                OrderService->>OrderService: Update Order (status: CANCELLED)
                OrderService-->>Client: Stream Push: status: CANCELLED
            and Saga Stock Restoration
                Kafka->>ProductService: Deliver "payment.failed" Event
                activate ProductService
                ProductService->>ProductService: Query local DB for reservation (Zero HTTP calls)
                ProductService->>ProductService: Increment stock (release_stock) & delete reservation
                deactivate ProductService
            and Reporting Service Failure Capture
                Kafka->>ReportingService: Deliver "payment.failed" Event
                activate ReportingService
                ReportingService->>ReportingService: Materialize Payment failure & Update Order status to CANCELLED
                deactivate ReportingService
            end
        end
        deactivate PaymentService
    else Stock Reservation Failed
        Kafka->>OrderService: Deliver "inventory.failed" Event
        activate OrderService
        OrderService->>OrderService: Update Order (status: CANCELLED)
        OrderService-->>Client: Stream Push: status: CANCELLED
        deactivate OrderService
        
        Kafka->>ReportingService: Deliver "inventory.failed" Event
        activate ReportingService
        ReportingService->>ReportingService: Update local Order status to CANCELLED
        deactivate ReportingService
    end
    
    OrderService--xClient: Close SSE Stream Connection
    deactivate OrderService
```

### Stripe Redirect-Based Checkout Flow vs. Automatic Payment Flow

The system supports two distinct payment flows specified via the `payment_method` attribute on order placement:
1. **`"AUTOMATIC"` (Default)**: Immediately triggers a credit card charge simulation once stock is reserved, confirming or cancelling the transaction in one go (standard Saga step).
2. **`"STRIPE"`**: Performs redirect-based asynchronous checkout. Once stock is reserved, the payment service initializes a checkout session, generates a checkout simulator URL, and publishes `payment.session_created`. The order service moves the order status to `AWAITING_PAYMENT` and records the URL. The client interacts with the Stripe simulator, and a webhook callback triggers the final confirmation or compensation Saga.

#### Stripe Redirect Flow Sequence Diagram:

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant OrderService as Order Service
    participant Kafka as Apache Kafka Broker
    participant ProductService as Product Service
    participant PaymentService as Payment Service
    participant StripeSimulator as Stripe Simulator (HTML)

    Client->>OrderService: POST /orders (payment_method: "STRIPE")
    activate OrderService
    OrderService->>ProductService: GET /products/{id} (Verify & resolve store_id)
    ProductService-->>OrderService: Return Product Details
    OrderService->>OrderService: Write Order (status: PENDING) & Outbox Message
    OrderService-->>Client: Returns Order DTO (PENDING, payment_url: null)
    deactivate OrderService
    OrderService->>Kafka: Outbox Publisher dispatches "order.created" Event

    Client->>OrderService: Establish SSE status-stream (GET /orders/{id}/status-stream)
    activate OrderService

    par Parallel Event Deliveries (CQRS Materialization)
        Kafka->>PaymentService: Deliver "order.created" Event
        PaymentService->>PaymentService: Materialize order locally (method: STRIPE)
    and Product Reservation
        Kafka->>ProductService: Deliver "order.created" Event
        ProductService->>ProductService: Decrement stock & save reservation
        ProductService->>Kafka: Outbox Publisher dispatches "inventory.reserved" Event
    end

    Kafka->>PaymentService: Deliver "inventory.reserved" Event
    activate PaymentService
    PaymentService->>PaymentService: Query local DB for materialized order
    PaymentService->>PaymentService: Create pending Stripe session & checkout URL
    PaymentService->>Kafka: Outbox Publisher dispatches "payment.session_created" Event
    deactivate PaymentService

    Kafka->>OrderService: Deliver "payment.session_created" Event
    OrderService->>OrderService: Update Order (status: AWAITING_PAYMENT, payment_url: "...")
    OrderService-->>Client: Stream Push: status: AWAITING_PAYMENT, payment_url: "..."

    Note over Client, StripeSimulator: Client redirects user to payment_url
    Client->>StripeSimulator: GET /payments/stripe-checkout/{id}
    activate StripeSimulator
    StripeSimulator-->>Client: Serve Stripe Checkout Simulator Page (HTML/JS)
    deactivate StripeSimulator

    alt Customer clicks "Pay Now" (Success path)
        Client->>StripeSimulator: Submit form (POST /payments/{id}/stripe-complete with success: true)
        activate StripeSimulator
        StripeSimulator->>PaymentService: Execute complete_stripe_payment
        PaymentService->>PaymentService: Save Payment (SUCCEEDED) & Outbox Message
        StripeSimulator-->>Client: Return status: processed (Redirect to Store)
        deactivate StripeSimulator
        PaymentService->>Kafka: Outbox Publisher dispatches "payment.succeeded" Event
        Kafka->>OrderService: Deliver "payment.succeeded" Event
        OrderService->>OrderService: Update Order (status: CONFIRMED) & evict cache
        OrderService-->>Client: Stream Push: status: CONFIRMED
    else Customer clicks "Cancel" (Failure/Compensating path)
        Client->>StripeSimulator: Submit form (POST /payments/{id}/stripe-complete with success: false)
        activate StripeSimulator
        StripeSimulator->>PaymentService: Execute complete_stripe_payment
        PaymentService->>PaymentService: Save Payment (FAILED) & Outbox Message
        StripeSimulator-->>Client: Return status: processed (Redirect to Store)
        deactivate StripeSimulator
        PaymentService->>Kafka: Outbox Publisher dispatches "payment.failed" Event
        par Saga Compensating Action
            Kafka->>OrderService: Deliver "payment.failed" Event
            OrderService->>OrderService: Update Order (status: CANCELLED) & evict cache
            OrderService-->>Client: Stream Push: status: CANCELLED
        and Saga Stock Restoration
            Kafka->>ProductService: Deliver "payment.failed" Event
            ProductService->>ProductService: Increment stock & release reservation
        end
    end

    deactivate OrderService
```

---

## ⚡ 5. Getting Started & Running the Platform

### A. Environment Configuration
Create your local environment file from the template:
```bash
cp .env.example .env
```
Fill in the custom database credentials, port configurations, and Redis credentials. The system automatically reads and applies these variables during startup.

### B. Start the Platform
1. **Launch the core system**:
   ```bash
   docker compose up --build -d
   ```
   This spins up the five microservices, their autonomous databases, Traefik, Keepalived high-availability instances, Redis cache, and the Kafka broker in KRaft mode.

2. **Launch the telemetry stack**:
   ```bash
   docker compose -f docker-compose.observability.yml up -d
   ```
   This starts the OpenTelemetry Collector, Prometheus, Grafana, Jaeger, Loki, and Alertmanager.

---

## 📊 6. Interactive Documentation & Dashboards

| Service / Interface | Host Port | Ingress Gateway Route / Address |
| :--- | :--- | :--- |
| **Traefik Ingress Gateway** | `80` | `http://localhost/` (or Virtual IP `172.20.0.100`) |
| **User Service OpenAPI Docs** | `8001` | `http://localhost/users/docs` or `http://localhost:8001/docs` |
| **Product Service OpenAPI Docs**| `8002` | `http://localhost/products/docs` or `http://localhost:8002/docs` |
| **Order Service OpenAPI Docs** | `8003` | `http://localhost/orders/docs` or `http://localhost:8003/docs` |
| **Payment Service OpenAPI Docs**| `8004` | `http://localhost/payments/docs` or `http://localhost:8004/docs` |
| **Reporting Service OpenAPI Docs**| `8005` | `http://localhost/reporting/docs` or `http://localhost:8005/docs` |
| **Webhook Service OpenAPI Docs** | `8006` | `http://localhost/webhooks/docs` or `http://localhost:8006/docs` |
| **AI Support Service OpenAPI Docs**| `8007` | `http://localhost/support/docs` or `http://localhost:8007/docs` |
| **MCP Server (FastMCP / SSE)** | `8008` | `http://localhost/mcp/sse` or `http://localhost:8008/sse` |
| **Discovery Service OpenAPI Docs**| `8009` | `http://localhost/discovery/docs` or `http://localhost:8009/docs` |
| **Merchant Copilot OpenAPI Docs** | `8010` | `http://localhost/copilot/docs` or `http://localhost:8010/docs` |
| **Knowledge Graph RAG Service**   | `8011` | `http://localhost/graphrag/health` or `http://localhost:8011/health` |
| **Knowledge Graph RAG OpenAPI Docs** | `8011` | `http://localhost/graphrag/docs` or `http://localhost:8011/docs` |
| **Dispute Resolution Service**    | `8012` | `http://localhost/disputes/health` or `http://localhost:8012/health` |
| **Dispute Resolution OpenAPI Docs**| `8012` | `http://localhost/disputes/docs` or `http://localhost:8012/docs` |
| **Qdrant Vector Database Web UI**| `6333` | `http://localhost:6333/dashboard` |
| **ClickHouse Web Client (Play UI)**| `8123` | `http://localhost:8123/play` |
| **Jaeger Distributed Tracing** | `16686` | `http://localhost:16686/` |
| **Grafana Telemetry Dashboard**| `3000` | `http://localhost:3000/` |
| **Grafana GraphRAG Monitoring** | `3000` | `http://localhost:3000/d/graphrag-monitoring/knowledge-graph-rag-graphrag-monitoring` |
| **Grafana Dispute Resolution**  | `3000` | `http://localhost:3000/d/dispute-monitoring/multi-agent-dispute-resolution-claims-monitoring` |
| **Grafana Backend Error Inbox** | `3000` | `http://localhost:3000/d/logs-traces/backend-logs-error-tracing` |
| **Grafana MCP Agent Monitoring** | `3000` | `http://localhost:3000/d/mcp-monitoring/model-context-protocol-mcp-ai-agent-gateway-monitoring` |
| **Prometheus Metrics Engine** | `9090` | `http://localhost:9090/` |
| **Alertmanager Controller** | `9093` | `http://localhost:9093/` |

---

## 🧪 7. End-to-End Integration Verification

You can easily verify the choreographed Saga and E2E resilience mechanisms directly through the Traefik Gateway (Port `80`).

### 1. User Registration (REST API Idempotency & Validation)
Register a new user context. Make sure to specify the `X-Idempotency-Key` header:
```bash
curl -i -X POST http://localhost/users \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: register-user-101" \
  -d '{"username": "johndoe", "email": "john@example.com", "password": "securepassword123"}'
```

**Verification**:
- **Idempotent Retry**: Send the exact same request again with the *same* `X-Idempotency-Key` header. You will receive an instantaneous `201 Created` response containing the cached details because the Redis-backed idempotency system intercepts the request.
- **Duplicate Email (New Key)**: Send a request using a *new* idempotency key, the same email `john@example.com`, but a different username:
  ```bash
  curl -i -X POST http://localhost/users \
    -H "Content-Type: application/json" \
    -H "X-Idempotency-Key: register-user-102" \
    -d '{"username": "newjohndoe", "email": "john@example.com", "password": "securepassword123"}'
  ```
  Expected Response: `400 Bad Request` with payload `{"detail":"Email 'john@example.com' is already registered."}`.
- **Duplicate Username (New Key)**: Send a request using a *new* idempotency key, the same username `johndoe`, but a different email:
  ```bash
  curl -i -X POST http://localhost/users \
    -H "Content-Type: application/json" \
    -H "X-Idempotency-Key: register-user-103" \
    -d '{"username": "johndoe", "email": "newjohn@example.com", "password": "securepassword123"}'
  ```
  Expected Response: `400 Bad Request` with payload `{"detail":"Username 'johndoe' is already registered."}`.


### 2. Product Catalog Creation
Create a product for ordering:
```bash
curl -i -X POST http://localhost/products \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: create-product-201" \
  -d '{"name": "Mechanical Keyboard", "price": 99.99, "stock": 15}'
```

### 3. Saga Transaction — Success Path (Stock & Payment Approved)
Place an order for 2 keyboards (Catalog has 15 in stock). The total price `$199.98` is under the simulated `$1000` limit, leading to successful processing:
```bash
curl -i -X POST http://localhost/orders \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: submit-order-301" \
  -d '{"user_id": 1, "product_id": 1, "quantity": 2, "total_price": 199.98}'
```
**Verification Logs**:
- `order-service` writes a `PENDING` order, publishes `order.created` to Kafka, and returns `201 Created`.
- `product-service` consumes `order.created`, decrements database stock from `15` to `13`, and publishes `inventory.reserved` to Kafka.
- `payment-service` consumes `inventory.reserved`, calls `order-service` via a resilient HTTP client to verify the amount, executes a simulated success gateway transaction, and publishes `payment.succeeded` to Kafka.
- `order-service` consumes `payment.succeeded` and transitions the order status to `CONFIRMED`.

Verify the final order status in real time or via API query:
* **Option A: Real-Time Stream (Highly Recommended)**
  Start a `curl` listener in one terminal *before* submitting the order in another terminal (adjust order ID if needed):
  ```bash
  curl -i -N http://localhost/orders/1/status-stream
  ```
  Expected Stream Output:
  ```text
  data: {"order_id": 1, "status": "PENDING"}
  data: {"order_id": 1, "status": "CONFIRMED"}
  ```
* **Option B: Standard GET Polling**
  ```bash
  # Get Order #1 Details (Should reflect status: CONFIRMED)
  curl http://localhost/orders/1
  ```

* **Verify remaining catalog stock** (Should reflect stock: 13):
  ```bash
  curl http://localhost/products/1
  ```

### 4. Saga Transaction — Failure Path (Insufficient Stock)
Attempt to place an order for 20 keyboards (Catalog has only 13 in stock):
```bash
curl -i -X POST http://localhost/orders \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: submit-order-302" \
  -d '{"user_id": 1, "product_id": 1, "quantity": 20, "total_price": 1999.80}'
```
**Verification Logs**:
- `order-service` writes a `PENDING` order, publishes `order.created` to Kafka, and returns `201 Created`.
- `product-service` consumes `order.created`, detects insufficient stock, and publishes `inventory.failed` to Kafka.
- `order-service` consumes `inventory.failed` and transitions the order status to `CANCELLED`.

Verify the final order status in real time or via API query:
* **Option A: Real-Time Stream (Highly Recommended)**
  Start a `curl` listener in one terminal *before* submitting the order (adjust order ID if needed):
  ```bash
  curl -i -N http://localhost/orders/2/status-stream
  ```
  Expected Stream Output:
  ```text
  data: {"order_id": 2, "status": "PENDING"}
  data: {"order_id": 2, "status": "CANCELLED"}
  ```
* **Option B: Standard GET Polling**
  ```bash
  # Get Order #2 Details (Should reflect status: CANCELLED)
  curl http://localhost/orders/2
  ```

* **Verify catalog stock** (Should retain original stock level: 13):
  ```bash
  curl http://localhost/products/1
  ```


### 4.1 Saga Transaction — Compensating Failure Paths (Payment Failures)
We can simulate and test two distinct Saga compensation rollbacks:

#### Scenario A: Simulated Payment Rejection (Total Price > $1000)
Submitting an order total price of `$1050.00` (which is over `$1000`) triggers an immediate payment rejection inside `payment-service`:
```bash
curl -i -X POST http://localhost/orders \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: saga-rejection-test-1" \
  -d '{"user_id": 1, "product_id": 1, "quantity": 1, "total_price": 1050.00}'
```
**Verification**:
- **Order Cancelled**: Listen to the stream `/orders/{id}/status-stream` or query `GET /orders/{id}`. The status transitions to `CANCELLED`.
- **Stock Restored (Compensated)**: Product stock level decreases temporarily during reservation but is immediately restored back to its original count because the `payment.failed` event coordinates a compensation trigger in `product-service`.

#### Scenario B: Simulated Payment Gateway Timeout (Quantity == 7)
Ordering a quantity of exactly `7` triggers a simulated 4-second gateway connection hang inside `payment-service`, leading to a TimeoutException:
```bash
curl -i -X POST http://localhost/orders \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: saga-timeout-test-1" \
  -d '{"user_id": 1, "product_id": 1, "quantity": 7, "total_price": 699.93}'
```
**Verification**:
- The order status stays `PENDING` during the 4-second hang.
- Once the simulated timeout is hit, the payment registers as failed. The order transitions to `CANCELLED` and stock is safely compensated back to the DB catalog.

### 4.2 Saga Transaction — Redirect Checkout Path (Stripe Flow)

Submit an order specifying `"payment_method": "STRIPE"`:
```bash
curl -i -X POST http://localhost/orders \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: submit-stripe-order-1" \
  -d '{"user_id": 1, "product_id": 1, "quantity": 2, "total_price": 199.98, "payment_method": "STRIPE"}'
```

**Verification**:
- **Immediate Response**: You will receive a `201 Created` response showing `status: "PENDING"` and `payment_url: null`.
- **Status Stream Redirection**: Open the status stream to watch status transitions (adjust order ID if needed):
  ```bash
  curl -N http://localhost/orders/3/status-stream
  ```
  Once stock is reserved, you will see a push with status `AWAITING_PAYMENT` and the target Stripe redirect URL:
  ```text
  data: {"order_id": 3, "status": "AWAITING_PAYMENT", "payment_url": "http://localhost:8004/payments/stripe-checkout/3"}
  ```
- **Simulate Payment Gateway Webhook Callback**:
  Submit a completion trigger (simulating a webhook callback from Stripe to the payment service):
  * **Success path**:
    ```bash
    curl -i -X POST http://localhost/payments/3/stripe-complete \
      -H "Content-Type: application/json" \
      -d '{"success": true}'
    ```
    Expected Stream Output: The status-stream immediately updates to `CONFIRMED`:
    ```text
    data: {"order_id": 3, "status": "CONFIRMED"}
    ```
    *Note: When querying `GET /orders/3`, the Redis cache is automatically evicted after the commit, serving the updated `CONFIRMED` status instantly.*
  * **Compensating Saga (Failure path)**:
    Create a new order with a new idempotency key, listen to its status-stream, and trigger a simulated failure:
    ```bash
    curl -i -X POST http://localhost/payments/4/stripe-complete \
      -H "Content-Type: application/json" \
      -d '{"success": false}'
    ```
    Expected Stream Output: The status-stream immediately updates to `CANCELLED`, releasing the reserved stock in the catalog.

### 5. Programmatic Circuit Breaker & Self-Healing Demo
Simulate a database server outage by stopping the User Postgres container:
```bash
docker compose stop user-db
```
Send GET requests to fetch User #1 through the Gateway:
```bash
curl -i http://localhost/users/1
```
**Verification**:
- The first 3 requests return `500 Internal Server Error` due to socket timeouts as the connection fails.
- The 4th request instantly triggers a `503 Service Unavailable` response from our `db_breaker`:
  ```json
  {"detail": "Database circuit breaker active: Circuit PostgresBreaker is OPEN."}
  ```
  This indicates that the circuit has tripped to `OPEN`, fast-failing downstream calls immediately.
- Restart the container: `docker compose start user-db`
- Wait 15 seconds (cooldown period), then query the endpoint again. The circuit probe transitions to `HALF-OPEN`, successfully queries User #1, closes the breaker, and returns `200 OK`.

### 6. Automated Resilience, Idempotency & Multi-Tenancy Test Suite (Pytest)

An end-to-end automated test suite validates the core resilience mechanisms after any code change:
```bash
.venv/bin/pytest tests/ -v
```

This suite executes:
- **Circuit Breaker State Machine**: Verifies transitions (`CLOSED` $\rightarrow$ `OPEN` $\rightarrow$ `HALF-OPEN` $\rightarrow$ `CLOSED`) and sub-millisecond fast-failing.
- **Idempotency & Race Conditions**: Sends concurrent in-flight requests with identical keys to verify atomic Redis locking, single database mutations, and tenant-namespaced key isolation.
- **Multi-Tenancy Schema Isolation**: Dynamically provisions tenants, verifies `X-Tenant-ID` header enforcement, and proves data cannot leak across store schemas.
- **Event-Driven CQRS Read Models**: Verifies asynchronous Kafka event materialization in Webhook and Reporting services.

### 7. Load & Performance Testing (Locust)
A robust `locustfile.py` load tester is included. To trigger headless performance testing:
```bash
locust --headless -u 10 -r 2 --run-time 1m --host http://localhost
```
Or open the Locust dashboard using:
```bash
locust
```
And navigate to `http://localhost:8089` to specify target users, ramp-up rates, and view live response-time and error graphs.


### 7. Asynchronous Consumer Retries & Dead Letter Queue (DLQ) Replay Demo

Simulate an asynchronous consumer processing failure (e.g. database down during saga event handling) and verify self-healing recovery:

1. **Stop Downstream Database**:
   ```bash
   docker compose stop product-db
   ```
2. **Submit Order Request** (Ensuring target product 6 is cached in Redis):
   ```bash
   curl -i -X POST http://localhost/orders \
     -H "Content-Type: application/json" \
     -H "X-Idempotency-Key: dlq-demo-key-1" \
     -d '{"user_id": 273, "product_id": 6, "quantity": 1, "total_price": 100.00}'
   ```
   *Note: Because `product-service` has cached product #6 in Redis, `order-service`'s client-side cache fallback allows it to verify the product, create the order, and write it to the outbox table. The REST API immediately responds with `201 Created` and status `PENDING`.*
3. **Verify Retries & DLQ Routing**:
   * Inspect the `product-service` logs to watch the consumer retry loop:
     ```bash
     docker logs product-service 2>&1 | grep -E "Transient|DLQ"
     ```
     You will observe the callback fail with name resolution errors, retry 3 times with exponential backoff, route the event to `order.created.deadletter`, and commit the partition offset.
4. **Inspect Grafana Dashboard**:
   * Open Grafana (`http://localhost:3000`) and view the **"Transactional Outbox & Read Resiliency"** dashboard.
   * Under the **"Consumer Retries & Dead Letter Queues (DLQ)"** row, you will see a spike in the **Unreplayed DLQ Backlog (Messages)** panel showing `1` message in the queue.
5. **Restore System**:
   ```bash
   docker compose start product-db
   ```
6. **Trigger DLQ Replay Utility**:
   * Execute the replay CLI command to drain and republish dead-lettered events:
     ```bash
     docker exec order-service python /app/shared/bin/replay_dlq.py
     ```
     The tool will scan `.deadletter` topics, republish the message back to `order.created`, and commit its DLQ offset.
7. **Verify Saga Completion**:
   * Query the order status to verify it has self-healed and transitioned to `CONFIRMED`:
     ```bash
     curl -i http://localhost/orders/75
     ```
   * On the Grafana dashboard, the **Unreplayed DLQ Backlog** metrics will immediately drop back down to `0`.

---

### 8. Real-Time Kafka Inspection & Complete Service API Reference

To make integration verification and debugging seamless, this section provides a complete reference of all microservice endpoints (accessible via the Traefik API Gateway) and the exact commands to monitor asynchronous event flows in Kafka in real time.

#### A. Comprehensive API Endpoint Map
All service interactions are routed through the Traefik Gateway on port `80`.

| Bounded Context | Method | Gateway Path | Direct Port Path | Expected Payload / Params | Idempotency Key Required |
| :--- | :--- | :--- | :--- | :--- | :---: |
| **User Service** | `POST` | `/users` | `:8001/` | `{"username", "email", "password"}` | Yes (`X-Idempotency-Key`) |
| **User Service** | `GET` | `/users/{id}` | `:8001/{id}` | None (Path Parameter) | No |
| **Product Service** | `POST` | `/products` | `:8002/` | `{"name", "price", "stock", "store_id"}` | Yes (`X-Idempotency-Key`) |
| **Product Service** | `GET` | `/products` | `:8002/` | None | No |
| **Product Service** | `GET` | `/products/{id}` | `:8002/{id}` | None (Path Parameter) | No |
| **Product Service** | `POST` | `/products/stores` | `:8002/stores` | `{"name", "webhook_url"}` | No |
| **Product Service** | `GET` | `/products/stores` | `:8002/stores` | None | No |
| **Product Service** | `GET` | `/products/stores/{store_id}` | `:8002/stores/{store_id}` | None (Path Parameter) | No |
| **Order Service** | `POST` | `/orders` | `:8003/` | `{"user_id", "product_id", "quantity", "total_price", "store_id", "payment_method"}` | Yes (`X-Idempotency-Key`) |
| **Order Service** | `GET` | `/orders` | `:8003/` | None | No |
| **Order Service** | `GET` | `/orders/{id}` | `:8003/{id}` | None (Path Parameter) | No |
| **Order Service** | `GET` | `/orders/{id}/status-stream` | `:8003/{id}/status-stream` | None (Real-time SSE Stream) | No |
| **Payment Service** | `GET` | `/payments` | `:8004/` | None | No |
| **Payment Service** | `GET` | `/payments/{order_id}` | `:8004/{order_id}` | None (Path Parameter) | No |
| **Payment Service** | `GET` | `/payments/stripe-checkout/{order_id}` | `:8004/stripe-checkout/{order_id}` | None (Stripe Checkout simulator page) | No |
| **Payment Service** | `POST` | `/payments/{order_id}/stripe-complete` | `:8004/{order_id}/stripe-complete` | `{"success"}` (Stripe completion webhook) | No |
| **Reporting Service** | `GET` | `/reporting/stores/{store_id}/dashboard` | `:8005/stores/{store_id}/dashboard` | None (Path Parameter) | No |
| **Reporting Service** | `GET` | `/reporting/customers/{customer_id}/dashboard` | `:8005/customers/{customer_id}/dashboard` | None (Path Parameter) | No |
| **Webhook Service**   | `GET` | `/webhooks/stores`                       | `:8006/stores`                          | None                  | No  |
| **Webhook Service**   | `GET` | `/webhooks/logs`                         | `:8006/logs`                            | None                  | No  |
| **AI Support Service**| `POST`| `/support/chat`                          | `:8007/chat`                            | `{"message", "session_id", "user_id"}`  | No  |
| **AI Support Service**| `POST`| `/support/actions/confirm`               | `:8007/actions/confirm`                 | `{"session_id", "approved"}`            | No  |
| **AI Support Service**| `GET` | `/support/eval/benchmark`                | `:8007/eval/benchmark`                 | None                                    | No  |
| **MCP Server**        | `GET` | `/mcp/sse`                               | `:8008/sse`                             | SSE Connection URL                      | No  |
| **MCP Server**        | `POST`| `/mcp/messages/`                         | `:8008/messages/`                       | JSON-RPC 2.0 Request Payload            | No  |
| **Discovery Service** | `POST`| `/discovery/chat`                        | `:8009/chat`                            | `{"query", "session_id", "budget"}`     | No  |
| **Discovery Service** | `GET` | `/discovery/health`                      | `:8009/health`                          | None                                    | No  |
| **Merchant Copilot**  | `POST`| `/copilot/chat`                          | `:8010/chat`                            | `{"query", "session_id", "tenant_id"}`  | No  |
| **Merchant Copilot**  | `GET` | `/copilot/health`                        | `:8010/health`                          | None                                    | No  |
| **Knowledge Graph RAG** | `POST` | `/graphrag/query`                     | `:8011/query`                           | `{"query", "session_id", "search_mode"}` | No |
| **Knowledge Graph RAG** | `GET`  | `/graphrag/subgraph`                  | `:8011/subgraph`                        | `?seeds=prod_gaming_laptop_pro&hops=2` | No  |
| **Knowledge Graph RAG** | `GET`  | `/graphrag/communities`               | `:8011/communities`                     | None                                    | No  |
| **Knowledge Graph RAG** | `GET`  | `/graphrag/stats`                     | `:8011/stats`                           | None                                    | No  |
| **Knowledge Graph RAG** | `GET`  | `/graphrag/nodes/{node_id}`           | `:8011/nodes/{node_id}`                 | None (Path Parameter)                   | No  |
| **Knowledge Graph RAG** | `POST` | `/graphrag/nodes`                     | `:8011/nodes`                           | `{"id", "name", "type", "description"}` | No  |
| **Knowledge Graph RAG** | `POST` | `/graphrag/edges`                     | `:8011/edges`                           | `{"source", "target", "relation"}`      | No  |
| **Knowledge Graph RAG** | `GET`  | `/graphrag/health`                    | `:8011/health`                          | None                                    | No  |
| **Dispute Resolution**  | `POST` | `/disputes/claims`                    | `:8012/claims`                          | `{"order_id", "customer_id", "product_name", "claim_amount", "reason", "customer_statement"}` | No |
| **Dispute Resolution**  | `GET`  | `/disputes/claims/{claim_id}`         | `:8012/claims/{claim_id}`               | None (Path Parameter)                   | No  |
| **Dispute Resolution**  | `GET`  | `/disputes/claims`                    | `:8012/claims`                          | None                                    | No  |
| **Dispute Resolution**  | `GET`  | `/disputes/stats`                     | `:8012/stats`                           | None                                    | No  |
| **Dispute Resolution**  | `GET`  | `/disputes/health`                    | `:8012/health`                          | None                                    | No  |

---

#### B. Copy-Pasteable API Curl Examples

##### 1. User Bounded Context
* **Register a New User**:
  ```bash
  curl -i -X POST http://localhost/users \
    -H "Content-Type: application/json" \
    -H "X-Idempotency-Key: register-user-user1" \
    -d '{"username": "dev_user", "email": "dev@example.com", "password": "SuperSecretPassword123"}'
  ```
* **Retrieve User Details**:
  ```bash
  curl -i http://localhost/users/1
  ```

##### 2. Product Catalog Bounded Context
* **Create a Catalog Product**:
  ```bash
  curl -i -X POST http://localhost/products/ \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -H "X-Idempotency-Key: create-product-prod1" \
    -d '{"name": "UltraWide Gaming Monitor", "price": 449.99, "stock": 10, "store_id": 1}'
  ```
* **List All Products in Tenant**:
  ```bash
  curl -i http://localhost/products/ \
    -H "X-Tenant-ID: store_tech"
  ```
* **Retrieve Specific Product**:
  ```bash
  curl -i http://localhost/products/1 \
    -H "X-Tenant-ID: store_tech"
  ```

##### 3. Order Checkout Bounded Context (Triggers Saga Flow)
* **Place an Order (Success Path - Stock Available)**:
  ```bash
  curl -i -X POST http://localhost/orders/ \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -H "X-Idempotency-Key: submit-order-ord1" \
    -d '{"user_id": 1, "product_id": 1, "quantity": 1, "total_price": 449.99, "store_id": 1}'
  ```
* **List All Placed Orders**:
  ```bash
  curl -i http://localhost/orders/ \
    -H "X-Tenant-ID: store_tech"
  ```
* **Retrieve Specific Order Details**:
  ```bash
  curl -i http://localhost/orders/1 \
    -H "X-Tenant-ID: store_tech"
  ```
* **Stream Real-Time Order Status Transitions (SSE)**:
  ```bash
  curl -i -N http://localhost/orders/1/status-stream \
    -H "X-Tenant-ID: store_tech"
  ```
* **Place an Order (Stripe Redirect Flow)**:
  ```bash
  curl -i -X POST http://localhost/orders/ \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -H "X-Idempotency-Key: submit-order-stripe" \
    -d '{"user_id": 1, "product_id": 1, "quantity": 1, "total_price": 449.99, "store_id": 1, "payment_method": "STRIPE"}'
  ```

##### 4. Payment Bounded Context
* **List All Placed Payments**:
  ```bash
  curl -i http://localhost/payments/ \
    -H "X-Tenant-ID: store_tech"
  ```
* **Retrieve Payment by Order ID**:
  ```bash
  curl -i http://localhost/payments/1 \
    -H "X-Tenant-ID: store_tech"
  ```
* **Simulate Stripe Checkout Webhook Completion**:
  ```bash
  curl -i -X POST http://localhost/payments/1/stripe-complete \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{"success": true}'
  ```

##### 5. Reporting Bounded Context (CQRS Customer & Store Dashboard)
* **Retrieve Consolidated Customer Report**:
  ```bash
  curl -s http://localhost/reporting/customers/1/dashboard \
    -H "X-Tenant-ID: store_tech"
  ```
* **Retrieve Store Sales Performance Dashboard**:
  ```bash
  curl -s http://localhost/reporting/stores/1/dashboard \
    -H "X-Tenant-ID: store_tech"
  ```

##### 6. Store Bounded Context & Webhook Management
* **Create a Partner Store (with Webhook URL)**:
  ```bash
  curl -i -X POST http://localhost/products/stores \
    -H "Content-Type: application/json" \
    -d '{"name": "Partner Store A", "webhook_url": "https://api.partner-a.com/webhook"}'
  ```
* **List All Registered Stores**:
  ```bash
  curl -i http://localhost/products/stores
  ```
* **Retrieve Specific Store Details**:
  ```bash
  curl -i http://localhost/products/stores/1
  ```
* **Retrieve Store Sales Performance Dashboard (CQRS View)**:
  ```bash
  curl -s http://localhost/reporting/stores/1/dashboard
  ```
* **Retrieve Materialized Store Configurations (Webhook Service Read Model)**:
  ```bash
  curl -i http://localhost/webhooks/stores
  ```
* **Retrieve Historical Webhook Delivery Logs**:
  ```bash
  curl -i http://localhost/webhooks/logs
  ```

##### 7. AI Customer Support Assistant (`support-service:8007`)
* **Policy FAQ Query (Two-Stage Hybrid Search & Re-Ranking)**:
  ```bash
  curl -s -X POST http://localhost/support/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "session_id": "faq_session_01",
      "message": "What is the return window for electronics and who pays for return shipping?"
    }'
  ```
* **Live Order Query (Self-RAG Reflection & Database Tool Lookup)**:
  ```bash
  curl -s -X POST http://localhost/support/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "user_id": "1",
      "session_id": "order_query_01",
      "message": "What is the status of my order #1 and when was it placed?"
    }'
  ```
* **Human-in-the-Loop Order Cancellation (Step 1: Freezes at Breakpoint)**:
  ```bash
  curl -s -X POST http://localhost/support/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "user_id": "1",
      "session_id": "hitl_cancel_01",
      "message": "Please cancel my order #1"
    }'
  ```
* **Human Approval Confirmation (Step 2: Resumes Execution & Mutates DB)**:
  ```bash
  curl -s -X POST http://localhost/support/actions/confirm \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "session_id": "hitl_cancel_01",
      "approved": true
    }'
  ```
* **Fetch Automated RAG Triad Benchmark Scores**:
  ```bash
  curl -s http://localhost/support/eval/benchmark
  ```

##### 8. Semantic Product Discovery & Bundle Builder (`discovery-service:8009`)
* **Health Check**:
  ```bash
  curl -s http://localhost/discovery/health
  ```
* **Semantic Vector Search (Natural Language Catalog Lookup)**:
  ```bash
  curl -s -X POST http://localhost/discovery/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "query": "Ergonomic chair and studio microphone for podcasting",
      "tenant_id": "store_tech"
    }'
  ```
* **Budget-Constrained Bundle Optimization (Knapsack Algorithm)**:
  ```bash
  curl -s -X POST http://localhost/discovery/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "query": "Complete creator setup with high quality mic and headphones under $600",
      "tenant_id": "store_tech",
      "budget": 600.0
    }'
  ```
* **Compound Query Decomposition & HyDE**:
  ```bash
  curl -s -X POST http://localhost/discovery/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "query": "Gaming laptop and ultrawide monitor with mechanical keyboard",
      "tenant_id": "store_tech",
      "budget": 2000.0
    }'
  ```

##### 9. Merchant Copilot Analytics & Policy Assistant (`merchant-copilot-service:8010`)
* **Health Check & OLAP Status**:
  ```bash
  curl -s http://localhost/copilot/health
  ```
* **Structured Analytics Query (Text-to-SQL on ClickHouse)**:
  ```bash
  curl -s -X POST http://localhost/copilot/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "query": "Show our top 5 products by price and stock levels",
      "tenant_id": "store_tech"
    }'
  ```
* **Store Revenue & Order Aggregations (ClickHouse Columnar Execution)**:
  ```bash
  curl -s -X POST http://localhost/copilot/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "query": "What is our total sales revenue and confirmed order count?",
      "tenant_id": "store_tech"
    }'
  ```
* **Hybrid Analytics + Store Return SLA Guidelines (ClickHouse + Qdrant Policy RAG)**:
  ```bash
  curl -s -X POST http://localhost/copilot/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "query": "Show our top products by price and what is our return and refund policy for electronics?",
      "tenant_id": "store_tech"
    }'
  ```
* **Tenant Isolation & Multi-Tenant Query Validation**:
  ```bash
  curl -s -X POST http://localhost/copilot/chat \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_gaming" \
    -d '{
      "query": "List all products in our catalog",
      "tenant_id": "store_gaming"
    }'
  ```

##### 10. Model Context Protocol (MCP) Server Endpoints (`mcp-service:8008`)
* **Health & Readiness Check**:
  ```bash
  curl -s http://localhost/mcp/health
  ```
* **Prometheus Metrics Endpoint**:
  ```bash
  curl -s http://localhost/mcp/metrics
  ```
* **Establish SSE Event Stream for AI Agents**:
  ```bash
  curl -i -N http://localhost/mcp/sse
  ```

##### 11. Knowledge Graph RAG (GraphRAG) Service Endpoints (`knowledge-graph-rag-service:8011`)
* **Health & Readiness Check**:
  ```bash
  curl -s http://localhost/graphrag/health
  ```
* **Real-Time Graph Stats (Nodes, Edges, Communities)**:
  ```bash
  curl -s http://localhost/graphrag/stats
  ```
* **Hierarchical Community Clusters (Louvain / Leiden)**:
  ```bash
  curl -s http://localhost/graphrag/communities
  ```
* **Interactive Mermaid Subgraph Visualization**:
  ```bash
  curl -s "http://localhost/graphrag/subgraph?seeds=prod_gaming_laptop_pro&hops=2"
  ```
* **Local Multi-Hop Root-Cause Investigation**:
  ```bash
  curl -s -X POST http://localhost/graphrag/query \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "query": "Why is the Gaming Laptop Pro overheating and which supplier is responsible for the defect?"
    }'
  ```
* **Global Platform Quality & Supplier Risk Map-Reduce**:
  ```bash
  curl -s -X POST http://localhost/graphrag/query \
    -H "Content-Type: application/json" \
    -H "X-Tenant-ID: store_tech" \
    -d '{
      "query": "Give me a high-level summary of all supplier defects across all products"
    }'
  ```
* **Dynamically Add / Ingest a New Entity Node**:
  ```bash
  curl -s -X POST http://localhost/graphrag/nodes \
    -H "Content-Type: application/json" \
    -d '{
      "id": "supp_samsung_vietnam",
      "name": "Samsung Electronics (Thai Nguyen, Vietnam)",
      "type": "Supplier",
      "description": "Tier-1 audio transducer and battery assembly facility.",
      "tenant_id": "store_tech"
    }'
  ```
* **Dynamically Link Two Entities with a Directed Edge**:
  ```bash
  curl -s -X POST http://localhost/graphrag/edges \
    -H "Content-Type: application/json" \
    -d '{
      "source": "prod_20",
      "target": "supp_samsung_vietnam",
      "relation": "SUPPLIED_BY",
      "description": "Assembled and QC certified at Thai Nguyen factory."
    }'
  ```
* **Inspect Specific Node in Knowledge Graph**:
  ```bash
  curl -s http://localhost/graphrag/nodes/prod_20
  ```

---

#### C. Real-Time Kafka Topic & Message Inspection

Since all microservices coordinate asynchronously via Kafka, you can inspect topics and event messages directly by running commands inside the running `kafka` container.

##### 1. List Active Kafka Topics
List all event-driven topics currently registered in the KRaft broker:
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```
*Expected Output:*
```text
inventory.failed
inventory.reserved
order.created
user.registered
```

##### 2. Stream Live Integration Events
Use `kafka-console-consumer` to listen to events in real time. Open a separate terminal window and run these commands to watch messages as you execute the API requests above.

* **Monitor `order.created` (Published by Order Service)**:
  ```bash
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order.created --from-beginning
  ```
* **Monitor `inventory.reserved` (Success Path - Published by Product Service)**:
  ```bash
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic inventory.reserved --from-beginning
  ```
* **Monitor `inventory.failed` (Failure Path - Published by Product Service)**:
  ```bash
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic inventory.failed --from-beginning
  ```
* **Monitor `payment.succeeded` (Success Path - Published by Payment Service)**:
  ```bash
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment.succeeded --from-beginning
  ```
* **Monitor `payment.failed` (Failure/Compensating Path - Published by Payment Service)**:
  ```bash
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment.failed --from-beginning
  ```
* **Monitor `payment.session_created` (Redirect flow session created)**:
  ```bash
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic payment.session_created --from-beginning
  ```
* **Monitor `user.registered` (Published by User Service)**:
  ```bash
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic user.registered --from-beginning
  ```

> [!TIP]
> Add `--property print.key=true --property key.separator=" | "` to the consumer commands to see the Kafka partition keys (used for ordering guarantees) alongside the JSON payload. For example:
> ```bash
> docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order.created --from-beginning --property print.key=true --property key.separator=" | "
> ```

---

## 📝 9. Stand-Alone System Design Algorithms (For Learning)

To explore core traffic shaping and resilience algorithms in pure, stand-alone Python (completely decoupled from the running microservices), navigate to the `algorithms/` folder:

| File | Pattern | Core Mechanism | How to Run |
|---|---|---|---|
| [`circuit_breaker.py`](algorithms/circuit_breaker.py) | **Circuit Breaker** | A state machine simulating failures, transition phases (`CLOSED`, `OPEN`, `HALF-OPEN`), and auto-recovery. | `python3 algorithms/circuit_breaker.py` |
| [`token_bucket.py`](algorithms/token_bucket.py) | **Token Bucket** | Efficient **lazy-refill strategy** allowing bursty traffic up to bucket capacity while capping average request throughput. | `python3 algorithms/token_bucket.py` |
| [`leaky_bucket.py`](algorithms/leaky_bucket.py) | **Leaky Bucket** | Efficient **lazy-leak strategy** smoothing out sudden bursts completely, outputting steady uniform flow. | `python3 algorithms/leaky_bucket.py` |
| [`bloom_filter.py`](algorithms/bloom_filter.py) | **Bloom Filter & Counting BF** | Probabilistic set membership with Kirsch-Mitzenmacher double hashing, dynamic mathematical sizing ($m, k$), and 8-bit counter deletions. | `python3 algorithms/bloom_filter.py` |
| [`lsm_tree.py`](algorithms/lsm_tree.py) | **Log-Structured Merge-Tree** | High-throughput write engine with in-memory `MemTable`, sequential `WAL`, immutable `SSTables` (with embedded Bloom filters & sparse index), and merge compaction. | `python3 algorithms/lsm_tree.py` |

---

## 🤖 10. AI Customer Support Service (Agentic RAG & LangGraph)

The **Support AI Bounded Context** (`support-service:8007`) provides an enterprise-grade agentic customer assistant designed for multi-tenant e-commerce platforms.

### A. Key Architectural Capabilities

1. **Two-Stage Hybrid Search & Reciprocal Rank Fusion (RRF)**:
   - Combines **Dense Semantic Embeddings** via Qdrant (`BAAI/bge-small-en-v1.5`) and **Sparse Keyword Search** via in-memory `BM25Okapi`.
   - Merges candidate rankings using standard Reciprocal Rank Fusion:
     $$RRF(d) = \sum_{m \in M} \frac{1}{60 + \text{rank}_m(d)}$$
2. **FlashRank Cross-Encoder Re-Ranking Pipeline**:
   - Scores Top-15 fused candidate chunks using a local CPU-optimized Cross-Encoder model (`ms-marco-TinyBERT-L-2-v2`).
   - Re-ranks for true semantic relevance with $< 20\text{ms}$ latency and $\$0$ marginal inference cost.
3. **Self-RAG (Hallucination Checking & Reflection Loops)**:
   - An independent **LLM-as-a-Judge Fact-Checker** evaluates draft answers against retrieved policies and live database results before delivery.
   - Automatically loops back to generation with corrective directives if ungrounded claims are detected (bounded by `max_retries=2`).
4. **Human-in-the-Loop (HITL) with LangGraph Breakpoints**:
   - High-risk state mutations (e.g. **Order Cancellations** and **Refund Requests**) trigger a LangGraph breakpoint (`interrupt_before=["execute_action"]`).
   - The graph state is frozen and persisted in the memory/Redis checkpointer, returning `status: "pending_approval"`.
   - Execution resumes and completes the database mutation only when explicit approval is submitted via `POST /support/actions/confirm`.
5. **Automated RAG Triad Evaluation Suite (Ragas / LLM-as-a-Judge)**:
   - Automated benchmark runner (`eval/run_evaluation.py`) scoring the 3 RAG Triad dimensions on a curated golden dataset:
     - **Context Relevance ($S_{context}$)**: Retriever precision (Target $\ge 0.85$).
     - **Faithfulness ($S_{faithful}$)**: Hallucination rate (Target $\ge 0.90$).
     - **Answer Relevance ($S_{answer}$)**: User intent satisfaction (Target $\ge 0.85$).

---

### B. Testing the AI Support Service

#### 1. Policy FAQ Query (Two-Stage Hybrid Search & Re-Ranking)
```bash
curl -X POST http://localhost/support/chat \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "session_id": "policy-test-01",
    "message": "What is the return window for clothing vs electronics, and who pays shipping?"
  }'
```

---

#### 2. Hybrid Policy + Live Order Query (Self-RAG Reflection)
```bash
curl -X POST http://localhost/support/chat \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "user_id": "1",
    "session_id": "hybrid-test-01",
    "message": "My order #1 arrived damaged, can I get a replacement sent?"
  }'
```

---

#### 3. Human-in-the-Loop Order Cancellation (Step 1: Pauses at Breakpoint)
```bash
curl -X POST http://localhost/support/chat \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "user_id": "1",
    "session_id": "hitl-test-01",
    "message": "Please cancel my order #1"
  }'
```
*Returns `status: "pending_approval"` with confirmation details. No database mutation has occurred yet.*

---

#### 4. Human Approval Confirmation (Step 2: Resumes & Cancels in DB)
```bash
curl -X POST http://localhost/support/actions/confirm \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "session_id": "hitl-test-01",
    "approved": true
  }'
```
*Resumes the frozen thread, calls `order-service` to cancel the order, and confirms the cancellation.*

---

#### 5. Run the Automated RAG Triad Benchmark Suite
```bash
# Run benchmark inside the container
docker compose exec support-service python eval/run_evaluation.py

# Inspect benchmark report via REST API
curl http://localhost/support/eval/benchmark
```

---

## 📈 11. Distributed Observability Stack (Jaeger, Loki, Prometheus & Grafana)

The platform includes a production-grade distributed telemetry stack defined in `docker-compose.observability.yml`:

| Telemetry Pillar | Tool / Port | Purpose in Platform |
| :--- | :--- | :--- |
| **Distributed Tracing** | **Jaeger** (`http://localhost:16686`) | Visual waterfall tracking across Traefik $\rightarrow$ Microservices $\rightarrow$ DB queries $\rightarrow$ Kafka Sagas $\rightarrow$ LangGraph AI nodes. |
| **Structured Logging** | **Loki & Grafana** (`http://localhost:3000`) | Aggregates structured JSON logs from all 7 services with correlated `trace_id`, `span_id`, and `service_name` stream labels. |
| **Metrics & Telemetry** | **Prometheus** (`http://localhost:9090`) | Scrapes `/metrics` endpoints for latency histograms, request counters, outbox backlog gauges, and DLQ counts. |
| **Telemetry Pipeline** | **OpenTelemetry Collector** (`:4317` / `:4318`) | Ingests OTLP telemetry batches, enriches spans with resource metadata, and routes to Jaeger, Loki, and Prometheus. |

### A. How to Start the Observability Stack

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```

### B. Inspecting Distributed Traces in Jaeger (`http://localhost:16686`)
1. Open **[http://localhost:16686](http://localhost:16686)**.
2. Select any service (`mcp-service`, `order-service`, `support-service`, `product-service`, `user-service`).
3. Click **Find Traces** to view the cascading execution graph:
   - **MCP AI Agent Tool Waterfall**: Shows `MCP tool: create_order` $\rightarrow$ `HTTP POST /orders/` $\rightarrow$ Traefik Gateway $\rightarrow$ `order-service` $\rightarrow$ DB Transactional Outbox $\rightarrow$ Kafka Saga propagation.
   - **Order Saga Waterfall**: Shows `POST /orders` $\rightarrow$ User verification $\rightarrow$ Product verification $\rightarrow$ DB Outbox write $\rightarrow$ Kafka `order.created` send $\rightarrow$ Product consumer reservation $\rightarrow$ Payment consumer execution.
   - **AI Support Agent Waterfall**: Shows `POST /support/chat` $\rightarrow$ `langgraph.router` $\rightarrow$ `langgraph.retrieve` (Qdrant + BM25 + FlashRank) $\rightarrow$ `langgraph.tools` (HTTP calls to `order-service` / `product-service`) $\rightarrow$ `langgraph.generate` $\rightarrow$ `langgraph.check_hallucination`.

### C. Querying Multi-Service Logs in Loki / Grafana (`http://localhost:3000`)
1. Open Grafana: **[http://localhost:3000](http://localhost:3000)** (Navigate to **Explore** $\rightarrow$ select **Loki**).
2. Query logs across all services:
   ```logql
   # Stream logs from all 7 microservices in real-time
   {service_name=~".+"}

   # Filter logs for a specific service
   {service_name="order-service"}
   {service_name="support-service"}

   # Find errors across the entire fleet
   {service_name=~".+"} |= "ERROR"
   ```
3. Click on any log line's **`trace_id`** badge to jump straight from Loki logs into the exact Jaeger trace!

### D. Bloom Filter & LSM Storage Telemetry
The Bloom Filter security layer and LSM Tree storage engine are instrumented with comprehensive Prometheus metrics, OpenTelemetry span attributes, and automated Prometheus alerts:

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `bloom_filter_queries_total` | Counter | `filter_name`, `result` (`hit`/`miss`) | Total Bloom membership checks across services. |
| `bloom_filter_fast_rejections_total` | Counter | `filter_name` | Total non-existent requests fast-rejected in RAM before touching DB/Cache. |
| `lsm_storage_appends_total` | Counter | `engine` | Total high-frequency append-only writes absorbed by LSM engine. |
| `lsm_storage_reads_total` | Counter | `engine`, `result` (`hit`/`miss`) | Total point reads served from MemTable / SSTables. |
| `lsm_storage_memtable_entries` | Gauge | `engine` | Current active in-memory write buffer count. |
| `lsm_storage_sstables_count` | Gauge | `engine` | Number of immutable on-disk SSTable files. |

- **Active Alert Rules ([`alert_rules.yml`](alert_rules.yml))**:
  - `BloomFilterHighFastRejectionRate`: Triggers when $>20\text{ rejections/sec}$ occur on any Bloom filter (flags scraping / cache penetration scans).
  - `LSMHighMemTableBacklog`: Triggers when MemTable entries stay near threshold ($>45$) for $>2\text{m}$, signaling disk I/O or compaction bottlenecks.

---

## 🤖 12. Model Context Protocol (MCP) Server for AI Agents

The platform includes a dedicated **Model Context Protocol (MCP)** microservice (`mcp-service:8008`, built with `mcp>=2.0.0` and `FastMCP`) enabling autonomous AI agents, desktop LLMs (Claude Desktop, Cursor, Antigravity), and agentic workflows to interact securely with the e-commerce platform.

### Architectural Alignment with DDD & Enterprise NFRs
- **Zero Direct Database Leakage**: Functions strictly as an **Anti-Corruption Layer (ACL)** calling API Gateway endpoints via `ResilientHTTPClient` rather than querying microservice databases directly.
- **Idempotency Protection**: Mutating tools (`create_order`, `register_user`, `cancel_order`) generate or accept unique `X-Idempotency-Key` headers stored in Redis to prevent duplicate purchases or double refunds during agent retry loops.
- **Circuit Breaker Fast-Failing**: Automatically traps `CircuitBreakerOpenException` to return actionable, LLM-friendly diagnostic explanations when backend services undergo maintenance.
- **Multi-Tenant Scoping**: Injects `X-Tenant-ID` into every HTTP request to enforce PostgreSQL schema-per-tenant isolation.
- **Asynchronous Saga Triggering**: Order creation initiates the decentralized Kafka Saga and returns `PENDING` status immediately.

```
┌────────────────────────────────────────────────────────┐
│             External / Internal AI Agent               │
│        (Claude Desktop, Cursor, Antigravity)           │
└──────────────────────────┬─────────────────────────────┘
                           │ JSON-RPC 2.0 (SSE / stdio)
┌──────────────────────────▼─────────────────────────────┐
│             mcp-service:8008 (FastMCP)                 │
│  ┌──────────────────┬──────────────────┬────────────┐  │
│  │      Tools       │    Resources     │  Prompts   │  │
│  │ (register, order,│(policies, FAQs,  │ (shopping, │  │
│  │  cancel, track)  │     catalog)     │troubleshoot│  │
│  └──────────────────┴──────────────────┴────────────┘  │
└──────────────────────────┬─────────────────────────────┘
                           │ ResilientHTTPClient + Circuit Breakers
┌──────────────────────────▼─────────────────────────────┐
│          API Gateway (Traefik / Keepalived VIP)        │
└────────────────────────────────────────────────────────┘
```

### Supported MCP Capabilities

#### 1. Executable Tools (`tools/list` & `tools/call`)
| Tool Name | Key Parameters | Description |
| :--- | :--- | :--- |
| `register_user` | `username`, `email`, `password`, `tenant_id` | Registers a customer account with idempotency protection. |
| `get_user_profile`| `user_id`, `tenant_id` | Looks up customer account details. |
| `list_products` | `tenant_id` | Lists all products, prices, and stock in the tenant catalog. |
| `get_product_details` | `product_id`, `tenant_id` | Fetches real-time price and `in_stock` boolean flag. |
| `create_order` | `user_id`, `product_id`, `quantity`, `total_price`, `payment_method`, `tenant_id` | Initiates the asynchronous Kafka Choreographed Saga. |
| `get_order_status`| `order_id`, `tenant_id` | Real-time status lookup (`PENDING`, `CONFIRMED`, `CANCELLED`). |
| `cancel_order` | `order_id`, `reason`, `tenant_id` | Triggers Saga compensation (inventory release & payment refund). |
| `discover_product_bundle` | `query`, `budget`, `tenant_id` | Semantic bundle builder and price optimizer under budget constraints. |
| `merchant_copilot_query` | `query`, `tenant_id` | Hybrid Text-to-SQL (ClickHouse) + Vector Policy RAG (Qdrant). |
| `graph_rag_query` | `query`, `search_mode`, `tenant_id` | Microsoft GraphRAG: Multi-hop supplier/defect reasoning and Louvain community detection. |
| `submit_dispute_claim` | `order_id`, `customer_id`, `product_name`, `claim_amount`, `reason`, `customer_statement`, `delivery_days_ago`, `tenant_id` | Submits claim to Multi-Agent Negotiation Arena (Buyer Advocate vs Merchant Defender) for judicial arbitration and supplier CAR generation. |
| `get_dispute_status` | `claim_id`, `tenant_id` | Retrieves full details, adversarial debate transcript, and arbitration outcome for a dispute claim. |
| `get_dispute_statistics` | `tenant_id` | Retrieves platform-wide dispute KPIs, auto-settlement ratios, and refunded amounts. |

#### 2. Contextual Resources (`resources/list` & `resources/read`)
- `ecommerce://policies/returns`: Official return, cancellation, and refund policies.
- `ecommerce://policies/shipping`: Delivery SLAs and multi-tenant fulfillment terms.
- `ecommerce://support/faq`: Common customer troubleshooting questions.

#### 3. Agent Prompt Workflows (`prompts/list` & `prompts/get`)
- `shopping_assistant`: Structured instructions for customer discovery, inventory checks, and order submission.
- `order_troubleshooting`: Structured instructions for order status checks and cancellation verification.

### How to Connect AI Agents to the MCP Server

#### Option A: Remote SSE / HTTP Transport (Docker Microservice on port 8008)
Add this to your AI client configuration (e.g. `claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "ecommerce-platform": {
      "url": "http://localhost:8008/sse"
    }
  }
}
```

#### Option B: Local stdio Transport
```json
{
  "mcpServers": {
    "ecommerce-local": {
      "command": "python",
      "args": ["-m", "src.main"],
      "env": {
        "PYTHONPATH": "/path/to/system_design/services/mcp-server:/path/to/system_design",
        "TRANSPORT_MODE": "stdio",
        "API_GATEWAY_URL": "http://localhost"
      }
    }
  }
}
```

#### Option C: Visual Debugging with MCP Inspector
```bash
# Test all tools, resources, and prompts interactively in the web UI:
npx @modelcontextprotocol/inspector python -m src.main
```

### 📊 MCP Observability & Grafana Dashboard
A dedicated Grafana telemetry dashboard (**`Model Context Protocol (MCP) Agent Monitoring`**) is automatically provisioned at `http://localhost:3000`:
- **AI Agent Tool Invocation Throughput**: Real-time rate of tool executions (`mcp_tool_calls_total`) categorized by action (`create_order`, `register_user`, `cancel_order`) and status (`submitted`, `created`, `found`, `cancelled`).
- **Circuit Breaker Fast-Fails**: Tracks degraded tool encounters (`mcp_circuit_breaker_trips_total`).
- **Execution Latency (P50/P95)**: Latency histogram for every tool (`mcp_tool_duration_seconds`).
- **Contextual Resource & Prompt Reads**: Monitors resource lookups (`mcp_resource_reads_total`) and prompt workflow generations (`mcp_prompt_requests_total`).
- **Multi-Tenant Segmentation**: Donut breakdown of tool calls by tenant (`store_tech`, `store_gaming`, `public`).
- **Live Loki Log Stream**: Real-time JSON log aggregation for `{service_name="mcp-service"}` with linked `trace_id` badges for Jaeger tracing.

---

## 🔍 13. Semantic Product Discovery & Bundle Builder (`discovery-service`)

A dedicated AI microservice on port **`8009`** executing **Advanced RAG (HyDE, Query Decomposition, Qdrant Payload Filtering)** and a **6-node LangGraph State Machine** to find complementary product setups within strict budget ceilings.

### 📐 LangGraph Workflow Architecture
```mermaid
graph TD
    Start([User Request: 'Desk setup under $500']) --> ConstraintParser[1. parse_constraints_node]
    ConstraintParser --> HyDE_Decomp[2. generate_hyde_and_subqueries_node]
    HyDE_Decomp --> ParallelSearch[3. qdrant_hybrid_search_node]
    ParallelSearch --> CrossEncoder[4. cross_encoder_rerank_node]
    CrossEncoder --> BundleOptimizer[5. bundle_optimizer_node]
    BundleOptimizer --> Synthesizer[6. synthesize_recommendation_node]
    Synthesizer --> End([Markdown Bundle Recommendation])
```

### 🧠 Core Techniques Implemented
1. **HyDE (Hypothetical Document Embeddings)**: Generates a hypothetical technical spec sheet before embedding to bridge the vocabulary gap between natural language user prompts and catalog metadata.
2. **Query Decomposition**: Splits compound requests (e.g. *"laptop and monitor"*) into discrete sub-queries for parallel vector execution.
3. **Qdrant Payload Filtering**: Enforces hard SQL-like constraints (`price <= budget`, `stock >= 1`, `tenant_id == store_tech`) directly inside vector space.
4. **Knapsack / Greedy Budget Optimizer**: LangGraph optimization node that guarantees the total bundle cost never exceeds the user's budget ceiling.
5. **Observability & Resilience**:
   - **Jaeger**: OpenTelemetry spans for every LangGraph node execution (`LangGraph node: bundle_optimizer`, `LangGraph node: qdrant_hybrid_search`).
   - **Prometheus & Grafana**: Scrapes `/metrics` for `discovery_requests_total`, `discovery_node_duration_seconds`, and `qdrant_search_duration_seconds`.
   - **Circuit Breakers**: `AsyncCircuitBreaker` fast-fails if the vector store is offline.
   - **MCP Tool**: Exposed as `discover_product_bundle` for autonomous AI agents.

### 🧪 Testing the Discovery & Bundle Builder Service

#### 1. Single-Item Semantic Search
```bash
curl -s -X POST http://localhost/discovery/chat \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "query": "Ergonomic chair with lumbar support",
    "tenant_id": "store_tech"
  }'
```

#### 2. Multi-Item Budget-Constrained Bundle Optimization
```bash
curl -s -X POST http://localhost/discovery/chat \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "query": "Audio interface and studio microphone setup",
    "tenant_id": "store_tech",
    "budget": 500.0
  }'
```

---

## 📊 14. Merchant Copilot Service (ClickHouse OLAP & Hybrid Policy RAG)

The **`merchant-copilot-service`** on port **`8010`** provides an enterprise-grade analytical copilot for store merchants. It combines **ClickHouse Columnar OLAP** for high-throughput aggregations, **Qdrant** for store policy guidelines, **`sqlglot`** for strict AST safety, and a **7-node LangGraph State Machine** featuring a self-correction feedback loop.

### 📐 LangGraph Workflow Architecture

```mermaid
flowchart TD
    subgraph StreamLayer["Event Stream Ingestion & Micro-Batching"]
        Kafka["Kafka Topics (8 Partitions)\nproduct.*, order.*, payment.*"] --> Consumer["aiokafka Consumer\n(merchant-copilot-group)"]
        Consumer --> Batcher["ClickHouseMicroBatcher\n(asyncio.Queue Buffer)"]
        Batcher -->|Flush: size >= 500 or time >= 1.0s| ClickHouse[("ClickHouse OLAP Database\n(ReplacingMergeTree Engine)")]
        ClickHouse -->|Batch Insert Acknowledged| CommitOffset["Commit Kafka Offset\n(Guaranteed At-Least-Once Delivery)"]
    end

    subgraph GraphWorkflow["LangGraph Text-to-SQL + Policy StateGraph"]
        MerchantQuery["Merchant Business Query"] --> IntentClass["1. intent_classifier\n(structured_analytics | policy_guidelines | hybrid)"]
        
        IntentClass -->|Structured / Hybrid| SchemaLink["2. schema_linking\n(Qdrant Vector Catalog)"]
        IntentClass -->|Policy / Hybrid| PolicyRetriever["3. policy_retriever\n(Qdrant Policy Store)"]
        
        SchemaLink --> TextToSQL["4. text_to_sql_generator\n(ClickHouse SQL with Tenant Filter)"]
        TextToSQL --> ASTValidation{"5. ast_validation\n(sqlglot ClickHouse Dialect)"}
        
        ASTValidation -->|Syntax Error / Policy Violation| SelfCorrection["6. sql_self_correction\n(LLM Self-Correction Edge Loop)"]
        SelfCorrection -->|Retry Attempt <= 3| ASTValidation
        
        ASTValidation -->|Valid & Safe| SQLExec["7. sql_executor\n(Execute on ClickHouse)"]
        SQLExec --> Synthesizer["8. response_synthesizer\n(Combine OLAP Table + Policy Markdown)"]
        PolicyRetriever --> Synthesizer
    end

    subgraph Delivery["API & Autonomous Agent Ingress"]
        Synthesizer --> FastAPIChat["POST /copilot/chat"]
        FastAPIChat --> TraefikRoute["Traefik Gateway (/copilot)"]
        TraefikRoute --> MCPTool["MCP Tool: merchant_copilot_query"]
    end
```

### 🧠 Core Architectural Innovations

#### 1. ClickHouse "Too Many Parts" Mitigation (`ClickHouseMicroBatcher`)
- **Challenge**: ClickHouse creates an immutable data part per insert. Inserting high-velocity individual events directly from Kafka causes the `DB::Exception: Too many parts in all data parts in table` error.
- **Solution**: Built `ClickHouseMicroBatcher` (`src/adapter/micro_batcher.py`) utilizing an asynchronous in-memory queue that buffers incoming Kafka events and flushes them in bulk when the queue reaches **500 records** or **1.0 second** has elapsed.

#### 2. Zero Data Loss & Crash Recovery
- **Challenge**: If an in-memory buffer crashes before records are persisted to disk, buffered messages could be lost.
- **Solution**: Kafka consumer message offsets are committed **only after** `clickhouse_client.insert_batch` returns success from disk. On container crash/restart, Kafka re-delivers uncommitted messages, and ClickHouse `ReplacingMergeTree` deduplicates records automatically.

#### 3. `sqlglot` AST Safety & Tenant Isolation Validator
- **Challenge**: LLMs can hallucinate destructive SQL (`DROP TABLE`, `DELETE`, `UPDATE`, `INSERT`) or omit tenant filtering, resulting in cross-tenant data leaks.
- **Solution**: Built `SQLASTValidator` (`src/adapter/ast_validator.py`) using `sqlglot`. It enforces that the parsed AST:
  - Is strictly a read-only `SELECT` or `UNION` statement.
  - Contains an explicit `tenant_id = '<store>'` predicate on all queried tables.
  - Rejects system commands, schema alterations, and multi-statement injection attempts.

#### 4. LangGraph Dynamic Self-Correction Feedback Loop
- When AST validation or ClickHouse execution fails, the graph routes execution to `sql_self_correction`, passing the AST error, tenant ID, and original query. The healed query routes back to `ast_validation` up to 3 times before graceful degradation.

#### 5. Dynamic Schema Linking via Qdrant Vector Catalog
- ClickHouse table DDLs and column descriptions are embedded and indexed in Qdrant (`clickhouse_schema_catalog`). The copilot dynamically links only relevant table schemas into the LLM prompt rather than overloading the context window with the entire database catalog.

#### 6. ClickHouse LSM Engine & Secondary Bloom Filter Skip Indexes
- **LSM Storage Engine (`ReplacingMergeTree`)**: ClickHouse ingests streaming batches from Kafka into immutable disk parts (SSTables) and merges duplicate records by primary key in the background, achieving $>100,000\text{ writes/sec}$ without lock contention.
- **Secondary Bloom Filter Skip Indexes (`INDEX ... TYPE bloom_filter(0.01)`)**: Configured across all analytical tables (`idx_store_id`, `idx_user_id`, `idx_product_id`, `idx_transaction_id`). During query execution, ClickHouse checks the Bloom filter in RAM for each 8,192-row granule and **completely skips reading and decompressing irrelevant disk blocks**, accelerating analytical filters by up to $10\times$.

---

### 🧪 Testing the Merchant Copilot Service

#### 1. Health & Backend Readiness
```bash
curl -s http://localhost/copilot/health
```
**Sample Response**:
```json
{
  "status": "healthy",
  "service": "merchant-copilot-service",
  "olap_backend": "ClickHouse",
  "vector_backend": "Qdrant",
  "rag_mode": "Hybrid Text-to-SQL + Policy RAG"
}
```

#### 2. Structured Analytics Query (ClickHouse Columnar Text-to-SQL)
```bash
curl -s -X POST http://localhost/copilot/chat \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "query": "Show our top 5 products by price and stock levels",
    "tenant_id": "store_tech"
  }'
```
**Sample Response**:
```markdown
### 📊 Merchant Copilot Executive Report (`store_tech`)

**Query**: *"Show our top 5 products by price and stock levels"*

#### 📈 Quantitative Data (ClickHouse OLAP)
| id | name | category | price | stock |
| --- | --- | --- | --- | --- |
| 2 | Gaming Laptop | Electronics | 1299.99 | 10 |
| 1 | Gaming Laptop | Electronics | 1299.99 | 12 |
| 15 | Aria Chair | Electronics | 695.0 | 12 |
| 14 | Herman Miller Aeron Ergonomic Office Chair | Electronics | 695.0 | 12 |
| 9 | Shure SM7B Dynamic Cardioid Vocal Microphone | Electronics | 399.0 | 15 |

> **Executed SQL**: `SELECT id, name, category, price, stock FROM copilot_analytics.products_analytics WHERE tenant_id = 'store_tech' ORDER BY price DESC LIMIT 5`
```

#### 3. Hybrid Analytics + Policy SLA Guidelines
```bash
curl -s -X POST http://localhost/copilot/chat \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "query": "Show our top products by price and what is our return and refund policy for electronics?",
    "tenant_id": "store_tech"
  }'
```
**Sample Response**:
```markdown
### 📊 Merchant Copilot Executive Report (`store_tech`)

**Query**: *"Show our top products by price and what is our return and refund policy for electronics?"*

#### 📈 Quantitative Data (ClickHouse OLAP)
| id | name | category | price | stock |
| --- | --- | --- | --- | --- |
| 2 | Gaming Laptop | Electronics | 1299.99 | 10 |
| 1 | Gaming Laptop | Electronics | 1299.99 | 12 |

#### 📜 Store Policies & SLA Guidelines (Qdrant Vector Store)
- **Standard 30-Day Customer Return Policy & Refund SLA**: Customers may initiate a return for any unopened or gently inspected item within 30 days of delivery. Inspection completed within 2 business days. Refunds processed in 3-5 banking days.
- **Electronics & Hardware Warranty Guidelines**: All hardware products include a 1-year comprehensive manufacturer warranty covering internal component defects.
```

#### 4. Automated Pytest Test Suite
```bash
PYTHONPATH=services/merchant-copilot-service ./.venv/bin/pytest tests/test_merchant_copilot_service.py -v
```
All 6 tests verify AST security parsing, micro-batching mechanics, Qdrant policy retrieval, LLM heuristics, LangGraph workflow execution, and FastAPI endpoints.

---

## 🕸️ 15. Knowledge Graph RAG (`knowledge-graph-rag-service:8011`)

The **Knowledge Graph RAG Service** implements the **Microsoft GraphRAG Paradigm** to solve the fundamental limitation of standard Vector RAG: **multi-hop relational reasoning and holistic global question answering**.

### 🌟 Key Architecture & Pillars

```
                                  ┌────────────────────────────────────────────────────────┐
                                  │             Merchant / Executive Business Query        │
                                  │ "Why are GPU returns spiking and which supplier is it?"│
                                  └──────────────────────────┬─────────────────────────────┘
                                                             │
                                                             ▼
                                  ┌────────────────────────────────────────────────────────┐
                                  │               1. graph_query_classifier                │
                                  │       (local_multihop  |  global_community_search)     │
                                  └──────────┬─────────────────────────────────┬───────────┘
                                             │                                 │
                     [Local Entity Search]   │                                 │ [Global Holistic Search]
                                             ▼                                 ▼
                     ┌───────────────────────────────┐ ┌─────────────────────────────────────────┐
                     │   2. entity_extractor_node    │ │    4. global_community_reducer_node     │
                     │  (Extract seed nodes from Q)  │ │   (Map-Reduce over Leiden Clusters)     │
                     └───────────────┬───────────────┘ └───────────────────┬─────────────────────┘
                                     │                                     │
                                     ▼                                     │
                     ┌───────────────────────────────┐                     │
                     │ 3. local_subgraph_traverser   │                     │
                     │ (2-3 Hop Relational Traversal)│                     │
                     └───────────────┬───────────────┘                     │
                                     │                                     │
                                     └─────────────────┬───────────────────┘
                                                       │
                                                       ▼
                                     ┌───────────────────────────────────┐
                                     │ 5. graph_reasoning_synthesizer    │
                                     │ (Multi-hop synthesis + Subgraph)  │
                                     └─────────────────┬─────────────────┘
                                                       │
                                                       ▼
                                     ┌───────────────────────────────────┐
                                     │      Executive Root-Cause Report  │
                                     │   + Interactive Subgraph Visual   │
                                     └───────────────────────────────────┘
```

1. **Entity-Relation Knowledge Graph Engine**:
   - Directed typed multigraph implemented in `NetworkX` with node types (`Product`, `Supplier`, `Component`, `Defect`, `Batch`, `Warehouse`, `Review`, `Store`) and typed relationships (`SUPPLIED_BY`, `CONTAINS_COMPONENT`, `REPORTED_DEFECT`, `SHIPPED_FROM`, `CAUSED_RETURN_IN`, `PRODUCED_IN_BATCH`, `SOLD_BY`).
   - Integrated with `Qdrant` collection `knowledge_graph_entities` for semantic vector entity linking.

2. **Hierarchical Community Detection (Leiden / Louvain Algorithm)**:
   - Partitions the global knowledge graph into topologically dense communities representing incident clusters and supplier failure groups.
   - Pre-computes and caches hierarchical executive summaries and risk ratings for each cluster.

3. **Dual-Mode Graph Retrieval (Local vs Global Search)**:
   - **Local Multi-Hop Search**: Starts from named entities in the query, searches $k$-hop neighbor subgraphs, and stitches relational evidence chains together (`Laptop` $\rightarrow$ `Vapor Chamber` $\rightarrow$ `CoolMaster Shenzhen` $\rightarrow$ `Batch #2026-B` $\rightarrow$ `Thermal Throttling Defect`).
   - **Global Map-Reduce Search**: Dispatches parallel map tasks across all community clusters, extracts relevant key points, and synthesizes a global executive risk briefing.

4. **Dynamic Stream-Based & REST Knowledge Ingestion**:
   - **Kafka Stream Ingestion**: Ingests real-time events (`product.*`, `inventory.failed`, `order.*`) to dynamically add nodes and edges to the live graph.
   - **Dynamic REST Ingestion**: Exposes `POST /graphrag/nodes` and `POST /graphrag/edges` to allow ERPs, PLM systems, and external pipelines to insert nodes/relationships with automatic Qdrant vector indexing and Louvain community updates.

5. **Automated Pytest Test Suite**:
   ```bash
   PYTHONPATH=services/knowledge-graph-rag-service ./.venv/bin/pytest tests/test_knowledge_graph_rag_service.py -v
   ```
   All 7 tests verify graph store operations, pathfinding, community clustering, mode classification, Qdrant linking, LangGraph workflow execution, and FastAPI endpoints.

### 🧪 Live GraphRAG Curl Testing & Verification

#### 1. Local Multi-Hop Root-Cause Investigation Query
```bash
curl -s -X POST http://localhost/graphrag/query \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "query": "Why is the Gaming Laptop Pro overheating and which supplier is responsible for the defect?"
  }'
```
**Sample Response**:
```json
{
  "session_id": "graph_8e94a102",
  "query": "Why is the Gaming Laptop Pro overheating and which supplier is responsible for the defect?",
  "tenant_id": "store_tech",
  "search_mode": "local_multihop",
  "nodes_traversed_count": 20,
  "edges_traversed_count": 38,
  "reasoning_hops": [
    "**[Gaming Laptop Pro (32GB RAM, RTX 4080)]** ──`CONTAINS_COMPONENT`──► **[Ultra-Thin Dual Vapor Chamber Heatsink]** (Laptop thermal dissipation relies on the CoolMaster dual vapor chamber.)",
    "**[Ultra-Thin Dual Vapor Chamber Heatsink]** ──`SUPPLIED_BY`──► **[CoolMaster Thermal Solutions Ltd (Shenzhen)]** (Vapor chamber heatsink manufactured and assembled by CoolMaster Shenzhen.)",
    "**[Ultra-Thin Dual Vapor Chamber Heatsink]** ──`PRODUCED_IN_BATCH`──► **[Production Batch #2026-B Vapor Chambers]** (Cooler unit belongs to the Q1 2026 Batch #2026-B production run.)",
    "**[Production Batch #2026-B Vapor Chambers]** ──`REPORTED_DEFECT`──► **[Defect #DEF-8802: Micro-Cavity Seal Leakage & Thermal Throttling]** (Batch #2026-B exhibits 14.8% micro-cavity solder seal failure.)",
    "**[Defect #DEF-8802: Micro-Cavity Seal Leakage & Thermal Throttling]** ──`CAUSED_RETURN_IN`──► **[Customer Review #REV-901: Instant thermal shutdown in Premiere Pro]** (Thermal throttling directly prompted customer return Review #REV-901.)"
  ],
  "final_markdown_report": "### 🕸️ GraphRAG Multi-Hop Root-Cause Investigation\n\n**Query**: *\"Why is the Gaming Laptop Pro overheating and which supplier is responsible for the defect?\"*\n\n#### 🔍 Causal Relational Chain (Multi-Hop Traversal)\n1. **[Gaming Laptop Pro]** ──`CONTAINS_COMPONENT`──► **[Ultra-Thin Dual Vapor Chamber Heatsink]**\n2. **[Ultra-Thin Dual Vapor Chamber Heatsink]** ──`SUPPLIED_BY`──► **[CoolMaster Thermal Solutions Ltd (Shenzhen)]**\n3. **[Ultra-Thin Dual Vapor Chamber Heatsink]** ──`PRODUCED_IN_BATCH`──► **[Production Batch #2026-B Vapor Chambers]**\n4. **[Production Batch #2026-B Vapor Chambers]** ──`REPORTED_DEFECT`──► **[Defect #DEF-8802: Micro-Cavity Seal Leakage & Thermal Throttling]**\n\n#### 🛠️ Recommended Action Items\n1. **Supplier Audit**: Issue a Corrective Action Request (CAR) to `CoolMaster Thermal Solutions Ltd (Shenzhen)` regarding manufacturing defect `Defect #DEF-8802: Micro-Cavity Seal Leakage`.\n2. **Inventory Quarantine**: Hold remaining stock associated with `Production Batch #2026-B Vapor Chambers` in fulfillment hubs.\n3. **Customer Remediation**: Proactively contact customers with open tickets offering expedited warranty replacements."
}
```

#### 2. Global Community Map-Reduce Executive Briefing
```bash
curl -s -X POST http://localhost/graphrag/query \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "query": "Give me a high-level summary of all supplier defects across all products"
  }'
```
**Sample Response Highlights**:
- Synthesizes risk matrix across **all 4 Louvain Community Clusters**:
  - `Community #4 [CRITICAL]`: CoolMaster Shenzhen vapor chamber micro-cavity solder leaks.
  - `Community #3 [CRITICAL]`: Neutrik XLR studio audio cable missing ground shell bridge Pin 1.
  - `Community #2 [CRITICAL]`: Great Lakes polymer lumbar bracket hydrolysis stress fractures under load.
  - `Community #1 [MEDIUM]`: TSMC / NVIDIA 4N GPU mobile die fabrication.

#### 3. Real-Time Topology & Subgraph Inspection
```bash
# A. Real-Time Graph Stats (Nodes, Edges, Communities)
curl -s http://localhost/graphrag/stats

# B. View Detected Louvain Community Clusters & Severity Ratings
curl -s http://localhost/graphrag/communities

# C. Extract 2-Hop Subgraph with Mermaid Diagram
curl -s "http://localhost/graphrag/subgraph?seeds=prod_gaming_laptop_pro&hops=2"
```

---

### 🔍 16. Distributed Observability & Jaeger Tracing for GraphRAG

1. **Jaeger Distributed Traces**:
   - Open **[http://localhost:16686/](http://localhost:16686/)** and search service `knowledge-graph-rag-service` or `traefik`.
   - Inspect the unified execution trace:
     - `traefik`: Ingress routing span (`/graphrag/query`)
     - `knowledge-graph-rag-service`: Server request span (`POST /query`)
     - `LangGraph node: graph_query_classifier`: Search mode routing (`local_multihop` vs `global_community`)
     - `LangGraph node: entity_extractor`: Discovers seed nodes (`Qdrant: search_entities`)
     - `LangGraph node: local_subgraph_traverser`: 2-hop relational pathfinding (`GraphStore: extract_subgraph`)
     - `LangGraph node: graph_reasoning_synthesizer`: Multi-hop synthesis report generation

2. **Grafana GraphRAG Dashboard**:
   - Open **[http://localhost:3000/d/graphrag-monitoring/knowledge-graph-rag-graphrag-monitoring](http://localhost:3000/d/graphrag-monitoring/knowledge-graph-rag-graphrag-monitoring)** to visualize query rates, node latencies, entity distributions, and live log streams with direct trace links.

---

## ⚖️ 17. Multi-Agent Dispute Resolution & Claims (`dispute-resolution-service:8012`)

The `dispute-resolution-service` provides an autonomous **Multi-Agent Negotiation Arena & Judicial Arbitration Engine** powered by **LangGraph**, **Self-RAG (Statutory Consumer Policy Grounding)**, **GraphRAG (Supplier Defect Evidence Traversal)**, **ClickHouse (Fraud & Chargeback Analytics)**, and **Kafka Saga Settlements**.

```
                           ┌──────────────────────────────────────────────┐
                           │            CLAIM SUBMITTED                   │
                           │   (POST /disputes/claims via Traefik :80)    │
                           └──────────────────────┬───────────────────────┘
                                                  │
                                                  ▼
                                   ┌──────────────────────────────┐
                                   │     Buyer Advocate Agent     │
                                   │  (Formulates legal claim)    │
                                   └──────────────┬───────────────┘
                                                  │
                                                  ▼
                                   ┌──────────────────────────────┐
                                   │   Merchant Defender Agent    │
                                   │ (Fulfillment & policy terms) │
                                   └──────────────┬───────────────┘
                                                  │
                                                  ▼
                               ┌──────────────────────────────────────┐
                               │     Multi-Source Evidence Engine     │
                               │  ├─ Self-RAG (Statutory Policies)    │
                               │  ├─ GraphRAG (:8011 Defect Subgraph) │
                               │  └─ ClickHouse (:8123 Fraud Scoring) │
                               └──────────────────┬───────────────────┘
                                                  │
                                                  ▼
                                   ┌──────────────────────────────┐
                                   │ Impartial Arbitrator Agent   │
                                   │  (Judicial Ruling & Splits)  │
                                   └──────────────┬───────────────┘
                                                  │
                                                  ▼
                               ┌──────────────────────────────────────┐
                               │        Settlement Engine             │
                               │  ├─ Auto-Disbursement ($ <= 200)     │
                               │  ├─ Kafka event: `dispute.resolved`  │
                               │  └─ Human Escalation Queue           │
                               └──────────────────────────────────────┘
```

### Multi-Agent Debate & Resolution Features
1. **Adversarial Legal Debate**: Simulates arguments between the `Buyer Advocate` (maximizing customer compensation) and `Merchant Defender` (safeguarding retailer operational margins and return window enforcement).
2. **Tri-Tier Evidence Grounding**:
   - **Self-RAG (Qdrant)**: Retrieves legally enforceable platform policies (Statutory 14-day window, latent defect exceptions, transit damage subrogation).
   - **GraphRAG (:8011)**: Cross-checks if the product is subject to an active Tier-1 OEM component defect or batch recall.
   - **ClickHouse (:8123)**: Computes historical customer refund abuse scores and merchant chargeback risk.
3. **Equitable Liability Allocation**: When a latent factory defect is confirmed, merchant liability is reduced to **0%**, full 100% refund is awarded to the customer, and a **Corrective Action Request (CAR)** is generated for the culpable OEM supplier.
4. **Autonomous Settlement vs. Human Escalation**: Automatically settles and emits financial refund events for low-risk claims ($\le \$200$), while escalating high-dollar or high-risk claims to compliance officers.

### Enterprise NFRs & Graceful Traffic Draining
- **Redis API Idempotency (`@idempotent_api`)**: Evaluates `X-Idempotency-Key` headers in Redis with atomic locks to prevent duplicate claim creation or double refunds during network retries.
- **Graceful Shutdown & Readiness Draining**: Implements a dedicated `/health/ready` probe. When a SIGTERM/SIGINT signal arrives, the service marks itself not ready (returning 503 so Traefik removes the instance from load-balancing), waits for in-flight LangGraph multi-agent debates to finish within a 5-second drain window, and safely closes Redis/Kafka connection pools.
- **Circuit Breaker Resilience**: Protects upstream GraphRAG and ClickHouse integrations with `AsyncCircuitBreaker` and deterministic heuristic fallbacks.
- **Multi-Tenant Scoping**: All claims, policies, and metrics are isolated per tenant via `X-Tenant-ID`.

### Copy-Pasteable Curl Examples

#### 1. Hardware Defect (Triggers GraphRAG Defect Link & Supplier CAR)
```bash
curl -s -X POST http://localhost/disputes/claims \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -H "X-Idempotency-Key: claim-defect-001" \
  -d '{
    "order_id": "ord-live-7701",
    "customer_id": 14,
    "product_name": "Gaming Laptop Pro (32GB RAM, RTX 4080)",
    "claim_amount": 1899.99,
    "reason": "DEFECTIVE_PRODUCT",
    "customer_statement": "The laptop overheats and thermal throttles during video rendering within 5 minutes.",
    "delivery_days_ago": 6
  }'
```

#### 2. Out-of-Window Discretionary Return (Triggers Claim Denial)
```bash
curl -s -X POST http://localhost/disputes/claims \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -H "X-Idempotency-Key: claim-remorse-002" \
  -d '{
    "order_id": "ord-live-8802",
    "customer_id": 99,
    "product_name": "RGB Desk Mat (Extra Large)",
    "claim_amount": 39.99,
    "reason": "BUYER_REMORSE",
    "customer_statement": "I just decided I dont like the color anymore.",
    "delivery_days_ago": 30
  }'
```

#### 3. Idempotent Retry Demonstration (Instant Cached 201 Response)
```bash
# Re-sending the exact same request with the same X-Idempotency-Key returns cached result instantaneously:
curl -i -s -X POST http://localhost/disputes/claims \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -H "X-Idempotency-Key: claim-defect-001" \
  -d '{
    "order_id": "ord-live-7701",
    "customer_id": 14,
    "product_name": "Gaming Laptop Pro (32GB RAM, RTX 4080)",
    "claim_amount": 1899.99,
    "reason": "DEFECTIVE_PRODUCT",
    "customer_statement": "The laptop overheats and thermal throttles during video rendering within 5 minutes.",
    "delivery_days_ago": 6
  }'
```

#### 4. Readiness & Platform Dispute Statistics
```bash
# Readiness Probe (Traffic Draining)
curl -s http://localhost/disputes/health/ready

# Dispute Aggregates
curl -s http://localhost/disputes/stats
```