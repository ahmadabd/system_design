# Support Service: Architecture & AI Engineering Guide

Welcome to the **Support Service** module. This service delivers an enterprise-grade, agentic AI Customer Support assistant integrated into our multi-tenant e-commerce platform.

---

## 1. System Architecture Overview

The `support-service` is structured according to **Clean Architecture** (Domain-Driven Design), matching all other microservices in the platform:

```
                           +----------------------+
                           |   Traefik Gateway    |
                           |     (Port 80/443)    |
                           +----------+-----------+
                                      |
         +-------------+--------------+---------------+-------------+
         |             |              |               |             |
   /users        /products        /orders        /payments     /support (NEW)
         |             |              |               |             |
+--------v----+ +------v-----+ +------v----+  +-------v-----+ +-----v-------------+
| user-       | | product-   | | order-    |  | payment-    | | support-service   |
| service     | | service    | | service   |  | service     | | (FastAPI + RAG)   |
| (Postgres)  | | (Postgres) | | (Postgres)|  | (Postgres)  | | (LangChain/Graph) |
+-------------+ +------------+ +-----------+  +-------------+ +-----+-------+-----+
                                                                     |       |
                                      +------------------------------+       |
                                      | (Resilient Tool Adapters)            | (Vector Index)
                                      v                                      v
                        [Order/Product/User Services]                  [Qdrant DB]
                                                                       (:6333)
```

---

## 2. Core Concepts: RAG vs. Agentic RAG (AI Engineering Fundamentals)

### A. What is RAG (Retrieval-Augmented Generation)?
Large Language Models (LLMs) have a fixed training cutoff and do not know your internal business rules, live order data, or private documents. 

**Standard RAG Pipeline**:
1. **Ingestion**: Raw documents -> Split into Chunks -> Generate Embeddings (Dense Vector Representation) -> Store in Vector DB (Qdrant).
2. **Retrieval**: User Query -> Generate Query Embedding -> Cosine Similarity Search -> Retrieve Top-K relevant chunks.
3. **Generation**: Combine Retrieved Chunks + User Question in a Contextual Prompt -> Pass to LLM -> Generate grounded, hallucination-free answer.

$$\text{Similarity}(q, d) = \frac{\mathbf{q} \cdot \mathbf{d}}{\|\mathbf{q}\| \|\mathbf{d}\|}$$

### B. Why Agentic RAG with LangGraph?
Standard RAG fails when users ask questions requiring dynamic data or actions (e.g., *"Where is my order #1042?"* or *"Can I cancel order #982?"*).

| Aspect | Standard RAG | Agentic RAG (LangGraph) |
| :--- | :--- | :--- |
| **Data Scope** | Static text (PDFs, Markdown policies) | Hybrid: Static policies + Live REST/DB Tools |
| **Execution Flow** | Fixed linear: Retrieve -> Generate | Dynamic State Machine: Route -> Retrieve -> Tool Call -> Grade -> Generate |
| **Self-Correction** | None (can hallucinate on irrelevant docs) | Reflection node grades retrieved docs for relevance before answering |
| **Multi-turn Memory** | Lost per request unless re-sent | Graph Checkpointer (Redis) preserves multi-turn session state |

---

## 3. LangGraph State Machine Architecture (Roadmap)

```
[User Message] -> [1. Intent Router Node]
                      |-- Policy / FAQ -> [2. Qdrant RAG Retriever] -> [Doc Grader] -> [Generate]
                      |-- Order / Action -> [3. Service API Tools] -> [Generate]
                      \-- Hybrid -> [4. Hybrid Node] -> [Generate]
```

---

## 4. Technology Stack

- **Framework**: FastAPI (Python 3.12, AsyncIO)
- **Orchestration**: LangChain, LangChain-Core, LangChain-Qdrant, LangGraph
- **Vector Database**: Qdrant (`qdrant/qdrant:latest`) on port `6333`
- **Embeddings**: `fastembed` (`BAAI/bge-small-en-v1.5` - fast, local, zero-cost, 384 dimensions)
- **LLM Provider**: OpenRouter API
  - **Default Model**: `nvidia/nemotron-3.5-lightning:free`
- **Observability**: OpenTelemetry OTLP (`otel-collector:4317`), Structured JSON Logging (Loki), Prometheus metrics
- **Resilience**: Asynchronous Circuit Breakers (`shared.common.resilience`), Redis Idempotency (`shared.common.idempotency`), Traefik rate limiting

---

## 5. Directory Structure

```
services/support-service/
├── ARCHITECTURE.md          # This documentation
├── Dockerfile               # Container definition
├── requirements.txt         # Dependencies
├── .env.example             # Configuration template
├── knowledge_base/          # E-Commerce markdown policy documents
│   ├── return_refund_policy.md
│   ├── shipping_delivery_sla.md
│   ├── damaged_missing_items.md
│   ├── order_cancellation_policy.md
│   └── payment_faq.md
└── src/
    ├── domain/              # Business entities & data models
    ├── application/         # Ingestion pipeline & RAG service use cases
    ├── adapter/             # Qdrant & OpenRouter clients with circuit breakers
    ├── infrastructure/      # Config, Vector DB setup, and LLM setup
    ├── presentation/        # FastAPI REST endpoints & schemas
    └── main.py              # Application entrypoint & lifespan
```
