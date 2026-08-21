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

## 3. LangGraph State Machine Architecture

The support agent uses a multi-node cyclical state machine compiled with LangGraph:

```
                      [ User Message ]
                             │
                             ▼
                     [ 1. router_node ]
                             │
       ┌─────────────────────┼──────────────────────┬──────────────────────┐
       ▼ (policy_faq)        ▼ (hybrid)             ▼ (order_inquiry)      ▼ (order_action)
 [ 2. retrieve_node ]  [ 2. retrieve_node ]   [ 3. tools_node ]      [ 7. prepare_action_node ]
 (Dense + BM25 +       (Dense + BM25 +        (Queries order/prod    (Checks eligibility & sets
  FlashRank Re-rank)    FlashRank Re-rank)     microservices)         pending_action approval)
       │                     │                      │                      │
       ▼                     ▼                      ▼                      ▼
 [ 4. grade_docs_node ] [ 3. tools_node ]           │             [ ⏸️ Breakpoint: execute_action ]
       │                     │                      │             (Pauses execution until
       │                     ▼                      │              confirmed via /actions/confirm)
       │               [ 4. grade_docs_node ]       │                      │
       └─────────────────────┼──────────────────────┘                      ▼
                             ▼                                    [ 8. execute_action_node ]
                     [ 5. generate_node ]                         (Executes order cancellation
                             │                                     in order-service)
                             ▼
                 [ 6. check_hallucination_node ]
                             │
              ┌──────────────┴──────────────┐
              ▼ (grounded)                  ▼ (not_grounded & retries <= 2)
            [ END ]                 [ Self-Correction Loop ]
       (Deliver Answer)             (Re-generates with feedback)
```

---

## 4. Advanced AI Engineering Patterns Implemented

### A. Two-Stage Hybrid Search & Reciprocal Rank Fusion (RRF)
- **Dense Vector Search**: `Qdrant` with `BAAI/bge-small-en-v1.5` embeddings captures high-level semantic intent.
- **Sparse Keyword Search**: `BM25Okapi` captures exact keyword matches ("SLA", "COD", "$250", "30-day", exact product names).
- **Rank Fusion**: Combines rankings using:
  $$RRF(d) = \sum_{m \in M} \frac{1}{60 + \text{rank}_m(d)}$$

### B. FlashRank Cross-Encoder Re-Ranking Pipeline
- Instead of feeding all candidate chunks into the prompt (which inflates token cost and causes "lost-in-the-middle" attention degradation), we re-score the Top-15 fused chunks using **FlashRank** (`ms-marco-TinyBERT-L-2-v2`).
- Runs locally on CPU in $< 20\text{ms}$ with $\$0$ marginal inference cost.

### C. Self-RAG (Hallucination Checking & Grounding Reflection)
- An independent **LLM-as-a-Judge Fact-Checker** evaluates draft answers against the Ground Truth context before delivery.
- If ungrounded claims are detected, it triggers an autonomous reflection loop with corrective instructions (bounded by `max_retries=2`).

### D. Human-in-the-Loop (HITL) with LangGraph Breakpoints
- High-stakes mutations (e.g. order cancellations and refund requests) pause at the `interrupt_before=["execute_action"]` breakpoint.
- The state is persisted in the checkpointer, returning `status: "pending_approval"`.
- Resumed safely only when the customer/admin submits `POST /support/actions/confirm` with `approved: true`.

### E. Automated RAG Triad Evaluation Suite
- Curated golden benchmark dataset (`eval/benchmark_dataset.json`) containing 10 diverse enterprise test cases.
- Automated evaluator (`eval/evaluator.py`) scoring the 3 RAG Triad dimensions:
  - **Context Relevance ($S_{context}$)**: Retriever precision (Target $\ge 0.85$).
  - **Faithfulness ($S_{faithful}$)**: Hallucination rate (Target $\ge 0.90$).
  - **Answer Relevance ($S_{answer}$)**: User intent satisfaction (Target $\ge 0.85$).
- Executable benchmark runner (`eval/run_evaluation.py`) outputting Markdown/JSON reports.

---

## 5. Technology Stack

- **Framework**: FastAPI (Python 3.11/3.12, AsyncIO)
- **Orchestration**: LangGraph, LangChain-Core, LangChain-Qdrant
- **Vector Database**: Qdrant (`qdrant/qdrant:latest`) on port `6333`
- **Sparse Keyword Search**: `rank-bm25` (`BM25Okapi`)
- **Cross-Encoder Re-ranking**: `flashrank` (`ms-marco-TinyBERT-L-2-v2`)
- **Embeddings**: `fastembed` (`BAAI/bge-small-en-v1.5` - 384 dimensions)
- **LLM Provider**: OpenRouter API (`nvidia/nemotron-3.5-lightning:free`)
- **Observability**: OpenTelemetry OTLP (`otel-collector:4317`), Structured JSON Logging (Loki), Prometheus metrics
- **Resilience**: Asynchronous Circuit Breakers (`shared.common.resilience`), Redis Idempotency (`shared.common.idempotency`)

---

## 6. Directory Structure

```
services/support-service/
├── ARCHITECTURE.md          # Architecture & engineering documentation
├── Dockerfile               # Container definition
├── requirements.txt         # Dependencies
├── eval/                    # Automated RAG Triad evaluation suite
│   ├── benchmark_dataset.json  # Golden test dataset
│   ├── evaluator.py            # RAG Triad metric evaluators
│   └── run_evaluation.py       # Benchmark runner script
├── knowledge_base/          # E-Commerce markdown policy documents
│   ├── return_refund_policy.md
│   ├── shipping_delivery_sla.md
│   ├── damaged_missing_items.md
│   ├── order_cancellation_policy.md
│   └── payment_faq.md
└── src/
    ├── domain/              # Business models & DTOs
    ├── application/         # Graph nodes, state, builder, ingestion & RAG services
    ├── adapter/             # Qdrant, BM25, FlashRank, OpenRouter & microservice clients
    ├── infrastructure/      # Config, Vector DB setup, and LLM setup
    ├── presentation/        # FastAPI REST endpoints & schemas
    └── main.py              # Application entrypoint & lifespan
```

---

## 7. cURL Testing Guide

### 1. Pure Policy FAQ Query
```bash
curl -X POST http://localhost/support/chat \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "session_id": "policy-test-01",
    "message": "What is the return window for clothing vs electronics, and who pays shipping?"
  }'
```

### 2. Live Order + Policy Hybrid Query (Self-RAG)
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

### 3. Human-in-the-Loop Order Cancellation (Breakpoint Pause)
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

### 4. Human Approval Confirmation (Resume & Mutate)
```bash
curl -X POST http://localhost/support/actions/confirm \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: store_tech" \
  -d '{
    "session_id": "hitl-test-01",
    "approved": true
  }'
```

### 5. Run Automated RAG Triad Benchmark
```bash
docker compose exec support-service python eval/run_evaluation.py
curl http://localhost/support/eval/benchmark
```

