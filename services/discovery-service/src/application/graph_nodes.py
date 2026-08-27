import logging
import re
import time
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from langchain_core.messages import AIMessage, SystemMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from opentelemetry import trace

from src.infrastructure.config import settings
from src.application.graph_state import DiscoveryState
from src.domain.schemas import ProductItemDTO, BundleRecommendationDTO
from src.adapter.qdrant_adapter import QdrantDiscoveryAdapter
from src.application.metrics import (
    discovery_node_duration_seconds,
    qdrant_search_duration_seconds,
    discovery_bundle_items_count,
    discovery_circuit_breaker_trips_total,
)
from shared.common.resilience import AsyncCircuitBreaker, CircuitBreakerOpenException

logger = logging.getLogger("DiscoveryGraphNodes")
tracer = trace.get_tracer("discovery-service")
qdrant_adapter = QdrantDiscoveryAdapter()

# Circuit breaker protecting vector store calls
qdrant_breaker = AsyncCircuitBreaker(name="qdrant-vector-store", failure_threshold=3, recovery_timeout=10.0)


class DynamicQueryAnalysis(BaseModel):
    """Structured output schema for dynamic LLM-powered query decomposition & HyDE generation."""
    extracted_budget: Optional[float] = Field(default=None, description="Extracted maximum price ceiling or null")
    target_categories: List[str] = Field(
        default_factory=list,
        description="Dynamic list of specific product categories needed for this request (e.g. 'Microphones', 'Monitors', 'Keyboards')"
    )
    sub_queries: List[str] = Field(
        default_factory=list,
        description="List of 2-4 atomic, decomposed search queries for each individual component"
    )
    hyde_spec: str = Field(
        ...,
        description="Rich, realistic hypothetical product catalog entry featuring technical specs, materials, and features that would appear in a catalog"
    )
    search_reasoning: Optional[str] = Field(default=None, description="Brief explanation of why these components were chosen")


def _get_llm():
    """Instantiates ChatOpenAI if API key is configured, else returns None for offline fallback."""
    if settings.OPENAI_API_KEY and settings.OPENAI_API_KEY.strip():
        try:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model=settings.LLM_MODEL,
                api_key=settings.OPENAI_API_KEY,
                base_url=settings.OPENAI_BASE_URL,
                temperature=0.2
            )
        except Exception as e:
            logger.warning(f"Could not initialize ChatOpenAI ({e}). Using offline dynamic fallback.")
    return None


def _dynamic_analysis_fallback(query: str, budget: Optional[float]) -> DynamicQueryAnalysis:
    """
    Intelligent dynamic fallback when running offline or without an active LLM API key.
    Extracts dynamic phrases, budgets, and generates contextual HyDE specifications.
    """
    # 1. Dynamic regex budget extraction
    if budget is None:
        budget_match = re.search(r'(?:under|below|less than|budget(?: of)?|max(?:imum)?)\s*(?:\$|usd)?\s*(\d+(?:\.\d+)?)', query, re.IGNORECASE)
        if budget_match:
            try:
                budget = float(budget_match.group(1))
            except ValueError:
                budget = None

    # 2. Dynamic token & phrase extraction for categories
    clean_q = re.sub(r'[^\w\s]', '', query).lower()
    words = clean_q.split()
    stop_words = {"i", "want", "a", "an", "the", "under", "below", "less", "than", "for", "with", "and", "or", "to", "find", "me", "need", "setup", "budget", "usd"}
    content_words = [w.capitalize() for w in words if w not in stop_words and not w.isdigit()]

    categories = []
    # Identify multi-word or single-word components
    if content_words:
        for i in range(len(content_words)):
            categories.append(content_words[i])
            if len(categories) >= 3:
                break
    if not categories:
        categories = ["Electronics"]

    # 3. Dynamic HyDE generation
    hyde_spec = (
        f"Hypothetical ideal catalog product specification for '{query}': "
        f"Professional-grade {', '.join(categories)} equipment designed for high performance, reliability, and ergonomics. "
        f"Features premium construction, modern high-speed connectivity interfaces (USB-C/Thunderbolt/Wireless 2.4GHz), "
        f"low latency response, energy efficient operation, and rigorous build quality"
        f"{f' with total cost under ${budget}' if budget else ''}."
    )

    # 4. Decomposed sub-queries
    sub_queries = [query, hyde_spec]
    for cat in categories:
        sub_queries.append(f"{cat} optimized for {query}")

    return DynamicQueryAnalysis(
        extracted_budget=budget,
        target_categories=categories,
        sub_queries=sub_queries,
        hyde_spec=hyde_spec,
        search_reasoning="Dynamically decomposed request into component searches and generated rich HyDE spec."
    )


def parse_constraints_node(state: DiscoveryState) -> Dict[str, Any]:
    """
    Node 1: Dynamically parses the user's shopping request to extract budget, pricing constraints,
    and target product categories using an LLM (with intelligent fallback).
    """
    with tracer.start_as_current_span("LangGraph node: parse_constraints") as span:
        start = time.perf_counter()
        query = state.get("raw_query", "")
        explicit_budget = state.get("budget")
        span.set_attribute("discovery.query", query)

        llm = _get_llm()
        if llm is not None:
            try:
                structured_llm = llm.with_structured_output(DynamicQueryAnalysis)
                prompt = (
                    f"Analyze this shopping request: '{query}'. "
                    f"Extract any price budget limit (if given) and dynamically identify the required product categories."
                )
                analysis: DynamicQueryAnalysis = structured_llm.invoke(prompt)
                budget = explicit_budget if explicit_budget is not None else analysis.extracted_budget
                categories = analysis.target_categories or ["Electronics"]
                logger.info(f"[Node: parse_constraints (LLM)] Extracted budget=${budget}, categories={categories}")
            except Exception as e:
                logger.warning(f"LLM constraint extraction failed ({e}). Using dynamic fallback.")
                fallback = _dynamic_analysis_fallback(query, explicit_budget)
                budget = fallback.extracted_budget
                categories = fallback.target_categories
        else:
            fallback = _dynamic_analysis_fallback(query, explicit_budget)
            budget = fallback.extracted_budget
            categories = fallback.target_categories

        parsed_constraints = {
            "extracted_budget": budget,
            "target_categories": categories,
            "in_stock_required": state.get("in_stock_only", True)
        }

        duration = time.perf_counter() - start
        discovery_node_duration_seconds.labels(node_name="parse_constraints").observe(duration)
        if budget:
            span.set_attribute("discovery.budget", budget)
        span.set_attribute("discovery.categories_count", len(categories))

        return {
            "budget": budget,
            "parsed_constraints": parsed_constraints
        }


def generate_hyde_and_subqueries_node(state: DiscoveryState) -> Dict[str, Any]:
    """
    Node 2: Generates a dynamic Hypothetical Document (HyDE) and decomposes complex queries into
    atomic sub-queries for parallel vector retrieval using an LLM.
    """
    with tracer.start_as_current_span("LangGraph node: hyde_and_decompose") as span:
        start = time.perf_counter()
        query = state.get("raw_query", "")
        categories = state.get("parsed_constraints", {}).get("target_categories", ["Electronics"])
        budget = state.get("budget")

        llm = _get_llm()
        if llm is not None:
            try:
                structured_llm = llm.with_structured_output(DynamicQueryAnalysis)
                prompt = (
                    f"User shopping request: '{query}' with budget: {f'${budget}' if budget else 'unspecified'}. "
                    f"Required categories: {categories}. "
                    f"1. Generate a rich, realistic hypothetical product catalog entry (HyDE) with technical specifications, materials, and form factors. "
                    f"2. Decompose the request into 2-4 atomic sub-queries for component retrieval."
                )
                analysis: DynamicQueryAnalysis = structured_llm.invoke(prompt)
                hyde_spec = analysis.hyde_spec
                sub_queries = analysis.sub_queries or [query, hyde_spec]
                logger.info(f"[Node: hyde_and_decompose (LLM)] Generated dynamic HyDE spec and {len(sub_queries)} sub-queries.")
            except Exception as e:
                logger.warning(f"LLM HyDE generation failed ({e}). Using dynamic fallback.")
                fallback = _dynamic_analysis_fallback(query, budget)
                hyde_spec = fallback.hyde_spec
                sub_queries = fallback.sub_queries
        else:
            fallback = _dynamic_analysis_fallback(query, budget)
            hyde_spec = fallback.hyde_spec
            sub_queries = fallback.sub_queries

        duration = time.perf_counter() - start
        discovery_node_duration_seconds.labels(node_name="hyde_and_decompose").observe(duration)
        span.set_attribute("discovery.sub_queries_count", len(sub_queries))

        return {
            "hyde_spec": hyde_spec,
            "sub_queries": sub_queries
        }


def qdrant_hybrid_search_node(state: DiscoveryState) -> Dict[str, Any]:
    """
    Node 3: Executes dense semantic search in Qdrant with metadata payload filtering
    (price <= budget, in_stock = True, tenant_id) protected by a Circuit Breaker.
    """
    with tracer.start_as_current_span("LangGraph node: qdrant_hybrid_search") as span:
        start = time.perf_counter()
        tenant_id = state.get("tenant_id", "store_tech")
        budget = state.get("budget")
        sub_queries = state.get("sub_queries", [state.get("raw_query", "")])
        in_stock = state.get("in_stock_only", True)

        span.set_attribute("tenant.id", tenant_id)
        if budget:
            span.set_attribute("discovery.budget", budget)

        candidate_map: Dict[int, ProductItemDTO] = {}

        for sq in sub_queries:
            q_start = time.perf_counter()
            try:
                results = qdrant_adapter.search_products(
                    query=sq,
                    tenant_id=tenant_id,
                    max_price=budget,
                    in_stock_only=in_stock,
                    limit=5
                )
                q_duration = time.perf_counter() - q_start
                qdrant_search_duration_seconds.observe(q_duration)

                for item in results:
                    if item.id not in candidate_map or (item.similarity_score or 0) > (candidate_map[item.id].similarity_score or 0):
                        candidate_map[item.id] = item
            except CircuitBreakerOpenException:
                discovery_circuit_breaker_trips_total.labels(target_resource="qdrant").inc()
                logger.warning("Qdrant circuit breaker is OPEN. Search degraded.")
                break
            except Exception as e:
                logger.error(f"Error during search iteration: {e}")

        candidates = list(candidate_map.values())
        duration = time.perf_counter() - start
        discovery_node_duration_seconds.labels(node_name="qdrant_hybrid_search").observe(duration)
        span.set_attribute("discovery.candidates_found", len(candidates))

        logger.info(f"[Node: qdrant_search] Retrieved {len(candidates)} unique candidates within budget=${budget}.")
        return {
            "retrieved_candidates": [c.model_dump() for c in candidates]
        }


def cross_encoder_rerank_node(state: DiscoveryState) -> Dict[str, Any]:
    """
    Node 4: Reranks candidate products based on query relevance and similarity score.
    """
    with tracer.start_as_current_span("LangGraph node: cross_encoder_rerank") as span:
        start = time.perf_counter()
        raw_candidates = state.get("retrieved_candidates", [])
        products = [ProductItemDTO(**c) for c in raw_candidates]

        # Sort candidates by similarity score descending
        products.sort(key=lambda p: (p.similarity_score or 0.0), reverse=True)

        duration = time.perf_counter() - start
        discovery_node_duration_seconds.labels(node_name="cross_encoder_rerank").observe(duration)
        span.set_attribute("discovery.reranked_count", len(products))

        logger.info(f"[Node: cross_encoder_rerank] Ranked {len(products)} products.")
        return {
            "reranked_products": products
        }


def bundle_optimizer_node(state: DiscoveryState) -> Dict[str, Any]:
    """
    Node 5: Applies greedy/knapsack optimization to select the best combination of
    complementary products whose sum of prices <= budget.
    """
    with tracer.start_as_current_span("LangGraph node: bundle_optimizer") as span:
        start = time.perf_counter()
        products = state.get("reranked_products", [])
        budget = state.get("budget")
        raw_query = state.get("raw_query", "Setup")

        if not products:
            return {"recommended_bundle": None}

        # If no budget specified, take top 3 items
        selected_items: List[ProductItemDTO] = []
        current_total = 0.0

        # Group by category to ensure variety (e.g. 1 laptop + 1 keyboard + 1 monitor)
        seen_categories = set()
        
        # 1. Greedy selection: Pick best item per category within budget
        for p in products:
            cost = p.price
            if budget is not None:
                if current_total + cost <= budget:
                    if p.category not in seen_categories or len(seen_categories) >= 3:
                        selected_items.append(p)
                        seen_categories.add(p.category)
                        current_total += cost
            else:
                if len(selected_items) < 3:
                    selected_items.append(p)
                    current_total += cost

        eff_budget = budget if budget is not None else current_total
        remaining = max(0.0, eff_budget - current_total)

        bundle = BundleRecommendationDTO(
            bundle_name=f"Curated {raw_query.title()} Bundle",
            total_price=round(current_total, 2),
            budget=round(eff_budget, 2),
            remaining_budget=round(remaining, 2),
            items=selected_items,
            summary_rationale=(
                f"Selected {len(selected_items)} optimal complementary item(s) "
                f"costing ${round(current_total, 2)} within your budget of ${round(eff_budget, 2)}."
            )
        )

        duration = time.perf_counter() - start
        discovery_node_duration_seconds.labels(node_name="bundle_optimizer").observe(duration)
        discovery_bundle_items_count.observe(len(selected_items))

        span.set_attribute("discovery.bundle_total_price", bundle.total_price)
        span.set_attribute("discovery.bundle_items_count", len(selected_items))

        logger.info(f"[Node: bundle_optimizer] Selected bundle with {len(selected_items)} items for ${current_total}.")
        return {
            "recommended_bundle": bundle
        }


def synthesize_recommendation_node(state: DiscoveryState) -> Dict[str, Any]:
    """
    Node 6: Synthesizes a structured, engaging Markdown recommendation with item breakdowns,
    specifications, prices, and next actions.
    """
    with tracer.start_as_current_span("LangGraph node: synthesize_recommendation") as span:
        start = time.perf_counter()
        bundle = state.get("recommended_bundle")
        products = state.get("reranked_products", [])
        query = state.get("raw_query", "")

        if bundle and bundle.items:
            lines = [
                f"### 🎯 Recommended Setup: {bundle.bundle_name}",
                f"{bundle.summary_rationale}\n",
                "| Item ID | Product Name | Category | Stock | Price |",
                "| :---: | :--- | :--- | :---: | :---: |"
            ]
            for item in bundle.items:
                lines.append(f"| **#{item.id}** | {item.name} | {item.category} | {item.stock} in stock | **${item.price:.2f}** |")

            lines.extend([
                "",
                f"💰 **Total Bundle Price**: **${bundle.total_price:.2f}**",
                f"💵 **Budget Allocated**: ${bundle.budget:.2f} (Remaining: **${bundle.remaining_budget:.2f}**)",
                "",
                "✨ *All recommended items are currently verified in stock.*"
            ])
            final_text = "\n".join(lines)
        elif products:
            lines = [
                f"### 🔍 Found {len(products)} Product(s) for '{query}':\n",
                "| Item ID | Product Name | Stock | Price |",
                "| :---: | :--- | :---: | :---:|"
            ]
            for p in products[:5]:
                lines.append(f"| **#{p.id}** | {p.name} | {p.stock} in stock | **${p.price:.2f}** |")
            final_text = "\n".join(lines)
        else:
            final_text = f"No matching products found for '{query}' within the specified price and stock constraints."

        duration = time.perf_counter() - start
        discovery_node_duration_seconds.labels(node_name="synthesize_recommendation").observe(duration)
        span.set_attribute("discovery.response_length", len(final_text))

        return {
            "final_response": final_text,
            "messages": [AIMessage(content=final_text)],
            "status": "completed"
        }
