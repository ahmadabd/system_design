import logging
from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Header
from langchain_core.messages import HumanMessage
from src.domain.schemas import (
    DiscoveryChatRequest,
    BundleDiscoveryRequest,
    DiscoveryResponse,
    BundleRecommendationDTO,
)
from src.application.graph_builder import build_discovery_graph
from src.adapter.qdrant_adapter import QdrantDiscoveryAdapter
from src.application.metrics import discovery_requests_total

logger = logging.getLogger("DiscoveryAPI")
router = APIRouter()
graph = build_discovery_graph()
qdrant_adapter = QdrantDiscoveryAdapter()


@router.post("/chat", response_model=DiscoveryResponse)
async def chat_discovery(
    request: DiscoveryChatRequest,
    x_tenant_id: str = Header(default="store_tech", alias="X-Tenant-ID")
):
    """
    Executes multi-turn semantic product discovery and conversational state graph.
    """
    effective_tenant = request.tenant_id or x_tenant_id
    config = {"configurable": {"thread_id": request.session_id}}

    initial_state = {
        "messages": [HumanMessage(content=request.query)],
        "session_id": request.session_id,
        "tenant_id": effective_tenant,
        "raw_query": request.query,
        "budget": request.budget,
        "category_filter": request.category,
        "in_stock_only": True,
        "parsed_constraints": {},
        "sub_queries": [],
        "hyde_spec": None,
        "retrieved_candidates": [],
        "reranked_products": [],
        "recommended_bundle": None,
        "final_response": "",
        "status": "in_progress"
    }

    try:
        final_state = await graph.ainvoke(initial_state, config=config)
        discovery_requests_total.labels(request_type="chat", tenant_id=effective_tenant, status="success").inc()
        return DiscoveryResponse(
            session_id=request.session_id,
            query=request.query,
            tenant_id=effective_tenant,
            parsed_constraints=final_state.get("parsed_constraints", {}),
            sub_queries_generated=final_state.get("sub_queries", []),
            hyde_hypothetical_spec=final_state.get("hyde_spec"),
            recommended_bundle=final_state.get("recommended_bundle"),
            candidate_products=final_state.get("reranked_products", []),
            final_markdown_response=final_state.get("final_response", "")
        )
    except Exception as e:
        discovery_requests_total.labels(request_type="chat", tenant_id=effective_tenant, status="error").inc()
        logger.error(f"Discovery graph execution error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bundle", response_model=BundleRecommendationDTO)
async def build_bundle(
    request: BundleDiscoveryRequest,
    x_tenant_id: str = Header(default="store_tech", alias="X-Tenant-ID")
):
    """
    Direct endpoint for building price-optimized bundles within strict budget ceilings.
    """
    effective_tenant = request.tenant_id or x_tenant_id
    config = {"configurable": {"thread_id": f"bundle_{request.target_intent}"}}

    initial_state = {
        "messages": [HumanMessage(content=request.target_intent)],
        "session_id": f"bundle_{request.target_intent}",
        "tenant_id": effective_tenant,
        "raw_query": request.target_intent,
        "budget": request.budget,
        "category_filter": None,
        "in_stock_only": True,
        "parsed_constraints": {"target_categories": request.required_categories or ["Electronics"]},
        "sub_queries": [],
        "hyde_spec": None,
        "retrieved_candidates": [],
        "reranked_products": [],
        "recommended_bundle": None,
        "final_response": "",
        "status": "in_progress"
    }

    try:
        final_state = await graph.ainvoke(initial_state, config=config)
        bundle = final_state.get("recommended_bundle")
        if not bundle:
            discovery_requests_total.labels(request_type="bundle", tenant_id=effective_tenant, status="not_found").inc()
            raise HTTPException(status_code=404, detail="Could not formulate a valid bundle within the budget.")
        discovery_requests_total.labels(request_type="bundle", tenant_id=effective_tenant, status="success").inc()
        return bundle
    except HTTPException:
        raise
    except Exception as e:
        discovery_requests_total.labels(request_type="bundle", tenant_id=effective_tenant, status="error").inc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/index-catalog")
async def index_catalog(
    products: List[Dict[str, Any]],
    tenant_id: str = "store_tech",
    x_tenant_id: str = Header(default="store_tech", alias="X-Tenant-ID")
):
    """
    Directly indexes or updates catalog products in Qdrant for semantic search and payload filtering.
    """
    effective_tenant = tenant_id or x_tenant_id
    count = qdrant_adapter.index_products(products, tenant_id=effective_tenant)
    return {"status": "indexed", "product_count": count, "tenant_id": effective_tenant}


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "discovery-service"}

