import uuid
import logging
from typing import Optional, List
from fastapi import APIRouter, Header, HTTPException, Query
from opentelemetry import trace

from src.domain.schemas import (
    GraphQueryRequest,
    GraphQueryResponse,
    SubgraphVisualizationResponse,
    GraphStatsResponse
)
from src.domain.graph_entities import GraphNode, GraphEdge
from src.application.workflow import graphrag_app
from src.infrastructure.graph_store import graph_store
from src.adapter.community_detector import HierarchicalCommunityDetector
from src.adapter.qdrant_entity_adapter import qdrant_entity_adapter
from src.infrastructure.metrics import graphrag_requests_total

logger = logging.getLogger("GraphRAGAPI")
router = APIRouter(tags=["Knowledge Graph RAG"])
tracer = trace.get_tracer("knowledge-graph-rag-service")

community_detector = HierarchicalCommunityDetector(graph_store)


@router.post("/query", response_model=GraphQueryResponse)
async def execute_graphrag_query(
    req: GraphQueryRequest,
    x_tenant_id: str = Header(default="store_tech", alias="X-Tenant-ID")
):
    """
    Executes Microsoft GraphRAG inquiry:
    - Automatically routes between Local Multi-Hop Traversal and Global Community Map-Reduce
    - Returns structured causal explanation, reasoning hops, and interactive Mermaid subgraph
    """
    tenant = req.tenant_id or x_tenant_id
    session_id = req.session_id or f"graph_{uuid.uuid4().hex[:8]}"

    with tracer.start_as_current_span("GraphRAG: execute_query") as span:
        span.set_attribute("graphrag.query", req.query)
        span.set_attribute("graphrag.session_id", session_id)
        span.set_attribute("tenant.id", tenant)
        span.set_attribute("graphrag.requested_mode", req.search_mode or "auto")

        initial_state = {
            "query": req.query,
            "tenant_id": tenant,
            "session_id": session_id,
            "search_mode": req.search_mode if req.search_mode in ["local_multihop", "global_community"] else "auto",
            "extracted_seed_entities": [],
            "matched_graph_nodes": [],
            "subgraph_nodes": [],
            "subgraph_edges": [],
            "community_clusters": [],
            "intermediate_map_insights": [],
            "final_markdown_report": "",
            "mermaid_subgraph": "",
            "reasoning_hops": [],
            "confidence_score": 1.0
        }

        try:
            final_state = await graphrag_app.ainvoke(initial_state)
            mode = final_state.get("search_mode", "local_multihop")
            span.set_attribute("graphrag.final_mode", mode)

            graphrag_requests_total.labels(search_mode=mode, status="success", tenant_id=tenant).inc()

            return GraphQueryResponse(
                session_id=session_id,
                query=req.query,
                tenant_id=tenant,
                search_mode=mode,
                seed_entities=final_state.get("extracted_seed_entities", []),
                nodes_traversed_count=len(final_state.get("subgraph_nodes", [])),
                edges_traversed_count=len(final_state.get("subgraph_edges", [])),
                communities_consulted_count=len(final_state.get("community_clusters", [])),
                reasoning_hops=final_state.get("reasoning_hops", []),
                final_markdown_report=final_state.get("final_markdown_report", "Analysis completed."),
                mermaid_subgraph=final_state.get("mermaid_subgraph"),
                confidence_score=1.0
            )
        except Exception as e:
            span.record_exception(e)
            logger.error(f"Error executing GraphRAG query: {e}", exc_info=True)
            graphrag_requests_total.labels(search_mode="error", status="failed", tenant_id=tenant).inc()
            raise HTTPException(status_code=500, detail=f"GraphRAG execution failed: {str(e)}")


@router.get("/subgraph", response_model=SubgraphVisualizationResponse)
async def get_subgraph(
    seeds: Optional[str] = Query(None, description="Comma-separated seed entity IDs (e.g. prod_gaming_laptop_pro,prod_shure_sm7b)"),
    hops: int = Query(2, ge=1, le=3, description="Maximum relational traversal hops")
):
    """Visualizes the entire knowledge graph or a focused k-hop neighborhood subgraph"""
    if seeds:
        seed_list = [s.strip() for s in seeds.split(",") if s.strip()]
        nodes, edges = graph_store.extract_subgraph(seed_node_ids=seed_list, max_hops=hops)
    else:
        nodes = graph_store.get_all_nodes()
        edges = graph_store.get_all_edges()

    mermaid = graph_store.to_mermaid(nodes, edges)
    communities = community_detector.get_cached_communities()

    return SubgraphVisualizationResponse(
        nodes=nodes,
        edges=edges,
        mermaid_code=mermaid,
        community_count=len(communities)
    )


@router.get("/communities")
async def list_communities():
    """Returns detected hierarchical community clusters with summaries and severity ratings"""
    clusters = community_detector.get_cached_communities()
    return {"total_communities": len(clusters), "communities": [c.model_dump() for c in clusters]}


@router.get("/stats", response_model=GraphStatsResponse)
async def get_graph_stats():
    """Returns real-time entity and relation counts and category breakdown"""
    nodes = graph_store.get_all_nodes()
    edges = graph_store.get_all_edges()
    clusters = community_detector.get_cached_communities()

    node_types: dict = {}
    for n in nodes:
        t = n.get("type", "Unknown")
        node_types[t] = node_types.get(t, 0) + 1

    edge_types: dict = {}
    for e in edges:
        r = e.get("relation", "Unknown")
        edge_types[r] = edge_types.get(r, 0) + 1

    return GraphStatsResponse(
        status="healthy",
        total_nodes=len(nodes),
        total_edges=len(edges),
        total_communities=len(clusters),
        node_types_breakdown=node_types,
        edge_types_breakdown=edge_types
    )


@router.get("/nodes/{node_id}")
async def get_graph_node(node_id: str):
    """Fetches a specific node from the knowledge graph"""
    node = graph_store.get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found in Knowledge Graph.")
    return node


@router.post("/nodes")
async def add_graph_node(node: GraphNode):
    """Dynamically adds or updates an entity node in the live Knowledge Graph & Vector Store"""
    graph_store.add_node(node)
    qdrant_entity_adapter.index_entities([node])
    community_detector.detect_communities()
    logger.info(f"Dynamically registered node '{node.id}' ({node.name}) into Knowledge Graph.")
    return {"status": "created", "node_id": node.id, "name": node.name, "type": str(node.type)}


@router.post("/edges")
async def add_graph_edge(edge: GraphEdge):
    """Dynamically adds a directed typed relationship between two entities"""
    graph_store.add_edge(edge)
    community_detector.detect_communities()
    logger.info(f"Dynamically created edge '{edge.source}' --[{edge.relation}]--> '{edge.target}'.")
    return {
        "status": "created",
        "source": edge.source,
        "target": edge.target,
        "relation": str(edge.relation)
    }


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "knowledge-graph-rag-service",
        "graph_engine": "NetworkX MultiDiGraph",
        "vector_backend": "Qdrant",
        "rag_mode": "Microsoft GraphRAG (Local Multi-Hop + Global Community Map-Reduce)"
    }
