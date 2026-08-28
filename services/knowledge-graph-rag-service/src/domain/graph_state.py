from typing import TypedDict, List, Dict, Any, Optional
from src.domain.graph_entities import GraphNode, GraphEdge, CommunityCluster


class GraphRAGState(TypedDict):
    """
    LangGraph state representation passed through all nodes in the GraphRAG pipeline.
    Supports both Local Multi-Hop Traversal and Global Community Map-Reduce.
    """
    query: str
    tenant_id: str
    session_id: str
    search_mode: str  # "local_multihop", "global_community", or "hybrid"
    extracted_seed_entities: List[str]
    matched_graph_nodes: List[Dict[str, Any]]
    subgraph_nodes: List[Dict[str, Any]]
    subgraph_edges: List[Dict[str, Any]]
    community_clusters: List[Dict[str, Any]]
    intermediate_map_insights: List[Dict[str, Any]]
    final_markdown_report: str
    mermaid_subgraph: str
    reasoning_hops: List[str]
    confidence_score: float
