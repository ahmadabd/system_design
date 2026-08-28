from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class GraphQueryRequest(BaseModel):
    query: str = Field(..., description="Natural language root-cause, supplier, or multi-hop question")
    tenant_id: Optional[str] = Field("store_tech", description="Multi-tenant store ID")
    session_id: Optional[str] = Field(None, description="Optional conversational session ID")
    search_mode: Optional[str] = Field("auto", description="Search mode: 'auto', 'local_multihop', 'global_community'")
    max_hops: Optional[int] = Field(2, description="Maximum relational traversal hops for local search (1-3)")


class GraphQueryResponse(BaseModel):
    session_id: str
    query: str
    tenant_id: str
    search_mode: str
    seed_entities: List[str]
    nodes_traversed_count: int
    edges_traversed_count: int
    communities_consulted_count: int
    reasoning_hops: List[str]
    final_markdown_report: str
    mermaid_subgraph: Optional[str] = None
    confidence_score: float = 1.0


class SubgraphVisualizationResponse(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]
    mermaid_code: str
    community_count: int


class GraphStatsResponse(BaseModel):
    status: str
    total_nodes: int
    total_edges: int
    total_communities: int
    node_types_breakdown: Dict[str, int]
    edge_types_breakdown: Dict[str, int]
