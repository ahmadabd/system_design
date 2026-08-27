from typing import Sequence, Dict, Any, Optional, List, Annotated
from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from src.domain.schemas import ProductItemDTO, BundleRecommendationDTO


class DiscoveryState(TypedDict):
    """
    LangGraph Typed State for Semantic Product Discovery and Bundle Building.
    Maintains user conversation turns, extracted constraints, HyDE documents,
    retrieved and reranked candidates, and the optimized bundle solution.
    """
    # Conversation memory
    messages: Annotated[Sequence[BaseMessage], add_messages]
    session_id: str
    tenant_id: str
    raw_query: str

    # Constraint extraction
    budget: Optional[float]
    category_filter: Optional[str]
    in_stock_only: bool
    parsed_constraints: Dict[str, Any]

    # Advanced RAG artifacts
    sub_queries: List[str]
    hyde_spec: Optional[str]

    # Retrieval & Ranking
    retrieved_candidates: List[Dict[str, Any]]
    reranked_products: List[ProductItemDTO]

    # Optimization & Synthesis
    recommended_bundle: Optional[BundleRecommendationDTO]
    final_response: str
    status: str
