from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ProductItemDTO(BaseModel):
    id: int
    name: str
    price: float
    stock: int
    store_id: int
    category: Optional[str] = "Electronics"
    specs: Optional[str] = None
    similarity_score: Optional[float] = 0.0


class DiscoveryChatRequest(BaseModel):
    query: str = Field(..., description="User search, question, or bundle request")
    session_id: str = Field(default="default_session", description="Conversational session identifier")
    tenant_id: str = Field(default="store_tech", description="Multi-tenant store partition")
    budget: Optional[float] = Field(default=None, description="Optional maximum price limit")
    category: Optional[str] = Field(default=None, description="Optional category filter")


class BundleDiscoveryRequest(BaseModel):
    target_intent: str = Field(..., description="Purpose of bundle, e.g., 'Programming Setup' or 'Gaming Station'")
    budget: float = Field(..., gt=0, description="Strict total price ceiling")
    tenant_id: str = Field(default="store_tech", description="Tenant store identifier")
    required_categories: Optional[List[str]] = Field(
        default_factory=lambda: ["Laptops", "Keyboards", "Monitors"],
        description="List of component categories to build the bundle with"
    )


class BundleRecommendationDTO(BaseModel):
    bundle_name: str
    total_price: float
    budget: float
    remaining_budget: float
    items: List[ProductItemDTO]
    summary_rationale: str


class DiscoveryResponse(BaseModel):
    session_id: str
    query: str
    tenant_id: str
    parsed_constraints: Dict[str, Any]
    sub_queries_generated: List[str]
    hyde_hypothetical_spec: Optional[str] = None
    recommended_bundle: Optional[BundleRecommendationDTO] = None
    candidate_products: List[ProductItemDTO] = []
    final_markdown_response: str
