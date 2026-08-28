from enum import Enum
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class EntityType(str, Enum):
    PRODUCT = "Product"
    SUPPLIER = "Supplier"
    COMPONENT = "Component"
    DEFECT = "Defect"
    BATCH = "Batch"
    WAREHOUSE = "Warehouse"
    REVIEW = "Review"
    STORE = "Store"


class RelationType(str, Enum):
    SUPPLIED_BY = "SUPPLIED_BY"
    CONTAINS_COMPONENT = "CONTAINS_COMPONENT"
    REPORTED_DEFECT = "REPORTED_DEFECT"
    SHIPPED_FROM = "SHIPPED_FROM"
    CAUSED_RETURN_IN = "CAUSED_RETURN_IN"
    PRODUCED_IN_BATCH = "PRODUCED_IN_BATCH"
    SOLD_BY = "SOLD_BY"
    ASSOCIATED_WITH = "ASSOCIATED_WITH"


class GraphNode(BaseModel):
    id: str
    name: str
    type: EntityType
    description: str = ""
    properties: Dict[str, Any] = Field(default_factory=dict)
    tenant_id: str = "store_tech"


class GraphEdge(BaseModel):
    source: str
    target: str
    relation: RelationType
    weight: float = 1.0
    description: str = ""
    properties: Dict[str, Any] = Field(default_factory=dict)


class CommunityCluster(BaseModel):
    id: int
    level: int = 0
    title: str
    summary: str
    member_node_ids: List[str] = Field(default_factory=list)
    key_findings: List[str] = Field(default_factory=list)
    severity_rating: str = "MEDIUM"  # LOW, MEDIUM, HIGH, CRITICAL
