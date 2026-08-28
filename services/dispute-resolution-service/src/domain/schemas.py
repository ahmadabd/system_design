from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from src.domain.dispute_entities import DisputeReason, DisputeClaim, ResolutionOutcome, DisputeStatus


class CreateDisputeClaimRequest(BaseModel):
    order_id: str
    customer_id: int
    product_id: Optional[str] = None
    product_name: str
    claim_amount: float
    reason: DisputeReason
    customer_statement: str
    delivery_days_ago: int = 5
    evidence_urls: List[str] = Field(default_factory=list)


class DisputeClaimResponse(BaseModel):
    claim_id: str
    order_id: str
    tenant_id: str
    status: DisputeStatus
    reason: DisputeReason
    claim_amount: float
    outcome: Optional[ResolutionOutcome] = None
    refund_amount: float = 0.0
    is_auto_settled: bool = False
    requires_human_approval: bool = False
    negotiation_turns_count: int = 0
    buyer_advocate_summary: str = ""
    merchant_defender_summary: str = ""
    judicial_rationale: str = ""
    car_issued_to_supplier: Optional[str] = None
    action_items: List[str] = Field(default_factory=list)
    claim: DisputeClaim


class DisputeStatsResponse(BaseModel):
    status: str = "healthy"
    total_claims: int
    auto_settled_claims: int
    escalated_claims: int
    total_refunded_amount: float
    outcomes_breakdown: Dict[str, int]
    reasons_breakdown: Dict[str, int]
