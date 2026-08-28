from enum import Enum
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field


class DisputeReason(str, Enum):
    DEFECTIVE_PRODUCT = "DEFECTIVE_PRODUCT"
    TRANSIT_DAMAGE = "TRANSIT_DAMAGE"
    WRONG_ITEM = "WRONG_ITEM"
    NON_DELIVERY = "NON_DELIVERY"
    BUYER_REMORSE = "BUYER_REMORSE"
    UNAUTHORIZED_TRANSACTION = "UNAUTHORIZED_TRANSACTION"


class ResolutionOutcome(str, Enum):
    FULL_REFUND_APPROVED = "FULL_REFUND_APPROVED"
    PARTIAL_REFUND_SETTLEMENT = "PARTIAL_REFUND_SETTLEMENT"
    REPLACEMENT_ORDER_ISSUED = "REPLACEMENT_ORDER_ISSUED"
    CLAIM_DENIED = "CLAIM_DENIED"
    ESCALATED_TO_HUMAN = "ESCALATED_TO_HUMAN"


class DisputeStatus(str, Enum):
    OPEN = "OPEN"
    IN_NEGOTIATION = "IN_NEGOTIATION"
    ARBITRATED = "ARBITRATED"
    SETTLED = "SETTLED"
    ESCALATED = "ESCALATED"


class NegotiationSpeaker(str, Enum):
    BUYER_ADVOCATE = "BUYER_ADVOCATE"
    MERCHANT_DEFENDER = "MERCHANT_DEFENDER"
    IMPARTIAL_ARBITRATOR = "IMPARTIAL_ARBITRATOR"


class NegotiationTurn(BaseModel):
    speaker: NegotiationSpeaker
    turn_index: int
    argument: str
    remedy_position: str
    evidence_referenced: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class EvidenceDossier(BaseModel):
    policy_citations: List[Dict[str, Any]] = Field(default_factory=list)
    graphrag_defect_subgraph: Optional[Dict[str, Any]] = None
    known_factory_defect: bool = False
    supplier_culpable: Optional[str] = None
    defect_description: Optional[str] = None
    buyer_dispute_history_count: int = 0
    buyer_fraud_risk_score: float = 0.05
    merchant_chargeback_rate_pct: float = 1.2
    delivery_confirmed_days_ago: int = 5
    telemetry_logs_provided: bool = False


class ArbitrationVerdict(BaseModel):
    claim_id: str
    outcome: ResolutionOutcome
    total_claim_amount: float
    refund_amount: float
    buyer_refund_pct: float
    merchant_liability_pct: float
    supplier_liability_pct: float
    is_auto_settled: bool
    requires_human_approval: bool
    confidence_score: float = 1.0
    judicial_rationale: str
    car_issued_to_supplier: Optional[str] = None
    action_items: List[str] = Field(default_factory=list)
    resolved_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DisputeClaim(BaseModel):
    claim_id: str
    order_id: str
    tenant_id: str = "store_tech"
    customer_id: int
    product_id: Optional[str] = None
    product_name: str
    claim_amount: float
    reason: DisputeReason
    customer_statement: str
    status: DisputeStatus = DisputeStatus.OPEN
    evidence_dossier: EvidenceDossier = Field(default_factory=EvidenceDossier)
    negotiation_transcript: List[NegotiationTurn] = Field(default_factory=list)
    verdict: Optional[ArbitrationVerdict] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
