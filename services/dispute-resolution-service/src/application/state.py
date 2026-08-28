from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from src.domain.dispute_entities import (
    DisputeClaim,
    NegotiationTurn,
    ArbitrationVerdict,
    EvidenceDossier
)


class DisputeWorkflowState(BaseModel):
    """
    LangGraph Workflow State passed between the multi-agent negotiation nodes.
    """
    claim: DisputeClaim
    current_turn_index: int = 1
    buyer_turn: Optional[NegotiationTurn] = None
    merchant_turn: Optional[NegotiationTurn] = None
    evidence_dossier: EvidenceDossier = Field(default_factory=EvidenceDossier)
    arbitration_verdict: Optional[ArbitrationVerdict] = None
    is_completed: bool = False
    errors: List[str] = Field(default_factory=list)
