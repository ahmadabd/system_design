from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class ChatRequest(BaseModel):
    """Payload sent by customer asking a support query"""
    message: str = Field(..., min_length=1, max_length=2000, description="Customer question or inquiry")
    session_id: Optional[str] = Field(default="default-session", description="Conversation session ID for memory persistence")
    user_id: Optional[str] = Field(default=None, description="Authenticated customer ID if logged in")
    top_k: Optional[int] = Field(default=4, ge=1, le=10, description="Number of knowledge base chunks to retrieve")

class SourceCitation(BaseModel):
    source: str
    title: str
    score: Optional[float] = None

class PendingActionDTO(BaseModel):
    """Details of an action paused for human approval"""
    action_type: str = Field(..., description="e.g. 'cancel_order', 'issue_refund'")
    order_id: int = Field(..., description="Target order ID")
    details: str = Field(..., description="Summary of order and financial impact")
    confirmation_prompt: str = Field(..., description="Message shown to customer for confirmation")

class ChatResponse(BaseModel):
    """Response returned to customer with grounded RAG answer and action status"""
    status: str = Field(default="completed", description="'completed' or 'pending_approval'")
    answer: str
    pending_action: Optional[PendingActionDTO] = None
    sources: List[SourceCitation] = []
    model: str
    processing_time_ms: float

class ActionConfirmRequest(BaseModel):
    """Payload to confirm or reject a pending Human-in-the-Loop action"""
    session_id: str = Field(..., description="Session thread ID holding the pending breakpoint")
    approved: bool = Field(..., description="True to approve execution, False to cancel")
    reason: Optional[str] = Field(default=None, description="Optional cancellation reason")


class IngestResponse(BaseModel):
    """Response returned after indexing markdown policies into Qdrant"""
    status: str
    files_processed: int = 0
    chunks_indexed: int = 0
    collection_name: Optional[str] = None
    message: Optional[str] = None

class HealthResponse(BaseModel):
    """Health check status with downstream connectivity details"""
    status: str
    service: str
    model: str
    qdrant: Dict[str, Any]
