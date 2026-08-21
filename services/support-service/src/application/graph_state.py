from typing import Sequence, Dict, Any, Optional, List, Annotated
from typing_extensions import TypedDict
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

class SupportAgentState(TypedDict):
    """
    Typed state representation passed across all nodes in the LangGraph support workflow.
    `messages` uses `add_messages` to automatically append new conversation turns.
    """
    messages: Annotated[Sequence[BaseMessage], add_messages]
    session_id: str
    user_id: Optional[str]
    intent: Optional[str]                     # "policy_faq", "order_inquiry", "product_inquiry", "hybrid", "general"
    extracted_entities: Dict[str, Any]        # {"order_id": 104, "product_id": 2, ...}
    retrieved_docs: List[Dict[str, Any]]       # [{"source": "...", "title": "...", "content": "...", "score": 0.9}]
    tool_results: List[Dict[str, Any]]         # [{"tool": "get_order_status", "output": {...}}]
    is_docs_relevant: Optional[bool]          # True if retrieved docs are relevant to the user query
    final_answer: Optional[str]
    sources: List[Dict[str, Any]]
    
    # Self-RAG Reflection & Loop Control Fields
    retry_count: int                          # Current regeneration loop count (max 2)
    hallucination_status: Optional[str]       # "grounded" vs "not_grounded"
    answer_quality: Optional[str]             # "useful" vs "not_useful"
    correction_feedback: Optional[str]        # Directive passed back to generator if hallucination was detected

