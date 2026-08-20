from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

@dataclass
class DocumentChunk:
    """Represents a chunked piece of business knowledge indexed in the vector database"""
    chunk_id: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    score: Optional[float] = None

@dataclass
class SupportQuery:
    """Domain representation of an incoming user customer support question"""
    user_id: Optional[str]
    session_id: str
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

@dataclass
class SupportResponse:
    """Domain representation of the generated answer grounded by RAG"""
    answer: str
    sources: List[Dict[str, Any]] = field(default_factory=list)
    model_name: str = ""
    processing_time_ms: float = 0.0
