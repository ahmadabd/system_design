import sys
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, patch
from langchain_core.documents import Document
from langchain_core.messages import HumanMessage, AIMessage

# Add support-service directory to sys.path
support_service_path = Path(__file__).parent.parent / "services" / "support-service"
if str(support_service_path) not in sys.path:
    sys.path.insert(0, str(support_service_path))

from src.domain.models import DocumentChunk, SupportQuery, SupportResponse
from src.application.ingestion_service import IngestionApplicationService
from src.application.rag_service import RAGApplicationService
from src.application.graph_state import SupportAgentState
from src.application.graph_nodes import router_node, grade_documents_node
from src.application.graph_builder import route_decision, post_retrieve_decision
from src.presentation.schemas import ChatRequest, ChatResponse

@pytest.mark.asyncio
async def test_router_node_intent_classification():
    """Verify that router node accurately identifies policy FAQs vs order inquiries vs hybrid questions"""
    
    # 1. Policy FAQ
    state_policy: SupportAgentState = {
        "messages": [HumanMessage(content="What is your return policy?")],
        "session_id": "test-1",
        "user_id": None,
        "intent": None,
        "extracted_entities": {},
        "retrieved_docs": [],
        "tool_results": [],
        "is_docs_relevant": None,
        "final_answer": None,
        "sources": []
    }
    res_policy = await router_node(state_policy)
    assert res_policy["intent"] == "policy_faq"

    # 2. Live Order Inquiry
    state_order: SupportAgentState = {
        "messages": [HumanMessage(content="Where is my order #1042?")],
        "session_id": "test-2",
        "user_id": None,
        "intent": None,
        "extracted_entities": {},
        "retrieved_docs": [],
        "tool_results": [],
        "is_docs_relevant": None,
        "final_answer": None,
        "sources": []
    }
    res_order = await router_node(state_order)
    assert res_order["intent"] == "order_inquiry"
    assert res_order["extracted_entities"].get("order_id") == 1042

    # 3. Hybrid Query (Order action + Return Policy)
    state_hybrid: SupportAgentState = {
        "messages": [HumanMessage(content="My order 55 arrived damaged. Can I get a refund?")],
        "session_id": "test-3",
        "user_id": None,
        "intent": None,
        "extracted_entities": {},
        "retrieved_docs": [],
        "tool_results": [],
        "is_docs_relevant": None,
        "final_answer": None,
        "sources": []
    }
    res_hybrid = await router_node(state_hybrid)
    assert res_hybrid["intent"] == "hybrid"
    assert res_hybrid["extracted_entities"].get("order_id") == 55

@pytest.mark.asyncio
async def test_grade_documents_node():
    """Verify document relevance evaluation logic"""
    # Relevant document chunk
    state_relevant: SupportAgentState = {
        "messages": [HumanMessage(content="test")],
        "session_id": "s1",
        "user_id": None,
        "intent": "policy_faq",
        "extracted_entities": {},
        "retrieved_docs": [{"source": "return.md", "title": "Return", "content": "30 days", "score": 0.85}],
        "tool_results": [],
        "is_docs_relevant": None,
        "final_answer": None,
        "sources": []
    }
    res_relevant = await grade_documents_node(state_relevant)
    assert res_relevant["is_docs_relevant"] is True

    # Low score / irrelevant document
    state_irrelevant: SupportAgentState = {
        "messages": [HumanMessage(content="test")],
        "session_id": "s2",
        "user_id": None,
        "intent": "policy_faq",
        "extracted_entities": {},
        "retrieved_docs": [{"source": "other.md", "title": "Other", "content": "random", "score": 0.12}],
        "tool_results": [],
        "is_docs_relevant": None,
        "final_answer": None,
        "sources": []
    }
    res_irrelevant = await grade_documents_node(state_irrelevant)
    assert res_irrelevant["is_docs_relevant"] is False

def test_graph_routing_decisions():
    """Verify conditional branching logic in LangGraph"""
    assert route_decision({"intent": "policy_faq"}) == "retrieve"
    assert route_decision({"intent": "order_inquiry"}) == "tools"
    assert route_decision({"intent": "hybrid"}) == "retrieve"
    assert route_decision({"intent": "general"}) == "generate"

    assert post_retrieve_decision({"intent": "hybrid"}) == "tools"
    assert post_retrieve_decision({"intent": "policy_faq"}) == "grade_docs"

@pytest.mark.asyncio
async def test_ingestion_text_splitter():
    """Verify knowledge base document chunking"""
    mock_vector_adapter = AsyncMock()
    mock_vector_adapter.add_documents.return_value = ["id1", "id2"]

    ingestion = IngestionApplicationService(mock_vector_adapter)
    
    sample_doc = Document(
        page_content="# Return Policy\n\nCustomers may return items within 30 days.\n\n## Shipping\nStandard delivery is 3-5 days.",
        metadata={"source": "test_policy.md"}
    )
    chunks = ingestion.text_splitter.split_documents([sample_doc])
    
    assert len(chunks) >= 1
    assert "30 days" in chunks[0].page_content
