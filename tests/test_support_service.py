import sys
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
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
from src.adapter.bm25_adapter import BM25SearchAdapter
from src.adapter.hybrid_retriever import compute_reciprocal_rank_fusion, TwoStageHybridRetriever
from src.presentation.schemas import ChatRequest, ChatResponse

# ---------------------------------------------------------------------------
# Track 1: Hybrid Search & BM25 Tests
# ---------------------------------------------------------------------------
def test_bm25_search_adapter():
    """Verify that BM25 accurately performs sparse keyword retrieval on exact terms"""
    bm25 = BM25SearchAdapter()
    
    docs = [
        DocumentChunk(
            chunk_id="doc1",
            content="Standard delivery SLA takes 3 to 5 business days with flat shipping fees.",
            metadata={"title": "Shipping SLA"}
        ),
        DocumentChunk(
            chunk_id="doc2",
            content="Returns are accepted within 30 calendar days of receipt.",
            metadata={"title": "Return Policy"}
        ),
        DocumentChunk(
            chunk_id="doc3",
            content="Cash on Delivery (COD) is available for orders under 250 dollars.",
            metadata={"title": "Payment FAQ"}
        )
    ]
    
    bm25.index_documents(docs)
    assert bm25.is_indexed is True

    # Search exact keyword "COD"
    results_cod = bm25.search("COD payment", k=2)
    assert len(results_cod) >= 1
    assert results_cod[0].chunk_id == "doc3"

    # Search exact keyword "SLA"
    results_sla = bm25.search("delivery SLA", k=2)
    assert len(results_sla) >= 1
    assert results_sla[0].chunk_id == "doc1"

def test_reciprocal_rank_fusion_math():
    """Verify that Reciprocal Rank Fusion (RRF) correctly merges dense and sparse rankings"""
    chunk_a = DocumentChunk(chunk_id="chunk_a", content="Return policy 30 days")
    chunk_b = DocumentChunk(chunk_id="chunk_b", content="Shipping SLA 3-5 days")
    chunk_c = DocumentChunk(chunk_id="chunk_c", content="COD payment terms")

    # Dense ranked: A (rank 1), B (rank 2)
    dense_results = [chunk_a, chunk_b]
    # Sparse ranked: B (rank 1), C (rank 2)
    sparse_results = [chunk_b, chunk_c]

    # RRF with k=60
    # Score(A) = 1/(60+1) = 1/61 ~ 0.016393
    # Score(B) = 1/(60+2) + 1/(60+1) = 1/62 + 1/61 ~ 0.032522 (Highest!)
    # Score(C) = 1/(60+2) = 1/62 ~ 0.016129
    fused = compute_reciprocal_rank_fusion(dense_results, sparse_results, k_constant=60)

    assert len(fused) == 3
    # Chunk B appeared in BOTH dense & sparse, so it MUST be ranked #1
    assert fused[0].chunk_id == "chunk_b"
    assert fused[0].score > fused[1].score

@pytest.mark.asyncio
async def test_two_stage_hybrid_retriever():
    """Verify that the TwoStageHybridRetriever orchestrates Stage 1 (Hybrid) and Stage 2 (Rerank)"""
    mock_vector_adapter = AsyncMock()
    mock_vector_adapter.similarity_search_with_score.return_value = [
        DocumentChunk(chunk_id="d1", content="Returns in 30 days", score=0.8),
        DocumentChunk(chunk_id="d2", content="Damaged items claim", score=0.6)
    ]

    mock_bm25 = MagicMock()
    mock_bm25.search.return_value = [
        DocumentChunk(chunk_id="d2", content="Damaged items claim", score=5.2)
    ]

    mock_reranker = AsyncMock()
    mock_reranker.rerank.return_value = [
        DocumentChunk(chunk_id="d2", content="Damaged items claim", score=0.95),
        DocumentChunk(chunk_id="d1", content="Returns in 30 days", score=0.45)
    ]

    retriever = TwoStageHybridRetriever(mock_vector_adapter, mock_bm25, mock_reranker)
    results = await retriever.retrieve_and_rerank("My package was damaged", top_k=2)

    assert len(results) == 2
    assert results[0].chunk_id == "d2"
    assert results[0].score == 0.95

# ---------------------------------------------------------------------------
# Router & LangGraph State Machine Tests
# ---------------------------------------------------------------------------
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
        "retrieved_docs": [{"source": "other.md", "title": "Other", "content": "random", "score": 0.01}],
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
