import sys
import os
sys.path.insert(0, os.path.abspath("services/discovery-service"))

import pytest
from unittest.mock import patch, MagicMock
from langchain_core.messages import HumanMessage
from src.application.graph_state import DiscoveryState
from src.domain.schemas import ProductItemDTO
from src.application.graph_nodes import (
    parse_constraints_node,
    generate_hyde_and_subqueries_node,
    bundle_optimizer_node,
    synthesize_recommendation_node,
)
from src.application.graph_builder import build_discovery_graph
from src.adapter.qdrant_adapter import QdrantDiscoveryAdapter


def test_parse_constraints_node():
    """Verify dynamic budget extraction and component category parsing."""
    state: DiscoveryState = {
        "messages": [HumanMessage(content="Find a gaming laptop and mechanical keyboard under $1500")],
        "raw_query": "Find a gaming laptop and mechanical keyboard under $1500",
        "budget": None,
        "category_filter": None,
        "in_stock_only": True,
        "session_id": "test",
        "tenant_id": "store_tech",
        "parsed_constraints": {},
        "sub_queries": [],
        "hyde_spec": None,
        "retrieved_candidates": [],
        "reranked_products": [],
        "recommended_bundle": None,
        "final_response": "",
        "status": "init"
    }

    result = parse_constraints_node(state)
    assert result["budget"] == 1500.0
    assert "Laptop" in result["parsed_constraints"]["target_categories"] or "Gaming" in result["parsed_constraints"]["target_categories"]


def test_dynamic_arbitrary_category_extraction():
    """Verify dynamic category extraction works on novel categories without hardcoded rules."""
    state: DiscoveryState = {
        "messages": [HumanMessage(content="Podcast setup with microphone and studio headphones under $400")],
        "raw_query": "Podcast setup with microphone and studio headphones under $400",
        "budget": None,
        "category_filter": None,
        "in_stock_only": True,
        "session_id": "test",
        "tenant_id": "store_tech",
        "parsed_constraints": {},
        "sub_queries": [],
        "hyde_spec": None,
        "retrieved_candidates": [],
        "reranked_products": [],
        "recommended_bundle": None,
        "final_response": "",
        "status": "init"
    }

    result = parse_constraints_node(state)
    assert result["budget"] == 400.0
    cats = [c.lower() for c in result["parsed_constraints"]["target_categories"]]
    assert any("podcast" in c or "microphone" in c or "headphones" in c for c in cats)


def test_hyde_and_subqueries_generation():
    """Verify dynamic HyDE hypothetical spec and decomposed sub-queries."""
    state: DiscoveryState = {
        "messages": [],
        "raw_query": "programming workstation",
        "budget": 800.0,
        "category_filter": None,
        "in_stock_only": True,
        "session_id": "test",
        "tenant_id": "store_tech",
        "parsed_constraints": {"target_categories": ["Laptops", "Monitors"]},
        "sub_queries": [],
        "hyde_spec": None,
        "retrieved_candidates": [],
        "reranked_products": [],
        "recommended_bundle": None,
        "final_response": "",
        "status": "init"
    }

    result = generate_hyde_and_subqueries_node(state)
    assert "Hypothetical" in result["hyde_spec"]
    assert len(result["sub_queries"]) >= 3


def test_bundle_optimizer_knapsack_constraints():
    """Verify knapsack budget allocation selects complementary products under ceiling."""
    products = [
        ProductItemDTO(id=1, name="Gaming Laptop", price=1299.99, stock=10, store_id=1, category="Laptops"),
        ProductItemDTO(id=2, name="Ultra 4K Monitor", price=399.0, stock=20, store_id=1, category="Monitors"),
        ProductItemDTO(id=3, name="Wireless Keyboard", price=50.0, stock=50, store_id=1, category="Keyboards"),
        ProductItemDTO(id=4, name="Gaming Mouse", price=35.0, stock=40, store_id=1, category="Accessories"),
    ]

    state: DiscoveryState = {
        "messages": [],
        "raw_query": "Desk Setup",
        "budget": 500.0,  # $500 budget cannot afford $1299 laptop, but can afford Monitor ($399) + Keyboard ($50) + Mouse ($35) = $484
        "category_filter": None,
        "in_stock_only": True,
        "session_id": "test",
        "tenant_id": "store_tech",
        "parsed_constraints": {},
        "sub_queries": [],
        "hyde_spec": None,
        "retrieved_candidates": [],
        "reranked_products": products,
        "recommended_bundle": None,
        "final_response": "",
        "status": "init"
    }

    result = bundle_optimizer_node(state)
    bundle = result["recommended_bundle"]
    assert bundle is not None
    assert bundle.total_price <= 500.0
    assert bundle.total_price == pytest.approx(484.0, 0.01)
    assert len(bundle.items) == 3
    item_ids = [i.id for i in bundle.items]
    assert 2 in item_ids  # Monitor
    assert 3 in item_ids  # Keyboard
    assert 4 in item_ids  # Mouse


def test_synthesize_recommendation():
    """Verify markdown synthesis produces valid tabular layout and pricing summary."""
    from src.domain.schemas import BundleRecommendationDTO
    bundle = BundleRecommendationDTO(
        bundle_name="Budget Coding Bundle",
        total_price=449.0,
        budget=500.0,
        remaining_budget=51.0,
        items=[
            ProductItemDTO(id=2, name="Ultra 4K Monitor", price=399.0, stock=20, store_id=1, category="Monitors"),
            ProductItemDTO(id=3, name="Wireless Keyboard", price=50.0, stock=50, store_id=1, category="Keyboards")
        ],
        summary_rationale="Curated 2 optimal items under $500."
    )

    state: DiscoveryState = {
        "messages": [],
        "raw_query": "Coding setup",
        "budget": 500.0,
        "category_filter": None,
        "in_stock_only": True,
        "session_id": "test",
        "tenant_id": "store_tech",
        "parsed_constraints": {},
        "sub_queries": [],
        "hyde_spec": None,
        "retrieved_candidates": [],
        "reranked_products": [],
        "recommended_bundle": bundle,
        "final_response": "",
        "status": "init"
    }

    result = synthesize_recommendation_node(state)
    assert "Total Bundle Price" in result["final_response"]
    assert "$449.00" in result["final_response"]
    assert "Ultra 4K Monitor" in result["final_response"]


@pytest.mark.asyncio
async def test_compiled_discovery_graph_end_to_end():
    """Verify complete StateGraph compilation and async execution."""
    sample_catalog = [
        {"id": 3, "name": "Wireless Keyboard", "price": 50.0, "stock": 99, "category": "Keyboards"},
        {"id": 4, "name": "Ultra HD Monitor", "price": 399.0, "stock": 23, "category": "Monitors"}
    ]

    graph = build_discovery_graph()

    with patch.object(QdrantDiscoveryAdapter, 'search_products') as mock_search:
        mock_search.return_value = [
            ProductItemDTO(id=3, name="Wireless Keyboard", price=50.0, stock=99, store_id=1, category="Keyboards", similarity_score=0.92),
            ProductItemDTO(id=4, name="Ultra HD Monitor", price=399.0, stock=23, store_id=1, category="Monitors", similarity_score=0.88)
        ]

        initial_state = {
            "messages": [HumanMessage(content="I want a coding keyboard and monitor under $500")],
            "session_id": "session_test_101",
            "tenant_id": "store_tech",
            "raw_query": "I want a coding keyboard and monitor under $500",
            "budget": None,
            "category_filter": None,
            "in_stock_only": True,
            "parsed_constraints": {},
            "sub_queries": [],
            "hyde_spec": None,
            "retrieved_candidates": [],
            "reranked_products": [],
            "recommended_bundle": None,
            "final_response": "",
            "status": "in_progress"
        }

        config = {"configurable": {"thread_id": "session_test_101"}}
        final_state = await graph.ainvoke(initial_state, config=config)

        assert final_state["status"] == "completed"
        assert final_state["budget"] == 500.0
        assert final_state["recommended_bundle"] is not None
        assert final_state["recommended_bundle"].total_price == 449.0
        assert len(final_state["recommended_bundle"].items) == 2
        assert "Total Bundle Price" in final_state["final_response"]


def test_fastapi_endpoints():
    """Verify FastAPI discovery endpoints (/health, /chat, /bundle)."""
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as client:
        # 1. Healthcheck
        h = client.get("/health")
        assert h.status_code == 200
        assert h.json()["status"] == "healthy"

        # 2. Chat discovery endpoint
        with patch.object(QdrantDiscoveryAdapter, 'search_products') as mock_search:
            mock_search.return_value = [
                ProductItemDTO(id=3, name="Wireless Keyboard", price=50.0, stock=99, store_id=1, category="Keyboards", similarity_score=0.95)
            ]
            resp = client.post("/chat", json={
                "query": "Find me a quiet keyboard under $100",
                "session_id": "session_http_test",
                "tenant_id": "store_tech"
            })
            assert resp.status_code == 200
            data = resp.json()
            assert data["session_id"] == "session_http_test"
            assert "Wireless Keyboard" in data["final_markdown_response"]


@pytest.mark.asyncio
async def test_kafka_discovery_event_consumer():
    """Verify event-driven auto-sync of Qdrant on ProductCreated and ProductDeleted events."""
    from src.adapter.messaging_sub import DiscoveryEventConsumer

    mock_qdrant = MagicMock()
    consumer = DiscoveryEventConsumer(qdrant_adapter=mock_qdrant)

    # 1. ProductCreated event
    created_payload = {
        "event_type": "ProductCreated",
        "product_id": 42,
        "name": "Shure SM7B Studio Dynamic Microphone",
        "price": 399.0,
        "stock": 10,
        "store_id": 1,
        "metadata": {"tenant_slug": "store_tech"}
    }
    await consumer.handle_product_event("product.created", created_payload)
    mock_qdrant.index_products.assert_called_once()
    call_args = mock_qdrant.index_products.call_args
    indexed_list = call_args[0][0]
    assert indexed_list[0]["id"] == 42
    assert indexed_list[0]["category"] == "Microphones"
    assert call_args[1]["tenant_id"] == "store_tech"

    # 2. ProductDeleted event
    deleted_payload = {
        "event_type": "ProductDeleted",
        "product_id": 42,
        "store_id": 1,
        "metadata": {"tenant_slug": "store_tech"}
    }
    await consumer.handle_product_event("product.deleted", deleted_payload)
    mock_qdrant.delete_product.assert_called_once_with(product_id=42, tenant_id="store_tech")

