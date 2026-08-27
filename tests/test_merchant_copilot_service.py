import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from httpx import AsyncClient, ASGITransport

from src.domain.schemas import CopilotQueryRequest
from src.adapter.ast_validator import ast_validator
from src.adapter.micro_batcher import ClickHouseMicroBatcher
from src.adapter.qdrant_policy_adapter import QdrantPolicyAdapter
from src.adapter.llm_adapter import CopilotLLMAdapter
from src.application.workflow import build_copilot_workflow
from src.main import app

@pytest.mark.asyncio
async def test_ast_safety_validator():
    """Verifies that the AST validator permits read-only SELECTs and blocks destructive queries"""
    # 1. Valid Read-Only Query with tenant_id predicate
    valid_sql = "SELECT id, name, price FROM copilot_analytics.products_analytics WHERE tenant_id = 'store_tech' ORDER BY price DESC LIMIT 5"
    is_valid, err = ast_validator.validate_sql(valid_sql, tenant_id="store_tech")
    assert is_valid is True
    assert err is None

    # 2. Block DROP TABLE
    drop_sql = "DROP TABLE copilot_analytics.products_analytics; SELECT * FROM products_analytics WHERE tenant_id = 'store_tech'"
    is_valid, err = ast_validator.validate_sql(drop_sql, tenant_id="store_tech")
    assert is_valid is False
    assert "Security violation" in err or "syntax" in err.lower()

    # 3. Block DELETE
    delete_sql = "DELETE FROM copilot_analytics.orders_analytics WHERE tenant_id = 'store_tech'"
    is_valid, err = ast_validator.validate_sql(delete_sql, tenant_id="store_tech")
    assert is_valid is False
    assert "Security violation" in err

    # 4. Block UPDATE
    update_sql = "UPDATE copilot_analytics.products_analytics SET price = 0 WHERE tenant_id = 'store_tech'"
    is_valid, err = ast_validator.validate_sql(update_sql, tenant_id="store_tech")
    assert is_valid is False
    assert "Security violation" in err

    # 5. Block query missing tenant_id filter predicate
    leak_sql = "SELECT * FROM copilot_analytics.orders_analytics"
    is_valid, err = ast_validator.validate_sql(leak_sql, tenant_id="store_tech")
    assert is_valid is False
    assert "Tenant safety violation" in err

@pytest.mark.asyncio
async def test_clickhouse_micro_batcher():
    """Tests the in-memory batch buffer size and timer flushing mechanics"""
    batcher = ClickHouseMicroBatcher(batch_size=3, flush_interval=0.2)
    committed = []

    def commit_cb():
        committed.append(True)

    with patch("src.adapter.micro_batcher.clickhouse_client.insert_batch", return_value=3) as mock_insert:
        await batcher.start()
        
        # Enqueue 2 items (below batch threshold)
        await batcher.enqueue("products_analytics", {"id": 1, "name": "A"}, on_commit_callback=commit_cb)
        await batcher.enqueue("products_analytics", {"id": 2, "name": "B"})
        assert mock_insert.call_count == 0

        # Enqueue 3rd item (triggers batch_size flush)
        await batcher.enqueue("products_analytics", {"id": 3, "name": "C"})
        assert mock_insert.call_count == 1
        assert len(committed) == 1

        # Test timed flush on remaining item
        await batcher.enqueue("orders_analytics", {"id": 101, "total_amount": 99.0})
        await asyncio.sleep(0.3)
        assert mock_insert.call_count == 2

        await batcher.stop()

@pytest.mark.asyncio
async def test_qdrant_policy_and_schema_adapter():
    """Tests policy seeding, search, and dynamic schema linking"""
    adapter = QdrantPolicyAdapter()
    
    # Test deterministic embedding vector generation
    vec = adapter.embed_text("What is our 30-day return policy?")
    assert len(vec) == 384
    assert abs(sum(x * x for x in vec) - 1.0) < 1e-3

    # Test schema linking
    schemas = adapter.link_relevant_schemas("What was our total revenue on orders?", limit=2)
    assert len(schemas) >= 1
    assert any("orders" in s.get("table_name", "") or "products" in s.get("table_name", "") for s in schemas)

    # Test policy retrieval
    policies = adapter.search_policies("damaged item DOA warranty return SLA", limit=2)
    assert len(policies) >= 1
    assert any("Return" in p.get("title", "") or "Damaged" in p.get("title", "") or "Warranty" in p.get("title", "") for p in policies)

@pytest.mark.asyncio
async def test_llm_adapter_heuristics():
    """Tests Intent Classification, SQL Generation, and Self-Correction fallback logic"""
    llm = CopilotLLMAdapter()

    # Intent Classification
    assert llm.classify_intent("Show total sales revenue for store_tech") == "structured_analytics"
    assert llm.classify_intent("What is the return SLA deadline for damaged items?") == "policy_guidelines"
    assert llm.classify_intent("What are our top products and what warranty policy applies to them?") == "hybrid"

    # SQL Generation
    sql = llm.generate_sql("Show top 5 products by price", tenant_id="store_tech", linked_schemas="")
    assert "SELECT" in sql
    assert "store_tech" in sql

    # SQL Self-Correction
    failed_sql = "SELECT * FROM copilot_analytics.orders_analytics"
    healed_sql = llm.fix_sql_error(
        query="Show orders",
        tenant_id="store_tech",
        failed_sql=failed_sql,
        error_message="Tenant safety violation",
        linked_schemas=""
    )
    assert "tenant_id = 'store_tech'" in healed_sql

@pytest.mark.asyncio
async def test_langgraph_workflow_end_to_end():
    """Tests the compiled LangGraph StateGraph execution for hybrid analytics + policy query"""
    workflow = build_copilot_workflow()

    initial_state = {
        "messages": [],
        "query": "Show our top products by price and explain our warranty guidelines",
        "tenant_id": "store_tech",
        "session_id": "test_session_001",
        "intent": "",
        "relevant_tables": [],
        "linked_schema_ddl": "",
        "retrieved_policies": [],
        "generated_sql": None,
        "is_sql_valid": False,
        "ast_error": None,
        "runtime_error": None,
        "correction_attempts": 0,
        "max_corrections": 3,
        "sql_result_rows": [],
        "final_markdown_report": "",
        "is_safe": True
    }

    # Execute StateGraph
    with patch("src.application.graph_nodes.clickhouse_client.execute_query", return_value=[
        {"id": 1, "name": "Gaming Laptop Pro", "category": "Laptops", "price": 1899.99, "stock": 5}
    ]):
        result = await workflow.ainvoke(initial_state)

    assert result["intent"] == "hybrid"
    assert len(result["retrieved_policies"]) > 0
    assert result["is_sql_valid"] is True
    assert len(result["sql_result_rows"]) == 1
    assert "Gaming Laptop Pro" in result["final_markdown_report"]
    assert "Warranty" in result["final_markdown_report"] or "Policy" in result["final_markdown_report"]

@pytest.mark.asyncio
async def test_fastapi_endpoints():
    """Tests FastAPI /chat and /health endpoints"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Health check
        h_resp = await client.get("/health")
        assert h_resp.status_code == 200
        assert h_resp.json()["olap_backend"] == "ClickHouse"

        # Chat endpoint
        with patch("src.application.graph_nodes.clickhouse_client.execute_query", return_value=[
            {"status": "CONFIRMED", "order_count": 42, "total_amount": 5420.50}
        ]):
            c_resp = await client.post(
                "/chat",
                headers={"X-Tenant-ID": "store_tech"},
                json={
                    "query": "Show total revenue and confirmed orders",
                    "tenant_id": "store_tech"
                }
            )
            assert c_resp.status_code == 200
            data = c_resp.json()
            assert data["tenant_id"] == "store_tech"
            assert data["is_safe"] is True
            assert "Report" in data["final_markdown_report"] or "ClickHouse" in data["final_markdown_report"]
