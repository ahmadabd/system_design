import pytest
from httpx import AsyncClient, ASGITransport
from src.main import app
from src.domain.dispute_entities import (
    DisputeClaim,
    DisputeReason,
    ResolutionOutcome,
    DisputeStatus
)
from src.application.state import DisputeWorkflowState
from src.application.workflow import dispute_app
from src.adapter.policy_rag_adapter import policy_rag_adapter
from src.adapter.graphrag_evidence_client import graphrag_evidence_client
from src.adapter.fraud_analytics_client import fraud_analytics_client
from src.adapter.llm_arbitrator_adapter import llm_arbitrator_adapter


@pytest.mark.asyncio
async def test_policy_rag_adapter():
    """Verifies Self-RAG policy retrieval returns relevant statutory clauses"""
    policies = policy_rag_adapter.retrieve_relevant_policies(
        query="The vapor chamber heatsink melted",
        reason=DisputeReason.DEFECTIVE_PRODUCT.value,
        top_k=2
    )
    assert len(policies) >= 1
    assert any("Statutory" in p.get("title", "") or "Defect" in p.get("title", "") for p in policies)


@pytest.mark.asyncio
async def test_graphrag_evidence_client():
    """Verifies GraphRAG client traces supplier and defect for Gaming Laptop Pro"""
    findings = await graphrag_evidence_client.investigate_product_defects(
        product_name="Gaming Laptop Pro (32GB RAM, RTX 4080)"
    )
    assert findings["has_known_defect"] is True
    assert "CoolMaster" in findings.get("supplier_culpable", "")


@pytest.mark.asyncio
async def test_fraud_analytics_client():
    """Verifies fraud risk scoring returns valid low-risk assessment"""
    stats = await fraud_analytics_client.calculate_fraud_risk(customer_id=14, tenant_id="store_tech")
    assert "fraud_risk_score" in stats
    assert stats["fraud_risk_score"] <= 0.65


@pytest.mark.asyncio
async def test_multi_agent_workflow_known_defect():
    """
    Test Case 1: Overheating Gaming Laptop Pro with known CoolMaster supplier defect
    - Buyer Advocate demands 100% refund ($1899.99)
    - Merchant Defender claims sealed box delivery
    - Arbitrator verifies GraphRAG defect and awards 100% refund with 0% Merchant liability + Supplier CAR
    """
    claim = DisputeClaim(
        claim_id="claim_test_defect_01",
        order_id="ord-101",
        tenant_id="store_tech",
        customer_id=14,
        product_name="Gaming Laptop Pro (32GB RAM, RTX 4080)",
        claim_amount=1899.99,
        reason=DisputeReason.DEFECTIVE_PRODUCT,
        customer_statement="Laptop hits 98C in 3 minutes and shuts down completely during Premiere Pro rendering."
    )

    state = DisputeWorkflowState(claim=claim)
    final_state = await dispute_app.ainvoke(state)

    res_claim = final_state["claim"]
    verdict = res_claim.verdict

    assert verdict is not None
    assert verdict.outcome == ResolutionOutcome.FULL_REFUND_APPROVED
    assert verdict.refund_amount == 1899.99
    assert verdict.merchant_liability_pct == 0.0
    assert verdict.supplier_liability_pct == 100.0
    assert verdict.car_issued_to_supplier is not None
    assert len(res_claim.negotiation_transcript) >= 2
    assert "CoolMaster" in verdict.judicial_rationale


@pytest.mark.asyncio
async def test_multi_agent_workflow_out_of_window_remorse():
    """
    Test Case 2: Discretionary return filed 35 days after delivery (out of 14-day window)
    - Verdict: CLAIM_DENIED
    """
    claim = DisputeClaim(
        claim_id="claim_test_remorse_02",
        order_id="ord-202",
        tenant_id="store_tech",
        customer_id=99,
        product_name="Ergonomic Mousepad",
        claim_amount=29.99,
        reason=DisputeReason.BUYER_REMORSE,
        customer_statement="I don't need this mousepad anymore.",
        evidence_dossier={"delivery_confirmed_days_ago": 35}
    )

    state = DisputeWorkflowState(claim=claim)
    final_state = await dispute_app.ainvoke(state)

    verdict = final_state["claim"].verdict
    assert verdict is not None
    assert verdict.outcome == ResolutionOutcome.CLAIM_DENIED
    assert verdict.refund_amount == 0.0


@pytest.mark.asyncio
async def test_fastapi_endpoints():
    """Verifies all FastAPI dispute endpoints execute and return valid responses"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Health check
        h_res = await client.get("/disputes/health")
        assert h_res.status_code == 200
        assert h_res.json()["status"] == "healthy"

        # 2. Readiness check
        r_res = await client.get("/disputes/health/ready")
        assert r_res.status_code == 200
        assert r_res.json()["status"] == "ready"

        # 3. Submit Claim with Idempotency Key
        payload = {
            "order_id": "ord-888",
            "customer_id": 42,
            "product_name": "Shure SM7B Vocal Microphone",
            "claim_amount": 399.00,
            "reason": "DEFECTIVE_PRODUCT",
            "customer_statement": "Continuous 60Hz ground hum noise during podcast recordings.",
            "delivery_days_ago": 10
        }
        create_res = await client.post(
            "/disputes/claims",
            headers={"X-Tenant-ID": "store_tech", "X-Idempotency-Key": "test-claim-key-001"},
            json=payload
        )
        assert create_res.status_code == 201
        data = create_res.json()
        claim_id = data["claim_id"]
        assert data["outcome"] == "FULL_REFUND_APPROVED"
        assert data["refund_amount"] == 399.00
        assert data["negotiation_turns_count"] >= 2

        # 4. Retrieve Claim by ID
        get_res = await client.get(f"/disputes/claims/{claim_id}")
        assert get_res.status_code == 200
        assert get_res.json()["claim_id"] == claim_id

        # 5. Get Stats
        stats_res = await client.get("/disputes/stats")
        assert stats_res.status_code == 200
        assert stats_res.json()["total_claims"] >= 1
