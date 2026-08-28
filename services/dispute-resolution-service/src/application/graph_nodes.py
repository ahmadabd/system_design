import time
import logging
from typing import Dict, Any
from opentelemetry import trace
from src.application.state import DisputeWorkflowState
from src.domain.dispute_entities import DisputeStatus
from src.adapter.policy_rag_adapter import policy_rag_adapter
from src.adapter.graphrag_evidence_client import graphrag_evidence_client
from src.adapter.fraud_analytics_client import fraud_analytics_client
from src.adapter.llm_arbitrator_adapter import llm_arbitrator_adapter
from src.adapter.messaging_pub import dispute_messaging_pub
from src.infrastructure.metrics import (
    dispute_claims_total,
    dispute_auto_settled_total,
    dispute_human_escalations_total,
    dispute_settlement_duration_seconds,
    dispute_refund_amount_dollars
)

logger = logging.getLogger("DisputeGraphNodes")
tracer = trace.get_tracer("dispute-resolution-service")


async def buyer_advocate_node(state: DisputeWorkflowState) -> Dict[str, Any]:
    """Node 1: Buyer Advocate builds opening claim and demands compensation"""
    with tracer.start_as_current_span("Node 1: Buyer Advocate Argumentation") as span:
        claim = state.claim
        claim.status = DisputeStatus.IN_NEGOTIATION
        
        turn = llm_arbitrator_adapter.generate_buyer_advocate_turn(claim, turn_index=1)
        claim.negotiation_transcript.append(turn)

        span.set_attribute("agent.speaker", "BUYER_ADVOCATE")
        span.set_attribute("agent.remedy_demanded", turn.remedy_position)
        logger.info(f"Buyer Advocate formulated claim for order #{claim.order_id}: {turn.remedy_position}")

        return {
            "claim": claim,
            "buyer_turn": turn,
            "current_turn_index": 2
        }


async def merchant_defender_node(state: DisputeWorkflowState) -> Dict[str, Any]:
    """Node 2: Merchant Defender checks fulfillment records and builds policy defense"""
    with tracer.start_as_current_span("Node 2: Merchant Defender Position") as span:
        claim = state.claim
        
        turn = llm_arbitrator_adapter.generate_merchant_defender_turn(claim, turn_index=state.current_turn_index)
        claim.negotiation_transcript.append(turn)

        span.set_attribute("agent.speaker", "MERCHANT_DEFENDER")
        span.set_attribute("agent.remedy_offered", turn.remedy_position)
        logger.info(f"Merchant Defender responded for order #{claim.order_id}: {turn.remedy_position}")

        return {
            "claim": claim,
            "merchant_turn": turn,
            "current_turn_index": state.current_turn_index + 1
        }


async def multi_source_evidence_node(state: DisputeWorkflowState) -> Dict[str, Any]:
    """
    Node 3: Multi-Source Evidence Synthesis
    - Self-RAG retrieves statutory consumer rights policies
    - GraphRAG traverses component batch & supplier defects
    - ClickHouse computes buyer fraud score
    """
    with tracer.start_as_current_span("Node 3: Multi-Source Evidence Synthesis") as span:
        claim = state.claim
        dossier = claim.evidence_dossier

        # 1. Self-RAG Policy Retrieval
        policies = policy_rag_adapter.retrieve_relevant_policies(
            query=claim.customer_statement,
            reason=claim.reason.value,
            top_k=2
        )
        dossier.policy_citations = policies

        # 2. GraphRAG Defect Traversal
        graph_findings = await graphrag_evidence_client.investigate_product_defects(
            product_name=claim.product_name,
            tenant_id=claim.tenant_id
        )
        dossier.known_factory_defect = graph_findings.get("has_known_defect", False)
        dossier.supplier_culpable = graph_findings.get("supplier_culpable")
        dossier.defect_description = graph_findings.get("defect_description")
        dossier.graphrag_defect_subgraph = graph_findings

        # 3. ClickHouse Behavioral Fraud Scoring
        fraud_stats = await fraud_analytics_client.calculate_fraud_risk(
            customer_id=claim.customer_id,
            tenant_id=claim.tenant_id
        )
        dossier.buyer_fraud_risk_score = fraud_stats.get("fraud_risk_score", 0.05)
        dossier.buyer_dispute_history_count = fraud_stats.get("prior_disputes", 0)

        span.set_attribute("evidence.known_factory_defect", dossier.known_factory_defect)
        span.set_attribute("evidence.supplier_culpable", str(dossier.supplier_culpable))
        span.set_attribute("evidence.fraud_risk_score", dossier.buyer_fraud_risk_score)
        span.set_attribute("evidence.policies_retrieved", len(policies))

        logger.info(
            f"Evidence synthesized for claim #{claim.claim_id}: Defect={dossier.known_factory_defect} "
            f"(Supplier={dossier.supplier_culpable}), FraudScore={dossier.buyer_fraud_risk_score}"
        )

        return {
            "claim": claim,
            "evidence_dossier": dossier
        }


async def impartial_arbitrator_node(state: DisputeWorkflowState) -> Dict[str, Any]:
    """Node 4: Impartial Judicial Arbitrator renders legally grounded verdict"""
    with tracer.start_as_current_span("Node 4: Impartial Judicial Arbitration") as span:
        claim = state.claim
        start_time = time.time()

        verdict = llm_arbitrator_adapter.arbitrate_dispute(claim)
        claim.verdict = verdict
        claim.status = DisputeStatus.ARBITRATED

        duration = time.time() - start_time
        dispute_settlement_duration_seconds.labels(outcome=verdict.outcome.value).observe(duration)

        span.set_attribute("arbitration.outcome", verdict.outcome.value)
        span.set_attribute("arbitration.refund_amount", verdict.refund_amount)
        span.set_attribute("arbitration.is_auto_settled", verdict.is_auto_settled)

        logger.info(
            f"Arbitrator rendered verdict for claim #{claim.claim_id}: {verdict.outcome.value} "
            f"(${verdict.refund_amount:.2f}, AutoSettled={verdict.is_auto_settled})"
        )

        return {
            "claim": claim,
            "arbitration_verdict": verdict
        }


async def settlement_engine_node(state: DisputeWorkflowState) -> Dict[str, Any]:
    """Node 5: Settlement Execution & Kafka Event Emission"""
    with tracer.start_as_current_span("Node 5: Settlement & Financial Execution") as span:
        claim = state.claim
        verdict = claim.verdict

        if verdict and verdict.is_auto_settled:
            claim.status = DisputeStatus.SETTLED
            dispute_auto_settled_total.labels(outcome=verdict.outcome.value, tenant_id=claim.tenant_id).inc()
            dispute_refund_amount_dollars.labels(tenant_id=claim.tenant_id).inc(verdict.refund_amount)
        else:
            claim.status = DisputeStatus.ESCALATED
            dispute_human_escalations_total.labels(reason=claim.reason.value, tenant_id=claim.tenant_id).inc()

        dispute_claims_total.labels(
            reason=claim.reason.value,
            outcome=verdict.outcome.value if verdict else "UNKNOWN",
            tenant_id=claim.tenant_id
        ).inc()

        # Emit Kafka event for payment refund & reporting pipelines
        await dispute_messaging_pub.publish_dispute_resolved({
            "claim_id": claim.claim_id,
            "order_id": claim.order_id,
            "customer_id": claim.customer_id,
            "tenant_id": claim.tenant_id,
            "outcome": verdict.outcome.value if verdict else "ESCALATED_TO_HUMAN",
            "refund_amount": verdict.refund_amount if verdict else 0.0,
            "is_auto_settled": verdict.is_auto_settled if verdict else False,
            "car_issued_to_supplier": verdict.car_issued_to_supplier if verdict else None
        })

        span.set_attribute("settlement.final_status", claim.status.value)
        return {
            "claim": claim,
            "is_completed": True
        }
