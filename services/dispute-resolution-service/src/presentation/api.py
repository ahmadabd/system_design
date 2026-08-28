import uuid
import asyncio
import logging
from typing import Dict, List, Optional
from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from opentelemetry import trace
from shared.common.idempotency import IdempotencyManager, idempotent_api
from shared.common.resilience import CircuitBreakerOpenException
from src.infrastructure.config import settings
from src.domain.schemas import (
    CreateDisputeClaimRequest,
    DisputeClaimResponse,
    DisputeStatsResponse
)
from src.domain.dispute_entities import DisputeClaim, DisputeStatus, EvidenceDossier
from src.application.state import DisputeWorkflowState
from src.application.workflow import dispute_app
from src.infrastructure.metrics import dispute_claims_total

logger = logging.getLogger("DisputeAPI")
router = APIRouter(tags=["Dispute Resolution"])
tracer = trace.get_tracer("dispute-resolution-service")

# Initialize Redis-backed Idempotency Manager
idempotency_manager = IdempotencyManager(settings.REDIS_URL)

# In-memory registry of claims
CLAIMS_DB: Dict[str, DisputeClaim] = {}

# Graceful Shutdown & Readiness State
is_shutting_down: bool = False
in_flight_claims_count: int = 0
in_flight_lock = asyncio.Lock()


async def drain_in_flight_claims(timeout: float = 5.0):
    """Cooperatively drains in-flight dispute claims before container termination"""
    global is_shutting_down
    is_shutting_down = True
    logger.warning("Initiated Dispute Resolution traffic draining. Readiness probe will return 503.")
    
    start_time = asyncio.get_event_loop().time()
    while in_flight_claims_count > 0:
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed >= timeout:
            logger.warning(f"Drain timeout reached ({timeout}s). {in_flight_claims_count} claims still in-flight. Forcing shutdown.")
            break
        logger.info(f"Draining traffic: waiting for {in_flight_claims_count} in-flight dispute evaluations to complete...")
        await asyncio.sleep(0.5)

    try:
        await idempotency_manager.close()
        logger.info("Dispute idempotency Redis connection pool safely closed.")
    except Exception as e:
        logger.warning(f"Error closing Redis pool: {e}")


@router.post("/claims", response_model=DisputeClaimResponse, status_code=status.HTTP_201_CREATED)
@idempotent_api(idempotency_manager)
async def create_and_resolve_claim(
    request_data: CreateDisputeClaimRequest,
    request: Request,
    x_tenant_id: Optional[str] = Header("store_tech", alias="X-Tenant-ID")
):
    """
    Submits a dispute claim with Redis Idempotency protection, triggers the
    Multi-Agent Negotiation Arena, evaluates Self-RAG policies & GraphRAG defect
    evidence, and renders a legally binding arbitration verdict.
    """
    global in_flight_claims_count

    if is_shutting_down:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Dispute Resolution Service is undergoing graceful shutdown and draining traffic."
        )

    tenant = x_tenant_id or "store_tech"
    claim_id = f"claim_{uuid.uuid4().hex[:8]}"

    with tracer.start_as_current_span("POST /disputes/claims") as span:
        span.set_attribute("claim.id", claim_id)
        span.set_attribute("tenant.id", tenant)
        span.set_attribute("order.id", request_data.order_id)
        span.set_attribute("claim.reason", request_data.reason.value)

        # Track in-flight claim
        async with in_flight_lock:
            in_flight_claims_count += 1

        try:
            # 1. Initialize Claim Entity
            claim = DisputeClaim(
                claim_id=claim_id,
                order_id=request_data.order_id,
                tenant_id=tenant,
                customer_id=request_data.customer_id,
                product_id=request_data.product_id,
                product_name=request_data.product_name,
                claim_amount=request_data.claim_amount,
                reason=request_data.reason,
                customer_statement=request_data.customer_statement,
                evidence_dossier=EvidenceDossier(
                    delivery_confirmed_days_ago=request_data.delivery_days_ago,
                    telemetry_logs_provided=bool(request_data.evidence_urls)
                )
            )

            initial_state = DisputeWorkflowState(claim=claim)

            # 2. Execute Multi-Agent Negotiation & Judicial Arbitration StateGraph
            final_state = await dispute_app.ainvoke(initial_state)
            resolved_claim = final_state["claim"]
            CLAIMS_DB[claim_id] = resolved_claim

            # Extract summaries
            buyer_summary = ""
            merchant_summary = ""
            for turn in resolved_claim.negotiation_transcript:
                if turn.speaker.value == "BUYER_ADVOCATE":
                    buyer_summary = turn.argument
                elif turn.speaker.value == "MERCHANT_DEFENDER":
                    merchant_summary = turn.argument

            verdict = resolved_claim.verdict

            return DisputeClaimResponse(
                claim_id=claim_id,
                order_id=resolved_claim.order_id,
                tenant_id=resolved_claim.tenant_id,
                status=resolved_claim.status,
                reason=resolved_claim.reason,
                claim_amount=resolved_claim.claim_amount,
                outcome=verdict.outcome if verdict else None,
                refund_amount=verdict.refund_amount if verdict else 0.0,
                is_auto_settled=verdict.is_auto_settled if verdict else False,
                requires_human_approval=verdict.requires_human_approval if verdict else True,
                negotiation_turns_count=len(resolved_claim.negotiation_transcript),
                buyer_advocate_summary=buyer_summary,
                merchant_defender_summary=merchant_summary,
                judicial_rationale=verdict.judicial_rationale if verdict else "",
                car_issued_to_supplier=verdict.car_issued_to_supplier if verdict else None,
                action_items=verdict.action_items if verdict else [],
                claim=resolved_claim
            )

        except CircuitBreakerOpenException as cbe:
            span.record_exception(cbe)
            logger.warning(f"Upstream circuit breaker active during claim evaluation: {cbe}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Circuit breaker active: {str(cbe)}. Evidence systems undergoing maintenance."
            )
        except Exception as e:
            span.record_exception(e)
            logger.error(f"Failed to execute dispute resolution workflow: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Dispute resolution execution failed: {str(e)}")
        finally:
            async with in_flight_lock:
                in_flight_claims_count = max(0, in_flight_claims_count - 1)


@router.get("/claims/{claim_id}", response_model=DisputeClaimResponse)
async def get_dispute_claim(claim_id: str):
    """Retrieves full dispute details, debate transcript, and arbitration verdict"""
    claim = CLAIMS_DB.get(claim_id)
    if not claim:
        raise HTTPException(status_code=404, detail=f"Dispute claim '{claim_id}' not found.")

    buyer_summary = ""
    merchant_summary = ""
    for turn in claim.negotiation_transcript:
        if turn.speaker.value == "BUYER_ADVOCATE":
            buyer_summary = turn.argument
        elif turn.speaker.value == "MERCHANT_DEFENDER":
            merchant_summary = turn.argument

    verdict = claim.verdict

    return DisputeClaimResponse(
        claim_id=claim.claim_id,
        order_id=claim.order_id,
        tenant_id=claim.tenant_id,
        status=claim.status,
        reason=claim.reason,
        claim_amount=claim.claim_amount,
        outcome=verdict.outcome if verdict else None,
        refund_amount=verdict.refund_amount if verdict else 0.0,
        is_auto_settled=verdict.is_auto_settled if verdict else False,
        requires_human_approval=verdict.requires_human_approval if verdict else True,
        negotiation_turns_count=len(claim.negotiation_transcript),
        buyer_advocate_summary=buyer_summary,
        merchant_defender_summary=merchant_summary,
        judicial_rationale=verdict.judicial_rationale if verdict else "",
        car_issued_to_supplier=verdict.car_issued_to_supplier if verdict else None,
        action_items=verdict.action_items if verdict else [],
        claim=claim
    )


@router.get("/claims", response_model=List[DisputeClaim])
async def list_all_claims():
    """Lists all active and settled dispute claims in the system"""
    return list(CLAIMS_DB.values())


@router.get("/stats", response_model=DisputeStatsResponse)
async def get_dispute_stats():
    """Returns platform-wide dispute claims and auto-settlement metrics"""
    total = len(CLAIMS_DB)
    auto_settled = sum(1 for c in CLAIMS_DB.values() if c.verdict and c.verdict.is_auto_settled)
    escalated = sum(1 for c in CLAIMS_DB.values() if c.status == DisputeStatus.ESCALATED)
    refund_total = sum(c.verdict.refund_amount for c in CLAIMS_DB.values() if c.verdict)

    outcomes: Dict[str, int] = {}
    reasons: Dict[str, int] = {}

    for c in CLAIMS_DB.values():
        r = c.reason.value
        reasons[r] = reasons.get(r, 0) + 1
        if c.verdict:
            o = c.verdict.outcome.value
            outcomes[o] = outcomes.get(o, 0) + 1

    return DisputeStatsResponse(
        status="healthy",
        total_claims=total,
        auto_settled_claims=auto_settled,
        escalated_claims=escalated,
        total_refunded_amount=round(refund_total, 2),
        outcomes_breakdown=outcomes,
        reasons_breakdown=reasons
    )


@router.get("/health/ready")
async def readiness_check():
    """Readiness probe checking container draining status for Traefik traffic routing"""
    if is_shutting_down:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "shutting_down", "service": "dispute-resolution-service"}
        )
    return {"status": "ready", "service": "dispute-resolution-service", "in_flight_claims": in_flight_claims_count}


@router.get("/health/live")
@router.get("/health")
async def health_check():
    """Health and liveness check endpoint"""
    return {
        "status": "healthy",
        "service": "dispute-resolution-service",
        "agents": ["BuyerAdvocateAgent", "MerchantDefenderAgent", "ImpartialArbitratorAgent"],
        "rag_capabilities": ["Self-RAG (Statutory Policies)", "GraphRAG (Defect Verification)", "ClickHouse (Fraud Scoring)"],
        "nfr_protections": ["Redis Idempotency (@idempotent_api)", "AsyncCircuitBreakers", "Multi-Tenant Isolation", "Graceful Traffic Draining"]
    }
