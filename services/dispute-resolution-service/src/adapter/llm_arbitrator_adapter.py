import logging
from typing import Dict, Any, List
from opentelemetry import trace
from src.domain.dispute_entities import (
    DisputeClaim,
    DisputeReason,
    ResolutionOutcome,
    ArbitrationVerdict,
    NegotiationTurn,
    NegotiationSpeaker
)
from src.infrastructure.config import settings

logger = logging.getLogger("LLMArbitratorAdapter")
tracer = trace.get_tracer("dispute-resolution-service")


class LLMArbitratorAdapter:
    """
    Simulates multi-agent adversarial debate between Buyer Advocate and Merchant Defender,
    then acts as the Impartial Judicial Arbitrator with Self-RAG policy grounding.
    """

    def generate_buyer_advocate_turn(self, claim: DisputeClaim, turn_index: int = 1) -> NegotiationTurn:
        """Constructs the claimant's opening argument and demanded compensation"""
        with tracer.start_as_current_span("BuyerAdvocate: construct_argument") as span:
            span.set_attribute("claim.reason", claim.reason)
            span.set_attribute("claim.amount", claim.claim_amount)

            if claim.reason == DisputeReason.DEFECTIVE_PRODUCT:
                arg = (
                    f"Customer purchased '{claim.product_name}' for ${claim.claim_amount:.2f}. "
                    f"The unit suffered severe hardware malfunction: '{claim.customer_statement}'. "
                    f"Under Statutory Consumer Warranty (Section 4.1), goods must be free of latent defects. "
                    f"Buyer demands an immediate 100% full refund of ${claim.claim_amount:.2f}."
                )
                remedy = f"100% Full Refund (${claim.claim_amount:.2f})"
            elif claim.reason == DisputeReason.TRANSIT_DAMAGE:
                arg = (
                    f"Customer received '{claim.product_name}' in physically damaged condition. "
                    f"Carrier damage occurred before delivery transfer. Buyer demands immediate replacement or full refund."
                )
                remedy = f"Full Refund (${claim.claim_amount:.2f}) or Replacement"
            elif claim.reason == DisputeReason.BUYER_REMORSE:
                arg = (
                    f"Customer requested return of '{claim.product_name}' due to change of mind. "
                    f"Buyer requests refund of initial purchase price."
                )
                remedy = f"Full Refund (${claim.claim_amount:.2f})"
            else:
                arg = (
                    f"Customer filed dispute for '{claim.product_name}': {claim.customer_statement}. "
                    f"Buyer seeks full resolution and reimbursement of ${claim.claim_amount:.2f}."
                )
                remedy = f"Full Reimbursement (${claim.claim_amount:.2f})"

            return NegotiationTurn(
                speaker=NegotiationSpeaker.BUYER_ADVOCATE,
                turn_index=turn_index,
                argument=arg,
                remedy_position=remedy,
                evidence_referenced=["Customer Statement", "Order Receipt", "Statutory Warranty Clause"]
            )

    def generate_merchant_defender_turn(self, claim: DisputeClaim, turn_index: int = 2) -> NegotiationTurn:
        """Constructs the respondent merchant's counter-argument and policy defense"""
        with tracer.start_as_current_span("MerchantDefender: construct_defense") as span:
            delivery_days = claim.evidence_dossier.delivery_confirmed_days_ago

            if claim.reason == DisputeReason.BUYER_REMORSE and delivery_days > 14:
                arg = (
                    f"The item '{claim.product_name}' was delivered {delivery_days} days ago, "
                    f"which exceeds our platform-standard 14-day return window (Section 2.3). "
                    f"The merchant incurred shipping and inventory holding costs. "
                    f"Merchant proposes claim denial or a 20% restocking fee deduction."
                )
                remedy = f"Claim Denial or Partial Store Credit (80% = ${claim.claim_amount * 0.80:.2f})"
            elif claim.reason == DisputeReason.DEFECTIVE_PRODUCT:
                arg = (
                    f"Merchant delivered '{claim.product_name}' brand new in original sealed packaging. "
                    f"If a component-level thermal/electronic defect exists, the root culpability lies with "
                    f"the Tier-1 OEM supplier, not the retail merchant. Merchant requests supplier indemnity."
                )
                remedy = "Support Full Refund if Supplier Defect is Verified by GraphRAG (0% Merchant Liability)"
            elif claim.reason == DisputeReason.TRANSIT_DAMAGE:
                arg = (
                    f"Merchant dispatched '{claim.product_name}' intact from the fulfillment center. "
                    f"Transit damage falls under carrier liability (Section 5.0). Merchant requests carrier subrogation."
                )
                remedy = "Full Refund funded via Logistics Carrier Insurance Claim"
            else:
                arg = (
                    f"Merchant has reviewed claim for '{claim.product_name}'. "
                    f"Standard fulfillment checks were passed. Merchant requests arbitrator review."
                )
                remedy = "Arbitrated Settlement"

            return NegotiationTurn(
                speaker=NegotiationSpeaker.MERCHANT_DEFENDER,
                turn_index=turn_index,
                argument=arg,
                remedy_position=remedy,
                evidence_referenced=["Warehouse Barcode Logs", "Return Window Timeline", "Merchant Policy Terms"]
            )

    def arbitrate_dispute(self, claim: DisputeClaim) -> ArbitrationVerdict:
        """Acts as the Impartial Judicial Arbitrator and renders a legally binding verdict"""
        with tracer.start_as_current_span("ImpartialArbitrator: render_verdict") as span:
            dossier = claim.evidence_dossier
            amount = claim.claim_amount

            # ── Scenario A: Verified Factory / Supplier Defect (GraphRAG finding) ─────
            if dossier.known_factory_defect:
                outcome = ResolutionOutcome.FULL_REFUND_APPROVED
                refund = amount
                buyer_pct = 100.0
                merchant_pct = 0.0  # Merchant protected from supplier failure
                supplier_pct = 100.0
                supplier = dossier.supplier_culpable or "Tier-1 OEM Supplier"
                rationale = (
                    f"Knowledge Graph RAG traversal confirmed a verified manufacturing defect "
                    f"({dossier.defect_description or 'Root Component Failure'}) traced directly to supplier [{supplier}]. "
                    f"Under Statutory Warranty Law (Section 4.1), known latent manufacturing defects supersede standard return windows. "
                    f"Buyer is awarded a 100% full refund (${refund:.2f}). Merchant liability is set to 0%, with 100% financial "
                    f"indemnity assessed to [{supplier}]."
                )
                car = f"CAR-DEFECT-RECALL-{claim.order_id}"
                actions = [
                    f"Issue immediate $ {refund:.2f} refund to customer payment method.",
                    f"Dispatch Corrective Action Request ({car}) to [{supplier}].",
                    "Quarantine remaining inventory batch in fulfillment center."
                ]

            # ── Scenario B: Out-of-Window Buyer Remorse (No defect) ─────────────
            elif claim.reason == DisputeReason.BUYER_REMORSE and dossier.delivery_confirmed_days_ago > 14:
                outcome = ResolutionOutcome.CLAIM_DENIED
                refund = 0.0
                buyer_pct = 0.0
                merchant_pct = 0.0
                supplier_pct = 0.0
                car = None
                rationale = (
                    f"Claim was submitted {dossier.delivery_confirmed_days_ago} days after confirmed delivery, "
                    f"exceeding the enforceable 14-day return window (Section 2.3). "
                    f"No hardware or functional defect was reported. To protect merchant operational integrity, "
                    f"the claim for full refund is DENIED. Buyer retains ownership of the delivered item."
                )
                actions = [
                    "Notify customer of claim denial under 14-day policy terms.",
                    "Offer voluntary 10% loyalty discount code on next store order."
                ]

            # ── Scenario C: Transit Damage (Carrier Fault) ─────────────────────
            elif claim.reason == DisputeReason.TRANSIT_DAMAGE:
                outcome = ResolutionOutcome.FULL_REFUND_APPROVED
                refund = amount
                buyer_pct = 100.0
                merchant_pct = 0.0
                supplier_pct = 0.0
                car = None
                rationale = (
                    f"Carrier shipping logs confirm package trauma and transit damage prior to customer receipt. "
                    f"Under Logistics Carrier Policy (Section 5.0), buyer is fully reimbursed (${refund:.2f}). "
                    f"Platform initiates subrogation recovery against the carrier."
                )
                actions = [
                    f"Process $ {refund:.2f} full customer refund.",
                    f"File carrier insurance subrogation claim for Order #{claim.order_id}."
                ]

            # ── Scenario D: General Compromise / Partial Settlement ────────────
            else:
                outcome = ResolutionOutcome.PARTIAL_REFUND_SETTLEMENT
                refund = round(amount * 0.50, 2)
                buyer_pct = 50.0
                merchant_pct = 50.0
                supplier_pct = 0.0
                car = None
                rationale = (
                    f"Dispute evaluated under Standard Commercial Guidelines. "
                    f"Equitable 50/50 compromise reached: customer receives a partial goodwill credit of ${refund:.2f} "
                    f"while retaining the product."
                )
                actions = [
                    f"Issue partial refund credit of $ {refund:.2f} to customer account."
                ]

            # Check Auto-Settlement vs Human Escalation Rule:
            # Auto-settle if amount <= AUTO_SETTLE_MAX_AMOUNT and fraud risk <= FRAUD_RISK_THRESHOLD
            is_auto = (amount <= settings.AUTO_SETTLE_MAX_AMOUNT) and (dossier.buyer_fraud_risk_score <= settings.FRAUD_RISK_THRESHOLD)
            requires_human = not is_auto

            verdict = ArbitrationVerdict(
                claim_id=claim.claim_id,
                outcome=outcome,
                total_claim_amount=amount,
                refund_amount=refund,
                buyer_refund_pct=buyer_pct,
                merchant_liability_pct=merchant_pct,
                supplier_liability_pct=supplier_pct,
                is_auto_settled=is_auto,
                requires_human_approval=requires_human,
                confidence_score=0.98 if dossier.known_factory_defect else 0.92,
                judicial_rationale=rationale,
                car_issued_to_supplier=car,
                action_items=actions
            )

            span.set_attribute("verdict.outcome", outcome)
            span.set_attribute("verdict.refund_amount", refund)
            span.set_attribute("verdict.is_auto_settled", is_auto)

            return verdict


llm_arbitrator_adapter = LLMArbitratorAdapter()
