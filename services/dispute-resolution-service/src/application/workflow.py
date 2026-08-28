import logging
from langgraph.graph import StateGraph, START, END
from src.application.state import DisputeWorkflowState
from src.application.graph_nodes import (
    buyer_advocate_node,
    merchant_defender_node,
    multi_source_evidence_node,
    impartial_arbitrator_node,
    settlement_engine_node
)

logger = logging.getLogger("DisputeWorkflow")


def build_dispute_resolution_workflow():
    """
    Constructs and compiles the Multi-Agent Negotiation & Dispute Resolution StateGraph:
    START -> buyer_advocate -> merchant_defender -> multi_source_evidence -> impartial_arbitrator -> settlement_engine -> END
    """
    builder = StateGraph(DisputeWorkflowState)

    # Register Nodes
    builder.add_node("buyer_advocate", buyer_advocate_node)
    builder.add_node("merchant_defender", merchant_defender_node)
    builder.add_node("multi_source_evidence", multi_source_evidence_node)
    builder.add_node("impartial_arbitrator", impartial_arbitrator_node)
    builder.add_node("settlement_engine", settlement_engine_node)

    # Define Linear & Multi-Agent Transition Edges
    builder.add_edge(START, "buyer_advocate")
    builder.add_edge("buyer_advocate", "merchant_defender")
    builder.add_edge("merchant_defender", "multi_source_evidence")
    builder.add_edge("multi_source_evidence", "impartial_arbitrator")
    builder.add_edge("impartial_arbitrator", "settlement_engine")
    builder.add_edge("settlement_engine", END)

    compiled_app = builder.compile()
    logger.info("Dispute Resolution Multi-Agent StateGraph compiled successfully.")
    return compiled_app


dispute_app = build_dispute_resolution_workflow()
