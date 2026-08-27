import logging
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from src.application.graph_state import DiscoveryState
from src.application.graph_nodes import (
    parse_constraints_node,
    generate_hyde_and_subqueries_node,
    qdrant_hybrid_search_node,
    cross_encoder_rerank_node,
    bundle_optimizer_node,
    synthesize_recommendation_node,
)

logger = logging.getLogger("DiscoveryGraphBuilder")


def build_discovery_graph():
    """
    Constructs and compiles the 6-node LangGraph workflow for Product Discovery & Bundle Building.
    """
    workflow = StateGraph(DiscoveryState)

    # 1. Add nodes
    workflow.add_node("parse_constraints", parse_constraints_node)
    workflow.add_node("hyde_and_decompose", generate_hyde_and_subqueries_node)
    workflow.add_node("qdrant_search", qdrant_hybrid_search_node)
    workflow.add_node("cross_encoder_rerank", cross_encoder_rerank_node)
    workflow.add_node("bundle_optimizer", bundle_optimizer_node)
    workflow.add_node("synthesizer", synthesize_recommendation_node)

    # 2. Add linear execution edges
    workflow.set_entry_point("parse_constraints")
    workflow.add_edge("parse_constraints", "hyde_and_decompose")
    workflow.add_edge("hyde_and_decompose", "qdrant_search")
    workflow.add_edge("qdrant_search", "cross_encoder_rerank")
    workflow.add_edge("cross_encoder_rerank", "bundle_optimizer")
    workflow.add_edge("bundle_optimizer", "synthesizer")
    workflow.add_edge("synthesizer", END)

    # 3. In-memory checkpointer for session state preservation across conversational turns
    checkpointer = MemorySaver()
    compiled_graph = workflow.compile(checkpointer=checkpointer)

    logger.info("Compiled Semantic Product Discovery LangGraph state machine.")
    return compiled_graph
