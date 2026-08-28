import logging
from langgraph.graph import StateGraph, START, END

from src.domain.graph_state import GraphRAGState
from src.application.graph_nodes import (
    graph_query_classifier_node,
    entity_extractor_node,
    local_subgraph_traverser_node,
    global_community_reducer_node,
    graph_reasoning_synthesizer_node
)

logger = logging.getLogger("GraphRAGWorkflow")


def route_search_mode(state: GraphRAGState) -> str:
    """Routes execution to Local Multi-Hop Branch or Global Community Map-Reduce Branch"""
    mode = state.get("search_mode", "local_multihop")
    if mode == "global_community":
        return "global_branch"
    return "local_branch"


def create_graphrag_workflow():
    """Builds and compiles the Microsoft GraphRAG LangGraph StateMachine"""
    workflow = StateGraph(GraphRAGState)

    # 1. Register Nodes
    workflow.add_node("graph_query_classifier", graph_query_classifier_node)
    workflow.add_node("entity_extractor", entity_extractor_node)
    workflow.add_node("local_subgraph_traverser", local_subgraph_traverser_node)
    workflow.add_node("global_community_reducer", global_community_reducer_node)
    workflow.add_node("graph_reasoning_synthesizer", graph_reasoning_synthesizer_node)

    # 2. Add Flow Edges
    workflow.add_edge(START, "graph_query_classifier")

    # Conditional Branching: Local Multi-Hop vs Global Community Search
    workflow.add_conditional_edges(
        "graph_query_classifier",
        route_search_mode,
        {
            "local_branch": "entity_extractor",
            "global_branch": "global_community_reducer"
        }
    )

    # Local Branch Pipeline
    workflow.add_edge("entity_extractor", "local_subgraph_traverser")
    workflow.add_edge("local_subgraph_traverser", "graph_reasoning_synthesizer")

    # Global Branch Pipeline
    workflow.add_edge("global_community_reducer", "graph_reasoning_synthesizer")

    # Terminal Edge
    workflow.add_edge("graph_reasoning_synthesizer", END)

    app = workflow.compile()
    logger.info("Compiled GraphRAG LangGraph StateMachine successfully.")
    return app


graphrag_app = create_graphrag_workflow()
