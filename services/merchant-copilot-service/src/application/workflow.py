import logging
from langgraph.graph import StateGraph, END
from src.domain.state import CopilotState
from src.application.graph_nodes import (
    intent_classification_node,
    schema_linking_and_policy_retrieval_node,
    sql_generation_node,
    ast_validation_node,
    sql_self_correction_node,
    clickhouse_execution_node,
    hybrid_synthesis_node
)

logger = logging.getLogger("CopilotWorkflow")

def route_after_linking(state: CopilotState) -> str:
    """Routes to SQL generation or straight to synthesis if pure policy intent"""
    intent = state.get("intent", "structured_analytics")
    if intent == "policy_guidelines":
        return "hybrid_synthesis"
    return "sql_generation"

def route_after_validation(state: CopilotState) -> str:
    """Routes to ClickHouse execution or self-correction healing loop"""
    is_valid = state.get("is_sql_valid", False)
    attempts = state.get("correction_attempts", 0)
    max_attempts = state.get("max_corrections", 3)
    
    if is_valid:
        return "clickhouse_execution"
    elif attempts < max_attempts:
        logger.info(f"SQL validation failed. Routing to self-correction (Attempt {attempts + 1}/{max_attempts}).")
        return "sql_self_correction"
    else:
        logger.warning("Max SQL self-correction attempts reached. Proceeding to synthesis with partial context.")
        return "hybrid_synthesis"

def route_after_execution(state: CopilotState) -> str:
    """Routes to self-correction if runtime error occurred, else synthesis"""
    runtime_err = state.get("runtime_error")
    attempts = state.get("correction_attempts", 0)
    max_attempts = state.get("max_corrections", 3)
    
    if runtime_err and attempts < max_attempts:
        logger.info(f"ClickHouse execution failed ({runtime_err}). Routing to self-correction.")
        return "sql_self_correction"
    return "hybrid_synthesis"

def build_copilot_workflow():
    """Builds and compiles the Merchant Copilot LangGraph StateGraph"""
    builder = StateGraph(CopilotState)
    
    # 1. Register Nodes
    builder.add_node("intent_classification", intent_classification_node)
    builder.add_node("schema_linking_and_policy_retrieval", schema_linking_and_policy_retrieval_node)
    builder.add_node("sql_generation", sql_generation_node)
    builder.add_node("ast_validation", ast_validation_node)
    builder.add_node("sql_self_correction", sql_self_correction_node)
    builder.add_node("clickhouse_execution", clickhouse_execution_node)
    builder.add_node("hybrid_synthesis", hybrid_synthesis_node)
    
    # 2. Set Entry Point
    builder.set_entry_point("intent_classification")
    
    # 3. Add Edges & Conditional Routing
    builder.add_edge("intent_classification", "schema_linking_and_policy_retrieval")
    
    builder.add_conditional_edges(
        "schema_linking_and_policy_retrieval",
        route_after_linking,
        {
            "sql_generation": "sql_generation",
            "hybrid_synthesis": "hybrid_synthesis"
        }
    )
    
    builder.add_edge("sql_generation", "ast_validation")
    
    builder.add_conditional_edges(
        "ast_validation",
        route_after_validation,
        {
            "clickhouse_execution": "clickhouse_execution",
            "sql_self_correction": "sql_self_correction",
            "hybrid_synthesis": "hybrid_synthesis"
        }
    )
    
    # Healing loop back to validation
    builder.add_edge("sql_self_correction", "ast_validation")
    
    builder.add_conditional_edges(
        "clickhouse_execution",
        route_after_execution,
        {
            "sql_self_correction": "sql_self_correction",
            "hybrid_synthesis": "hybrid_synthesis"
        }
    )
    
    builder.add_edge("hybrid_synthesis", END)
    
    workflow = builder.compile()
    logger.info("Merchant Copilot LangGraph StateGraph compiled successfully.")
    return workflow

copilot_app = build_copilot_workflow()
