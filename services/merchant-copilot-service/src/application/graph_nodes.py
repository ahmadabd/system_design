import time
import logging
from opentelemetry import trace
from src.domain.state import CopilotState
from src.adapter.llm_adapter import llm_adapter
from src.adapter.qdrant_policy_adapter import policy_adapter
from src.adapter.ast_validator import ast_validator
from src.infrastructure.clickhouse_client import clickhouse_client
from src.application.metrics import (
    copilot_node_duration_seconds,
    sql_self_corrections_total,
    clickhouse_query_duration_seconds
)

logger = logging.getLogger("CopilotGraphNodes")
tracer = trace.get_tracer("merchant-copilot-service")

def intent_classification_node(state: CopilotState) -> dict:
    """Classifies user intent: 'structured_analytics', 'policy_guidelines', or 'hybrid'"""
    with tracer.start_as_current_span("LangGraph node: intent_classification") as span:
        start = time.perf_counter()
        query = state["query"]
        intent = llm_adapter.classify_intent(query)
        span.set_attribute("copilot.intent", intent)
        span.set_attribute("copilot.query", query)
        
        duration = time.perf_counter() - start
        copilot_node_duration_seconds.labels(node_name="intent_classification").observe(duration)
        logger.info(f"[Node: intent_classification] Query: '{query}' -> Intent: '{intent}' ({duration:.3f}s)")
        return {"intent": intent}

def schema_linking_and_policy_retrieval_node(state: CopilotState) -> dict:
    """Retrieves relevant ClickHouse DDL schemas and Qdrant policy documents"""
    with tracer.start_as_current_span("LangGraph node: schema_linking_and_policy_retrieval") as span:
        start = time.perf_counter()
        query = state["query"]
        intent = state["intent"]
        
        relevant_tables = []
        linked_ddl_parts = []
        retrieved_policies = []
        
        # 1. Retrieve schemas if SQL is required
        if intent in ["structured_analytics", "hybrid"]:
            matched_schemas = policy_adapter.link_relevant_schemas(query, limit=3)
            for s in matched_schemas:
                tbl = s.get("table_name", "")
                if tbl and tbl not in relevant_tables:
                    relevant_tables.append(tbl)
                ddl = s.get("ddl", "")
                if ddl:
                    linked_ddl_parts.append(ddl)
        
        # 2. Retrieve policies if policy context is required
        if intent in ["policy_guidelines", "hybrid"]:
            retrieved_policies = policy_adapter.search_policies(query, limit=2)
            span.set_attribute("copilot.policies_retrieved_count", len(retrieved_policies))

        linked_schema_ddl = "\n\n".join(linked_ddl_parts)
        span.set_attribute("copilot.linked_tables", ",".join(relevant_tables))

        duration = time.perf_counter() - start
        copilot_node_duration_seconds.labels(node_name="schema_linking_and_policy_retrieval").observe(duration)
        logger.info(f"[Node: schema_linking] Linked tables: {relevant_tables}, Policies: {len(retrieved_policies)} ({duration:.3f}s)")
        return {
            "relevant_tables": relevant_tables,
            "linked_schema_ddl": linked_schema_ddl,
            "retrieved_policies": retrieved_policies
        }

def sql_generation_node(state: CopilotState) -> dict:
    """Generates ClickHouse SQL query using schema context"""
    with tracer.start_as_current_span("LangGraph node: sql_generation") as span:
        start = time.perf_counter()
        query = state["query"]
        tenant_id = state["tenant_id"]
        linked_schema_ddl = state.get("linked_schema_ddl", "")
        
        generated_sql = llm_adapter.generate_sql(query, tenant_id, linked_schema_ddl)
        span.set_attribute("copilot.generated_sql", generated_sql)

        duration = time.perf_counter() - start
        copilot_node_duration_seconds.labels(node_name="sql_generation").observe(duration)
        logger.info(f"[Node: sql_generation] Generated SQL: '{generated_sql}' ({duration:.3f}s)")
        return {"generated_sql": generated_sql}

def ast_validation_node(state: CopilotState) -> dict:
    """Validates ClickHouse SQL AST for read-only safety and multi-tenant predicate enforcement"""
    with tracer.start_as_current_span("LangGraph node: ast_validation") as span:
        start = time.perf_counter()
        sql = state.get("generated_sql", "")
        tenant_id = state["tenant_id"]
        
        is_valid, error = ast_validator.validate_sql(sql, tenant_id=tenant_id)
        span.set_attribute("copilot.sql_valid", is_valid)
        if error:
            span.set_attribute("copilot.ast_error", error)

        duration = time.perf_counter() - start
        copilot_node_duration_seconds.labels(node_name="ast_validation").observe(duration)
        logger.info(f"[Node: ast_validation] Valid={is_valid}, Error={error} ({duration:.3f}s)")
        return {
            "is_sql_valid": is_valid,
            "ast_error": error,
            "is_safe": is_valid
        }

def sql_self_correction_node(state: CopilotState) -> dict:
    """Heals a failed SQL query using LLM reflection on the error message"""
    with tracer.start_as_current_span("LangGraph node: sql_self_correction") as span:
        start = time.perf_counter()
        query = state["query"]
        tenant_id = state["tenant_id"]
        failed_sql = state.get("generated_sql", "")
        error_msg = state.get("ast_error") or state.get("runtime_error") or "Unknown SQL error"
        linked_schema_ddl = state.get("linked_schema_ddl", "")
        attempts = state.get("correction_attempts", 0) + 1
        
        sql_self_corrections_total.labels(reason="ast_or_runtime_error", tenant_id=tenant_id).inc()
        corrected_sql = llm_adapter.fix_sql_error(query, tenant_id, failed_sql, error_msg, linked_schema_ddl)
        
        span.set_attribute("copilot.correction_attempt", attempts)
        span.set_attribute("copilot.corrected_sql", corrected_sql)

        duration = time.perf_counter() - start
        copilot_node_duration_seconds.labels(node_name="sql_self_correction").observe(duration)
        logger.info(f"[Node: sql_self_correction] Attempt #{attempts}: '{corrected_sql}' (Reason: {error_msg})")
        return {
            "generated_sql": corrected_sql,
            "correction_attempts": attempts,
            "ast_error": None,
            "runtime_error": None
        }

def clickhouse_execution_node(state: CopilotState) -> dict:
    """Executes validated read-only SQL on ClickHouse OLAP engine"""
    with tracer.start_as_current_span("LangGraph node: clickhouse_execution") as span:
        start = time.perf_counter()
        sql = state.get("generated_sql", "")
        span.set_attribute("db.statement", sql)
        
        try:
            ch_start = time.perf_counter()
            rows = clickhouse_client.execute_query(sql)
            ch_duration = time.perf_counter() - ch_start
            clickhouse_query_duration_seconds.observe(ch_duration)
            
            span.set_attribute("copilot.rows_returned", len(rows))
            duration = time.perf_counter() - start
            copilot_node_duration_seconds.labels(node_name="clickhouse_execution").observe(duration)
            logger.info(f"[Node: clickhouse_execution] Returned {len(rows)} rows in {ch_duration:.3f}s.")
            return {"sql_result_rows": rows, "runtime_error": None}
        except Exception as e:
            logger.warning(f"ClickHouse execution error: {e}")
            span.record_exception(e)
            return {"runtime_error": str(e), "is_sql_valid": False}

def hybrid_synthesis_node(state: CopilotState) -> dict:
    """Synthesizes SQL results and policy documentation into executive markdown"""
    with tracer.start_as_current_span("LangGraph node: hybrid_synthesis") as span:
        start = time.perf_counter()
        query = state["query"]
        tenant_id = state["tenant_id"]
        sql_rows = state.get("sql_result_rows", [])
        generated_sql = state.get("generated_sql")
        policies = state.get("retrieved_policies", [])
        
        report = llm_adapter.synthesize_report(
            query=query,
            tenant_id=tenant_id,
            sql_rows=sql_rows,
            generated_sql=generated_sql,
            policies=policies
        )
        
        duration = time.perf_counter() - start
        copilot_node_duration_seconds.labels(node_name="hybrid_synthesis").observe(duration)
        logger.info(f"[Node: hybrid_synthesis] Generated report ({len(report)} chars) in {duration:.3f}s.")
        return {"final_markdown_report": report}
