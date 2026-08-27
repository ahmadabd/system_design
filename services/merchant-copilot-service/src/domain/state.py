from typing import TypedDict, List, Dict, Any, Optional
from langchain_core.messages import BaseMessage

class CopilotState(TypedDict):
    """LangGraph State for Merchant Copilot Hybrid Text-to-SQL + Policy RAG"""
    messages: List[BaseMessage]
    query: str
    tenant_id: str
    session_id: str
    
    # 1. Intent & Routing
    intent: str  # "structured_analytics", "policy_guidelines", "hybrid"
    
    # 2. Schema Linking & Policy Retrieval
    relevant_tables: List[str]
    linked_schema_ddl: str
    retrieved_policies: List[Dict[str, Any]]
    
    # 3. SQL Generation & Validation
    generated_sql: Optional[str]
    is_sql_valid: bool
    ast_error: Optional[str]
    runtime_error: Optional[str]
    correction_attempts: int
    max_corrections: int
    
    # 4. Execution & Synthesis
    sql_result_rows: List[Dict[str, Any]]
    final_markdown_report: str
    is_safe: bool
