from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

class CopilotQueryRequest(BaseModel):
    query: str = Field(..., description="Natural language analytical or merchant policy question")
    session_id: Optional[str] = Field(default=None, description="Optional session tracking identifier")
    tenant_id: str = Field(default="store_tech", description="Merchant store tenant identifier")

class SQLQueryPlanDTO(BaseModel):
    intent: str = Field(..., description="Query intent: 'structured_analytics', 'policy_guidelines', or 'hybrid'")
    generated_sql: Optional[str] = Field(default=None, description="Generated ClickHouse SQL statement")
    is_valid: bool = Field(default=False, description="Whether the SQL passed AST safety validation")
    validation_error: Optional[str] = Field(default=None, description="AST error if invalid")
    correction_attempts: int = Field(default=0, description="Number of self-correction attempts made")

class CopilotResponseDTO(BaseModel):
    session_id: str
    query: str
    tenant_id: str
    intent: str
    generated_sql: Optional[str] = None
    sql_execution_result: Optional[List[Dict[str, Any]]] = None
    retrieved_policies: Optional[List[Dict[str, Any]]] = None
    correction_attempts: int = 0
    final_markdown_report: str
    is_safe: bool = True
