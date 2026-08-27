import uuid
import logging
from fastapi import APIRouter, Header, HTTPException
from opentelemetry import trace
from src.domain.schemas import CopilotQueryRequest, CopilotResponseDTO
from src.application.workflow import copilot_app
from src.application.metrics import copilot_requests_total

logger = logging.getLogger("CopilotAPI")
router = APIRouter(tags=["Merchant Copilot"])
tracer = trace.get_tracer("merchant-copilot-service")

@router.post("/chat", response_model=CopilotResponseDTO)
async def copilot_chat(
    req: CopilotQueryRequest,
    x_tenant_id: str = Header(default="store_tech", alias="X-Tenant-ID")
):
    """
    Execute Hybrid Text-to-SQL + Policy RAG via LangGraph StateGraph.
    Queries ClickHouse OLAP for quantitative analytics and Qdrant for policy guidelines.
    """
    tenant = req.tenant_id or x_tenant_id
    session_id = req.session_id or f"copilot_{uuid.uuid4().hex[:8]}"
    
    with tracer.start_as_current_span("Merchant Copilot: Chat Workflow") as span:
        span.set_attribute("copilot.query", req.query)
        span.set_attribute("copilot.session_id", session_id)
        span.set_attribute("tenant.id", tenant)

        initial_state = {
            "messages": [],
            "query": req.query,
            "tenant_id": tenant,
            "session_id": session_id,
            "intent": "",
            "relevant_tables": [],
            "linked_schema_ddl": "",
            "retrieved_policies": [],
            "generated_sql": None,
            "is_sql_valid": False,
            "ast_error": None,
            "runtime_error": None,
            "correction_attempts": 0,
            "max_corrections": 3,
            "sql_result_rows": [],
            "final_markdown_report": "",
            "is_safe": True
        }
        
        try:
            final_state = await copilot_app.ainvoke(initial_state)
            intent = final_state.get("intent", "structured_analytics")
            span.set_attribute("copilot.intent", intent)
            span.set_attribute("copilot.correction_attempts", final_state.get("correction_attempts", 0))
            span.set_attribute("copilot.is_safe", final_state.get("is_safe", True))
            
            copilot_requests_total.labels(intent=intent, status="success", tenant_id=tenant).inc()
            
            return CopilotResponseDTO(
                session_id=session_id,
                query=req.query,
                tenant_id=tenant,
                intent=intent,
                generated_sql=final_state.get("generated_sql"),
                sql_execution_result=final_state.get("sql_result_rows"),
                retrieved_policies=final_state.get("retrieved_policies"),
                correction_attempts=final_state.get("correction_attempts", 0),
                final_markdown_report=final_state.get("final_markdown_report", "Report generated successfully."),
                is_safe=final_state.get("is_safe", True)
            )
        except Exception as e:
            span.record_exception(e)
            logger.error(f"Error executing copilot workflow: {e}", exc_info=True)
            copilot_requests_total.labels(intent="error", status="failed", tenant_id=tenant).inc()
            raise HTTPException(status_code=500, detail=f"Copilot execution failed: {str(e)}")

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "merchant-copilot-service",
        "olap_backend": "ClickHouse",
        "vector_backend": "Qdrant",
        "rag_mode": "Hybrid Text-to-SQL + Policy RAG"
    }
