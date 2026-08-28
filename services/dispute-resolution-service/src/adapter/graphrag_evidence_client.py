import logging
import httpx
from typing import Dict, Any, Optional
from opentelemetry import trace
from shared.common.resilience import AsyncCircuitBreaker, CircuitBreakerOpenException
from src.infrastructure.config import settings

logger = logging.getLogger("GraphRAGEvidenceClient")
tracer = trace.get_tracer("dispute-resolution-service")

graphrag_circuit_breaker = AsyncCircuitBreaker(
    name="graphrag-evidence-breaker",
    failure_threshold=3,
    recovery_timeout=5.0
)


class GraphRAGEvidenceClient:
    """
    HTTP Client querying the Knowledge Graph RAG service to verify
    factory defects, batch recalls, and supplier culpability.
    """
    def __init__(self, base_url: str = settings.GRAPHRAG_SERVICE_URL):
        self.base_url = base_url

    async def investigate_product_defects(self, product_name: str, tenant_id: str = "store_tech") -> Dict[str, Any]:
        """Queries GraphRAG to detect multi-hop component & supplier defects"""
        with tracer.start_as_current_span("GraphRAG: investigate_product_defects") as span:
            span.set_attribute("product.name", product_name)
            span.set_attribute("tenant.id", tenant_id)

            async def _call_graphrag():
                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.post(
                        f"{self.base_url}/graphrag/query",
                        headers={"X-Tenant-ID": tenant_id, "Content-Type": "application/json"},
                        json={
                            "query": f"Is there any reported component or supplier defect with {product_name}?"
                        }
                    )
                    resp.raise_for_status()
                    return resp.json()

            try:
                data = await graphrag_circuit_breaker.call(_call_graphrag)
                span.set_attribute("graphrag.status", "success")

                # Parse GraphRAG response
                nodes_count = data.get("nodes_traversed_count", 0)
                reasoning_hops = data.get("reasoning_hops", [])
                
                # Check for defect / supplier keywords
                has_defect = any("defect" in hop.lower() or "throttle" in hop.lower() or "micro-cavity" in hop.lower() for hop in reasoning_hops)
                
                supplier = None
                if "coolmaster" in str(data).lower():
                    supplier = "CoolMaster Thermal Solutions Ltd (Shenzhen)"
                elif "neutrik" in str(data).lower():
                    supplier = "Neutrik AG (Liechtenstein)"
                elif "great lakes" in str(data).lower():
                    supplier = "Great Lakes Polymer Industries (Ohio)"

                return {
                    "has_known_defect": has_defect or nodes_count > 3,
                    "supplier_culpable": supplier,
                    "reasoning_hops": reasoning_hops,
                    "report": data.get("final_markdown_report", ""),
                    "source": "live_graphrag_service"
                }

            except Exception as e:
                logger.warning(f"GraphRAG service call failed or circuit open ({e}). Falling back to local defect heuristics.")
                span.record_exception(e)
                return self._local_defect_heuristic(product_name)

    def _local_defect_heuristic(self, product_name: str) -> Dict[str, Any]:
        """In-memory heuristic knowledge fallback when GraphRAG container is unreachable"""
        name_lower = product_name.lower()
        if "laptop" in name_lower or "gaming" in name_lower:
            return {
                "has_known_defect": True,
                "supplier_culpable": "CoolMaster Thermal Solutions Ltd (Shenzhen)",
                "defect_description": "Defect #DEF-8802: Micro-Cavity Seal Leakage & Thermal Throttling in Vapor Chamber Batch #2026-B",
                "reasoning_hops": [
                    "Gaming Laptop Pro -> Vapor Chamber -> Batch #2026-B -> Defect #DEF-8802"
                ],
                "source": "heuristic_knowledge_fallback"
            }
        elif "shure" in name_lower or "sm7b" in name_lower or "microphone" in name_lower or "vocal mic" in name_lower:
            return {
                "has_known_defect": True,
                "supplier_culpable": "Neutrik AG (Liechtenstein)",
                "defect_description": "Defect #DEF-3011: Unsoldered Pin 1 Ground Shell Shielding in Batch #501",
                "reasoning_hops": [
                    "Shure SM7B -> XLR Cable -> Batch #501 -> Defect #DEF-3011"
                ],
                "source": "heuristic_knowledge_fallback"
            }
        elif "chair" in name_lower:
            return {
                "has_known_defect": True,
                "supplier_culpable": "Great Lakes Polymer Industries (Ohio)",
                "defect_description": "Defect #DEF-4419: Elastomer Hydrolysis Fracture in Batch #889",
                "reasoning_hops": [
                    "ErgoChair Pro -> Lumbar Bracket -> Batch #889 -> Defect #DEF-4419"
                ],
                "source": "heuristic_knowledge_fallback"
            }
        return {
            "has_known_defect": False,
            "supplier_culpable": None,
            "defect_description": None,
            "reasoning_hops": [],
            "source": "heuristic_knowledge_fallback"
        }


graphrag_evidence_client = GraphRAGEvidenceClient()
