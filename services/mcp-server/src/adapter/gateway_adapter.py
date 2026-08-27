import logging
import uuid
from typing import Dict, Any, Optional
import httpx

from shared.common.http_client import ResilientHTTPClient
from shared.common.resilience import CircuitBreakerOpenException
from src.infrastructure.config import settings

logger = logging.getLogger("MCPGatewayAdapter")


class GatewayAdapter:
    """
    Adapter encapsulating communication between the MCP Server and the
    e-commerce microservices platform via Traefik or direct service routes.
    Preserves DDD boundary isolation and applies circuit breakers,
    tenant headers, and idempotency keys.
    """

    def __init__(self, http_client: Optional[ResilientHTTPClient] = None):
        self.client = http_client or ResilientHTTPClient(timeout=6.0)

    def _build_headers(
        self,
        tenant_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        extra_headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "X-Tenant-ID": tenant_id or settings.DEFAULT_TENANT,
        }
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key
        if extra_headers:
            headers.update(extra_headers)
        return headers

    async def register_user(
        self,
        username: str,
        email: str,
        password: str,
        tenant_id: str = "public",
        idempotency_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """Call User Service to register a new account."""
        key = idempotency_key or f"mcp-reg-{email}-{uuid.uuid4().hex[:8]}"
        headers = self._build_headers(tenant_id=tenant_id, idempotency_key=key)
        url = f"{settings.USER_SERVICE_URL}/"

        try:
            response = await self.client.post(
                url,
                json={"username": username, "email": email, "password": password},
                headers=headers
            )
            if response.status_code == 201:
                return {
                    "success": True,
                    "status": "created",
                    "data": response.json(),
                    "idempotency_key": key
                }
            return {
                "success": False,
                "status": "error",
                "message": response.text,
                "status_code": response.status_code
            }
        except CircuitBreakerOpenException as cbe:
            logger.warning(f"User service circuit breaker open: {cbe}")
            return {
                "success": False,
                "status": "degraded",
                "message": "User registration service is temporarily degraded/unavailable. Please retry shortly."
            }
        except Exception as exc:
            logger.error(f"Error registering user: {exc}")
            return {
                "success": False,
                "status": "error",
                "message": f"Failed to register user: {str(exc)}"
            }

    async def get_user_profile(self, user_id: int, tenant_id: str = "public") -> Dict[str, Any]:
        """Fetch user profile information."""
        headers = self._build_headers(tenant_id=tenant_id)
        url = f"{settings.USER_SERVICE_URL}/{user_id}"

        try:
            response = await self.client.get(url, headers=headers)
            if response.status_code == 200:
                return {"success": True, "status": "found", "data": response.json()}
            elif response.status_code == 404:
                return {"success": False, "status": "not_found", "message": f"User #{user_id} not found."}
            return {"success": False, "status": "error", "message": response.text}
        except CircuitBreakerOpenException:
            return {"success": False, "status": "degraded", "message": "User lookup degraded due to active circuit breaker."}
        except Exception as exc:
            return {"success": False, "status": "error", "message": str(exc)}

    async def get_product(self, product_id: int, tenant_id: str = "store_tech") -> Dict[str, Any]:
        """Fetch single product details with live stock count."""
        headers = self._build_headers(tenant_id=tenant_id)
        url = f"{settings.PRODUCT_SERVICE_URL}/{product_id}"

        try:
            response = await self.client.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "status": "found",
                    "data": data,
                    "in_stock": (data.get("stock", 0) > 0)
                }
            elif response.status_code == 404:
                return {"success": False, "status": "not_found", "message": f"Product #{product_id} not found."}
            return {"success": False, "status": "error", "message": response.text}
        except CircuitBreakerOpenException:
            return {"success": False, "status": "degraded", "message": "Product catalog service is degraded."}
        except Exception as exc:
            return {"success": False, "status": "error", "message": str(exc)}

    async def list_products(self, tenant_id: str = "store_tech") -> Dict[str, Any]:
        """List products for a tenant catalog."""
        headers = self._build_headers(tenant_id=tenant_id)
        url = f"{settings.PRODUCT_SERVICE_URL}/"

        try:
            response = await self.client.get(url, headers=headers)
            if response.status_code == 200:
                return {"success": True, "status": "success", "data": response.json()}
            return {"success": False, "status": "error", "message": response.text}
        except CircuitBreakerOpenException:
            return {"success": False, "status": "degraded", "message": "Catalog listing service degraded."}
        except Exception as exc:
            return {"success": False, "status": "error", "message": str(exc)}

    async def create_order(
        self,
        user_id: int,
        product_id: int,
        quantity: int,
        total_price: float,
        store_id: int = 1,
        payment_method: str = "CREDIT_CARD",
        tenant_id: str = "store_tech",
        idempotency_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """Submit a new order, initiating the Kafka Choreographed Saga."""
        key = idempotency_key or f"mcp-ord-{user_id}-{product_id}-{uuid.uuid4().hex[:8]}"
        headers = self._build_headers(tenant_id=tenant_id, idempotency_key=key)
        url = f"{settings.ORDER_SERVICE_URL}/"
        payload = {
            "user_id": user_id,
            "product_id": product_id,
            "quantity": quantity,
            "total_price": total_price,
            "store_id": store_id,
            "payment_method": payment_method
        }

        try:
            response = await self.client.post(url, json=payload, headers=headers)
            if response.status_code == 201:
                return {
                    "success": True,
                    "status": "submitted",
                    "data": response.json(),
                    "idempotency_key": key,
                    "message": "Order created in PENDING state. Asynchronous Saga initiated."
                }
            return {
                "success": False,
                "status": "error",
                "message": response.text,
                "status_code": response.status_code
            }
        except CircuitBreakerOpenException as cbe:
            return {
                "success": False,
                "status": "degraded",
                "message": f"Order checkout degraded: {str(cbe)}. Try again in a few moments."
            }
        except Exception as exc:
            return {"success": False, "status": "error", "message": str(exc)}

    async def get_order_status(self, order_id: int, tenant_id: str = "store_tech") -> Dict[str, Any]:
        """Check order status and details."""
        headers = self._build_headers(tenant_id=tenant_id)
        url = f"{settings.ORDER_SERVICE_URL}/{order_id}"

        try:
            response = await self.client.get(url, headers=headers)
            if response.status_code == 200:
                return {"success": True, "status": "found", "data": response.json()}
            elif response.status_code == 404:
                return {"success": False, "status": "not_found", "message": f"Order #{order_id} not found."}
            return {"success": False, "status": "error", "message": response.text}
        except CircuitBreakerOpenException:
            return {"success": False, "status": "degraded", "message": "Order tracking service degraded."}
        except Exception as exc:
            return {"success": False, "status": "error", "message": str(exc)}

    async def cancel_order(
        self,
        order_id: int,
        reason: str = "Cancelled by customer via AI Agent",
        tenant_id: str = "store_tech"
    ) -> Dict[str, Any]:
        """Cancel an order and trigger Saga reversal/compensations."""
        key = f"mcp-cancel-{order_id}-{uuid.uuid4().hex[:6]}"
        headers = self._build_headers(tenant_id=tenant_id, idempotency_key=key)
        url = f"{settings.ORDER_SERVICE_URL}/{order_id}/cancel"

        try:
            response = await self.client.put(url, headers=headers)
            if response.status_code == 200:
                return {
                    "success": True,
                    "status": "cancelled",
                    "data": response.json(),
                    "message": f"Order #{order_id} has been cancelled successfully. Refund initiated."
                }
            elif response.status_code == 404:
                return {"success": False, "status": "not_found", "message": f"Order #{order_id} not found."}
            return {"success": False, "status": "error", "message": response.text}
        except CircuitBreakerOpenException:
            return {"success": False, "status": "degraded", "message": "Order cancellation service degraded."}
        except Exception as exc:
            return {"success": False, "status": "error", "message": str(exc)}

    async def discover_bundle(
        self,
        query: str,
        budget: Optional[float] = None,
        tenant_id: str = "store_tech"
    ) -> Dict[str, Any]:
        """Execute semantic product discovery and bundle optimization via LangGraph."""
        headers = self._build_headers(tenant_id=tenant_id)
        url = f"{settings.DISCOVERY_SERVICE_URL}/chat"
        payload = {
            "query": query,
            "session_id": f"mcp_disc_{uuid.uuid4().hex[:8]}",
            "tenant_id": tenant_id,
            "budget": budget
        }

        try:
            response = await self.client.post(url, json=payload, headers=headers)
            if response.status_code == 200:
                return {"success": True, "status": "success", "data": response.json()}
            return {"success": False, "status": "error", "message": response.text}
        except CircuitBreakerOpenException:
            return {"success": False, "status": "degraded", "message": "Product discovery service is degraded."}
        except Exception as exc:
            return {"success": False, "status": "error", "message": str(exc)}

    async def close(self):
        await self.client.close()


gateway_adapter = GatewayAdapter()
