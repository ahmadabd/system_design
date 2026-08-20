import logging
from typing import Dict, Any, Optional
import redis.asyncio as aioredis
from shared.common.http_client import ResilientHTTPClient
from shared.common.resilience import AsyncCircuitBreaker
from src.infrastructure.config import settings

logger = logging.getLogger("SupportServiceClients")

class OrderServiceClient:
    """Resilient client adapter for querying live order status from order-service"""
    def __init__(self, http_client: ResilientHTTPClient, base_url: str, redis_url: Optional[str] = None):
        self.http_client = http_client
        self.base_url = base_url.rstrip("/")
        self.redis = aioredis.from_url(redis_url, decode_responses=True) if redis_url else None
        self.breaker = AsyncCircuitBreaker("order-client-breaker", failure_threshold=3, recovery_timeout=10.0)

    async def get_order(self, order_id: int) -> Optional[Dict[str, Any]]:
        """Fetches live order details by ID with circuit breaker and Redis cache fallback"""
        url = f"{self.base_url}/{order_id}"
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        headers = {"X-Tenant-ID": tenant.slug if tenant else "store_tech"}

        async def _fetch():
            logger.info(f"Querying order-service for order_id={order_id} via {url} (Tenant: {headers['X-Tenant-ID']})")
            response = await self.http_client.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                # Cache order details for fallback if order-service temporarily degrades
                if self.redis:
                    try:
                        import json
                        await self.redis.set(f"cache:order:{order_id}", json.dumps(data), ex=300)
                    except Exception as e:
                        logger.warning(f"Failed to cache order {order_id}: {e}")
                return data
            elif response.status_code == 404:
                logger.warning(f"Order {order_id} not found in order-service")
                return None
            else:
                response.raise_for_status()

        try:
            return await self.breaker.call(_fetch)
        except Exception as e:
            logger.error(f"Error fetching order {order_id} via HTTP: {e}. Attempting cache fallback...")
            if self.redis:
                try:
                    import json
                    cached = await self.redis.get(f"cache:order:{order_id}")
                    if cached:
                        logger.info(f"Retrieved order {order_id} from Redis cache fallback.")
                        return json.loads(cached)
                except Exception as cache_err:
                    logger.warning(f"Cache fallback lookup failed for order {order_id}: {cache_err}")
            return None

    async def list_user_orders(self, user_id: int) -> list[Dict[str, Any]]:
        """Fetches all platform orders and filters those belonging to user_id"""
        url = f"{self.base_url}/"
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        headers = {"X-Tenant-ID": tenant.slug if tenant else "store_tech"}

        async def _fetch():
            logger.info(f"Querying order-service for orders belonging to user_id={user_id} (Tenant: {headers['X-Tenant-ID']})")
            response = await self.http_client.get(url, headers=headers)
            if response.status_code == 200:
                all_orders = response.json()
                return [o for o in all_orders if int(o.get("user_id", -1)) == int(user_id)]
            return []

        try:
            return await self.breaker.call(_fetch)
        except Exception as e:
            logger.error(f"Error listing orders for user {user_id}: {e}")
            return []



class ProductServiceClient:
    """Resilient client adapter for querying product catalog from product-service"""
    def __init__(self, http_client: ResilientHTTPClient, base_url: str, redis_url: Optional[str] = None):
        self.http_client = http_client
        self.base_url = base_url.rstrip("/")
        self.redis = aioredis.from_url(redis_url, decode_responses=True) if redis_url else None
        self.breaker = AsyncCircuitBreaker("product-client-breaker", failure_threshold=3, recovery_timeout=10.0)

    async def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        """Fetches product details by ID"""
        url = f"{self.base_url}/{product_id}"
        from shared.common.tenant import get_tenant_or_none
        tenant = get_tenant_or_none()
        headers = {"X-Tenant-ID": tenant.slug if tenant else "store_tech"}

        async def _fetch():
            logger.info(f"Querying product-service for product_id={product_id} via {url} (Tenant: {headers['X-Tenant-ID']})")
            response = await self.http_client.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"Product {product_id} not found in product-service")
                return None
            else:
                response.raise_for_status()


        try:
            return await self.breaker.call(_fetch)
        except Exception as e:
            logger.error(f"Error fetching product {product_id} via HTTP: {e}")
            return None


class UserServiceClient:
    """Resilient client adapter for querying customer identity from user-service"""
    def __init__(self, http_client: ResilientHTTPClient, base_url: str, redis_url: Optional[str] = None):
        self.http_client = http_client
        self.base_url = base_url.rstrip("/")
        self.redis = aioredis.from_url(redis_url, decode_responses=True) if redis_url else None
        self.breaker = AsyncCircuitBreaker("user-client-breaker", failure_threshold=3, recovery_timeout=10.0)

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Fetches customer profile by ID"""
        url = f"{self.base_url}/{user_id}"

        async def _fetch():
            logger.info(f"Querying user-service for user_id={user_id} via {url}")
            response = await self.http_client.get(url)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                response.raise_for_status()

        try:
            return await self.breaker.call(_fetch)
        except Exception as e:
            logger.error(f"Error fetching user {user_id} via HTTP: {e}")
            return None

# Singleton instances for support service
_shared_http = ResilientHTTPClient(timeout=5.0)
order_client = OrderServiceClient(_shared_http, settings.ORDER_SERVICE_URL, redis_url=settings.REDIS_URL)
product_client = ProductServiceClient(_shared_http, settings.PRODUCT_SERVICE_URL, redis_url=settings.REDIS_URL)
user_client = UserServiceClient(_shared_http, settings.USER_SERVICE_URL, redis_url=settings.REDIS_URL)
