import logging
import httpx
import redis.asyncio as aioredis
import json
from src.domain.services import UserClient, ProductClient
from shared.common.http_client import ResilientHTTPClient

logger = logging.getLogger("HTTPServiceClients")

class HTTPUserClient(UserClient):
    """Concrete adapter for REST communication with user-service"""

    def __init__(self, http_client: ResilientHTTPClient, base_url: str, redis_url: str = None):
        self.http_client = http_client
        self.base_url = base_url.rstrip("/")
        self.redis = aioredis.from_url(redis_url, decode_responses=True) if redis_url else None

    async def verify_user(self, user_id: int) -> bool:
        url = f"{self.base_url}/users/{user_id}"
        try:
            logger.info(f"Verifying user {user_id} via HTTP: {url}")
            response = await self.http_client.get(url)
            if response.status_code == 200:
                logger.info(f"User {user_id} verified successfully.")
                return True
            logger.warning(f"User {user_id} verification failed: HTTP status {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to contact user-service for user {user_id}: {e}")

        # If HTTP call failed, attempt cache fallback
        if self.redis:
            cache_key = f"cache:user:{user_id}"
            try:
                cached_val = await self.redis.get(cache_key)
                if cached_val:
                    logger.info(f"User {user_id} verified from Redis Cache Fallback (User Service down).")
                    return True
            except Exception as cache_err:
                logger.warning(f"Error querying Redis cache for user {user_id}: {cache_err}")

        return False

class HTTPProductClient(ProductClient):
    """Concrete adapter for REST communication with product-service"""

    def __init__(self, http_client: ResilientHTTPClient, base_url: str, redis_url: str = None):
        self.http_client = http_client
        self.base_url = base_url.rstrip("/")
        self.redis = aioredis.from_url(redis_url, decode_responses=True) if redis_url else None

    async def verify_product(self, product_id: int) -> bool:
        url = f"{self.base_url}/products/{product_id}"
        try:
            logger.info(f"Verifying product {product_id} via HTTP: {url}")
            response = await self.http_client.get(url)
            if response.status_code == 200:
                logger.info(f"Product {product_id} verified successfully.")
                return True
            logger.warning(f"Product {product_id} verification failed: HTTP status {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to contact product-service for product {product_id}: {e}")

        # If HTTP call failed, attempt cache fallback
        if self.redis:
            cache_key = f"cache:product:{product_id}"
            try:
                cached_val = await self.redis.get(cache_key)
                if cached_val:
                    logger.info(f"Product {product_id} verified from Redis Cache Fallback (Product Service down).")
                    return True
            except Exception as cache_err:
                logger.warning(f"Error querying Redis cache for product {product_id}: {cache_err}")

        return False
