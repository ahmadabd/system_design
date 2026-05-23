import logging
import httpx
from src.domain.services import UserClient, ProductClient
from shared.common.http_client import ResilientHTTPClient

logger = logging.getLogger("HTTPServiceClients")

class HTTPUserClient(UserClient):
    """Concrete adapter for REST communication with user-service"""

    def __init__(self, http_client: ResilientHTTPClient, base_url: str):
        self.http_client = http_client
        self.base_url = base_url.rstrip("/")

    async def verify_user(self, user_id: int) -> bool:
        url = f"{self.base_url}/users/{user_id}"
        try:
            logger.info(f"Verifying user {user_id} via HTTP: {url}")
            response = await self.http_client.get(url)
            if response.status_code == 200:
                logger.info(f"User {user_id} verified successfully.")
                return True
            logger.warning(f"User {user_id} verification failed: HTTP status {response.status_code}")
            return False
        except Exception as e:
            logger.error(f"Failed to contact user-service for user {user_id}: {e}")
            return False

class HTTPProductClient(ProductClient):
    """Concrete adapter for REST communication with product-service"""

    def __init__(self, http_client: ResilientHTTPClient, base_url: str):
        self.http_client = http_client
        self.base_url = base_url.rstrip("/")

    async def verify_product(self, product_id: int) -> bool:
        url = f"{self.base_url}/products/{product_id}"
        try:
            logger.info(f"Verifying product {product_id} via HTTP: {url}")
            response = await self.http_client.get(url)
            if response.status_code == 200:
                logger.info(f"Product {product_id} verified successfully.")
                return True
            logger.warning(f"Product {product_id} verification failed: HTTP status {response.status_code}")
            return False
        except Exception as e:
            logger.error(f"Failed to contact product-service for product {product_id}: {e}")
            return False
