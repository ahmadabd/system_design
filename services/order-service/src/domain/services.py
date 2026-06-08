from abc import ABC, abstractmethod

class UserClient(ABC):
    """Abstract Port representing outbound user verification boundary"""

    @abstractmethod
    async def verify_user(self, user_id: int) -> bool:
        """Verify if a user exists in the system"""
        pass

class ProductClient(ABC):
    """Abstract Port representing outbound product validation boundary"""

    @abstractmethod
    async def verify_product(self, product_id: int) -> bool:
        """Verify if a product exists and is valid"""
        pass

    @abstractmethod
    async def get_product_details(self, product_id: int) -> dict | None:
        """Fetch details of a product, or None if it doesn't exist"""
        pass
