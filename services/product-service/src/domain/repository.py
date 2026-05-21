from abc import ABC, abstractmethod
from src.domain.product import Product

class ProductRepository(ABC):
    """Abstract interface for Product repository (Port)"""

    @abstractmethod
    async def save(self, product: Product) -> Product:
        """Persist a Product aggregate"""
        pass

    @abstractmethod
    async def find_by_id(self, product_id: int) -> Product | None:
        """Find a Product by ID"""
        pass

    @abstractmethod
    async def find_all(self) -> list[Product]:
        """Retrieve all catalog products"""
        pass
