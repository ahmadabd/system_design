from abc import ABC, abstractmethod
from src.domain.product import Product
from src.domain.store import Store

class ProductRepository(ABC):
    """Abstract Outbound Port for Product Persistence"""
    @abstractmethod
    async def save(self, product: Product) -> Product:
        """Persist product aggregate changes to backing store"""
        pass

    @abstractmethod
    async def find_by_id(self, product_id: int, for_update: bool = False) -> Product | None:
        """Find product by unique key identity"""
        pass

    @abstractmethod
    async def find_all(self) -> list[Product]:
        """Fetch all catalog products"""
        pass

class StoreRepository(ABC):
    """Abstract Outbound Port for Store Persistence"""
    @abstractmethod
    async def save(self, store: Store) -> Store:
        """Persist store aggregate changes"""
        pass

    @abstractmethod
    async def find_by_id(self, store_id: int) -> Store | None:
        """Find store by unique key identity"""
        pass

    @abstractmethod
    async def find_all(self) -> list[Store]:
        """Fetch all stores"""
        pass
