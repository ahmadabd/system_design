from abc import ABC, abstractmethod
from src.domain.order import Order

class OrderRepository(ABC):
    """Abstract interface for Order repository (Port)"""

    @abstractmethod
    async def save(self, order: Order) -> Order:
        """Persist an Order aggregate"""
        pass

    @abstractmethod
    async def find_by_id(self, order_id: int) -> Order | None:
        """Find an Order by ID"""
        pass

    @abstractmethod
    async def find_all(self) -> list[Order]:
        """Fetch all platform orders"""
        pass
