from abc import ABC, abstractmethod
from src.domain.payment import Payment

class PaymentRepository(ABC):
    """Abstract Outbound Port for Payment Persistence"""
    
    @abstractmethod
    async def save(self, payment: Payment) -> Payment:
        """Persist Payment state changes"""
        pass

    @abstractmethod
    async def find_by_id(self, payment_id: str) -> Payment | None:
        """Fetch payment record by primary key id"""
        pass

    @abstractmethod
    async def find_by_order_id(self, order_id: int) -> Payment | None:
        """Fetch payment record by order reference key"""
        pass

    @abstractmethod
    async def find_all(self) -> list[Payment]:
        """Fetch all historical platform payments"""
        pass

    @abstractmethod
    async def save_materialized_order(self, order_id: int, total_price: float, quantity: int, store_id: int = 1) -> None:
        """Save/upsert local materialized order details (CQRS view)"""
        pass

    @abstractmethod
    async def find_materialized_order(self, order_id: int) -> dict | None:
        """Find local materialized order details by order ID (CQRS view)"""
        pass
