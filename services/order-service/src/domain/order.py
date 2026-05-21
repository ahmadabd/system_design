from typing import List

class Order:
    """Order Aggregate Root"""
    def __init__(
        self,
        user_id: int,
        product_id: int,
        quantity: int,
        total_price: float,
        status: str = "PENDING",
        id: int | None = None
    ):
        self.id = id
        self.user_id = user_id
        self.product_id = product_id
        self.quantity = quantity
        self.total_price = total_price
        self.status = status
        self.domain_events: List[dict] = []

    @classmethod
    def create(
        cls,
        user_id: int,
        product_id: int,
        quantity: int,
        total_price: float
    ) -> "Order":
        """Factory method to place an order, starting in PENDING state"""
        if quantity <= 0:
            raise ValueError("Quantity must be at least 1")
        if total_price <= 0:
            raise ValueError("Total price must be positive")

        order = cls(
            user_id=user_id,
            product_id=product_id,
            quantity=quantity,
            total_price=total_price,
            status="PENDING"
        )
        
        # Raise Domain Event
        order.record_event({
            "event_type": "OrderCreated",
            "user_id": user_id,
            "product_id": product_id,
            "quantity": quantity,
            "total_price": total_price
        })
        return order

    def confirm(self) -> None:
        """Confirm the order after successful inventory allocation"""
        if self.status != "PENDING":
            raise ValueError(f"Cannot confirm order in status '{self.status}'")
        self.status = "CONFIRMED"

    def cancel(self, reason: str = "") -> None:
        """Cancel the order due to payment or stock failures"""
        if self.status != "PENDING":
            raise ValueError(f"Cannot cancel order in status '{self.status}'")
        self.status = "CANCELLED"
        self.record_event({
            "event_type": "OrderCancelled",
            "order_id": self.id,
            "reason": reason
        })

    def record_event(self, event: dict) -> None:
        """Add a domain event to be dispatched later"""
        self.domain_events.append(event)

    def clear_events(self) -> None:
        """Clear recorded events after they are dispatched"""
        self.domain_events.clear()
