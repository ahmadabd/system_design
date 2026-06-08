from typing import List

class Product:
    """Product Aggregate Root"""
    def __init__(
        self,
        name: str,
        price: float,
        stock: int,
        store_id: int = 1,
        id: int | None = None
    ):
        self.id = id
        self.name = name
        self.price = price
        self.stock = stock
        self.store_id = store_id
        self.domain_events: List[dict] = []

    @classmethod
    def create(cls, name: str, price: float, stock: int, store_id: int = 1) -> "Product":
        """Factory to create a new product"""
        if price < 0:
            raise ValueError("Price cannot be negative")
        if stock < 0:
            raise ValueError("Initial stock cannot be negative")
        return cls(name=name, price=price, stock=stock, store_id=store_id)

    def reserve_stock(self, quantity: int, order_id: int) -> None:
        """Reserve a given quantity of stock for an order"""
        if quantity <= 0:
            raise ValueError("Reservation quantity must be positive")
        if self.stock < quantity:
            # Raise event for failure
            self.record_event({
                "event_type": "InventoryFailed",
                "order_id": order_id,
                "product_id": self.id,
                "reason": f"Insufficient stock. Available: {self.stock}, Requested: {quantity}"
            })
            raise ValueError(f"Insufficient stock for product '{self.name}'. Requested: {quantity}, Available: {self.stock}")

        # Commit stock mutation
        self.stock -= quantity
        
        # Record success event
        self.record_event({
            "event_type": "InventoryReserved",
            "order_id": order_id,
            "product_id": self.id,
            "quantity": quantity
        })

    def release_stock(self, quantity: int) -> None:
        """Release/add back stock when an order is cancelled"""
        if quantity <= 0:
            raise ValueError("Release quantity must be positive")
        self.stock += quantity

    def record_event(self, event: dict) -> None:
        """Add a domain event to be dispatched later"""
        self.domain_events.append(event)

    def clear_events(self) -> None:
        """Clear recorded events after they are dispatched"""
        self.domain_events.clear()
