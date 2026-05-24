from typing import List

class Payment:
    """Payment Domain Aggregate Root"""
    def __init__(
        self,
        order_id: int,
        amount: float,
        status: str = "PENDING",
        id: str | None = None
    ):
        self.id = id
        self.order_id = order_id
        self.amount = amount
        self.status = status
        self.domain_events: List[dict] = []

    @classmethod
    def create(cls, order_id: int, amount: float) -> "Payment":
        """Factory method to initialize a payment in PENDING state"""
        if amount <= 0:
            raise ValueError("Payment amount must be positive")
        return cls(order_id=order_id, amount=amount, status="PENDING")

    def succeed(self, payment_id: str) -> None:
        """Mark payment as successfully processed"""
        if self.status != "PENDING":
            raise ValueError(f"Cannot complete payment in status '{self.status}'")
        self.id = payment_id
        self.status = "SUCCEEDED"
        self.record_event({
            "event_type": "PaymentSucceeded",
            "payment_id": self.id,
            "order_id": self.order_id,
            "amount": self.amount
        })

    def fail(self, payment_id: str, reason: str = "") -> None:
        """Mark payment as failed"""
        if self.status != "PENDING":
            raise ValueError(f"Cannot fail payment in status '{self.status}'")
        self.id = payment_id
        self.status = "FAILED"
        self.record_event({
            "event_type": "PaymentFailed",
            "payment_id": self.id,
            "order_id": self.order_id,
            "amount": self.amount,
            "reason": reason
        })

    def record_event(self, event: dict) -> None:
        """Record domain events for integration publishing"""
        self.domain_events.append(event)

    def clear_events(self) -> None:
        """Clear recorded events after they are dispatched"""
        self.domain_events.clear()
