import uuid
from datetime import datetime, timezone
from pydantic import BaseModel, Field

class EventMetadata(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: str = "1.0"

class DomainEvent(BaseModel):
    metadata: EventMetadata = Field(default_factory=EventMetadata)

# --- Specific Event Contracts ---

class UserRegisteredEvent(DomainEvent):
    event_type: str = "UserRegistered"
    user_id: int
    email: str
    username: str

class OrderCreatedEvent(DomainEvent):
    event_type: str = "OrderCreated"
    order_id: int
    user_id: int
    product_id: int
    quantity: int
    total_price: float
    store_id: int
    is_famous: bool = False
    payment_method: str = "AUTOMATIC"

class InventoryReservedEvent(DomainEvent):
    event_type: str = "InventoryReserved"
    order_id: int
    product_id: int
    quantity: int

class InventoryFailedEvent(DomainEvent):
    event_type: str = "InventoryFailed"
    order_id: int
    product_id: int
    reason: str

class PaymentSessionCreatedEvent(DomainEvent):
    event_type: str = "PaymentSessionCreated"
    order_id: int
    checkout_url: str
    session_id: str

class PaymentSucceededEvent(DomainEvent):
    event_type: str = "PaymentSucceeded"
    payment_id: str
    order_id: int
    amount: float

class PaymentFailedEvent(DomainEvent):
    event_type: str = "PaymentFailed"
    payment_id: str
    order_id: int
    amount: float
    reason: str

class StoreRegisteredEvent(DomainEvent):
    event_type: str = "StoreRegistered"
    store_id: int
    name: str
    webhook_url: str | None
    is_famous: bool = False

class OrderConfirmedEvent(DomainEvent):
    event_type: str = "OrderConfirmed"
    order_id: int
    store_id: int
    total_price: float
    is_famous: bool = False


