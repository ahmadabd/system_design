from pydantic import BaseModel

class CreateOrderCommand(BaseModel):
    user_id: int
    product_id: int
    quantity: int
    total_price: float
    store_id: int | None = None

class ConfirmOrderCommand(BaseModel):
    order_id: int

class CancelOrderCommand(BaseModel):
    order_id: int
    reason: str
