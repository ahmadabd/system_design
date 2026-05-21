from pydantic import BaseModel

class CreateOrderCommand(BaseModel):
    user_id: int
    product_id: int
    quantity: int
    total_price: float

class ConfirmOrderCommand(BaseModel):
    order_id: int

class CancelOrderCommand(BaseModel):
    order_id: int
    reason: str
