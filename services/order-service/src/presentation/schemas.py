from pydantic import BaseModel, Field

class CreateOrderRequest(BaseModel):
    user_id: int = Field(..., gt=0)
    product_id: int = Field(..., gt=0)
    quantity: int = Field(..., gt=0)
    total_price: float = Field(..., gt=0.0)
    store_id: int | None = Field(default=None, gt=0)
    payment_method: str = Field(default="AUTOMATIC")
