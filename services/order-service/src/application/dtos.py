from pydantic import BaseModel

class OrderDTO(BaseModel):
    id: int
    user_id: int
    product_id: int
    quantity: int
    total_price: float
    status: str
    store_id: int

    class Config:
        from_attributes = True
