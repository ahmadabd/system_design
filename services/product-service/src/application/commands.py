from pydantic import BaseModel

class CreateProductCommand(BaseModel):
    name: str
    price: float
    stock: int

class ReserveInventoryCommand(BaseModel):
    order_id: int
    product_id: int
    quantity: int
