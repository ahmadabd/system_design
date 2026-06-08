from pydantic import BaseModel

class CreateProductCommand(BaseModel):
    name: str
    price: float
    stock: int
    store_id: int

class ReserveInventoryCommand(BaseModel):
    order_id: int
    product_id: int
    quantity: int

class CreateStoreCommand(BaseModel):
    name: str
    webhook_url: str | None = None
