from pydantic import BaseModel

class ProductDTO(BaseModel):
    id: int
    name: str
    price: float
    stock: int
    store_id: int

    class Config:
        from_attributes = True

class StoreDTO(BaseModel):
    id: int
    name: str
    webhook_url: str | None = None

    class Config:
        from_attributes = True
