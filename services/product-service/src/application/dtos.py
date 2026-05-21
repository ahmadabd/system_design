from pydantic import BaseModel

class ProductDTO(BaseModel):
    id: int
    name: str
    price: float
    stock: int

    class Config:
        from_attributes = True
