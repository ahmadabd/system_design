from pydantic import BaseModel, Field

class CreateProductRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    price: float = Field(..., gt=0.0)
    stock: int = Field(..., ge=0)
