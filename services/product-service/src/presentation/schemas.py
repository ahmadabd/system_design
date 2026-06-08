from pydantic import BaseModel, Field

class CreateProductRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    price: float = Field(..., gt=0.0)
    stock: int = Field(..., ge=0)
    store_id: int = Field(default=1, gt=0)

class CreateStoreRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    webhook_url: str | None = Field(default=None, max_length=255)
