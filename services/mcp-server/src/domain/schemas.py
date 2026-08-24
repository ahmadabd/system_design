from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, EmailStr


class RegisterUserInput(BaseModel):
    """Input payload for registering a new customer account."""
    username: str = Field(..., description="Unique customer username (e.g., 'john_doe')")
    email: EmailStr = Field(..., description="Valid customer email address for confirmations and login")
    password: str = Field(..., min_length=6, description="Account password (minimum 6 characters)")
    tenant_id: str = Field(default="public", description="Store tenant identifier (default: 'public')")


class CreateOrderInput(BaseModel):
    """Input payload for submitting a new order."""
    user_id: int = Field(..., description="Numeric customer ID placing the order")
    product_id: int = Field(..., description="Numeric product ID to purchase")
    quantity: int = Field(..., gt=0, description="Quantity of items to purchase (must be > 0)")
    total_price: float = Field(..., gt=0, description="Expected total order price")
    store_id: int = Field(default=1, description="Store ID fulfilling the purchase")
    payment_method: str = Field(
        default="CREDIT_CARD",
        description="Payment method: 'CREDIT_CARD', 'WALLET', or 'CASH_ON_DELIVERY'"
    )
    tenant_id: str = Field(default="store_tech", description="Store tenant identifier (e.g. 'store_tech')")
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Optional unique idempotency key. If omitted, a unique key is generated automatically."
    )


class CancelOrderInput(BaseModel):
    """Input payload for requesting order cancellation."""
    order_id: int = Field(..., description="Numeric ID of the order to cancel")
    reason: str = Field(
        default="Cancelled by customer via AI Agent",
        description="Reason for cancellation (will be recorded in Saga audit log)"
    )
    tenant_id: str = Field(default="store_tech", description="Store tenant identifier")


class ProductSearchInput(BaseModel):
    """Input parameters for searching catalog products."""
    query: Optional[str] = Field(default=None, description="Keywords to match against product title or description")
    category_id: Optional[int] = Field(default=None, description="Optional category filter ID")
    max_price: Optional[float] = Field(default=None, description="Maximum price filter")
    tenant_id: str = Field(default="store_tech", description="Store tenant identifier")


class ToolResult(BaseModel):
    """Standardized response envelope returned to AI agents."""
    success: bool
    status: str
    data: Optional[Dict[str, Any]] = None
    message: Optional[str] = None
    idempotency_key: Optional[str] = None
