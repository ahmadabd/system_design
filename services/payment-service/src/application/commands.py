from pydantic import BaseModel

class ProcessPaymentCommand(BaseModel):
    order_id: int
    event_id: str
