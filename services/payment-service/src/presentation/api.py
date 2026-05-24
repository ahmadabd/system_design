from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.db_setup import db
from src.infrastructure.config import settings
from src.adapter.repository import SQLAlchemyPaymentRepository
from src.adapter.messaging_pub import PaymentMessagingPublisher
from src.application.payment_service import PaymentApplicationService
from shared.common.messaging import KafkaManager
from shared.common.resilience import CircuitBreakerOpenException
from pydantic import BaseModel

router = APIRouter(prefix="", tags=["Payments"])

# Establish broker manager for outbound events
mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

class PaymentDTO(BaseModel):
    id: str | None
    order_id: int
    amount: float
    status: str

    class Config:
        from_attributes = True

async def get_payment_service(
    session: AsyncSession = Depends(db.get_session)
) -> PaymentApplicationService:
    """Dependency injector mapping persistence adapters to core use cases"""
    repo = SQLAlchemyPaymentRepository(session)
    
    # Connect to Kafka if not open
    if not mq_manager.producer:
        await mq_manager.connect()
        
    publisher = PaymentMessagingPublisher(mq_manager)
    return PaymentApplicationService(repo, publisher)

@router.get("/", response_model=list[PaymentDTO])
async def list_payments(
    service: PaymentApplicationService = Depends(get_payment_service)
):
    """REST endpoint to retrieve all platform payments"""
    try:
        payments = await service.get_all_payments()
        return payments
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}. Read operations degraded."
        )

@router.get("/{order_id:int}", response_model=PaymentDTO)
async def get_payment_by_order_id(
    order_id: int,
    service: PaymentApplicationService = Depends(get_payment_service)
):
    """REST endpoint to fetch payment details by order ID reference"""
    try:
        payment = await service.get_payment_by_order_id(order_id)
        if not payment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Payment record for Order {order_id} not found"
            )
        return payment
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}."
        )
