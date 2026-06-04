from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.db_setup import db
from src.infrastructure.config import settings
from src.adapter.repository import SQLAlchemyPaymentRepository
from src.adapter.messaging_pub import PaymentMessagingPublisher
from src.application.payment_service import PaymentApplicationService
from shared.common.messaging import KafkaManager
from shared.common.resilience import CircuitBreakerOpenException
from shared.common.idempotency import IdempotencyManager
from shared.common.cache import cache_fallback
from pydantic import BaseModel

router = APIRouter(prefix="", tags=["Payments"])

# Establish broker manager for outbound events
mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

# Establish Redis Idempotency/Cache Manager
idempotency_manager = IdempotencyManager(settings.REDIS_URL)

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
    publisher = PaymentMessagingPublisher(session)
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
@cache_fallback(idempotency_manager, db.db_breaker, key_prefix="payment", id_param="order_id")
async def get_payment_by_order_id(
    order_id: int,
    request: Request,
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
