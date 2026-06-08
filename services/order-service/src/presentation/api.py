from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.db_setup import db
from src.infrastructure.config import settings
from src.adapter.repository import SQLAlchemyOrderRepository
from src.adapter.messaging_pub import OrderMessagingPublisher
from src.adapter.service_clients import HTTPUserClient, HTTPProductClient
from src.application.order_service import OrderApplicationService
from src.application.commands import CreateOrderCommand
from src.application.dtos import OrderDTO
from src.presentation.schemas import CreateOrderRequest
from shared.common.messaging import KafkaManager
from shared.common.idempotency import IdempotencyManager, idempotent_api
from shared.common.resilience import CircuitBreakerOpenException
from shared.common.http_client import ResilientHTTPClient
from shared.common.cache import cache_fallback

router = APIRouter(prefix="", tags=["Orders"])

# Establish Redis Idempotency Manager
idempotency_manager = IdempotencyManager(settings.REDIS_URL)

# Establish broker manager for API checkout triggers
mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

# Establish resilient HTTP client and downstream service client adapters
http_client = ResilientHTTPClient(timeout=5.0)
user_client = HTTPUserClient(http_client, settings.USER_SERVICE_URL, redis_url=settings.REDIS_URL)
product_client = HTTPProductClient(http_client, settings.PRODUCT_SERVICE_URL, redis_url=settings.REDIS_URL)

async def get_order_service(
    session: AsyncSession = Depends(db.get_session)
) -> OrderApplicationService:
    """Dependency injector mapping persistence adapters to core use cases"""
    repo = SQLAlchemyOrderRepository(session)
    publisher = OrderMessagingPublisher(session)
    return OrderApplicationService(repo, publisher, user_client=user_client, product_client=product_client)

@router.post("/", response_model=OrderDTO, status_code=status.HTTP_201_CREATED)
@idempotent_api(idempotency_manager)
async def create_order(
    request_data: CreateOrderRequest,
    request: Request,  # Injected by FastAPI so the @idempotent_api decorator can inspect headers
    service: OrderApplicationService = Depends(get_order_service)
):
    """REST endpoint to submit a checkout order in a PENDING state"""
    try:
        command = CreateOrderCommand(
            user_id=request_data.user_id,
            product_id=request_data.product_id,
            quantity=request_data.quantity,
            total_price=request_data.total_price,
            store_id=request_data.store_id
        )
        return await service.create_order(command)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Circuit breaker active: {str(cb_err)}. Downstream services are temporarily degraded."
        )

@router.get("/", response_model=list[OrderDTO])
async def list_orders(
    service: OrderApplicationService = Depends(get_order_service)
):
    """REST endpoint to retrieve all platform orders"""
    try:
        return await service.get_all_orders()
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}. Read operations degraded."
        )

@router.get("/{order_id:int}", response_model=OrderDTO)
@cache_fallback(idempotency_manager, db.db_breaker, key_prefix="order", id_param="order_id")
async def get_order(
    order_id: int,
    request: Request,
    service: OrderApplicationService = Depends(get_order_service)
):
    """REST endpoint to fetch order details by identity ID"""
    try:
        order = await service.get_order_by_id(order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Order not found"
            )
        return order
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}."
        )

@router.get("/{order_id:int}/status-stream")
async def order_status_stream(
    order_id: int,
    service: OrderApplicationService = Depends(get_order_service)
):
    """
    Server-Sent Events (SSE) endpoint to stream real-time order status transitions.
    Can be tested with: curl -N http://localhost/orders/1/status-stream
    """
    from fastapi.responses import StreamingResponse
    import asyncio

    async def event_generator():
        last_status = None
        # Max check: 60 seconds (120 iterations * 0.5s) to prevent infinite connection hanging
        for _ in range(120):
            try:
                order = await service.get_order_by_id(order_id)
                if not order:
                    yield f"data: {{\"error\": \"Order not found\"}}\n\n"
                    break
                
                if order.status != last_status:
                    last_status = order.status
                    yield f"data: {{\"order_id\": {order_id}, \"status\": \"{order.status}\"}}\n\n"
                    
                if order.status in ["CONFIRMED", "CANCELLED"]:
                    break
            except Exception as e:
                yield f"data: {{\"error\": \"{str(e)}\"}}\n\n"
                break
            await asyncio.sleep(0.5)
            
    return StreamingResponse(event_generator(), media_type="text/event-stream")

