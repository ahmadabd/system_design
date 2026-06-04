from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.db_setup import db
from src.infrastructure.config import settings
from src.adapter.repository import SQLAlchemyProductRepository
from src.adapter.messaging_pub import ProductMessagingPublisher
from src.application.product_service import ProductApplicationService
from src.application.commands import CreateProductCommand
from src.application.dtos import ProductDTO
from src.presentation.schemas import CreateProductRequest
from shared.common.messaging import KafkaManager
from shared.common.idempotency import IdempotencyManager, idempotent_api
from shared.common.resilience import CircuitBreakerOpenException

router = APIRouter(prefix="", tags=["Products"])

# Establish Redis Idempotency Manager
idempotency_manager = IdempotencyManager(settings.REDIS_URL)

# Establish independent broker manager for product API calls
mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

async def get_product_service(
    session: AsyncSession = Depends(db.get_session)
) -> ProductApplicationService:
    """Dependency injector mapping persistence adapters to core use cases"""
    repo = SQLAlchemyProductRepository(session)
    publisher = ProductMessagingPublisher(session)
    return ProductApplicationService(repo, publisher)

@router.post("/", response_model=ProductDTO, status_code=status.HTTP_201_CREATED)
@idempotent_api(idempotency_manager)
async def create_product(
    request_data: CreateProductRequest,
    request: Request,  # Injected by FastAPI so the @idempotent_api decorator can inspect headers
    service: ProductApplicationService = Depends(get_product_service)
):
    """REST endpoint to register a new product in the catalog"""
    try:
        command = CreateProductCommand(
            name=request_data.name,
            price=request_data.price,
            stock=request_data.stock
        )
        return await service.create_product(command)
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

@router.get("/", response_model=list[ProductDTO])
async def list_products(
    service: ProductApplicationService = Depends(get_product_service)
):
    """REST endpoint to retrieve all products in the catalog"""
    try:
        return await service.get_all_products()
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}. Read operations degraded."
        )

@router.get("/{product_id:int}", response_model=ProductDTO)
async def get_product(
    product_id: int,
    service: ProductApplicationService = Depends(get_product_service)
):
    """REST endpoint to fetch product details by identity ID"""
    try:
        product = await service.get_product_by_id(product_id)
        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )
        return product
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}."
        )
