from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.db_setup import db
from src.infrastructure.config import settings
from src.adapter.repository import SQLAlchemyProductRepository, SQLAlchemyStoreRepository
from src.adapter.messaging_pub import ProductMessagingPublisher
from src.application.product_service import ProductApplicationService
from src.application.commands import CreateProductCommand, CreateStoreCommand
from src.application.dtos import ProductDTO, StoreDTO
from src.presentation.schemas import CreateProductRequest, CreateStoreRequest
from shared.common.messaging import KafkaManager
from shared.common.idempotency import IdempotencyManager, idempotent_api
from shared.common.resilience import CircuitBreakerOpenException
from shared.common.cache import cache_fallback

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
    store_repo = SQLAlchemyStoreRepository(session)
    publisher = ProductMessagingPublisher(session)
    return ProductApplicationService(repo, publisher, store_repo=store_repo)

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
            stock=request_data.stock,
            store_id=request_data.store_id
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
@cache_fallback(idempotency_manager, db.db_breaker, key_prefix="product", id_param="product_id")
async def get_product(
    product_id: int,
    request: Request,
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

@router.post("/stores", response_model=StoreDTO, status_code=status.HTTP_201_CREATED)
async def create_store(
    request_data: CreateStoreRequest,
    service: ProductApplicationService = Depends(get_product_service)
):
    """REST endpoint to register a new store"""
    try:
        command = CreateStoreCommand(
            name=request_data.name,
            webhook_url=request_data.webhook_url,
            is_famous=request_data.is_famous
        )
        return await service.create_store(command)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Circuit breaker active: {str(cb_err)}. Downstream services are degraded."
        )

@router.get("/stores", response_model=list[StoreDTO])
async def list_stores(
    service: ProductApplicationService = Depends(get_product_service)
):
    """REST endpoint to retrieve all stores"""
    try:
        return await service.get_all_stores()
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}. Read operations degraded."
        )

@router.get("/stores/{store_id:int}", response_model=StoreDTO)
@cache_fallback(idempotency_manager, db.db_breaker, key_prefix="store", id_param="store_id")
async def get_store(
    store_id: int,
    request: Request,
    service: ProductApplicationService = Depends(get_product_service)
):
    """REST endpoint to fetch store details by identity ID"""
    try:
        store = await service.get_store_by_id(store_id)
        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Store not found"
            )
        return store
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}."
        )
