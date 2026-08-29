from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.db_setup import db
from src.infrastructure.config import settings
from src.adapter.repository import SQLAlchemyUserRepository
from src.adapter.messaging_pub import UserMessagingPublisher
from src.application.user_service import UserApplicationService
from src.application.commands import RegisterUserCommand
from src.application.dtos import UserDTO
from src.presentation.schemas import RegisterUserRequest
from shared.common.messaging import KafkaManager
from shared.common.idempotency import IdempotencyManager, idempotent_api
from shared.common.resilience import CircuitBreakerOpenException
from shared.common.cache import cache_fallback
from shared.common.bloom import bloom_guard
from algorithms.bloom_filter import BloomFilter

router = APIRouter(prefix="", tags=["Users"])

# Establish Redis Idempotency Manager
idempotency_manager = IdempotencyManager(settings.REDIS_URL)

# Establish Bloom Filters
# 1. User ID Bloom Filter for Cache Penetration Defense
user_id_bloom_filter = BloomFilter(expected_elements=100000, false_positive_rate=0.01)

# 2. User Identity Bloom Filter for Email and Username Uniqueness Pre-checks
user_identity_bloom_filter = BloomFilter(expected_elements=200000, false_positive_rate=0.01)

# Instantiate background Kafka connection specifically for API publications
mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

async def get_user_service(
    session: AsyncSession = Depends(db.get_session)
) -> UserApplicationService:
    """Dependency injector mapping persistence adapters to the core use case layer"""
    repo = SQLAlchemyUserRepository(session)
    publisher = UserMessagingPublisher(session)
    return UserApplicationService(repo, publisher)

@router.post("/", response_model=UserDTO, status_code=status.HTTP_201_CREATED)
@idempotent_api(idempotency_manager)
async def register(
    request_data: RegisterUserRequest,
    request: Request,  # Injected by FastAPI so the @idempotent_api decorator can inspect headers
    service: UserApplicationService = Depends(get_user_service)
):
    """REST endpoint to register a new user account"""
    try:
        command = RegisterUserCommand(
            username=request_data.username,
            email=request_data.email,
            password=request_data.password
        )
        saved = await service.register_user(command)
        
        # Register in Bloom Filters
        user_id_bloom_filter.add(str(saved.id))
        user_identity_bloom_filter.add(f"email:{saved.email.lower()}")
        user_identity_bloom_filter.add(f"username:{saved.username.lower()}")
        
        return saved
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

@router.get("/{user_id:int}", response_model=UserDTO)
@bloom_guard(bloom_filter=user_id_bloom_filter, id_param="user_id", filter_name="user_id_bloom", not_found_message="User not found (Bloom Guard: fast rejection)")
@cache_fallback(idempotency_manager, db.db_breaker, key_prefix="user", id_param="user_id")
async def get_user(
    user_id: int,
    request: Request,
    service: UserApplicationService = Depends(get_user_service)
):
    """REST endpoint to fetch user details by identity ID"""
    try:
        user = await service.get_user_by_id(user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        return user
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}."
        )
