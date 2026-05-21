from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
    AsyncEngine
)
from sqlalchemy.orm import declarative_base
from shared.common.resilience import AsyncCircuitBreaker, CircuitBreakerOpenException

# Shared declarative base for all ORM models across services
Base = declarative_base()

class Database:
    """Async Database Session Manager with Circuit Breaker resilience"""
    def __init__(self, db_url: str):
        self._engine: AsyncEngine = create_async_engine(
            db_url,
            pool_pre_ping=True,
            echo=False,
            pool_size=10,
            max_overflow=20
        )
        self._session_maker = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False
        )
        # Unique database circuit breaker per microservice instance
        self.db_breaker = AsyncCircuitBreaker(
            name="postgres-database",
            failure_threshold=5,
            recovery_timeout=15.0
        )

    async def close(self) -> None:
        """Safely dispose of connection pools"""
        await self._engine.dispose()

    async def initialize_schema(self, base_metadata, logger=None) -> None:
        """Resiliently connect to the database and initialize the schema tables with retries"""
        import asyncio
        # Safely extract metadata whether Base or Base.metadata is passed
        metadata = getattr(base_metadata, "metadata", base_metadata)
        
        retries = 20
        delay = 2
        for attempt in range(1, retries + 1):
            try:
                async with self._engine.begin() as conn:
                    await conn.run_sync(metadata.create_all)
                if logger:
                    logger.info("Database schema initialized successfully!")
                return
            except Exception as e:
                if attempt == retries:
                    if logger:
                        logger.error(f"Failed to connect to database and initialize schema after {retries} attempts.")
                    raise e
                if logger:
                    logger.warning(
                        f"Database is not ready yet (Attempt {attempt}/{retries}). "
                        f"Retrying in {delay} seconds... Error: {e}"
                    )
                await asyncio.sleep(delay)

    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Dependency generator to retrieve DB sessions with automatic cleanup and circuit breaker wrapping"""
        # Fast-fail if the circuit breaker is OPEN
        if self.db_breaker.state == "OPEN":
            await self.db_breaker._before_call()
            if self.db_breaker.state == "OPEN":
                raise CircuitBreakerOpenException(
                    "Database circuit breaker is OPEN. Fast-failing database transaction request."
                )

        async with self._session_maker() as session:
            try:
                yield session
                await session.commit()
                # If transaction successfully committed, notify the circuit breaker of a success
                await self.db_breaker._on_success()
            except Exception as e:
                await session.rollback()
                # If a database error occurs, register it as a failure
                await self.db_breaker._on_failure(e)
                raise e
            finally:
                await session.close()

