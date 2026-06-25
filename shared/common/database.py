import os
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
    AsyncEngine
)
from sqlalchemy import text, event
import time
from shared.common.resilience import AsyncCircuitBreaker, CircuitBreakerOpenException
from fastapi import Request

try:
    from prometheus_client import Histogram, Gauge
    db_query_duration = Histogram(
        "db_query_duration_seconds",
        "Time spent executing DB queries",
        ["db_name"]
    )
    postgresql_connections = Gauge(
        "postgresql_connections",
        "Number of active PostgreSQL connections in the pool",
        ["db", "state"]
    )
    postgresql_connections_max = Gauge(
        "postgresql_connections_max",
        "Maximum size of the PostgreSQL connection pool",
        ["db"]
    )
except ImportError:
    db_query_duration = None
    postgresql_connections = None
    postgresql_connections_max = None

# Shared declarative base for all ORM models across services
from sqlalchemy.orm import declarative_base
Base = declarative_base()

class Database:
    """Async Database Session Manager with Circuit Breaker resilience"""
    def __init__(self, db_url: str):
        pool_size = int(os.getenv("DB_POOL_SIZE", "25"))
        max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "25"))
        self._engine: AsyncEngine = create_async_engine(
            db_url,
            pool_pre_ping=True,
            echo=False,
            pool_size=pool_size,
            max_overflow=max_overflow
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

        # Register transparent query execution listeners to record query latencies
        if db_query_duration:
            @event.listens_for(self._engine.sync_engine, "before_cursor_execute")
            def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                context._query_start_time = time.perf_counter()

            @event.listens_for(self._engine.sync_engine, "after_cursor_execute")
            def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
                if hasattr(context, "_query_start_time"):
                    total_time = time.perf_counter() - context._query_start_time
                    db_name = conn.engine.url.database or "unknown"
                    db_query_duration.labels(db_name=db_name).observe(total_time)

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

    def run_migrations(self, alembic_ini_path: str = "alembic.ini") -> None:
        """Run database migrations programmatically using Alembic"""
        from alembic.config import Config
        from alembic import command
        import logging

        alembic_logger = logging.getLogger("alembic")
        alembic_logger.setLevel(logging.INFO)

        config = Config(alembic_ini_path)
        # Force database URL from engine connection instance
        db_url = self._engine.url.render_as_string(hide_password=False)
        config.set_main_option("sqlalchemy.url", db_url)
        
        command.upgrade(config, "head")

    async def get_session(self, request: Request = None) -> AsyncGenerator[AsyncSession, None]:
        """Dependency generator to retrieve DB sessions with automatic cleanup and circuit breaker wrapping"""
        is_write = True
        if request and request.method == "GET":
            is_write = False

        # Fast-fail if the circuit breaker is OPEN (only for write operations)
        if is_write and self.db_breaker.state == "OPEN":
            await self.db_breaker._before_call()
            if self.db_breaker.state == "OPEN":
                raise CircuitBreakerOpenException(
                    "Database circuit breaker is OPEN. Fast-failing database transaction request."
                )

        # Update connection pool metrics before session starts
        db_name = self._engine.url.database or "unknown"
        if postgresql_connections:
            # Active (checked out) connections
            postgresql_connections.labels(db=db_name, state="active").set(self._engine.pool.checkedout())
            # Idle (checked in) connections
            postgresql_connections.labels(db=db_name, state="idle").set(self._engine.pool.checkedin())
        if postgresql_connections_max:
            # Max pool size
            postgresql_connections_max.labels(db=db_name).set(self._engine.pool.size())

        async with self._session_maker() as session:
            try:
                yield session
                await session.commit()
                # If transaction successfully committed, notify the circuit breaker of a success
                await self.db_breaker._on_success()
            except Exception as e:
                await session.rollback()
                # Only register database-level driver/connection failures as circuit breaker failures
                from sqlalchemy.exc import DBAPIError
                if isinstance(e, DBAPIError) or isinstance(e, (OSError, ConnectionError)):
                    await self.db_breaker._on_failure(e)
                raise e
            finally:
                await session.close()


from sqlalchemy import Column, Integer, String, JSON, DateTime, Boolean
from datetime import datetime

class OutboxMessage(Base):
    """ORM representation of a message to be published to Kafka resiliently"""
    __tablename__ = "outbox_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String(255), nullable=False)
    payload = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False)



