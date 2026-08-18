import os
from contextlib import asynccontextmanager
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



    def run_migrations(self, alembic_ini_path: str = "alembic.ini") -> None:
        """Run database migrations programmatically using Alembic with sync psycopg2 driver"""
        from alembic.config import Config
        from alembic import command
        import logging

        alembic_logger = logging.getLogger("alembic")
        alembic_logger.setLevel(logging.INFO)

        config = Config(alembic_ini_path)
        # Force database URL from engine connection instance, converting to sync psycopg2 driver
        db_url = self._engine.url.render_as_string(hide_password=False)
        db_url = db_url.replace("+asyncpg", "+psycopg2")
        config.set_main_option("sqlalchemy.url", db_url)
        
        command.upgrade(config, "head")

    async def run_migrations_for_schema(self, schema: str, alembic_ini_path: str = "alembic.ini") -> None:
        """Run Alembic upgrade head scoped to a specific PostgreSQL schema."""
        import asyncio
        await asyncio.to_thread(self._run_migrations_sync, schema, alembic_ini_path)

    def _run_migrations_sync(self, schema: str, alembic_ini_path: str = "alembic.ini") -> None:
        from alembic.config import Config
        from alembic import command
        config = Config(alembic_ini_path)
        db_url = self._engine.url.render_as_string(hide_password=False)
        # asyncpg driver is async-only; Alembic needs sync psycopg2
        db_url = db_url.replace("+asyncpg", "+psycopg2")
        config.set_main_option("sqlalchemy.url", db_url)
        config.set_main_option("target_schema", schema)
        command.upgrade(config, "head")

    @asynccontextmanager
    async def session_scope(self, request: Request = None, tenant_slug: str = None) -> AsyncGenerator[AsyncSession, None]:
        """Asynchronous context manager for background workers, tasks, and scripts."""
        from shared.common.tenant import set_tenant, TenantContext
        if tenant_slug:
            set_tenant(TenantContext(slug=tenant_slug))
        else:
            set_tenant(None)
        async for session in self.get_session(request=request):
            yield session

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
            from shared.common.tenant import get_tenant_or_none, set_tenant, TenantContext
            tenant = get_tenant_or_none()

            # BaseHTTPMiddleware loses ContextVars across task boundary — re-hydrate from request.state
            if tenant is None and request is not None:
                slug = getattr(request.state, "tenant_slug", None)
                if slug:
                    set_tenant(TenantContext(slug=slug))
                    tenant = TenantContext(slug=slug)

            if tenant and tenant.slug:
                await session.execute(
                    text(f"SET search_path TO {tenant.slug}, public")
                )
            else:
                await session.execute(
                    text("SET search_path TO public")
                )

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
                try:
                    await session.execute(text("SET search_path TO public"))
                    await session.commit()
                except Exception:
                    pass
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

class IdempotentConsumerDB(Base):
    """SQL-backed Inbox Pattern message deduplication table"""
    __tablename__ = "idempotent_consumers"

    message_id = Column(String(255), primary_key=True)
    processed_at = Column(DateTime, default=datetime.utcnow)



