import os
import json
import logging
from functools import wraps
from typing import Callable, Any
from fastapi import Request, HTTPException, status
from pydantic import BaseModel
from shared.common.resilience import CircuitBreakerOpenException

logger = logging.getLogger("CacheFallback")


def _serialize(data: Any) -> Any:
    """Helper to convert Pydantic models, dicts, lists, and primitives into JSON-serializable structures"""
    if hasattr(data, "model_dump") and callable(getattr(data, "model_dump")):
        return data.model_dump()
    elif hasattr(data, "dict") and callable(getattr(data, "dict")):
        return data.dict()
    elif isinstance(data, list):
        return [_serialize(item) for item in data]
    elif isinstance(data, dict):
        return {k: _serialize(v) for k, v in data.items()}
    return data


def cache_fallback(
    idempotency_manager,
    db_breaker,
    key_prefix: str,
    id_param: str,
    ttl_seconds: int = 300
):
    """
    FastAPI Route Decorator for Redis-backed Read Fallbacks.
    Attempts to read from Redis cache first.
    On cache miss, checks the database circuit breaker:
      - If DB breaker is OPEN: raises CircuitBreakerOpenException.
      - If DB breaker is CLOSED/HALF-OPEN: queries database, caches response to Redis, and returns.
    """
    def decorator(func: Callable[..., Any]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract request context to check if this decorator was called via an HTTP route
            request: Request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            if not request:
                for k, v in kwargs.items():
                    if isinstance(v, Request):
                        request = v
                        break

            # Find the identity resource ID in path/query parameters
            resource_id = kwargs.get(id_param)
            if resource_id is None:
                logger.warning(
                    f"id_param '{id_param}' not found in route kwargs. Bypassing caching."
                )
                return await func(*args, **kwargs)

            # Redis cache key
            redis_key = f"cache:{key_prefix}:{resource_id}"
            redis_client = idempotency_manager.redis

            # 1. Cache Check
            try:
                cached_val = await redis_client.get(redis_key)
                if cached_val:
                    logger.info(f"Cache HIT for key: '{redis_key}'")
                    return json.loads(cached_val)
            except Exception as cache_err:
                logger.warning(f"Error querying Redis cache for key '{redis_key}': {cache_err}")

            # 2. Database Circuit Breaker Status Check on Cache Miss
            if db_breaker.state == "OPEN":
                # Give the circuit breaker a chance to transition to HALF-OPEN if recovery timeout passed
                await db_breaker._before_call()
                if db_breaker.state == "OPEN":
                    logger.warning(f"Database breaker is OPEN and cache MISS for '{redis_key}'. Read operations degraded.")
                    raise CircuitBreakerOpenException(
                        f"Database is offline and no cached details are available for {key_prefix} ID: {resource_id}."
                    )

            # 3. Query Execution and Write-Through Cache Update
            try:
                result = await func(*args, **kwargs)
                if result is not None:
                    try:
                        serialized_result = _serialize(result)
                        await redis_client.set(redis_key, json.dumps(serialized_result), ex=ttl_seconds)
                        logger.info(f"Updated Redis cache for key: '{redis_key}' (TTL: {ttl_seconds}s)")
                    except Exception as cache_err:
                        logger.warning(f"Failed to write database result to Redis cache: {cache_err}")
                return result
            except Exception as db_err:
                # Propagate exception so that the session cleanup can register failure on the DB breaker
                raise db_err

        return wrapper
    return decorator
