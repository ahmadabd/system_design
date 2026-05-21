import os
import json
import logging
from functools import wraps
from typing import Callable, Any
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger("Idempotency")

class IdempotencyManager:
    """Redis-backed API Idempotency Key Manager"""
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

    async def check_and_lock(self, key: str, lock_ttl: int = 120) -> tuple[bool, dict | None]:
        """
        Checks the status of the idempotency key.
        Returns:
            (is_new, cached_response_dict)
            If cached_response_dict is None and is_new is False, it means the request is currently PROCESSING.
        """
        # Atomically set key to "PROCESSING" if it doesn't exist
        is_set = await self.redis.set(key, "PROCESSING", ex=lock_ttl, nx=True)
        if is_set:
            return True, None

        # Key already exists. Check its value.
        val = await self.redis.get(key)
        if val == "PROCESSING":
            return False, None
        
        try:
            cached_data = json.loads(val)
            return False, cached_data
        except Exception as e:
            logger.error(f"Error parsing cached idempotency response: {e}")
            return False, None

    async def save_response(self, key: str, status_code: int, body: Any, ttl: int = 86400) -> None:
        """Saves completed request status and response payload with 24-hour TTL"""
        payload = {
            "status_code": status_code,
            "body": body
        }
        await self.redis.set(key, json.dumps(payload), ex=ttl)

    async def unlock(self, key: str) -> None:
        """Deletes key to allow retry if an operation failed during execution"""
        await self.redis.delete(key)

    async def close(self) -> None:
        """Safely close Redis connection pool"""
        await self.redis.close()


def _serialize(data: Any) -> Any:
    """Helper to convert Pydantic models, dicts, lists, and primitives into JSON-serializable structures"""
    if isinstance(data, BaseModel):
        return data.model_dump()
    elif isinstance(data, list):
        return [_serialize(item) for item in data]
    elif isinstance(data, dict):
        return {k: _serialize(v) for k, v in data.items()}
    return data


def idempotent_api(manager_instance: IdempotencyManager, expire_seconds: int = 86400):
    """
    FastAPI Route Decorator to enforce X-Idempotency-Key headers on mutating actions.
    Uses Redis to lock, cache, and immediately return duplicates.
    """
    def decorator(func: Callable[..., Any]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Find the HTTP Request object in parameters to access headers
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

            if not request:
                # If no request object in parameters, raise error as idempotency cannot be evaluated
                logger.error(f"Request object not found in arguments for route: {func.__name__}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Idempotency check failed: Request context not available."
                )

            idempotency_key = request.headers.get("X-Idempotency-Key")
            if not idempotency_key:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Missing required X-Idempotency-Key header."
                )

            # Route prefix + key to prevent clashes across microservices in shared Redis
            service_prefix = os.getenv("SERVICE_NAME", "default")
            redis_key = f"idem:{service_prefix}:{idempotency_key}"

            is_new, cached_response = await manager_instance.check_and_lock(redis_key)
            
            if not is_new:
                if cached_response is None:
                    # Request is in progress
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="A request with this idempotency key is already in progress."
                    )
                # Request is completed, return the cached result
                logger.info(f"Idempotency hit! Returning cached response for key: {redis_key}")
                return JSONResponse(
                    content=cached_response["body"],
                    status_code=cached_response["status_code"]
                )

            # It's a new request. Execute the actual function.
            try:
                result = await func(*args, **kwargs)
                
                # Deduce status code: default to 201 for POST mutating, else 200
                status_code = status.HTTP_201_CREATED if request.method == "POST" else status.HTTP_200_OK
                
                # If the function returns a Response object or tuple, serialize it
                serialized_body = _serialize(result)
                
                await manager_instance.save_response(redis_key, status_code, serialized_body, expire_seconds)
                return result
            except HTTPException as http_ex:
                # API error, unlock key so client can retry
                await manager_instance.unlock(redis_key)
                raise http_ex
            except Exception as e:
                # System/DB error, unlock key so client can retry
                await manager_instance.unlock(redis_key)
                logger.error(f"Exception during idempotent call execution: {e}", exc_info=True)
                raise e

        return wrapper
    return decorator


async def check_and_register_event(session: AsyncSession, event_id: str) -> bool:
    """
    SQL-Backed Inbox Pattern Message Deduplication.
    Checks if event_id is registered. If registered, returns True (duplicate).
    If new, registers event_id atomically and returns False.
    """
    try:
        # Check if already processed
        result = await session.execute(
            text("SELECT 1 FROM idempotent_consumers WHERE message_id = :message_id FOR UPDATE"),
            {"message_id": event_id}
        )
        if result.scalar() is not None:
            return True
            
        # Register the new message
        await session.execute(
            text("INSERT INTO idempotent_consumers (message_id) VALUES (:message_id)"),
            {"message_id": event_id}
        )
        return False
    except Exception as e:
        # If any db unique constraint violation occurs, it means another thread just registered it
        logger.warning(f"Database insertion failed for event_id {event_id}, treating as duplicate: {e}")
        return True
