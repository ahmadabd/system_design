import asyncio
import time
import logging
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger("Resilience")

class CircuitBreakerOpenException(Exception):
    """Raised when the circuit breaker is in OPEN state and fast-fails requests"""
    pass

class AsyncCircuitBreaker:
    """Thread-safe, highly observable asynchronous Circuit Breaker"""
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 15.0,
        expected_exceptions: tuple = (Exception,)
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions
        
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.failure_count = 0
        self.last_state_change = 0.0
        self._lock = asyncio.Lock()

    def decorator(self) -> Callable[..., Any]:
        """Returns a decorator that wraps async functions inside the circuit breaker"""
        def deco(func: Callable[..., Any]):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await self.call(func, *args, **kwargs)
            return wrapper
        return deco

    async def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Executes the wrapped function inside the circuit breaker context"""
        await self._before_call()
        
        if self.state == "OPEN":
            raise CircuitBreakerOpenException(
                f"Circuit Breaker '{self.name}' is OPEN. Request immediately fast-failed."
            )
            
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except self.expected_exceptions as e:
            await self._on_failure(e)
            raise e

    async def _before_call(self):
        async with self._lock:
            if self.state == "OPEN":
                # Check if recovery timeout has elapsed to allow probe request (transition to HALF-OPEN)
                elapsed = time.time() - self.last_state_change
                if elapsed >= self.recovery_timeout:
                    self.state = "HALF-OPEN"
                    self.last_state_change = time.time()
                    logger.warning(
                        f"Circuit Breaker '{self.name}' shifted from OPEN to HALF-OPEN. Probing target service..."
                    )

    async def _on_success(self):
        async with self._lock:
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                self.last_state_change = time.time()
                logger.info(
                    f"Circuit Breaker '{self.name}' probe succeeded! Circuit reset to CLOSED."
                )
            elif self.state == "CLOSED":
                self.failure_count = 0

    async def _on_failure(self, exception: Exception):
        async with self._lock:
            self.failure_count += 1
            logger.warning(
                f"Circuit Breaker '{self.name}' detected failure #{self.failure_count} (Exception: {exception})"
            )
            
            if self.state == "HALF-OPEN" or self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                self.last_state_change = time.time()
                logger.error(
                    f"Circuit Breaker '{self.name}' tripped to OPEN state! "
                    f"Blocking requests for the next {self.recovery_timeout} seconds."
                )
