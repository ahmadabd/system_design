import asyncio
import time
import logging
from functools import wraps
from typing import Callable, Any

try:
    from prometheus_client import Gauge
    # Gauge metric tracking circuit state: 0 = CLOSED, 1 = OPEN, 2 = HALF-OPEN
    circuit_breaker_state_gauge = Gauge(
        "circuit_breaker_state",
        "State of active circuit breakers (0=CLOSED, 1=OPEN, 2=HALF-OPEN)",
        ["name"]
    )
except ImportError:
    circuit_breaker_state_gauge = None

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

        # Initialize metric value to CLOSED
        if circuit_breaker_state_gauge:
            circuit_breaker_state_gauge.labels(name=self.name).set(0)

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
                    if circuit_breaker_state_gauge:
                        circuit_breaker_state_gauge.labels(name=self.name).set(2)
                    logger.warning(
                        f"Circuit Breaker '{self.name}' shifted from OPEN to HALF-OPEN. Probing target service..."
                    )

    async def _on_success(self):
        async with self._lock:
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                self.last_state_change = time.time()
                if circuit_breaker_state_gauge:
                    circuit_breaker_state_gauge.labels(name=self.name).set(0)
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
                if circuit_breaker_state_gauge:
                    circuit_breaker_state_gauge.labels(name=self.name).set(1)
                logger.error(
                    f"Circuit Breaker '{self.name}' tripped to OPEN state! "
                    f"Blocking requests for the next {self.recovery_timeout} seconds."
                )


def is_retriable_exception(e: Exception) -> bool:
    """
    Determines if an exception is retriable for consumer messaging loops.
    - If it is an httpx.HTTPStatusError, we check the status code. Only 5xx status codes are retriable.
    - 4xx status codes (like 401, 403, 404) are NOT retriable.
    - Non-transient errors (validation, programming, data, unique constraint errors) are NOT retriable.
    - General network or DB connection errors/timeouts are transient and retriable.
    """
    try:
        import httpx
        if isinstance(e, httpx.HTTPStatusError):
            status_code = e.response.status_code
            # 5xx errors are retriable, 4xx are not
            return status_code >= 500
        
        # Check if there are nested exceptions or wrapped HTTPStatusErrors
        if hasattr(e, "response") and isinstance(getattr(e, "response"), httpx.Response):
            status_code = e.response.status_code
            return status_code >= 500
    except ImportError:
        pass

    # List of exception class names that indicate programming, validation,
    # or database constraint/data/syntax errors that will never succeed on retry.
    non_retriable_names = {
        # Python built-ins / standard library errors
        "ValidationError", "ValueError", "TypeError", "KeyError", 
        "AttributeError", "NameError", "JSONDecodeError", "IndexError",
        "SyntaxError", "ZeroDivisionError", "ModuleNotFoundError", 
        "ImportError", "AssertionError",
        # Database data, integrity, constraint, and syntax errors
        "DataError", "IntegrityError", "ProgrammingError", "CompileError",
        "InvalidRequestError", "UnsupportedCompilationError",
        "IntegrityConstraintViolationError", "SyntaxOrAccessError",
        "FeatureNotSupportedError", "InvalidTransactionStateError"
    }

    # Helper function to check recursively if the exception or any of its
    # causes/context/original database errors are non-retriable.
    def check_non_retriable(exc: Exception) -> bool:
        for cls in exc.__class__.__mro__:
            if cls.__name__ in non_retriable_names:
                return True
        # Inspect SQLAlchemy wrapped original error
        if hasattr(exc, "orig") and exc.orig is not None and isinstance(exc.orig, Exception):
            return check_non_retriable(exc.orig)
        # Inspect chained exceptions
        if hasattr(exc, "__cause__") and exc.__cause__ is not None:
            return check_non_retriable(exc.__cause__)
        if hasattr(exc, "__context__") and exc.__context__ is not None:
            return check_non_retriable(exc.__context__)
        return False

    if check_non_retriable(e):
        return False

    return True


