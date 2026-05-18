import time
import logging
import asyncio
from typing import Callable, Any, Optional
from opentelemetry import trace
from prometheus_client import Gauge, Counter

logger = logging.getLogger("circuit_breaker")
tracer = trace.get_tracer("circuit_breaker")

# Prometheus Metrics for Circuit Breaker
CB_STATE_GAUGE = Gauge(
    "circuit_breaker_state",
    "Current state of the circuit breaker (0=CLOSED, 1=HALF-OPEN, 2=OPEN)",
    ["name"]
)
CB_FAILURES_COUNTER = Counter(
    "circuit_breaker_failures_total",
    "Total number of failures captured by the circuit breaker",
    ["name"]
)
CB_CALLS_COUNTER = Counter(
    "circuit_breaker_calls_total",
    "Total number of calls handled by the circuit breaker",
    ["name", "result"] # result: success, failed, fast_failed
)

class CircuitBreakerOpenException(Exception):
    """Exception raised when the circuit breaker is in OPEN state and fast-fails requests."""
    def __init__(self, message: str, fallback_data: Optional[Any] = None):
        super().__init__(message)
        self.fallback_data = fallback_data

class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,       # Trip after 3 consecutive failures
        recovery_timeout: float = 10.0,    # 10 seconds in OPEN state before trying again
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.failure_count = 0
        self.last_state_change = time.time()
        
        # Initialize Prometheus state
        CB_STATE_GAUGE.labels(name=self.name).set(0) # 0 = CLOSED
        
    def _transition_to(self, new_state: str):
        old_state = self.state
        self.state = new_state
        self.last_state_change = time.time()
        
        logger.warning(
            f"[CircuitBreaker-{self.name}] State changed: {old_state} -> {new_state}"
        )
        
        # Update Prometheus Gauge
        state_val = 0 if new_state == "CLOSED" else (1 if new_state == "HALF-OPEN" else 2)
        CB_STATE_GAUGE.labels(name=self.name).set(state_val)
        
        # Record state change as an OTel trace event if there is an active span
        span = trace.get_current_span()
        if span.is_recording():
            span.add_event(
                "circuit_breaker_state_change",
                {
                    "cb.name": self.name,
                    "cb.old_state": old_state,
                    "cb.new_state": new_state,
                }
            )

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        current_time = time.time()
        
        # Check current state and see if recovery timeout has passed
        if self.state == "OPEN":
            if current_time - self.last_state_change > self.recovery_timeout:
                self._transition_to("HALF-OPEN")
            else:
                logger.info(f"[CircuitBreaker-{self.name}] State is OPEN. Fast-failing.")
                CB_CALLS_COUNTER.labels(name=self.name, result="fast_failed").inc()
                raise CircuitBreakerOpenException(
                    f"Circuit Breaker '{self.name}' is OPEN. Fast-failing."
                )

        span = trace.get_current_span()
        if span.is_recording():
            span.set_attribute("cb.name", self.name)
            span.set_attribute("cb.state", self.state)

        try:
            # Execute the call (handles both async and sync functions)
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # If we are in HALF-OPEN and succeed, we reset to CLOSED
            if self.state == "HALF-OPEN":
                logger.info(f"[CircuitBreaker-{self.name}] Call succeeded in HALF-OPEN. Resetting to CLOSED.")
                self.failure_count = 0
                self._transition_to("CLOSED")
            elif self.state == "CLOSED":
                # Reset failure count on success
                self.failure_count = 0
                
            CB_CALLS_COUNTER.labels(name=self.name, result="success").inc()
            return result
            
        except Exception as e:
            # Track failure
            self.failure_count += 1
            CB_FAILURES_COUNTER.labels(name=self.name).inc()
            CB_CALLS_COUNTER.labels(name=self.name, result="failed").inc()
            
            logger.error(
                f"[CircuitBreaker-{self.name}] Caught failure {self.failure_count}/{self.failure_threshold}. Error: {str(e)}"
            )
            
            if span.is_recording():
                span.set_attribute("cb.failure_count", self.failure_count)
                span.record_exception(e)
            
            if self.state == "CLOSED":
                if self.failure_count >= self.failure_threshold:
                    logger.warning(
                        f"[CircuitBreaker-{self.name}] Reached failure threshold ({self.failure_threshold}). Tripping to OPEN."
                    )
                    self._transition_to("OPEN")
            elif self.state == "HALF-OPEN":
                logger.warning(
                    f"[CircuitBreaker-{self.name}] Call failed in HALF-OPEN. Tripping back to OPEN."
                )
                self._transition_to("OPEN")
                
            raise e
