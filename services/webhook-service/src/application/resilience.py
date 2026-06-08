import logging
from shared.common.resilience import AsyncCircuitBreaker

logger = logging.getLogger("WebhookResilience")

class StoreCircuitBreakerRegistry:
    """Manages separate AsyncCircuitBreaker instances mapped per store_id"""
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 15.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.breakers = {}

    def get_breaker(self, store_id: int) -> AsyncCircuitBreaker:
        if store_id not in self.breakers:
            name = f"store-webhook-{store_id}"
            logger.info(f"Creating new circuit breaker for Store {store_id} (threshold: {self.failure_threshold}, timeout: {self.recovery_timeout}s)")
            self.breakers[store_id] = AsyncCircuitBreaker(
                name=name,
                failure_threshold=self.failure_threshold,
                recovery_timeout=self.recovery_timeout
            )
        return self.breakers[store_id]

# Instantiate global circuit breaker registry
breaker_registry = StoreCircuitBreakerRegistry()
