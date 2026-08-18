import asyncio
import time
import pytest
from shared.common.resilience import AsyncCircuitBreaker, CircuitBreakerOpenException

@pytest.mark.asyncio
async def test_circuit_breaker_state_transitions():
    """
    Test the AsyncCircuitBreaker state machine:
    CLOSED (0) -> fails threshold times -> OPEN (1) -> cooling timeout -> HALF-OPEN (2) -> success -> CLOSED (0)
    """
    breaker = AsyncCircuitBreaker(
        name="test-unit-breaker",
        failure_threshold=3,
        recovery_timeout=0.5, # 500ms recovery timeout for fast testing
        expected_exceptions=(RuntimeError,)
    )

    assert breaker.state == "CLOSED"
    assert breaker.failure_count == 0

    async def failing_operation():
        raise RuntimeError("Simulated service outage")

    async def successful_operation():
        return "OK"

    # 1. First 2 failures: Breaker stays CLOSED
    with pytest.raises(RuntimeError):
        await breaker.call(failing_operation)
    assert breaker.state == "CLOSED"
    assert breaker.failure_count == 1

    with pytest.raises(RuntimeError):
        await breaker.call(failing_operation)
    assert breaker.state == "CLOSED"
    assert breaker.failure_count == 2

    # 2. 3rd failure: Reaches threshold -> Trips to OPEN
    with pytest.raises(RuntimeError):
        await breaker.call(failing_operation)
    assert breaker.state == "OPEN"

    # 3. While OPEN: Fast-fails immediately without executing wrapped function
    start = time.perf_counter()
    with pytest.raises(CircuitBreakerOpenException) as exc_info:
        await breaker.call(failing_operation)
    duration = time.perf_counter() - start

    assert "is OPEN" in str(exc_info.value)
    assert duration < 0.01  # Sub-millisecond fail-fast

    # 4. Wait for recovery timeout (500ms)
    await asyncio.sleep(0.55)

    # 5. Next call should probe in HALF-OPEN and on success reset to CLOSED
    result = await breaker.call(successful_operation)
    assert result == "OK"
    assert breaker.state == "CLOSED"
    assert breaker.failure_count == 0


@pytest.mark.asyncio
async def test_resilient_http_client_circuit_breaker(async_client, auth_headers):
    """
    Verify that live microservices expose functioning circuit breakers tracked in Prometheus metrics.
    """
    # Query Prometheus metrics endpoint on product-service
    resp = await async_client.get("/products/metrics")
    assert resp.status_code == 200
    metrics_text = resp.text

    assert "circuit_breaker_state" in metrics_text
    assert 'circuit_breaker_state{name="postgres-database"} 0.0' in metrics_text
    assert 'circuit_breaker_state{name="kafka-message-broker"} 0.0' in metrics_text

    # Query Prometheus metrics endpoint on order-service
    resp_orders = await async_client.get("/orders/metrics")
    assert resp_orders.status_code == 200
    order_metrics = resp_orders.text

    assert "circuit_breaker_state" in order_metrics
    assert 'circuit_breaker_state{name="postgres-database"} 0.0' in order_metrics
    assert 'circuit_breaker_state{name="kafka-message-broker"} 0.0' in order_metrics
