import logging
from urllib.parse import urlparse
import httpx
from typing import Any, Dict
from shared.common.resilience import AsyncCircuitBreaker

logger = logging.getLogger("ResilientHTTPClient")

class ResilientHTTPClient:
    """
    Asynchronous HTTP Client with dynamic, host-specific circuit breakers
    for resilient service-to-service REST communication.
    """
    def __init__(self, timeout: float = 5.0):
        self.client = httpx.AsyncClient(timeout=timeout, trust_env=False)
        self.breakers: Dict[str, AsyncCircuitBreaker] = {}
        self.default_timeout = timeout

    def _get_breaker(self, host: str) -> AsyncCircuitBreaker:
        """Retrieves or creates a dedicated circuit breaker for the given host"""
        if host not in self.breakers:
            logger.info(f"Registering new service-to-service circuit breaker for host: {host}")
            self.breakers[host] = AsyncCircuitBreaker(
                name=f"http-service-{host}",
                failure_threshold=5,
                recovery_timeout=15.0,
                expected_exceptions=(
                    httpx.HTTPError,
                    httpx.TimeoutException,
                    httpx.ConnectError
                )
            )
        return self.breakers[host]

    async def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Executes HTTP request wrapped within the host's circuit breaker"""
        parsed = urlparse(url)
        host = parsed.netloc or "local-api"
        breaker = self._get_breaker(host)

        async def _execute_call() -> httpx.Response:
            from shared.common.tenant import get_tenant_or_none
            
            kwargs.setdefault("headers", {})

            # Auto-inject tenant context header for downstream service
            tenant_ctx = get_tenant_or_none()
            if tenant_ctx:
                kwargs["headers"]["X-Tenant-ID"] = tenant_ctx.slug

            # Propagate OpenTelemetry distributed trace context
            try:
                from opentelemetry import trace
                from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
                
                TraceContextTextMapPropagator().inject(kwargs["headers"])
                tracer = trace.get_tracer("http-client")
                
                with tracer.start_as_current_span(
                    name=f"HTTP {method.upper()} {parsed.path or '/'}",
                    kind=trace.SpanKind.CLIENT
                ) as span:
                    span.set_attribute("http.method", method.upper())
                    span.set_attribute("http.url", url)
                    span.set_attribute("http.host", host)
                    if tenant_ctx:
                        span.set_attribute("tenant.id", tenant_ctx.slug)

                    # Re-inject inside active client span so downstream sees this span as parent
                    TraceContextTextMapPropagator().inject(kwargs["headers"])

                    response = await self.client.request(method, url, **kwargs)
                    span.set_attribute("http.status_code", response.status_code)
                    
                    # Treat HTTP 5xx Server Errors as failures to trip the circuit
                    if response.status_code >= 500:
                        logger.error(
                            f"ResilientHTTPClient: Downstream host '{host}' returned HTTP {response.status_code}"
                        )
                        response.raise_for_status()
                    return response
            except ImportError:
                response = await self.client.request(method, url, **kwargs)
                if response.status_code >= 500:
                    logger.error(
                        f"ResilientHTTPClient: Downstream host '{host}' returned HTTP {response.status_code}"
                    )
                    response.raise_for_status()
                return response

        return await breaker.call(_execute_call)

    async def get(self, url: str, **kwargs) -> httpx.Response:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs) -> httpx.Response:
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs) -> httpx.Response:
        return await self.request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs) -> httpx.Response:
        return await self.request("DELETE", url, **kwargs)

    async def close(self) -> None:
        """Safely close active HTTPX client session"""
        await self.client.aclose()
        logger.info("Closed ResilientHTTPClient sessions.")
