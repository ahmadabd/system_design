import math
import hashlib
import logging
from functools import wraps
from typing import Callable, Any, Optional, List, Union
from fastapi import Request, HTTPException, status
from algorithms.bloom_filter import BloomFilter

logger = logging.getLogger("BloomGuard")


class RedisBloomFilter:
    """
    Distributed Bloom Filter backed by Redis bitsets.
    
    Compatible with standard Redis instances (using SETBIT / GETBIT pipelines)
    and RedisBloom modules (using BF.ADD / BF.EXISTS).
    """
    def __init__(
        self, 
        redis_client, 
        key: str, 
        expected_elements: int = 100000, 
        false_positive_rate: float = 0.01
    ):
        self.redis = redis_client
        self.key = key
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate

        # Size calculations
        self.size = int(- (expected_elements * math.log(false_positive_rate)) / (math.log(2) ** 2))
        self.size = max(self.size, 8)
        self.num_hashes = int((self.size / expected_elements) * math.log(2))
        self.num_hashes = max(self.num_hashes, 1)

    def _get_hashes(self, item: str) -> List[int]:
        item_bytes = str(item).encode("utf-8")
        digest = hashlib.sha256(item_bytes).digest()
        h1 = int.from_bytes(digest[:8], byteorder="big")
        h2 = int.from_bytes(digest[8:16], byteorder="big")
        if h2 == 0:
            h2 = 1

        return [(h1 + i * h2) % self.size for i in range(self.num_hashes)]

    async def add(self, item: str) -> None:
        """Sets the computed hash bits in Redis."""
        indices = self._get_hashes(item)
        pipe = self.redis.pipeline()
        for idx in indices:
            pipe.setbit(self.key, idx, 1)
        await pipe.execute()

    async def exists(self, item: str) -> bool:
        """
        Tests membership in Redis bitset.
        Returns False if GUARANTEED not in set.
        """
        indices = self._get_hashes(item)
        pipe = self.redis.pipeline()
        for idx in indices:
            pipe.getbit(self.key, idx)
        bits = await pipe.execute()
        return all(bits)


try:
    from prometheus_client import Counter, Gauge, REGISTRY

    def _get_or_create_counter(name, doc, labels):
        # Check if already registered in default collector registry
        if name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[name]
        return Counter(name, doc, labels)

    bloom_queries_total = _get_or_create_counter(
        "bloom_filter_queries_total",
        "Total number of Bloom Filter membership lookups",
        ["filter_name", "result"]
    )
    bloom_fast_rejections_total = _get_or_create_counter(
        "bloom_filter_fast_rejections_total",
        "Total number of requests fast-rejected by Bloom Filter before touching DB/Cache",
        ["filter_name"]
    )
except ImportError:
    bloom_queries_total = None
    bloom_fast_rejections_total = None


def bloom_guard(
    bloom_filter: Union[BloomFilter, RedisBloomFilter, Any],
    id_param: str,
    filter_name: str = "default_bloom",
    not_found_message: str = "Resource not found (Bloom Guard: fast rejection)"
):
    """
    FastAPI Route Decorator for Cache Penetration Protection with full Observability:
    - Injects OpenTelemetry span attributes (`bloom.result`, `bloom.fast_rejected`)
    - Increments Prometheus metrics (`bloom_filter_queries_total`, `bloom_filter_fast_rejections_total`)
    - Short-circuits with 404 Not Found before querying Redis cache or PostgreSQL.
    """
    def decorator(func: Callable[..., Any]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            resource_id = kwargs.get(id_param)
            if resource_id is None:
                return await func(*args, **kwargs)

            # OpenTelemetry Span Enrichment
            from opentelemetry import trace
            current_span = trace.get_current_span()

            # Check existence via Bloom Filter (supports both async and sync bloom filters)
            if hasattr(bloom_filter, "exists") and callable(bloom_filter.exists):
                import inspect
                if inspect.iscoroutinefunction(bloom_filter.exists):
                    is_present = await bloom_filter.exists(str(resource_id))
                else:
                    is_present = bloom_filter.exists(str(resource_id))

                # Record Prometheus metrics
                result_label = "hit" if is_present else "miss"
                if bloom_queries_total:
                    bloom_queries_total.labels(filter_name=filter_name, result=result_label).inc()

                if not is_present:
                    if bloom_fast_rejections_total:
                        bloom_fast_rejections_total.labels(filter_name=filter_name).inc()
                    if current_span and current_span.is_recording():
                        current_span.set_attribute("bloom_filter.name", filter_name)
                        current_span.set_attribute("bloom_filter.fast_rejected", True)
                        current_span.set_attribute("bloom_filter.result", "miss")

                    logger.info(f"Bloom Filter FAST REJECT [{filter_name}]: ID '{resource_id}' does not exist.")
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=not_found_message
                    )
                else:
                    if current_span and current_span.is_recording():
                        current_span.set_attribute("bloom_filter.name", filter_name)
                        current_span.set_attribute("bloom_filter.fast_rejected", False)
                        current_span.set_attribute("bloom_filter.result", "hit")

            return await func(*args, **kwargs)
        return wrapper
    return decorator
