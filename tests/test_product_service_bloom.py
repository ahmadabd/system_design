import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException
from algorithms.bloom_filter import BloomFilter
from shared.common.bloom import bloom_guard
from pydantic import BaseModel

class ProductDTO(BaseModel):
    id: int
    name: str
    price: float
    stock: int
    store_id: int
    is_famous: bool = False


@pytest.mark.asyncio
async def test_product_service_bloom_filter_cache_penetration_defense():
    """
    Verify that non-existent product IDs are intercepted at the edge by the Bloom Filter
    without making any calls to PostgreSQL or Redis Cache.
    """
    # 1. Initialize product bloom filter
    test_bloom = BloomFilter(expected_elements=1000, false_positive_rate=0.01)

    # 2. Add existing valid products
    valid_product_ids = [101, 102, 103, 104, 105]
    for pid in valid_product_ids:
        test_bloom.add(str(pid))

    # Mock database service
    mock_db_service = MagicMock()
    mock_db_service.get_product_by_id = AsyncMock(
        return_value=ProductDTO(
            id=101,
            name="Mechanical Keyboard",
            price=120.0,
            stock=50,
            store_id=1,
            is_famous=False
        )
    )

    # Route handler guarded with Bloom Filter
    @bloom_guard(bloom_filter=test_bloom, id_param="product_id", not_found_message="Product not found (Bloom Guard: fast rejection)")
    async def simulated_get_product_endpoint(product_id: int):
        return await mock_db_service.get_product_by_id(product_id)

    # A. Query an existing product (ID: 101) -> Passes through Bloom Guard to DB
    product = await simulated_get_product_endpoint(product_id=101)
    assert product.id == 101
    assert mock_db_service.get_product_by_id.call_count == 1

    # B. Simulate a Cache Penetration Attack: 1000 random non-existent product IDs queried by bots
    for non_existent_id in range(9000, 10000):
        with pytest.raises(HTTPException) as exc_info:
            await simulated_get_product_endpoint(product_id=non_existent_id)
        assert exc_info.value.status_code == 404
        assert "Bloom Guard: fast rejection" in exc_info.value.detail

    # Crucial assertion: Database service call count MUST STILL BE 1!
    # Zero queries hit the database for the 1000 non-existent IDs.
    assert mock_db_service.get_product_by_id.call_count == 1
