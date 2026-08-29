import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException
from algorithms.bloom_filter import BloomFilter
from shared.common.bloom import bloom_guard
from pydantic import BaseModel

class UserDTO(BaseModel):
    id: int
    username: str
    email: str


@pytest.mark.asyncio
async def test_user_service_bloom_filter_cache_penetration_defense():
    """
    Verify that non-existent user profile lookups are intercepted by the Bloom Filter
    without hitting PostgreSQL or Redis.
    """
    # 1. Initialize user ID bloom filter
    user_id_bloom = BloomFilter(expected_elements=1000, false_positive_rate=0.01)

    # 2. Add existing user IDs
    valid_user_ids = [1, 2, 3, 4, 5]
    for uid in valid_user_ids:
        user_id_bloom.add(str(uid))

    # Mock user application service
    mock_user_service = MagicMock()
    mock_user_service.get_user_by_id = AsyncMock(
        return_value=UserDTO(
            id=1,
            username="alice",
            email="alice@example.com"
        )
    )

    @bloom_guard(bloom_filter=user_id_bloom, id_param="user_id", not_found_message="User not found (Bloom Guard: fast rejection)")
    async def simulated_get_user_endpoint(user_id: int):
        return await mock_user_service.get_user_by_id(user_id)

    # A. Valid user lookup (ID: 1) -> Passes through to DB
    user = await simulated_get_user_endpoint(user_id=1)
    assert user.id == 1
    assert mock_user_service.get_user_by_id.call_count == 1

    # B. Cache Penetration Defense: 500 non-existent user IDs queried
    for missing_id in range(5000, 5500):
        with pytest.raises(HTTPException) as exc_info:
            await simulated_get_user_endpoint(user_id=missing_id)
        assert exc_info.value.status_code == 404
        assert "Bloom Guard: fast rejection" in exc_info.value.detail

    # DB service call count remains 1
    assert mock_user_service.get_user_by_id.call_count == 1


def test_user_identity_bloom_uniqueness_precheck():
    """
    Verify that user identity Bloom filter provides guaranteed uniqueness checking:
    If email/username is not in Bloom filter, it is 100% guaranteed unique.
    """
    identity_bloom = BloomFilter(expected_elements=5000, false_positive_rate=0.01)

    # Add existing registered emails/usernames
    identity_bloom.add("email:alice@example.com")
    identity_bloom.add("username:alice")

    # 1. Existing user checks
    assert "email:alice@example.com" in identity_bloom
    assert "username:alice" in identity_bloom

    # 2. Brand new user checks (Guaranteed 100% unique -> zero DB collision pre-query needed)
    assert "email:bob@example.com" not in identity_bloom
    assert "username:bob" not in identity_bloom
