import pytest
from algorithms.bloom_filter import BloomFilter, CountingBloomFilter


def test_bloom_filter_zero_false_negatives():
    """Verify that every added element is guaranteed to be detected (Zero False Negatives)."""
    bf = BloomFilter(expected_elements=1000, false_positive_rate=0.01)
    
    inserted_keys = [f"item_{i}" for i in range(1000)]
    for key in inserted_keys:
        bf.add(key)
        
    for key in inserted_keys:
        assert key in bf, f"Expected {key} to be in BloomFilter"
        assert bf.exists(key) is True


def test_bloom_filter_false_positive_rate():
    """Verify that the empirical false positive rate stays within bounds."""
    expected_n = 2000
    target_p = 0.02
    bf = BloomFilter(expected_elements=expected_n, false_positive_rate=target_p)
    
    for i in range(expected_n):
        bf.add(f"member_{i}")
        
    false_positives = 0
    non_members = 2000
    for i in range(expected_n, expected_n + non_members):
        if f"member_{i}" in bf:
            false_positives += 1
            
    empirical_rate = false_positives / non_members
    assert empirical_rate <= 0.05, f"False positive rate {empirical_rate} exceeded tolerance"


def test_bloom_filter_serialization():
    """Verify serialization to bytes and deserialization from bytes."""
    bf = BloomFilter(expected_elements=500, false_positive_rate=0.01)
    for i in range(500):
        bf.add(f"key_{i}")
        
    raw_bytes = bf.to_bytes()
    restored_bf = BloomFilter.from_bytes(raw_bytes, expected_elements=500, false_positive_rate=0.01)
    
    for i in range(500):
        assert f"key_{i}" in restored_bf
    assert "non_existent_key_xyz" not in restored_bf


def test_counting_bloom_filter_add_and_remove():
    """Verify that CountingBloomFilter correctly supports both additions and deletions."""
    cbf = CountingBloomFilter(expected_elements=100, false_positive_rate=0.01)
    
    cbf.add("temp_user_1")
    cbf.add("temp_user_2")
    
    assert "temp_user_1" in cbf
    assert "temp_user_2" in cbf
    
    # Remove temp_user_1
    removed = cbf.remove("temp_user_1")
    assert removed is True
    assert "temp_user_1" not in cbf
    assert "temp_user_2" in cbf  # temp_user_2 remains unharmed
    
    # Removing an un-inserted item returns False
    assert cbf.remove("never_added") is False


@pytest.mark.asyncio
async def test_bloom_guard_decorator_fast_rejection():
    """Verify that @bloom_guard rejects unknown IDs with 404 before executing wrapped handler."""
    from fastapi import HTTPException
    from shared.common.bloom import bloom_guard

    bf = BloomFilter(expected_elements=100, false_positive_rate=0.01)
    bf.add("prod_valid_1")

    db_hit_count = 0

    @bloom_guard(bloom_filter=bf, id_param="product_id")
    async def get_product_from_db(product_id: str):
        nonlocal db_hit_count
        db_hit_count += 1
        return {"id": product_id, "name": "Test Product"}

    # 1. Valid product -> Passes through
    res = await get_product_from_db(product_id="prod_valid_1")
    assert res["id"] == "prod_valid_1"
    assert db_hit_count == 1

    # 2. Invalid product -> Intercepted and raised 404 (DB is NEVER hit!)
    with pytest.raises(HTTPException) as exc_info:
        await get_product_from_db(product_id="prod_invalid_999")
    
    assert exc_info.value.status_code == 404
    assert db_hit_count == 1  # Still 1! Zero database load
