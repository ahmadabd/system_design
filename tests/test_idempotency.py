import asyncio
import uuid
import pytest
import httpx

@pytest.mark.asyncio
async def test_sequential_idempotency(async_client, auth_headers):
    """
    Test that sending duplicate requests with the same X-Idempotency-Key
    returns the cached response without creating duplicate database records.
    """
    key = f"seq-idem-{uuid.uuid4()}"
    headers = {**auth_headers, "X-Idempotency-Key": key}
    payload = {
        "user_id": 1,
        "product_id": 1,
        "quantity": 1,
        "total_price": 1299.99,
        "store_id": 1
    }

    # First request: creates the order
    resp1 = await async_client.post("/orders/", json=payload, headers=headers)
    assert resp1.status_code == 201, f"First request failed: {resp1.text}"
    data1 = resp1.json()
    order_id1 = data1["id"]

    # Second request: must return cached response with the SAME order ID
    resp2 = await async_client.post("/orders/", json=payload, headers=headers)
    assert resp2.status_code == 201, f"Second request failed: {resp2.text}"
    data2 = resp2.json()
    order_id2 = data2["id"]

    assert order_id1 == order_id2, "Idempotency failed: generated different order IDs for same key!"
    assert data1["total_price"] == data2["total_price"]


@pytest.mark.asyncio
async def test_concurrent_race_condition_idempotency(async_client, auth_headers):
    """
    Test that sending 15 simultaneous requests with the SAME idempotency key
    results in exactly ONE database creation and consistent responses.
    """
    key = f"race-idem-{uuid.uuid4()}"
    headers = {**auth_headers, "X-Idempotency-Key": key}
    payload = {
        "user_id": 1,
        "product_id": 1,
        "quantity": 1,
        "total_price": 1299.99,
        "store_id": 1
    }

    # Fire 15 concurrent POST requests with the identical key
    tasks = [async_client.post("/orders/", json=payload, headers=headers) for _ in range(15)]
    responses = await asyncio.gather(*tasks)

    status_codes = [r.status_code for r in responses]
    # Idempotency lock returns 201 (success/cached) or 409 Conflict (in-flight lock)
    assert all(code in [201, 409] for code in status_codes), f"Unexpected status codes: {status_codes}"

    # Exactly ONE unique order ID was created in the database across all 201 responses
    order_ids = {r.json().get("id") for r in responses if r.status_code == 201}
    assert len(order_ids) == 1, f"Concurrency race condition: multiple orders created: {order_ids}"


@pytest.mark.asyncio
async def test_idempotency_tenant_isolation(async_client):
    """
    Test that the same idempotency key used in two different tenants
    does NOT collide (Redis keys are namespaced by tenant slug).
    """
    shared_key = f"cross-tenant-{uuid.uuid4()}"
    
    # Provision store_gaming if not exists in both product-service and order-service
    await async_client.post("/products/admin/tenants", json={
        "slug": "store_gaming",
        "name": "Gaming Superstore",
        "owner_email": "gaming@store.com"
    })
    await async_client.post("/orders/admin/tenants", json={
        "slug": "store_gaming",
        "name": "Gaming Superstore",
        "owner_email": "gaming@store.com"
    })

    # Create product in store_gaming
    await async_client.post("/products/", json={
        "name": "Gaming Mouse",
        "price": 49.99,
        "stock": 100,
        "store_id": 1
    }, headers={"X-Tenant-ID": "store_gaming", "X-Idempotency-Key": f"prod-{uuid.uuid4()}"})

    # Request 1 in store_tech
    resp_tech = await async_client.post("/orders/", json={
        "user_id": 1,
        "product_id": 1,
        "quantity": 1,
        "total_price": 1299.99,
        "store_id": 1
    }, headers={"X-Tenant-ID": "store_tech", "X-Idempotency-Key": shared_key})

    # Request 2 in store_gaming with SAME idempotency key
    resp_gaming = await async_client.post("/orders/", json={
        "user_id": 1,
        "product_id": 1,
        "quantity": 1,
        "total_price": 49.99,
        "store_id": 1
    }, headers={"X-Tenant-ID": "store_gaming", "X-Idempotency-Key": shared_key})

    assert resp_tech.status_code == 201
    assert resp_gaming.status_code == 201
    # Both tenants successfully process their own order without cross-tenant key conflict
    assert resp_tech.json()["total_price"] != resp_gaming.json()["total_price"]
