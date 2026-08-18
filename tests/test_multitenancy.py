import uuid
import pytest
import httpx

@pytest.mark.asyncio
async def test_tenant_header_validation(async_client):
    """
    Verify that protected endpoints reject missing or unprovisioned X-Tenant-ID headers.
    """
    # 1. Missing header on products -> 400 Bad Request
    resp_missing_p = await async_client.get("/products/")
    assert resp_missing_p.status_code == 400
    assert "Missing required header: X-Tenant-ID" in resp_missing_p.json()["detail"]

    # 2. Missing header on orders -> 400 Bad Request
    resp_missing_o = await async_client.get("/orders/")
    assert resp_missing_o.status_code == 400
    assert "Missing required header: X-Tenant-ID" in resp_missing_o.json()["detail"]

    # 3. Unregistered tenant on products -> 404 Not Found
    resp_invalid_p = await async_client.get("/products/", headers={"X-Tenant-ID": "non_existent_tenant_999"})
    assert resp_invalid_p.status_code == 404
    assert "not found" in resp_invalid_p.json()["detail"].lower()

    # 4. Unregistered tenant on orders -> 404 Not Found
    resp_invalid_o = await async_client.get("/orders/", headers={"X-Tenant-ID": "non_existent_tenant_999"})
    assert resp_invalid_o.status_code == 404
    assert "not found" in resp_invalid_o.json()["detail"].lower()


@pytest.mark.asyncio
async def test_dynamic_tenant_provisioning_and_isolation(async_client):
    """
    Verify dynamic provisioning of a new tenant and schema isolation:
    A product created in store_alpha must NOT be visible or orderable in store_beta.
    """
    tenant_a = f"tenant_a_{uuid.uuid4().hex[:6]}"
    tenant_b = f"tenant_b_{uuid.uuid4().hex[:6]}"

    # 1. Provision Tenant A across products and orders
    resp_prov_p_a = await async_client.post("/products/admin/tenants", json={"slug": tenant_a})
    assert resp_prov_p_a.status_code in [200, 201]
    resp_prov_o_a = await async_client.post("/orders/admin/tenants", json={"slug": tenant_a})
    assert resp_prov_o_a.status_code in [200, 201]

    # 2. Provision Tenant B across products and orders
    resp_prov_p_b = await async_client.post("/products/admin/tenants", json={"slug": tenant_b})
    assert resp_prov_p_b.status_code in [200, 201]
    resp_prov_o_b = await async_client.post("/orders/admin/tenants", json={"slug": tenant_b})
    assert resp_prov_o_b.status_code in [200, 201]

    # 3. Create Product in Tenant A
    prod_a_resp = await async_client.post("/products/", json={
        "name": "Secret Product A",
        "price": 99.00,
        "stock": 10,
        "store_id": 1
    }, headers={"X-Tenant-ID": tenant_a, "X-Idempotency-Key": f"prod-a-{uuid.uuid4()}"})
    assert prod_a_resp.status_code == 201
    prod_a_id = prod_a_resp.json()["id"]

    # 4. Verify Tenant A can see its product
    get_a = await async_client.get(f"/products/{prod_a_id}", headers={"X-Tenant-ID": tenant_a})
    assert get_a.status_code == 200
    assert get_a.json()["name"] == "Secret Product A"

    # 5. Verify Tenant B CANNOT see Tenant A's product (Returns 404 / null)
    get_b = await async_client.get(f"/products/{prod_a_id}", headers={"X-Tenant-ID": tenant_b})
    assert get_b.status_code == 404 or get_b.json() is None

    # 6. Verify Tenant B CANNOT place an order for Tenant A's product
    order_b_resp = await async_client.post("/orders/", json={
        "user_id": 1,
        "product_id": prod_a_id,
        "quantity": 1,
        "total_price": 99.00,
        "store_id": 1
    }, headers={"X-Tenant-ID": tenant_b, "X-Idempotency-Key": f"order-cross-{uuid.uuid4()}"})
    
    assert order_b_resp.status_code == 400
    assert "does not exist" in order_b_resp.json()["detail"]
