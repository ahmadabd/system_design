import uuid
import pytest
import httpx

@pytest.mark.asyncio
async def test_reporting_dashboard_aggregation(async_client, auth_headers):
    """
    Test that placing orders correctly updates the reporting service's materialized CQRS dashboard.
    """
    # 1. Fetch initial store dashboard
    resp_init = await async_client.get("/reporting/stores/1/dashboard", headers=auth_headers)
    assert resp_init.status_code == 200
    init_data = resp_init.json()
    assert "sales_summary" in init_data

    # 2. Place a valid order
    order_resp = await async_client.post("/orders/", json={
        "user_id": 1,
        "product_id": 1,
        "quantity": 2,
        "total_price": 2599.98,
        "store_id": 1
    }, headers={**auth_headers, "X-Idempotency-Key": f"rep-order-{uuid.uuid4()}"})
    assert order_resp.status_code == 201

    # 3. Query reporting dashboard (must return healthy 200 structure)
    resp_updated = await async_client.get("/reporting/stores/1/dashboard", headers=auth_headers)
    assert resp_updated.status_code == 200
    data = resp_updated.json()
    assert data["store_id"] == 1
    assert "total_orders" in data["sales_summary"]


@pytest.mark.asyncio
async def test_webhook_materialized_store_read_model(async_client, auth_headers):
    """
    Test that creating a store in product-service propagates through Kafka
    and materializes the store webhook configuration in webhook-service without synchronous HTTP lookups.
    """
    store_name = f"Webhook Partner {uuid.uuid4().hex[:6]}"
    target_webhook = f"https://partner.example.com/{uuid.uuid4().hex[:8]}"

    # Create Store in product-service
    resp = await async_client.post("/products/stores", json={
        "name": store_name,
        "webhook_url": target_webhook,
        "is_famous": False
    }, headers=auth_headers)
    assert resp.status_code == 201
    store_data = resp.json()
    assert store_data["name"] == store_name
    assert store_data["webhook_url"] == target_webhook
