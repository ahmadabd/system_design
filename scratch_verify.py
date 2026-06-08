import httpx
import time
import sys

BASE_URL = "http://localhost"

def test_marketplace_flow():
    print("=== STARTING MARKETPLACE INTEGRATION VERIFICATION ===")
    
    # 1. Register User
    print("\n1. Registering user...")
    ts = int(time.time())
    user_payload = {
        "username": f"buyer_bob_{ts}",
        "email": f"bob_{ts}@example.com",
        "password": "supersecretpassword"
    }
    headers = {
        "X-Idempotency-Key": f"reg-buyer-bob-{ts}",
        "Content-Type": "application/json"
    }
    res = httpx.post(f"{BASE_URL}/users", json=user_payload, headers=headers)
    print(f"Status: {res.status_code}")
    print(res.text)
    if res.status_code not in [200, 201]:
        print("ERROR: User registration failed.")
        sys.exit(1)
        
    # Register Store dynamically
    print("\nRegistering store dynamically...")
    store_payload = {
        "name": "Verify Store 99",
        "webhook_url": "https://api.verifystore99.com/webhook"
    }
    res = httpx.post(f"{BASE_URL}/products/stores", json=store_payload)
    print(f"Status: {res.status_code}")
    print(res.text)
    if res.status_code not in [200, 201]:
        print("ERROR: Store registration failed.")
        sys.exit(1)
    
    store_id = res.json().get("id")
    print(f"Created Store ID: {store_id}")
        
    # 2. Create product for dynamic Store ID
    print(f"\n2. Creating product for Store #{store_id}...")
    prod_payload = {
        "name": "Wireless Noise-Cancelling Headphones",
        "price": 150.00,
        "stock": 10,
        "store_id": store_id
    }
    ts = int(time.time())
    headers = {
        "X-Idempotency-Key": f"create-prod-headphones-{ts}",
        "Content-Type": "application/json"
    }
    res = httpx.post(f"{BASE_URL}/products", json=prod_payload, headers=headers)
    print(f"Status: {res.status_code}")
    print(res.text)
    if res.status_code not in [200, 201]:
        print("ERROR: Product creation failed.")
        sys.exit(1)
    
    product_id = res.json().get("id")
    print(f"Created Product ID: {product_id}")
 
    # 3. Fetch product details to verify store_id
    print(f"\n3. Fetching product details for ID {product_id}...")
    res = httpx.get(f"{BASE_URL}/products/{product_id}")
    print(f"Status: {res.status_code}")
    print(res.text)
    product_data = res.json()
    if product_data.get("store_id") != store_id:
        print(f"ERROR: Expected store_id to be {store_id}, got {product_data.get('store_id')}")
        sys.exit(1)
        
    # 4. Place an order (without specifying store_id; order-service should resolve it dynamically)
    print("\n4. Placing order for Product #1...")
    order_payload = {
        "user_id": 1,
        "product_id": product_id,
        "quantity": 2,
        "total_price": 300.00
    }
    ts = int(time.time())
    headers = {
        "X-Idempotency-Key": f"checkout-order-marketplace-{ts}",
        "Content-Type": "application/json"
    }
    res = httpx.post(f"{BASE_URL}/orders", json=order_payload, headers=headers)
    print(f"Status: {res.status_code}")
    print(res.text)
    if res.status_code not in [200, 201]:
        print("ERROR: Order placement failed.")
        sys.exit(1)
        
    order_id = res.json().get("id")
    print(f"Created Order ID: {order_id}")
 
    # Wait for Saga workflow completion (Stock reservation & Payment succeeded)
    print("\nWaiting 5 seconds for Kafka events to propagate and saga to finish...")
    time.sleep(5)
    
    # 5. Fetch order status
    print(f"\n5. Fetching order status for ID {order_id}...")
    res = httpx.get(f"{BASE_URL}/orders/{order_id}")
    print(f"Status: {res.status_code}")
    print(res.text)
    order_data = res.json()
    if order_data.get("status") != "CONFIRMED":
        print(f"ERROR: Expected order status to be CONFIRMED, got {order_data.get('status')}")
        sys.exit(1)
    if order_data.get("store_id") != store_id:
        print(f"ERROR: Expected order store_id to be {store_id}, got {order_data.get('store_id')}")
        sys.exit(1)

    # 6. Fetch Store Dashboard from Reporting Service
    print(f"\n6. Fetching Store #{store_id} Performance Dashboard...")
    res = httpx.get(f"{BASE_URL}/reporting/stores/{store_id}/dashboard")
    print(f"Status: {res.status_code}")
    print(res.text)
    dashboard_data = res.json()
    
    summary = dashboard_data.get("sales_summary", {})
    if summary.get("total_orders") != 1:
        print(f"ERROR: Expected total_orders to be 1, got {summary.get('total_orders')}")
        sys.exit(1)
    if summary.get("successful_orders") != 1:
        print(f"ERROR: Expected successful_orders to be 1, got {summary.get('successful_orders')}")
        sys.exit(1)
    if summary.get("total_revenue") != 300.00:
        print(f"ERROR: Expected total_revenue to be 300.00, got {summary.get('total_revenue')}")
        sys.exit(1)

    print("\n=== ALL MARKETPLACE TESTS COMPLETED SUCCESSFULLY! ===")

if __name__ == "__main__":
    test_marketplace_flow()
