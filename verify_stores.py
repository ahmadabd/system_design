import httpx
import sys

BASE_URL = "http://localhost"

def test_store_functionality():
    print("=== STARTING STORES & WEBHOOKS VERIFICATION ===")
    
    # Wait a few seconds for services to settle
    print("Verifying product service availability...")
    try:
        res = httpx.get(f"{BASE_URL}/products/health")
        print(f"Health check status: {res.status_code}, body: {res.json()}")
    except Exception as e:
        print(f"Error checking health: {e}")
        sys.exit(1)

    # 1. Fetch default store (seeded store 1)
    print("\n1. Fetching seeded default store (ID 1)...")
    res = httpx.get(f"{BASE_URL}/products/stores/1")
    print(f"Status: {res.status_code}")
    print(res.text)
    if res.status_code != 200:
        print("ERROR: Default store not found or query failed.")
        sys.exit(1)
    
    store_1 = res.json()
    if store_1.get("name") != "Default Store" or store_1.get("webhook_url") != "http://localhost/webhooks/default":
        print(f"ERROR: Default store values incorrect: {store_1}")
        sys.exit(1)

    # 2. Create a new store with a webhook
    print("\n2. Creating a new store 'Partner Store A' with webhook URL...")
    store_payload = {
        "name": "Partner Store A",
        "webhook_url": "https://api.partner-a.com/webhook"
    }
    res = httpx.post(f"{BASE_URL}/products/stores", json=store_payload)
    print(f"Status: {res.status_code}")
    print(res.text)
    if res.status_code != 201:
        print("ERROR: Store creation failed.")
        sys.exit(1)
        
    store_data = res.json()
    store_id = store_data.get("id")
    print(f"Created Store ID: {store_id}")
    
    if store_data.get("webhook_url") != "https://api.partner-a.com/webhook":
        print("ERROR: webhook_url was not returned correctly.")
        sys.exit(1)

    # 3. List all stores
    print("\n3. Listing all stores...")
    res = httpx.get(f"{BASE_URL}/products/stores")
    print(f"Status: {res.status_code}")
    print(res.text)
    if res.status_code != 200:
        print("ERROR: Listing stores failed.")
        sys.exit(1)
        
    stores = res.json()
    if len(stores) < 2:
        print(f"ERROR: Expected at least 2 stores, found {len(stores)}")
        sys.exit(1)

    # 4. Create a product under the new store_id
    print(f"\n4. Creating product under store_id {store_id}...")
    prod_payload = {
        "name": "Partner Product X",
        "price": 49.99,
        "stock": 100,
        "store_id": store_id
    }
    headers = {
        "X-Idempotency-Key": "create-partner-prod-x",
        "Content-Type": "application/json"
    }
    res = httpx.post(f"{BASE_URL}/products", json=prod_payload, headers=headers)
    print(f"Status: {res.status_code}")
    print(res.text)
    if res.status_code != 201:
        print("ERROR: Product creation under new store failed.")
        sys.exit(1)

    # 5. Create a product under a non-existent store_id (should fail validation)
    print("\n5. Creating product under a non-existent store_id 99999 (should fail)...")
    invalid_prod_payload = {
        "name": "Ghost Product",
        "price": 10.00,
        "stock": 5,
        "store_id": 99999
    }
    headers = {
        "X-Idempotency-Key": "create-ghost-prod",
        "Content-Type": "application/json"
    }
    res = httpx.post(f"{BASE_URL}/products", json=invalid_prod_payload, headers=headers)
    print(f"Status: {res.status_code} (Expected: 400)")
    print(res.text)
    if res.status_code != 400:
        print("ERROR: Expected product creation to fail with 400 Bad Request, but it succeeded or returned different status.")
        sys.exit(1)
        
    if "does not exist" not in res.json().get("detail", ""):
        print(f"ERROR: Error detail did not mention store non-existence: {res.json()}")
        sys.exit(1)

    print("\n=== ALL STORE & WEBHOOK TESTS COMPLETED SUCCESSFULLY! ===")

if __name__ == "__main__":
    test_store_functionality()
