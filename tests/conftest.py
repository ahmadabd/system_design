import pytest
import httpx
import uuid

BASE_URL = "http://localhost"
DEFAULT_TENANT = "store_tech"

@pytest.fixture
def base_url():
    return BASE_URL

@pytest.fixture
def tenant_slug():
    return DEFAULT_TENANT

@pytest.fixture
def auth_headers(tenant_slug):
    return {
        "X-Tenant-ID": tenant_slug,
        "Content-Type": "application/json"
    }

@pytest.fixture
def unique_idempotency_key():
    return f"idem-test-{uuid.uuid4()}"

@pytest.fixture
async def async_client():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0, trust_env=False) as client:
        yield client
