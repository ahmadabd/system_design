import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from shared.common.tenant import set_tenant, TenantContext
from shared.common.tenant_registry import TenantRegistry

logger = logging.getLogger("TenantMiddleware")

# Paths that don't require an X-Tenant-ID header
TENANT_EXEMPT_PATHS = {"/health", "/metrics", "/docs", "/openapi.json", "/redoc"}
TENANT_EXEMPT_PREFIXES = ("/admin/",)


class TenantMiddleware(BaseHTTPMiddleware):
    """
    Reads the X-Tenant-ID header from every incoming HTTP request,
    validates that the tenant exists in the registry,
    and sets the TenantContext ContextVar for the duration of the request.

    Returns:
        400 Bad Request  — if X-Tenant-ID header is missing on a protected path
        404 Not Found    — if the tenant slug is not in the registry
    """

    def __init__(self, app, registry: TenantRegistry):
        super().__init__(app)
        self.registry = registry

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Skip tenant resolution for system/admin paths
        is_exempt = path in TENANT_EXEMPT_PATHS or any(
            path.startswith(p) for p in TENANT_EXEMPT_PREFIXES
        )
        if is_exempt:
            return await call_next(request)

        # Read the tenant header
        slug = request.headers.get("X-Tenant-ID")

        if not slug:
            return JSONResponse(
                status_code=400,
                content={"detail": "Missing required header: X-Tenant-ID"}
            )

        if not await self.registry.exists(slug):
            return JSONResponse(
                status_code=404,
                content={"detail": f"Tenant '{slug}' not found. Provision it first via POST /admin/tenants"}
            )

        # Set the tenant context in request.state (survives BaseHTTPMiddleware task boundary)
        request.state.tenant_slug = slug
        # Also set ContextVar for code that doesn't have access to request object
        set_tenant(TenantContext(slug=slug))
        logger.debug(f"Tenant context set: slug='{slug}'")

        return await call_next(request)
