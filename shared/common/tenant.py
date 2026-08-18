from contextvars import ContextVar
from dataclasses import dataclass


@dataclass
class TenantContext:
    """Immutable tenant identity for the current request."""
    slug: str  # e.g. "store_acme" — also the PostgreSQL schema name


# One ContextVar per process. Each concurrent asyncio Task gets its own copy.
_current_tenant: ContextVar[TenantContext | None] = ContextVar(
    "current_tenant", default=None
)


def set_tenant(ctx: TenantContext) -> None:
    """Set the tenant for the current async task (request scope)."""
    _current_tenant.set(ctx)


def get_tenant() -> TenantContext:
    """
    Get the tenant for the current async task.
    Raises RuntimeError if no tenant has been set (missing middleware or
    called from a context where tenant is not applicable).
    """
    ctx = _current_tenant.get()
    if ctx is None:
        raise RuntimeError(
            "No TenantContext is set. Ensure TenantMiddleware is registered "
            "and the request carries an X-Tenant-ID header."
        )
    return ctx


def get_tenant_or_none() -> TenantContext | None:
    """
    Get the tenant or None if not set.
    Use this in background tasks and health check paths where no tenant is expected.
    """
    return _current_tenant.get()
