import logging
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text

logger = logging.getLogger("TenantRegistry")


class TenantRegistry:
    """
    Manages the public.tenants control-plane table.
    Maintains an in-memory cache to avoid a DB round-trip on every HTTP request.
    """

    def __init__(self, engine: AsyncEngine):
        self._engine = engine
        self._cache: set[str] = set()  # in-memory set of valid tenant slugs

    async def bootstrap(self) -> None:
        """
        Create the public.tenants registry table if it doesn't exist.
        Also populate the in-memory cache from existing rows.
        Called once at service startup (inside lifespan).
        """
        async with self._engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.tenants (
                    slug        VARCHAR(100) PRIMARY KEY,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

        # Populate in-memory cache
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT slug FROM public.tenants")
            )
            for row in result:
                self._cache.add(row.slug)

        logger.info(f"TenantRegistry bootstrapped. Known tenants: {self._cache}")

    async def register(self, slug: str) -> None:
        """
        Insert a new tenant slug into the registry.
        Idempotent — safe to call even if slug already exists.
        Updates the in-memory cache immediately.
        """
        async with self._engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO public.tenants (slug) VALUES (:slug) ON CONFLICT DO NOTHING"),
                {"slug": slug}
            )
        self._cache.add(slug)
        logger.info(f"Tenant '{slug}' registered.")

    async def exists(self, slug: str) -> bool:
        """
        Check if a tenant slug is valid.
        Uses the in-memory cache first — O(1).
        If not in cache, queries the DB. If found, adds to cache.
        """
        if slug in self._cache:
            return True
            
        async with self._engine.connect() as conn:
            result = await conn.execute(
                text("SELECT 1 FROM public.tenants WHERE slug = :slug"),
                {"slug": slug}
            )
            if result.scalar() is not None:
                self._cache.add(slug)
                return True
                
        return False

    def list_all(self) -> list[str]:
        """Return all known tenant slugs from the in-memory cache."""
        return list(self._cache)
