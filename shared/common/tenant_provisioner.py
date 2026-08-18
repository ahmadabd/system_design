import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text
from shared.common.tenant_registry import TenantRegistry

logger = logging.getLogger("TenantProvisioner")


class TenantProvisioner:
    """
    Handles the full lifecycle of onboarding a new tenant:
      1. Create PostgreSQL schema
      2. Register in public.tenants registry
      3. Run Alembic migrations inside that schema (creates all service tables)
    """

    def __init__(
        self,
        engine: AsyncEngine,
        registry: TenantRegistry,
        alembic_ini_path: str = "alembic.ini"
    ):
        self._engine = engine
        self._registry = registry
        self._alembic_ini_path = alembic_ini_path

    async def provision(self, slug: str) -> None:
        """
        Full tenant provisioning sequence.
        Idempotent — safe to call even if schema already exists.

        Args:
            slug: The tenant identifier, e.g. "store_acme".
                  This becomes the PostgreSQL schema name.
        """
        logger.info(f"Provisioning tenant: '{slug}'")

        # Step 1: Create the PostgreSQL schema
        async with self._engine.begin() as conn:
            # IF NOT EXISTS makes this idempotent
            await conn.execute(
                text(f"CREATE SCHEMA IF NOT EXISTS {slug}")
            )
        logger.info(f"Schema '{slug}' created (or already exists).")

        # Step 2: Register in public.tenants control plane
        await self._registry.register(slug)

        # Step 3: Run Alembic migrations inside the new schema
        # Must run in a thread because Alembic is synchronous
        await asyncio.to_thread(self._run_migrations_sync, slug)

        # Step 4: Seed default data (e.g. default store) inside the tenant schema
        async with self._engine.begin() as conn:
            await conn.execute(
                text(f"SET LOCAL search_path TO {slug}, public")
            )
            try:
                # Check if store exists, if not insert it
                await conn.execute(
                    text("""
                    INSERT INTO stores (id, name, webhook_url, is_famous)
                    VALUES (1, :name, 'http://localhost/webhooks/default', FALSE)
                    ON CONFLICT (id) DO NOTHING
                    """),
                    {"name": f"Default Store for {slug}"}
                )
            except Exception as e:
                # If the 'stores' table doesn't exist (e.g. we are running migrations for a DB that doesn't have it), just ignore.
                logger.debug(f"Skipping default store insertion: {e}")
            # Need to catch potential error if 'stores' table doesn't exist (e.g. reporting service)
            # but since product-service always creates 'stores', we assume it's there. 
            # Wait, reporting-service doesn't have 'stores' table! So this might fail if we run provisioning 
            # from reporting-service? No, reporting service HAS a stores table? No, it has reporting_orders which has store_id.
            # But provisioning runs from product-service API! The API router for provisioning is only in product-service.

        logger.info(f"Tenant '{slug}' fully provisioned and ready.")

    def _run_migrations_sync(self, schema: str) -> None:
        """
        Synchronous Alembic migration runner.
        Sets search_path before running so all CREATE TABLE statements
        land inside the tenant schema, not public.
        """
        from alembic.config import Config
        from alembic import command

        config = Config(self._alembic_ini_path)
        db_url = self._engine.url.render_as_string(hide_password=False).replace("+asyncpg", "+psycopg2")
        config.set_main_option("sqlalchemy.url", db_url)
        # Pass the target schema as a custom config option — read by env.py
        config.set_main_option("target_schema", schema)

        logger.info(f"Running Alembic 'upgrade head' for schema '{schema}'...")
        command.upgrade(config, "head")
        logger.info(f"Alembic migrations complete for schema '{schema}'.")
