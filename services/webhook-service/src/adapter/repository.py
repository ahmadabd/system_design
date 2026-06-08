from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from src.adapter.db_models import MaterializedStoreDB, WebhookDeliveryLogDB

class SQLAlchemyWebhookRepository:
    """SQLAlchemy repository managing local materialized store configurations and delivery audits"""
    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_materialized_store(self, store_id: int, name: str, webhook_url: str | None, is_famous: bool = False) -> None:
        """Upsert a materialized store view (CQRS read model)"""
        db_store = await self.session.get(MaterializedStoreDB, store_id)
        if db_store:
            db_store.name = name
            db_store.webhook_url = webhook_url
            db_store.is_famous = is_famous
        else:
            db_store = MaterializedStoreDB(
                id=store_id,
                name=name,
                webhook_url=webhook_url,
                is_famous=is_famous
            )
            self.session.add(db_store)
        await self.session.flush()

    async def find_materialized_store(self, store_id: int) -> MaterializedStoreDB | None:
        """Retrieve a local store by its store_id identity"""
        return await self.session.get(MaterializedStoreDB, store_id)

    async def find_all_stores(self) -> list[MaterializedStoreDB]:
        """List all materialized store configurations"""
        query = select(MaterializedStoreDB)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def log_delivery(
        self,
        order_id: int,
        store_id: int,
        event_type: str,
        webhook_url: str,
        request_payload: dict,
        response_status: int | None,
        response_body: str | None,
        attempt: int,
        success: bool
    ) -> WebhookDeliveryLogDB:
        """Persist a webhook delivery attempt log for audit tracking"""
        log = WebhookDeliveryLogDB(
            order_id=order_id,
            store_id=store_id,
            event_type=event_type,
            webhook_url=webhook_url,
            request_payload=request_payload,
            response_status=response_status,
            response_body=response_body,
            attempt=attempt,
            success=success
        )
        self.session.add(log)
        await self.session.flush()
        return log

    async def find_all_logs(self) -> list[WebhookDeliveryLogDB]:
        """Fetch historical webhook delivery logs ordered by newest first"""
        query = select(WebhookDeliveryLogDB).order_by(WebhookDeliveryLogDB.id.desc())
        result = await self.session.execute(query)
        return list(result.scalars().all())
