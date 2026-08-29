from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from src.infrastructure.db_setup import db
from src.adapter.repository import SQLAlchemyWebhookRepository
from shared.common.resilience import CircuitBreakerOpenException
from datetime import datetime

router = APIRouter(prefix="", tags=["Webhooks"])

# Pydantic Schemas for JSON serialization
class MaterializedStoreDTO(BaseModel):
    id: int
    name: str
    webhook_url: str | None
    is_famous: bool = False

    class Config:
        from_attributes = True

class WebhookDeliveryLogDTO(BaseModel):
    id: int
    order_id: int
    store_id: int
    event_type: str
    webhook_url: str
    request_payload: dict
    response_status: int | None
    response_body: str | None
    attempt: int
    success: bool
    created_at: datetime

    class Config:
        from_attributes = True

async def get_repository(
    session: AsyncSession = Depends(db.get_session)
) -> SQLAlchemyWebhookRepository:
    return SQLAlchemyWebhookRepository(session)

@router.get("/stores", response_model=list[MaterializedStoreDTO])
async def list_stores(
    repo: SQLAlchemyWebhookRepository = Depends(get_repository)
):
    """Retrieve all store webhook configurations materialized inside the service"""
    try:
        stores = await repo.find_all_stores()
        return stores
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}. Read operations degraded."
        )

@router.get("/logs", response_model=list[WebhookDeliveryLogDTO])
async def list_logs(
    repo: SQLAlchemyWebhookRepository = Depends(get_repository)
):
    """Retrieve historical logs auditing webhook delivery execution attempts"""
    try:
        logs = await repo.find_all_logs()
        return logs
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}. Read operations degraded."
        )

@router.get("/lsm-logs/{log_key:path}")
async def get_lsm_log(log_key: str):
    """
    Retrieve a delivery audit record directly from the LSM Tree Engine
    (Checks RAM MemTable -> SSTables with embedded Bloom Filter).
    """
    from src.infrastructure.lsm_storage import webhook_lsm_engine
    record = webhook_lsm_engine.get_delivery_log(log_key)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Log key '{log_key}' not found in LSM storage (Bloom filter / SSTable miss)."
        )
    return record

@router.get("/lsm-stats")
async def get_lsm_stats():
    """Returns real-time LSM storage metrics (MemTable size, SSTables count, Bloom filter items)"""
    from src.infrastructure.lsm_storage import webhook_lsm_engine
    return {
        "data_dir": webhook_lsm_engine.data_dir,
        "memtable_entries": len(webhook_lsm_engine.lsm.memtable),
        "sstables_count": len(webhook_lsm_engine.lsm.sstables),
        "dedup_bloom_items": webhook_lsm_engine.dedup_bloom.count
    }
