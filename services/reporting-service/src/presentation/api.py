from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.infrastructure.db_setup import db
from src.infrastructure.config import settings
from src.adapter.db_models import ReportingProfileDB, ReportingOrderDB, ReportingPaymentDB
from shared.common.idempotency import IdempotencyManager
from shared.common.cache import cache_fallback

router = APIRouter(prefix="", tags=["Reporting"])

# Establish Redis Idempotency/Cache Manager
idempotency_manager = IdempotencyManager(settings.REDIS_URL)

@router.get("/customers/{user_id:int}/dashboard", status_code=status.HTTP_200_OK)
@cache_fallback(idempotency_manager, db.db_breaker, key_prefix="dashboard", id_param="user_id")
async def get_customer_dashboard(
    user_id: int,
    request: Request,
    session: AsyncSession = Depends(db.get_session)
):
    """
    CQRS Read Model REST Endpoint.
    Retrieves consolidated customer profile, order history, and payment transactions
    with zero downstream HTTP calls.
    """
    try:
        # 1. Fetch Profile Materialized View
        profile_res = await session.execute(
            select(ReportingProfileDB).where(ReportingProfileDB.user_id == user_id)
        )
        profile = profile_res.scalar_one_or_none()
        if not profile:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Customer dashboard not found for user ID: {user_id}. No profile materialized yet."
            )

        # 2. Fetch Orders Materialized View
        orders_res = await session.execute(
            select(ReportingOrderDB)
            .where(ReportingOrderDB.user_id == user_id)
            .order_by(ReportingOrderDB.order_id.desc())
        )
        orders = orders_res.scalars().all()

        # 3. Fetch Related Payments Materialized View
        order_ids = [o.order_id for o in orders]
        payments = []
        if order_ids:
            payments_res = await session.execute(
                select(ReportingPaymentDB).where(ReportingPaymentDB.order_id.in_(order_ids))
            )
            payments = payments_res.scalars().all()

        # 4. Construct Response Payload
        return {
            "customer_profile": {
                "user_id": profile.user_id,
                "username": profile.username,
                "email": profile.email
            },
            "orders_summary": {
                "total_orders": len(orders),
                "successful_orders": sum(1 for o in orders if o.status == "CONFIRMED"),
                "cancelled_orders": sum(1 for o in orders if o.status == "CANCELLED"),
                "pending_orders": sum(1 for o in orders if o.status == "PENDING")
            },
            "orders": [
                {
                    "order_id": o.order_id,
                    "product_id": o.product_id,
                    "quantity": o.quantity,
                    "total_price": o.total_price,
                    "status": o.status
                }
                for o in orders
            ],
            "payments": [
                {
                    "payment_id": p.payment_id,
                    "order_id": p.order_id,
                    "amount": p.amount,
                    "status": p.status
                }
                for p in payments
            ]
        }
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error executing reporting CQRS dashboard lookup: {str(e)}"
        )

@router.get("/stores/{store_id:int}/dashboard", status_code=status.HTTP_200_OK)
@cache_fallback(idempotency_manager, db.db_breaker, key_prefix="store_dashboard", id_param="store_id")
async def get_store_dashboard(
    store_id: int,
    request: Request,
    session: AsyncSession = Depends(db.get_session)
):
    """
    CQRS Read Model REST Endpoint.
    Retrieves consolidated store performance: order history, sales summary, and revenue
    with zero downstream HTTP calls.
    """
    try:
        # 1. Fetch Orders Materialized View for this store
        orders_res = await session.execute(
            select(ReportingOrderDB)
            .where(ReportingOrderDB.store_id == store_id)
            .order_by(ReportingOrderDB.order_id.desc())
        )
        orders = orders_res.scalars().all()

        # 2. Fetch Related Payments Materialized View
        order_ids = [o.order_id for o in orders]
        payments = []
        if order_ids:
            payments_res = await session.execute(
                select(ReportingPaymentDB).where(ReportingPaymentDB.order_id.in_(order_ids))
            )
            payments = payments_res.scalars().all()

        # Calculate revenue (confirmed orders only)
        total_revenue = sum(o.total_price for o in orders if o.status == "CONFIRMED")

        # 3. Construct Response Payload
        return {
            "store_id": store_id,
            "sales_summary": {
                "total_orders": len(orders),
                "successful_orders": sum(1 for o in orders if o.status == "CONFIRMED"),
                "cancelled_orders": sum(1 for o in orders if o.status == "CANCELLED"),
                "pending_orders": sum(1 for o in orders if o.status == "PENDING"),
                "total_revenue": round(total_revenue, 2)
            },
            "orders": [
                {
                    "order_id": o.order_id,
                    "user_id": o.user_id,
                    "product_id": o.product_id,
                    "quantity": o.quantity,
                    "total_price": o.total_price,
                    "status": o.status
                }
                for o in orders
            ],
            "payments": [
                {
                    "payment_id": p.payment_id,
                    "order_id": p.order_id,
                    "amount": p.amount,
                    "status": p.status
                }
                for p in payments
            ]
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error executing reporting CQRS store dashboard lookup: {str(e)}"
        )
