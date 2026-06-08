from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from src.domain.payment import Payment
from src.domain.repository import PaymentRepository
from src.adapter.db_models import PaymentDB, MaterializedOrderDB

class SQLAlchemyPaymentRepository(PaymentRepository):
    """Concrete SQLAlchemy Repository mapping Payment aggregates to the DB"""
    def __init__(self, session: AsyncSession):
        self.session = session

    def _to_domain(self, db_pay: PaymentDB) -> Payment:
        """Map database model to Domain Aggregate"""
        return Payment(
            id=db_pay.id,
            order_id=db_pay.order_id,
            amount=db_pay.amount,
            status=db_pay.status
        )

    async def save(self, payment: Payment) -> Payment:
        """Persist Domain Aggregate to the Database"""
        db_pay = await self.session.get(PaymentDB, payment.id) if payment.id else None
        
        if db_pay:
            # Update existing
            db_pay.amount = payment.amount
            db_pay.status = payment.status
        else:
            # Create new
            db_pay = PaymentDB(
                id=payment.id,
                order_id=payment.order_id,
                amount=payment.amount,
                status=payment.status
            )
            self.session.add(db_pay)
        
        await self.session.flush()
        return self._to_domain(db_pay)

    async def find_by_id(self, payment_id: str) -> Payment | None:
        """Fetch payment by primary key ID"""
        db_pay = await self.session.get(PaymentDB, payment_id)
        if not db_pay:
            return None
        return self._to_domain(db_pay)

    async def find_by_order_id(self, order_id: int) -> Payment | None:
        """Fetch payment by order reference ID"""
        query = select(PaymentDB).where(PaymentDB.order_id == order_id)
        result = await self.session.execute(query)
        db_pay = result.scalars().first()
        if not db_pay:
            return None
        return self._to_domain(db_pay)

    async def find_all(self) -> list[Payment]:
        """Fetch all payments"""
        query = select(PaymentDB)
        result = await self.session.execute(query)
        db_payments = result.scalars().all()
        return [self._to_domain(p) for p in db_payments]

    async def save_materialized_order(self, order_id: int, total_price: float, quantity: int, store_id: int = 1) -> None:
        """Save/upsert local materialized order details (CQRS view)"""
        db_order = await self.session.get(MaterializedOrderDB, order_id)
        if db_order:
            db_order.total_price = total_price
            db_order.quantity = quantity
            db_order.store_id = store_id
        else:
            db_order = MaterializedOrderDB(
                order_id=order_id,
                total_price=total_price,
                quantity=quantity,
                store_id=store_id
            )
            self.session.add(db_order)
        await self.session.flush()

    async def find_materialized_order(self, order_id: int) -> dict | None:
        """Find local materialized order details by order ID (CQRS view)"""
        db_order = await self.session.get(MaterializedOrderDB, order_id)
        if not db_order:
            return None
        return {
            "order_id": db_order.order_id,
            "total_price": db_order.total_price,
            "quantity": db_order.quantity,
            "store_id": db_order.store_id
        }
