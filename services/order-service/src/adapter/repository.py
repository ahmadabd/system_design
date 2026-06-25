from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from src.domain.order import Order
from src.domain.repository import OrderRepository
from src.adapter.db_models import OrderDB

class SQLAlchemyOrderRepository(OrderRepository):
    """Concrete repository mapping between SQLAlchemy DB models and the Order Domain Aggregate"""
    def __init__(self, session: AsyncSession):
        self.session = session

    def _to_domain(self, db_order: OrderDB) -> Order:
        """Map ORM entity to Domain Aggregate"""
        return Order(
            id=db_order.id,
            user_id=db_order.user_id,
            product_id=db_order.product_id,
            quantity=db_order.quantity,
            total_price=db_order.total_price,
            status=db_order.status,
            store_id=db_order.store_id,
            is_famous=db_order.is_famous,
            payment_method=db_order.payment_method,
            payment_url=db_order.payment_url
        )

    async def save(self, order: Order) -> Order:
        """Persist Domain Aggregate to the Database"""
        if order.id is not None:
            # Update existing
            db_order = await self.session.get(OrderDB, order.id)
            if db_order:
                db_order.user_id = order.user_id
                db_order.product_id = order.product_id
                db_order.quantity = order.quantity
                db_order.total_price = order.total_price
                db_order.status = order.status
                db_order.store_id = order.store_id
                db_order.is_famous = order.is_famous
                db_order.payment_method = order.payment_method
                db_order.payment_url = order.payment_url
        else:
            # Create new
            db_order = OrderDB(
                user_id=order.user_id,
                product_id=order.product_id,
                quantity=order.quantity,
                total_price=order.total_price,
                status=order.status,
                store_id=order.store_id,
                is_famous=order.is_famous,
                payment_method=order.payment_method,
                payment_url=order.payment_url
            )
            self.session.add(db_order)
        
        await self.session.flush() # Populate generated primary key id
        return self._to_domain(db_order)

    async def find_by_id(self, order_id: int) -> Order | None:
        """Find order by ID and map to Domain Aggregate"""
        db_order = await self.session.get(OrderDB, order_id)
        if not db_order:
            return None
        return self._to_domain(db_order)

    async def find_all(self) -> list[Order]:
        """Fetch all orders and map to Domain Aggregates"""
        query = select(OrderDB)
        result = await self.session.execute(query)
        db_orders = result.scalars().all()
        return [self._to_domain(o) for o in db_orders]
