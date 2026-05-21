from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from src.domain.product import Product
from src.domain.repository import ProductRepository
from src.adapter.db_models import ProductDB

class SQLAlchemyProductRepository(ProductRepository):
    """Concrete repository mapping between SQLAlchemy DB models and the Product Domain Aggregate"""
    def __init__(self, session: AsyncSession):
        self.session = session

    def _to_domain(self, db_prod: ProductDB) -> Product:
        """Map ORM entity to Domain Aggregate"""
        return Product(
            id=db_prod.id,
            name=db_prod.name,
            price=db_prod.price,
            stock=db_prod.stock
        )

    async def save(self, product: Product) -> Product:
        """Persist Domain Aggregate to the Database"""
        if product.id is not None:
            # Update existing
            db_prod = await self.session.get(ProductDB, product.id)
            if db_prod:
                db_prod.name = product.name
                db_prod.price = product.price
                db_prod.stock = product.stock
        else:
            # Create new
            db_prod = ProductDB(
                name=product.name,
                price=product.price,
                stock=product.stock
            )
            self.session.add(db_prod)
        
        await self.session.flush() # Populate generated primary key id
        return self._to_domain(db_prod)

    async def find_by_id(self, product_id: int) -> Product | None:
        """Find product by ID and map to Domain Aggregate"""
        db_prod = await self.session.get(ProductDB, product_id)
        if not db_prod:
            return None
        return self._to_domain(db_prod)

    async def find_all(self) -> list[Product]:
        """Fetch all products and map to Domain Aggregates"""
        query = select(ProductDB)
        result = await self.session.execute(query)
        db_products = result.scalars().all()
        return [self._to_domain(p) for p in db_products]
