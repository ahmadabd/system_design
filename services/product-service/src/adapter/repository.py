from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from src.domain.product import Product
from src.domain.store import Store
from src.domain.repository import ProductRepository, StoreRepository
from src.adapter.db_models import ProductDB, StoreDB

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
            stock=db_prod.stock,
            store_id=db_prod.store_id
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
                db_prod.store_id = product.store_id
        else:
            # Create new
            db_prod = ProductDB(
                name=product.name,
                price=product.price,
                stock=product.stock,
                store_id=product.store_id
            )
            self.session.add(db_prod)
        
        await self.session.flush() # Populate generated primary key id
        return self._to_domain(db_prod)

    async def find_by_id(self, product_id: int, for_update: bool = False) -> Product | None:
        """Find product by ID and map to Domain Aggregate"""
        if for_update:
            stmt = select(ProductDB).where(ProductDB.id == product_id).with_for_update()
            result = await self.session.execute(stmt)
            db_prod = result.scalar_one_or_none()
        else:
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

class SQLAlchemyStoreRepository(StoreRepository):
    """Concrete repository mapping between SQLAlchemy DB models and the Store Domain Aggregate"""
    def __init__(self, session: AsyncSession):
        self.session = session

    def _to_domain(self, db_store: StoreDB) -> Store:
        """Map ORM entity to Domain Aggregate"""
        return Store(
            id=db_store.id,
            name=db_store.name,
            webhook_url=db_store.webhook_url,
            is_famous=db_store.is_famous
        )

    async def save(self, store: Store) -> Store:
        """Persist Domain Aggregate to the Database"""
        if store.id is not None:
            db_store = await self.session.get(StoreDB, store.id)
            if db_store:
                db_store.name = store.name
                db_store.webhook_url = store.webhook_url
                db_store.is_famous = store.is_famous
        else:
            db_store = StoreDB(
                name=store.name,
                webhook_url=store.webhook_url,
                is_famous=store.is_famous
            )
            self.session.add(db_store)
        await self.session.flush()
        return self._to_domain(db_store)

    async def find_by_id(self, store_id: int) -> Store | None:
        """Find store by ID and map to Domain Aggregate"""
        db_store = await self.session.get(StoreDB, store_id)
        if not db_store:
            return None
        return self._to_domain(db_store)

    async def find_all(self) -> list[Store]:
        """Fetch all stores and map to Domain Aggregates"""
        query = select(StoreDB)
        result = await self.session.execute(query)
        db_stores = result.scalars().all()
        return [self._to_domain(s) for s in db_stores]
