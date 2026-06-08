from sqlalchemy import Column, Integer, String, Float, ForeignKey
from shared.common.database import Base

class StoreDB(Base):
    """SQLAlchemy model for stores table"""
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    webhook_url = Column(String(255), nullable=True)

class ProductDB(Base):
    """SQLAlchemy model for products table"""
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    stock = Column(Integer, nullable=False, default=0)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False, default=1)

class MaterializedReservationDB(Base):
    """SQLAlchemy model for local materialized order stock reservations (CQRS read model)"""
    __tablename__ = "materialized_reservations"

    order_id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)

