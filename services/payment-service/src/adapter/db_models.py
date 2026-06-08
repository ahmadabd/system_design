from sqlalchemy import Column, Integer, String, Float
from shared.common.database import Base

class PaymentDB(Base):
    """SQLAlchemy database model for payments table"""
    __tablename__ = "payments"

    id = Column(String(255), primary_key=True, index=True)
    order_id = Column(Integer, nullable=False, unique=True, index=True)
    amount = Column(Float, nullable=False)
    status = Column(String(50), nullable=False, default="PENDING")

class MaterializedOrderDB(Base):
    """SQLAlchemy model for local materialized orders view (CQRS read model)"""
    __tablename__ = "materialized_orders"

    order_id = Column(Integer, primary_key=True, index=True)
    total_price = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False, default=1)
    store_id = Column(Integer, nullable=False, default=1)
