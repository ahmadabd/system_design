from sqlalchemy import Column, Integer, String, Float
from shared.common.database import Base

class OrderDB(Base):
    """SQLAlchemy model for orders table"""
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    product_id = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    total_price = Column(Float, nullable=False)
    status = Column(String(50), nullable=False, default="PENDING")
    store_id = Column(Integer, nullable=False, default=1)
