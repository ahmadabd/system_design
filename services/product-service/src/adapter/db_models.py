from sqlalchemy import Column, Integer, String, Float
from shared.common.database import Base

class ProductDB(Base):
    """SQLAlchemy model for products table"""
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    stock = Column(Integer, nullable=False, default=0)
