from sqlalchemy import Column, Integer, String, Float
from shared.common.database import Base

class PaymentDB(Base):
    """SQLAlchemy database model for payments table"""
    __tablename__ = "payments"

    id = Column(String(255), primary_key=True, index=True)
    order_id = Column(Integer, nullable=False, unique=True, index=True)
    amount = Column(Float, nullable=False)
    status = Column(String(50), nullable=False, default="PENDING")
