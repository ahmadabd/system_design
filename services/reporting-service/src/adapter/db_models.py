from sqlalchemy import Column, Integer, String, Float
from shared.common.database import Base

class ReportingProfileDB(Base):
    """SQLAlchemy model for customer profiles reporting table"""
    __tablename__ = "reporting_profiles"

    user_id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), nullable=False)
    email = Column(String(100), nullable=False)


class ReportingOrderDB(Base):
    """SQLAlchemy model for orders reporting table"""
    __tablename__ = "reporting_orders"

    order_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    product_id = Column(Integer, nullable=False)
    quantity = Column(Integer, nullable=False)
    total_price = Column(Float, nullable=False)
    status = Column(String(50), nullable=False, default="PENDING")


class ReportingPaymentDB(Base):
    """SQLAlchemy model for payments reporting table"""
    __tablename__ = "reporting_payments"

    payment_id = Column(String(255), primary_key=True, index=True)
    order_id = Column(Integer, nullable=False, index=True)
    amount = Column(Float, nullable=False)
    status = Column(String(50), nullable=False)
