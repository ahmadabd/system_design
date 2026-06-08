from sqlalchemy import Column, Integer, String, Boolean, Text, JSON, DateTime, func
from shared.common.database import Base

class MaterializedStoreDB(Base):
    """SQLAlchemy model for local materialized stores view (CQRS read model)"""
    __tablename__ = "materialized_stores"

    id = Column(Integer, primary_key=True, index=True) # Represents store_id
    name = Column(String(255), nullable=False)
    webhook_url = Column(String(255), nullable=True)
    is_famous = Column(Boolean, nullable=False, default=False)

class WebhookDeliveryLogDB(Base):
    """SQLAlchemy database model for auditing outgoing store webhooks"""
    __tablename__ = "webhook_delivery_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, nullable=False, index=True)
    store_id = Column(Integer, nullable=False, index=True)
    event_type = Column(String(100), nullable=False)
    webhook_url = Column(String(255), nullable=False)
    request_payload = Column(JSON, nullable=False)
    response_status = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    attempt = Column(Integer, nullable=False, default=1)
    success = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
