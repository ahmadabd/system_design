from sqlalchemy import Column, Integer, String
from shared.common.database import Base

class UserDB(Base):
    """SQLAlchemy model for users table"""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), nullable=False, unique=True)
    email = Column(String(100), nullable=False, unique=True)
    hashed_password = Column(String(255), nullable=False)
