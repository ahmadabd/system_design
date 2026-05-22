from abc import ABC, abstractmethod
from src.domain.user import User

class UserRepository(ABC):
    """Abstract interface for User repository (Port)"""
    
    @abstractmethod
    async def save(self, user: User) -> User:
        """Persist a User aggregate to the store"""
        pass

    @abstractmethod
    async def find_by_id(self, user_id: int) -> User | None:
        """Find a User by their unique identifier"""
        pass

    @abstractmethod
    async def find_by_email(self, email: str) -> User | None:
        """Find a User by their email address"""
        pass

    @abstractmethod
    async def find_by_username(self, username: str) -> User | None:
        """Find a User by their username"""
        pass
