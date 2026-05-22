from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from src.domain.user import User
from src.domain.repository import UserRepository
from src.adapter.db_models import UserDB

class SQLAlchemyUserRepository(UserRepository):
    """Concrete repository mapping between the SQLAlchemy DB models and the User Domain Aggregate"""
    def __init__(self, session: AsyncSession):
        self.session = session

    def _to_domain(self, db_user: UserDB) -> User:
        """Map ORM entity to Domain Aggregate"""
        return User(
            id=db_user.id,
            username=db_user.username,
            email=db_user.email,
            hashed_password=db_user.hashed_password
        )

    async def save(self, user: User) -> User:
        """Persist Domain Aggregate to the Database"""
        if user.id is not None:
            # Update existing
            db_user = await self.session.get(UserDB, user.id)
            if db_user:
                db_user.username = user.username
                db_user.email = user.email
                db_user.hashed_password = user.hashed_password
        else:
            # Create new
            db_user = UserDB(
                username=user.username,
                email=user.email,
                hashed_password=user.hashed_password
            )
            self.session.add(db_user)
        
        await self.session.flush() # Populate generated primary key id
        return self._to_domain(db_user)

    async def find_by_id(self, user_id: int) -> User | None:
        """Find user by ID and map to Domain Aggregate"""
        db_user = await self.session.get(UserDB, user_id)
        if not db_user:
            return None
        return self._to_domain(db_user)

    async def find_by_email(self, email: str) -> User | None:
        """Find user by email and map to Domain Aggregate"""
        query = select(UserDB).where(UserDB.email == email)
        result = await self.session.execute(query)
        db_user = result.scalar_one_or_none()
        if not db_user:
            return None
        return self._to_domain(db_user)

    async def find_by_username(self, username: str) -> User | None:
        """Find user by username and map to Domain Aggregate"""
        query = select(UserDB).where(UserDB.username == username)
        result = await self.session.execute(query)
        db_user = result.scalar_one_or_none()
        if not db_user:
            return None
        return self._to_domain(db_user)
