from shared.common.database import Database, Base
from src.infrastructure.config import settings

# Instantiate database session pool specifically for the Payment bounded context
db = Database(settings.DATABASE_URL)
