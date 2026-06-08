from shared.common.database import Database
from src.infrastructure.config import settings

# Instantiate database session pool specifically for the Webhook bounded context
db = Database(settings.DATABASE_URL)
