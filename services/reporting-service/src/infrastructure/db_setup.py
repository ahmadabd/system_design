from shared.common.database import Database, Base
from src.infrastructure.config import settings

# Instantiate the shared database class specifically for the Reporting bounded context
db = Database(settings.DATABASE_URL)
