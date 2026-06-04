import logging
from sqlalchemy.ext.asyncio import AsyncSession
from shared.contracts.events import UserRegisteredEvent
from shared.common.outbox import save_to_outbox

logger = logging.getLogger("UserMessagingPublisher")

class UserMessagingPublisher:
    """Outbound Messaging Adapter for User Service using Outbox Pattern"""
    def __init__(self, session: AsyncSession):
        self.session = session
        self.exchange_name = "ecommerce.events"

    async def publish_user_registered(self, event: UserRegisteredEvent) -> None:
        """Queue a UserRegisteredEvent into the database outbox"""
        # Convert Pydantic event structure to dict, transforming datetime to string
        event_dict = event.model_dump()
        event_dict["metadata"]["timestamp"] = event.metadata.timestamp.isoformat()
        
        routing_key = "user.registered"
        logger.info(f"Writing user registered event to outbox for user_id: {event.user_id}")
        await save_to_outbox(self.session, routing_key, event_dict)

