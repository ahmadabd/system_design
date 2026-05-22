import logging
import hashlib
import os
from src.domain.user import User
from src.domain.repository import UserRepository
from src.application.commands import RegisterUserCommand
from src.application.dtos import UserDTO
from shared.contracts.events import UserRegisteredEvent

logger = logging.getLogger("UserApplicationService")

class UserApplicationService:
    def __init__(self, user_repo: UserRepository, event_publisher):
        self.user_repo = user_repo
        self.event_publisher = event_publisher

    async def register_user(self, command: RegisterUserCommand) -> UserDTO:
        """Register a new user, persist, and publish a UserRegistered integration event"""
        logger.info(f"Attempting to register user: {command.email}")
        
        # Enforce business invariant: username uniqueness
        existing_user_by_username = await self.user_repo.find_by_username(command.username)
        if existing_user_by_username:
            raise ValueError(f"Username '{command.username}' is already registered.")

        # Enforce business invariant: email uniqueness
        existing_user = await self.user_repo.find_by_email(command.email)
        if existing_user:
            raise ValueError(f"Email '{command.email}' is already registered.")

        # Hash the password using PBKDF2-HMAC-SHA256 (stdlib — no external dependency)
        salt = os.urandom(16).hex()
        dk = hashlib.pbkdf2_hmac("sha256", command.password.encode(), salt.encode(), 260000)
        hashed_password = f"{salt}${dk.hex()}"

        # Create aggregate root
        user = User.register(
            username=command.username,
            email=command.email,
            hashed_password=hashed_password
        )

        # Persist aggregate
        saved_user = await self.user_repo.save(user)
        logger.info(f"User aggregate successfully persisted with ID: {saved_user.id}")

        # Dispatch integration events
        for event in user.domain_events:
            integration_event = UserRegisteredEvent(
                user_id=saved_user.id,
                email=saved_user.email,
                username=saved_user.username
            )
            await self.event_publisher.publish_user_registered(integration_event)
        
        # Clear aggregate events queue
        user.clear_events()

        return UserDTO.model_validate(saved_user)

    async def get_user_by_id(self, user_id: int) -> UserDTO | None:
        """Retrieve user details by ID"""
        user = await self.user_repo.find_by_id(user_id)
        if not user:
            return None
        return UserDTO.model_validate(user)
