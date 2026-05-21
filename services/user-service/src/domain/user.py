from typing import List

class User:
    """User Aggregate Root"""
    def __init__(
        self,
        username: str,
        email: str,
        hashed_password: str,
        id: int | None = None
    ):
        self.id = id
        self.username = username
        self.email = email
        self.hashed_password = hashed_password
        self.domain_events: List[dict] = []

    @classmethod
    def register(cls, username: str, email: str, hashed_password: str) -> "User":
        """Factory method to register a new user and raise a domain event"""
        user = cls(username=username, email=email, hashed_password=hashed_password)
        
        # Record a domain event representing the registration
        user.record_event({
            "event_type": "UserRegistered",
            "username": username,
            "email": email
        })
        return user

    def record_event(self, event: dict) -> None:
        """Add a domain event to be dispatched later"""
        self.domain_events.append(event)

    def clear_events(self) -> None:
        """Clear recorded events after they are dispatched"""
        self.domain_events.clear()
