class Store:
    """Store Domain Aggregate Root"""
    def __init__(
        self,
        name: str,
        webhook_url: str | None = None,
        id: int | None = None,
        is_famous: bool = False
    ):
        self.id = id
        self.name = name
        self.webhook_url = webhook_url
        self.is_famous = is_famous

    @classmethod
    def create(cls, name: str, webhook_url: str | None = None, is_famous: bool = False) -> "Store":
        """Factory method to create a new store"""
        if not name or len(name.strip()) < 2:
            raise ValueError("Store name must be at least 2 characters long")
        return cls(name=name.strip(), webhook_url=webhook_url, is_famous=is_famous)
