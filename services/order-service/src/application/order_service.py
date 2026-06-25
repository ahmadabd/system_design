import logging
from src.domain.order import Order
from src.domain.repository import OrderRepository
from src.domain.services import UserClient, ProductClient
from src.application.commands import CreateOrderCommand, ConfirmOrderCommand, CancelOrderCommand, SetAwaitingPaymentCommand
from src.application.dtos import OrderDTO
from shared.contracts.events import OrderCreatedEvent

logger = logging.getLogger("OrderApplicationService")

class OrderApplicationService:
    def __init__(
        self, 
        order_repo: OrderRepository, 
        event_publisher,
        user_client: UserClient = None,
        product_client: ProductClient = None
    ):
        self.order_repo = order_repo
        self.event_publisher = event_publisher
        self.user_client = user_client
        self.product_client = product_client

    async def create_order(self, command: CreateOrderCommand) -> OrderDTO:
        """Place a pending order and dispatch OrderCreated integration event"""
        logger.info(f"Creating order for User: {command.user_id}, Product: {command.product_id}")
        
        # Verify downstream invariants via domain ports
        if self.user_client and not await self.user_client.verify_user(command.user_id):
            raise ValueError(f"User with ID {command.user_id} does not exist.")
            
        product_details = None
        if self.product_client:
            product_details = await self.product_client.get_product_details(command.product_id)
            if not product_details:
                raise ValueError(f"Product with ID {command.product_id} does not exist.")
        
        # Resolve store_id and is_famous
        store_id = command.store_id
        is_famous = False
        if product_details:
            if store_id is None:
                store_id = product_details.get("store_id")
            is_famous = product_details.get("is_famous", False)
        if store_id is None:
            store_id = 1
        
        # Instantiate aggregate root
        order = Order.create(
            user_id=command.user_id,
            product_id=command.product_id,
            quantity=command.quantity,
            total_price=command.total_price,
            store_id=store_id,
            is_famous=is_famous,
            payment_method=command.payment_method
        )

        # Persist aggregate
        saved_order = await self.order_repo.save(order)
        logger.info(f"Order successfully placed with ID: {saved_order.id}")

        # Publish integration events
        for event in order.domain_events:
            if event["event_type"] == "OrderCreated":
                integration_event = OrderCreatedEvent(
                    order_id=saved_order.id,
                    user_id=event["user_id"],
                    product_id=event["product_id"],
                    quantity=event["quantity"],
                    total_price=event["total_price"],
                    store_id=event["store_id"],
                    is_famous=event["is_famous"],
                    payment_method=event["payment_method"]
                )
                await self.event_publisher.publish_order_created(integration_event)

        # Clear aggregate event loop queue
        order.clear_events()

        return OrderDTO.model_validate(saved_order)

    async def set_awaiting_payment(self, command: SetAwaitingPaymentCommand) -> None:
        """Set order status to AWAITING_PAYMENT and record redirect URL"""
        logger.info(f"Setting order {command.order_id} to AWAITING_PAYMENT with URL {command.payment_url}")
        order = await self.order_repo.find_by_id(command.order_id)
        if not order:
            logger.error(f"Order ID {command.order_id} not found during set awaiting payment.")
            return

        order.mark_awaiting_payment(command.payment_url)
        await self.order_repo.save(order)
        logger.info(f"Order {command.order_id} successfully marked as awaiting payment!")

    async def confirm_order(self, command: ConfirmOrderCommand) -> None:
        """Confirm the order after inventory reservation success"""
        logger.info(f"Confirming order: {command.order_id}")
        order = await self.order_repo.find_by_id(command.order_id)
        if not order:
            logger.error(f"Order ID {command.order_id} not found during confirmation.")
            return

        order.confirm()
        saved = await self.order_repo.save(order)
        logger.info(f"Order {command.order_id} successfully confirmed in database!")

        # Publish OrderConfirmedEvent via Transactional Outbox
        from shared.contracts.events import OrderConfirmedEvent
        event = OrderConfirmedEvent(
            order_id=saved.id,
            store_id=saved.store_id,
            total_price=saved.total_price,
            is_famous=saved.is_famous
        )
        await self.event_publisher.publish_order_confirmed(event)

    async def cancel_order(self, command: CancelOrderCommand) -> None:
        """Cancel the order due to inventory allocation failures"""
        logger.info(f"Cancelling order {command.order_id}. Reason: {command.reason}")
        order = await self.order_repo.find_by_id(command.order_id)
        if not order:
            logger.error(f"Order ID {command.order_id} not found during cancellation.")
            return

        order.cancel(command.reason)
        await self.order_repo.save(order)
        logger.info(f"Order {command.order_id} successfully cancelled in database.")

    async def get_order_by_id(self, order_id: int) -> OrderDTO | None:
        """Fetch details of a single order"""
        order = await self.order_repo.find_by_id(order_id)
        if not order:
            return None
        return OrderDTO.model_validate(order)

    async def get_all_orders(self) -> list[OrderDTO]:
        """Fetch all orders recorded in the database"""
        orders = await self.order_repo.find_all()
        return [OrderDTO.model_validate(o) for o in orders]
