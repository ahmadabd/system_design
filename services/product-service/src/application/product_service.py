import logging
from src.domain.product import Product
from src.domain.repository import ProductRepository
from src.application.commands import CreateProductCommand, ReserveInventoryCommand
from src.application.dtos import ProductDTO
from shared.contracts.events import InventoryReservedEvent, InventoryFailedEvent

logger = logging.getLogger("ProductApplicationService")

class ProductApplicationService:
    def __init__(self, product_repo: ProductRepository, event_publisher):
        self.product_repo = product_repo
        self.event_publisher = event_publisher

    async def create_product(self, command: CreateProductCommand) -> ProductDTO:
        """Register a new catalog product with initial stock"""
        logger.info(f"Creating catalog product: {command.name}")
        product = Product.create(
            name=command.name,
            price=command.price,
            stock=command.stock
        )
        saved = await self.product_repo.save(product)
        return ProductDTO.model_validate(saved)

    async def get_product_by_id(self, product_id: int) -> ProductDTO | None:
        """Fetch details of a single product"""
        p = await self.product_repo.find_by_id(product_id)
        if not p:
            return None
        return ProductDTO.model_validate(p)

    async def get_all_products(self) -> list[ProductDTO]:
        """Fetch all products in the catalog"""
        products = await self.product_repo.find_all()
        return [ProductDTO.model_validate(p) for p in products]

    async def reserve_stock(self, command: ReserveInventoryCommand) -> None:
        """Reserve inventory for a customer's order. Publishes success or failure event."""
        logger.info(f"Attempting inventory reservation: Order {command.order_id}, Product {command.product_id}")
        
        product = await self.product_repo.find_by_id(command.product_id)
        if not product:
            logger.error(f"Catalog product ID {command.product_id} not found during reservation.")
            # Dispatch event indicating reservation failure
            fail_event = InventoryFailedEvent(
                order_id=command.order_id,
                product_id=command.product_id,
                reason=f"Product with ID {command.product_id} does not exist in inventory catalog."
            )
            await self.event_publisher.publish_inventory_failed(fail_event)
            return

        try:
            # Reserve stock (mutates aggregate state & records domain event)
            product.reserve_stock(command.quantity, command.order_id)
            
            # Save state changes
            await self.product_repo.save(product)
            logger.info(f"Inventory reserved successfully in database for Order: {command.order_id}")

            # Publish integration success event
            for event in product.domain_events:
                if event["event_type"] == "InventoryReserved":
                    success_event = InventoryReservedEvent(
                        order_id=event["order_id"],
                        product_id=event["product_id"],
                        quantity=event["quantity"]
                    )
                    await self.event_publisher.publish_inventory_reserved(success_event)

        except ValueError as e:
            logger.warning(f"Inventory reservation validation failed for Order {command.order_id}: {e}")
            # Publish integration failure event
            for event in product.domain_events:
                if event["event_type"] == "InventoryFailed":
                    failed_event = InventoryFailedEvent(
                        order_id=event["order_id"],
                        product_id=event["product_id"],
                        reason=event["reason"]
                    )
                    await self.event_publisher.publish_inventory_failed(failed_event)
        finally:
            product.clear_events()
