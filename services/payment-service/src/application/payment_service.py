import logging
import asyncio
import uuid
from src.domain.payment import Payment
from src.domain.repository import PaymentRepository
from src.application.commands import ProcessPaymentCommand
from shared.contracts.events import PaymentSucceededEvent, PaymentFailedEvent
from shared.common.http_client import ResilientHTTPClient
from src.infrastructure.config import settings

logger = logging.getLogger("PaymentApplicationService")

class PaymentApplicationService:
    def __init__(self, payment_repo: PaymentRepository, event_publisher):
        self.payment_repo = payment_repo
        self.event_publisher = event_publisher

    async def get_payment_by_order_id(self, order_id: int) -> Payment | None:
        """Fetch payment by order ID"""
        return await self.payment_repo.find_by_order_id(order_id)

    async def get_all_payments(self) -> list[Payment]:
        """Fetch all payments"""
        return await self.payment_repo.find_all()

    async def process_payment(self, command: ProcessPaymentCommand) -> None:
        """Process payment asynchronously using a choreographic Saga step"""
        order_id = command.order_id
        logger.info(f"Processing payment for Order: {order_id}")

        # Check if already processed to avoid concurrent race conditions
        existing = await self.payment_repo.find_by_order_id(order_id)
        if existing:
            logger.warning(f"Payment already exists for Order {order_id} (Status: {existing.status}). Skipping.")
            return

        # 1. Fetch order details from order-service using the ResilientHTTPClient
        client = ResilientHTTPClient(timeout=5.0)
        try:
            url = f"{settings.ORDER_SERVICE_URL}/{order_id}"
            logger.info(f"Querying order-service: GET {url}")
            response = await client.get(url)
            response.raise_for_status()
            order_data = response.json()
        except Exception as http_err:
            logger.error(f"Downstream validation failed: Could not fetch details for Order {order_id}: {http_err}")
            # Dispatch event indicating payment failure due to network/system error
            fail_event = PaymentFailedEvent(
                payment_id=f"pay-fail-{uuid.uuid4().hex[:8]}",
                order_id=order_id,
                amount=0.0,
                reason=f"System error: Unable to resolve order amount from downstream order-service. Error: {http_err}"
            )
            await self.event_publisher.publish_payment_failed(fail_event)
            return
        finally:
            await client.close()

        amount = order_data.get("total_price")
        quantity = order_data.get("quantity", 1)

        if amount is None:
            logger.error(f"Order data retrieved for Order {order_id} contains no total_price: {order_data}")
            fail_event = PaymentFailedEvent(
                payment_id=f"pay-fail-{uuid.uuid4().hex[:8]}",
                order_id=order_id,
                amount=0.0,
                reason="Invalid order: order amount is null or missing."
            )
            await self.event_publisher.publish_payment_failed(fail_event)
            return

        # 2. Instantiate payment aggregate root
        payment = Payment.create(order_id=order_id, amount=amount)

        # 3. Simulate Payment Gateway processing with custom resilience rules
        payment_id = f"pay-{uuid.uuid4().hex[:8]}"

        try:
            # Scenario A: Simulated Timeout for Sagas (e.g. quantity == 7)
            if quantity == 7:
                logger.warning(f"SIMULATION TIMEOUT: Order {order_id} quantity is 7. Inducing 4-second delay to simulate slow gateway.")
                await asyncio.sleep(4.0)
                raise TimeoutError("Credit card gateway connection timed out.")

            # Scenario B: Simulated Rejection for Sagas (e.g. price > $1000)
            if amount > 1000.0:
                logger.warning(f"SIMULATION REJECTION: Order {order_id} amount {amount} exceeds limit. Rejecting payment.")
                raise ValueError(f"Insufficient funds: Transaction amount ${amount} exceeds limit of $1000.")

            # Success Path
            logger.info(f"Payment gateway approved transaction of ${amount} for Order {order_id}")
            payment.succeed(payment_id)
            
            # Save payment record
            await self.payment_repo.save(payment)

            # Publish Succeeded Integration Event
            for event in payment.domain_events:
                if event["event_type"] == "PaymentSucceeded":
                    success_event = PaymentSucceededEvent(
                        payment_id=event["payment_id"],
                        order_id=event["order_id"],
                        amount=event["amount"]
                    )
                    await self.event_publisher.publish_payment_succeeded(success_event)

        except Exception as e:
            logger.warning(f"Payment gateway failed for Order {order_id}: {e}")
            payment.fail(payment_id, reason=str(e))
            
            # Save payment record in failed state
            await self.payment_repo.save(payment)

            # Publish Failed Integration Event (Triggers Saga Compensation)
            for event in payment.domain_events:
                if event["event_type"] == "PaymentFailed":
                    failed_event = PaymentFailedEvent(
                        payment_id=event["payment_id"],
                        order_id=event["order_id"],
                        amount=event["amount"],
                        reason=event["reason"]
                    )
                    await self.event_publisher.publish_payment_failed(failed_event)

        finally:
            payment.clear_events()
