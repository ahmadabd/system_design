from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from src.infrastructure.db_setup import db
from src.infrastructure.config import settings
from src.adapter.repository import SQLAlchemyPaymentRepository
from src.adapter.messaging_pub import PaymentMessagingPublisher
from src.application.payment_service import PaymentApplicationService
from shared.common.messaging import KafkaManager
from shared.common.resilience import CircuitBreakerOpenException
from shared.common.idempotency import IdempotencyManager
from shared.common.cache import cache_fallback
from pydantic import BaseModel

from fastapi.responses import HTMLResponse

router = APIRouter(prefix="", tags=["Payments"])

# Establish broker manager for outbound events
mq_manager = KafkaManager(settings.KAFKA_BOOTSTRAP_SERVERS)

# Establish Redis Idempotency/Cache Manager
idempotency_manager = IdempotencyManager(settings.REDIS_URL)

class PaymentDTO(BaseModel):
    id: str | None
    order_id: int
    amount: float
    status: str
    checkout_url: str | None = None

    class Config:
        from_attributes = True

class CompleteStripePaymentRequest(BaseModel):
    success: bool

async def get_payment_service(
    session: AsyncSession = Depends(db.get_session)
) -> PaymentApplicationService:
    """Dependency injector mapping persistence adapters to core use cases"""
    repo = SQLAlchemyPaymentRepository(session)
    publisher = PaymentMessagingPublisher(session)
    return PaymentApplicationService(repo, publisher)

@router.get("/", response_model=list[PaymentDTO])
async def list_payments(
    service: PaymentApplicationService = Depends(get_payment_service)
):
    """REST endpoint to retrieve all platform payments"""
    try:
        payments = await service.get_all_payments()
        return payments
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}. Read operations degraded."
        )

@router.get("/{order_id:int}", response_model=PaymentDTO)
@cache_fallback(idempotency_manager, db.db_breaker, key_prefix="payment", id_param="order_id")
async def get_payment_by_order_id(
    order_id: int,
    request: Request,
    service: PaymentApplicationService = Depends(get_payment_service)
):
    """REST endpoint to fetch payment details by order ID reference"""
    try:
        payment = await service.get_payment_by_order_id(order_id)
        if not payment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Payment record for Order {order_id} not found"
            )
        return payment
    except CircuitBreakerOpenException as cb_err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database circuit breaker active: {str(cb_err)}."
        )

@router.get("/stripe-checkout/{order_id:int}", response_class=HTMLResponse)
async def stripe_checkout_page(
    order_id: int,
    service: PaymentApplicationService = Depends(get_payment_service)
):
    """Serves a modern, interactive HTML Stripe Checkout Simulator page"""
    payment = await service.get_payment_by_order_id(order_id)
    if not payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Payment session for Order {order_id} not found"
        )
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Stripe Checkout Simulator</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
        <style>
            body {{
                font-family: 'Inter', sans-serif;
                background: linear-gradient(135deg, #0f172a, #1e293b);
                color: #f8fafc;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                margin: 0;
            }}
            .card {{
                background: rgba(30, 41, 59, 0.7);
                backdrop-filter: blur(12px);
                border: 1px solid rgba(255, 255, 255, 0.1);
                padding: 2.5rem;
                border-radius: 1.5rem;
                box-shadow: 0 20px 25px -5px rgb(0 0 0 / 0.5), 0 8px 10px -6px rgb(0 0 0 / 0.5);
                max-width: 400px;
                width: 100%;
                text-align: center;
            }}
            h1 {{
                font-size: 1.75rem;
                font-weight: 800;
                margin-bottom: 0.5rem;
                background: linear-gradient(to right, #38bdf8, #818cf8);
                -webkit-background-clip: text;
                -webkit-background-clip: text;
                color: transparent;
                background-clip: text;
            }}
            .subtitle {{
                color: #94a3b8;
                font-size: 0.875rem;
                margin-bottom: 2rem;
            }}
            .details {{
                background: rgba(15, 23, 42, 0.5);
                border-radius: 1rem;
                padding: 1.5rem;
                margin-bottom: 2rem;
                border: 1px solid rgba(255, 255, 255, 0.05);
                text-align: left;
            }}
            .detail-row {{
                display: flex;
                justify-content: space-between;
                margin-bottom: 0.75rem;
            }}
            .detail-row:last-child {{
                margin-bottom: 0;
                padding-top: 0.75rem;
                border-top: 1px solid rgba(255, 255, 255, 0.1);
                font-weight: 600;
            }}
            .label {{
                color: #64748b;
            }}
            .val {{
                color: #e2e8f0;
            }}
            .val.amount {{
                color: #38bdf8;
                font-size: 1.125rem;
            }}
            .btn {{
                display: block;
                width: 100%;
                padding: 1rem;
                border-radius: 0.75rem;
                font-weight: 600;
                font-size: 1rem;
                border: none;
                cursor: pointer;
                transition: all 0.2s ease;
                margin-bottom: 1rem;
            }}
            .btn-pay {{
                background: linear-gradient(135deg, #6366f1, #4f46e5);
                color: white;
                box-shadow: 0 4px 14px 0 rgba(99, 102, 241, 0.4);
            }}
            .btn-pay:hover {{
                transform: translateY(-2px);
                box-shadow: 0 6px 20px 0 rgba(99, 102, 241, 0.6);
            }}
            .btn-cancel {{
                background: rgba(255, 255, 255, 0.05);
                color: #94a3b8;
                border: 1px solid rgba(255, 255, 255, 0.1);
            }}
            .btn-cancel:hover {{
                background: rgba(255, 255, 255, 0.1);
                color: #e2e8f0;
            }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>Stripe Checkout</h1>
            <p class="subtitle">Simulating Stripe Redirect Gateway</p>
            
            <div class="details">
                <div class="detail-row">
                    <span class="label">Order Reference</span>
                    <span class="val">#{order_id}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Status</span>
                    <span class="val" style="color: #fbbf24;">{payment.status}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Total Amount</span>
                    <span class="val amount">${payment.amount:.2f}</span>
                </div>
            </div>

            <button class="btn btn-pay" onclick="completePayment(true)">Simulate Success Payment</button>
            <button class="btn btn-cancel" onclick="completePayment(false)">Cancel Checkout</button>
        </div>

        <script>
            async function completePayment(success) {{
                try {{
                    const response = await fetch('/payments/{order_id}/stripe-complete', {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json'
                        }},
                        body: JSON.stringify({{ success: success }})
                    }});
                    
                    if (response.ok) {{
                        alert(success ? 'Payment completed successfully!' : 'Payment was cancelled.');
                        window.location.reload();
                    }} else {{
                        alert('Error completing simulated payment.');
                    }}
                }} catch (err) {{
                    console.error(err);
                    alert('Connection error.');
                }}
            }}
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@router.post("/{order_id:int}/stripe-complete")
async def complete_stripe_payment_endpoint(
    order_id: int,
    request_data: CompleteStripePaymentRequest,
    service: PaymentApplicationService = Depends(get_payment_service)
):
    """REST endpoint to simulate stripe webhook completion callback"""
    try:
        await service.complete_stripe_payment(order_id, request_data.success)
        return {"status": "processed", "order_id": order_id, "success": request_data.success}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error finalizing stripe payment: {str(e)}"
        )
