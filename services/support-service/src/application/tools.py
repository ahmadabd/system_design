import logging
from typing import Dict, Any, Optional
from langchain_core.tools import tool
from src.adapter.service_clients import order_client, product_client, user_client

logger = logging.getLogger("SupportTools")

@tool
async def get_order_status(order_id: int) -> Dict[str, Any]:
    """
    Look up real-time status and details for an order by its numeric order ID.
    Returns status (PENDING, PAID, SHIPPED, OUT_FOR_DELIVERY, DELIVERED, CANCELLED),
    total price, product ID, and payment details.
    """
    logger.info(f"[Tool: get_order_status] Looking up order_id={order_id}")
    order_data = await order_client.get_order(order_id)
    if order_data:
        return {
            "found": True,
            "order_id": order_id,
            "status": order_data.get("status"),
            "product_id": order_data.get("product_id"),
            "quantity": order_data.get("quantity"),
            "total_price": order_data.get("total_price"),
            "payment_url": order_data.get("payment_url"),
            "created_at": order_data.get("created_at")
        }
    return {
        "found": False,
        "order_id": order_id,
        "message": f"Order #{order_id} could not be found in our database. Please double-check the order number."
    }

@tool
async def get_product_info(product_id: int) -> Dict[str, Any]:
    """
    Look up information about a specific product in our catalog by its product ID.
    Returns product name, current price, and stock availability.
    """
    logger.info(f"[Tool: get_product_info] Looking up product_id={product_id}")
    product_data = await product_client.get_product(product_id)
    if product_data:
        return {
            "found": True,
            "product_id": product_id,
            "name": product_data.get("name"),
            "price": product_data.get("price"),
            "stock": product_data.get("stock"),
            "in_stock": (product_data.get("stock", 0) > 0)
        }
    return {
        "found": False,
        "product_id": product_id,
        "message": f"Product #{product_id} was not found in the catalog."
    }

@tool
async def get_user_orders(user_id: int) -> Dict[str, Any]:
    """
    Look up all orders placed by a specific customer using their numeric user_id.
    Use this tool when a logged-in user asks "Where is my order?", "Track my package",
    or "Show my order history" without specifying a specific order number.
    """
    logger.info(f"[Tool: get_user_orders] Looking up orders for user_id={user_id}")
    orders = await order_client.list_user_orders(user_id)
    if orders:
        return {
            "found": True,
            "user_id": user_id,
            "total_orders": len(orders),
            "orders": orders
        }
    return {
        "found": False,
        "user_id": user_id,
        "message": f"No orders were found for customer account #{user_id}."
    }

@tool
async def get_user_profile(user_id: int) -> Dict[str, Any]:
    """
    Look up customer account details by numeric user ID.
    """
    logger.info(f"[Tool: get_user_profile] Looking up user_id={user_id}")
    user_data = await user_client.get_user(user_id)
    if user_data:
        return {
            "found": True,
            "user_id": user_id,
            "username": user_data.get("username"),
            "email": user_data.get("email")
        }
    return {
        "found": False,
        "user_id": user_id,
        "message": f"User account #{user_id} not found."
    }

SUPPORT_TOOLS = [get_order_status, get_user_orders, get_product_info, get_user_profile]

