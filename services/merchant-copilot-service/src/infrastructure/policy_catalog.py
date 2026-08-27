from typing import List, Dict, Any

# Unstructured Merchant Policies, SLAs, Warranty Guidelines, and Return Rules
DEFAULT_MERCHANT_POLICIES: List[Dict[str, Any]] = [
    {
        "id": "policy_return_01",
        "title": "Standard 30-Day Customer Return Policy & Refund SLA",
        "category": "Returns & Refunds",
        "content": (
            "Customers may initiate a return for any unopened or gently inspected item within 30 days of delivery. "
            "Upon receiving the returned item at the fulfillment center, merchant inspection must be completed within 2 business days. "
            "Refunds are processed to the original payment method via Stripe within 3 to 5 banking days. "
            "Return shipping is free for damaged or defective items; customer pays standard shipping for change-of-mind returns."
        )
    },
    {
        "id": "policy_damaged_02",
        "title": "Damaged In Transit & DOA (Dead on Arrival) Equipment SLA",
        "category": "Damaged Goods & RMA",
        "content": (
            "If an item arrives damaged or non-functional (DOA), the customer must report it within 7 days of delivery. "
            "Merchant must issue an immediate prepaid return label and dispatch a replacement unit within 24 hours of scan confirmation. "
            "Claims above $500 require carrier inspection documentation and RMA authorization."
        )
    },
    {
        "id": "policy_warranty_03",
        "title": "Electronics & Hardware Warranty Guidelines",
        "category": "Warranty & Repairs",
        "content": (
            "All hardware products (laptops, studio microphones, audio interfaces, monitors, mechanical keyboards) "
            "include a 1-year comprehensive manufacturer warranty covering internal component defects, power supplies, and switch failures. "
            "Warranty does not cover accidental liquid spills, drops, or unauthorized user modifications. "
            "Extended 2-year merchant protection plans provide zero-deductible replacement."
        )
    },
    {
        "id": "policy_shipping_04",
        "title": "Merchant Shipping Tiers, Cutoff Times & Dispatch SLA",
        "category": "Shipping & Logistics",
        "content": (
            "Standard Ground Shipping (3-5 business days) is complimentary on all orders exceeding $75. "
            "Express 2-Day and Next-Day Air shipping are available at checkout. "
            "Orders placed before 2:00 PM EST Monday through Friday must be fulfilled and handed over to the courier on the same day. "
            "Late dispatches breach the merchant SLA and trigger automatic carrier fee credits."
        )
    },
    {
        "id": "policy_commission_05",
        "title": "Platform Merchant Fee & Commission Schedule",
        "category": "Merchant Operations & Finance",
        "content": (
            "Platform transactions incur a flat 2.9% + $0.30 payment processing fee plus a 5% marketplace commission on completed orders. "
            "Merchant revenue payouts are settled every Wednesday for the previous week's confirmed orders. "
            "Disputed or chargeback transactions incur a $15 administrative fee unless covered by Seller Protection."
        )
    }
]
