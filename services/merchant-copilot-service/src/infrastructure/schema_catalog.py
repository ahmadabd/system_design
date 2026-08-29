from typing import Dict, List, Any

# ClickHouse Analytical Schema DDLs and Semantic Metadata for Schema Linking
CLICKHOUSE_TABLE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "products_analytics": {
        "table_name": "products_analytics",
        "description": "Stores catalog products, inventory stock counts, prices, categories, and store associations.",
        "ddl": """
CREATE TABLE copilot_analytics.products_analytics (
    id UInt64,                          -- Unique product identifier
    tenant_id LowCardinality(String),   -- Store tenant slug (e.g. 'store_tech', 'store_gaming')
    name String,                        -- Full product name
    category LowCardinality(String),    -- Category (e.g. 'Electronics', 'Laptops', 'Microphones', 'Keyboards')
    price Float64,                      -- Unit retail price
    stock UInt32,                       -- Current inventory available in stock
    store_id UInt32,                    -- Store identifier
    updated_at DateTime,                -- Last updated timestamp
    INDEX idx_store_id (store_id) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (tenant_id, id);
        """.strip(),
        "common_queries": [
            "Top products by stock: SELECT id, name, price, stock FROM copilot_analytics.products_analytics WHERE tenant_id = 'store_tech' ORDER BY stock DESC LIMIT 5",
            "Average price per category: SELECT category, count() AS total_items, avg(price) AS avg_price FROM copilot_analytics.products_analytics WHERE tenant_id = 'store_tech' GROUP BY category"
        ]
    },
    "orders_analytics": {
        "table_name": "orders_analytics",
        "description": "Stores customer orders, total order revenue amounts, order status, and creation timestamps.",
        "ddl": """
CREATE TABLE copilot_analytics.orders_analytics (
    id UInt64,                          -- Order identifier
    tenant_id LowCardinality(String),   -- Store tenant slug
    user_id UInt64,                     -- Customer user ID
    total_amount Float64,               -- Total order monetary amount in USD
    status LowCardinality(String),      -- Order status ('PENDING', 'CONFIRMED', 'CANCELLED', 'FAILED')
    created_at DateTime,                -- Order creation timestamp
    INDEX idx_user_id (user_id) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(created_at) ORDER BY (tenant_id, created_at, id);
        """.strip(),
        "common_queries": [
            "Total revenue: SELECT sum(total_amount) AS revenue, count() AS total_orders FROM copilot_analytics.orders_analytics WHERE tenant_id = 'store_tech' AND status = 'CONFIRMED'",
            "Orders by status: SELECT status, count() AS count, sum(total_amount) AS total FROM copilot_analytics.orders_analytics WHERE tenant_id = 'store_tech' GROUP BY status"
        ]
    },
    "order_items_analytics": {
        "table_name": "order_items_analytics",
        "description": "Line items per order, recording product IDs, names, categories, quantities sold, and unit prices.",
        "ddl": """
CREATE TABLE copilot_analytics.order_items_analytics (
    id UInt64,                          -- Order item ID
    order_id UInt64,                    -- Associated order ID
    tenant_id LowCardinality(String),   -- Store tenant slug
    product_id UInt64,                  -- Associated product ID
    product_name String,                -- Product title
    category LowCardinality(String),    -- Product category
    unit_price Float64,                 -- Price charged per unit
    quantity UInt32,                    -- Quantity purchased
    created_at DateTime,                -- Item creation timestamp
    INDEX idx_product_id (product_id) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(created_at) ORDER BY (tenant_id, order_id, product_id);
        """.strip(),
        "common_queries": [
            "Best-selling products: SELECT product_name, sum(quantity) AS units_sold, sum(unit_price * quantity) AS total_sales FROM copilot_analytics.order_items_analytics WHERE tenant_id = 'store_tech' GROUP BY product_name ORDER BY units_sold DESC LIMIT 5"
        ]
    },
    "payments_analytics": {
        "table_name": "payments_analytics",
        "description": "Payment transactions, payment methods (Stripe, Card, Wallet), transaction statuses, and amounts.",
        "ddl": """
CREATE TABLE copilot_analytics.payments_analytics (
    id UInt64,                          -- Payment identifier
    order_id UInt64,                    -- Associated order ID
    tenant_id LowCardinality(String),   -- Store tenant slug
    amount Float64,                     -- Amount processed in USD
    status LowCardinality(String),      -- Payment status ('SUCCEEDED', 'FAILED', 'REFUNDED')
    payment_method LowCardinality(String), -- Payment gateway / method ('STRIPE', 'CARD', 'WALLET')
    transaction_id String,              -- External gateway reference ID
    created_at DateTime                 -- Transaction timestamp
) ENGINE = ReplacingMergeTree(created_at) ORDER BY (tenant_id, order_id, id);
        """.strip(),
        "common_queries": [
            "Payment method breakdown: SELECT payment_method, status, count() AS transactions, sum(amount) AS total_volume FROM copilot_analytics.payments_analytics WHERE tenant_id = 'store_tech' GROUP BY payment_method, status"
        ]
    }
}
