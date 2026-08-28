from prometheus_client import Counter, Histogram, Gauge

dispute_claims_total = Counter(
    "dispute_claims_total",
    "Total dispute claims processed by the multi-agent resolution system",
    ["reason", "outcome", "tenant_id"]
)

dispute_auto_settled_total = Counter(
    "dispute_auto_settled_total",
    "Total dispute claims resolved automatically without human escalation",
    ["outcome", "tenant_id"]
)

dispute_human_escalations_total = Counter(
    "dispute_human_escalations_total",
    "Total dispute claims escalated to human compliance officers",
    ["reason", "tenant_id"]
)

dispute_settlement_duration_seconds = Histogram(
    "dispute_settlement_duration_seconds",
    "Time taken for the multi-agent debate and judicial arbitration to settle a claim",
    ["outcome"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

dispute_refund_amount_dollars = Counter(
    "dispute_refund_amount_dollars_total",
    "Total dollar volume of refunds disbursed to buyers through dispute resolution",
    ["tenant_id"]
)
