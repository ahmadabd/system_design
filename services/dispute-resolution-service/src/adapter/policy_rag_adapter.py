import logging
from typing import List, Dict, Any
from opentelemetry import trace
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from src.infrastructure.config import settings

logger = logging.getLogger("PolicyRAGAdapter")
tracer = trace.get_tracer("dispute-resolution-service")

# Standard statutory consumer laws and platform dispute guidelines
DEFAULT_DISPUTE_POLICIES = [
    {
        "id": "pol_statutory_defect",
        "title": "Statutory Manufacturer Defect Clause (Section 4.1)",
        "content": "If a product fails due to an inherent manufacturing defect or known component batch failure, the customer is legally entitled to a 100% full refund or free replacement regardless of whether the standard 14-day merchant return window has expired. The merchant shall not bear financial liability if the root defect is attributable to a Tier-1/Tier-2 supplier.",
        "category": "DEFECTIVE_PRODUCT",
        "priority": "STATUTORY_MANDATE"
    },
    {
        "id": "pol_return_window",
        "title": "Standard 14-Day Return Window (Section 2.3)",
        "content": "For discretionary returns (e.g. buyer remorse or change of mind), the return must be initiated within 14 calendar days of delivery. Items returned after 14 days without verifiable defects are subject to claim denial or a 20% restocking fee at merchant discretion.",
        "category": "BUYER_REMORSE",
        "priority": "MERCHANT_PROTECTION"
    },
    {
        "id": "pol_transit_damage",
        "title": "Carrier Transit Damage & Lost Parcel Clause (Section 5.0)",
        "content": "If an item arrives physically damaged in transit or the courier tracking fails to confirm delivery, the customer is entitled to an immediate replacement or full refund. The platform shall file a carrier insurance subrogation claim against the logistics carrier.",
        "category": "TRANSIT_DAMAGE",
        "priority": "CARRIER_SUBROGATION"
    },
    {
        "id": "pol_unauthorized_fraud",
        "title": "Unauthorized Transaction & Account Takeover (Section 8.2)",
        "content": "Claims of unauthorized purchases require verification of IP address, 2FA logs, and shipping address consistency. If fraud is confirmed, a full reversal is issued and the offending account is quarantined.",
        "category": "UNAUTHORIZED_TRANSACTION",
        "priority": "FRAUD_MITIGATION"
    },
    {
        "id": "pol_wrong_item",
        "title": "Wrong SKU / Mislabeled Item Shipped (Section 3.4)",
        "content": "If warehouse barcode scanning records show an incorrect SKU was dispatched, the merchant must cover return shipping and expedite the correct item or issue a full refund immediately.",
        "category": "WRONG_ITEM",
        "priority": "WAREHOUSE_ERROR"
    }
]


class PolicyRAGAdapter:
    """
    Self-RAG Policy Retrieval Adapter using Qdrant vector similarity
    with in-memory keyword and cosine fallback.
    """
    def __init__(self):
        self.qdrant_client = None
        self.embedding_model = None
        self.collection_name = settings.POLICY_COLLECTION
        self._init_client()

    def _init_client(self):
        try:
            self.qdrant_client = QdrantClient(host=settings.QDRANT_HOST, port=settings.QDRANT_PORT, timeout=3.0)
            from fastembed import TextEmbedding
            self.embedding_model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
            self._ensure_collection_and_index()
            logger.info("PolicyRAGAdapter initialized with Qdrant vector index.")
        except Exception as e:
            logger.warning(f"Could not connect to Qdrant for policy RAG ({e}). Operating in memory heuristic mode.")

    def _ensure_collection_and_index(self):
        if not self.qdrant_client or not self.embedding_model:
            return
        try:
            collections = self.qdrant_client.get_collections().collections
            exists = any(c.name == self.collection_name for c in collections)
            if not exists:
                self.qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=qmodels.VectorParams(size=384, distance=qmodels.Distance.COSINE)
                )
            
            # Seed policies into Qdrant
            texts = [f"{p['title']}: {p['content']}" for p in DEFAULT_DISPUTE_POLICIES]
            embeddings = list(self.embedding_model.embed(texts))
            points = []
            for idx, (p, emb) in enumerate(zip(DEFAULT_DISPUTE_POLICIES, embeddings)):
                points.append(qmodels.PointStruct(
                    id=idx + 1,
                    vector=emb.tolist(),
                    payload=p
                ))
            self.qdrant_client.upsert(collection_name=self.collection_name, points=points)
            logger.info(f"Seeded {len(points)} dispute resolution policies into Qdrant.")
        except Exception as e:
            logger.warning(f"Failed to upsert policies into Qdrant: {e}")

    def retrieve_relevant_policies(self, query: str, reason: str, top_k: int = 2) -> List[Dict[str, Any]]:
        """Retrieves legally grounded policy chunks relevant to the dispute"""
        with tracer.start_as_current_span("PolicyRAG: retrieve_relevant_policies") as span:
            span.set_attribute("dispute.reason", reason)
            span.set_attribute("dispute.query", query)

            # 1. Try Qdrant semantic vector search
            if self.qdrant_client and self.embedding_model:
                try:
                    query_emb = list(self.embedding_model.embed([f"{reason} {query}"]))[0].tolist()
                    hits = self.qdrant_client.search(
                        collection_name=self.collection_name,
                        query_vector=query_emb,
                        limit=top_k
                    )
                    results = [hit.payload for hit in hits if hit.payload]
                    if results:
                        span.set_attribute("rag.retrieval_source", "qdrant_vector")
                        return results
                except Exception as q_err:
                    logger.warning(f"Qdrant policy search fallback: {q_err}")

            # 2. Heuristic exact category matching fallback
            span.set_attribute("rag.retrieval_source", "memory_fallback")
            matched = [p for p in DEFAULT_DISPUTE_POLICIES if p["category"] == reason]
            if not matched:
                matched = DEFAULT_DISPUTE_POLICIES[:top_k]
            return matched[:top_k]


policy_rag_adapter = PolicyRAGAdapter()
