import logging
import uuid
import hashlib
from typing import List, Dict, Any, Optional
from opentelemetry import trace
from qdrant_client import QdrantClient
from qdrant_client.http import models
from src.infrastructure.config import settings
from src.infrastructure.policy_catalog import DEFAULT_MERCHANT_POLICIES
from src.infrastructure.schema_catalog import CLICKHOUSE_TABLE_SCHEMAS

logger = logging.getLogger("QdrantPolicyAdapter")

class QdrantPolicyAdapter:
    """Manages policy vector storage and schema linking vector indexing in Qdrant"""
    def __init__(self, host: str = settings.QDRANT_HOST, port: int = settings.QDRANT_PORT):
        self.host = host
        self.port = port
        self.policy_collection = "merchant_policies"
        self.schema_collection = "clickhouse_schema_catalog"
        self.vector_size = 384
        self._embedder = None
        self._client: Optional[QdrantClient] = None

    @property
    def client(self) -> QdrantClient:
        if self._client is None:
            self._client = QdrantClient(host=self.host, port=self.port, timeout=5.0)
        return self._client

    def _get_embedder(self):
        if self._embedder is None:
            try:
                from fastembed import TextEmbedding
                self._embedder = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
                logger.info("FastEmbed initialized successfully for Policy RAG.")
            except Exception as e:
                logger.warning(f"FastEmbed not loaded ({e}). Using deterministic fallback embedding.")
                self._embedder = None
        return self._embedder

    def embed_text(self, text: str) -> List[float]:
        """Encodes text into a 384-dimensional vector using FastEmbed or fallback"""
        embedder = self._get_embedder()
        if embedder:
            try:
                vectors = list(embedder.embed([text]))
                return vectors[0].tolist()
            except Exception as e:
                logger.warning(f"FastEmbed inference error: {e}. Using deterministic fallback.")
        
        # Deterministic fallback embedding
        hasher = hashlib.sha256(text.encode("utf-8"))
        digest = hasher.digest()
        vector = []
        for i in range(self.vector_size):
            byte_val = digest[i % len(digest)]
            norm_val = (byte_val / 128.0) - 1.0 + (0.01 * (i % 10))
            vector.append(norm_val)
        norm = sum(x * x for x in vector) ** 0.5 or 1.0
        return [x / norm for x in vector]

    def init_collections(self) -> None:
        """Creates policy and schema catalog collections if they don't exist"""
        try:
            existing = [c.name for c in self.client.get_collections().collections]
            
            # 1. Merchant Policies Collection
            if self.policy_collection not in existing:
                self.client.create_collection(
                    collection_name=self.policy_collection,
                    vectors_config=models.VectorParams(
                        size=self.vector_size,
                        distance=models.Distance.COSINE
                    )
                )
                logger.info(f"Created Qdrant collection '{self.policy_collection}'.")
                self.seed_policies()

            # 2. Schema Catalog Collection
            if self.schema_collection not in existing:
                self.client.create_collection(
                    collection_name=self.schema_collection,
                    vectors_config=models.VectorParams(
                        size=self.vector_size,
                        distance=models.Distance.COSINE
                    )
                )
                logger.info(f"Created Qdrant collection '{self.schema_collection}'.")
                self.seed_schema_catalog()

        except Exception as e:
            logger.warning(f"Qdrant collection init notice: {e}")

    def seed_policies(self) -> int:
        """Seeds baseline merchant policies into Qdrant"""
        points = []
        for p in DEFAULT_MERCHANT_POLICIES:
            text = f"Title: {p['title']}. Category: {p['category']}. Content: {p['content']}"
            vec = self.embed_text(text)
            p_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, p["id"]))
            points.append(models.PointStruct(
                id=p_uuid,
                vector=vec,
                payload=p
            ))
        if points:
            try:
                self.client.upsert(collection_name=self.policy_collection, points=points)
                logger.info(f"Successfully seeded {len(points)} merchant policy documents into Qdrant.")
                return len(points)
            except Exception as e:
                logger.warning(f"Could not seed policies: {e}")
        return 0

    def seed_schema_catalog(self) -> int:
        """Seeds ClickHouse DDL schemas for dynamic schema linking"""
        points = []
        for tbl_name, info in CLICKHOUSE_TABLE_SCHEMAS.items():
            text = f"Table: {tbl_name}. Description: {info['description']}. DDL: {info['ddl']}. Queries: {' '.join(info['common_queries'])}"
            vec = self.embed_text(text)
            t_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, tbl_name))
            points.append(models.PointStruct(
                id=t_uuid,
                vector=vec,
                payload={"table_name": tbl_name, "description": info["description"], "ddl": info["ddl"]}
            ))
        if points:
            try:
                self.client.upsert(collection_name=self.schema_collection, points=points)
                logger.info(f"Successfully seeded {len(points)} schema catalog definitions into Qdrant.")
                return len(points)
            except Exception as e:
                logger.warning(f"Could not seed schema catalog: {e}")
        return 0

    def search_policies(self, query: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Retrieves top matching merchant policy documents"""
        tracer = trace.get_tracer("merchant-copilot-service")
        with tracer.start_as_current_span("Qdrant: search_policies") as span:
            span.set_attribute("qdrant.collection", self.policy_collection)
            span.set_attribute("qdrant.query", query)
            span.set_attribute("qdrant.limit", limit)

            vec = self.embed_text(query)
            try:
                if hasattr(self.client, "query_points"):
                    res = self.client.query_points(
                        collection_name=self.policy_collection,
                        query=vec,
                        limit=limit
                    ).points
                else:
                    res = self.client.search(
                        collection_name=self.policy_collection,
                        query_vector=vec,
                        limit=limit
                    )
                policies = []
                for hit in res:
                    payload = hit.payload or {}
                    payload["score"] = float(hit.score) if hasattr(hit, "score") else 0.0
                    policies.append(payload)
                span.set_attribute("qdrant.hits_count", len(policies))
                return policies
            except Exception as e:
                span.record_exception(e)
                logger.warning(f"Policy search failed ({e}). Returning fallback static matches.")
                return DEFAULT_MERCHANT_POLICIES[:limit]

    def link_relevant_schemas(self, query: str, limit: int = 2) -> List[Dict[str, Any]]:
        """Retrieves relevant table DDL definitions for schema linking"""
        tracer = trace.get_tracer("merchant-copilot-service")
        with tracer.start_as_current_span("Qdrant: link_relevant_schemas") as span:
            span.set_attribute("qdrant.collection", self.schema_collection)
            span.set_attribute("qdrant.query", query)
            span.set_attribute("qdrant.limit", limit)

            vec = self.embed_text(query)
            try:
                if hasattr(self.client, "query_points"):
                    res = self.client.query_points(
                        collection_name=self.schema_collection,
                        query=vec,
                        limit=limit
                    ).points
                else:
                    res = self.client.search(
                        collection_name=self.schema_collection,
                        query_vector=vec,
                        limit=limit
                    )
                schemas = []
                for hit in res:
                    payload = hit.payload or {}
                    schemas.append(payload)
                result_schemas = schemas if schemas else list(CLICKHOUSE_TABLE_SCHEMAS.values())[:limit]
                span.set_attribute("qdrant.linked_schemas_count", len(result_schemas))
                return result_schemas
            except Exception as e:
                span.record_exception(e)
                logger.warning(f"Schema linking failed ({e}). Returning default schemas.")
                return list(CLICKHOUSE_TABLE_SCHEMAS.values())[:limit]


policy_adapter = QdrantPolicyAdapter()
