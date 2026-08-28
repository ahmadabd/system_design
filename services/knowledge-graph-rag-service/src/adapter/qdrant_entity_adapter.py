import logging
import uuid
import hashlib
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.http import models
from opentelemetry import trace

from src.infrastructure.config import settings
from src.domain.graph_entities import GraphNode

from shared.common.resilience import AsyncCircuitBreaker, CircuitBreakerOpenException

logger = logging.getLogger("QdrantEntityAdapter")
tracer = trace.get_tracer("knowledge-graph-rag-service")


class QdrantEntityAdapter:
    """Manages vector embeddings for Knowledge Graph Entities to enable semantic entity linking"""
    def __init__(self, host: str = settings.QDRANT_HOST, port: int = settings.QDRANT_PORT):
        self.host = host
        self.port = port
        self.collection_name = "knowledge_graph_entities"
        self.vector_size = 384
        self._embedder = None
        self._client: Optional[QdrantClient] = None
        self.breaker = AsyncCircuitBreaker(
            name="qdrant-graph-breaker",
            failure_threshold=3,
            recovery_timeout=10.0
        )

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
                logger.info("FastEmbed initialized successfully for Entity Vector Linking.")
            except Exception as e:
                logger.warning(f"FastEmbed not loaded ({e}). Using deterministic fallback embedding.")
                self._embedder = None
        return self._embedder

    def embed_text(self, text: str) -> List[float]:
        """Encodes text into a 384-dimensional vector using FastEmbed or deterministic fallback"""
        embedder = self._get_embedder()
        if embedder:
            try:
                vecs = list(embedder.embed([text]))
                return vecs[0].tolist()
            except Exception as e:
                logger.warning(f"Embedding error ({e}). Using fallback.")

        # Deterministic 384-dim pseudo-vector fallback
        hash_digest = hashlib.sha256(text.encode("utf-8")).digest()
        vec = []
        for i in range(self.vector_size):
            byte_val = hash_digest[i % len(hash_digest)]
            norm_val = (byte_val / 128.0) - 1.0
            vec.append(round(norm_val, 6))
        return vec

    def init_collection(self) -> None:
        """Ensures Qdrant collection exists with cosine similarity"""
        try:
            existing = [c.name for c in self.client.get_collections().collections]
            if self.collection_name not in existing:
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(
                        size=self.vector_size,
                        distance=models.Distance.COSINE
                    )
                )
                logger.info(f"Created Qdrant collection '{self.collection_name}'.")
        except Exception as e:
            logger.warning(f"Qdrant collection init notice: {e}")

    def index_entities(self, nodes: List[GraphNode]) -> int:
        """Vectorizes and upserts entity descriptions into Qdrant for semantic linking"""
        if not nodes:
            return 0
        points = []
        for node in nodes:
            text = f"Entity: {node.name}. Type: {node.type.value if hasattr(node.type, 'value') else node.type}. Description: {node.description}. Properties: {node.properties}"
            vec = self.embed_text(text)
            p_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(node.id)))
            points.append(models.PointStruct(
                id=p_uuid,
                vector=vec,
                payload={
                    "node_id": node.id,
                    "name": node.name,
                    "type": node.type.value if hasattr(node.type, 'value') else str(node.type),
                    "description": node.description,
                    "tenant_id": node.tenant_id,
                    "properties": node.properties
                }
            ))

        try:
            self.client.upsert(collection_name=self.collection_name, points=points)
            logger.info(f"Successfully indexed {len(points)} entity nodes in Qdrant vector store.")
            return len(points)
        except Exception as e:
            logger.warning(f"Could not index entities in Qdrant: {e}")
            return 0

    def search_entities(self, query: str, limit: int = 4, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Finds the most semantically relevant seed entities for a natural language question"""
        tracer = trace.get_tracer("knowledge-graph-rag-service")
        with tracer.start_as_current_span("Qdrant: search_entities") as span:
            span.set_attribute("qdrant.query", query)
            span.set_attribute("qdrant.limit", limit)

            if self.breaker.state == "OPEN":
                logger.warning("[CircuitBreaker: qdrant-graph-breaker is OPEN] Fast-failing to keyword search.")
                return []

            vec = self.embed_text(query)
            try:
                filter_condition = None
                if tenant_id:
                    filter_condition = models.Filter(
                        must=[
                            models.FieldCondition(
                                key="tenant_id",
                                match=models.MatchValue(value=tenant_id)
                            )
                        ]
                    )

                if hasattr(self.client, "query_points"):
                    res = self.client.query_points(
                        collection_name=self.collection_name,
                        query=vec,
                        query_filter=filter_condition,
                        limit=limit
                    ).points
                else:
                    res = self.client.search(
                        collection_name=self.collection_name,
                        query_vector=vec,
                        query_filter=filter_condition,
                        limit=limit
                    )

                entities = []
                for hit in res:
                    payload = hit.payload or {}
                    payload["score"] = float(hit.score) if hasattr(hit, "score") else 0.0
                    entities.append(payload)

                span.set_attribute("qdrant.matched_entities_count", len(entities))
                return entities
            except Exception as e:
                span.record_exception(e)
                logger.warning(f"Entity search failed ({e}). Returning fallback matches.")
                return []


qdrant_entity_adapter = QdrantEntityAdapter()
