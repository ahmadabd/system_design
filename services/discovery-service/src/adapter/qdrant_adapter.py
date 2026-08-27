import logging
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.http import models
from src.infrastructure.config import settings
from src.domain.schemas import ProductItemDTO

logger = logging.getLogger("QdrantDiscoveryAdapter")


class QdrantDiscoveryAdapter:
    """
    Manages vector indexing and payload-filtered dense semantic search in Qdrant.
    Combines dense embeddings with hard metadata filters (price <= budget, in_stock = True, tenant_id).
    """
    def __init__(self, host: str = settings.QDRANT_HOST, port: int = settings.QDRANT_PORT):
        self.client = QdrantClient(host=host, port=port, timeout=5.0)
        self.collection_name = settings.QDRANT_COLLECTION_NAME
        self.vector_dim = 384  # Standard all-MiniLM-L6-v2 vector dimension
        self._embedder = None

    def _get_embedder(self):
        """Lazy load fastembed or sentence transformer embedder"""
        if self._embedder is None:
            try:
                from fastembed import TextEmbedding
                self._embedder = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
            except Exception:
                try:
                    from sentence_transformers import SentenceTransformer
                    self._embedder = SentenceTransformer(settings.EMBEDDING_MODEL)
                except Exception as e:
                    logger.debug(f"Using deterministic embedding fallback ({e}).")
                    self._embedder = "mock"
        return self._embedder

    def embed_text(self, text: str) -> List[float]:
        """Generates 384-dimensional dense embedding vector for text"""
        embedder = self._get_embedder()
        if embedder == "mock":
            # Deterministic hash-based 384-d normalized vector for offline/testing resilience
            import hashlib
            import math
            h = hashlib.sha256(text.encode("utf-8")).digest()
            vec = [float(b) / 255.0 for b in h]
            while len(vec) < self.vector_dim:
                vec.extend(vec[:self.vector_dim - len(vec)])
            vec = vec[:self.vector_dim]
            norm = math.sqrt(sum(x * x for x in vec)) or 1.0
            return [x / norm for x in vec]
        else:
            try:
                if hasattr(embedder, "embed"):
                    embeddings = list(embedder.embed([text]))
                    return embeddings[0].tolist()
                return embedder.encode(text).tolist()
            except Exception as e:
                logger.debug(f"Embedder failed ({e}). Using deterministic fallback.")
                import hashlib
                import math
                h = hashlib.sha256(text.encode("utf-8")).digest()
                vec = [float(b) / 255.0 for b in h]
                while len(vec) < self.vector_dim:
                    vec.extend(vec[:self.vector_dim - len(vec)])
                vec = vec[:self.vector_dim]
                norm = math.sqrt(sum(x * x for x in vec)) or 1.0
                return [x / norm for x in vec]

    def init_collection(self) -> None:
        """Ensures Qdrant collection and payload indexes exist"""
        try:
            collections = self.client.get_collections().collections
            exists = any(c.name == self.collection_name for c in collections)
            if not exists:
                logger.info(f"Creating Qdrant discovery collection '{self.collection_name}'...")
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(
                        size=self.vector_dim,
                        distance=models.Distance.COSINE
                    )
                )
                # Create payload index for price and tenant filtering
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="price",
                    field_schema=models.PayloadSchemaType.FLOAT
                )
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="tenant_id",
                    field_schema=models.PayloadSchemaType.KEYWORD
                )
                logger.info(f"Collection '{self.collection_name}' initialized with payload indexes.")
        except Exception as e:
            logger.warning(f"Qdrant collection check encountered error: {e}")

    def index_products(self, products: List[Dict[str, Any]], tenant_id: str = "store_tech") -> int:
        """Indexes a batch of product records with rich descriptive text and metadata payloads"""
        self.init_collection()
        points = []
        import uuid
        for p in products:
            desc = f"Product: {p.get('name')}. Category: {p.get('category', 'Electronics')}. Price: ${p.get('price')}. Specs: {p.get('specs', 'High quality durable hardware')}"
            vec = self.embed_text(desc)
            pid = p.get("id")
            # Generate deterministic UUID per tenant and product ID to avoid multi-tenant collision
            point_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{tenant_id}_{pid}"))
            payload = {
                "id": pid,
                "name": p.get("name"),
                "price": float(p.get("price", 0.0)),
                "stock": int(p.get("stock", 0)),
                "store_id": int(p.get("store_id", 1)),
                "category": p.get("category", "Electronics"),
                "specs": p.get("specs", ""),
                "tenant_id": tenant_id,
                "text": desc
            }
            points.append(models.PointStruct(id=point_uuid, vector=vec, payload=payload))

        if points:
            try:
                self.client.upsert(collection_name=self.collection_name, points=points)
                logger.info(f"Successfully upserted {len(points)} products into Qdrant for tenant '{tenant_id}'.")
            except Exception as e:
                logger.warning(f"Could not upsert products to Qdrant ({e}). Continuing in resilient mode.")
        return len(points)

    def delete_product(self, product_id: int, tenant_id: str = "store_tech") -> bool:
        """Deletes a product point from Qdrant vector index upon deletion event."""
        import uuid
        try:
            point_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{tenant_id}_{product_id}"))
            self.client.delete(
                collection_name=self.collection_name,
                points_selector=models.PointIdsList(points=[point_uuid])
            )
            logger.info(f"Successfully deleted product point {product_id} (UUID {point_uuid}) from Qdrant.")
            return True
        except Exception as e:
            logger.warning(f"Could not delete product {product_id} from Qdrant: {e}")
            return False

    def search_products(
        self,
        query: str,
        tenant_id: str = "store_tech",
        max_price: Optional[float] = None,
        in_stock_only: bool = True,
        limit: int = 5
    ) -> List[ProductItemDTO]:
        """
        Executes dense vector similarity search constrained by Qdrant metadata filters.
        Filters by: tenant_id, price <= max_price, stock >= 1
        """
        query_vec = self.embed_text(query)
        must_filters = [
            models.FieldCondition(
                key="tenant_id",
                match=models.MatchValue(value=tenant_id)
            )
        ]
        if max_price is not None and max_price > 0:
            must_filters.append(
                models.FieldCondition(
                    key="price",
                    range=models.Range(lte=max_price)
                )
            )
        if in_stock_only:
            must_filters.append(
                models.FieldCondition(
                    key="stock",
                    range=models.Range(gte=1)
                )
            )

        qdrant_filter = models.Filter(must=must_filters)

        try:
            if hasattr(self.client, "query_points"):
                response = self.client.query_points(
                    collection_name=self.collection_name,
                    query=query_vec,
                    query_filter=qdrant_filter,
                    limit=limit
                )
                results = response.points
            elif hasattr(self.client, "search"):
                results = self.client.search(
                    collection_name=self.collection_name,
                    query_vector=query_vec,
                    query_filter=qdrant_filter,
                    limit=limit
                )
            else:
                results = []
            items = []
            for hit in results:
                p = hit.payload or {}
                score = getattr(hit, "score", 1.0)
                items.append(ProductItemDTO(
                    id=p.get("id"),
                    name=p.get("name"),
                    price=p.get("price"),
                    stock=p.get("stock"),
                    store_id=p.get("store_id"),
                    category=p.get("category", "Electronics"),
                    specs=p.get("specs"),
                    similarity_score=float(score)
                ))
            return items
        except Exception as e:
            logger.error(f"Error during Qdrant search: {e}")
            return []
