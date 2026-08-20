import logging
from typing import Optional
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
from langchain_qdrant import QdrantVectorStore
from src.infrastructure.config import settings

logger = logging.getLogger("QdrantSetup")

class QdrantManager:
    """Manages Qdrant vector database connection, collections, and embeddings"""
    def __init__(self):
        self._client: Optional[QdrantClient] = None
        self._embeddings: Optional[FastEmbedEmbeddings] = None
        self._vector_store: Optional[QdrantVectorStore] = None

    @property
    def client(self) -> QdrantClient:
        if self._client is None:
            logger.info(f"Connecting to Qdrant at {settings.QDRANT_URL}")
            self._client = QdrantClient(url=settings.QDRANT_URL, timeout=10.0)
        return self._client

    @property
    def embeddings(self) -> FastEmbedEmbeddings:
        if self._embeddings is None:
            logger.info(f"Initializing FastEmbed model: {settings.EMBEDDING_MODEL_NAME}")
            self._embeddings = FastEmbedEmbeddings(model_name=settings.EMBEDDING_MODEL_NAME)
        return self._embeddings

    def ensure_collection(self) -> None:
        """Verifies or creates the support knowledge base collection in Qdrant"""
        try:
            collections = self.client.get_collections().collections
            exists = any(c.name == settings.QDRANT_COLLECTION_NAME for c in collections)
            
            if not exists:
                logger.info(f"Creating Qdrant collection: {settings.QDRANT_COLLECTION_NAME}")
                self.client.create_collection(
                    collection_name=settings.QDRANT_COLLECTION_NAME,
                    vectors_config=qmodels.VectorParams(
                        size=settings.EMBEDDING_VECTOR_SIZE,
                        distance=qmodels.Distance.COSINE
                    )
                )
                logger.info(f"Qdrant collection '{settings.QDRANT_COLLECTION_NAME}' created successfully.")
            else:
                logger.info(f"Qdrant collection '{settings.QDRANT_COLLECTION_NAME}' already exists.")
        except Exception as e:
            logger.error(f"Error ensuring Qdrant collection: {e}")
            raise

    def get_vector_store(self) -> QdrantVectorStore:
        """Returns the LangChain QdrantVectorStore wrapper"""
        if self._vector_store is None:
            self.ensure_collection()
            self._vector_store = QdrantVectorStore(
                client=self.client,
                collection_name=settings.QDRANT_COLLECTION_NAME,
                embedding=self.embeddings
            )
        return self._vector_store

qdrant_manager = QdrantManager()
