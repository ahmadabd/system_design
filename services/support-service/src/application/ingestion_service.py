import os
import glob
import logging
from typing import List, Dict, Any
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from src.adapter.vector_adapter import QdrantVectorAdapter
from src.infrastructure.config import settings

logger = logging.getLogger("IngestionService")

class IngestionApplicationService:
    """Application use case for loading, chunking, and indexing policy documents into Qdrant"""
    def __init__(self, vector_adapter: QdrantVectorAdapter):
        self.vector_adapter = vector_adapter
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=600,
            chunk_overlap=100,
            separators=["\n## ", "\n### ", "\n\n", "\n", " "]
        )

    async def ingest_directory(self, dir_path: str | None = None) -> Dict[str, Any]:
        """Loads all markdown files in the specified directory, chunks them, and indexes them into Qdrant"""
        target_dir = dir_path or settings.KNOWLEDGE_BASE_DIR
        logger.info(f"Starting knowledge base ingestion from: {target_dir}")

        if not os.path.exists(target_dir):
            # Check relative directory if running locally outside container
            local_fallback = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "knowledge_base")
            if os.path.exists(local_fallback):
                target_dir = local_fallback
            else:
                raise FileNotFoundError(f"Knowledge base directory not found at {target_dir} or {local_fallback}")

        md_files = glob.glob(os.path.join(target_dir, "*.md"))
        logger.info(f"Found {len(md_files)} knowledge base markdown files: {md_files}")

        all_documents: List[Document] = []
        for file_path in md_files:
            file_name = os.path.basename(file_path)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            raw_doc = Document(
                page_content=content,
                metadata={
                    "source": file_name,
                    "title": file_name.replace("_", " ").replace(".md", "").title()
                }
            )
            # Split document into semantically sized chunks
            chunks = self.text_splitter.split_documents([raw_doc])
            for i, chunk in enumerate(chunks):
                chunk.metadata["chunk_index"] = i
                chunk.metadata["total_chunks"] = len(chunks)
            
            all_documents.extend(chunks)

        logger.info(f"Generated {len(all_documents)} total chunks across {len(md_files)} documents.")
        
        if all_documents:
            indexed_ids = await self.vector_adapter.add_documents(all_documents)
            logger.info(f"Successfully indexed {len(indexed_ids)} chunks into Qdrant collection '{settings.QDRANT_COLLECTION_NAME}'.")
            return {
                "status": "success",
                "files_processed": len(md_files),
                "chunks_indexed": len(all_documents),
                "collection_name": settings.QDRANT_COLLECTION_NAME
            }
        else:
            return {
                "status": "warning",
                "message": "No documents found to ingest",
                "files_processed": 0,
                "chunks_indexed": 0
            }
