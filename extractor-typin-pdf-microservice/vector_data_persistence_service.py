import os
import uuid
from typing import List, Dict
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from logger_config import setup_logger

logger = setup_logger("vector-persistence")

class VectorDataPersistenceService:
    def __init__(self):
        # Qdrant config
        self.qdrant_host = os.getenv("QDRANT_HOST", "qdrant-pdf-vector-db")
        self.qdrant_port = int(os.getenv("QDRANT_PORT", 6333))
        self.collection_name = "pdf_chunks"
        
        logger.info("Initializing Embedding Model (SentenceTransformer)...")
        self.model = SentenceTransformer(os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2"))
        
        logger.info(f"Initializing Qdrant client at {self.qdrant_host}:{self.qdrant_port}")
        self.qdrant_client = QdrantClient(host=self.qdrant_host, port=self.qdrant_port)
        self._ensure_qdrant_collection()

    def _ensure_qdrant_collection(self):
        """Creates the Qdrant collection if it doesn't already exist."""
        try:
            if not self.qdrant_client.collection_exists(self.collection_name):
                logger.info(f"Creating Qdrant collection: {self.collection_name}")
                self.qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=384, distance=Distance.COSINE),
                )
        except Exception as e:
            logger.error(f"Error ensuring Qdrant collection: {str(e)}")
            raise

    def generate_embeddings(self, chunks: List[Dict]) -> List[List[float]]:
        """
        Generates vector embeddings for a list of text chunks.
        Best Practice: Wrap in try-except to catch model/memory issues and provide context.
        """
        if not chunks:
            return []
        
        try:
            texts = [str(c.get("text", "")) for c in chunks]
            logger.info(f"Generating embeddings for {len(texts)} chunks...")
            embeddings = self.model.encode(texts)
            return embeddings.tolist()
        except Exception as e:
            logger.error(f"Embedding generation failed: {str(e)} | Chunks count: {len(chunks)}")
            raise RuntimeError(f"Could not generate embeddings: {str(e)}") from e

    def upsert_to_qdrant(self, pdf_id: str, pdf_name: str, chunks: List[Dict], embeddings: List[List[float]], chunk_ids: List[str]):
        """
        Persists embeddings and optimized payloads to Qdrant as Points.
        Uses chunk_ids from SQL database for strict synchronization.
        """
        logger.info(f"Upserting {len(chunks)} optimized points to Qdrant for PDF {pdf_id}...")
        model_name = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        
        try:
            points = []
            for i, chunk in enumerate(chunks):
                # Use the ID from Postgres for perfect sync
                point_id = chunk_ids[i]
                
                # Truncate text to first 10 words for the snippet
                full_text = chunk.get("text", "")
                words = full_text.split()
                text_snippet = " ".join(words[:10])
                if len(words) > 10:
                    text_snippet += "..."

                points.append(PointStruct(
                    id=point_id,
                    vector=embeddings[i],
                    payload={
                        "pdf_id": pdf_id,
                        "pdf_name": pdf_name,
                        "chunk_id": point_id,
                        "model_name": model_name,
                        "page_number": chunk.get("page_number"),
                        "chunk_index": chunk.get("chunk_index"),
                        "text_snippet": text_snippet
                    }
                ))
            
            self.qdrant_client.upsert(
                collection_name=self.collection_name,
                points=points
            )
            logger.info(f"Successfully upserted {len(chunks)} points to Qdrant for PDF {pdf_id}.")
        except Exception as e:
            logger.error(f"Qdrant upsert failed for PDF {pdf_id}: {str(e)}")
            raise RuntimeError(f"Failed to upsert vectors to Qdrant: {str(e)}") from e
