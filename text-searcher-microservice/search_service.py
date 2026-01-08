import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from logger_config import setup_logger

logger = setup_logger("search-service")

class SearchService:
    def __init__(self):
        # Qdrant config
        self.qdrant_host = os.getenv("QDRANT_HOST", "qdrant-pdf-vector-db")
        self.qdrant_port = int(os.getenv("QDRANT_PORT", 6333))
        self.collection_name = "pdf_chunks"
        
        # Database config
        self.db_host = os.getenv("DB_HOST", "postgres-pdf-text-metadata-db")
        self.db_name = os.getenv("DB_NAME", "pdfdb")
        self.db_user = os.getenv("DB_USER", "pdfuser")
        self.db_pass = os.getenv("DB_PASS", "pdfpassword")
        
        logger.info("Initializing Embedding Model (SentenceTransformer)...")
        self.model = SentenceTransformer(os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2"))
        
        logger.info(f"Initializing Qdrant client at {self.qdrant_host}:{self.qdrant_port}")
        self.qdrant_client = QdrantClient(host=self.qdrant_host, port=self.qdrant_port)

    def _get_db_conn(self):
        """Creates and returns a new database connection."""
        return psycopg2.connect(
            host=self.db_host,
            database=self.db_name,
            user=self.db_user,
            password=self.db_pass
        )

    def find_by_text(self, query: str, top_the_most_similar_chunks: int = 5) -> List[Dict]:
        """
        Encodes query text and searches for similar chunks in Qdrant.
        """
        try:
            logger.info(f"Searching for: '{query}' (limit: {top_the_most_similar_chunks})")
            
            # 1. Generate Query Vector
            query_vector = self.model.encode(query).tolist()
            
            # 2. Search in Qdrant top the most similar chunks
            search_result = self.qdrant_client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=top_the_most_similar_chunks,
                with_payload=True,
                with_vectors=False
            )

            # 3. Get text data and metadata for each chunk by chunk id from postgres
            chunk_ids = [str(hit.id) for hit in search_result]
            logger.info(f"Targeting chunk IDs: {chunk_ids}")
            db_data = self._fetch_postgres_data(chunk_ids)

            # 4. Return results with text data and metadata
            results = []
            for hit in search_result:
                cid = str(hit.id)
                postgres_record = db_data.get(cid, {})
                
                if not postgres_record:
                    logger.warning(f"No Postgres record found for chunk_id: {cid}")
                
                results.append({
                    "score": hit.score,
                    "pdf_id": hit.payload.get("pdf_id"),
                    "pdf_name": hit.payload.get("pdf_name"),
                    "page_number": hit.payload.get("page_number"),
                    "chunk_index": hit.payload.get("chunk_index"),                    
                    "chunk_id": cid,
                    "full_text": postgres_record.get("text", ""),
                    "metadata": postgres_record.get("extra", {})
                })
            
            logger.info(f"Found {len(results)} matches for query.")
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise RuntimeError(f"Search operation failed: {str(e)}")

    def _fetch_postgres_data(self, chunk_ids: List[str]) -> Dict[str, Dict]:
        """Fetches full text and metadata from Postgres for a list of chunk IDs."""
        if not chunk_ids:
            return {}
            
        logger.info(f"Fetching full text and metadata for {len(chunk_ids)} chunks from Postgres...")
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Optimized join to get text and metadata in one go
                cur.execute(
                    """
                    SELECT 
                        ct.chunk_id, 
                        ct.text, 
                        pm.extra 
                    FROM chunk_text ct
                    LEFT JOIN pdf_metadata pm ON ct.pdf_id = pm.pdf_id
                    WHERE ct.chunk_id = ANY(%s::uuid[])
                    """,
                    (chunk_ids,)
                )
                rows = cur.fetchall()
                logger.info(f"Postgres returned {len(rows)} records for {len(chunk_ids)} searched IDs.")
                
                # Return as a mapping for easy lookup
                return {str(row['chunk_id']): row for row in rows}
        except Exception as e:
            logger.error(f"Database fetch error: {str(e)}")
            return {}
        finally:
            if conn:
                conn.close()
