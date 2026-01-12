import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict
from llm_client import LLMClient
from qdrant_client import QdrantClient
from qdrant_client.http import models
from logger_config import setup_logger

logger = setup_logger("search-service")

class SearchService:
    def __init__(self):
        # Qdrant config
        self.qdrant_host = os.getenv("QDRANT_HOST", "qdrant-vector-db")
        self.qdrant_port = int(os.getenv("QDRANT_PORT", 6333))
        self.collection_name = os.getenv("QDRANT_COLLECTION", "pdf_chunks")
        
        # Database config
        self.db_host = os.getenv("DB_HOST", "pg-typing-pdf-extractor-db")
        self.db_name = os.getenv("DB_NAME", "pdfdb")
        self.db_user = os.getenv("DB_USER", "pdfuser")
        self.db_pass = os.getenv("DB_PASS", "pdfpassword")
        
        logger.info("Initializing LLM Client for embeddings...")
        self.llm = LLMClient()
        
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

    def find_by_text(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Encodes query to vector and searches for similar chunks in Qdrant using named vectors.
        """
        try:
            logger.info(f"Searching for: '{query}' (limit: {limit})")
            
            # 1. Generate Query Vector via remote LLM
            query_vector = self.llm.get_embedding(query)
            
            # 2. Search in Qdrant using named vector 'full_chunk_text'
            # Using query_points as 'search' is deprecated/unavailable in newer qdrant-client versions
            query_response = self.qdrant_client.query_points(
                collection_name=self.collection_name,
                query=query_vector,
                using="full_chunk_text",
                limit=limit,
                with_payload=True,
                with_vectors=False
            )
            search_result = query_response.points

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
                    "pdf_name": postgres_record.get("pdf_name", "Unknown PDF"),
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
                # Join with pdf table to get the filename as pdf_name
                cur.execute(
                    """
                    SELECT 
                        ct.chunk_id, 
                        ct.text, 
                        p.filename as pdf_name,
                        pm.extra 
                    FROM chunk_text ct
                    JOIN pdf p ON ct.pdf_id = p.pdf_id
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
