import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from typing import List, Dict
from logger_config import setup_logger

logger = setup_logger("persistence-service")

class TextDataPersistenceService:
    def __init__(self):
        # Database config from environment
        self.db_host = os.getenv("DB_HOST", "postgres-pdf-text-metadata-db")
        self.db_name = os.getenv("DB_NAME", "pdfdb")
        self.db_user = os.getenv("DB_USER", "pdfuser")
        self.db_pass = os.getenv("DB_PASS", "pdfpassword")

    def _get_db_conn(self):
        """Creates and returns a new database connection."""
        return psycopg2.connect(
            host=self.db_host,
            database=self.db_name,
            user=self.db_user,
            password=self.db_pass
        )

    def save_pdf_data(self, bucket: str, filename: str, metadata: Dict, chunks: List[Dict]) -> Dict:
        """
        Orchestrates the persistence of PDF metadata and text chunks.
        Returns a dict with tokens for synchronization: {"pdf_id": str, "chunk_ids": List[str]}
        """
        logger.info(f"Persisting data for {filename} from bucket {bucket}...")
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Upsert PDF record and get ID
                pdf_id = self._upsert_pdf(cur, bucket, filename)

                # 2. Upsert Metadata
                self._upsert_metadata(cur, pdf_id, metadata)

                # 3. Save Chunks and get their IDs
                chunk_ids = self._save_chunks(cur, pdf_id, chunks)

            conn.commit()
            logger.info(f"Successfully persisted PDF {pdf_id} with {len(chunks)} chunks.")
            return {
                "pdf_id": str(pdf_id),
                "chunk_ids": [str(cid) for cid in chunk_ids]
            }
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database persistence error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def delete_pdf_data(self, pdf_id: str):
        """
        Deletes all data associated with a pdf_id from the database.
        Used for rollbacks in case of processing failures.
        """
        logger.warning(f"Rolling back SQL data for PDF {pdf_id}...")
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                cur.execute("DELETE FROM chunk_text WHERE pdf_id = %s", (pdf_id,))
                cur.execute("DELETE FROM pdf_metadata WHERE pdf_id = %s", (pdf_id,))
                cur.execute("DELETE FROM pdf WHERE pdf_id = %s", (pdf_id,))
            conn.commit()
            logger.info(f"Successfully deleted SQL data for PDF {pdf_id}.")
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error during SQL rollback for PDF {pdf_id}: {str(e)}")
        finally:
            if conn:
                conn.close()

    def _upsert_pdf(self, cur, bucket: str, filename: str) -> str:
        """Inserts or updates the PDF entry and returns the pdf_id."""
        cur.execute(
            """
            INSERT INTO pdf (filename, bucket) 
            VALUES (%s, %s) 
            ON CONFLICT (filename, bucket) DO UPDATE SET filename = EXCLUDED.filename
            RETURNING pdf_id;
            """,
            (filename, bucket)
        )
        return cur.fetchone()['pdf_id']

    def _upsert_metadata(self, cur, pdf_id: str, metadata: Dict):
        """Inserts or updates the PDF metadata."""
        cur.execute(
            """
            INSERT INTO pdf_metadata (pdf_id, creator, lineage, document_type, language, extra)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (pdf_id) DO UPDATE SET 
                creator = EXCLUDED.creator,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (
                pdf_id, 
                metadata.get("creator"), 
                "pdf_typing_microservice", 
                "pdf", 
                metadata.get("language", "unknown"),
                Json(metadata)
            )
        )

    def _save_chunks(self, cur, pdf_id: str, chunks: List[Dict]) -> List[str]:
        """Clears existing chunks, inserts new ones, and returns the generated UUIDs."""
        cur.execute("DELETE FROM chunk_text WHERE pdf_id = %s", (pdf_id,))
        chunk_ids = []
        for chunk in chunks:
            cur.execute(
                """
                INSERT INTO chunk_text (pdf_id, page_number, chunk_index, text)
                VALUES (%s, %s, %s, %s)
                RETURNING chunk_id;
                """,
                (pdf_id, chunk.get("page_number", 0), chunk.get("chunk_index"), chunk.get("text"))
            )
            chunk_ids.append(cur.fetchone()['chunk_id'])
        return chunk_ids
