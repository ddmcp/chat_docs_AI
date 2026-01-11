import logging
from typing import List, Dict, Any
from qdrant_client.http import models
from qdrant_service import QdrantService
from pg_service import PgService
from llm_client import LLMClient
from logger_config import setup_logger

logger = setup_logger("processor")

class EmbeddingProcessor:
    def __init__(self):
        self.qdrant = QdrantService()
        self.pg = PgService()
        self.llm = LLMClient()

    def _get_short_text(self, text: str) -> str:
        """KISS: returns the first 4 words of the text."""
        words = text.split()
        return " ".join(words[:4])

    def fetch_batch_to_process(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Atomically fetch and lock a batch of PDFs.
        Returns the raw results from PG.
        """
        logger.info(f"--- FETCHING BATCH (limit={limit}) ---")
        pdfs = self.pg.atomic_fetch_and_lock_pdfs(limit)
        
        if not pdfs:
            logger.info("No new PDFs found for embedding.")
            return []
        
        logger.info(f"Fetched and locked {len(pdfs)} PDFs for processing")
        return pdfs

    def process_locked_pdfs_background(self, pdfs: List[Dict[str, Any]]):
        """
        Background task to process a list of already locked PDFs.
        """
        logger.info(f"--- STARTING BACKGROUND PROCESSING FOR {len(pdfs)} PDFs ---")
        for pdf in pdfs:
            pdf_id = str(pdf["pdf_id"])
            filename = pdf["filename"]
            try:
                logger.info(f"Processing PDF in background: {filename} (ID: {pdf_id})")
                self.process_pdf_embeddings(pdf_id)
            except Exception as e:
                logger.error(f"Failed to process {filename} in background: {str(e)}")
        logger.info("--- BACKGROUND PROCESSING COMPLETE ---")

    def process_batch(self, limit: int = 5) -> Dict[str, Any]:
        """
        Sync version for backward compatibility or direct calls.
        """
        pdfs = self.fetch_batch_to_process(limit)
        self.process_locked_pdfs_background(pdfs)
        return {"total_fetched": len(pdfs), "pdfs": pdfs}

    def get_bulk_statuses(self, pdf_ids: List[str]) -> List[Dict]:
        """Fetches statuses and calculates duration for a list of pdf_ids."""
        import datetime
        results = self.pg.get_pdf_statuses(pdf_ids)
        for r in results:
            if r.get('embedding_started_at') and r.get('embedding_completed_at'):
                start = datetime.datetime.fromisoformat(r['embedding_started_at'])
                end = datetime.datetime.fromisoformat(r['embedding_completed_at'])
                duration = (end - start).total_seconds()
                r['duration_seconds'] = round(duration, 2)
            else:
                r['duration_seconds'] = None
        return results



    def process_pdf_embeddings(self, pdf_id: str):
        """
        Orchestrates the embedding flow:
        1. Fetch chunks from Postgres
        2. Generate embeddings via Ollama
        3. Formulate points for Qdrant
        4. Upsert to Qdrant
        5. Update SQL status
        """
        logger.info(f"--- START EMBEDDING PROCESS FOR PDF: {pdf_id} ---")
        try:
            # 1. Update status to 'in progress'
            self.pg.update_embedding_status(pdf_id, "in progress")


            # 2. Read all chunks for this pdf_id from Postgres
            chunks = self.pg.get_chunks_to_embed(pdf_id)
            if not chunks:
                logger.warning(f"No chunks found for PDF {pdf_id}")
                self.pg.update_embedding_status(pdf_id, "done") # Nothing to do
                return

            logger.info(f"Found {len(chunks)} chunks to embed.")

            points = []
            for chunk in chunks:
                chunk_id = chunk["chunk_id"]
                text = chunk["text"]
                
                # 3. Create embeddings for each chunk
                logger.debug(f"Generating embeddings for chunk {chunk_id}...")
                full_chunk_text_vector = self.llm.get_embedding(text)
                
                # For summary_vector, we can either summarize and then embed,
                # or for this demonstration, we'll embed the "short text" 
                # to show that different vectors can be stored.
                short_text = self._get_short_text(text)
                # TODO: add summary generation via LLM
                chunk_summary_vector = self.llm.get_embedding(short_text)

                # 4. Create Qdrant point with named vectors
                point = models.PointStruct(
                    id=str(chunk_id), # Use chunk_id as point ID
                    vector={
                        "full_chunk_text": full_chunk_text_vector,
                        "chunk_summary": chunk_summary_vector
                    },
                    payload={
                        "pdf_id": str(pdf_id),
                        "chunk_id": str(chunk_id),
                        "short_chunk_text": short_text,
                        "page_number": chunk.get("page_number"),
                        "chunk_index": chunk.get("chunk_index")
                    }
                )
                points.append(point)

            # 5. Upsert to Qdrant
            logger.info(f"Upserting {len(points)} points to Qdrant...")
            self.qdrant.upsert_to_qdrant(pdf_id, points)

            # 6. Update status to 'done'
            self.pg.update_embedding_status(pdf_id, "done")
            logger.info(f"--- SUCCESS: PDF {pdf_id} embeddings generated and stored ---")

        except Exception as e:
            logger.error(f"FATAL ERROR while embedding PDF {pdf_id}: {str(e)}")
            self.pg.update_embedding_status(pdf_id, "failed")
            raise