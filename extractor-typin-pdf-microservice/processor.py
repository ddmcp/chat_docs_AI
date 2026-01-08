import os
import uuid
import fitz  # PyMuPDF
from minio import Minio
from minio.error import S3Error
from typing import List, Dict, Optional
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from text_data_persistance_service import TextDataPersistenceService
from vector_data_persistence_service import VectorDataPersistenceService
from logger_config import setup_logger

logger = setup_logger("pdf-processor")

class PdfProcessor:
    def __init__(self):
        # MinIO config
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.minio_pass = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.minio_secure = os.getenv("MINIO_SECURE", "False").lower() == "true"
        
        self.default_chunk_size = int(os.getenv("DEFAULT_CHUNK_SIZE", 500))
        self.default_chunk_overlap = int(os.getenv("DEFAULT_CHUNK_OVERLAP", 50))

        logger.info(f"Initializing MinIO client connection to {self.minio_endpoint}")
        self.minio_client = Minio(
            self.minio_endpoint,
            access_key=self.minio_user,
            secret_key=self.minio_pass,
            secure=self.minio_secure
        )
        
        # Initialize persistence services
        self.persistence_service = TextDataPersistenceService()
        self.vector_service = VectorDataPersistenceService()

    def download_file(self, bucket_name: str, object_name: str, local_path: str):
        """Downloads a file from MinIO to a local path."""
        logger.info(f"Downloading {object_name} from bucket {bucket_name}...")
        try:
            self.minio_client.fget_object(bucket_name, object_name, local_path)
        except Exception as e:
            logger.error(f"MinIO download error: {str(e)}")
            raise

    def extract_and_chunk(self, pdf_path: str, chunk_size: int, overlap: int) -> List[Dict]:
        """Extracts text from PDF and splits it into chunks, preserving page references."""
        logger.info(f"Extracting and chunking PDF: {pdf_path}")
        chunks = []
        try:
            with fitz.open(pdf_path) as doc:
                for page_num, page in enumerate(doc, start=1):
                    text = page.get_text()
                    if not text.strip():
                        continue
                    
                    # Page-level chunking
                    start = 0
                    chunk_idx = 0
                    while start < len(text):
                        end = start + chunk_size
                        chunk_text = text[start:end]
                        chunks.append({
                            "page_number": page_num,
                            "chunk_index": chunk_idx,
                            "text": chunk_text
                        })
                        start += chunk_size - overlap
                        chunk_idx += 1
                        if overlap >= chunk_size:
                            break
            return chunks
        except Exception as e:
            logger.error(f"PDF extraction error: {str(e)}")
            raise

    def process_pdf(self, file_path: str, chunk_size: Optional[int] = None, overlap: Optional[int] = None) -> Dict:
        """
        Orchestrates the full PDF processing pipeline:
        1. Parse file path (bucket/object)
        2. Download from MinIO
        3. Extract text and metadata
        4. Chunk text
        5. Persist to SQL Database
        6. Create embeddings (Delegated to Vector Service)
        7. Persist to Vector Database (Delegated to Vector Service)
        """
        if "/" not in file_path:
            raise ValueError("Invalid file_path format. Expected 'bucket/object_name'")
        
        bucket_name, object_name = file_path.split("/", 1)
        temp_pdf = f"/tmp/{os.path.basename(object_name)}"
        
        c_size = chunk_size or self.default_chunk_size
        c_overlap = overlap or self.default_chunk_overlap
        pdf_id = None
        try:
            # 1. Download
            self.download_file(bucket_name, object_name, temp_pdf)
            
            # 2. Extract Metadata
            with fitz.open(temp_pdf) as doc:
                metadata = doc.metadata
            
            # 3. Extract and Chunk Text
            chunks = self.extract_and_chunk(temp_pdf, c_size, c_overlap)
            
            # 4. Save to SQL Database (now returns dict with sync IDs)
            persistence_result = self.persistence_service.save_pdf_data(bucket_name, object_name, metadata, chunks)
            pdf_id = persistence_result["pdf_id"]
            chunk_ids = persistence_result["chunk_ids"]
            
            # 5. Create Embeddings
            embeddings = self.vector_service.generate_embeddings(chunks)
            
            # 6. Save to Qdrant embedding vector and short metadata
            self.vector_service.upsert_to_qdrant(
                pdf_id=pdf_id,
                pdf_name=object_name,
                chunks=chunks,
                embeddings=embeddings,
                chunk_ids=chunk_ids
            )
            
            return {
                "pdf_id": pdf_id,
                "file": file_path,
                "chunks_count": len(chunks),
                "status": "processed and persisted (SQL + Vector)"
            }
        except Exception as e:
            logger.error(f"Processing failed for {file_path}: {str(e)}")
            # Rollback SQL if it was successful but vector storage failed
            if pdf_id:
                self.persistence_service.delete_pdf_data(pdf_id)
            raise
        finally:
            if os.path.exists(temp_pdf):
                os.remove(temp_pdf)
