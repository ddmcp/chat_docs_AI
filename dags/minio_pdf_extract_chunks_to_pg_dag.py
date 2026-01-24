import json
import logging
import requests
from functools import lru_cache
from datetime import datetime
from typing import List, Set, Optional
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio import Minio

# Configure logging
logger = logging.getLogger("airflow.task")

# Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
EXTRACTION_SERVICE_URL = "http://typing-pdf-extractor-service:8000/process"
POSTGRES_CONN_ID = "PDF_DB"

# --------------------------------------------------------------------------
# PRIVATE SQL HELPERS For file status processing (KISS: Keep It Simple, Secure, and Separated)
# --------------------------------------------------------------------------

def _get_seen_files(hook: PostgresHook, bucket: str) -> Set[str]:
    """Fetch filenames already in registry for this bucket."""
    query = "SELECT filename FROM airflow_file_registry WHERE bucket = %s"
    records = hook.get_records(query, parameters=(bucket,))
    return {row[0] for row in records}

def _get_failed_files(hook: PostgresHook, bucket: str) -> List[str]:
    """Fetch filenames with status 'failed' for retry."""
    query = "SELECT filename FROM airflow_file_registry WHERE bucket = %s AND status = 'failed'"
    records = hook.get_records(query, parameters=(bucket,))
    return [row[0] for row in records]

def _register_new_files(hook: PostgresHook, bucket: str, files: List[str]):
    """
    Insert new files into registry as 'pending'.
    The 'pending' status acts as a soft-lock to ensure that even if multiple 
    DAG runs overlap, they see the file as 'registered' and avoid duplicates.
    """
    for file_name in files:
        hook.run(
            "INSERT INTO airflow_file_registry (bucket, filename, status) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            parameters=(bucket, file_name, 'pending')
        )
        logger.info(f"Registered: [{bucket}] {file_name} (pending)")

def _update_file_status(hook: PostgresHook, bucket: str, filename: str, status: str, error: Optional[str] = None):
    """
    Update file processing status and log errors if they occur.
    """
    query = """
        UPDATE airflow_file_registry 
        SET status = %s, error_message = %s, processed_at = CURRENT_TIMESTAMP 
        WHERE bucket = %s AND filename = %s
        AND (status != 'processed' OR %s = 'processed')
    """
    hook.run(query, parameters=(status, error, bucket, filename, status))
    logger.info(f"Updated: [{bucket}] {filename} -> status: {status}")

# --------------------------------------------------------------------------
# PRIVATE MINIO HELPERS
# --------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_minio_client() -> Minio:
    """
    Initialize and return a MinIO client.
    """
    return Minio(
        MINIO_ENDPOINT, 
        access_key=MINIO_ACCESS_KEY, 
        secret_key=MINIO_SECRET_KEY, 
        secure=False
    )

# --------------------------------------------------------------------------
# DAG TASKS
# --------------------------------------------------------------------------

def list_minio_buckets():
    """List all available buckets from MinIO."""
    try:
        client = _get_minio_client()
        buckets = client.list_buckets()
        bucket_names = [b.name for b in buckets]
        logger.info(f"Found buckets: {bucket_names}")
        return bucket_names
    except Exception as e:
        logger.error(f"Failed to list MinIO buckets: {e}")
        raise

def list_pdf_files(ti):
    """List all .pdf files from all discovered MinIO buckets."""
    try:
        buckets = ti.xcom_pull(task_ids="list_minio_buckets")
        if not buckets:
            logger.info("No buckets found to process.")
            return []

        client = _get_minio_client()
        all_pdf_objects = [] # List of {"bucket": str, "filename": str}

        for bucket in buckets:
            logger.info(f"Scanning bucket: {bucket}")
            objects = client.list_objects(bucket, recursive=True)
            for obj in objects:
                if obj.object_name.lower().endswith('.pdf'):
                    all_pdf_objects.append({"bucket": bucket, "filename": obj.object_name})
        
        logger.info(f"Total PDFs found: {len(all_pdf_objects)}")
        return all_pdf_objects
    except Exception as e:
        logger.error(f"MinIO PDF listing failed: {e}")
        raise

def filter_new_pdf_files(ti):
    """
    Filter files into a processing queue (New + Pending + Failed) for all buckets.
    """
    try:
        all_pdfs = ti.xcom_pull(task_ids="list_pdf_files")
        if not all_pdfs: 
            logger.info("No PDFs discovered in MinIO.")
            return []

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        queue = []
        
        buckets_in_discovery = set(item["bucket"] for item in all_pdfs)
        logger.info(f"Checking status for files in buckets: {buckets_in_discovery}")
        
        for bucket in buckets_in_discovery:
            files_in_this_bucket = [i["filename"] for i in all_pdfs if i["bucket"] == bucket]
            seen = _get_seen_files(hook, bucket)
            
            # 1. Discovery: Register brand new files
            new_files = [f for f in files_in_this_bucket if f not in seen]
            if new_files:
                logger.info(f"Found {len(new_files)} new files in bucket '{bucket}'. Registering...")
                _register_new_files(hook, bucket, new_files)
            
            # 2. Collection: Get files that need work (Pending + Failed)
            # We fetch from DB because 'seen' only gives us names, not statuses.
            query = "SELECT filename FROM airflow_file_registry WHERE bucket = %s AND status IN ('pending', 'failed')"
            records = hook.get_records(query, parameters=(bucket,))
            to_process = [row[0] for row in records]
            
            # Match discovered files with those that need work
            # This ensures we only process files that actually exist in MinIO right now
            for file_name in to_process:
                if file_name in files_in_this_bucket:
                    queue.append({"bucket": bucket, "filename": file_name})
        
        logger.info(f"Final processing queue contains {len(queue)} items.")
        return queue
    except Exception as e:
        logger.error(f"Filtering logic failed: {e}")
        raise

def extract_pdf_text_chunk_and_persist(bucket: str, filename: str):
    """
    Process a single file via Extraction API and update SQL registry.
    This task is dynamically mapped to run in parallel for different files.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    try:
        logger.info(f"--- START EXTRACTION TASK: [{bucket}] {filename} ---")
        
        payload = {"file_path": f"{bucket}/{filename}"}
        logger.info(f"Step 1: Preparing payload: {payload}")
        
        logger.info(f"Step 2: Sending POST request to {EXTRACTION_SERVICE_URL} (timeout=300s)...")
        resp = requests.post(EXTRACTION_SERVICE_URL, json=payload, timeout=300)
        
        logger.info(f"Step 3: Received response. Status: {resp.status_code}")
        if resp.status_code == 200:
            logger.info("Step 4: Extraction successful. Updating status to 'processed' in SQL...")
            _update_file_status(hook, bucket, filename, 'processed')
            logger.info(f"--- SUCCESS: [{bucket}] {filename} processed ---")
        else:
            error_text = resp.text[:255]
            logger.warning(f"Step 4: Extraction failed with service error. Response: {error_text}")
            _update_file_status(hook, bucket, filename, 'failed', error_text)
            logger.info(f"--- HANDLED FAILURE: [{bucket}] {filename} marked as failed ---")
            
    except Exception as e:
        error_msg = f"Crash in extraction task: {str(e)[:255]}"
        logger.error(f"FATAL ERROR: {error_msg}")
        _update_file_status(hook, bucket, filename, 'failed', error_msg)
        logger.info(f"--- CRASHED: [{bucket}] {filename} marked as failed ---")
        raise

# --------------------------------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------------------------------

with DAG(
    dag_id="minio_pdf_extract_chunks_to_pg_dag",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["minio", "extraction", "postgres", "multibucket"],
) as dag:

    list_minio_buckets_task = PythonOperator(task_id="list_minio_buckets", python_callable=list_minio_buckets)
    list_pdf_files_task = PythonOperator(task_id="list_pdf_files", python_callable=list_pdf_files)
    filter_new_pdf_files_task = PythonOperator(task_id="filter_new_pdf_files", python_callable=filter_new_pdf_files)
    
    # Dynamic Task Mapping: Create one task per file in the queue
    extract_pdf_text_chunk_and_persist_pg_task = PythonOperator.partial(
        task_id="extract_pdf_text_chunk_and_persist", 
        python_callable=extract_pdf_text_chunk_and_persist,
        map_index_template="{{ bucket }}/{{ filename }}"
    ).expand(op_kwargs=filter_new_pdf_files_task.output)

    list_minio_buckets_task >> list_pdf_files_task >> filter_new_pdf_files_task >> extract_pdf_text_chunk_and_persist_pg_task
