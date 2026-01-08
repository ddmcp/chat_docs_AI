import json
import logging
import requests
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
BUCKET_NAME = "test1"
EMBEDDING_SERVICE_URL = "http://extractor-typin-pdf-microservice:8000/process"
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
        logger.info(f"Registered: {file_name} (pending)")

def _update_file_status(hook: PostgresHook, bucket: str, filename: str, status: str, error: Optional[str] = None):
    """
    Update file processing status and log errors if they occur.
    
    CRASH LOGIC & STATUS RESOLVING:
    - If processing crashes (timeout, network, service down), the status is caught in the 
      task's try-except block and updated to 'failed'.
    - ROBUSTNESS: We add a status check to the UPDATE query to ensure we never regress 
      a 'processed' file back to 'failed' (e.g. if a delayed retry run finishes after 
       a successful run).
    """
    query = """
        UPDATE airflow_file_registry 
        SET status = %s, error_message = %s, processed_at = CURRENT_TIMESTAMP 
        WHERE bucket = %s AND filename = %s
        AND (status != 'processed' OR %s = 'processed')
    """
    # If the new status is 'processed', we allow the update.
    # If the current status is already 'processed', we block updates to 'failed'.
    hook.run(query, parameters=(status, error, bucket, filename, status))
    logger.info(f"Updated: {filename} -> status: {status}")

# --------------------------------------------------------------------------
# DAG TASKS
# --------------------------------------------------------------------------

def list_pdf_files():
    """List all .pdf files from MinIO."""
    try:
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
        objects = client.list_objects(BUCKET_NAME, recursive=True)
        files = [obj.object_name for obj in objects if obj.object_name.lower().endswith('.pdf')]
        logger.info(f"MinIO listed {len(files)} PDFs.")
        return files
    except Exception as e:
        # If MinIO listing fails, we raise to let Airflow mark the task as failed.
        # No file status changes needed here as we haven't reached the processing stage.
        logger.error(f"MinIO listing failed: {e}")
        raise

def filter_new_pdf_files(ti):
    """
    Filter files into a processing queue (New + Failed).
    
    SYSTEM RESILIENCE:
    - New files are registered as 'pending' immediately after discovery.
    - Failed files are automatically pulled back into the queue for the next run.
    - 'processed' files are ignored, acting as a permanent cache.
    """
    try:
        all_pdfs = ti.xcom_pull(task_ids="list_pdf_files")
        if not all_pdfs: return []

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 1. Handle New Files
        seen = _get_seen_files(hook, BUCKET_NAME)
        new = [f for f in all_pdfs if f not in seen]
        if new: _register_new_files(hook, BUCKET_NAME, new)
        
        # 2. Handle Retries
        failed = _get_failed_files(hook, BUCKET_NAME)
        
        queue = list(set(new + failed))
        logger.info(f"Queue: {len(new)} new, {len(failed)} retries. Total: {len(queue)}")
        return queue
    except Exception as e:
        logger.error(f"Filtering logic failed: {e}")
        raise

def send_minio_pdf_links(ti):
    """
    Process queue via Embedding API and update SQL registry.
    
    EXCEPTION HANDLING:
    - Each file is processed in an isolated try-except block.
    - If the API returns a non-200 or the request crashes, the status 
      is updated to 'error' (failed) with the specific message for auditing.
    """
    queue = ti.xcom_pull(task_ids="filter_new_pdf_files")
    if not queue: 
        logger.info("Nothing to process.")
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    for file_name in queue:
        try:
            logger.info(f"Processing: {file_name}")
            resp = requests.post(EMBEDDING_SERVICE_URL, json={"file_path": f"{BUCKET_NAME}/{file_name}"}, timeout=60)
            
            if resp.status_code == 200:
                _update_file_status(hook, BUCKET_NAME, file_name, 'processed')
            else:
                # Capture API errors (e.g. 500, 404) and update status to failed
                _update_file_status(hook, BUCKET_NAME, file_name, 'failed', resp.text[:255])
        except Exception as e:
            # Capture network crashes or logic errors and update status to failed
            _update_file_status(hook, BUCKET_NAME, file_name, 'failed', f"Crash: {str(e)[:255]}")

# --------------------------------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------------------------------

with DAG(
    dag_id="minio_pdf_processor_dag",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    tags=["minio", "embedding", "postgres", "refactored", "solid"],
) as dag:

    list_task = PythonOperator(task_id="list_pdf_files", python_callable=list_pdf_files)
    filter_task = PythonOperator(task_id="filter_new_pdf_files", python_callable=filter_new_pdf_files)
    send_task = PythonOperator(task_id="send_minio_pdf_links", python_callable=send_minio_pdf_links)

    list_task >> filter_task >> send_task
