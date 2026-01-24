import logging
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Configure logging
logger = logging.getLogger("airflow.task")

# Configuration
EMBEDDING_SERVICE_URL = "http://embedding-service:8000/process-batch"
BATCH_LIMIT = 5

# --------------------------------------------------------------------------
# DAG TASK
# --------------------------------------------------------------------------

def trigger_batch_processing():
    """
    Trigger the embedding service to process a batch of PDFs.
    The service handles all the logic: fetch, lock, process.
    This task just triggers and logs the detailed response.
    """
    logger.info(f"Triggering batch processing (limit={BATCH_LIMIT})...")
    
    try:
        payload = {"limit": BATCH_LIMIT}
        resp = requests.post(EMBEDDING_SERVICE_URL, json=payload, timeout=300)
        
        if resp.status_code == 200:
            result = resp.json()
            
            total = result.get("total_fetched", 0)
            processed = result.get("processed", [])
            failed = result.get("failed", [])
            
            logger.info(f"=== BATCH PROCESSING COMPLETE ===")
            logger.info(f"Total PDFs fetched: {total}")
            logger.info(f"Successfully processed: {len(processed)}")
            logger.info(f"Failed: {len(failed)}")
            
            # Log details for each processed PDF
            if processed:
                logger.info("--- Successfully Processed PDFs ---")
                for pdf in processed:
                    logger.info(f"  ✓ {pdf['filename']} (ID: {pdf['pdf_id']}) - Status: {pdf['status']}")
            
            # Log details for each failed PDF
            if failed:
                logger.warning("--- Failed PDFs ---")
                for pdf in failed:
                    logger.warning(f"  ✗ {pdf['filename']} (ID: {pdf['pdf_id']}) - Status: {pdf['status']}")
                    logger.warning(f"    Error: {pdf.get('error', 'Unknown error')}")
            
            if total == 0:
                logger.info("No new PDFs found for processing.")
                
        else:
            error_msg = f"Service returned error {resp.status_code}: {resp.text[:200]}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
    except Exception as e:
        logger.error(f"Failed to trigger batch processing: {str(e)}")
        raise

# --------------------------------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="pg_chunks_to_vectors_dag",
    default_args=default_args,
    description="Triggers embedding service to process new PDFs in batch",
    schedule="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["embedding", "qdrant", "postgres", "batch"],
) as dag:

    trigger_task = PythonOperator(
        task_id="trigger_batch_processing",
        python_callable=trigger_batch_processing,
    )
    trigger_task

