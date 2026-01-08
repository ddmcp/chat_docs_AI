import os
import logging
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field
from typing import Optional
from processor import PdfProcessor

from logger_config import setup_logger

logger = setup_logger("api-server")

# Load app config
APP_NAME = os.getenv("APP_NAME", "PDF-Typing-Service")
APP_ENV = os.getenv("APP_ENV", "dev")

app = FastAPI(
    title=f"{APP_NAME} ({APP_ENV})",
    description="Microservice for PDF parsing and data storage.",
    version="1.2.0"
)

# Initialize processor
pdf_processor = PdfProcessor()

class ProcessRequest(BaseModel):
    file_path: str = Field(..., description="Full path to the file in MinIO (format: 'bucket/object')")
    chunk_size: Optional[int] = Field(None, description="Size of text chunks (optional, overrides default)")
    chunk_overlap: Optional[int] = Field(None, description="Overlap between chunks (optional, overrides default)")

    model_config = {
        "json_schema_extra": {
            "example": {
                "file_path": "test1/statements_04.09.23.pdf",
                "chunk_size": 500,
                "chunk_overlap": 50
            }
        }
    }

@app.get("/health", tags=["Monitoring"])
def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "app": APP_NAME,
        "env": APP_ENV
    }

@app.post("/process", tags=["Processing"])
async def process_file(request: ProcessRequest):
    """
    Process a PDF file from MinIO:
    1. Download from MinIO
    2. Extract text from PDF (page-aware)
    3. Save to Postgres
    """
    logger.info(f"Processing request for: {request.file_path}")
    try:
        result = pdf_processor.process_pdf(
            request.file_path, 
            chunk_size=request.chunk_size, 
            overlap=request.chunk_overlap
        )
        return result
    except ValueError as ve:
        logger.warning(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Internal server error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during processing.")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
