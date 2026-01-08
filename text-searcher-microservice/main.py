import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict
from search_service import SearchService
from logger_config import setup_logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env.dev")

logger = setup_logger("text-searcher-api")

# Global service instance
search_service = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle events to initialize services."""
    global search_service
    logger.info("Starting Text Searcher Microservice...")
    try:
        search_service = SearchService()
        logger.info("Service initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize SearchService: {e}")
        # We don't raise here to allow the app to start (and fail on actual requests or health checks)
    yield
    logger.info("Shutting down Text Searcher Microservice...")

app = FastAPI(
    title="PDF Text Searcher API",
    description="Semantic search service for PDF documents using Vector Embeddings.",
    version="1.0.0",
    lifespan=lifespan
)

class SearchRequest(BaseModel):
    query: str
    limit: int = 5

class SearchResponse(BaseModel):
    score: float
    pdf_id: str
    pdf_name: str
    page_number: int
    chunk_index: int
    full_text: str
    metadata: Dict
    chunk_id: str

@app.get("/health")
async def health():
    """Basic health check endpoint."""
    if search_service is None:
        raise HTTPException(status_code=503, detail="Search service not initialized")
    return {
        "status": "healthy",
        "service": os.getenv("APP_NAME", "text-searcher-microservice"),
        "qdrant_host": search_service.qdrant_host
    }

@app.post("/search", response_model=List[SearchResponse])
async def find_by_text(request: SearchRequest):
    """
    Search for semantically similar text chunks across processed PDFs using a POST request.
    """
    if search_service is None:
        raise HTTPException(status_code=503, detail="Search service is currently unavailable")
    
    try:
        results = search_service.find_by_text(request.query, request.limit)
        return results
    except Exception as e:
        logger.error(f"API Error during search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
