import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
from search_service import SearchService
from chat_with_ollama_llm import ChatWithOllamaLlm
from prompts import SYSTEM_PROMPT
from logger_config import setup_logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env.dev")

logger = setup_logger("text-searcher-api")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle events to initialize services."""
    logger.info("Starting Text Searcher Microservice...")
    try:
        app.state.search_service = SearchService()
        app.state.chat_with_ollama_llm = ChatWithOllamaLlm()
        logger.info("Services initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
    yield
    logger.info("Shutting down Text Searcher Microservice...")

app = FastAPI(
    title="PDF Text Searcher API",
    description="Semantic search service for PDF documents using Vector Embeddings.",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for the UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development; refine for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SearchRequest(BaseModel):
    query: str
    limit: int = 3
    debug_flag: bool = False

class SearchResponse(BaseModel):
    score: float
    pdf_id: Optional[str] = None
    pdf_name: Optional[str] = "Unknown"
    page_number: Optional[int] = None
    chunk_index: Optional[int] = None
    full_text: str
    metadata: Optional[Dict] = None
    chunk_id: str

class AskLlmResponse(BaseModel):
    final_llm_answer: str
    duration_seconds: float
    sources: Optional[List[SearchResponse]] = None
    debug_info: Optional[Dict] = None

@app.get("/health")
async def health(request: Request):
    """Basic health check endpoint."""
    search_service = getattr(request.app.state, "search_service", None)
    if search_service is None:
        raise HTTPException(status_code=503, detail="Search service not initialized")
    return {
        "status": "healthy",
        "service": os.getenv("APP_NAME", "text-searcher-microservice"),
        "qdrant_host": search_service.qdrant_host
    }

@app.post("/search_in_pdf_chunks_by_full_text_similarity", response_model=List[SearchResponse])
async def find_by_text(request: Request, search_request: SearchRequest):
    """
    Search for semantically similar text chunks across processed PDFs.
    
    Workflow:
    1. Convert user query to embedding with ollama-llm-embedding-service.
    2. Find the most similar chunks in Qdrant (top 5).
    3. Read context from Postgres (full text, filename, metadata).
    4. Format and return results.
    """
    search_service = getattr(request.app.state, "search_service", None)
    if search_service is None:
        raise HTTPException(status_code=503, detail="Search service is currently unavailable")
    
    try:
        logger.info(f"Triggering semantic search for query: {search_request.query}")
        results = search_service.find_by_text(search_request.query, search_request.limit)
        return results
    except Exception as e:
        logger.error(f"API Error during search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ask_llm_model", response_model=AskLlmResponse, response_model_exclude_none=True)
async def ask_llm_model(request: Request, search_request: SearchRequest):
    """
    Complete RAG Workflow with optional debugging:
    1. Refine search intent.
    2. Perform semantic search.
    3. Construct context and generate final answer.
    """
    search_service = getattr(request.app.state, "search_service", None)
    chat_with_ollama_llm = getattr(request.app.state, "chat_with_ollama_llm", None)
    
    if search_service is None or chat_with_ollama_llm is None:
        raise HTTPException(status_code=503, detail="Services not initialized")
    
    start_time = time.time()
    try:
        user_looking_for_query = search_request.query
        # Step 1: Refine query for searching
        # TODO Can be used for a better search intent
        
        # # Step 2 & 3: Search Qdrant and get PostgreSQL context
        logger.info(f"Step 2/3: Searching Qdrant and fetching context for: {user_looking_for_query}")
        search_results = search_service.find_by_text(user_looking_for_query, search_request.limit)
        logger.info(f"Retrieved {len(search_results)} relevant chunks.")
                
        # Step 5: Combine context and generate final answer
        logger.info("Step 5: Constructing context and generating final response...")
        context_text = "\n\n".join([
            f"Source: {res.get('pdf_name')}\nContent: {res.get('full_text')}" 
            for res in search_results
        ])
        
        # Step 5: Generate final answer
        llm_answer, llm_messages = chat_with_ollama_llm.generate_response(
            SYSTEM_PROMPT, 
            context=context_text, 
            user_prompt=user_looking_for_query
        )
        logger.info("Step 5: Final answer generated.")
        
        duration = round(time.time() - start_time, 2)
        
        # Prepare response based on debug_flag
        response = AskLlmResponse(
            final_llm_answer=llm_answer,
            duration_seconds=duration
        )
        
        if search_request.debug_flag:            
            response.sources = search_results
            
            # Extract unique documents with their metadata
            docs_info = {}
            for res in search_results:
                pdf_id = res.get('pdf_id')
                if pdf_id and pdf_id not in docs_info:
                    docs_info[pdf_id] = {
                        'pdf_id': pdf_id,
                        'pdf_name': res.get('pdf_name', 'Unknown'),
                        'metadata': res.get('metadata', {})
                    }
            
            # Build detailed chunk information
            chunks_info = []
            for res in search_results:
                chunks_info.append({
                    'chunk_id': res.get('chunk_id'),
                    'pdf_id': res.get('pdf_id'),
                    'pdf_name': res.get('pdf_name', 'Unknown'),
                    'page_number': res.get('page_number'),
                    'chunk_index': res.get('chunk_index'),
                    'similarity_score': res.get('score'),
                    'metadata': res.get('metadata', {}),
                    'text_preview': res.get('full_text', '')[:100] + '...' if len(res.get('full_text', '')) > 100 else res.get('full_text', '')
                })
            
            response.debug_info = {
                "refined_query": user_looking_for_query,
                "documents_used": list(docs_info.values()),
                "chunks_details": chunks_info,
                "total_documents": len(docs_info),
                "total_chunks": len(chunks_info),
                "llm_messages": [m.dict() for m in llm_messages]
            }
        
        return response
        
    except Exception as e:
        logger.error(f"Error in RAG flow: {e}")
        raise HTTPException(status_code=500, detail=str(e))



if __name__ == "__main__":
    import uvicorn
    #uvicorn.run(app, host="0.0.0.0", port=80
    # Note: reload=True requires passing the app as an import string "main:app"
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="debug")
