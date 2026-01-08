# PDF Text Searcher Microservice

A semantic search microservice that allows users to find information across processed PDF documents using vector embeddings and natural language queries.

## 🚀 Features

- **Semantic Search**: Find relevant text chunks even if keywords don't match exactly.
- **FastAPI Core**: High-performance API with automatic interactive documentation.
- **Qdrant Integration**: Efficient vector search powered by Qdrant.
- **LLM Embeddings**: Uses `sentence-transformers` (default: `all-MiniLM-L6-v2`) locally.

## 🛠 Setup & Running

The service is integrated into the main `docker-compose.yml`.

### Start the Service
```bash
docker-compose up -d text-searcher-microservice
```

### Access Documentation
- **Swagger UI**: [http://localhost:8003/docs](http://localhost:8003/docs)
- **Health Check**: [http://localhost:8003/health](http://localhost:8003/health)

## 📖 API Usage

### Search for Text
**Endpoint**: `POST /search`

**Request Body**:
```json
{
  "query": "machine learning",
  "limit": 3
}
```

**Example Request**:
```bash
curl -X POST "http://localhost:8003/search" \
     -H "Content-Type: application/json" \
     -d '{"query": "machine learning", "limit": 3}'
```

**Example Response**:
```json
[
  {
    "score": 0.852,
    "pdf_id": "uuid-123",
    "pdf_name": "ai_trends.pdf",
    "page_number": 5,
    "chunk_index": 12,
    "text_snippet": "Machine learning is a subset of AI that focuses on...",
    "chunk_id": "chunk-uuid-abc"
  }
]
```

## ⚙️ Configuration

The service uses environment variables defined in `.env.dev`.

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_NAME` | Name of the service | `text-searcher-microservice` |
| `EMBEDDING_MODEL` | HuggingFace model name | `all-MiniLM-L6-v2` |
| `QDRANT_HOST` | Qdrant host address | `qdrant-pdf-vector-db` |
| `QDRANT_PORT` | Qdrant HTTP port | `6333` |

## 📂 Project Structure

```text
text-searcher-microservice/
├── main.py              # FastAPI application & endpoints
├── search_service.py    # Vector search logic
├── logger_config.py     # Logging configuration
├── requirements.txt     # Python dependencies
├── .env.dev             # Development configuration
└── Dockerfile           # Container definition
```
