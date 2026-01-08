# PDF MachineTyping Embedding Microservice

A production-ready microservice for extracting text from PDF documents stored in MinIO and generating high-quality embedding vectors using open-source LLM models.

## 🚀 Features

- **Standardized API**: Built with FastAPI, featuring automatic Swagger documentation.
- **MinIO Integration**: Seamlessly retrieves documents from S3-compatible storage.
- **High-Performance Extraction**: Uses `PyMuPDF` (fitz) for fast and accurate text parsing.
- **Local AI Embeddings**: Uses `sentence-transformers` locally (default: `all-MiniLM-L6-v2`).
- **Dual Persistence**: 
    - **PostgreSQL**: Stores structured metadata and text chunks.
    - **Qdrant**: Stores high-dimensional vector embeddings for semantic search.
- **Robust Error Handling**: Transactional-like consistency between SQL and Vector stores with automatic rollbacks.
- **Multi-Environment**: Easy setup for `dev` and `prod` via `.env` files.

## 🛠 Setup & Running

The service is integrated into the main `docker-compose.yml`.

### Start the Service
```bash
docker-compose up -d extractor-typin-pdf-microservice
```

### Access Documentation & Dashboards
Once running, you can access the following interfaces:
- **Swagger UI**: [http://localhost:8002/docs](http://localhost:8002/docs)
- **Qdrant Web UI (Dashboard)**: [http://localhost:6333/dashboard](http://localhost:6333/dashboard)
  - *Use this to visualize collections, points, and payload data directly.*


## 🔍 Vector Database Operations (Qdrant Console)

Access the [Qdrant Dashboard](http://localhost:6333/dashboard) and use the **Console** tab to execute these operations.

### 📋 Collection Management
```http
// List all active collections
GET collections

// View schema and health of the pdf_chunks collection
GET collections/pdf_chunks
```

### 📂 Data Retrieval (Direct Sync)
We use a **Strict ID Synchronization** strategy where the Qdrant Point ID matches the PostgreSQL `chunk_id`.

```http
// Fetch a single point directly by UUID
GET collections/pdf_chunks/points/27dbcb98-20e2-4ea7-80fe-c1748d65895b

// Batch retrieve specific points with full vector data
POST collections/pdf_chunks/points
{
  "ids": ["27dbcb98-20e2-4ea7-80fe-c1748d65895b"],
  "with_vector": true,
  "with_payload": true
}

// Paginate through all points
POST collections/pdf_chunks/points/scroll
{
  "limit": 10,
  "with_payload": true
}
```

### 🔎 Keyword & Attribute Filtering
Search through optimized payloads (pdf_name, chunk_id, and 10-word snippets).

```http
// Filter by exact chunk_id within payload
POST collections/pdf_chunks/points/scroll
{
  "filter": {
    "must": [
      {
        "key": "chunk_id",
        "match": { "value": "27dbcb98-20e2-4ea7-80fe-c1748d65895b" }
      }
    ]
  },
  "limit": 1
}

// Full-text keyword search for "Location" in snippets
POST collections/pdf_chunks/points/scroll
{
  "filter": {
    "must": [
      {
        "key": "text_snippet",
        "match": { "text": "Location" }
      }
    ]
  },
  "limit": 10,
  "with_payload": true
}
```

### 🧠 Semantic Vector Search
Find the most relevant chunks by comparing vector similarity.

```http
// Vector search (find most similar content)
POST collections/pdf_chunks/points/search
{
  "vector": [0.0119, -0.0559, -0.0500, -0.0438], // Example 384-dim vector slice
  "limit": 5,
  "with_payload": true
}
```



## 📖 API Usage

### Health Check
```bash
curl http://localhost:8002/health
```

### Process a PDF
**Endpoint**: `POST /process`

**Payload**:
```json
{
  "file_path": "test1/statements_04.09.23.pdf",
  "chunk_size": 500,
  "chunk_overlap": 50
}
```

**Example Request**:
```bash
curl -X POST "http://localhost:8002/process" \
     -H "Content-Type: application/json" \
     -d '{"file_path": "test1/statements_04.09.23.pdf"}'
```

The service uses environment variables defined in `.env.dev`.

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_NAME` | Name of the service | `extractor-typin-pdf-microservice` |
| `MINIO_ENDPOINT` | MinIO server address | `minio:9000` |
| `EMBEDDING_MODEL` | HuggingFace model name | `all-MiniLM-L6-v2` |
| `DB_HOST` | Postgres host address | `postgres-pdf-text-metadata-db` |
| `QDRANT_HOST` | Qdrant host address | `qdrant-pdf-vector-db` |
| `QDRANT_PORT` | Qdrant HTTP port | `6333` |

## 📂 Project Structure

```text
extractor-typin-pdf-microservice/
├── main.py                           # FastAPI application & endpoints
├── processor.py                      # Orchestrer (Logic Flow)
├── text_data_persistance_service.py  # SQL Persistence (Postgres)
├── vector_data_persistence_service.py # Vector Persistence (Qdrant + Embeddings)
├── logger_config.py                  # Standardized Logging
├── requirements.txt                  # Python dependencies
└── .env.dev                          # Development configuration
```
