import os
import requests
from typing import List
from logger_config import setup_logger

logger = setup_logger("llm-client")

class LLMClient:
    def __init__(self):
        # Using environment variables for flexibility
        self.base_url = os.getenv("OLLAMA_EMBEDDING_URL", "http://ollama-llm-embedding:11434")
        self.model = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")

    def get_embedding(self, text: str) -> List[float]:
        """Fetches embedding for a single string from Ollama."""
        try:
            url = f"{self.base_url}/api/embeddings"
            payload = {
                "model": self.model,
                "prompt": text
            }
            logger.info(f"Fetching embedding from {url} for model {self.model}")
            resp = requests.post(url, json=payload, timeout=60)
            resp.raise_for_status()
            return resp.json()["embedding"]
        except Exception as e:
            logger.error(f"Error fetching embedding from Ollama: {str(e)}")
            raise
