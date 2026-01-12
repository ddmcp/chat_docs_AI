import os
import requests
import json
from logger_config import setup_logger

logger = setup_logger("chat-client")

class ChatWithOllamaLlm:
    def __init__(self):
        # Ollama chat service URL
        self.base_url = os.getenv("OLLAMA_CHAT_URL", "http://ollama-llm-chat:11434")
        self.model = os.getenv("OLLAMA_CHAT_MODEL", "gemma:2b") # Using Gemma for balanced performance

    def generate_response(self, system_prompt: str, user_input: str) -> str:
        """Generates a response from the LLM using a system prompt and user input."""
        try:
            url = f"{self.base_url}/api/chat"
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input}
                ],
                "stream": False
            }
            logger.info(f"Sending chat request to Ollama: {self.model}")
            resp = requests.post(url, json=payload, timeout=60)
            resp.raise_for_status()
            
            content = resp.json().get("message", {}).get("content", "").strip()
            return content
        except Exception as e:
            logger.error(f"Error calling Ollama chat: {str(e)}")
            raise
