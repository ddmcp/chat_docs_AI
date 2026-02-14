import os
from langchain_ollama import ChatOllama
from langchain_core.messages import SystemMessage, HumanMessage, BaseMessage
from logger_config import setup_logger

logger = setup_logger("chat-client")

class ChatWithOllamaLlm:
    def __init__(self):
        # Ollama chat service URL
        self.base_url = os.getenv("OLLAMA_CHAT_URL", "http://ollama-llm-chat:11434")
        self.model = os.getenv("OLLAMA_CHAT_MODEL", "gemma:2b")
        
        # Initialize LangChain ChatOllama
        self.llm = ChatOllama(
            base_url=self.base_url,
            model=self.model,
            temperature=0, # Optional: deterministic responses
        )
        logger.info(f"Initialized ChatOllama with model: {self.model}")

    def generate_response(self, system_prompt: str, context: str, user_prompt: str) -> tuple[str, list[BaseMessage]]:
        """Generates a response from the LLM and returns the content plus the messages used."""
        try:
            logger.info(f"Sending chat request to Ollama via LangChain: {self.model}")
            
            # Combine context and user prompt for the model
            final_user_content = f"\n\n#Context:\n{context}\n\n#User Question:\n{user_prompt}"
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=final_user_content),
            ]
            
            logger.info(f"=========> messages to Ollama: {messages}")
            response = self.llm.invoke(messages)    
            logger.info(f"=========> response from Ollama: {response}")
            
            return response.content.strip(), messages
        except Exception as e:
            logger.error(f"Error calling Ollama chat via LangChain: {str(e)}")
            raise
