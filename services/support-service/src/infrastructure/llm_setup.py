import logging
from langchain_openai import ChatOpenAI
from src.infrastructure.config import settings

logger = logging.getLogger("LLMSetup")

class LLMManager:
    """Manages OpenRouter LLM client connections with Nvidia free models"""
    def __init__(self):
        self._llm: ChatOpenAI | None = None

    def get_llm(self) -> ChatOpenAI:
        """Returns initialized ChatOpenAI instance pointed to OpenRouter API"""
        if self._llm is None:
            logger.info(
                f"Initializing OpenRouter LLM: model={settings.OPENROUTER_MODEL}, "
                f"temp={settings.LLM_TEMPERATURE}, base_url={settings.OPENROUTER_BASE_URL}"
            )
            self._llm = ChatOpenAI(
                model=settings.OPENROUTER_MODEL,
                openai_api_key=settings.OPENROUTER_API_KEY or "dummy_key_if_unspecified",
                openai_api_base=settings.OPENROUTER_BASE_URL,
                temperature=settings.LLM_TEMPERATURE,
                max_tokens=settings.LLM_MAX_TOKENS,
                timeout=settings.LLM_REQUEST_TIMEOUT,
                default_headers={
                    "HTTP-Referer": "http://localhost:8007",
                    "X-Title": "SystemDesign-ECommerce-Support"
                }
            )
        return self._llm

llm_manager = LLMManager()
