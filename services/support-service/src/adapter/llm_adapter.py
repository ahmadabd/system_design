import logging
from typing import List, AsyncIterator
from langchain_core.messages import BaseMessage
from shared.common.resilience import AsyncCircuitBreaker
from src.infrastructure.llm_setup import LLMManager

logger = logging.getLogger("LLMAdapter")

class OpenRouterLLMAdapter:
    """Adapter for invoking OpenRouter Nvidia LLM with resilience and streaming support"""
    def __init__(self, manager: LLMManager):
        self.manager = manager
        self.breaker = AsyncCircuitBreaker(
            name="openrouter-breaker",
            failure_threshold=3,
            recovery_timeout=15.0
        )

    async def invoke(self, messages: List[BaseMessage]) -> str:
        """Invokes the OpenRouter LLM under circuit breaker protection"""
        async def _invoke():
            llm = self.manager.get_llm()
            response = await llm.ainvoke(messages)
            return response.content

        return await self.breaker.call(_invoke)

    async def stream(self, messages: List[BaseMessage]) -> AsyncIterator[str]:
        """Streams tokens from OpenRouter LLM under circuit breaker protection"""
        llm = self.manager.get_llm()
        async for chunk in llm.astream(messages):
            if chunk.content:
                yield chunk.content
