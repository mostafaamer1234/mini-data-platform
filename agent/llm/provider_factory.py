from __future__ import annotations

from agent.llm.base import LLMProvider
from agent.llm.openai_provider import OpenAIProvider
from agent.settings import AgentSettings


def get_provider(settings: AgentSettings) -> LLMProvider:
    return OpenAIProvider(model=settings.openai_model)

