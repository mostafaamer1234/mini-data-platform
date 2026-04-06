from __future__ import annotations

from agent.llm.base import LLMProvider
from agent.llm.openai_provider import OpenAIProvider
from agent.rate_limit import SlidingWindowRateLimiter
from agent.settings import AgentSettings


def get_provider(
    settings: AgentSettings,
    rate_limiter: SlidingWindowRateLimiter | None = None,
) -> LLMProvider:
    return OpenAIProvider(model=settings.openai_model, rate_limiter=rate_limiter)

