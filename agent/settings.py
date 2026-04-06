from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class AgentSettings:
    warehouse_path: Path = Path("warehouse/data.duckdb")
    platform_config_path: Path = Path("agent/config/platform.json")
    default_schemas: list[str] = field(default_factory=lambda: ["marts"])
    all_schemas: list[str] = field(default_factory=lambda: ["marts", "staging", "raw"])
    max_rows: int = 200
    query_timeout_seconds: int = 30
    openai_model: str = "gpt-5.4-mini-2026-03-17"
    rag_enabled: bool = True
    rag_embedding_model: str = "text-embedding-3-small"
    rag_chunk_chars: int = 1200
    rag_chunk_overlap: int = 180
    rag_candidate_k: int = 24
    rag_top_k: int = 6
    # Cap OpenAI Chat Completions (plan / SQL / summarize) per rolling minute per process.
    openai_rate_limit_enabled: bool = True
    openai_calls_per_minute: int = 60

