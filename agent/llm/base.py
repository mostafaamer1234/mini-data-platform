from __future__ import annotations

from typing import Protocol

from agent.models import Answer, Plan, SQLQuery


class LLMProvider(Protocol):
    def plan(
        self,
        question: str,
        schema_scope: list[str],
        conversation_context: str | None = None,
    ) -> Plan:
        ...

    def generate_sql(
        self,
        question: str,
        plan: Plan,
        catalog_summary: str,
        retrieved_context: str,
        platform_contract: dict[str, str] | None = None,
        conversation_context: str | None = None,
        previous_error: str | None = None,
    ) -> SQLQuery:
        ...

    def summarize(
        self,
        question: str,
        query: SQLQuery,
        result_rows: list[dict],
        review_notes: list[str],
        conversation_context: str | None = None,
    ) -> Answer:
        ...

