from __future__ import annotations

import json
import os
from dataclasses import dataclass

from agent.llm.base import LLMProvider
from agent.llm.json_utils import make_json_safe
from agent.models import Answer, Plan, SQLQuery
from agent.rate_limit import SlidingWindowRateLimiter


@dataclass(slots=True)
class OpenAIProvider(LLMProvider):
    model: str
    rate_limiter: SlidingWindowRateLimiter | None = None

    def _throttle(self) -> None:
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()

    def _client(self):
        from openai import OpenAI

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        return OpenAI(api_key=api_key)

    def plan(
        self,
        question: str,
        schema_scope: list[str],
        conversation_context: str | None = None,
    ) -> Plan:
        prompt = (
            "You are an analytics planner. Return JSON with keys: intent, "
            "needs_clarification, assumptions (array), schema_scope (array), steps (array). "
            "Keep schema_scope exactly as provided."
        )
        client = self._client()
        self._throttle()
        response = client.chat.completions.create(
            model=self.model,
            temperature=0.0,
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": json.dumps(
                        make_json_safe(
                            {
                                "question": question,
                                "schema_scope": schema_scope,
                                "conversation_context": conversation_context,
                            }
                        )
                    ),
                },
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(response.choices[0].message.content or "{}")
        payload.setdefault("schema_scope", schema_scope)
        return Plan.model_validate(payload)

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
        contract = platform_contract or {}
        table_hint = contract.get("orders_table", "marts.fct_orders")
        date_col = contract.get("transaction_date_column", "transaction_date")
        revenue_col = contract.get("revenue_column", "total")
        tx_col = contract.get("transaction_id_column", "transaction_id")
        prompt = (
            "Generate safe DuckDB SQL for analytics. Return JSON with keys: sql, rationale, "
            "expected_columns (array), expected_grain, safety_notes (array). "
            "Only output SELECT/CTE queries. Use only allowed schema scope. "
            "Use DuckDB SQL dialect only. Do not use DATEADD, GETDATE, DATEDIFF, or other SQL Server functions. "
            f"Prefer DuckDB interval arithmetic like: {date_col} >= date_trunc('quarter', current_date) - interval '3 months'. "
            f"For relative time windows (last quarter/month/year), anchor time to available data by using max({date_col}) "
            "from the primary orders table instead of wall-clock current_date. "
            f"For 'bought together' or co-purchase questions, compute pair_count using COUNT(DISTINCT {tx_col}), "
            "not COUNT(*), and ensure pair_count reflects transactions not line-item pair rows. "
            "Do not end SQL with a trailing semicolon. "
            f"Use this platform contract: orders_table={table_hint}, transaction_date_column={date_col}, "
            f"revenue_column={revenue_col}, transaction_id_column={tx_col}."
        )
        client = self._client()
        self._throttle()
        response = client.chat.completions.create(
            model=self.model,
            temperature=0.0,
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": json.dumps(
                        make_json_safe(
                            {
                                "question": question,
                                "plan": plan.model_dump(),
                                "catalog_summary": catalog_summary[:8000],
                                "retrieved_context": retrieved_context[:8000],
                                "platform_contract": contract,
                                "conversation_context": conversation_context[:8000]
                                if conversation_context
                                else None,
                                "previous_error": previous_error,
                            }
                        )
                    ),
                },
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(response.choices[0].message.content or "{}")
        return SQLQuery.model_validate(payload)

    def summarize(
        self,
        question: str,
        query: SQLQuery,
        result_rows: list[dict],
        review_notes: list[str],
        conversation_context: str | None = None,
    ) -> Answer:
        prompt = (
            "Summarize analytics query results for a business user. Return JSON with keys: "
            "narrative, assumptions (array), follow_ups (array), confidence. "
            "Set confidence as one of: low, medium, high."
        )
        client = self._client()
        self._throttle()
        response = client.chat.completions.create(
            model=self.model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": json.dumps(
                        make_json_safe(
                            {
                                "question": question,
                                "sql": query.sql,
                                "sample_rows": result_rows[:20],
                                "review_notes": review_notes,
                                "conversation_context": conversation_context[:8000]
                                if conversation_context
                                else None,
                            }
                        )
                    ),
                },
            ],
            response_format={"type": "json_object"},
        )
        payload = json.loads(response.choices[0].message.content or "{}")
        return Answer.model_validate(payload)

