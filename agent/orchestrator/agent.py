from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from agent.analytics.postprocess import rolling_zscore_anomalies
from agent.llm.provider_factory import get_provider
from agent.metadata.service import MetadataService
from agent.models import AgentResponse
from agent.platform.adapter import resolve_platform_adapter
from agent.retrieval.service import RetrievalService
from agent.reviewer.reviewer import Reviewer
from agent.settings import AgentSettings
from agent.tools.sql_tools import DuckDBTools
from agent.validation.sql_validator import validate_sql


@dataclass(slots=True)
class AgentOrchestrator:
    settings: AgentSettings
    root: Path

    def run(
        self,
        question: str,
        schema_scope_override: str | None = None,
        conversation_context: str | None = None,
    ) -> AgentResponse:
        tools = DuckDBTools(self.settings.warehouse_path)
        metadata = MetadataService(tools=tools)
        retrieval = RetrievalService(
            root=self.root,
            embedding_model=self.settings.rag_embedding_model,
            chunk_chars=self.settings.rag_chunk_chars,
            chunk_overlap=self.settings.rag_chunk_overlap,
            candidate_k=self.settings.rag_candidate_k,
        )
        platform = resolve_platform_adapter(self.settings, self.root, tools)
        if self.settings.rag_enabled:
            retrieval.load_corpus(platform.retrieval_candidates(self.root))
        reviewer = Reviewer()
        provider = get_provider(self.settings)

        schema_scope = self._schema_scope(
            question,
            schema_scope_override,
            web_analytics_hints=platform.web_analytics_hints,
        )
        catalog = metadata.build_catalog(schema_scope)
        catalog_summary = metadata.summarize_catalog(catalog)
        snippets = retrieval.retrieve_context(question, limit=self.settings.rag_top_k) if self.settings.rag_enabled else []
        retrieved_context = "\n\n".join(
            [
                f"[{s.path}] fused={s.fused_score:.4f} lexical={s.lexical_score:.4f} semantic={s.semantic_score:.4f}\n{s.content}"
                for s in snippets
            ]
        )

        plan = provider.plan(question, schema_scope, conversation_context=conversation_context)
        query = provider.generate_sql(
            question=question,
            plan=plan,
            catalog_summary=catalog_summary,
            retrieved_context=retrieved_context,
            platform_contract=platform.llm_contract(),
            conversation_context=conversation_context,
        )
        last_error: str | None = None
        result = None
        for attempt in range(3):
            try:
                validate_sql(query.sql, allowed_schemas=schema_scope)
                if self._is_bought_together_question(question):
                    query.sql = self._enforce_distinct_pair_count(
                        query.sql,
                        transaction_id_column=platform.transaction_id_column,
                    )
                result = tools.run_sql(query.sql, max_rows=self.settings.max_rows)
                break
            except Exception as exc:
                last_error = str(exc)
                if attempt == 2:
                    raise RuntimeError(
                        f"Query generation failed after retry. Last error: {last_error}"
                    ) from exc
                query = provider.generate_sql(
                    question=question,
                    plan=plan,
                    catalog_summary=catalog_summary,
                    retrieved_context=retrieved_context,
                    platform_contract=platform.llm_contract(),
                    conversation_context=conversation_context,
                    previous_error=last_error,
                )
                query.safety_notes.append(
                    "SQL regenerated after validation/execution failure in previous attempt."
                )

        # If aggregate output is null for relative time questions, retry with data-anchored window.
        if self._is_relative_period_question(question) and result and self._result_is_all_null(result):
            fallback_sql = """
            WITH bounds AS (
                SELECT MAX({transaction_date_column}) AS max_txn_date
                FROM {orders_table}
            ),
            q AS (
                SELECT
                    date_trunc('quarter', max_txn_date) - interval '3 months' AS start_q,
                    date_trunc('quarter', max_txn_date) AS end_q
                FROM bounds
            )
            SELECT
                MIN(q.start_q) AS quarter_start,
                MIN(q.end_q) - interval '1 day' AS quarter_end,
                SUM(f.{revenue_column}) AS total_sales_last_quarter,
                COUNT(DISTINCT f.{transaction_id_column}) AS total_orders_last_quarter
            FROM {orders_table} f
            CROSS JOIN q
            WHERE f.{transaction_date_column} >= q.start_q
              AND f.{transaction_date_column} < q.end_q
            """.format(
                orders_table=platform.orders_table,
                transaction_date_column=platform.transaction_date_column,
                revenue_column=platform.revenue_column,
                transaction_id_column=platform.transaction_id_column,
            )
            try:
                validate_sql(fallback_sql, allowed_schemas=schema_scope)
                result = tools.run_sql(fallback_sql, max_rows=self.settings.max_rows)
                query.sql = fallback_sql
                query.safety_notes.append(
                    "Applied data-anchored relative-period fallback using max(transaction_date)."
                )
            except Exception:
                # Keep original result if fallback unexpectedly fails.
                pass

        assumptions = list(plan.assumptions)
        anomaly_notes: list[str] = []
        if "anomal" in question.lower() and result and result.rows:
            anomalies = rolling_zscore_anomalies(result.rows, "day", "revenue")
            if anomalies:
                anomaly_notes.append(
                    f"Detected {len(anomalies)} rolling z-score anomalies in the returned series."
                )
            else:
                anomaly_notes.append("No rolling z-score anomalies detected in returned series.")

        if result is None:
            raise RuntimeError("Query execution did not return a result.")

        review = reviewer.review(question, query.sql, result.row_count, assumptions + anomaly_notes)
        answer = provider.summarize(
            question=question,
            query=query,
            result_rows=result.rows,
            review_notes=review.notes + anomaly_notes,
            conversation_context=conversation_context,
        )
        if anomaly_notes:
            answer.assumptions.extend(anomaly_notes)

        return AgentResponse(
            plan=plan,
            query=query,
            result=result,
            review=review,
            answer=answer,
        )

    def _schema_scope(
        self,
        question: str,
        schema_scope_override: str | None,
        web_analytics_hints: list[str],
    ) -> list[str]:
        if schema_scope_override == "all":
            return self.settings.all_schemas
        if schema_scope_override == "marts":
            return self.settings.default_schemas

        lower = question.lower()
        if any(token in lower for token in web_analytics_hints):
            return self.settings.all_schemas
        return self.settings.default_schemas

    def _is_bought_together_question(self, question: str) -> bool:
        lower = question.lower()
        return (
            "bought together" in lower
            or "frequently bought" in lower
            or "co-purchase" in lower
            or "co purchase" in lower
        )

    def _is_relative_period_question(self, question: str) -> bool:
        lower = question.lower()
        return any(token in lower for token in ("last quarter", "last month", "last year"))

    def _result_is_all_null(self, result) -> bool:
        if result.row_count != 1 or not result.rows:
            return False
        first = result.rows[0]
        return all(value is None for value in first.values())

    def _enforce_distinct_pair_count(self, sql: str, transaction_id_column: str) -> str:
        # Guardrail for market basket questions: pair_count should be transaction-based.
        lower = sql.lower()
        if "pair_count" in lower and "count(distinct" not in lower:
            replacement = f"COUNT(DISTINCT {transaction_id_column}) AS pair_count"
            sql = sql.replace("COUNT(*) AS pair_count", replacement)
            sql = sql.replace("count(*) as pair_count", replacement)
        return sql

