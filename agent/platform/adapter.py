from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from agent.settings import AgentSettings
from agent.tools.sql_tools import DuckDBTools


@dataclass(slots=True)
class MiniPlatformAdapter:
    orders_table: str = "marts.fct_orders"
    transaction_date_column: str = "transaction_date"
    revenue_column: str = "total"
    transaction_id_column: str = "transaction_id"
    user_id_column: str = "user_id"
    web_analytics_hints: list[str] = field(
        default_factory=lambda: [
            "pageview",
            "pageviews",
            "session",
            "sessions",
            "browser",
            "device",
            "traffic",
            "funnel",
            "page_type",
        ]
    )
    retrieval_corpus_paths: list[str] = field(
        default_factory=lambda: [
            "dbt_project/models/marts/fct_orders.sql",
            "dbt_project/models/staging/stg_pageviews.sql",
            "dbt_project/models/staging/_sources.yml",
        ]
    )

    def retrieval_candidates(self, root: Path) -> list[tuple[str, Path]]:
        candidates: list[tuple[str, Path]] = []
        for rel_path in self.retrieval_corpus_paths:
            source = "dbt" if rel_path.startswith("dbt_project/") else "evidence"
            candidates.append((source, root / rel_path))
        existing = [(source, path) for source, path in candidates if path.exists()]

        # Discovery-first merge so adapter works when data platform files differ.
        discovered: list[tuple[str, Path]] = []
        for path in sorted((root / "dbt_project/models").rglob("*.sql"))[:24]:
            discovered.append(("dbt", path))
        for path in sorted((root / "dbt_project/models").rglob("*.yml"))[:16]:
            discovered.append(("dbt", path))
        for path in sorted((root / "dbt_project/models").rglob("*.yaml"))[:16]:
            discovered.append(("dbt", path))
        for path in sorted((root / "evidence/sources").rglob("*.sql"))[:12]:
            discovered.append(("evidence", path))
        for path in sorted((root / "evidence/pages").glob("*.md"))[:8]:
            discovered.append(("evidence", path))

        merged: list[tuple[str, Path]] = []
        seen: set[str] = set()
        for source, path in [*existing, *discovered]:
            key = str(path)
            if key in seen or not path.exists():
                continue
            seen.add(key)
            merged.append((source, path))
        return merged
        return candidates

    def llm_contract(self) -> dict[str, str]:
        return {
            "orders_table": self.orders_table,
            "transaction_date_column": self.transaction_date_column,
            "revenue_column": self.revenue_column,
            "transaction_id_column": self.transaction_id_column,
            "user_id_column": self.user_id_column,
        }


def load_platform_adapter(settings: AgentSettings, root: Path) -> MiniPlatformAdapter:
    path = settings.platform_config_path
    if not path:
        return MiniPlatformAdapter()

    config_path = path if path.is_absolute() else root / path
    if not config_path.exists():
        return MiniPlatformAdapter()

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return MiniPlatformAdapter(
        orders_table=payload.get("orders_table", "marts.fct_orders"),
        transaction_date_column=payload.get("transaction_date_column", "transaction_date"),
        revenue_column=payload.get("revenue_column", "total"),
        transaction_id_column=payload.get("transaction_id_column", "transaction_id"),
        user_id_column=payload.get("user_id_column", "user_id"),
        web_analytics_hints=payload.get("web_analytics_hints")
        or MiniPlatformAdapter().web_analytics_hints,
        retrieval_corpus_paths=payload.get("retrieval_corpus_paths")
        or MiniPlatformAdapter().retrieval_corpus_paths,
    )


def resolve_platform_adapter(
    settings: AgentSettings,
    root: Path,
    tools: DuckDBTools,
) -> MiniPlatformAdapter:
    adapter = load_platform_adapter(settings, root)
    inferred = _infer_from_warehouse(tools)
    if inferred is None:
        return adapter

    # Prefer explicit config values, but replace non-existing defaults when inference is strong.
    default = MiniPlatformAdapter()
    if adapter.orders_table == default.orders_table:
        adapter.orders_table = inferred.orders_table
    if adapter.transaction_date_column == default.transaction_date_column:
        adapter.transaction_date_column = inferred.transaction_date_column
    if adapter.revenue_column == default.revenue_column:
        adapter.revenue_column = inferred.revenue_column
    if adapter.transaction_id_column == default.transaction_id_column:
        adapter.transaction_id_column = inferred.transaction_id_column
    if adapter.user_id_column == default.user_id_column:
        adapter.user_id_column = inferred.user_id_column
    return adapter


def _infer_from_warehouse(tools: DuckDBTools) -> MiniPlatformAdapter | None:
    try:
        schemas = [s for s in tools.list_schemas() if s not in {"information_schema", "pg_catalog"}]
        tables = tools.list_tables(schemas)
    except Exception:
        return None
    if not tables:
        return None

    table_columns: dict[str, list[str]] = {}
    for schema, table in tables:
        fq = f"{schema}.{table}"
        try:
            cols = [c["column_name"] for c in tools.describe_table(schema, table)]
        except Exception:
            continue
        table_columns[fq] = cols
    if not table_columns:
        return None

    best_table = max(table_columns.keys(), key=lambda t: _table_score(table_columns[t]))
    cols = table_columns[best_table]

    return MiniPlatformAdapter(
        orders_table=best_table,
        transaction_date_column=_pick_column(
            cols,
            ["transaction_date", "order_date", "created_at", "event_time", "date", "timestamp"],
            fallback="created_at" if "created_at" in cols else cols[0],
        ),
        revenue_column=_pick_column(
            cols,
            ["total", "revenue", "amount", "sales_amount", "order_total", "gross_amount"],
            fallback=cols[0],
        ),
        transaction_id_column=_pick_column(
            cols,
            ["transaction_id", "order_id", "id", "event_id"],
            fallback=cols[0],
        ),
        user_id_column=_pick_column(
            cols,
            ["user_id", "customer_id", "client_id", "account_id"],
            fallback=cols[0],
        ),
    )


def _table_score(columns: list[str]) -> int:
    lower = {c.lower() for c in columns}
    score = 0
    if {"total", "revenue", "amount"} & lower:
        score += 4
    if {"transaction_date", "order_date", "created_at", "event_time", "date"} & lower:
        score += 3
    if {"transaction_id", "order_id"} & lower:
        score += 3
    if {"user_id", "customer_id"} & lower:
        score += 2
    if {"product_id", "product_name"} & lower:
        score += 1
    return score


def _pick_column(columns: list[str], candidates: list[str], fallback: str) -> str:
    lower_map = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    for candidate in candidates:
        for lower, original in lower_map.items():
            if candidate.lower() in lower:
                return original
    return fallback

