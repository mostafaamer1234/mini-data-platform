from __future__ import annotations

import re
from pathlib import Path

import duckdb

from agent.models import QueryResult


class DuckDBTools:
    def __init__(self, warehouse_path: Path) -> None:
        self.warehouse_path = warehouse_path

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.warehouse_path), read_only=True)

    def list_schemas(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                select schema_name
                from information_schema.schemata
                order by schema_name
                """
            ).fetchall()
        return [row[0] for row in rows]

    def list_tables(self, schemas: list[str]) -> list[tuple[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                select table_schema, table_name
                from information_schema.tables
                where table_type in ('BASE TABLE', 'VIEW')
                  and table_schema = any(?)
                order by table_schema, table_name
                """,
                [schemas],
            ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def describe_table(self, schema: str, table: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                select column_name, data_type, is_nullable
                from information_schema.columns
                where table_schema = ? and table_name = ?
                order by ordinal_position
                """,
                [schema, table],
            ).fetchall()
        return [
            {"column_name": r[0], "data_type": r[1], "is_nullable": r[2]}
            for r in rows
        ]

    def run_sql(self, sql: str, max_rows: int = 200) -> QueryResult:
        # LLMs often emit SQL with trailing semicolons; strip them before nesting.
        normalized_sql = sql.strip().rstrip(";").strip()
        repaired_sql = self._repair_common_dialect_mismatches(normalized_sql)
        bounded_sql = f"select * from ({repaired_sql}) as q limit {max_rows}"
        with self._connect() as conn:
            cursor = conn.execute(bounded_sql)
            columns = [d[0] for d in cursor.description]
            raw_rows = cursor.fetchall()
        rows = [dict(zip(columns, row, strict=False)) for row in raw_rows]
        return QueryResult(columns=columns, rows=rows, row_count=len(rows))

    def _repair_common_dialect_mismatches(self, sql: str) -> str:
        # Convert DATEADD(unit, n, expr) into DuckDB-compatible interval arithmetic.
        # Example: DATEADD('month', -3, CURRENT_DATE) -> (CURRENT_DATE + INTERVAL '-3 month')
        unit_map = {
            "month": "month",
            "day": "day",
            "year": "year",
            "week": "week",
            "quarter": "month",
        }

        def _dateadd_repl(match: re.Match[str]) -> str:
            unit = match.group("unit").lower()
            amount_raw = match.group("amount")
            expr = match.group("expr").strip()
            if unit == "quarter":
                amount = str(int(amount_raw) * 3)
            else:
                amount = amount_raw
            mapped = unit_map[unit]
            return f"({expr} + INTERVAL '{amount} {mapped}')"

        dateadd_pattern = re.compile(
            r"(?is)dateadd\s*\(\s*'?(?P<unit>month|day|year|week|quarter)'?\s*,\s*(?P<amount>[-+]?\d+)\s*,\s*(?P<expr>[^)]+)\)"
        )
        repaired = re.sub(dateadd_pattern, _dateadd_repl, sql)
        repaired = re.sub(r"(?i)\bgetdate\s*\(\s*\)", "CURRENT_TIMESTAMP", repaired)
        return repaired

