from __future__ import annotations

import re

from sqlglot import exp, parse_one


class SQLValidationError(ValueError):
    pass


BLOCKED_TOKENS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "attach",
    "copy",
    "install",
    "load",
    "call",
    "pragma",
    "vacuum",
}


def validate_sql(sql: str, allowed_schemas: list[str]) -> None:
    cleaned = sql.strip().strip(";")
    lowered = cleaned.lower()
    for token in BLOCKED_TOKENS:
        if re.search(rf"\b{token}\b", lowered):
            raise SQLValidationError(f"Blocked SQL token detected: {token}")

    try:
        expression = parse_one(cleaned, read="duckdb")
    except Exception as exc:  # pragma: no cover
        raise SQLValidationError(f"SQL parse error: {exc}") from exc

    if not isinstance(expression, (exp.Select, exp.With, exp.Subquery, exp.Union)):
        raise SQLValidationError("Only SELECT/CTE queries are allowed")

    tables = list(expression.find_all(exp.Table))
    for table in tables:
        schema_name = table.db
        if schema_name and schema_name not in allowed_schemas:
            raise SQLValidationError(
                f"Schema '{schema_name}' is not allowed for this query"
            )

