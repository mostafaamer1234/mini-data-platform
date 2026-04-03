from __future__ import annotations

from dataclasses import dataclass

from agent.tools.sql_tools import DuckDBTools


@dataclass(slots=True)
class MetadataService:
    tools: DuckDBTools

    def build_catalog(self, schemas: list[str]) -> dict:
        catalog: dict[str, dict] = {"schemas": {}}
        tables = self.tools.list_tables(schemas)
        for schema, table in tables:
            schema_bucket = catalog["schemas"].setdefault(schema, {"tables": {}})
            schema_bucket["tables"][table] = {
                "columns": self.tools.describe_table(schema, table),
            }
        return catalog

    def summarize_catalog(self, catalog: dict) -> str:
        lines: list[str] = []
        for schema, schema_data in catalog.get("schemas", {}).items():
            lines.append(f"Schema: {schema}")
            for table, table_data in schema_data.get("tables", {}).items():
                column_names = [c["column_name"] for c in table_data.get("columns", [])]
                lines.append(f"- {schema}.{table}: {', '.join(column_names[:15])}")
        return "\n".join(lines)

