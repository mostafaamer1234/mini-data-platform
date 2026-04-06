#!/usr/bin/env bash
# verify_setup.sh — sanity-check that setup.sh produced a usable warehouse.
# Usage (from repo root): ./scripts/verify_setup.sh
# Requires: ./setup.sh already run; uv on PATH (or python3 -m uv).

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

run_uv() {
  if command -v uv >/dev/null 2>&1; then
    uv "$@"
    return
  fi
  if python3 -m uv --version >/dev/null 2>&1; then
    python3 -m uv "$@"
    return
  fi
  echo "❌ uv not found. Install uv or run: python3 -m pip install --user uv"
  exit 1
}

DB="$PROJECT_ROOT/warehouse/data.duckdb"
if [[ ! -f "$DB" ]]; then
  echo "❌ Missing warehouse at $DB — run ./setup.sh first."
  exit 1
fi

echo "🔎 Verifying DuckDB warehouse and marts..."
run_uv run python <<'PY'
import os
from pathlib import Path

import duckdb

root = Path(os.getcwd()).resolve()
db = root / "warehouse" / "data.duckdb"
con = duckdb.connect(str(db), read_only=True)
schemas = [r[0] for r in con.execute(
    "select schema_name from information_schema.schemata order by 1"
).fetchall()]
if "marts" not in schemas:
    raise SystemExit(f"FAIL: expected schema 'marts', have {schemas}")

tables = con.execute(
    """
    select table_name from information_schema.tables
    where table_schema = 'marts' and table_type in ('BASE TABLE', 'VIEW')
    """
).fetchall()
if not tables:
    raise SystemExit("FAIL: no tables in marts")

n = len(tables)
print(f"✓ DuckDB OK: {n} object(s) in marts (e.g. {tables[0][0]!r})")
con.close()
PY

echo "✅ verify_setup: all checks passed."
