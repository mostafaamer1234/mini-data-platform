#!/usr/bin/env bash
# setup.sh - One-command bootstrap for mini-data-platform

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_DIR="$PROJECT_ROOT/airflow"
AIRFLOW_DB_PATH="$AIRFLOW_DIR/airflow.db"
EVIDENCE_DIR="$PROJECT_ROOT/evidence"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "❌ Missing required command: $1"
    echo "Install it, then re-run ./setup.sh"
    exit 1
  fi
}

run_uv() {
  if command -v uv >/dev/null 2>&1; then
    uv "$@"
    return
  fi
  if python3 -m uv --version >/dev/null 2>&1; then
    python3 -m uv "$@"
    return
  fi
  echo "❌ uv is required but was not found on PATH and python3 -m uv is unavailable."
  echo "Install uv, then re-run ./setup.sh"
  exit 1
}

ensure_uv() {
  if command -v uv >/dev/null 2>&1; then
    return
  fi
  if python3 -m uv --version >/dev/null 2>&1; then
    return
  fi

  echo "ℹ️  uv not found. Attempting automatic install..."
  if ! python3 -m pip --version >/dev/null 2>&1; then
    python3 -m ensurepip --upgrade >/dev/null 2>&1 || true
  fi

  if python3 -m pip install --user uv; then
    echo "✓ uv installed via python3 -m pip"
    return
  fi

  echo "❌ Failed to install uv automatically."
  echo "Please install uv manually and re-run ./setup.sh"
  exit 1
}

echo "🚀 Bootstrapping Mini Data Platform (one command)..."
echo ""

echo "🔎 Checking prerequisites..."
require_cmd python3
require_cmd node
require_cmd npm
ensure_uv
echo "✓ Found python3, node, npm, and uv"
echo ""

echo "📦 Step 1/7: Installing Python dependencies (uv sync)..."
cd "$PROJECT_ROOT"
run_uv sync
echo "✓ Python dependencies installed"
echo ""

echo "📦 Step 2/7: Installing Evidence dependencies (npm install)..."
cd "$EVIDENCE_DIR"
npm install
echo "✓ Evidence dependencies installed"
echo ""

echo "⚙️  Step 3/7: Configuring Airflow..."
sed -i.bak "s|sql_alchemy_conn = .*|sql_alchemy_conn = sqlite:///$AIRFLOW_DB_PATH|g" "$AIRFLOW_DIR/airflow.cfg"
rm -f "$AIRFLOW_DIR/airflow.cfg.bak"
echo "✓ Airflow configured with database at: $AIRFLOW_DB_PATH"
echo ""

echo "📊 Step 4/7: Generating synthetic data..."
cd "$PROJECT_ROOT"
run_uv run python scripts/generate_all.py
echo "✓ Synthetic data generated"
echo ""

echo "⚙️  Step 5/7: Initializing Airflow metadata database..."
cd "$AIRFLOW_DIR"
export AIRFLOW_HOME="$AIRFLOW_DIR"
run_uv run airflow db migrate
echo "✓ Airflow metadata initialized"
echo ""

echo "📥 Step 6/7: Running ingestion + dbt transformations..."
run_uv run python dags/ingest_products.py
run_uv run python dags/ingest_users.py
run_uv run python dags/ingest_transactions.py
run_uv run python dags/ingest_campaigns.py
run_uv run python dags/ingest_pageviews.py
run_uv run python dags/run_dbt.py
echo "✓ Warehouse loaded and transformed"
echo ""

echo "📈 Step 7/7: Building Evidence sources..."
cd "$EVIDENCE_DIR"
npm run sources
echo "✓ Evidence sources ready"
echo ""

echo "✅ Setup complete. Everything is initialized."
echo ""
echo "Next steps:"
echo "  • Start Evidence dashboard:"
echo "      cd evidence && npm run dev"
echo "  • Ask data questions:"
echo "      export OPENAI_API_KEY=your_key_here"
echo "      astronomer"
echo ""

