# Mini Data Platform Agent

This project is a synthetic analytics platform plus an AI-powered CLI agent for ad-hoc data questions.

It combines:

- A synthetic e-commerce data pipeline (`sources` -> `raw` -> `staging` -> `marts`)
- A DuckDB warehouse for analytical querying
- dbt models for transformations
- Airflow DAGs for ingestion/transformation orchestration
- Evidence dashboards for BI exploration
- An agentic CLI (`mini-data-agent`, `astronomer`) that turns natural language questions into validated SQL answers

Core goal: let a user ask business questions from dataset in plain language and get grounded, query-backed answers with confidence and assumptions.

## Inference-First Principle

This implementation is intentionally designed to infer behavior from:

- code artifacts (dbt SQL/YAML, Evidence SQL)
- warehouse metadata (`information_schema`)
- query execution feedback

instead of relying on explicit narrative documentation about the data platform upfront.

When data files or models are replaced, the agent should still reason from discovered structure first, then use any available docs only as secondary context.

---

## System Design and Architecture

The agent runtime follows a workflow-first, multi-stage architecture:

1. **Orchestrator** (`agent/orchestrator/agent.py`)
   - Coordinates the full lifecycle for each question
   - Selects schema scope
   - Builds metadata context
   - Runs retrieval (advanced hybrid RAG)
   - Calls planner and SQL generation LLM steps
   - Validates and executes SQL
   - Calls summarization

2. **Planner (LLM call #1)**
   - Produces structured intent and plan (`Plan`)
   - Captures assumptions and target schema scope

3. **SQL Generator (LLM call #2)**
   - Produces structured SQL output (`SQLQuery`)
   - Uses catalog summary + retrieved context + platform contract
   - Retries with error feedback when query fails validation/execution

4. **Validation + Execution**
   - SQL safety checks (`SELECT`/`WITH` only, schema allowlist)
   - Read-only DuckDB execution
   - Result preview extraction

5. **Reviewer**
   - Lightweight post-check confidence and notes

6. **Summarizer (LLM call #3)**
   - Produces business-facing narrative (`Answer`)
   - Includes assumptions/follow-ups/confidence

### Agent Data Flow

Question -> plan -> SQL -> validation -> DuckDB -> result -> summary

With context enrichment from:

- Warehouse metadata (`information_schema` + dbt-facing table structures)
- Retrieval corpus (code-first snippets via hybrid RAG)
- Optional session memory in chat mode

---

## RAG Pipeline

The project includes a hybrid RAG implementation (`agent/retrieval/service.py`):

- **Chunking** with overlap
- **Lexical retrieval** (BM25-style scoring)
- **Semantic retrieval** (OpenAI embeddings, when API key is available)
- **Rank fusion** (reciprocal rank fusion)
- **MMR diversification** to avoid redundant chunks
- **Persistent local cache** (`agent/cache/rag_index.json`) with file-manifest invalidation

This gives grounded SQL generation while remaining resilient if embeddings are unavailable (lexical fallback still works).

Retrieval priority is inference-first:

1. dbt model SQL/YAML
2. Evidence SQL sources
3. Optional page/documentation markdown

This keeps behavior robust even when explicit docs are incomplete or absent.

---

## Platform Adaptation and Genericity

The agent is designed to be reused across similar mini data platforms.

### Platform Adapter

Config file: `agent/config/platform.json`

Defines mapping for:

- Primary fact table (`orders_table`)
- Date/revenue/transaction/user key columns
- Web analytics routing hints
- Retrieval corpus paths

### Runtime Inference

If defaults do not fit a new dataset, the system attempts to infer a compatible primary table and key columns from DuckDB metadata at runtime.

This reduces hard dependencies on a specific schema layout.

### Inference-First Retrieval

The retrieval layer is configured to prioritize discoverable technical artifacts (dbt SQL/YAML, Evidence SQL sources) and uses path discovery as a fallback/merge strategy. This keeps the agent grounded in code + warehouse metadata rather than relying on explicit prose documentation.

In practical terms, replacing the underlying data/model files should still give the agent enough structural signal to adapt without hand-authored platform notes.

---

## Features

- Natural-language analytics Q&A over DuckDB
- Interactive multi-turn chat (`astronomer`)
- Structured outputs (`Plan`, `SQLQuery`, `Answer`)
- SQL safety guardrails and read-only execution
- AI Answer Includes confidence and assumptions for maximum transperacy
- Query regeneration retries on failure
- Relative-period fallback behavior for sparse/dated datasets
- Co-purchase guardrail (`pair_count` transaction-based intent)
- Advanced hybrid RAG with caching
- Configurable platform adapter for reuse
- Evaluation harness for regression checks

---

## Repository Structure

```text
mini-data-platform/
├── agent/                               # AI agent runtime
│   ├── cli.py                           # One-shot CLI command (`mini-data-agent`)
│   ├── chat_cli.py                      # Interactive chat entrypoint (`astronomer`)
│   ├── settings.py                      # Runtime settings/model/RAG controls
│   ├── models.py                        # Pydantic response/request models
│   ├── chat_session.py                  # Session memory rendering
│   ├── orchestrator/
│   │   └── agent.py                     # Planner -> SQL -> execute -> summarize loop
│   ├── llm/
│   │   ├── openai_provider.py           # OpenAI planner/sql/summarizer calls
│   │   ├── base.py                      # Provider protocol
│   │   └── json_utils.py                # Safe JSON normalization
│   ├── retrieval/
│   │   ├── service.py                   # Hybrid RAG (chunking, lexical, embeddings, RRF, MMR)
│   │   └── __init__.py
│   ├── validation/
│   │   └── sql_validator.py             # SQL safety policy
│   ├── tools/
│   │   └── sql_tools.py                 # DuckDB introspection + execution helpers
│   ├── metadata/
│   │   └── service.py                   # Catalog builder/summarizer
│   ├── reviewer/
│   │   └── reviewer.py                  # Confidence/reviewer notes
│   ├── platform/
│   │   └── adapter.py                   # Platform config + runtime schema inference
│   ├── config/
│   │   ├── platform.json                # Platform adapter mapping
│   │   └── metrics.json                 # Metric seed definitions
│   ├── evals/
│   │   ├── questions.json               # Benchmark question set
│   │   └── run_eval.py                  # Eval runner
│   └── cache/
│       └── rag_index.json               # Local retrieval index cache
├── airflow/
│   ├── dags/
│   │   ├── ingest_products.py
│   │   ├── ingest_users.py
│   │   ├── ingest_transactions.py
│   │   ├── ingest_campaigns.py
│   │   ├── ingest_pageviews.py
│   │   ├── run_dbt.py
│   │   └── build_evidence.py
│   └── utils/
│       └── warehouse.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/                     # stg_* models + source configs
│       └── marts/                       # dim_* + fct_orders
├── evidence/
│   ├── package.json
│   ├── pages/                           # BI dashboard pages
│   └── sources/warehouse/               # SQL source definitions used by Evidence
├── scripts/
│   └── generate_all.py                  # Synthetic source-data generator
├── sources/                             # Generated CSV source files
├── warehouse/
│   └── data.duckdb                      # Analytical warehouse
├── setup.sh                             # One-command bootstrap
├── pyproject.toml                       # Python deps + CLI scripts
└── README.md / README2.md               # Docs (`README2.md` is canonical)
```

---

## Data Pipeline Layers

- **Raw (`raw`)**: loaded from source files by ingestion DAGs
- **Staging (`staging`)**: cleaned/standardized dbt models (`stg_*`)
- **Marts (`marts`)**: analytics-ready dimensions/facts (for example `dim_customers`, `dim_products`, `fct_orders`)

Default agent behavior is **marts-first**, then expand scope when needed.

---

## Installation and Setup

## Prerequisites

- `python >= 3.11`
- `node >= 18`
- `npm >= 7`

`setup.sh` will automatically install `uv` (via `python3 -m pip install --user uv`) if `uv` is not already available.

## One-command bootstrap

From repo root:

```bash
./setup.sh
```

This command:

1. Installs Python dependencies (`uv sync`)
2. Installs Evidence dependencies (`npm install`)
3. Generates synthetic data
4. Initializes Airflow metadata DB
5. Runs ingestion scripts into DuckDB
6. Runs dbt transformations
7. Builds Evidence sources

### Optional manual path (if you want step-by-step control)

```bash
# install deps
uv sync

# generate source files
uv run python scripts/generate_all.py

# airflow + ingestion + dbt
cd airflow
export AIRFLOW_HOME=$(pwd)
uv run airflow db migrate
uv run python dags/ingest_products.py
uv run python dags/ingest_users.py
uv run python dags/ingest_transactions.py
uv run python dags/ingest_campaigns.py
uv run python dags/ingest_pageviews.py
uv run python dags/run_dbt.py
```

---

## Running the System

### 1) Interactive chat mode

```bash
export OPENAI_API_KEY=your_key_here
astronomer
```

Chat commands:

- `/help`
- `/verbose on`
- `/verbose off`
- `/reset`
- `/exit` or `/quit`

### 2) Start dashboards

```bash
cd evidence
npm run dev
```

Then open: `http://localhost:3000`

---

## Configuration

### Model selection

Default model is configured in `agent/settings.py` and can be overridden:

```bash
uv run mini-data-agent "question" --openai-model gpt-5.4-mini-2026-03-17
```

### Platform adapter override

```bash
uv run mini-data-agent "question" --platform-config-path ./my-platform.json --verbose
```

### Schema scope

- `auto` (default): marts-first, expands for web analytics intents
- `marts`
- `all`

```bash
uv run mini-data-agent "Which page types generate most sessions?" --schema-scope all
```

---

## Evaluation / Regression Testing

Run eval harness:

```bash
uv run python -m agent.evals.run_eval
```

Outputs are written to:

- `agent/evals/latest_results.json`

For stronger reliability, run repeated hard-question sweeps and compare answers against direct DuckDB validation queries.

---

## Security and Safety Posture

- Read-only DuckDB connections for execution
- Rate limiting on OpenAI Chat Completions (plan, SQL generation, summarization): a sliding-window cap per process limits burst calls and reduces cost or misuse if an API key is exposed. Configure with `AGENT_OPENAI_CALLS_PER_MINUTE` and `AGENT_OPENAI_RATE_LIMIT`, or `mini-data-agent --rate-limit-per-minute` / `--no-rate-limit`.
- SQL safety checks (statement type + schema constraints)
- Controlled LLM payloads with structured JSON responses
- Guardrails for common query failure modes
- API key loaded from environment (`OPENAI_API_KEY`)

---

## Current Limitations

- Metric-heavy intents can still drift when definitions are ambiguous (for example repeat-purchase semantics, co-purchase windows, cancellation rate denominators)
- Narrative summaries can be numerically right but phrased in ways that fail strict automated checks (for example signed percentages vs "X% lower")
- LLM output always has the same structure regardless of query, if I had more time I would make it more flexible.

## Recommended Next Improvements

1. **Deterministic semantic layer**: extend metric contracts and add template SQL for fragile intents (co-purchase, repeat cadence, cancellation semantics) so correctness is not purely LLM-driven when definitions matter.
2. **Observability**: structured tracing (request correlation id, per-step latency, token usage, SQL fingerprint, retrieval chunk ids) for debugging, regression analysis, and production tuning.
3. **Automated testing**: unit tests for SQL validation, platform adapter inference, and RAG indexing; integration tests against a small fixture DuckDB to catch regressions before release.
4. **Production security posture**: secrets only via environment and secret managers in deployment; no real API keys in VCS; optional audit logging for tool and API calls.
5. **Verifier and continuous evaluation**: a verifier step that inspects SQL and result semantics with bounded repair; paired with eval pipelines (scenario generation, semantic scoring, failure clustering) to prioritize fixes.
6. **Grounded provenance**: require answer claims to cite evidence spans (retrieval chunks + SQL fields + result cells) for trust and debuggability.
7. **Secure MCP tool plane**: dedicated MCP server for tools and APIs with mTLS between services, centralized tool registry, and policy-controlled access so capabilities stay portable, auditable, and production-safe.

