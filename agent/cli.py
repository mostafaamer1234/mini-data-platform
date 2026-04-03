from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from agent.orchestrator.agent import AgentOrchestrator
from agent.settings import AgentSettings

app = typer.Typer(help="Agentic CLI for mini-data-platform analytics Q&A")
console = Console()


@app.command()
def ask(
    question: str = typer.Argument(..., help="Natural language analytics question"),
    schema_scope: str = typer.Option(
        "auto", help="Schema policy: auto, marts, or all", case_sensitive=False
    ),
    openai_model: str = typer.Option(
        "gpt-5.4-mini-2026-03-17",
        help="OpenAI model to use for planning, SQL generation, and summarization.",
    ),
    warehouse_path: Optional[Path] = typer.Option(
        None, help="Override DuckDB file path (defaults to warehouse/data.duckdb)"
    ),
    platform_config_path: Optional[Path] = typer.Option(
        None,
        help="Optional platform adapter config JSON (defaults to agent/config/platform.json).",
    ),
    verbose: bool = typer.Option(False, help="Show plan, SQL, and reviewer notes"),
) -> None:
    load_dotenv()

    settings = AgentSettings(openai_model=openai_model)
    if warehouse_path is not None:
        settings.warehouse_path = warehouse_path
    if platform_config_path is not None:
        settings.platform_config_path = platform_config_path

    import os

    if not os.getenv("OPENAI_API_KEY"):
        raise typer.BadParameter(
            "OPENAI_API_KEY is not set. Export it first, then retry the command."
        )

    if not settings.warehouse_path.exists():
        raise typer.BadParameter(
            f"Warehouse does not exist at {settings.warehouse_path}. Run setup first."
        )

    orchestrator = AgentOrchestrator(settings=settings, root=Path.cwd())
    response = orchestrator.run(
        question=question,
        schema_scope_override=None if schema_scope == "auto" else schema_scope,
    )

    console.print("\n[bold green]Answer[/bold green]")
    console.print(response.answer.narrative)
    console.print(f"\n[bold]Confidence:[/bold] {response.answer.confidence}")

    if response.answer.assumptions:
        console.print("\n[bold]Assumptions[/bold]")
        for assumption in response.answer.assumptions:
            console.print(f"- {assumption}")

    if verbose:
        console.print("\n[bold]Plan[/bold]")
        console.print(f"- Intent: {response.plan.intent}")
        console.print(f"- Schema scope: {', '.join(response.plan.schema_scope)}")
        for step in response.plan.steps:
            console.print(f"  - {step}")

        console.print("\n[bold]SQL[/bold]")
        console.print(response.query.sql)

        if response.review.notes:
            console.print("\n[bold]Reviewer Notes[/bold]")
            for note in response.review.notes:
                console.print(f"- {note}")

    table = Table(title="Result Preview")
    for col in response.result.columns[:8]:
        table.add_column(col)
    for row in response.result.rows[:10]:
        table.add_row(*[str(row.get(col, "")) for col in response.result.columns[:8]])
    console.print(table)
    console.print(f"\nRows returned (preview-limited): {response.result.row_count}")
    console.print("\n[dim]Executed SQL is shown with --verbose.[/dim]")


if __name__ == "__main__":
    app()

