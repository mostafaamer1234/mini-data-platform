from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from agent.chat_session import SessionMemory
from agent.orchestrator.agent import AgentOrchestrator
from agent.settings import AgentSettings


def main() -> None:
    load_dotenv()
    console = Console()

    if not os.getenv("OPENAI_API_KEY"):
        console.print(
            "[red]OPENAI_API_KEY is not set.[/red] Set it in `.env` or export it, then retry."
        )
        raise SystemExit(1)

    settings = AgentSettings()
    if not settings.warehouse_path.exists():
        console.print(
            f"[red]Warehouse not found at {settings.warehouse_path}.[/red] Run setup first."
        )
        raise SystemExit(1)

    orchestrator = AgentOrchestrator(settings=settings, root=Path.cwd())
    memory = SessionMemory()
    verbose = False

    console.print("[bold green]Astronomer Agent[/bold green] interactive chat started.")
    console.print(
        "[dim]Commands: /exit, /quit, /reset, /verbose on, /verbose off, /help[/dim]"
    )

    while True:
        try:
            user_input = input("\nastronomer> ").strip()
        except (KeyboardInterrupt, EOFError):
            console.print("\n[dim]Session ended.[/dim]")
            break

        if not user_input:
            continue

        cmd = user_input.lower()
        if cmd in {"/exit", "/quit"}:
            console.print("[dim]Goodbye.[/dim]")
            break
        if cmd == "/reset":
            memory.clear()
            console.print("[yellow]Conversation context cleared.[/yellow]")
            continue
        if cmd == "/help":
            console.print(
                "[dim]/exit /quit /reset /verbose on /verbose off[/dim]"
            )
            continue
        if cmd == "/verbose on":
            verbose = True
            console.print("[yellow]Verbose mode enabled.[/yellow]")
            continue
        if cmd == "/verbose off":
            verbose = False
            console.print("[yellow]Verbose mode disabled.[/yellow]")
            continue

        context = memory.render_context()
        response = orchestrator.run(
            question=user_input,
            schema_scope_override=None,
            conversation_context=context,
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

        memory.add_turn(
            user=user_input,
            assistant=response.answer.narrative,
            sql=response.query.sql,
            confidence=response.answer.confidence,
        )

