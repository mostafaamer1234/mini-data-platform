from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from openai import AuthenticationError
from rich.console import Console
from rich.table import Table

from agent.chat_session import SessionMemory
from agent.orchestrator.agent import AgentOrchestrator
from agent.settings import AgentSettings


def main() -> None:
    load_dotenv()
    console = Console()

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        console.print(
            "[red]OPENAI_API_KEY is not set.[/red] Set it in `.env` or export it, then retry."
        )
        raise SystemExit(1)
    if api_key.startswith("<") or "enter your" in api_key.lower():
        console.print(
            "[red]OPENAI_API_KEY looks like a placeholder (e.g. from a template).[/red] "
            "Replace it with a real secret key from https://platform.openai.com/account/api-keys"
        )
        raise SystemExit(1)

    settings = AgentSettings()
    if os.getenv("AGENT_OPENAI_RATE_LIMIT", "1").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        settings.openai_rate_limit_enabled = False
    # Cap for plan / SQL / summarize Chat Completions only (not embedding API).
    rpm = os.getenv("AGENT_OPENAI_CALLS_PER_MINUTE", "").strip()
    if rpm.isdigit() and int(rpm) >= 1:
        settings.openai_calls_per_minute = int(rpm)
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
        "[dim]Commands: exit, quit, reset, verbose on/off, help (with or without /)[/dim]"
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
        if cmd in {"/exit", "/quit", "exit", "quit"}:
            console.print("[dim]Goodbye.[/dim]")
            break
        if cmd in {"/reset", "reset"}:
            memory.clear()
            console.print("[yellow]Conversation context cleared.[/yellow]")
            continue
        if cmd in {"/help", "help"}:
            console.print(
                "[dim]exit / quit / reset / verbose on / verbose off / help[/dim]"
            )
            continue
        if cmd in {"/verbose on", "verbose on"}:
            verbose = True
            console.print("[yellow]Verbose mode enabled.[/yellow]")
            continue
        if cmd in {"/verbose off", "verbose off"}:
            verbose = False
            console.print("[yellow]Verbose mode disabled.[/yellow]")
            continue

        context = memory.render_context()
        try:
            response = orchestrator.run(
                question=user_input,
                schema_scope_override=None,
                conversation_context=context,
            )
        except AuthenticationError:
            console.print(
                "[red]OpenAI rejected the API key (401).[/red] "
                "Use a valid key from https://platform.openai.com/account/api-keys — "
                "update `.env` (OPENAI_API_KEY=sk-...) or export it, then restart."
            )
            continue

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
