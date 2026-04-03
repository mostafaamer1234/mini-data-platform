from __future__ import annotations

import json
import os
from pathlib import Path

from agent.orchestrator.agent import AgentOrchestrator
from agent.settings import AgentSettings


def main() -> None:
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required to run evals.")

    root = Path.cwd()
    eval_file = root / "agent/evals/questions.json"
    questions = json.loads(eval_file.read_text(encoding="utf-8"))

    settings = AgentSettings()
    orchestrator = AgentOrchestrator(settings=settings, root=root)

    results: list[dict] = []
    for q in questions:
        response = orchestrator.run(q["question"])
        results.append(
            {
                "id": q["id"],
                "question": q["question"],
                "confidence": response.answer.confidence,
                "sql": response.query.sql,
                "row_count": response.result.row_count,
                "review_notes": response.review.notes,
            }
        )

    out_file = root / "agent/evals/latest_results.json"
    out_file.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Wrote eval output to {out_file}")


if __name__ == "__main__":
    main()

