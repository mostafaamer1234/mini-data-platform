from __future__ import annotations

from dataclasses import dataclass

from agent.models import ReviewResult


@dataclass(slots=True)
class Reviewer:
    def review(
        self,
        question: str,
        sql: str,
        row_count: int,
        assumptions: list[str],
    ) -> ReviewResult:
        notes: list[str] = []
        confidence = "medium"

        lower_q = question.lower()
        lower_sql = sql.lower()

        if row_count == 0:
            confidence = "low"
            notes.append("Query returned no rows; answer may be incomplete.")
        if "last quarter" in lower_q and "date_trunc" not in lower_sql and "quarter" not in lower_sql:
            confidence = "low"
            notes.append("Question asked for last quarter but SQL does not reference quarter logic.")
        if "lifetime value" in lower_q and "sum(total)" not in lower_sql.replace(" ", ""):
            notes.append("CLV requested; verify whether realized lifetime value was intended.")
        if assumptions:
            notes.append("Answer includes assumptions due to ambiguity.")

        return ReviewResult(confidence=confidence, notes=notes)

