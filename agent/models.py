from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


def _normalize_confidence(value: object) -> str:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"low", "medium", "high"}:
            return text
        try:
            numeric = float(text)
        except ValueError:
            return "medium"
    elif isinstance(value, (int, float)):
        numeric = float(value)
    else:
        return "medium"

    # Map numeric confidence to the app's qualitative scale.
    if numeric >= 0.8:
        return "high"
    if numeric >= 0.5:
        return "medium"
    return "low"


class Plan(BaseModel):
    intent: str
    needs_clarification: bool = False
    assumptions: list[str] = Field(default_factory=list)
    schema_scope: list[str] = Field(default_factory=list)
    steps: list[str] = Field(default_factory=list)


class SQLQuery(BaseModel):
    sql: str
    rationale: str
    expected_columns: list[str] = Field(default_factory=list)
    expected_grain: str = "unknown"
    safety_notes: list[str] = Field(default_factory=list)


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[dict]
    row_count: int


class ReviewResult(BaseModel):
    confidence: str
    notes: list[str] = Field(default_factory=list)

    @field_validator("confidence", mode="before")
    @classmethod
    def normalize_confidence(cls, value: object) -> str:
        return _normalize_confidence(value)


class Answer(BaseModel):
    narrative: str
    assumptions: list[str] = Field(default_factory=list)
    follow_ups: list[str] = Field(default_factory=list)
    confidence: str = "medium"

    @field_validator("confidence", mode="before")
    @classmethod
    def normalize_confidence(cls, value: object) -> str:
        return _normalize_confidence(value)


class AgentResponse(BaseModel):
    plan: Plan
    query: SQLQuery
    result: QueryResult
    review: ReviewResult
    answer: Answer

