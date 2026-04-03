from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class ConversationTurn:
    user: str
    assistant: str
    sql: str
    confidence: str


@dataclass(slots=True)
class SessionMemory:
    turns: list[ConversationTurn] = field(default_factory=list)

    def add_turn(self, user: str, assistant: str, sql: str, confidence: str) -> None:
        self.turns.append(
            ConversationTurn(
                user=user.strip(),
                assistant=assistant.strip(),
                sql=sql.strip(),
                confidence=confidence.strip(),
            )
        )

    def clear(self) -> None:
        self.turns.clear()

    def render_context(self, max_turns: int = 20) -> str:
        if not self.turns:
            return ""
        tail = self.turns[-max_turns:]
        lines: list[str] = ["Conversation history:"]
        for idx, turn in enumerate(tail, start=1):
            lines.append(f"Turn {idx} user: {turn.user}")
            lines.append(f"Turn {idx} assistant: {turn.assistant}")
            lines.append(f"Turn {idx} confidence: {turn.confidence}")
            lines.append(f"Turn {idx} sql: {turn.sql}")
        return "\n".join(lines)

