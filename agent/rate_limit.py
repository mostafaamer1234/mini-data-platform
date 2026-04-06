from __future__ import annotations

import threading
import time
from collections import deque


class SlidingWindowRateLimiter:
    """Limits how many calls can occur within a rolling window (e.g. OpenAI Chat Completions).

    Blocks the current thread until a slot is available. Reduces cost and misuse if a key
    leaks or a loop keeps calling the API.
    """

    __slots__ = ("_lock", "_max_calls", "_timestamps", "window_seconds")

    def __init__(self, max_calls: int, window_seconds: float = 60.0) -> None:
        if max_calls < 1:
            raise ValueError("max_calls must be >= 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        self._max_calls = max_calls
        self.window_seconds = window_seconds
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Wait until this call may proceed (one slot consumed)."""
        while True:
            sleep_for: float
            with self._lock:
                now = time.monotonic()
                cutoff = now - self.window_seconds
                while self._timestamps and self._timestamps[0] < cutoff:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._max_calls:
                    self._timestamps.append(now)
                    return
                sleep_for = self._timestamps[0] + self.window_seconds - now
            time.sleep(max(sleep_for, 0.001))
