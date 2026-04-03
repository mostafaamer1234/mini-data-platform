from __future__ import annotations

from datetime import date, datetime
from statistics import mean, pstdev


def rolling_zscore_anomalies(
    rows: list[dict], date_key: str, value_key: str, window: int = 14, z_threshold: float = 2.5
) -> list[dict]:
    values: list[tuple[date | datetime | str, float]] = []
    for row in rows:
        raw_value = row.get(value_key)
        raw_date = row.get(date_key)
        if raw_value is None or raw_date is None:
            continue
        try:
            values.append((raw_date, float(raw_value)))
        except (TypeError, ValueError):
            continue

    anomalies: list[dict] = []
    series = [v for _, v in values]
    for idx in range(window, len(series)):
        history = series[idx - window : idx]
        mu = mean(history)
        sigma = pstdev(history) or 1e-9
        z_score = (series[idx] - mu) / sigma
        if abs(z_score) >= z_threshold:
            anomalies.append(
                {
                    "index": idx,
                    "date": values[idx][0],
                    "value": series[idx],
                    "z_score": round(z_score, 3),
                }
            )
    return anomalies

