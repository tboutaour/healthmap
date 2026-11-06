from collections.abc import Iterator
from datetime import date, timedelta


def daterange(since: date, until: date, step: int = 1) -> Iterator[date]:
    current = since
    while current <= until:
        yield current
        current += timedelta(days=step)


def windows(since: date, until: date, max_days: int) -> Iterator[tuple[date, date]]:
    """Divide [since, until] (inclusivo) en ventanas de como mucho max_days días."""
    if max_days < 1:
        raise ValueError("max_days debe ser >= 1")
    start = since
    while start <= until:
        end = min(start + timedelta(days=max_days - 1), until)
        yield start, end
        start = end + timedelta(days=1)


def is_recent(d: date, today: date, recent_days: int) -> bool:
    """True si la fecha cae en los últimos `recent_days` días (o en el futuro)."""
    return d >= today - timedelta(days=recent_days)
