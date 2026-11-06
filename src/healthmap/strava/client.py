"""
Cliente Strava con control de rate limit.

Strava devuelve en cada respuesta:
  X-RateLimit-Limit / X-RateLimit-Usage           → "15min,diario" (global)
  X-ReadRateLimit-Limit / X-ReadRateLimit-Usage   → "15min,diario" (lecturas)
Si nos acercamos al límite de 15 min dormimos hasta la siguiente ventana; si se agota
el diario, se lanza DailyLimitReached y la extracción se reanuda otro día.
"""

import logging
import time
from datetime import datetime, timedelta

import httpx

from healthmap.http import HttpClient
from healthmap.strava.auth import auth_header

logger = logging.getLogger(__name__)

API = "https://www.strava.com/api/v3"
SAFETY = 3


class DailyLimitReached(Exception):
    pass


def parse_pair(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    try:
        a, b = value.split(",")[:2]
        return int(a), int(b)
    except ValueError:
        return None


def seconds_to_next_quarter(now: datetime | None = None) -> float:
    now = now or datetime.now()
    nxt = now.replace(second=0, microsecond=0) + timedelta(minutes=15 - now.minute % 15)
    return (nxt - now).total_seconds() + 5


def usage_state(headers: httpx.Headers) -> tuple[bool, bool]:
    """Devuelve (cerca_limite_15min, cerca_limite_diario) usando el más restrictivo."""
    near_short = near_daily = False
    for prefix in ("X-RateLimit", "X-ReadRateLimit"):
        limit = parse_pair(headers.get(f"{prefix}-Limit"))
        usage = parse_pair(headers.get(f"{prefix}-Usage"))
        if not limit or not usage:
            continue
        near_short |= usage[0] >= limit[0] - SAFETY
        near_daily |= usage[1] >= limit[1] - SAFETY
    return near_short, near_daily


class StravaClient(HttpClient):
    def __init__(self) -> None:
        super().__init__(API, auth_header, min_interval=0.2)

    def after_response(self, response: httpx.Response) -> None:
        near_short, near_daily = usage_state(response.headers)
        if near_daily:
            raise DailyLimitReached(
                "Límite diario de Strava alcanzado. Vuelve a ejecutar mañana: se reanuda donde quedó."
            )
        if near_short:
            wait = seconds_to_next_quarter()
            logger.info("Strava: cerca del límite de 15 min — esperando %.0fs", wait)
            time.sleep(wait)

    def retry_wait(self, response: httpx.Response, attempt: int) -> float:
        if response.status_code == 429:
            return seconds_to_next_quarter()
        return super().retry_wait(response, attempt)
