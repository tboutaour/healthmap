"""
Extracción Strava: perfil, estadísticas, zonas, todas las actividades (todos los deportes)
y por cada una: detalle, streams y zonas. Más el material (gear) usado.
"""

import json
import logging
from datetime import date, datetime

from healthmap.raw_store import RawStore
from healthmap.strava.client import DailyLimitReached, StravaClient

logger = logging.getLogger(__name__)

SOURCE = "strava"
STREAM_KEYS = (
    "time,distance,latlng,altitude,heartrate,cadence,watts,velocity_smooth,"
    "grade_smooth,temp,moving"
)
ALL_NAMES = ["athlete", "athlete_stats", "athlete_zones", "activities_list",
             "activity_detail", "activity_streams", "activity_zones", "gear"]


class StravaExtractor:
    def __init__(self, store: RawStore, force: bool = False):
        self.store = store
        self.force = force
        self.client = StravaClient()
        self.stats: dict[str, dict[str, int]] = {}

    def _stat(self, endpoint: str, what: str) -> None:
        self.stats.setdefault(endpoint, {}).setdefault(what, 0)
        self.stats[endpoint][what] += 1

    def _fetch(self, endpoint: str, key: str, path: str, params: dict | None = None,
               refresh: bool = False):
        if not (self.force or refresh) and self.store.has(SOURCE, endpoint, key):
            self._stat(endpoint, "skipped")
            p = self.store.path(SOURCE, endpoint, key)
            return json.loads(p.read_text("utf-8")) if p.exists() else None

        resp = self.client.get(path, params=params)
        if resp.status_code == 404:
            self.store.mark_empty(SOURCE, endpoint, key, "404")
            self._stat(endpoint, "empty")
            return None
        if resp.status_code == 403 and "Inactive" in resp.text:
            raise RuntimeError(
                "La aplicación de Strava está inactiva (Application Status: Inactive). "
                "Reactívala en https://www.strava.com/settings/api y vuelve a ejecutar."
            )
        if resp.status_code >= 400:
            self._stat(endpoint, "error")
            logger.warning("Strava %s %s → HTTP %s %s", endpoint, key, resp.status_code,
                           resp.text[:200])
            return None
        payload = resp.json()
        self.store.write_json(SOURCE, endpoint, key, payload)
        self._stat(endpoint, "fetched")
        return payload

    def _want(self, name: str, only: set[str] | None) -> bool:
        return not only or name in only

    def run(self, since: date, until: date, only: set[str] | None = None,
            limit: int | None = None) -> dict:
        try:
            self._run(since, until, only, limit)
        except DailyLimitReached as e:
            logger.warning(str(e))
            self._stat("_rate_limit", "daily_limit_reached")
        finally:
            self.client.close()
            logger.info("Strava · llamadas HTTP: %d", self.client.calls)
        return self.stats

    def _run(self, since: date, until: date, only: set[str] | None, limit: int | None) -> None:
        today = date.today().isoformat()
        athlete = self._fetch("athlete", today, "/athlete", refresh=True) or {}
        if athlete.get("id") and self._want("athlete_stats", only):
            self._fetch("athlete_stats", today, f"/athletes/{athlete['id']}/stats", refresh=True)
        if self._want("athlete_zones", only):
            self._fetch("athlete_zones", today, "/athlete/zones", refresh=True)

        # Listado: siempre se refresca (pocas llamadas y detecta actividades nuevas)
        after = int(datetime(since.year, since.month, since.day).timestamp())
        before = int(datetime(until.year, until.month, until.day, 23, 59, 59).timestamp())
        activities: list[dict] = []
        page = 1
        while True:
            batch = self._fetch(
                "activities_list", f"{since}_{until}_{page:03d}", "/athlete/activities",
                {"after": after, "before": before, "page": page, "per_page": 200}, refresh=True,
            ) or []
            activities.extend(batch)
            if len(batch) < 200:
                break
            page += 1
        logger.info("Strava · %d actividades entre %s y %s", len(activities), since, until)

        # Las más recientes primero se procesan al final; orden cronológico
        activities.sort(key=lambda a: a.get("start_date", ""))
        if limit:
            activities = activities[-limit:]

        gear_ids: set[str] = set()
        for i, act in enumerate(activities, 1):
            aid = act["id"]
            if act.get("gear_id"):
                gear_ids.add(act["gear_id"])
            if self._want("activity_detail", only):
                self._fetch("activity_detail", str(aid), f"/activities/{aid}",
                            {"include_all_efforts": "true"})
            if self._want("activity_streams", only):
                self._fetch("activity_streams", str(aid), f"/activities/{aid}/streams",
                            {"keys": STREAM_KEYS, "key_by_type": "true"})
            if self._want("activity_zones", only):
                self._fetch("activity_zones", str(aid), f"/activities/{aid}/zones")
            if i % 20 == 0:
                logger.info("Strava · actividades procesadas: %d/%d", i, len(activities))

        if self._want("gear", only):
            for gid in sorted(gear_ids):
                self._fetch("gear", gid, f"/gear/{gid}")
