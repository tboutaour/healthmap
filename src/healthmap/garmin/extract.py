"""
Extracción Garmin Connect Personal API (pull síncrono).

Los endpoints se declaran en ENDPOINTS con su estrategia:
  - once:  una llamada (perfil, zonas)
  - range: ventanas de como mucho `max_days` días
  - daily: una llamada por día (o cada `step` días)
Las actividades se tratan aparte: listado paginado + detalle + workouts + fichero original.

Spec de referencia: docs/garmin_personal_openapi.json
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

import httpx

from healthmap.config import settings
from healthmap.dates import daterange, is_recent, windows
from healthmap.garmin.auth import BASE_URL, auth_header
from healthmap.http import HttpClient
from healthmap.raw_store import RawStore

logger = logging.getLogger(__name__)

SOURCE = "garmin"


@dataclass(frozen=True)
class Endpoint:
    name: str
    kind: Literal["once", "range", "daily"]
    path: str
    params: dict[str, str] = field(default_factory=dict)
    max_days: int = 0
    step: int = 1
    optional: bool = False


ENDPOINTS: list[Endpoint] = [
    Endpoint("profile_settings", "once", "/userprofile-service/userprofile/settings"),
    Endpoint("hr_zones", "once", "/biometric-service/heartRateZones/v2"),
    Endpoint("daily_summaries_v2", "range", "/usersummary-service/v2/dailysummaries",
             {"startDate": "{start}", "endDate": "{end}"}, max_days=365),
    Endpoint("daily_summary", "daily", "/usersummary-service/usersummary/daily",
             {"calendarDate": "{date}"}),
    Endpoint("sleep", "range", "/sleep-service/sleep/dailySleepSummariesByDate",
             {"startDate": "{start}", "endDate": "{end}"}, max_days=31),
    Endpoint("hrv_daily", "range", "/hrv-service/hrv/daily/{start}/{end}", max_days=31),
    Endpoint("hrv", "daily", "/hrv-service/hrv/{date}"),
    Endpoint("heart_rate", "daily", "/wellness-service/wellness/dailyHeartRate", {"date": "{date}"}),
    Endpoint("stress", "range", "/usersummary-service/stats/stress/daily/{start}/{end}", max_days=28),
    Endpoint("steps", "range", "/usersummary-service/stats/steps/daily/{start}/{end}", max_days=28),
    Endpoint("body_battery", "range", "/wellness-service/wellness/bodyBattery/reports/daily",
             {"startDate": "{start}", "endDate": "{end}"}, max_days=31),
    Endpoint("spo2", "daily", "/wellness-service/wellness/daily/spo2acclimation/{date}"),
    Endpoint("hydration", "range", "/usersummary-service/stats/hydration/daily/{start}/{end}",
             max_days=31),
    Endpoint("weight", "range", "/weight-service/weight/all/{start}/{end}", max_days=90),
    Endpoint("fitness_age", "daily", "/fitnessage-service/fitnessage/v2/{date}", step=7),
    Endpoint("wellness_activities", "range",
             "/wellnessactivity-service/activity/summary/range/{start}/{end}", max_days=31),
    Endpoint("nutrition", "daily", "/nutrition-service/food/logs/{date}",
             {"includeMedia": "false"}, optional=True),
]

ACTIVITY_ENDPOINTS = ["activities_list", "activity_detail", "activity_workouts", "activity_files"]
ALL_NAMES = [e.name for e in ENDPOINTS] + ACTIVITY_ENDPOINTS


class Forbidden(Exception):
    """El endpoint no está permitido para esta key: se salta entero."""


class GarminExtractor:
    def __init__(self, store: RawStore, force: bool = False, today: date | None = None):
        self.store = store
        self.force = force
        self.today = today or date.today()
        self.client = HttpClient(BASE_URL, auth_header, min_interval=settings.garmin_min_interval)
        self.stats: dict[str, dict[str, int]] = {}

    # ── utilidades ──────────────────────────────────────────────────────────

    def _stat(self, endpoint: str, what: str) -> None:
        self.stats.setdefault(endpoint, {}).setdefault(what, 0)
        self.stats[endpoint][what] += 1

    def _log_error(self, endpoint: str, key: str, resp: httpx.Response) -> None:
        path = self.store.base / SOURCE / "_errors.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f:
            f.write(json.dumps({
                "at": datetime.now().isoformat(timespec="seconds"),
                "endpoint": endpoint, "key": key, "status": resp.status_code,
                "url": str(resp.request.url), "body": resp.text[:500],
            }) + "\n")

    def _fetch(self, endpoint: str, key: str, path: str, params: dict | None,
               refresh: bool = False, binary_ext: str | None = None) -> bool:
        """Descarga y guarda una respuesta. Devuelve True si hay datos guardados."""
        ext = binary_ext or "json"
        if not (self.force or refresh) and self.store.has(SOURCE, endpoint, key, ext):
            self._stat(endpoint, "skipped")
            return True

        resp = self.client.get(path, params=params)
        if resp.status_code in (204, 404):
            self.store.mark_empty(SOURCE, endpoint, key, str(resp.status_code))
            self._stat(endpoint, "empty")
            return False
        if resp.status_code == 403:
            self._log_error(endpoint, key, resp)
            raise Forbidden(endpoint)
        if resp.status_code >= 400:
            self._log_error(endpoint, key, resp)
            self._stat(endpoint, "error")
            logger.warning("%s %s → HTTP %s", endpoint, key, resp.status_code)
            return False

        if binary_ext:
            self.store.write_bytes(SOURCE, endpoint, key, resp.content, binary_ext)
        else:
            if not resp.content.strip():
                self.store.mark_empty(SOURCE, endpoint, key, "empty body")
                self._stat(endpoint, "empty")
                return False
            try:
                payload = resp.json()
            except ValueError:
                payload = {"_raw_text": resp.text}
            self.store.write_json(SOURCE, endpoint, key, payload)
        self._stat(endpoint, "fetched")
        return True

    @staticmethod
    def _fmt(template: str, **values: str) -> str:
        return template.format(**values)

    # ── endpoints declarativos ──────────────────────────────────────────────

    def run_endpoint(self, ep: Endpoint, since: date, until: date) -> None:
        logger.info("Garmin · %s (%s)", ep.name, ep.kind)
        try:
            if ep.kind == "once":
                self._fetch(ep.name, self.today.isoformat(), ep.path, ep.params or None,
                            refresh=True)
            elif ep.kind == "range":
                for start, end in windows(since, until, ep.max_days):
                    v = {"start": start.isoformat(), "end": end.isoformat()}
                    self._fetch(
                        ep.name, f"{v['start']}_{v['end']}", self._fmt(ep.path, **v),
                        {k: self._fmt(p, **v) for k, p in ep.params.items()} or None,
                        refresh=is_recent(end, self.today, settings.refresh_recent_days),
                    )
            else:
                for d in daterange(since, until, ep.step):
                    v = {"date": d.isoformat()}
                    self._fetch(
                        ep.name, v["date"], self._fmt(ep.path, **v),
                        {k: self._fmt(p, **v) for k, p in ep.params.items()} or None,
                        refresh=is_recent(d, self.today, settings.refresh_recent_days),
                    )
        except Forbidden:
            logger.warning("Garmin · %s no permitido (403) — se omite", ep.name)
            self._stat(ep.name, "forbidden")

    # ── actividades ─────────────────────────────────────────────────────────

    def list_activities(self, since: date, until: date) -> list[dict]:
        activities: list[dict] = []
        page_size = 99
        start = 0
        while True:
            key = f"{since}_{until}_{start:05d}"
            params = {
                "startDate": since.isoformat(), "endDate": until.isoformat(),
                "start": start, "limit": page_size, "sortOrder": "asc",
                "excludeChildren": "false",
            }
            # El listado siempre se refresca: es barato y detecta actividades nuevas/editadas
            got = self._fetch("activities_list", key, "/activitylist-service/activities/search/activities",
                              params, refresh=True)
            page = self.store.read_json(SOURCE, "activities_list", key) if got else []
            if not isinstance(page, list):
                page = []
            activities.extend(page)
            if len(page) < page_size:
                break
            start += page_size
        logger.info("Garmin · %d actividades entre %s y %s", len(activities), since, until)
        return activities

    def run_activities(self, since: date, until: date, only: set[str] | None,
                       limit: int | None) -> None:
        try:
            activities = self.list_activities(since, until)
        except Forbidden:
            logger.warning("Garmin · listado de actividades no permitido (403)")
            return
        if limit:
            activities = activities[-limit:]

        per_activity = [
            ("activity_detail", "/activity-service/activity/{id}", None),
            ("activity_workouts", "/activity-service/activity/{id}/workouts", None),
            ("activity_files", "/download-service/files/activity/{id}", "zip"),
        ]
        forbidden: set[str] = set()
        for i, act in enumerate(activities, 1):
            aid = act.get("activityId")
            if not aid:
                continue
            for name, tpl, binary in per_activity:
                if name in forbidden or (only and name not in only):
                    continue
                try:
                    self._fetch(name, str(aid), tpl.format(id=aid), None, binary_ext=binary)
                except Forbidden:
                    logger.warning("Garmin · %s no permitido (403) — se omite", name)
                    forbidden.add(name)
            if i % 20 == 0:
                logger.info("Garmin · actividades procesadas: %d/%d", i, len(activities))

    # ── orquestación ────────────────────────────────────────────────────────

    def run(self, since: date, until: date, only: set[str] | None = None,
            limit: int | None = None, with_optional: bool = False) -> dict:
        for ep in ENDPOINTS:
            if only and ep.name not in only:
                continue
            if ep.optional and not with_optional and not (only and ep.name in only):
                continue
            self.run_endpoint(ep, since, until)

        if not only or only & set(ACTIVITY_ENDPOINTS):
            self.run_activities(since, until, only & set(ACTIVITY_ENDPOINTS) if only else None, limit)

        self.client.close()
        logger.info("Garmin · llamadas HTTP: %d", self.client.calls)
        return self.stats
