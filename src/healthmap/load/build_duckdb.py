"""
Construye data/healthmap.duckdb a partir de data/raw/.

raw es la fuente de verdad: este proceso se puede re-ejecutar siempre desde cero.
  1. Cada builder recorre los JSON de un endpoint y genera filas (dicts aplanados).
  2. Se deduplican por clave natural (gana el fichero más reciente).
  3. Se escriben a data/staging/*.ndjson y se cargan con read_json_auto.
Las series temporales grandes (streams de Strava, records FIT, FC intradía) van a CSV
por actividad/día en staging y se cachean: solo se regeneran si el raw es más nuevo.
"""

import csv
import io
import json
import logging
import zipfile
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from healthmap.config import settings
from healthmap.load.flatten import flatten, semicircles_to_degrees
from healthmap.raw_store import RawStore

logger = logging.getLogger(__name__)

Rows = Iterable[dict[str, Any]]


def _files_by_mtime(store: RawStore, source: str, endpoint: str) -> Iterator[tuple[str, Any]]:
    d = store.base / source / endpoint
    if not d.exists():
        return
    for p in sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime):
        yield p.stem, json.loads(p.read_text("utf-8"))


def _as_list(payload: Any, *keys: str) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in keys:
            if isinstance(payload.get(k), list):
                return payload[k]
    return []


# ── Tablas "pequeñas" (NDJSON) ──────────────────────────────────────────────


@dataclass
class Table:
    name: str
    source: str
    endpoint: str
    rows: Callable[[str, Any], Rows]
    key: Callable[[dict], Any] | None = None


def _g_list(*keys: str, depth: int = 3):
    def rows(_k: str, payload: Any) -> Rows:
        for item in _as_list(payload, *keys):
            if isinstance(item, dict):
                yield flatten(item, max_depth=depth)
    return rows


def _g_single(depth: int = 1):
    def rows(_k: str, payload: Any) -> Rows:
        if isinstance(payload, dict) and payload:
            yield flatten(payload, max_depth=depth)
    return rows


def _by(*cols: str):
    return lambda r: tuple(r.get(c) for c in cols)


def _activity_splits(_k: str, payload: Any) -> Rows:
    if not isinstance(payload, dict):
        return
    for i, s in enumerate(payload.get("splitSummaries") or []):
        yield {"activityId": payload.get("activityId"), "split_idx": i} | flatten(s, max_depth=2)


def _hrv_readings(k: str, payload: Any) -> Rows:
    if not isinstance(payload, dict):
        return
    for r in payload.get("hrvReadings") or []:
        yield {"calendarDate": k} | flatten(r)


def _strava_activities(_k: str, payload: Any) -> Rows:
    for a in _as_list(payload):
        yield flatten(a, max_depth=2)


def _strava_detail(_k: str, payload: Any) -> Rows:
    if isinstance(payload, dict) and payload.get("id"):
        yield flatten(payload, max_depth=2)


def _strava_child(field: str):
    def rows(_k: str, payload: Any) -> Rows:
        if not isinstance(payload, dict):
            return
        for i, item in enumerate(payload.get(field) or []):
            yield {"activity_id": payload["id"], "idx": i} | flatten(item, max_depth=2)
    return rows


def _strava_zones(k: str, payload: Any) -> Rows:
    for z in _as_list(payload):
        for i, b in enumerate(z.get("distribution_buckets") or []):
            yield {
                "activity_id": int(k), "zone_type": z.get("type"), "bucket": i,
                "min": b.get("min"), "max": b.get("max"), "time_s": b.get("time"),
                "score": z.get("score"), "sensor_based": z.get("sensor_based"),
            }


TABLES: list[Table] = [
    # Garmin
    Table("garmin_daily_summary", "garmin", "daily_summary", _g_single(1), _by("calendarDate")),
    Table("garmin_daily_v2", "garmin", "daily_summaries_v2", _g_list("data"), _by("calendarDate")),
    Table("garmin_sleep", "garmin", "sleep", _g_list(), _by("calendarDate")),
    Table("garmin_hrv", "garmin", "hrv_daily", _g_list("hrvSummaries"), _by("calendarDate")),
    Table("garmin_hrv_readings", "garmin", "hrv", _hrv_readings, _by("readingTimeGMT")),
    Table("garmin_hr_daily", "garmin", "heart_rate", _g_single(1), _by("calendarDate")),
    Table("garmin_stress_daily", "garmin", "stress", _g_list(), _by("calendarDate")),
    Table("garmin_steps_daily", "garmin", "steps", _g_list(), _by("calendarDate")),
    Table("garmin_body_battery_daily", "garmin", "body_battery", _g_list(depth=2), _by("date")),
    Table("garmin_spo2_daily", "garmin", "spo2", _g_single(1), _by("calendarDate")),
    Table("garmin_hydration", "garmin", "hydration", _g_list(), _by("calendarDate")),
    Table("garmin_weight", "garmin", "weight", _g_list(), _by("timestampGMT", "date")),
    Table("garmin_fitness_age", "garmin", "fitness_age", _g_list(depth=2)),
    Table("garmin_wellness_activities", "garmin", "wellness_activities", _g_list(depth=2)),
    Table("garmin_activities", "garmin", "activities_list", _g_list(depth=2), _by("activityId")),
    Table("garmin_activity_detail", "garmin", "activity_detail", _g_single(3), _by("activityId")),
    Table("garmin_activity_splits", "garmin", "activity_detail", _activity_splits,
          _by("activityId", "split_idx")),
    Table("garmin_hr_zones", "garmin", "hr_zones", _g_list(depth=2)),
    Table("garmin_profile", "garmin", "profile_settings", _g_single(2)),
    # Strava
    Table("strava_activities", "strava", "activities_list", _strava_activities, _by("id")),
    Table("strava_activity_detail", "strava", "activity_detail", _strava_detail, _by("id")),
    Table("strava_laps", "strava", "activity_detail", _strava_child("laps"),
          _by("activity_id", "idx")),
    Table("strava_splits", "strava", "activity_detail", _strava_child("splits_metric"),
          _by("activity_id", "idx")),
    Table("strava_best_efforts", "strava", "activity_detail", _strava_child("best_efforts"),
          _by("activity_id", "idx")),
    Table("strava_segment_efforts", "strava", "activity_detail", _strava_child("segment_efforts"),
          _by("activity_id", "idx")),
    Table("strava_activity_zones", "strava", "activity_zones", _strava_zones,
          _by("activity_id", "zone_type", "bucket")),
    Table("strava_gear", "strava", "gear", _g_single(2), _by("id")),
    Table("strava_athlete_stats", "strava", "athlete_stats", _g_single(3)),
]


def collect_rows(store: RawStore, table: Table) -> list[dict]:
    keyed: dict[Any, dict] = {}
    plain: list[dict] = []
    seen_plain: set[str] = set()
    for k, payload in _files_by_mtime(store, table.source, table.endpoint):
        for row in table.rows(k, payload):
            if table.key:
                keyed[table.key(row)] = row  # el fichero más reciente sobrescribe
            else:
                sig = json.dumps(row, sort_keys=True, default=str)
                if sig not in seen_plain:
                    seen_plain.add(sig)
                    plain.append(row)
    return list(keyed.values()) if table.key else plain


# ── Series temporales (CSV por fichero, cacheadas) ─────────────────────────


def _fresh(src: Path, dst: Path) -> bool:
    return dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime


def _write_csv(dst: Path, header: list[str], rows: Iterable[list]) -> int:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".tmp")
    n = 0
    with tmp.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
            n += 1
    tmp.replace(dst)
    return n


STRAVA_STREAM_COLS = ["time", "distance", "lat", "lng", "altitude", "heartrate", "cadence",
                      "watts", "velocity_smooth", "grade_smooth", "temp", "moving"]


def strava_stream_rows(activity_id: str, payload: dict) -> Iterator[list]:
    if not isinstance(payload, dict) or not payload:
        return
    n = max((len(s.get("data") or []) for s in payload.values() if isinstance(s, dict)), default=0)
    def col(name: str, i: int):
        s = payload.get(name)
        data = s.get("data") if isinstance(s, dict) else None
        return data[i] if data and i < len(data) else None
    for i in range(n):
        ll = col("latlng", i)
        yield [activity_id, i, col("time", i), col("distance", i),
               ll[0] if ll else None, ll[1] if ll else None,
               col("altitude", i), col("heartrate", i), col("cadence", i), col("watts", i),
               col("velocity_smooth", i), col("grade_smooth", i), col("temp", i), col("moving", i)]


FIT_COLS = ["timestamp", "heart_rate", "speed", "cadence", "altitude", "distance",
            "lat", "lng", "power", "temperature", "respiration_rate", "vertical_oscillation",
            "stance_time", "step_length"]


def fit_record_rows(activity_id: str, zip_path: Path) -> Iterator[list]:
    from fitparse import FitFile

    with zipfile.ZipFile(zip_path) as zf:
        fit_names = [n for n in zf.namelist() if n.lower().endswith(".fit")]
        for name in fit_names:
            fit = FitFile(io.BytesIO(zf.read(name)))
            for msg in fit.get_messages("record"):
                v = {f.name: f.value for f in msg.fields}
                ts = v.get("timestamp")
                yield [
                    activity_id,
                    ts.replace(tzinfo=UTC).isoformat() if isinstance(ts, datetime) else ts,
                    v.get("heart_rate"),
                    v.get("enhanced_speed", v.get("speed")),
                    v.get("cadence"),
                    v.get("enhanced_altitude", v.get("altitude")),
                    v.get("distance"),
                    semicircles_to_degrees(v.get("position_lat")),
                    semicircles_to_degrees(v.get("position_long")),
                    v.get("power"),
                    v.get("temperature"),
                    v.get("enhanced_respiration_rate", v.get("respiration_rate")),
                    v.get("vertical_oscillation"),
                    v.get("stance_time"),
                    v.get("step_length"),
                ]


def _descriptor_series(payload: dict, values_key: str, desc_key: str, idx_key: str,
                       name_key: str) -> tuple[list[str], list[list]]:
    descs = payload.get(desc_key) or []
    names = [None] * len(descs)
    for d in descs:
        i = d.get(idx_key)
        if isinstance(i, int) and i < len(names):
            names[i] = d.get(name_key)
    return [n or f"v{i}" for i, n in enumerate(names)], payload.get(values_key) or []


def body_battery_rows(payload: Any) -> Iterator[list]:
    for day in _as_list(payload):
        names, values = _descriptor_series(
            day, "bodyBatteryValuesArray", "bodyBatteryValueDescriptorDTOList",
            "bodyBatteryValueDescriptorIndex", "bodyBatteryValueDescriptorKey")
        ti = names.index("timestamp") if "timestamp" in names else 0
        level = [j for j, n in enumerate(names) if "level" in n.lower()]
        li = level[0] if level else (1 if ti == 0 else 0)
        for v in values:
            if isinstance(v, list) and len(v) > max(ti, li):
                yield [day.get("date"), v[ti], v[li]]


def build_series(store: RawStore, staging: Path) -> dict[str, tuple[Path, list[str]]]:
    """Genera CSVs por fichero. Devuelve {tabla: (directorio, columnas)}."""
    out: dict[str, tuple[Path, list[str]]] = {}

    # Strava streams
    d = staging / "strava_streams"
    for p in store.iter_files("strava", "activity_streams", "json"):
        dst = d / f"{p.stem}.csv"
        if not _fresh(p, dst):
            _write_csv(dst, ["activity_id", "idx", *STRAVA_STREAM_COLS],
                       strava_stream_rows(p.stem, json.loads(p.read_text("utf-8"))))
    out["strava_streams"] = (d, ["activity_id", "idx", *STRAVA_STREAM_COLS])

    # Garmin FIT records
    d = staging / "garmin_activity_records"
    for p in store.iter_files("garmin", "activity_files", "zip"):
        dst = d / f"{p.stem}.csv"
        if not _fresh(p, dst):
            try:
                _write_csv(dst, ["activity_id", *FIT_COLS], fit_record_rows(p.stem, p))
            except Exception as e:  # FIT corrupto o zip sin .fit
                logger.warning("No se pudo parsear FIT %s: %s", p.name, e)
    out["garmin_activity_records"] = (d, ["activity_id", *FIT_COLS])

    # Garmin FC intradía y Body Battery intradía (timestamp en ms)
    d = staging / "garmin_hr_intraday"
    for p in store.iter_files("garmin", "heart_rate", "json"):
        dst = d / f"{p.stem}.csv"
        if not _fresh(p, dst):
            payload = json.loads(p.read_text("utf-8"))
            names, values = _descriptor_series(payload, "heartRateValues",
                                               "heartRateValueDescriptors", "index", "key")
            ti = names.index("timestamp") if "timestamp" in names else 0
            hi = names.index("heartrate") if "heartrate" in names else 1
            _write_csv(dst, ["calendar_date", "ts_ms", "heart_rate"],
                       ([p.stem, v[ti], v[hi]] for v in values
                        if isinstance(v, list) and len(v) > max(ti, hi)))
    out["garmin_hr_intraday"] = (d, ["calendar_date", "ts_ms", "heart_rate"])

    d = staging / "garmin_body_battery_intraday"
    for p in store.iter_files("garmin", "body_battery", "json"):
        dst = d / f"{p.stem}.csv"
        if not _fresh(p, dst):
            _write_csv(dst, ["calendar_date", "ts_ms", "body_battery"],
                       body_battery_rows(json.loads(p.read_text("utf-8"))))
    out["garmin_body_battery_intraday"] = (d, ["calendar_date", "ts_ms", "body_battery"])
    return out


# ── Vista diaria ────────────────────────────────────────────────────────────


def _columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str] | None:
    try:
        return {r[0].lower() for r in con.execute(f"DESCRIBE {table}").fetchall()}
    except duckdb.Error:
        return None


def create_daily_view(con: duckdb.DuckDBPyConnection, start: str) -> None:
    def pick(table: str, alias: str, cols: dict[str, str]) -> tuple[list[str], bool]:
        available = _columns(con, table)
        exprs = []
        for out_name, expr_col in cols.items():
            base = expr_col.split("::")[0].split("/")[0].strip()
            ok = available is not None and base.lower() in available
            exprs.append(f"{alias}.{expr_col} AS {out_name}" if ok else f"NULL AS {out_name}")
        return exprs, available is not None and "calendardate" in available

    selects: list[str] = ["c.day"]
    joins: list[str] = []

    for table, alias, cols in [
        ("garmin_daily_summary", "g", {
            "resting_hr": "restingHeartRate", "min_hr": "minHeartRate", "max_hr": "maxHeartRate",
            "rhr_7d_avg": "lastSevenDaysAvgRestingHeartRate", "steps": "totalSteps",
            "kcal_total": "totalKilocalories", "kcal_active": "activeKilocalories",
            "stress_avg": "averageStressLevel", "stress_max": "maxStressLevel",
            "bb_high": "bodyBatteryHighestValue", "bb_low": "bodyBatteryLowestValue",
            "bb_charged": "bodyBatteryChargedValue", "bb_drained": "bodyBatteryDrainedValue",
            "bb_wake": "bodyBatteryAtWakeTime", "intensity_min_moderate": "moderateIntensityMinutes",
            "intensity_min_vigorous": "vigorousIntensityMinutes", "spo2_avg": "averageSpo2",
            "respiration_avg": "avgWakingRespirationValue",
        }),
        ("garmin_sleep", "s", {
            "sleep_hours": "sleepTimeSeconds / 3600.0", "deep_s": "deepSleepSeconds",
            "light_s": "lightSleepSeconds", "rem_s": "remSleepSeconds",
            "awake_s": "awakeSleepSeconds", "sleep_score": "sleepScores_overall_value",
            "sleep_stress": "avgSleepStress", "sleep_avg_hr": "avgHeartRate",
            "sleep_respiration": "averageRespirationValue",
        }),
        ("garmin_hrv", "h", {
            "hrv_last_night": "lastNightAvg", "hrv_weekly": "weeklyAvg", "hrv_status": "status",
        }),
    ]:
        exprs, joinable = pick(table, alias, cols)
        if joinable:
            selects += exprs
            joins.append(f"LEFT JOIN {table} {alias} ON {alias}.calendarDate::DATE = c.day")
        else:
            selects += [f"NULL AS {n}" for n in cols]

    ga = _columns(con, "garmin_activities")
    if ga and {"starttimelocal", "duration"} <= ga:
        load = "SUM(activityTrainingLoad)" if "activitytrainingload" in ga else "NULL"
        dist = "SUM(distance) / 1000.0" if "distance" in ga else "NULL"
        joins.append(f"""LEFT JOIN (
            SELECT startTimeLocal::DATE AS day, COUNT(*) AS n, SUM(duration) / 60.0 AS minutes,
                   {dist} AS km, {load} AS training_load
            FROM garmin_activities GROUP BY 1) ga ON ga.day = c.day""")
        selects += ["COALESCE(ga.n, 0) AS garmin_activities", "ga.minutes AS garmin_minutes",
                    "ga.km AS garmin_km", "ga.training_load AS garmin_training_load"]
    else:
        selects += ["NULL AS garmin_activities", "NULL AS garmin_minutes", "NULL AS garmin_km",
                    "NULL AS garmin_training_load"]

    sa = _columns(con, "strava_activities")
    if sa and {"start_date_local", "moving_time"} <= sa:
        suffer = "SUM(suffer_score)" if "suffer_score" in sa else "NULL"
        joins.append(f"""LEFT JOIN (
            SELECT start_date_local::TIMESTAMP::DATE AS day, COUNT(*) AS n,
                   SUM(moving_time) / 60.0 AS minutes, SUM(distance) / 1000.0 AS km,
                   {suffer} AS suffer
            FROM strava_activities GROUP BY 1) sa ON sa.day = c.day""")
        selects += ["COALESCE(sa.n, 0) AS strava_activities", "sa.minutes AS strava_minutes",
                    "sa.km AS strava_km", "sa.suffer AS strava_suffer_score"]
    else:
        selects += ["NULL AS strava_activities", "NULL AS strava_minutes", "NULL AS strava_km",
                    "NULL AS strava_suffer_score"]

    sql = f"""
    CREATE OR REPLACE VIEW v_daily_state AS
    WITH c AS (
        SELECT range::DATE AS day
        FROM range(DATE '{start}', current_date + INTERVAL 1 DAY, INTERVAL 1 DAY)
    )
    SELECT {', '.join(selects)}
    FROM c
    {' '.join(joins)}
    ORDER BY c.day
    """
    con.execute(sql)


def create_activity_link_view(con: duckdb.DuckDBPyConnection) -> None:
    ga, sa = _columns(con, "garmin_activities"), _columns(con, "strava_activities")
    if not ga or not sa or "starttimegmt" not in ga or "start_date" not in sa:
        return
    con.execute("""
    CREATE OR REPLACE VIEW v_activity_link AS
    SELECT g.activityId AS garmin_activity_id, s.id AS strava_activity_id,
           g.startTimeGMT::TIMESTAMP AS start_gmt
    FROM garmin_activities g
    JOIN strava_activities s
      ON abs(epoch(g.startTimeGMT::TIMESTAMP) - epoch(s.start_date::TIMESTAMPTZ::TIMESTAMP)) <= 120
    """)


# ── Orquestación ────────────────────────────────────────────────────────────


def build(store: RawStore | None = None, db_path: Path | None = None,
          staging: Path | None = None) -> dict[str, int]:
    store = store or RawStore(settings.raw_dir)
    db_path = db_path or settings.db_path
    staging = staging or settings.staging_dir
    staging.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}
    con = duckdb.connect(str(db_path))
    try:
        for t in TABLES:
            rows = collect_rows(store, t)
            if not rows:
                con.execute(f"DROP TABLE IF EXISTS {t.name}")
                continue
            path = staging / f"{t.name}.ndjson"
            with path.open("w") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
            con.execute(
                f"CREATE OR REPLACE TABLE {t.name} AS SELECT * FROM read_json_auto(?, "
                f"format='newline_delimited', sample_size=-1, union_by_name=true)",
                [str(path)],
            )
            counts[t.name] = len(rows)

        for name, (d, _cols) in build_series(store, staging).items():
            if not d.exists() or not any(d.glob("*.csv")):
                con.execute(f"DROP TABLE IF EXISTS {name}")
                continue
            con.execute(
                f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_csv(?, header=true, "
                f"union_by_name=true, sample_size=-1)",
                [str(d / "*.csv")],
            )
            counts[name] = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]

        create_daily_view(con, settings.start_date.isoformat())
        create_activity_link_view(con)
    finally:
        con.close()
    return counts
