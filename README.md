# toni-healthmap

Extracción de datos personales de salud y entrenamiento desde **Garmin Connect Personal API**
y **Strava API v3**, guardados en crudo (`data/raw/`) y cargados en DuckDB
(`data/healthmap.duckdb`) para un futuro panel.

## Setup

```bash
uv sync
cp .env.example .env   # rellenar STRAVA_* y GARMIN_API_KEY
```

- **Garmin**: crea una Personal API Key en
  <https://connect.garmin.com/app/settings/personalApiToken> → `GARMIN_API_KEY`.
  El access token (24 h) se pide y renueva solo. Spec: `docs/garmin_personal_openapi.json`.
- **Strava**: `STRAVA_CLIENT_ID/SECRET/REFRESH_TOKEN`. El refresh token rota y se guarda en
  `data/tokens/strava.json`. Para `/athlete/zones` hace falta el scope `profile:read_all`:
  `uv run healthmap strava auth`.

## Uso

```bash
uv run healthmap garmin token                               # verifica la key
uv run healthmap garmin extract --since 2026-11-01 --limit 2  # prueba corta
uv run healthmap strava extract --since 2026-11-01 --limit 2

uv run healthmap extract --since 2026-01-01                 # todo + build
uv run healthmap build                                       # solo raw → DuckDB
uv run healthmap status                                      # qué hay descargado
```

Opciones de `garmin|strava extract`: `--since --until --only ep1,ep2 --limit N --force`
(Garmin además `--with-optional` para nutrición).

La extracción es **reanudable e idempotente**: lo ya descargado se salta, salvo los últimos
3 días (Garmin los recalcula) y los listados. Strava tiene límite diario de peticiones: si se
alcanza, el proceso para y continúa donde quedó al relanzarlo.

## Datos

| Fuente | Endpoints raw (`data/raw/{fuente}/{endpoint}/`) |
|---|---|
| Garmin | daily_summary, daily_summaries_v2, sleep, hrv_daily, hrv, heart_rate, stress, steps, body_battery, spo2, hydration, weight, fitness_age, wellness_activities, activities_list, activity_detail, activity_workouts, activity_files (FIT .zip), profile_settings, hr_zones, nutrition (opcional) |
| Strava | athlete, athlete_stats, athlete_zones, activities_list, activity_detail, activity_streams, activity_zones, gear |

Tablas DuckDB principales: `garmin_daily_summary`, `garmin_sleep`, `garmin_hrv`,
`garmin_hr_intraday`, `garmin_body_battery_intraday`, `garmin_activities`,
`garmin_activity_records` (FIT segundo a segundo), `strava_activities`, `strava_laps`,
`strava_streams`, … y las vistas `v_daily_state` (1 fila/día) y `v_activity_link`
(empareja actividades Garmin↔Strava).
# healthmap
# healthmap
# healthmap
# healthmap
