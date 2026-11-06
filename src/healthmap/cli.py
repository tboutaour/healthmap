import logging
from datetime import date

import click

from healthmap.config import settings
from healthmap.raw_store import RawStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)


def _dates(since: str | None, until: str | None) -> tuple[date, date]:
    s = date.fromisoformat(since) if since else settings.start_date
    u = date.fromisoformat(until) if until else date.today()
    if s > u:
        raise click.BadParameter("--since debe ser <= --until")
    return s, u


def _only(value: str | None, valid: list[str]) -> set[str] | None:
    if not value:
        return None
    names = {v.strip() for v in value.split(",") if v.strip()}
    bad = names - set(valid)
    if bad:
        raise click.BadParameter(f"Endpoints desconocidos: {sorted(bad)}. Válidos: {valid}")
    return names


def _print_stats(title: str, stats: dict) -> None:
    click.echo(f"\n{title}")
    for ep, s in sorted(stats.items()):
        click.echo(f"  {ep:24s} " + "  ".join(f"{k}={v}" for k, v in sorted(s.items())))


common = [
    click.option("--since", help="Fecha inicio YYYY-MM-DD (defecto: START_DATE)"),
    click.option("--until", help="Fecha fin YYYY-MM-DD (defecto: hoy)"),
    click.option("--only", help="Lista de endpoints separada por comas"),
    click.option("--limit", type=int, help="Solo las N actividades más recientes (pruebas)"),
    click.option("--force", is_flag=True, help="Re-descargar aunque ya exista en raw"),
]


def with_common(f):
    for opt in reversed(common):
        f = opt(f)
    return f


class FriendlyGroup(click.Group):
    """Muestra los RuntimeError (config/auth) como mensaje, sin traceback."""

    def invoke(self, ctx):
        try:
            return super().invoke(ctx)
        except RuntimeError as e:
            raise click.ClickException(str(e)) from e


@click.group(cls=FriendlyGroup)
def cli():
    """Extracción de datos de salud y entrenamiento (Strava + Garmin)."""


# ── Garmin ───────────────────────────────────────────────────────────────────


@cli.group()
def garmin():
    """Garmin Connect Personal API."""


@garmin.command("token")
def garmin_token():
    """Obtiene (o renueva) el access token a partir de GARMIN_API_KEY."""
    from healthmap.garmin.auth import request_token

    data = request_token()
    click.echo(f"✓ Token obtenido. Caduca: {date.fromtimestamp(data['expires_at'])} "
               f"(claves de respuesta: {data['response_keys']})")


@garmin.command("extract")
@with_common
@click.option("--with-optional", is_flag=True, help="Incluye endpoints opcionales (nutrición)")
def garmin_extract(since, until, only, limit, force, with_optional):
    """Descarga todos los endpoints Garmin al almacén raw."""
    from healthmap.garmin.extract import ALL_NAMES, GarminExtractor

    s, u = _dates(since, until)
    stats = GarminExtractor(RawStore(settings.raw_dir), force=force).run(
        s, u, only=_only(only, ALL_NAMES), limit=limit, with_optional=with_optional)
    _print_stats(f"Garmin {s} → {u}", stats)


# ── Strava ───────────────────────────────────────────────────────────────────


@cli.group()
def strava():
    """Strava API v3."""


@strava.command("auth")
@click.argument("code", required=False)
def strava_auth(code):
    """Autoriza la app (scopes activity:read_all, profile:read_all)."""
    from healthmap.strava.auth import authorize_url, exchange_code

    if not code:
        click.echo("1) Abre esta URL y autoriza:\n")
        click.echo(authorize_url())
        click.echo("\n2) Te redirigirá a http://localhost:8080/callback?...&code=XXXX")
        code = click.prompt("Pega el valor de 'code'")
    data = exchange_code(code.strip())
    a = data.get("athlete", {})
    click.echo(f"✓ Autenticado como {a.get('firstname', '')} {a.get('lastname', '')}. "
               f"Tokens guardados en {settings.tokens_dir / 'strava.json'}")


@strava.command("extract")
@with_common
def strava_extract(since, until, only, limit, force):
    """Descarga actividades, streams, zonas y gear de Strava al almacén raw."""
    from healthmap.strava.extract import ALL_NAMES, StravaExtractor

    s, u = _dates(since, until)
    stats = StravaExtractor(RawStore(settings.raw_dir), force=force).run(
        s, u, only=_only(only, ALL_NAMES), limit=limit)
    _print_stats(f"Strava {s} → {u}", stats)


# ── Todo junto ───────────────────────────────────────────────────────────────


@cli.command("extract")
@click.option("--since")
@click.option("--until")
@click.option("--force", is_flag=True)
@click.option("--skip-garmin", is_flag=True)
@click.option("--skip-strava", is_flag=True)
def extract_all(since, until, force, skip_garmin, skip_strava):
    """Extrae Garmin y Strava y reconstruye DuckDB."""
    s, u = _dates(since, until)
    store = RawStore(settings.raw_dir)
    if not skip_garmin:
        from healthmap.garmin.extract import GarminExtractor
        try:
            _print_stats("Garmin", GarminExtractor(store, force=force).run(s, u))
        except RuntimeError as e:
            click.echo(f"✗ Garmin: {e}", err=True)
    if not skip_strava:
        from healthmap.strava.extract import StravaExtractor
        try:
            _print_stats("Strava", StravaExtractor(store, force=force).run(s, u))
        except RuntimeError as e:
            click.echo(f"✗ Strava: {e}", err=True)
    _build()


# ── Carga y estado ───────────────────────────────────────────────────────────


def _build():
    from healthmap.load.build_duckdb import build

    counts = build()
    click.echo(f"\nDuckDB → {settings.db_path}")
    for t, n in sorted(counts.items()):
        click.echo(f"  {t:32s} {n:>9,}")


@cli.command("build")
def build_cmd():
    """Reconstruye data/healthmap.duckdb desde data/raw."""
    _build()


@cli.command("status")
def status():
    """Resumen de lo descargado en raw (ficheros por endpoint y rango de claves)."""
    store = RawStore(settings.raw_dir)
    for source in ("garmin", "strava"):
        eps = store.endpoints(source)
        if not eps:
            click.echo(f"{source}: sin datos")
            continue
        click.echo(f"\n{source}")
        for ep in eps:
            d = store.base / source / ep
            data = sorted(p.stem for p in d.iterdir() if p.suffix in (".json", ".zip"))
            empty = sum(1 for _ in d.glob("*.empty"))
            rng = f"{data[0]} … {data[-1]}" if data else "—"
            click.echo(f"  {ep:22s} ficheros={len(data):5d} vacíos={empty:4d}  {rng}")
    err = store.base / "garmin" / "_errors.jsonl"
    if err.exists():
        n = sum(1 for _ in err.open())
        click.echo(f"\nErrores Garmin registrados: {n} (ver {err})")


if __name__ == "__main__":
    cli()
