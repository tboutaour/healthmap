"""
Autenticación Strava (OAuth2 con refresh token).

Strava rota el refresh token: cada renovación puede devolver uno nuevo, así que se
persiste en data/tokens/strava.json. El de .env solo se usa la primera vez.
Adaptado de hybrid-trainer/src/ingestion/strava.py.
"""

import logging
import time
from typing import Any

import httpx

from healthmap.config import settings
from healthmap.tokens import load_token, save_token

logger = logging.getLogger(__name__)

AUTH_URL = "https://www.strava.com/oauth/authorize"
TOKEN_URL = "https://www.strava.com/oauth/token"
REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "read,activity:read_all,profile:read_all"


def _token_file():
    return settings.tokens_dir / "strava.json"


def _require_client() -> None:
    if not settings.strava_client_id or not settings.strava_client_secret:
        raise RuntimeError("Faltan STRAVA_CLIENT_ID / STRAVA_CLIENT_SECRET en .env")


def _store(payload: dict[str, Any], scope: str | None = None) -> dict[str, Any]:
    data = {
        "access_token": payload["access_token"],
        "refresh_token": payload["refresh_token"],
        "expires_at": int(payload["expires_at"]),
    }
    if scope:
        data["scope"] = scope
    else:
        prev = load_token(_token_file()) or {}
        if "scope" in prev:
            data["scope"] = prev["scope"]
    save_token(_token_file(), data)
    return data


def refresh(refresh_token: str) -> dict[str, Any]:
    _require_client()
    resp = httpx.post(TOKEN_URL, data={
        "client_id": settings.strava_client_id,
        "client_secret": settings.strava_client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Error renovando token Strava: HTTP {resp.status_code} {resp.text[:300]}\n"
            "Si el refresh token ya no es válido, re-autoriza con: healthmap strava auth"
        )
    logger.info("Token Strava renovado")
    return _store(resp.json())


def get_token(force: bool = False) -> dict[str, Any]:
    data = load_token(_token_file())
    if data and not force and data["expires_at"] - 300 > time.time():
        return data
    refresh_token = (data or {}).get("refresh_token") or settings.strava_refresh_token
    if not refresh_token:
        raise RuntimeError("No hay refresh token de Strava. Ejecuta: healthmap strava auth")
    return refresh(refresh_token)


def auth_header(force_refresh: bool = False) -> dict[str, str]:
    return {"Authorization": f"Bearer {get_token(force=force_refresh)['access_token']}"}


def authorize_url() -> str:
    _require_client()
    return (
        f"{AUTH_URL}?client_id={settings.strava_client_id}&response_type=code"
        f"&redirect_uri={REDIRECT_URI}&approval_prompt=force&scope={SCOPES}"
    )


def exchange_code(code: str) -> dict[str, Any]:
    _require_client()
    resp = httpx.post(TOKEN_URL, data={
        "client_id": settings.strava_client_id,
        "client_secret": settings.strava_client_secret,
        "code": code,
        "grant_type": "authorization_code",
    }, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Error intercambiando código: HTTP {resp.status_code} {resp.text[:300]}")
    payload = resp.json()
    return _store(payload, scope=SCOPES) | {"athlete": payload.get("athlete", {})}
