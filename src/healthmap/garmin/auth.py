"""
Autenticación Garmin Connect Personal API.

La Personal API Key (creada en connect.garmin.com/app/settings/personalApiToken) se
intercambia por un access token OAuth2 válido 24 h (o hasta que caduque la key).
No hay refresh token: cuando caduca se pide uno nuevo con la misma key.
"""

import logging
import time
from typing import Any

import httpx

from healthmap.config import settings
from healthmap.tokens import load_token, save_token

logger = logging.getLogger(__name__)

BASE_URL = "https://personal.connectapi.garmin.com"
TOKEN_PATH = "/personalapitoken-service/personal-api/personal-api-key/token"
DEFAULT_TTL = 24 * 3600
SAFETY_MARGIN = 10 * 60


def _token_file():
    return settings.tokens_dir / "garmin.json"


def _extract_token(payload: Any) -> tuple[str, int]:
    """La doc no detalla el formato de respuesta: aceptamos las variantes habituales."""
    if isinstance(payload, str):
        return payload.strip(), DEFAULT_TTL
    if not isinstance(payload, dict):
        raise RuntimeError(f"Respuesta de token Garmin inesperada: {type(payload)}")
    token = None
    for k in ("access_token", "accessToken", "token", "personalApiAccessToken"):
        if payload.get(k):
            token = payload[k]
            break
    if not token:
        raise RuntimeError(f"No encuentro el access token en la respuesta (claves: {list(payload)})")
    ttl = DEFAULT_TTL
    for k in ("expires_in", "expiresIn", "expiresInSeconds"):
        if payload.get(k):
            ttl = int(payload[k])
            break
    return token, ttl


def request_token() -> dict[str, Any]:
    if not settings.garmin_api_key:
        raise RuntimeError(
            "Falta GARMIN_API_KEY en .env. Créala en "
            "https://connect.garmin.com/app/settings/personalApiToken"
        )
    resp = httpx.post(
        BASE_URL + TOKEN_PATH,
        headers={"Authorization": f"ApiKey {settings.garmin_api_key}", "Accept": "application/json"},
        timeout=30,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Error obteniendo token Garmin: HTTP {resp.status_code} {resp.text[:300]}")
    try:
        payload = resp.json()
    except ValueError:
        payload = resp.text
    token, ttl = _extract_token(payload)
    data = {
        "access_token": token,
        "token_type": (payload.get("token_type") if isinstance(payload, dict) else None) or "Bearer",
        "expires_at": int(time.time()) + ttl,
        "response_keys": list(payload) if isinstance(payload, dict) else [],
    }
    save_token(_token_file(), data)
    logger.info("Token Garmin obtenido (válido %.1f h)", ttl / 3600)
    return data


def get_token(force: bool = False) -> dict[str, Any]:
    data = None if force else load_token(_token_file())
    if not data or data.get("expires_at", 0) - SAFETY_MARGIN < time.time():
        data = request_token()
    return data


def auth_header(force_refresh: bool = False) -> dict[str, str]:
    data = get_token(force=force_refresh)
    token_type = data.get("token_type", "Bearer")
    if token_type.lower() == "bearer":
        token_type = "Bearer"
    return {"Authorization": f"{token_type} {data['access_token']}", "Accept": "application/json"}
