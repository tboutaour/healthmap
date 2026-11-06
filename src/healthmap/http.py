"""Cliente HTTP base con throttle y reintentos."""

import logging
import time
from collections.abc import Callable

import httpx

logger = logging.getLogger(__name__)

RETRY_STATUS = {429, 500, 502, 503, 504}


class HttpClient:
    def __init__(
        self,
        base_url: str,
        auth_header: Callable[[bool], dict[str, str]],
        min_interval: float = 0.0,
        max_retries: int = 5,
        timeout: float = 60.0,
    ):
        """
        auth_header(force_refresh) devuelve las cabeceras de autenticación; se llama con
        force_refresh=True tras un 401 para renovar el token una vez.
        """
        self._client = httpx.Client(base_url=base_url, timeout=timeout, follow_redirects=True)
        self._auth_header = auth_header
        self._min_interval = min_interval
        self._max_retries = max_retries
        self._last_request = 0.0
        self.calls = 0

    def _throttle(self) -> None:
        wait = self._min_interval - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def retry_wait(self, response: httpx.Response, attempt: int) -> float:
        """Segundos a esperar antes de reintentar. Las subclases pueden especializarlo."""
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return float(retry_after)
        return min(2**attempt * 2, 120)

    def after_response(self, response: httpx.Response) -> None:
        """Hook para inspeccionar cabeceras (p. ej. rate limits)."""

    def get(self, path: str, params: dict | None = None) -> httpx.Response:
        refreshed = False
        attempt = 0
        while True:
            self._throttle()
            try:
                resp = self._client.get(path, params=params, headers=self._auth_header(False))
            except httpx.TransportError as e:
                attempt += 1
                if attempt > self._max_retries:
                    raise
                wait = min(2**attempt * 2, 120)
                logger.warning("Error de red en %s (%s); reintento en %ss", path, e, wait)
                time.sleep(wait)
                continue

            self.calls += 1
            self.after_response(resp)

            if resp.status_code == 401 and not refreshed:
                logger.info("401 en %s — renovando token", path)
                self._auth_header(True)
                refreshed = True
                continue

            if resp.status_code in RETRY_STATUS and attempt < self._max_retries:
                attempt += 1
                wait = self.retry_wait(resp, attempt)
                logger.warning("HTTP %s en %s; reintento %d en %.0fs", resp.status_code, path,
                               attempt, wait)
                time.sleep(wait)
                continue

            return resp

    def close(self) -> None:
        self._client.close()
