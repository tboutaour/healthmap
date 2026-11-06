"""
Almacén de respuestas crudas.

Cada respuesta se guarda íntegra en data/raw/{source}/{endpoint}/{key}.{ext}.
La existencia del fichero actúa como checkpoint: re-ejecutar no vuelve a descargar.
Las respuestas vacías (404/204) se marcan con {key}.empty para no reintentarlas.
"""

import json
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any


class RawStore:
    def __init__(self, base: Path):
        self.base = base

    def _dir(self, source: str, endpoint: str) -> Path:
        return self.base / source / endpoint

    def path(self, source: str, endpoint: str, key: str, ext: str = "json") -> Path:
        return self._dir(source, endpoint) / f"{key}.{ext}"

    def has(self, source: str, endpoint: str, key: str, ext: str = "json") -> bool:
        return (
            self.path(source, endpoint, key, ext).exists()
            or self.path(source, endpoint, key, "empty").exists()
        )

    def _atomic_write(self, target: Path, data: bytes) -> Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, target)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        # Si antes estaba marcado como vacío, ya no lo está
        empty = target.with_suffix(".empty")
        if empty != target:
            empty.unlink(missing_ok=True)
        return target

    def write_json(self, source: str, endpoint: str, key: str, payload: Any) -> Path:
        data = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
        return self._atomic_write(self.path(source, endpoint, key, "json"), data)

    def write_bytes(self, source: str, endpoint: str, key: str, data: bytes, ext: str) -> Path:
        return self._atomic_write(self.path(source, endpoint, key, ext), data)

    def mark_empty(self, source: str, endpoint: str, key: str, reason: str = "") -> Path:
        target = self.path(source, endpoint, key, "empty")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(reason)
        return target

    def read_json(self, source: str, endpoint: str, key: str) -> Any:
        return json.loads(self.path(source, endpoint, key).read_text("utf-8"))

    def iter_json(self, source: str, endpoint: str) -> Iterator[tuple[str, Any]]:
        d = self._dir(source, endpoint)
        if not d.exists():
            return
        for p in sorted(d.glob("*.json")):
            yield p.stem, json.loads(p.read_text("utf-8"))

    def iter_files(self, source: str, endpoint: str, ext: str) -> Iterator[Path]:
        d = self._dir(source, endpoint)
        if d.exists():
            yield from sorted(d.glob(f"*.{ext}"))

    def endpoints(self, source: str) -> list[str]:
        d = self.base / source
        return sorted(p.name for p in d.iterdir() if p.is_dir()) if d.exists() else []
