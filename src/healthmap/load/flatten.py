from typing import Any

SCALARS = (str, int, float, bool)


def flatten(obj: dict[str, Any], prefix: str = "", max_depth: int = 3, sep: str = "_") -> dict:
    """
    Aplana dicts anidados a columnas `padre_hijo`. Las listas se descartan (van a tablas
    hijas propias). Profundidad limitada para no generar cientos de columnas.
    """
    out: dict[str, Any] = {}
    for k, v in obj.items():
        name = f"{prefix}{sep}{k}" if prefix else k
        if v is None or isinstance(v, SCALARS):
            out[name] = v
        elif isinstance(v, dict) and max_depth > 1:
            out.update(flatten(v, name, max_depth - 1, sep))
    return out


def semicircles_to_degrees(value: int | float | None) -> float | None:
    if value is None:
        return None
    return value * (180.0 / 2**31)
