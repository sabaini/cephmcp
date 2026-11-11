"""Compact, model-friendly rendering of tool results.

Tool results are JSON text. Lists of similar records are rendered as
{"columns": [...], "rows": [[...], ...]} so that keys are not repeated per row.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

_UNITS = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB")


def fmt_bytes(n: float | int | None) -> str | None:
    """1536 -> '1.5 KiB'. Three significant digits, IEC units."""
    if n is None:
        return None
    n = float(n)
    if math.isnan(n):
        return None
    sign, n = ("-", -n) if n < 0 else ("", n)
    i = 0
    while n >= 1024 and i < len(_UNITS) - 1:
        n /= 1024
        i += 1
    if i == 0:
        return f"{sign}{int(n)} B"
    return f"{sign}{n:.3g} {_UNITS[i]}"


def fmt_rate(n: float | int | None) -> str | None:
    s = fmt_bytes(n)
    return None if s is None else s + "/s"


def num(x: Any, digits: int = 3) -> Any:
    """Round floats to `digits` decimals, drop NaN/inf, pass everything else through."""
    if isinstance(x, bool) or not isinstance(x, float):
        return x
    if math.isnan(x) or math.isinf(x):
        return None
    r = round(x, digits)
    return int(r) if r == int(r) and abs(r) < 1e15 else r


def pct(part: float | int | None, whole: float | int | None, digits: int = 1) -> float | None:
    if part is None or not whole:
        return None
    return round(100.0 * float(part) / float(whole), digits)


def table(
    rows: Iterable[Mapping[str, Any]], columns: Sequence[str] | None = None
) -> dict[str, Any]:
    """Render records as columns + rows.

    Columns default to first-seen key order, leaving out those that are empty
    on every row.
    """
    rows = list(rows)
    if columns is None:
        seen: dict[str, None] = {}
        for row in rows:
            for key, value in row.items():
                if value is not None:
                    seen.setdefault(key)
        columns = list(seen)
    return {
        "columns": list(columns),
        "rows": [[num(row.get(c)) for c in columns] for row in rows],
    }


def limited(
    rows: Sequence[Mapping[str, Any]],
    limit: int,
    columns: Sequence[str] | None = None,
    *,
    what: str = "rows",
) -> dict[str, Any]:
    """table() of the first `limit` rows, with a note when rows were left out."""
    out = table(rows[:limit], columns)
    out["count"] = len(rows)
    if len(rows) > limit:
        out["truncated"] = f"showing {limit} of {len(rows)} {what}; filter or raise limit"
    return out


def compact(obj: Any) -> Any:
    """Recursively drop None values and empty containers, and round floats."""
    if isinstance(obj, Mapping):
        out = {}
        for key, value in obj.items():
            value = compact(value)
            if value is None or value == {} or value == []:
                continue
            out[key] = value
        return out
    if isinstance(obj, list | tuple):
        return [compact(v) for v in obj]
    return num(obj)


def dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)


def clip(text: str, limit: int) -> str:
    """Cut text to `limit` characters, saying so."""
    if len(text) <= limit:
        return text
    return (
        text[:limit]
        + f"\n[output truncated: {limit} of {len(text)} characters shown; narrow the request]"
    )
