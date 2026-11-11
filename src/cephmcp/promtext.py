"""Parser for the Prometheus text exposition format.

Small and dependency-free. Each distinct sample name is treated as its own
metric: Ceph exposes latency counters as separate `<name>_sum` and
`<name>_count` series, and histograms as `<name>_bucket`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_LABEL_RE = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"\\]*)"')
_SUFFIXES = ("_bucket", "_sum", "_count", "_total", "_created")


@dataclass(frozen=True)
class Sample:
    labels: tuple[tuple[str, str], ...]
    value: float

    def label(self, name: str, default: str = "") -> str:
        for key, value in self.labels:
            if key == name:
                return value
        return default


@dataclass
class Metric:
    name: str
    type: str = "untyped"
    help: str = ""
    samples: list[Sample] = field(default_factory=list)


def _unescape(value: str) -> str:
    out, i, n = [], 0, len(value)
    while i < n:
        ch = value[i]
        if ch == "\\" and i + 1 < n:
            nxt = value[i + 1]
            out.append({"n": "\n", "\\": "\\", '"': '"'}.get(nxt, "\\" + nxt))
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _parse_labels(block: str) -> tuple[tuple[str, str], ...]:
    if "\\" not in block:
        return tuple(_LABEL_RE.findall(block))
    # Slow path: label values containing escapes.
    labels: list[tuple[str, str]] = []
    i, n = 0, len(block)
    while i < n:
        while i < n and block[i] in ", \t":
            i += 1
        eq = block.find("=", i)
        if eq < 0:
            break
        name = block[i:eq].strip()
        start = block.find('"', eq)
        if start < 0:
            break
        j = start + 1
        while j < n and block[j] != '"':
            j += 2 if block[j] == "\\" else 1
        labels.append((name, _unescape(block[start + 1 : j])))
        i = j + 1
    return tuple(labels)


def _closing_brace(line: str, start: int) -> int:
    """Index of the '}' ending the label block opened at `start`, or -1."""
    in_quotes = False
    i, n = start + 1, len(line)
    while i < n:
        ch = line[i]
        if in_quotes:
            if ch == "\\":
                i += 1
            elif ch == '"':
                in_quotes = False
        elif ch == '"':
            in_quotes = True
        elif ch == "}":
            return i
        i += 1
    return -1


def _to_float(token: str) -> float:
    try:
        return float(token)
    except ValueError:
        low = token.lower()
        if low in ("+inf", "inf"):
            return float("inf")
        if low == "-inf":
            return float("-inf")
        return float("nan")


def parse(text: str) -> dict[str, Metric]:
    """Parse exposition text into {metric name: Metric}. Malformed lines are skipped."""
    metrics: dict[str, Metric] = {}
    declared: dict[str, tuple[str, str]] = {}  # family -> (type, help)

    def get(name: str) -> Metric:
        metric = metrics.get(name)
        if metric is None:
            mtype, mhelp = declared.get(name, ("", ""))
            if not mtype:
                for suffix in _SUFFIXES:
                    if name.endswith(suffix) and name[: -len(suffix)] in declared:
                        mtype, mhelp = declared[name[: -len(suffix)]]
                        break
            metric = metrics[name] = Metric(name, mtype or "untyped", mhelp)
        return metric

    for line in text.splitlines():
        if not line or line.isspace():
            continue
        if line[0] == "#":
            parts = line.split(None, 3)
            if len(parts) >= 3 and parts[1] in ("HELP", "TYPE"):
                family = parts[2]
                rest = parts[3].strip() if len(parts) > 3 else ""
                mtype, mhelp = declared.get(family, ("", ""))
                if parts[1] == "TYPE":
                    mtype = rest
                else:
                    mhelp = _unescape(rest) if "\\" in rest else rest
                declared[family] = (mtype, mhelp)
                existing = metrics.get(family)
                if existing is not None:
                    existing.type, existing.help = mtype or existing.type, mhelp
            continue

        brace = line.find("{")
        space = line.find(" ")
        if brace != -1 and (space == -1 or brace < space):
            end = _closing_brace(line, brace)
            if end < 0:
                continue
            name = line[:brace].strip()
            labels = _parse_labels(line[brace + 1 : end])
            rest = line[end + 1 :].split()
        else:
            fields = line.split()
            name, labels, rest = fields[0], (), fields[1:]
        if not name or not rest:
            continue
        get(name).samples.append(Sample(labels, _to_float(rest[0])))

    return metrics
