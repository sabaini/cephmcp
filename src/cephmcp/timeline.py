"""Turn the cluster's own records into one timeline of what happened.

Sources: the cluster log (health transitions, OSDs going down and up,
elections), the audit log (commands that changed something), the history of
the configuration database, crash reports, the progress module, and the
events the recorder noted. Routine chatter is dropped and repeats collapsed,
so that the result reads as a sequence of events.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from .redact import REDACTED, is_sensitive_name, redact_keys, redact_text

CLUSTER, AUDIT, CONFIG, CRASH, PROGRESS, RECORDER, JOURNAL = (
    "cluster log",
    "audit log",
    "config",
    "crash",
    "progress",
    "recorder",
    "cephmcp",
)


@dataclass(frozen=True)
class Entry:
    ts: float
    source: str
    text: str
    level: str = "info"
    key: str = ""
    """Entries with the same non-empty key that follow each other are one event."""
    count: int = 1
    until: float | None = None


_FORMATS = ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S.%f%z")


def parse_stamp(stamp: Any) -> float | None:
    """'2026-11-11T14:06:35.464323+0000' (or with Z, or naive UTC) -> epoch seconds."""
    if isinstance(stamp, int | float):
        return float(stamp)
    if not isinstance(stamp, str) or not stamp:
        return None
    text = stamp.strip()
    for fmt in _FORMATS:
        try:
            return datetime.strptime(text, fmt).timestamp()
        except ValueError:
            continue
    try:
        naive = datetime.strptime(text[:19].replace(" ", "T"), "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None
    return naive.replace(tzinfo=timezone.utc).timestamp()


def fmt_time(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------- cluster log

# Periodic or per-PG chatter that says nothing about what happened.
_NOISE = re.compile(
    r"^overall HEALTH_|^pgmap v|continuing backfill|scrub (starts|ok)$|^OSD bench result|"
    r"^mkfs |^from='|^osdmap e\d+|^fsmap |^mgrmap e\d+|^Standby manager daemon .* started$"
)
_HEALTH = re.compile(r"^Health check (failed|update|cleared): .*?\(?([A-Z][A-Z0-9_]{3,})\)?")
_HEALTH_CODE = re.compile(r"\(([A-Z][A-Z0-9_]{3,})\)$|^Health check cleared: ([A-Z][A-Z0-9_]{3,})")
_ADDRESSES = re.compile(r" \[v[12]:[^\]]*\]")
_LEVELS = {"[DBG]": "debug", "[INF]": "info", "[SEC]": "warn", "[WRN]": "warn", "[ERR]": "error"}


def cluster_entries(log: Iterable[Mapping[str, Any]]) -> list[Entry]:
    out = []
    for item in log:
        ts = parse_stamp(item.get("stamp"))
        message = str(item.get("message") or "").strip()
        if ts is None or not message or _NOISE.search(message):
            continue
        key = ""
        if message.startswith("Health check update"):
            # The count in a check changes all the time during recovery.
            code = _HEALTH_CODE.search(message)
            key = f"update:{code.group(1) or code.group(2)}" if code else ""
        else:
            # The same thing happening to several daemons in a row is one line.
            key = re.sub(r"\d+", "N", message)[:120]
        level = _LEVELS.get(str(item.get("priority") or ""), "info")
        # "osd.3 [v2:10.0.0.1:6802/1,v1:10.0.0.1:6803/1] boot": the addresses add nothing.
        message = _ADDRESSES.sub("", message)
        out.append(Entry(ts, CLUSTER, message, level, key))
    return out


# ------------------------------------------------------------------ audit log

_AUDIT = re.compile(
    r"entity='(?P<entity>[^']*)'\s+cmd='?(?P<cmd>.*?)'?\s*:\s*(?P<outcome>dispatch|finished)\s*$",
    re.DOTALL,
)
# Commands that daemons run on their own behalf all day long.
_AUTOMATIC = re.compile(r"^(mgr|osd|mon|mds|rgw)\.")
_AUTOMATIC_KEEP = re.compile(
    r"^(osd (pool|out|in|down|purge|destroy|reweight|rm)|fs |osd pg-upmap)"
)
# The configuration log tells these better, with the previous value.
_IN_CONFIG_LOG = re.compile(r"^config (set|rm|assimilate-conf|reset)$")
_SKIP_ARGS = {"prefix", "format", "target"}
_VALUE_ARGS = {"value", "val"}


def render_command(command: Mapping[str, Any], *, secrets: bool = False) -> str:
    """{"prefix": "osd pool set", "pool": "vms", "var": "size", "val": "2"} ->
    'osd pool set pool=vms var=size val=2'.

    Unless secrets are allowed, a value is masked when its own argument is
    named like a secret (password=...), when it is the value of an option or
    key named like one (key=mgr/dashboard/jwt_secret val=...), or when it
    looks like a key."""
    names = [str(command.get(k) or "") for k in ("name", "key", "var")]
    sensitive = any(is_sensitive_name(n) for n in names if n)
    parts = [str(command.get("prefix") or "?")]
    for name, value in command.items():
        if name in _SKIP_ARGS or value in (None, "", []):
            continue
        if isinstance(value, list):
            value = ",".join(str(v) for v in value)
        text = str(value)[:80]
        if not secrets:
            if is_sensitive_name(name) or (sensitive and name in _VALUE_ARGS):
                text = REDACTED
            else:
                text = redact_keys(text)
        parts.append(f"{name}={text}")
    return " ".join(parts)


def _command(text: str) -> dict[str, Any] | None:
    try:
        command = json.loads(text)
    except ValueError:
        return None
    if isinstance(command, list):
        command = command[0] if command else None
    return command if isinstance(command, dict) else None


def audit_message(message: str, *, secrets: bool = False) -> str:
    """An audit log line with its command in readable form, secrets masked:
    'client.admin finished: osd pool set pool=vms var=size val=2'."""
    match = _AUDIT.search(message)
    command = _command(match["cmd"]) if match else None
    if match and command is not None:
        rendered = render_command(command, secrets=secrets)
        return f"{match['entity']} {match['outcome']}: {rendered}"
    return message if secrets else redact_text(message)


def audit_entries(
    log: Iterable[Mapping[str, Any]],
    read_only: Callable[[str], bool],
    *,
    secrets: bool = False,
) -> list[Entry]:
    """Commands that completed and changed something, by whom."""
    out = []
    for item in log:
        ts = parse_stamp(item.get("stamp"))
        match = _AUDIT.search(str(item.get("message") or ""))
        if ts is None or not match or match["outcome"] != "finished":
            continue
        command = _command(match["cmd"])
        if command is None:
            continue
        prefix, entity = str(command.get("prefix") or ""), match["entity"]
        if not prefix or read_only(prefix) or _IN_CONFIG_LOG.match(prefix):
            continue
        automatic = bool(_AUTOMATIC.match(entity))
        if automatic and not _AUTOMATIC_KEEP.match(prefix):
            continue
        text = f"{entity} ran: {render_command(command, secrets=secrets)}"
        out.append(Entry(ts, AUDIT, text, key=f"{entity}:{prefix}" if automatic else ""))
    return out


# -------------------------------------------------------------- other sources


def config_entries(
    config_log: Iterable[Mapping[str, Any]], *, secrets: bool = False
) -> list[Entry]:
    out = []
    for item in config_log:
        ts = parse_stamp(item.get("timestamp"))
        if ts is None:
            continue
        for change in item.get("changes") or []:
            name = str(change.get("name") or "")
            old, new = change.get("previous_value"), change.get("new_value")
            if not secrets and is_sensitive_name(name):
                old = REDACTED if old is not None else None
                new = REDACTED if new is not None else None
            if new is None:
                text = f"{name} removed (was {old})"
            elif old is None:
                text = f"{name} set to {new}"
            else:
                text = f"{name} changed from {old} to {new}"
            out.append(Entry(ts, CONFIG, text))
    return out


def crash_entries(crashes: Iterable[Mapping[str, Any]]) -> list[Entry]:
    out = []
    for crash in crashes:
        ts = parse_stamp(crash.get("timestamp"))
        if ts is not None:
            out.append(
                Entry(
                    ts,
                    CRASH,
                    f"{crash.get('entity_name', 'a daemon')} crashed (crash_list "
                    f"crash_id='{crash.get('crash_id')}' has the backtrace)",
                    "error",
                )
            )
    return out


def progress_entries(progress: Mapping[str, Any] | None) -> list[Entry]:
    out = []
    for event in (progress or {}).get("completed") or []:
        start, end = event.get("started_at"), event.get("finished_at")
        if not isinstance(start, int | float) or not event.get("message"):
            continue
        took = f" (took {_span(end - start)})" if isinstance(end, int | float) else ""
        out.append(Entry(float(start), PROGRESS, f"{event['message']}{took}"))
    for event in (progress or {}).get("events") or []:
        start = event.get("started_at")
        if isinstance(start, int | float) and event.get("message"):
            done = round(100 * float(event.get("progress") or 0))
            out.append(Entry(float(start), PROGRESS, f"{event['message']} ({done}% so far)"))
    return out


def _span(seconds: float) -> str:
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds / size:.3g}{unit}"
    return f"{seconds:.0f}s"


# ------------------------------------------------------------------- merging


def collapse(entries: Iterable[Entry]) -> list[Entry]:
    """Time order, with runs of the same kind of entry merged into one."""
    out: list[Entry] = []
    last: dict[tuple[str, str], int] = {}
    for entry in sorted(entries, key=lambda e: e.ts):
        slot = (entry.source, entry.key)
        index = last.get(slot)
        # Merge with the previous entry of the same kind unless something
        # else from that source happened in between.
        if entry.key and index is not None and _run_unbroken(out, index, entry.source):
            previous = out[index]
            out[index] = replace(
                previous,
                count=previous.count + 1,
                until=entry.ts,
                text=entry.text if entry.key.startswith("update:") else previous.text,
            )
            continue
        out.append(entry)
        last[slot] = len(out) - 1
    return out


def _run_unbroken(entries: list[Entry], index: int, source: str) -> bool:
    return all(e.source != source for e in entries[index + 1 :])


def describe(entry: Entry) -> str:
    if entry.count <= 1:
        return entry.text
    until = fmt_time(entry.until)
    more = entry.count - 1
    return f"{entry.text} (and {more} more like it, until {until[11:] if until else '?'})"
