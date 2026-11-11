"""Local history: a small SQLite file with metric samples, cluster events and
the journal of changes made through this server.

Ceph keeps no history of its own beyond a short log, and MicroCeph ships no
Prometheus. Recording a few dozen figures once a minute is enough to answer
"since when", "how fast" and "what was normal".

Samples are kept at full resolution for two days and as hourly averages (with
minimum and maximum) for as long as the retention says. One file per cluster,
named after its fsid. Without a state directory the store lives in memory and
ends with the process.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

RAW_KEEP = 48 * 3600
ROLLUP_STEP = 3600
SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS series (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL);
CREATE TABLE IF NOT EXISTS points (
    series INTEGER NOT NULL, ts INTEGER NOT NULL, value REAL NOT NULL,
    PRIMARY KEY (series, ts)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS rollup (
    series INTEGER NOT NULL, ts INTEGER NOT NULL,
    avg REAL NOT NULL, min REAL NOT NULL, max REAL NOT NULL, n INTEGER NOT NULL,
    PRIMARY KEY (series, ts)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY, ts REAL NOT NULL, kind TEXT NOT NULL,
    subject TEXT NOT NULL, message TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS events_ts ON events (ts);
CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS changes (
    id INTEGER PRIMARY KEY, ts REAL NOT NULL, tool TEXT NOT NULL, summary TEXT NOT NULL,
    arguments TEXT NOT NULL, commands TEXT NOT NULL, undo TEXT NOT NULL,
    client TEXT NOT NULL, outcome TEXT NOT NULL, undone_by INTEGER, undoes INTEGER);
CREATE TABLE IF NOT EXISTS lease (
    id INTEGER PRIMARY KEY CHECK (id = 1), owner TEXT NOT NULL, expires REAL NOT NULL);
"""


@dataclass(frozen=True)
class Event:
    ts: float
    kind: str
    subject: str
    message: str


@dataclass(frozen=True)
class Change:
    id: int
    ts: float
    tool: str
    summary: str
    arguments: dict[str, Any]
    commands: list[str]
    undo: list[list[str]]
    client: str
    outcome: str
    undone_by: int | None
    undoes: int | None


@dataclass(frozen=True)
class Coverage:
    first: float | None
    last: float | None
    samples: int
    series: int
    events: int
    size_bytes: int | None


Point = tuple[float, float, float, float]
"""(time, average, minimum, maximum) of one bucket."""


def database_path(state_dir: str | None, fsid: str) -> str:
    """Where the history of a cluster lives; ':memory:' without a state directory."""
    if not state_dir:
        return ":memory:"
    safe = "".join(c for c in fsid if c.isalnum() or c == "-") or "cluster"
    return str(Path(state_dir).expanduser() / f"{safe}.db")


class Store:
    """All access to the history file. Methods block briefly; call them in a
    worker thread from async code (see `App.history`)."""

    def __init__(self, path: str = ":memory:"):
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # The file names pools, hosts and the commands that were run: owner only.
            Path(path).touch(mode=0o600, exist_ok=True)
            with contextlib.suppress(OSError):
                Path(path).chmod(0o600)
        self._db = sqlite3.connect(path, check_same_thread=False, timeout=15, isolation_level=None)
        self._lock = threading.RLock()
        self._series: dict[str, int] = {}
        with self._lock:
            if path != ":memory:":
                self._db.execute("PRAGMA journal_mode=WAL")
                self._db.execute("PRAGMA synchronous=NORMAL")
            self._db.executescript(_SCHEMA)
            self._db.execute(
                "INSERT OR IGNORE INTO meta VALUES ('schema', ?)", (str(SCHEMA_VERSION),)
            )

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ------------------------------------------------------------- recorder

    def acquire(self, owner: str, now: float, ttl: float) -> bool:
        """Take or renew the right to record. One recorder per file, so that
        several servers on the same cluster do not all sample it."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute("SELECT owner, expires FROM lease").fetchone()
                if row and row[0] != owner and row[1] > now:
                    return False
                self._db.execute(
                    "INSERT OR REPLACE INTO lease VALUES (1, ?, ?)", (owner, now + ttl)
                )
                return True
            finally:
                self._db.execute("COMMIT")

    def release(self, owner: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM lease WHERE owner = ?", (owner,))

    def recorder(self, now: float) -> str | None:
        """Who is recording right now, if anyone."""
        with self._lock:
            row = self._db.execute("SELECT owner, expires FROM lease").fetchone()
        return row[0] if row and row[1] > now else None

    def _ids(self, names: Iterable[str]) -> dict[str, int]:
        missing = [n for n in names if n not in self._series]
        if missing:
            self._db.executemany(
                "INSERT OR IGNORE INTO series (name) VALUES (?)", [(n,) for n in missing]
            )
            for name, sid in self._db.execute("SELECT name, id FROM series"):
                self._series[name] = sid
        return self._series

    def write(self, ts: float, values: Mapping[str, float], events: Sequence[Event] = ()) -> None:
        """Store one sample of every series, and the events found with it."""
        stamp = int(ts)
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                ids = self._ids(values)
                self._db.executemany(
                    "INSERT OR REPLACE INTO points VALUES (?, ?, ?)",
                    [(ids[name], stamp, float(v)) for name, v in values.items() if v == v],
                )
                self._db.executemany(
                    "INSERT INTO events (ts, kind, subject, message) VALUES (?, ?, ?, ?)",
                    [(e.ts, e.kind, e.subject, e.message) for e in events],
                )
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def compact(self, now: float, keep_days: float) -> None:
        """Fold samples older than two days into hourly averages, and drop
        what is older than the retention."""
        cutoff = int(now - RAW_KEEP) // ROLLUP_STEP * ROLLUP_STEP
        horizon = now - keep_days * 86400
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._db.execute(
                    """INSERT OR REPLACE INTO rollup
                       SELECT series, ts / :step * :step, avg(value), min(value), max(value),
                              count(*)
                       FROM points WHERE ts < :cutoff GROUP BY series, ts / :step""",
                    {"step": ROLLUP_STEP, "cutoff": cutoff},
                )
                self._db.execute("DELETE FROM points WHERE ts < ?", (cutoff,))
                self._db.execute("DELETE FROM rollup WHERE ts < ?", (horizon,))
                self._db.execute("DELETE FROM events WHERE ts < ?", (horizon,))
                self._db.execute(
                    "DELETE FROM series WHERE id NOT IN (SELECT series FROM points) "
                    "AND id NOT IN (SELECT series FROM rollup)"
                )
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._series.clear()

    def get_state(self, key: str) -> Any:
        with self._lock:
            row = self._db.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def set_state(self, key: str, value: Any) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO state VALUES (?, ?)", (key, json.dumps(value)))

    # --------------------------------------------------------------- reading

    def coverage(self) -> Coverage:
        with self._lock:
            first, last, samples = self._db.execute(
                "SELECT min(ts), max(ts), count(DISTINCT ts) FROM points"
            ).fetchone()
            old_first, old_last, old_n = self._db.execute(
                "SELECT min(ts), max(ts), count(DISTINCT ts) FROM rollup"
            ).fetchone()
            series = self._db.execute("SELECT count(*) FROM series").fetchone()[0]
            events = self._db.execute("SELECT count(*) FROM events").fetchone()[0]
        if old_first is not None:
            first = old_first if first is None else min(first, old_first)
            last = old_last + ROLLUP_STEP if last is None else last
        size = None
        if self.path != ":memory:":
            # Recent writes sit in the write-ahead log until SQLite folds them in.
            size = 0
            for name in (self.path, self.path + "-wal"):
                with contextlib.suppress(OSError):
                    size += Path(name).stat().st_size
        return Coverage(first, last, (samples or 0) + (old_n or 0), series, events, size)

    def names(self, pattern: str = "*") -> list[str]:
        with self._lock:
            rows = self._db.execute(
                "SELECT name FROM series WHERE name GLOB ? ORDER BY name", (pattern,)
            ).fetchall()
        return [r[0] for r in rows]

    def read(self, pattern: str, start: float, end: float) -> dict[str, list[Point]]:
        """Series matching a glob pattern, each as time-ordered points. Hourly
        buckets cover the time before the full-resolution samples begin."""
        with self._lock:
            fine = self._db.execute(
                """SELECT s.name, p.ts, p.value, p.value, p.value FROM points p
                   JOIN series s ON s.id = p.series
                   WHERE s.name GLOB ? AND p.ts BETWEEN ? AND ? ORDER BY s.name, p.ts""",
                (pattern, start, end),
            ).fetchall()
            coarse = self._db.execute(
                """SELECT s.name, r.ts, r.avg, r.min, r.max FROM rollup r
                   JOIN series s ON s.id = r.series
                   WHERE s.name GLOB ? AND r.ts BETWEEN ? AND ? ORDER BY s.name, r.ts""",
                (pattern, start - ROLLUP_STEP, end),
            ).fetchall()
        out: dict[str, list[Point]] = {}
        for name, ts, avg, low, high in coarse:
            # An hourly bucket stands for its hour; place it at the middle.
            out.setdefault(name, []).append((ts + ROLLUP_STEP / 2, avg, low, high))
        for name, ts, avg, low, high in fine:
            out.setdefault(name, []).append((float(ts), avg, low, high))
        return out

    def latest(self, pattern: str = "*") -> dict[str, tuple[float, float]]:
        """The most recent sample of each matching series: name -> (time, value)."""
        with self._lock:
            rows = self._db.execute(
                """SELECT s.name, p.ts, p.value FROM points p JOIN series s ON s.id = p.series
                   WHERE s.name GLOB ?
                     AND p.ts = (SELECT max(ts) FROM points WHERE series = p.series)""",
                (pattern,),
            ).fetchall()
        return {name: (float(ts), value) for name, ts, value in rows}

    def events(self, start: float, end: float) -> list[Event]:
        with self._lock:
            rows = self._db.execute(
                "SELECT ts, kind, subject, message FROM events WHERE ts BETWEEN ? AND ? "
                "ORDER BY ts, id",
                (start, end),
            ).fetchall()
        return [Event(*row) for row in rows]

    # --------------------------------------------------------------- journal

    def log_change(
        self,
        ts: float,
        tool: str,
        summary: str,
        arguments: Mapping[str, Any],
        commands: Sequence[str],
        undo: Sequence[Sequence[str]],
        client: str,
        outcome: str,
        undoes: int | None = None,
    ) -> int:
        with self._lock:
            cursor = self._db.execute(
                "INSERT INTO changes (ts, tool, summary, arguments, commands, undo, client, "
                "outcome, undoes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ts,
                    tool,
                    summary,
                    json.dumps(arguments, default=str),
                    json.dumps(list(commands)),
                    json.dumps([list(c) for c in undo]),
                    client,
                    outcome,
                    undoes,
                ),
            )
            change_id = int(cursor.lastrowid or 0)
            if undoes is not None:
                self._db.execute(
                    "UPDATE changes SET undone_by = ? WHERE id = ?", (change_id, undoes)
                )
            return change_id

    def changes(
        self, start: float = 0.0, end: float = float("inf"), limit: int = 1000
    ) -> list[Change]:
        """Journal entries in a time window, newest first."""
        with self._lock:
            rows = self._db.execute(
                "SELECT id, ts, tool, summary, arguments, commands, undo, client, outcome, "
                "undone_by, undoes FROM changes WHERE ts BETWEEN ? AND ? ORDER BY id DESC LIMIT ?",
                (start, min(end, 1e18), limit),
            ).fetchall()
        return [self._change(row) for row in rows]

    def change(self, change_id: int) -> Change | None:
        with self._lock:
            row = self._db.execute(
                "SELECT id, ts, tool, summary, arguments, commands, undo, client, outcome, "
                "undone_by, undoes FROM changes WHERE id = ?",
                (change_id,),
            ).fetchone()
        return self._change(row) if row else None

    @staticmethod
    def _change(row: Sequence[Any]) -> Change:
        return Change(
            id=row[0],
            ts=row[1],
            tool=row[2],
            summary=row[3],
            arguments=json.loads(row[4]),
            commands=json.loads(row[5]),
            undo=json.loads(row[6]),
            client=row[7],
            outcome=row[8],
            undone_by=row[9],
            undoes=row[10],
        )
