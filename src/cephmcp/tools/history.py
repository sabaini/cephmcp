"""History: what changed, how figures developed, when space runs out, which
disk is drifting away from its peers."""

from __future__ import annotations

import fnmatch
import re
import statistics
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from .. import timeline
from ..analysis import Point, Shift, Trend, buckets, find_shift, fit, outliers
from ..app import App, UsageError
from ..ceph import gather
from ..history import RAW_KEEP, ROLLUP_STEP, Store
from ..http import HttpError
from ..policy import is_read_only
from ..render import compact, fmt_bytes, fmt_rate, limited, num, pct, table
from ..topology import Osd, Topology, build
from .perf import parse_time

DAY = 86400.0
LOG_WINDOW = 10000
MAX_SHIFTS = 15
SAME_TIME = 300.0
"""Shifts of the same metric within this many seconds count as one event."""
OutlierMetric = Literal["commit_ms", "apply_ms", "op_w_latency_ms", "op_r_latency_ms", "used_pct"]
# Series in which a lasting change of level is an event: (pattern, at least
# this relative change, at least this absolute change).
SHIFT_RULES: tuple[tuple[str, float, float], ...] = (
    ("*_latency_ms", 0.5, 2.0),
    ("*/commit_ms*", 0.5, 5.0),
    ("*/apply_ms", 0.5, 5.0),
    ("*_bytes_sec", 0.5, 1024.0 * 1024),
    ("*_ops_sec", 0.5, 20.0),
    ("*_per_sec", 0.5, 20.0),
    ("cluster/degraded_pct", 0.5, 0.5),
    ("cluster/misplaced_pct", 0.5, 0.5),
    ("cluster/pgs_not_clean", 0.5, 1.0),
)


def fmt_value(series: str, value: float | None) -> Any:
    """A figure with the unit its series name implies."""
    if value is None:
        return None
    if series.endswith("_bytes_sec"):
        return fmt_rate(value)
    if series.endswith("_bytes"):
        return fmt_bytes(value)
    if series.endswith("_ms") or "_ms_" in series:
        return f"{value:.3g} ms"
    if series.endswith("_pct"):
        return f"{value:.3g}%"
    if series.endswith("_sec"):
        return f"{value:.3g}/s"
    return num(float(value), 2)


def span(seconds: float) -> str:
    for unit, size in (("d", DAY), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds / size:.3g}{unit}"
    return f"{seconds:.0f}s"


def window(start: str, end: str, now: float) -> tuple[float, float]:
    t0, t1 = parse_time(start, now), parse_time(end or "now", now)
    if isinstance(t0, str):
        t0 = timeline.parse_stamp(t0)
    if isinstance(t1, str):
        t1 = timeline.parse_stamp(t1)
    if t0 is None or t1 is None:
        raise UsageError(
            "times must be relative ('-6h', '-2d'), epoch seconds or like 2026-11-11T14:00:00Z"
        )
    if t0 >= t1:
        raise UsageError("the start must be before the end")
    return float(t0), float(t1)


def shift_rule(series: str) -> tuple[float, float] | None:
    for pattern, relative, absolute in SHIFT_RULES:
        if fnmatch.fnmatchcase(series, pattern):
            return relative, absolute
    return None


def shift_of(series: str, points: list[Point], since: float = 0.0) -> Shift | None:
    rule = shift_rule(series)
    if rule is None:
        return None
    shift = find_shift(
        [(t, v) for t, v, _, _ in points], min_relative=rule[0], min_absolute=rule[1]
    )
    return shift if shift and shift.at >= since else None


def describe_shift(series: str, shift: Shift) -> str:
    verb = "rose" if shift.change > 0 else "fell"
    return (
        f"{verb} from {fmt_value(series, shift.before)} to {fmt_value(series, shift.after)} "
        f"at {timeline.fmt_time(shift.at)}"
    )


def summarize_shifts(shifts: list[tuple[float, str, Shift]]) -> list[dict[str, Any]]:
    """Rows for the shifts found, the most pronounced first. When several OSDs
    (or pools, or hosts) show the same shift at about the same time, that is
    one event and becomes one row."""
    groups: dict[tuple[str, str, bool, int], list[tuple[float, str, Shift]]] = {}
    for score, name, shift in shifts:
        kind, _, rest = name.partition("/")
        metric = rest.rpartition("/")[2]
        key = (kind, metric, shift.change > 0, int(shift.at // SAME_TIME))
        groups.setdefault(key if kind != "cluster" else (name, "", True, 0), []).append(
            (score, name, shift)
        )
    rows = []
    for (kind, metric, _, _), members in groups.items():
        members.sort(key=lambda m: -m[0])
        score, name, shift = members[0]
        if len(members) >= 3:
            before = statistics.median(m[2].before for m in members)
            after = statistics.median(m[2].after for m in members)
            at = statistics.median(m[2].at for m in members)
            rows.append(
                (
                    score,
                    {
                        "series": f"{kind}/*/{metric}",
                        "at": timeline.fmt_time(at),
                        "before": fmt_value(metric, before),
                        "after": fmt_value(metric, after),
                        "seen_on": f"{len(members)} {kind}s (medians)",
                    },
                )
            )
            continue
        for score, name, shift in members:
            rows.append(
                (
                    score,
                    {
                        "series": name,
                        "at": timeline.fmt_time(shift.at),
                        "before": fmt_value(name, shift.before),
                        "after": fmt_value(name, shift.after),
                    },
                )
            )
    rows.sort(key=lambda r: -r[0])
    return [row for _, row in rows]


async def prometheus_range(
    app: App, base: str, query: str, start: float, end: float
) -> list[tuple[float, float]]:
    """One series from a Prometheus server as (time, value) points."""
    s = app.settings
    auth = (s.prometheus_username, s.prometheus_password or "") if s.prometheus_username else None
    step = max(60.0, round((end - start) / 500))
    response = await app.http.request(
        "POST",
        base + "/api/v1/query_range",
        data={"query": query, "start": start, "end": end, "step": step},
        auth=auth,
    )
    body = response.json()
    if body.get("status") != "success":
        raise UsageError(str(body.get("error", response.status_code)))
    result = (body.get("data") or {}).get("result") or []
    if not result:
        return []
    return [(float(t), float(v)) for t, v in result[0].get("values") or []]


@dataclass
class Capacity:
    """What a capacity forecast rests on."""

    now: float
    store: Store
    topology: Topology
    usage: list[tuple[float, float]]
    source: str
    trend: Trend | None
    total: int
    used: int
    fullest: Osd | None
    notes: list[str]

    @property
    def skew(self) -> float:
        """How much fuller the fullest OSD is than the average."""
        if not self.fullest or not self.used or not self.total:
            return 1.0
        return (self.fullest.used_pct / 100) / (self.used / self.total)

    def levels(self) -> list[tuple[str, float, float]]:
        """(limit, its ratio, the raw usage at which the fullest OSD reaches it).
        The fullest OSD gets there first; this assumes the OSDs keep growing
        in the proportion they are filled now."""
        topo = self.topology
        return [
            (label, ratio, ratio * self.total / max(self.skew, 1.0))
            for label, ratio in (
                ("nearfull", topo.nearfull_ratio),
                ("backfillfull", topo.backfillfull_ratio),
                ("full", topo.full_ratio),
            )
        ]


async def capacity_basis(app: App, days: float) -> Capacity:
    ceph, history = app.ceph, app.history
    now = app.clock()
    start = now - days * DAY
    store = await history.store()
    df_tree, osd_dump, rules, df = await gather(
        lambda: ceph.json(["osd", "df", "tree"]),
        lambda: ceph.json(["osd", "dump"]),
        lambda: ceph.try_json(["osd", "crush", "rule", "dump"], []),
        lambda: ceph.json(["df", "detail"]),
    )
    topo = build(df_tree, osd_dump, rules, df)
    stats = (df or {}).get("stats") or {}
    total, used = stats.get("total_bytes") or 0, stats.get("total_used_raw_bytes") or 0
    recorded = await history.call(store.read, "cluster/used_bytes", start, now)
    usage = [(t, v) for t, v, _, _ in recorded.get("cluster/used_bytes", [])]
    source = "recorded history"
    notes: list[str] = []
    found = await app.metrics.discover()
    if found.prometheus_url:
        try:
            remote = await prometheus_range(
                app, found.prometheus_url, "ceph_cluster_total_used_raw_bytes", start, now
            )
        except (HttpError, UsageError, ValueError) as exc:
            remote = []
            notes.append(f"Prometheus could not be asked for more history: {exc}")
        if remote and (not usage or remote[-1][0] - remote[0][0] > usage[-1][0] - usage[0][0]):
            usage, source = remote, "Prometheus"
    in_osds = [o for o in topo.osds.values() if o.in_ and o.size]
    return Capacity(
        now=now,
        store=store,
        topology=topo,
        usage=usage,
        source=source,
        trend=fit(usage) if len(usage) >= 3 else None,
        total=total,
        used=used,
        fullest=max(in_osds, key=lambda o: o.used_pct, default=None),
        notes=notes,
    )


def register(server: MCPServer, app: App) -> None:
    ceph, history = app.ceph, app.history

    async def recording(store: Store) -> dict[str, Any]:
        """Where the history stands: what it covers and whether it is growing."""
        now = app.clock()
        cover = await history.call(store.coverage)
        recorder = await history.call(store.recorder, now)
        notes = []
        if recorder is None:
            notes.append(
                "Nothing is recording right now."
                if not app.settings.record
                else "The recorder of this server has not taken a sample yet."
            )
        if not app.settings.state_dir:
            notes.append(
                "History is kept in memory only (no state directory): it ends with this server."
            )
        elif recorder is None or (cover.first and now - cover.first < DAY):
            notes.append(
                "For history from times when no client is connected, run `cephmcp "
                "--record-only` as a service on a cluster node."
            )
        return compact(
            {
                "from": timeline.fmt_time(cover.first),
                "to": timeline.fmt_time(cover.last),
                "covers": span(cover.last - cover.first) if cover.first and cover.last else None,
                "samples": cover.samples,
                "series": cover.series,
                "recorder": recorder or "not running",
                "interval_s": app.settings.record_interval if recorder else None,
                "file": store.path if store.path != ":memory:" else "in memory",
                "size": fmt_bytes(cover.size_bytes),
                "notes": notes,
            }
        )

    @app.tool(server, title="What changed")
    async def what_changed(
        since: Annotated[
            str,
            Field(
                description="Start of the period: relative ('-2h', '-1d', '-7d'), epoch "
                "seconds or a time like 2026-11-11T14:00:00Z."
            ),
        ] = "-24h",
        until: Annotated[str, Field(description="End of the period; default now.")] = "now",
        limit: Annotated[
            int, Field(ge=1, le=1000, description="Most events to return; the latest are kept.")
        ] = 150,
    ) -> dict[str, Any]:
        """A timeline of what happened to the cluster in a period, merged from
        everything that keeps a record: health checks raised and cleared, OSDs
        and monitors going down and coming back, commands that changed the
        cluster and who ran them, configuration changes with old and new value,
        daemon crashes, autoscaler and orchestrator activity, changes made
        through this server, and lasting shifts in recorded metrics (latency,
        throughput, degraded data) with the time they began.

        The tool for "what changed since yesterday", "what happened before the
        cluster went into WARN" and for reconstructing an incident. Times are
        UTC. `coverage` says how far back each source reaches: the cluster's
        own logs hold a limited number of entries.
        """
        now = app.clock()
        start, end = window(since, until, now)
        store = await history.store()
        cluster_log, audit_log, config_log, crash_list_, progress = await gather(
            lambda: ceph.try_json(["log", "last", str(LOG_WINDOW), "info", "cluster"], []),
            lambda: ceph.try_json(["log", "last", str(LOG_WINDOW), "info", "audit"], []),
            lambda: ceph.try_json(["config", "log", "200"], []),
            lambda: ceph.try_json(["crash", "ls"], []),
            lambda: ceph.try_json(["progress", "json"], {}),
        )
        secrets = app.settings.allow_secrets
        entries = [
            *timeline.cluster_entries(cluster_log or []),
            *timeline.audit_entries(audit_log or [], is_read_only, secrets=secrets),
            *timeline.config_entries(config_log or [], secrets=secrets),
            *timeline.crash_entries(crash_list_ or []),
            *timeline.progress_entries(progress),
        ]

        def first_stamp(log: Any, key: str = "stamp") -> float | None:
            stamps = [timeline.parse_stamp(e.get(key)) for e in log or []]
            return min((s for s in stamps if s is not None), default=None)

        log_from = first_stamp(cluster_log)
        # What the recorder saw matters where the cluster's own log no longer reaches.
        recorded = await history.call(store.events, start, end)
        for event in recorded:
            if event.kind != "recorder" and (log_from is None or event.ts < log_from):
                entries.append(timeline.Entry(event.ts, timeline.RECORDER, event.message))
        for change in await history.call(store.changes, start, end):
            entries.append(
                timeline.Entry(
                    change.ts,
                    timeline.JOURNAL,
                    f"{change.tool}: {app.scrub(change.summary)} "
                    f"[change {change.id}, {app.scrub(change.outcome)[:80]}]",
                )
            )
        merged = timeline.collapse(e for e in entries if start <= e.ts <= end)
        rows = [
            {
                "time": timeline.fmt_time(e.ts),
                "source": e.source,
                "what": timeline.describe(e),
            }
            for e in merged
        ]
        events = limited(rows[-limit:], limit, what="events")
        events["count"] = len(rows)
        if len(rows) > limit:
            events["truncated"] = (
                f"showing the latest {limit} of {len(rows)} events; narrow the period"
            )

        # Shifts: read twice the period, so that a change near its start has a "before".
        series = await history.call(store.read, "*", start - (end - start), end)
        shifts = []
        for name, points in series.items():
            shift = shift_of(name, points, since=start)
            if shift:
                shifts.append((shift.score, name, shift))
        shift_rows = summarize_shifts(shifts)
        cover = await history.call(store.coverage)
        reach = {
            "cluster log": log_from,
            "audit log": first_stamp(audit_log),
            "config log": first_stamp(config_log, "timestamp"),
            "recorded metrics": cover.first,
        }
        notes = []
        # A source that begins after the period does may be missing its early part.
        short = [name for name, t in reach.items() if t is not None and t > start + 60]
        if short:
            notes.append(
                "These records begin after the start of the period, so earlier events "
                f"may be missing from them (see coverage): {', '.join(short)}."
            )
        if cover.first is None:
            notes.append(
                "No metrics have been recorded yet, so shifts in latency or load cannot be "
                "shown. history_query without arguments explains the recorder."
            )
        return compact(
            {
                "period": {"from": timeline.fmt_time(start), "to": timeline.fmt_time(end)},
                "events": events if rows else "nothing on record in this period",
                "metric_shifts": table(shift_rows[:MAX_SHIFTS]) if shift_rows else None,
                "more_shifts": len(shift_rows) - MAX_SHIFTS
                if len(shift_rows) > MAX_SHIFTS
                else None,
                "coverage": {
                    name: f"since {timeline.fmt_time(t)}" if t is not None else "nothing"
                    for name, t in reach.items()
                },
                "notes": notes,
            }
        )

    @app.tool(server, title="Recorded history of a metric")
    async def history_query(
        series: Annotated[
            str,
            Field(
                description="Series name or glob pattern: 'cluster/write_bytes_sec', "
                "'cluster/*latency*', 'osd/*/commit_ms', 'pool/vms/*', 'host/*/used_pct'. "
                "Leave empty for what is recorded and since when."
            ),
        ] = "",
        since: Annotated[
            str, Field(description="Start: relative ('-6h', '-7d'), epoch seconds or a time.")
        ] = "-6h",
        until: Annotated[str, Field(description="End; default now.")] = "now",
        points: Annotated[
            int, Field(ge=2, le=200, description="How many intervals to divide the period into.")
        ] = 24,
        stat: Annotated[
            Literal["avg", "max", "min"],
            Field(description="Which figure to give per interval; 'max' shows peaks."),
        ] = "avg",
        limit: Annotated[int, Field(ge=1, le=200, description="Most series to return.")] = 20,
    ) -> dict[str, Any]:
        """How a recorded figure developed over time, from this server's own
        history (it needs no Prometheus). Per series: the values per interval,
        minimum, average, maximum and latest, and, where the level changed
        lastingly, when it did and from what to what ("when did latency start
        to rise").

        Series are named kind/entity/metric and the suffix is the unit: _bytes,
        _bytes_sec, _ops_sec and _per_sec (per second), _ms, _pct. Recorded for
        the cluster (capacity, client and recovery throughput, degraded data,
        health as 0/1/2), per pool (stored, objects, I/O), per OSD (utilisation,
        commit and apply latency, and op latencies where exporters exist) and
        per host. Samples are a minute apart for two days, hourly before that.
        """
        store = await history.store()
        if not series:
            names = await history.call(store.names)
            kinds: dict[str, set[str]] = {}
            for name in names:
                kind, _, rest = name.partition("/")
                kinds.setdefault(kind, set()).add(rest.rpartition("/")[2])
            return compact(
                {
                    "history": await recording(store),
                    "recorded": {
                        kind: f"{kind}/{'<name>/' if kind != 'cluster' else ''}"
                        + "{"
                        + ",".join(sorted(metrics))
                        + "}"
                        for kind, metrics in sorted(kinds.items())
                    },
                    "hint": "pass series, e.g. 'cluster/*_bytes_sec' or 'osd/*/commit_ms'",
                }
            )
        if not re.fullmatch(r"[A-Za-z0-9_.*?\[\]/:+-]{1,200}", series):
            raise UsageError("series must be a name or a glob pattern such as 'osd/*/commit_ms'")
        now = app.clock()
        start, end = window(since, until, now)
        found = await history.call(store.read, series, start, end)
        if not found:
            known = await history.call(store.names, series)
            state = await recording(store)
            if known:
                raise UsageError(
                    f"{series!r} has no samples in that period; history covers "
                    f"{state.get('from', 'nothing')} to {state.get('to', 'nothing')}"
                )
            raise UsageError(
                f"nothing recorded matches {series!r}; call history_query without "
                "arguments for the series that exist"
            )
        index = {"avg": 1, "min": 2, "max": 3}[stat]
        rows = []
        for name in sorted(found)[:limit]:
            pts = found[name]
            values = [p[1] for p in pts]
            shift = shift_of(name, pts)
            rows.append(
                {
                    "series": name,
                    "min": num(min(p[2] for p in pts), 3),
                    "avg": num(sum(values) / len(values), 3),
                    "max": num(max(p[3] for p in pts), 3),
                    "last": num(values[-1], 3),
                    "shift": describe_shift(name, shift) if shift else None,
                    "values": [
                        num(b[index], 3) if b[index] is not None else None
                        for b in buckets(pts, start, end, points)
                    ],
                }
            )
        out = limited(rows, limit, what="series")
        out["count"] = len(found)
        if len(found) > limit:
            out["truncated"] = f"showing {limit} of {len(found)} series; narrow the pattern"
        step = (end - start) / points
        out["times"] = [timeline.fmt_time(start + i * step) for i in range(points)]
        out["interval"] = span(step)
        if start < now - RAW_KEEP and step < ROLLUP_STEP:
            out["note"] = "older than two days only hourly averages are kept"
        return out

    @app.tool(server, title="Capacity forecast")
    async def capacity_forecast(
        days: Annotated[
            float,
            Field(ge=0.05, le=365, description="How many days of history to base the trend on."),
        ] = 30,
    ) -> dict[str, Any]:
        """When the cluster and each pool run out of space, from the growth so
        far: growth per day, and the dates on which the nearfull limit (Ceph
        starts to warn), the backfillfull limit (recovery stops) and the full
        limit (writes stop) are reached.

        The limits apply to the fullest OSD, not to the average, so the dates
        account for how unevenly the OSDs are filled. The trend is a straight
        line through the recorded usage, or through Prometheus where that has
        more history; `basis` says how much data it rests on. Treat a forecast
        from less than a week of history as a rough indication.
        """
        basis = await capacity_basis(app, days)
        now, store, topo = basis.now, basis.store, basis.topology
        start = now - days * DAY
        usage, source, notes, trend = basis.usage, basis.source, basis.notes, basis.trend
        total, used, fullest = basis.total, basis.used, basis.fullest
        out: dict[str, Any] = {
            "now": {
                "raw_used": fmt_bytes(used),
                "raw_total": fmt_bytes(total),
                "used_pct": pct(used, total),
                "fullest_osd": f"osd.{fullest.id} at {fullest.used_pct:.1f}%" if fullest else None,
            }
        }
        if trend is None or trend.span < 600:
            have = span(usage[-1][0] - usage[0][0]) if len(usage) > 1 else "none"
            out["forecast"] = (
                f"not enough history for a trend (have {have}; ten minutes is the least, a "
                "week gives a useful forecast)"
            )
            state = await recording(store)
            out["history"] = state
            out["notes"] = notes
            return compact(out)
        per_day = trend.slope * DAY
        if trend.span < DAY:
            confidence = "low: less than a day of history"
        elif trend.span < 7 * DAY:
            confidence = "moderate: less than a week of history"
        else:
            confidence = "good" if trend.r2 >= 0.6 else "moderate: usage does not follow a line"
        out["basis"] = {
            "source": source,
            "history": span(trend.span),
            "samples": trend.points,
            "confidence": confidence,
        }
        out["growth_per_day"] = fmt_bytes(per_day)
        if per_day <= 0:
            out["forecast"] = "raw usage is not growing; no limit is being approached"
        elif total and used:
            # The fullest OSD reaches a limit first. Assume all OSDs keep
            # growing in the proportion they are filled now.
            skew = basis.skew
            limits = {}
            for label, ratio, level in basis.levels():
                when = trend.reaches(level, now, current=used)
                if when is None:
                    continue
                left = (when - now) / DAY
                limits[f"{label} ({100 * ratio:.0f}%)"] = (
                    "already reached"
                    if left <= 0
                    else {
                        "in_days": num(left, 1 if left >= 1 else 2),
                        # Within a week the hour matters; beyond it the day does.
                        "at": timeline.fmt_time(when)[: 16 if left < 7 else 10],
                    }
                )
            out["limits_reached"] = limits
            if skew > 1.1:
                notes.append(
                    f"The fullest OSD is {skew:.2f} times as full as the average; evening "
                    "that out (balancer) would move the dates back."
                )
        pools = []
        series = await history.call(store.read, "pool/*/stored_bytes", start, now)
        for pool in sorted(topo.pools.values(), key=lambda p: -p.stored):
            points = [(t, v) for t, v, _, _ in series.get(f"pool/{pool.name}/stored_bytes", [])]
            pool_trend = fit(points) if len(points) >= 3 else None
            growth = pool_trend.slope * DAY if pool_trend else None
            room = pool.max_avail
            pools.append(
                {
                    "pool": pool.name,
                    "stored": fmt_bytes(pool.stored),
                    "room_left": fmt_bytes(room),
                    "growth_per_day": fmt_bytes(growth) if growth is not None else None,
                    "full_in_days": num(room / growth, 1)
                    if growth and growth > 0 and room is not None
                    else None,
                }
            )
        if pools:
            out["pools"] = limited(pools, 100, what="pools")
        out["notes"] = notes
        return compact(out)

    @app.tool(server, title="OSDs that differ from their peers")
    async def osd_outliers(
        metric: Annotated[
            OutlierMetric,
            Field(
                description="What to compare: commit_ms or apply_ms (disk latency, always "
                "available), op_w_latency_ms or op_r_latency_ms (client operation latency, "
                "needs metric exporters), used_pct (how full)."
            ),
        ] = "commit_ms",
        since: Annotated[
            str, Field(description="Period to judge over, e.g. '-24h' or '-7d'.")
        ] = "-24h",
    ) -> dict[str, Any]:
        """Find OSDs that behave unlike the others of their device class: a disk
        whose latency is a multiple of its peers', or that has been drifting
        away from them over the period, which is how failing disks announce
        themselves before they fail. With used_pct: OSDs much fuller than the rest.

        Each OSD's typical value over the period is compared with the median
        of its class. Without recorded history the comparison is made on the
        current values alone, and drift cannot be seen.
        """
        now = app.clock()
        start, _ = window(since, "now", now)
        store = await history.store()
        df_tree, perf = await gather(
            lambda: ceph.json(["osd", "df", "tree"]),
            lambda: ceph.try_json(["osd", "perf"], {}),
        )
        topo = build(df_tree, {}, [], None)
        recorded = await history.call(store.read, f"osd/*/{metric}", start, now)
        by_osd: dict[int, list[Point]] = {}
        for name, points in recorded.items():
            osd = name.split("/")[1]
            if osd.isdigit() and int(osd) in topo.osds:
                by_osd[int(osd)] = points
        basis = "recorded history"
        enough = sum(1 for pts in by_osd.values() if len(pts) >= 8)
        if enough < 3:
            live = {
                p["id"]: p.get("perf_stats") or {}
                for p in ((perf or {}).get("osdstats") or {}).get("osd_perf_infos") or []
            }
            by_osd = {}
            for osd in topo.osds.values():
                if metric == "used_pct":
                    value: Any = osd.used_pct
                elif metric in ("commit_ms", "apply_ms"):
                    value = live.get(osd.id, {}).get(metric.replace("_ms", "_latency_ms"))
                else:
                    value = None
                if isinstance(value, int | float) and osd.up:
                    by_osd[osd.id] = [(now, float(value), float(value), float(value))]
            basis = "current values only (no history yet): drift cannot be judged"
            if not by_osd:
                raise UsageError(
                    f"no values for {metric}: it is recorded only where metric exporters "
                    "provide per-OSD counters (metrics_sources explains), and needs a few "
                    "samples of history"
                )
        latency = metric != "used_pct"
        rows = []
        groups = 0
        for device_class in sorted({o.device_class or "" for o in topo.osds.values()}):
            members = {
                f"osd.{i}": pts
                for i, pts in by_osd.items()
                if (topo.osds[i].device_class or "") == device_class
            }
            if len(members) < 3:
                continue
            groups += 1
            found, _ = outliers(
                members,
                min_ratio=2.0 if latency else 1.25,
                min_excess=5.0 if latency else 10.0,
                floor=1.0,
            )
            for o in found:
                osd = topo.osds[int(o.name[4:])]
                rows.append(
                    {
                        "osd": o.name,
                        "host": osd.host,
                        "class": device_class or None,
                        metric: num(o.value, 2),
                        "peers_median": num(o.peers, 2),
                        "times_peers": num(o.ratio, 1),
                        "earlier": num(o.earlier_ratio, 1),
                        "verdict": "drifting away from its peers" if o.drifting else "outlier",
                    }
                )
        out: dict[str, Any] = {
            "metric": metric,
            "basis": basis,
            "compared": f"{len(by_osd)} OSDs in {groups} device class group(s)",
        }
        if groups == 0:
            out["result"] = "fewer than 3 OSDs per device class: nothing to compare with"
        elif rows:
            out["outliers"] = table(rows)
            out["next"] = (
                "daemon_ops kind=historic_slow and daemon_perf on the OSD, and "
                "ceph_command ['device', 'ls-by-daemon', '<osd>'] for its disk"
            )
        else:
            out["result"] = "no OSD stands out from its peers"
        return compact(out)
