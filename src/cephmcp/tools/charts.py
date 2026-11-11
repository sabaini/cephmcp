"""Pictures: history as line charts, the OSDs as a grid, capacity with its
projection. Registered when Pillow is installed (the 'charts' extra)."""

from __future__ import annotations

import re
import statistics
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.utilities.types import Image
from pydantic import Field

from .. import charts, timeline
from ..analysis import buckets
from ..app import App, UsageError
from ..ceph import gather
from ..render import compact, fmt_bytes, num, pct
from ..topology import build
from .history import DAY, capacity_basis, describe_shift, shift_of, span, window

MAX_PANELS = 4
MAX_LINES = 40
DRAWN_POINTS = 240
GridMetric = Literal["used_pct", "commit_ms", "apply_ms", "pgs"]
TITLES = {
    "used_pct": "How full the OSDs are",
    "commit_ms": "OSD commit latency",
    "apply_ms": "OSD apply latency",
    "pgs": "Placement groups per OSD",
}


def entity_of(series: str) -> str:
    """'osd/3/commit_ms' -> 'osd.3'; 'pool/vms/stored_bytes' -> 'pool vms'."""
    kind, _, rest = series.partition("/")
    name = rest.rpartition("/")[0]
    if kind == "cluster" or not name:
        return "cluster"
    return f"osd.{name}" if kind == "osd" else f"{kind} {name}"


def panel_title(names: list[str]) -> str:
    """What a panel shows: the series itself, or the pattern its lines share."""
    if len(names) == 1:
        return names[0]
    kind = names[0].partition("/")[0]
    return f"{kind}/*/{names[0].rpartition('/')[2]}"


def register(server: MCPServer, app: App) -> None:
    ceph, history = app.ceph, app.history

    async def history_chart(
        patterns: list[str], since: str, until: str, theme: charts.Theme
    ) -> list[Any]:
        now = app.clock()
        start, end = window(since, until, now)
        store = await history.store()
        found: dict[str, Any] = {}
        for pattern in patterns:
            if not re.fullmatch(r"[A-Za-z0-9_.*?\[\]/:+-]{1,200}", pattern):
                raise UsageError(f"{pattern!r} is not a series name or pattern")
            found.update(await history.call(store.read, pattern, start, end))
        if not found:
            raise UsageError(
                f"nothing recorded matches {', '.join(patterns)} in that period; "
                "history_query without arguments lists what is recorded"
            )
        # One panel per metric: different units never share a scale.
        groups: dict[str, list[str]] = {}
        for name in sorted(found):
            groups.setdefault(name.rpartition("/")[2], []).append(name)
        dropped = max(0, len(groups) - MAX_PANELS)
        panels, table_rows = [], []
        for metric, names in list(groups.items())[:MAX_PANELS]:
            unit = charts.unit_of(metric)
            lines = []
            for name in names[:MAX_LINES]:
                drawn = [
                    (t + (end - start) / DRAWN_POINTS / 2, avg)
                    for t, avg, _, _ in buckets(found[name], start, end, DRAWN_POINTS)
                    if avg is not None
                ]
                lines.append(charts.Line(name, drawn, entity=entity_of(name)))
                values = [p[1] for p in found[name]]
                shift = shift_of(name, found[name])
                table_rows.append(
                    {
                        "series": name,
                        "min": charts.fmt(min(p[2] for p in found[name]), unit),
                        "avg": charts.fmt(sum(values) / len(values), unit),
                        "max": charts.fmt(max(p[3] for p in found[name]), unit),
                        "last": charts.fmt(values[-1], unit),
                        "shift": describe_shift(name, shift) if shift else None,
                    }
                )
            others = charts.emphasise(lines)
            note = f"{others} of {len(lines)} in grey" if others else ""
            panels.append(charts.Panel(panel_title(names), unit, lines, note=note))
        cover = f"{charts.clock(start)} to {charts.clock(end)} UTC"
        png = charts.line_chart(
            ", ".join(patterns)[:90], f"{cover} · recorded by cephmcp", panels, start, end, theme
        )
        summary = {
            "chart": "lines over time, one panel per unit",
            "period": {"from": timeline.fmt_time(start), "to": timeline.fmt_time(end)},
            "series": {
                "columns": list(table_rows[0]),
                "rows": [list(r.values()) for r in table_rows[:MAX_LINES]],
            },
            "not_drawn": f"{dropped} more metrics; narrow the pattern" if dropped else None,
        }
        return [Image(data=png, format="png"), compact(summary)]

    async def grid_chart(metric: str, theme: charts.Theme) -> list[Any]:
        df_tree, perf, osd_dump = await gather(
            lambda: ceph.json(["osd", "df", "tree"]),
            lambda: ceph.try_json(["osd", "perf"], {}),
            lambda: ceph.try_json(["osd", "dump"], {}),
        )
        topo = build(df_tree, osd_dump, [], None)
        latency = {
            p["id"]: p.get("perf_stats") or {}
            for p in ((perf or {}).get("osdstats") or {}).get("osd_perf_infos") or []
        }

        def value_of(osd: Any) -> float | None:
            if not osd.up:
                return None
            if metric == "used_pct":
                return osd.used_pct
            if metric == "pgs":
                return float(osd.pgs)
            got = latency.get(osd.id, {}).get(metric.replace("_ms", "_latency_ms"))
            return float(got) if isinstance(got, int | float) else None

        values = {o.id: value_of(o) for o in topo.osds.values()}
        known = [v for v in values.values() if v is not None]
        if not known:
            raise UsageError(f"no OSD reports {metric} right now")
        unit = charts.unit_of(metric)
        if metric == "used_pct":
            domain, limit = (0.0, 100.0), 100 * topo.nearfull_ratio
            flag_note = f"at or above the nearfull limit ({limit:.0f}%)"
        elif metric == "pgs":
            domain, limit, flag_note = (0.0, max(known)), float("inf"), ""
        else:
            middle = statistics.median(known)
            domain, limit = (0.0, max(max(known), 10.0)), max(50.0, 3 * middle)
            flag_note = "more than 3 times the median, and over 50 ms"
        rows = []
        for host, osds in sorted(topo.hosts().items()):
            cells = [
                charts.Cell(
                    f"osd.{o.id}" + ("" if o.up else " down"),
                    values[o.id],
                    values[o.id] is not None and values[o.id] >= limit,  # type: ignore[operator]
                )
                for o in sorted(osds, key=lambda o: o.id)
            ]
            rows.append((host, cells))
        flagged = [c.label for _, cells in rows for c in cells if c.flagged]
        worst = max(
            topo.osds.values(), key=lambda o: values[o.id] if values[o.id] is not None else -1
        )
        subtitle = (
            f"{len(topo.osds)} OSDs on {len(rows)} host{'s' if len(rows) != 1 else ''} · median "
            f"{charts.fmt(statistics.median(known), unit)} · highest osd.{worst.id} "
            f"{charts.fmt(values[worst.id] or 0, unit)}"
        )
        png = charts.osd_grid(
            TITLES[metric],
            subtitle,
            rows,
            unit,
            domain,
            theme,
            flag_note=flag_note if flagged else "",
        )
        summary = {
            "chart": "OSDs by host, shaded by " + metric,
            "by_host": {
                host: {c.label: num(c.value, 1) if c.value is not None else "down" for c in cells}
                for host, cells in rows
            },
            "median": num(statistics.median(known), 2),
            "flagged": flagged,
            "flag_means": flag_note if flagged else None,
        }
        return [Image(data=png, format="png"), compact(summary)]

    async def capacity_chart(days: float, theme: charts.Theme) -> list[Any]:
        basis = await capacity_basis(app, days)
        trend, now = basis.trend, basis.now
        if trend is None or trend.span < 600 or not basis.total:
            raise UsageError(
                "not enough history to draw a trend yet (ten minutes is the least); "
                "capacity_forecast says what there is"
            )
        start = basis.usage[0][0]
        levels = basis.levels()
        full = trend.reaches(levels[-1][2], now, current=basis.used)
        horizon = now + max(3 * trend.span, 7 * DAY)
        end = min(full + 0.08 * (full - start), horizon) if full else now + 0.15 * (now - start)
        measured = charts.Line("raw used", basis.usage, entity="raw used")
        lines = [measured]
        if trend.slope > 0:
            reach = min(end, full) if full else end
            projected = [
                (now, float(basis.used)),
                (reach, basis.used + trend.slope * (reach - now)),
            ]
            lines.append(charts.Line("projection", projected, entity="raw used", projected=True))
        marks = [
            charts.Mark(level, f"{label} ({100 * ratio:.0f}%)")
            for label, ratio, level in levels
            if level <= max(basis.used + trend.slope * (end - now), basis.used) * 1.6
        ]
        growth = f"{fmt_bytes(trend.slope * DAY)} a day" if trend.slope > 0 else "not growing"
        panel = charts.Panel(
            "raw space used; limits as the fullest OSD reaches them", "bytes", lines, marks
        )
        png = charts.line_chart(
            "Capacity",
            f"{fmt_bytes(basis.used)} of {fmt_bytes(basis.total)} used · {growth} · trend from "
            f"{span(trend.span)} of {basis.source}",
            [panel],
            start,
            end,
            theme,
            now=now,
        )
        summary = {
            "chart": "raw usage over time with its projection (dashed) and the limits",
            "used": fmt_bytes(basis.used),
            "total": fmt_bytes(basis.total),
            "used_pct": pct(basis.used, basis.total),
            "growth_per_day": fmt_bytes(trend.slope * DAY),
            "limits_reached": {
                f"{label} ({100 * ratio:.0f}%)": timeline.fmt_time(when)
                for label, ratio, level in levels
                if (when := trend.reaches(level, now, current=basis.used)) is not None
            },
            "basis": f"{span(trend.span)} of {basis.source}",
            "notes": basis.notes,
        }
        return [Image(data=png, format="png"), compact(summary)]

    @app.tool(server, title="Draw a chart")
    async def chart(
        kind: Annotated[
            Literal["history", "osds", "capacity"],
            Field(
                description="history: recorded series as lines over time. osds: every OSD "
                "as a cell, by host, shaded by `metric` (a heat map). capacity: raw usage "
                "with its projection and the nearfull and full limits."
            ),
        ] = "history",
        series: Annotated[
            str,
            Field(
                description="For 'history': series names or glob patterns as in history_query, "
                "comma-separated, e.g. 'osd/*/commit_ms' or 'cluster/op_w_latency_ms, "
                "cluster/write_bytes_sec'. Each metric gets its own panel."
            ),
        ] = "cluster/op_w_latency_ms, cluster/write_bytes_sec",
        since: Annotated[
            str, Field(description="For 'history': start, e.g. '-6h', '-7d'.")
        ] = "-6h",
        until: Annotated[str, Field(description="For 'history': end; default now.")] = "now",
        metric: Annotated[
            GridMetric, Field(description="For 'osds': what the shading shows.")
        ] = "used_pct",
        days: Annotated[
            float, Field(ge=0.05, le=365, description="For 'capacity': days of history to use.")
        ] = 30,
        theme: Annotated[
            Literal["light", "dark"], Field(description="Colours for a light or a dark background.")
        ] = "light",
    ) -> list[Any]:
        """Draw a picture and return it as a PNG image, with the figures it
        shows as text beside it:

        - kind='history': how recorded metrics developed (latency, throughput,
          degraded data, usage). With many series of one kind, e.g. a latency
          per OSD, the few that stand out are coloured and the rest are grey.
        - kind='osds': the cluster at a glance, one cell per OSD grouped by
          host: how full each is, or its latency; outliers are marked.
        - kind='capacity': usage so far, where the trend leads, and when it
          crosses the nearfull and full limits.

        Use it when the person asks to see something, or when a picture makes
        the point better than a table. The numbers come back as text too.
        """
        look = charts.THEMES[theme]
        if kind == "osds":
            return await grid_chart(metric, look)
        if kind == "capacity":
            return await capacity_chart(days, look)
        patterns = [p.strip() for p in series.split(",") if p.strip()]
        if not patterns:
            raise UsageError("series is empty; give a name or pattern such as 'osd/*/commit_ms'")
        return await history_chart(patterns, since, until, look)
