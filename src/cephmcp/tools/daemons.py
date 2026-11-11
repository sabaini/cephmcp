"""Per-daemon inspection: configuration, perf counters and the op tracker."""

from __future__ import annotations

import re
import time
from typing import Annotated, Any, Literal

from anyio import sleep
from mcp.server.mcpserver import MCPServer
from pydantic import Field

from ..app import App, UsageError
from ..ceph import CephError, gather
from ..render import compact, limited, num
from .common import config_option, config_value, config_who, daemon_name

DEFAULT_PRIORITY = 8  # PRIO_INTERESTING: what Ceph itself considers worth showing


def _pair(value: Any) -> tuple[float, float] | None:
    """(sum, count) of an averaged counter such as a latency."""
    if isinstance(value, dict) and "avgcount" in value:
        return float(value.get("sum") or 0), float(value.get("avgcount") or 0)
    return None


def perf_rows(
    dump: dict[str, Any],
    schema: dict[str, Any],
    *,
    match: re.Pattern[str] | None,
    include_zero: bool,
    earlier: dict[str, Any] | None = None,
    elapsed: float = 0.0,
) -> list[dict[str, Any]]:
    """Flatten a perf dump into rows. With an earlier dump, add rates over the interval."""
    rows = []
    for group, counters in dump.items():
        for name, value in counters.items():
            full = f"{group}.{name}"
            meta = (schema.get(group) or {}).get(name) or {}
            if match is not None:
                if not match.search(full):
                    continue
            elif meta.get("priority", 0) < DEFAULT_PRIORITY:
                continue
            pair = _pair(value)
            before = (earlier or {}).get(group, {}).get(name) if earlier else None
            row: dict[str, Any] = {"counter": full}
            if pair is not None:
                total, count = pair
                if not include_zero and count == 0:
                    continue
                seconds = meta.get("value_type", "real-integer-pair").startswith("real")
                scale, key = (1000.0, "avg_ms") if seconds else (1.0, "avg")
                row["count"] = int(count)
                row[key] = num(scale * total / count, 3) if count else None
                prev = _pair(before)
                if prev is not None and elapsed > 0:
                    d_total, d_count = total - prev[0], count - prev[1]
                    if d_count >= 0:
                        row["per_sec"] = num(d_count / elapsed, 2)
                        row[f"interval_{key}"] = (
                            num(scale * d_total / d_count, 3) if d_count else None
                        )
            elif isinstance(value, int | float):
                if not include_zero and value == 0:
                    continue
                row["value"] = num(value, 3)
                if (
                    isinstance(before, int | float)
                    and elapsed > 0
                    and meta.get("metric_type") == "counter"
                    and value >= before
                ):
                    row["per_sec"] = num((value - before) / elapsed, 2)
            else:
                continue
            rows.append(row)
    return rows


def _duration(seconds: float) -> str:
    """0.00151 -> '1.51ms', 1.24 -> '1.24s'."""
    return f"{seconds:.3g}s" if seconds >= 1 else f"{1000 * seconds:.3g}ms"


def op_rows(ops: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for op in ops:
        data = op.get("type_data") or {}
        events = [e for e in data.get("events") or [] if isinstance(e, dict)]
        slowest = sorted(
            (e for e in events if isinstance(e.get("duration"), int | float)),
            key=lambda e: e["duration"],
            reverse=True,
        )[:2]
        rows.append(
            {
                "age_s": num(op.get("age"), 3),
                "duration_s": num(op.get("duration"), 4),
                "state": data.get("flag_point"),
                "client": (data.get("client_info") or {}).get("client"),
                "slowest_steps": ", ".join(
                    f"{e.get('event')} {_duration(e['duration'])}"
                    for e in slowest
                    if e["duration"] > 0
                )
                or None,
                "description": (op.get("description") or "")[:240],
            }
        )
    rows.sort(key=lambda r: r["duration_s"] or 0, reverse=True)
    return rows


def register(server: MCPServer, app: App) -> None:
    ceph = app.ceph
    schemas: dict[str, dict[str, Any]] = {}

    async def tell(target: str, *command: str) -> Any:
        """`ceph tell <daemon> <command>`, explaining the usual permission failure."""
        try:
            return await ceph.json(["tell", target, *command])
        except CephError as exc:
            if "EACCES" in str(exc):
                raise CephError(
                    f"{exc}\nThe server's Ceph identity may not query daemons directly "
                    "(that takes full caps on the daemon type, e.g. osd 'allow *'). "
                    "Use metrics_rate and metrics_query, which read the exporters."
                ) from None
            raise

    @app.tool(server, title="Read configuration")
    async def config_get(
        who: Annotated[
            str | None,
            Field(
                description="Whose configuration: 'global', a daemon type ('osd', 'mon', "
                "'mgr', 'mds', 'client') or one daemon ('osd.3')."
            ),
        ] = None,
        option: Annotated[
            str | None, Field(description="Option name, e.g. 'osd_max_backfills'.")
        ] = None,
        running: Annotated[
            bool,
            Field(
                description="With who set to one daemon: report what that daemon is "
                "actually running with (non-default values and where each comes from)."
            ),
        ] = False,
    ) -> dict[str, Any]:
        """Read Ceph configuration.

        - no arguments: every setting stored in the cluster configuration database
        - option only: its documentation, type, default and whether it applies at runtime
        - who and option: the value in effect for that daemon or daemon type
        - who only: all settings that apply to it; with running=true, the values a
          specific daemon runs with right now
        """
        if who:
            config_who(who)
        if option:
            config_option(option)
        if option and not who:
            info = await ceph.json(["config", "help", option])
            keep = (
                "name",
                "type",
                "level",
                "desc",
                "long_desc",
                "default",
                "min",
                "max",
                "enum_values",
                "services",
                "see_also",
                "can_update_at_runtime",
            )
            return compact({k: info.get(k) for k in keep if info.get(k) != ""})
        if who and option:
            value, info = await gather(
                lambda: config_value(ceph, who, option),
                lambda: ceph.try_json(["config", "help", option], {}),
            )
            return compact(
                {
                    "who": who,
                    "option": option,
                    "value": value,
                    "default": (info or {}).get("default"),
                    "desc": (info or {}).get("desc"),
                    "can_update_at_runtime": (info or {}).get("can_update_at_runtime"),
                }
            )
        if who and running:
            if "." not in who:
                raise UsageError("running=true needs one daemon, e.g. who='osd.3'")
            shown = await ceph.json(["config", "show", who])
            rows = [
                {"option": o.get("name"), "value": o.get("value"), "source": o.get("source")}
                for o in shown or []
            ]
            return limited(rows, 500, what="options")
        if who:
            settings = await ceph.json(["config", "get", who]) or {}
            rows = [
                {"option": name, "value": o.get("value"), "set_on": o.get("section")}
                for name, o in settings.items()
            ]
            return limited(rows, 500, what="options")
        dump = await ceph.json(["config", "dump"]) or []
        rows = [
            {
                "who": o.get("section"),
                "mask": o.get("mask") or None,
                "option": o.get("name"),
                "value": o.get("value"),
            }
            for o in dump
        ]
        return limited(rows, 1000, what="options")

    @app.tool(server, title="Daemon perf counters")
    async def daemon_perf(
        daemon: Annotated[
            str, Field(description="Daemon to query, e.g. 'osd.3', 'mon.a', 'mds.x' or 'mgr.y'.")
        ],
        match: Annotated[
            str,
            Field(
                description="Regular expression searched in 'group.counter' names, e.g. "
                "'^osd\\.op_.*latency', 'bluestore\\..*_lat$', 'rocksdb'. Default: the "
                "counters Ceph rates as most useful."
            ),
        ] = "",
        seconds: Annotated[
            float,
            Field(
                ge=0,
                le=120,
                description="If above 0, sample twice this many seconds apart and add "
                "per-second rates and the average over just that interval.",
            ),
        ] = 0,
        include_zero: Annotated[bool, Field(description="Also list counters that are 0.")] = False,
        limit: Annotated[int, Field(ge=1, le=1000)] = 150,
    ) -> dict[str, Any]:
        """Internal performance counters of one daemon, read live from the daemon
        (works without any metrics exporter).

        Latencies and other averaged counters show the number of samples and
        the average since the daemon started (avg_ms); with seconds > 0 also
        the average during the sampling interval (interval_avg_ms) and the
        rate (per_sec), which is what matters when diagnosing a current
        problem. Plain counters show their value and, when sampled, per_sec.
        """
        daemon_name(daemon)
        pattern = None
        if match:
            try:
                pattern = re.compile(match)
            except re.error as exc:
                raise UsageError(f"invalid regular expression: {exc}") from None
        kind = daemon.split(".", 1)[0]
        target = "mgr" if kind == "mgr" else daemon

        async def schema() -> dict[str, Any]:
            if kind not in schemas:
                schemas[kind] = await ceph.try_json(["tell", target, "perf", "schema"], {}) or {}
            return schemas[kind]

        first, meta = await gather(lambda: tell(target, "perf", "dump"), schema)
        earlier, elapsed = None, 0.0
        if seconds > 0:
            started = time.monotonic()
            await sleep(seconds)
            earlier, first = first, await tell(target, "perf", "dump")
            elapsed = time.monotonic() - started
        rows = perf_rows(
            first or {},
            meta,
            match=pattern,
            include_zero=include_zero,
            earlier=earlier,
            elapsed=elapsed,
        )
        # Plain values and averaged counters have different columns.
        plain = [r for r in rows if "value" in r]
        averaged = [r for r in rows if "value" not in r]
        out: dict[str, Any] = {"daemon": daemon}
        if elapsed:
            out["interval_s"] = num(elapsed, 1)
        if plain:
            out["counters"] = limited(plain, limit, what="counters")
        if averaged:
            out["averages"] = limited(averaged, limit, what="counters")
        if not rows:
            out["note"] = "no counter matches" if match else "all selected counters are 0"
        return out

    @app.tool(server, title="Daemon operations")
    async def daemon_ops(
        daemon: Annotated[str, Field(description="Daemon to query, usually an OSD: 'osd.3'.")],
        kind: Annotated[
            Literal["in_flight", "blocked", "historic", "historic_slow"],
            Field(
                description="in_flight: operations being processed now. blocked: those "
                "stuck beyond the complaint time. historic: recently completed. "
                "historic_slow: recent ones that exceeded the slow-op threshold."
            ),
        ] = "in_flight",
        limit: Annotated[int, Field(ge=1, le=200)] = 20,
    ) -> dict[str, Any]:
        """Operations tracked by a daemon, slowest first: how long each has taken,
        which processing step it is in, the steps that took longest, and what
        the operation is. The tool for explaining SLOW_OPS health warnings."""
        daemon_name(daemon)
        command = {
            "in_flight": "dump_ops_in_flight",
            "blocked": "dump_blocked_ops",
            "historic": "dump_historic_ops",
            "historic_slow": "dump_historic_slow_ops",
        }[kind]
        target = "mgr" if daemon.startswith("mgr.") else daemon
        data = await tell(target, command) or {}
        ops = data.get("ops") or data.get("Ops") or []
        out = limited(op_rows(ops), limit, what="operations")
        out["daemon"] = daemon
        if "complaint_time" in data:
            out["complaint_time_s"] = data["complaint_time"]
        return out
