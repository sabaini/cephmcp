"""Performance metrics from the Prometheus exporters and a Prometheus server."""

from __future__ import annotations

import re
import time
from collections.abc import Iterable, Mapping
from typing import Annotated, Any, Literal

from anyio import sleep
from mcp.server.mcpserver import MCPServer
from pydantic import Field

from ..app import App, UsageError
from ..ceph import gather
from ..http import HttpError
from ..metrics import (
    EXPORTER,
    MGR,
    Collection,
    Labels,
    aggregate,
    compile_pattern,
    rates,
    select,
)
from ..render import compact, fmt_rate, limited, num, table
from .cluster import digest_status
from .storage import osd_rows

Row = tuple[str, Labels, float]
Aggregate = Literal["sum", "avg", "min", "max", "count"]
MAX_COMMON_LABELS = 8


def sample_table(
    rows: Iterable[tuple[str, Labels, Mapping[str, Any]]], limit: int
) -> dict[str, Any]:
    """Render (metric, labels, values) rows compactly.

    Labels become columns. Whatever is identical on every row (often the
    metric name, or an instance label) is stated once under "common".
    """
    rows = list(rows)
    keys: dict[str, None] = {}
    for _, labels, _ in rows:
        for key, _ in labels:
            keys.setdefault(key)
    records = []
    for metric, labels, values in rows:
        lookup = dict(labels)
        records.append({"metric": metric, **{k: lookup.get(k, "") for k in keys}, **values})
    common: dict[str, Any] = {}
    if len(records) > 1:
        for key in ["metric", *keys]:
            first = records[0][key]
            if all(r[key] == first for r in records):
                common[key] = first
        for record in records:
            for key in common:
                del record[key]
    out = limited(records, limit, what="series")
    if common:
        out["common"] = common
    return out


def _wrap(fn: Any, what: str) -> Any:
    try:
        return fn()
    except ValueError as exc:
        raise UsageError(f"{what}: {exc}") from None


def _reduce(
    rows: list[Row], how: Aggregate | None, by: list[str] | None, top: int | None
) -> list[Row]:
    if how:
        rows = aggregate(rows, how, by or [])
    elif by:
        raise UsageError("group_by needs aggregate (sum, avg, min, max or count)")
    if top:
        rows = sorted(rows, key=lambda r: r[2], reverse=True)[:top]
    return rows


def latency_rows(
    before: Collection, after: Collection, base: str, labels: Mapping[str, str] | None
) -> list[tuple[Labels, float, float]]:
    """(labels, d_sum/s, d_count/s) for a <base>_sum / <base>_count pair."""
    enrich = after.enricher()
    sums = {lb: v for _, lb, v in rates(before, after, re.escape(base) + "_sum", labels)}
    out = []
    for _, lb, count_rate in rates(before, after, re.escape(base) + "_count", labels):
        if lb in sums:
            out.append((enrich(lb), sums[lb], count_rate))
    return out


def latency_value(d_sum: float, d_count: float) -> dict[str, Any]:
    return {
        "ops_per_sec": num(d_count, 2),
        "avg_ms": num(1000 * d_sum / d_count, 3) if d_count > 0 else None,
    }


def latency_summary(
    rows: Iterable[tuple[Labels, float, float]], by: list[str] | None
) -> list[tuple[Labels, dict[str, Any]]]:
    """Average latency per group, weighted by operations: sum(d_sum) / sum(d_count).
    by=None keeps every series separate."""
    if by is None:
        return [(labels, latency_value(d_sum, d_count)) for labels, d_sum, d_count in rows]
    groups: dict[Labels, list[float]] = {}
    for labels, d_sum, d_count in rows:
        lookup = dict(labels)
        key = tuple((b, lookup.get(b, "")) for b in by)
        acc = groups.setdefault(key, [0.0, 0.0])
        acc[0] += d_sum
        acc[1] += d_count
    return [(key, latency_value(d_sum, d_count)) for key, (d_sum, d_count) in groups.items()]


_RELATIVE = re.compile(r"^-(\d+(?:\.\d+)?)([smhdw])$")
_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_time(value: str | None, now: float) -> float | str | None:
    """'-15m', 'now', epoch seconds or an RFC 3339 timestamp."""
    if value is None:
        return None
    value = value.strip()
    if value in ("", "now"):
        return now
    m = _RELATIVE.match(value)
    if m:
        return now - float(m.group(1)) * _UNIT[m.group(2)]
    try:
        return float(value)
    except ValueError:
        return value  # let Prometheus parse a timestamp


def register(server: MCPServer, app: App) -> None:
    hub = app.metrics
    ceph = app.ceph

    async def collect(*, fresh: bool = False) -> Collection:
        found = await hub.discover()
        if not found.sources:
            notes = " ".join(found.notes) or "No metric endpoint was found."
            raise UsageError(f"No metrics available. {notes}")
        collection = await hub.collect(fresh=fresh)
        if not collection.metrics:
            raise UsageError(
                "No metric endpoint answered: " + "; ".join(collection.errors or ["no data"])
            )
        return collection

    async def sample_twice(
        seconds: float, names: Iterable[str]
    ) -> tuple[Collection, Collection, float, str]:
        """Two collections `seconds` apart. The mgr module refreshes its
        output only every scrape interval, so metrics that come from it alone
        need a longer wait to show any change."""
        before = await collect()
        found = await hub.discover()
        note = ""
        mgr_only = any(before.kinds_of(n) == {MGR} for n in names)
        if mgr_only and seconds <= found.mgr_scrape_interval:
            seconds = found.mgr_scrape_interval + 1
            note = (
                f"waited {seconds:g}s instead: the mgr prometheus module refreshes "
                f"only every {found.mgr_scrape_interval:g}s"
            )
        await sleep(seconds)
        # Never a reused scrape here: with a short interval it would be the
        # very sample the interval started from, and every rate would be zero.
        after = await collect(fresh=True)
        return before, after, after.time - before.time, note

    @app.tool(server, title="Metric sources")
    async def metrics_sources(
        refresh: Annotated[
            bool, Field(description="Look for endpoints again instead of using the cached result.")
        ] = False,
    ) -> dict[str, Any]:
        """Where performance metrics can be read from, and whether each endpoint
        answers: the mgr prometheus module (cluster, pool, PG and OSD state),
        ceph-exporter daemons (per-daemon performance counters) and a Prometheus
        server (history, PromQL). Explains what is missing and how to enable it.
        Call this first if a metrics tool reports no data."""
        found = await hub.discover(refresh=refresh)
        scrapes = await gather(*[lambda s=s: hub.scrape(s) for s in found.sources])
        sources = [
            {
                "kind": s.source.kind,
                "host": s.source.host or None,
                "url": s.source.url,
                "found_by": s.source.found_by,
                "ok": not s.error,
                "metrics": len(s.metrics) or None,
                "series": s.series or None,
                "error": s.error or None,
            }
            for s in scrapes
        ]
        return compact(
            {
                "sources": limited(sources, 100, what="sources") if sources else None,
                "prometheus_server": found.prometheus_url,
                "mgr_scrape_interval_s": found.mgr_scrape_interval
                if MGR in found.kinds()
                else None,
                "notes": found.notes
                + (
                    []
                    if found.prometheus_url
                    else ["No Prometheus server known, so prometheus_query is unavailable."]
                ),
            }
        )

    @app.tool(server, title="Search metric names")
    async def metrics_search(
        pattern: Annotated[
            str,
            Field(
                description="Case-insensitive regular expression searched in metric names "
                "and help texts, e.g. 'latency', 'ceph_pool_', 'bluestore.*commit', 'rgw'."
            ),
        ] = "",
        limit: Annotated[int, Field(ge=1, le=500)] = 60,
    ) -> dict[str, Any]:
        """Find out which metrics exist. With a pattern: matching metric names
        with type, number of series and description. Without: an overview of
        metric families and how many metrics each has.

        Latencies come as a pair `<name>_sum` (total seconds) and `<name>_count`
        (operations); pass the base `<name>` to metrics_rate to get averages.
        """
        collection = await collect()
        if not pattern:
            families: dict[str, int] = {}
            for name in collection.metrics:
                parts = name.split("_")
                family = "_".join(parts[:2]) if len(parts) > 2 else name
                families[family] = families.get(family, 0) + 1
            return {
                "metrics": len(collection.metrics),
                "families": dict(sorted(families.items(), key=lambda kv: -kv[1])),
                "hint": "pass a pattern, e.g. 'ceph_osd_op' or 'latency', to list metrics",
            }
        try:
            rx = re.compile(pattern, re.IGNORECASE)
        except re.error as exc:
            raise UsageError(f"invalid regular expression: {exc}") from None
        rows = [
            {
                "name": m.name,
                "type": m.type,
                "series": len(m.samples),
                "labels": ",".join(sorted({k for s in m.samples[:50] for k, _ in s.labels}))
                or None,
                "help": m.help[:140] or None,
            }
            for m in sorted(collection.metrics.values(), key=lambda m: m.name)
            if rx.search(m.name) or rx.search(m.help)
        ]
        return limited(rows, limit, what="metrics")

    @app.tool(server, title="Query current metric values")
    async def metrics_query(
        metric: Annotated[
            str,
            Field(
                description="Metric name, or a regular expression that must match the "
                "whole name, e.g. 'ceph_osd_commit_latency_ms' or 'ceph_pg_(degraded|undersized)'."
            ),
        ],
        labels: Annotated[
            dict[str, str] | None,
            Field(
                description="Keep only series whose labels match; values are regular "
                "expressions matching the whole label, e.g. {'ceph_daemon': 'osd\\\\.(1|2)'}, "
                "{'pool': 'rbd'}, {'hostname': 'node3'}."
            ),
        ] = None,
        aggregate: Annotated[
            Aggregate | None, Field(description="Combine series, per group_by group.")
        ] = None,
        group_by: Annotated[
            list[str] | None,
            Field(description="Labels to keep when aggregating, e.g. ['hostname'] or ['pool']."),
        ] = None,
        top: Annotated[
            int | None, Field(ge=1, le=1000, description="Only the N highest values.")
        ] = None,
        limit: Annotated[int, Field(ge=1, le=2000)] = 100,
    ) -> dict[str, Any]:
        """Current values of metrics from the exporters. Series labelled with a
        pool_id also get a `pool` label (the pool's name), and series labelled
        with ceph_daemon=osd.N get `hostname` and `device_class`, so you can
        filter and group by those.

        Counters only ever increase; to see activity use metrics_rate.
        """
        collection = await collect()
        enrich = collection.enricher()
        selected = _wrap(lambda: select(collection.metrics, metric), "metric")
        rows: list[Row] = [(name, enrich(s.labels), s.value) for name, s in selected]
        if labels:
            patterns = _wrap(
                lambda: {k: compile_pattern(v, f"label {k}") for k, v in labels.items()},
                "invalid label pattern",
            )
            rows = [
                r
                for r in rows
                if all(rx.fullmatch(dict(r[1]).get(k, "")) for k, rx in patterns.items())
            ]
        if not selected:
            raise UsageError(f"no metric matches {metric!r}; use metrics_search to find names")
        rows = _reduce(rows, aggregate, group_by, top)
        return sample_table(((n, lb, {"value": num(v, 4)}) for n, lb, v in rows), limit)

    @app.tool(server, title="Metric rates over an interval")
    async def metrics_rate(
        metric: Annotated[
            str,
            Field(
                description="A counter (name or whole-name regular expression), e.g. "
                "'ceph_osd_op_w' or 'ceph_pool_(rd|wr)_bytes'; or the base name of a "
                "latency pair, e.g. 'ceph_osd_op_w_latency', to get the average latency."
            ),
        ],
        seconds: Annotated[
            float, Field(ge=1, le=300, description="Length of the sampling interval.")
        ] = 10,
        labels: Annotated[
            dict[str, str] | None,
            Field(description="Label filters as in metrics_query."),
        ] = None,
        aggregate: Annotated[
            Aggregate | None,
            Field(description="Combine series per group_by group. Not needed for latencies."),
        ] = None,
        group_by: Annotated[
            list[str] | None,
            Field(
                description="Labels to group by, e.g. ['hostname'], ['pool'], ['device_class']. "
                "For a latency this gives the operation-weighted average per group; "
                "[] gives one cluster-wide figure."
            ),
        ] = None,
        top: Annotated[
            int | None, Field(ge=1, le=1000, description="Only the N highest values.")
        ] = None,
        limit: Annotated[int, Field(ge=1, le=2000)] = 100,
    ) -> dict[str, Any]:
        """Measure what is happening now: reads the exporters twice, `seconds`
        apart, and reports the per-second increase of counters (ops/s, bytes/s).

        For a latency pair it reports ops_per_sec and avg_ms, the average
        latency of the operations completed during the interval, which is the
        figure to look at for a current slowdown (the plain _sum/_count ratio
        is an average since daemon start). This call takes `seconds` to return.
        """
        first = await collect()
        pair = metric not in first.metrics and f"{metric}_sum" in first.metrics
        names = (
            [f"{metric}_sum", f"{metric}_count"]
            if pair
            else [n for n, _ in _wrap(lambda: select(first.metrics, metric), "metric")]
        )
        if not names:
            raise UsageError(f"no metric matches {metric!r}; use metrics_search to find names")
        before, after, elapsed, note = await sample_twice(seconds, set(names))
        enrich = after.enricher()
        patterns = _wrap(
            lambda: {k: compile_pattern(v, f"label {k}") for k, v in (labels or {}).items()},
            "invalid label pattern",
        )

        def keep(lb: Labels) -> bool:
            lookup = dict(lb)
            return all(rx.fullmatch(lookup.get(k, "")) for k, rx in patterns.items())

        if pair:
            lat = [r for r in latency_rows(before, after, metric, None) if keep(r[0])]
            summary = latency_summary(lat, group_by)
            summary.sort(key=lambda r: r[1]["avg_ms"] or 0, reverse=True)
            out = sample_table(((metric, lb, v) for lb, v in summary[: top or None]), limit)
        else:
            rows = [
                (n, enrich(lb), v)
                for n, lb, v in _wrap(lambda: rates(before, after, metric), "metric")
            ]
            rows = _reduce([r for r in rows if keep(r[1])], aggregate, group_by, top)
            out = sample_table(((n, lb, {"per_sec": num(v, 3)}) for n, lb, v in rows), limit)
        out["interval_s"] = num(elapsed, 1)
        if note:
            out["note"] = note
        return out

    @app.tool(server, title="Performance overview")
    async def perf_overview(
        seconds: Annotated[
            float,
            Field(
                ge=0,
                le=120,
                description="Sampling interval for per-OSD operation latencies from the "
                "exporters. 0 skips that part and returns immediately.",
            ),
        ] = 10,
        top: Annotated[int, Field(ge=1, le=50, description="How many slowest OSDs to list.")] = 5,
    ) -> dict[str, Any]:
        """One-call performance picture: cluster client and recovery throughput,
        per-pool I/O, the OSDs with the highest commit latency, slow-ops
        warnings, and, where the exporters provide per-daemon counters, read and
        write operation rates and latencies measured over the sampling interval,
        cluster-wide, per host and for the slowest OSDs.

        Use it to answer "is the cluster slow, and where". Drill down with
        metrics_rate, daemon_perf and daemon_ops.
        """
        status, pool_stats, df_tree, perf = await gather(
            lambda: ceph.json(["status"]),
            lambda: ceph.try_json(["osd", "pool", "stats"], []),
            lambda: ceph.try_json(["osd", "df", "tree"], {}),
            lambda: ceph.try_json(["osd", "perf"]),
        )
        digest = digest_status(status)
        pools = []
        for p in pool_stats or []:
            rate = p.get("client_io_rate") or {}
            rec = p.get("recovery_rate") or {}
            if not rate and not rec:
                continue
            pools.append(
                {
                    "pool": p.get("pool_name"),
                    "read_ops_s": rate.get("read_op_per_sec"),
                    "write_ops_s": rate.get("write_op_per_sec"),
                    "read": fmt_rate(rate["read_bytes_sec"])
                    if rate.get("read_bytes_sec")
                    else None,
                    "write": fmt_rate(rate["write_bytes_sec"])
                    if rate.get("write_bytes_sec")
                    else None,
                    "recovery": fmt_rate(rec["recovering_bytes_per_sec"])
                    if rec.get("recovering_bytes_per_sec")
                    else None,
                }
            )
        osds = [r for r in osd_rows(df_tree or {}, perf) if r["commit_ms"] is not None]
        commit = [r["commit_ms"] for r in osds]
        slowest = sorted(osds, key=lambda r: r["commit_ms"], reverse=True)[:top]
        slow_ops = [
            c["summary"]
            for c in digest.get("health", {}).get("checks", [])
            if c["code"]
            in (
                "SLOW_OPS",
                "BLUESTORE_SLOW_OP_ALERT",
                "OSD_SLOW_PING_TIME_BACK",
                "OSD_SLOW_PING_TIME_FRONT",
            )
        ]
        out: dict[str, Any] = {
            "health": digest.get("health", {}).get("status"),
            "client_io": digest.get("client_io") or "idle",
            "recovery_io": digest.get("recovery_io"),
            "pools_with_io": limited(pools, 100, what="pools") if pools else None,
            "osd_commit_latency_ms": {
                "avg": num(sum(commit) / len(commit), 2),
                "max": max(commit),
                "slowest": [
                    {"osd": r["id"], "host": r["host"], "commit_ms": r["commit_ms"]}
                    for r in slowest
                    if r["commit_ms"] > 0
                ],
            }
            if commit
            else None,
            "slow_ops": slow_ops,
        }
        notes: list[str] = []
        found = await hub.discover()
        notes.extend(found.notes)
        if seconds > 0 and found.sources:
            try:
                first = await collect()
            except UsageError as exc:
                first = None
                notes.append(str(exc))
            if first is not None and "ceph_osd_op_w_latency_sum" in first.metrics:
                wanted = {"ceph_osd_op_w_latency_sum", "ceph_osd_op_r_latency_sum"}
                before, after, elapsed, note = await sample_twice(seconds, wanted)
                if note:
                    notes.append(note)
                ops: dict[str, Any] = {
                    "interval_s": num(elapsed, 1),
                    "source": "ceph-exporter"
                    if EXPORTER in after.kinds_of("ceph_osd_op_w_latency_sum")
                    else "mgr prometheus module",
                }
                for label, base in (
                    ("write", "ceph_osd_op_w_latency"),
                    ("read", "ceph_osd_op_r_latency"),
                ):
                    lat = latency_rows(before, after, base, None)
                    overall = latency_summary(lat, [])
                    ops[label] = overall[0][1] if overall else None
                    per_osd = [
                        {"osd": dict(lb).get("ceph_daemon"), "host": dict(lb).get("hostname"), **v}
                        for lb, v in latency_summary(lat, ["ceph_daemon", "hostname"])
                        if v["avg_ms"] is not None
                    ]
                    per_osd.sort(key=lambda r: r["avg_ms"], reverse=True)
                    ops[f"slowest_osds_{label}"] = table(per_osd[:top]) if per_osd else None
                    hosts = [
                        {"host": dict(lb).get("hostname"), **v}
                        for lb, v in latency_summary(lat, ["hostname"])
                        if dict(lb).get("hostname") and v["avg_ms"] is not None
                    ]
                    if len(hosts) > 1:
                        hosts.sort(key=lambda r: r["avg_ms"], reverse=True)
                        ops[f"by_host_{label}"] = table(hosts[:50])
                out["osd_ops"] = ops
        out["notes"] = notes
        return compact(out)

    @app.tool(server, title="PromQL query")
    async def prometheus_query(
        query: Annotated[
            str,
            Field(
                description="PromQL expression, e.g. 'sum(rate(ceph_osd_op_w[5m]))' or "
                "'ceph_osd_commit_latency_ms > 50'."
            ),
        ],
        start: Annotated[
            str | None,
            Field(
                description="Start of a range query: relative ('-1h', '-30m', '-7d'), epoch "
                "seconds or an RFC 3339 time. Leave unset for the value right now."
            ),
        ] = None,
        end: Annotated[str | None, Field(description="End of the range; default now.")] = None,
        step: Annotated[
            float | None,
            Field(ge=1, description="Seconds between points; default gives about 60 points."),
        ] = None,
        limit: Annotated[int, Field(ge=1, le=500, description="Maximum number of series.")] = 50,
    ) -> dict[str, Any]:
        """Run a PromQL query against the cluster's Prometheus server. Unlike the
        other metrics tools this sees history: set start for a trend over time
        (returned as one row of values per series, aligned to "times").
        Available when a Prometheus server is configured or deployed by cephadm;
        metrics_sources tells."""
        found = await hub.discover()
        base = found.prometheus_url
        if not base:
            raise UsageError(
                "No Prometheus server is known. The operator can set --prometheus-url; "
                "meanwhile use metrics_query and metrics_rate, which read the exporters."
            )
        s = app.settings
        auth = (
            (s.prometheus_username, s.prometheus_password or "") if s.prometheus_username else None
        )
        now = hub.now()
        if start is None:
            path = "/api/v1/query"
            params: dict[str, Any] = {"query": query}
        else:
            t0, t1 = parse_time(start, now), parse_time(end or "now", now)
            if step is None:
                span = (t1 - t0) if isinstance(t0, float) and isinstance(t1, float) else 3600.0
                if span <= 0:
                    raise UsageError("start must be before end")
                step = max(1.0, round(span / 60))
            path = "/api/v1/query_range"
            params = {"query": query, "start": t0, "end": t1, "step": step}
        response = await app.http.request("POST", base + path, data=params, auth=auth)
        try:
            body = response.json()
        except ValueError:
            raise HttpError(f"Prometheus at {base} returned HTTP {response.status_code}") from None
        if body.get("status") != "success":
            raise UsageError(
                f"Prometheus rejected the query: {body.get('error', response.status_code)}"
            )
        data = body.get("data") or {}
        result = data.get("result")
        kind = data.get("resultType")
        notes = (body.get("warnings") or []) + (body.get("infos") or [])
        out: dict[str, Any]
        if kind in ("scalar", "string"):
            out = {"value": result[1] if result else None}
        elif kind == "vector":
            rows = [
                (
                    r["metric"].get("__name__", ""),
                    tuple((k, v) for k, v in r["metric"].items() if k != "__name__"),
                    {"value": num(float(r["value"][1]), 4)},
                )
                for r in result or []
            ]
            out = sample_table(rows, limit)
        else:
            times = sorted({point[0] for r in result or [] for point in r.get("values", [])})
            rows = []
            for r in result or []:
                points = {t: num(float(v), 4) for t, v in r.get("values", [])}
                rows.append(
                    (
                        r["metric"].get("__name__", ""),
                        tuple((k, v) for k, v in r["metric"].items() if k != "__name__"),
                        {"values": [points.get(t) for t in times]},
                    )
                )
            out = sample_table(rows, limit)
            out["times"] = [time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t)) for t in times]
            out["step_s"] = step
        if notes:
            out["notes"] = notes
        return out
