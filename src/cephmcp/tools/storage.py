"""OSDs, pools and placement groups."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from ..app import App, UsageError
from ..ceph import gather
from ..render import compact, fmt_bytes, fmt_rate, limited, num
from .common import osd_id, pool_name, short_time, word

KIB = 1024
POOL_TYPES = {1: "replicated", 3: "erasure"}
CRUSH_ITEM_NONE = 0x7FFFFFFF


def osd_rows(df_tree: dict[str, Any], perf: dict[str, Any] | None) -> list[dict[str, Any]]:
    """One row per OSD from `osd df tree`, with its host and latencies."""
    nodes = df_tree.get("nodes") or []
    parent: dict[int, dict[str, Any]] = {}
    for node in nodes:
        for child in node.get("children") or []:
            parent[child] = node

    def host_of(node_id: int) -> str | None:
        seen = 0
        while node_id in parent and seen < 32:
            bucket = parent[node_id]
            if bucket.get("type") == "host":
                return bucket.get("name")
            node_id, seen = bucket["id"], seen + 1
        return None

    latency = {
        p["id"]: p.get("perf_stats") or {}
        for p in ((perf or {}).get("osdstats") or {}).get("osd_perf_infos") or []
    }
    rows = []
    for node in [*nodes, *(df_tree.get("stray") or [])]:
        if node.get("type") != "osd":
            continue
        lat = latency.get(node["id"], {})
        rows.append(
            {
                "id": node["id"],
                "host": host_of(node["id"]),
                "class": node.get("device_class"),
                "up": node.get("status") == "up",
                "in": (node.get("reweight") or 0) > 0,
                "crush_weight": num(node.get("crush_weight"), 4),
                "reweight": num(node.get("reweight"), 3),
                "size": fmt_bytes((node.get("kb") or 0) * KIB),
                "used_pct": num(node.get("utilization"), 1),
                "var": num(node.get("var"), 2),
                "pgs": node.get("pgs"),
                "commit_ms": lat.get("commit_latency_ms"),
                "apply_ms": lat.get("apply_latency_ms"),
            }
        )
    return rows


def pool_rows(
    detail: list[dict[str, Any]],
    df: dict[str, Any],
    stats: list[dict[str, Any]] | None,
    rules: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    usage = {p["id"]: p.get("stats") or {} for p in df.get("pools") or []}
    io = {p["pool_id"]: p for p in stats or []}
    rule_names = {r.get("rule_id"): r.get("rule_name") for r in rules or []}
    rows = []
    for pool in detail:
        pid = pool.get("pool_id", pool.get("pool"))
        use = usage.get(pid, {})
        rate = (io.get(pid) or {}).get("client_io_rate") or {}
        recovery = (io.get(pid) or {}).get("recovery_rate") or {}
        kind = POOL_TYPES.get(pool.get("type"), str(pool.get("type")))
        if kind == "erasure":
            redundancy = f"ec:{pool.get('erasure_code_profile')} size {pool.get('size')}"
        else:
            redundancy = f"{pool.get('size')}x"
        quota = []
        if pool.get("quota_max_bytes"):
            quota.append(fmt_bytes(pool["quota_max_bytes"]))
        if pool.get("quota_max_objects"):
            quota.append(f"{pool['quota_max_objects']} objects")
        rows.append(
            {
                "name": pool.get("pool_name"),
                "id": pid,
                "redundancy": redundancy,
                "min_size": pool.get("min_size"),
                "pg_num": pool.get("pg_num"),
                "autoscale": pool.get("pg_autoscale_mode"),
                "crush_rule": rule_names.get(pool.get("crush_rule"), pool.get("crush_rule")),
                "application": ",".join((pool.get("application_metadata") or {}).keys()) or None,
                "objects": use.get("objects"),
                "stored": fmt_bytes(use.get("stored")),
                "used": fmt_bytes(use.get("bytes_used")),
                "used_pct": num(100 * use["percent_used"], 2) if "percent_used" in use else None,
                "max_avail": fmt_bytes(use.get("max_avail")),
                "quota": " / ".join(quota) or None,
                "read_ops_s": rate.get("read_op_per_sec"),
                "write_ops_s": rate.get("write_op_per_sec"),
                "read": fmt_rate(rate["read_bytes_sec"]) if rate.get("read_bytes_sec") else None,
                "write": fmt_rate(rate["write_bytes_sec"]) if rate.get("write_bytes_sec") else None,
                "recovery": fmt_rate(recovery["recovering_bytes_per_sec"])
                if recovery.get("recovering_bytes_per_sec")
                else None,
                "flags": pool.get("flags_names"),
            }
        )
    return rows


def _osds(ids: list[int] | None) -> list[int | None] | None:
    """An OSD set, with CRUSH's "no OSD" placeholder shown as null."""
    return None if ids is None else [None if i == CRUSH_ITEM_NONE else i for i in ids]


def pg_rows(stats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for pg in stats:
        s = pg.get("stat_sum") or {}
        rows.append(
            {
                "pgid": pg.get("pgid"),
                "state": pg.get("state"),
                "up": _osds(pg.get("up")),
                "acting": _osds(pg.get("acting")),
                "primary": pg.get("acting_primary"),
                "objects": s.get("num_objects"),
                "size": fmt_bytes(s.get("num_bytes")),
                "degraded": s.get("num_objects_degraded") or None,
                "misplaced": s.get("num_objects_misplaced") or None,
                "unfound": s.get("num_objects_unfound") or None,
                "last_active": short_time(pg.get("last_active")),
                "last_scrub": short_time(pg.get("last_scrub_stamp")),
                "last_deep_scrub": short_time(pg.get("last_deep_scrub_stamp")),
            }
        )
    return rows


def register(server: MCPServer, app: App) -> None:
    ceph = app.ceph

    @app.tool(server, title="List OSDs")
    async def osd_list(
        host: Annotated[str | None, Field(description="Only OSDs on this host.")] = None,
        device_class: Annotated[
            str | None, Field(description="Only this device class, e.g. 'hdd', 'ssd', 'nvme'.")
        ] = None,
        state: Annotated[
            Literal["all", "up", "down", "in", "out"],
            Field(description="Only OSDs in this state."),
        ] = "all",
        sort_by: Annotated[
            Literal["id", "used_pct", "pgs", "commit_ms"],
            Field(description="Sort order; anything but 'id' sorts highest first."),
        ] = "id",
        limit: Annotated[int, Field(ge=1, le=2000)] = 200,
    ) -> dict[str, Any]:
        """One row per OSD: host, device class, up/in state, CRUSH weight and
        reweight, size, utilisation (used_pct, and var = utilisation relative to
        the cluster average), PG count, and commit/apply latency in ms.

        Use it to find down or out OSDs, unbalanced or nearly full OSDs
        (sort_by='used_pct') and slow disks (sort_by='commit_ms').
        """
        df_tree, perf = await gather(
            lambda: ceph.json(["osd", "df", "tree"]),
            lambda: ceph.try_json(["osd", "perf"]),
        )
        rows = osd_rows(df_tree, perf)
        counts = {
            "osds": len(rows),
            "up": sum(1 for r in rows if r["up"]),
            "in": sum(1 for r in rows if r["in"]),
        }
        if host:
            rows = [r for r in rows if r["host"] == host]
        if device_class:
            rows = [r for r in rows if r["class"] == device_class]
        if state in ("up", "down"):
            rows = [r for r in rows if r["up"] == (state == "up")]
        elif state in ("in", "out"):
            rows = [r for r in rows if r["in"] == (state == "in")]
        if sort_by == "id":
            rows.sort(key=lambda r: r["id"])
        else:
            rows.sort(key=lambda r: r[sort_by] if r[sort_by] is not None else -1, reverse=True)
        summary = df_tree.get("summary") or {}
        out = limited(rows, limit, what="OSDs")
        out["summary"] = compact(
            {
                **counts,
                "raw_total": fmt_bytes((summary.get("total_kb") or 0) * KIB),
                "raw_used_pct": num(summary.get("average_utilization"), 1),
                "used_pct_stddev": num(summary.get("dev"), 2),
            }
        )
        return out

    @app.tool(server, title="List pools")
    async def pool_list(
        pool: Annotated[str | None, Field(description="Only this pool.")] = None,
    ) -> dict[str, Any]:
        """One row per pool: redundancy (replica count or erasure-code profile),
        min_size, PG count and autoscale mode, CRUSH rule, application, stored
        data, raw usage, space left (max_avail), quota, and current client
        read/write and recovery rates."""
        if pool:
            pool_name(pool)
        detail, df, stats, rules = await gather(
            lambda: ceph.json(["osd", "pool", "ls", "detail"]),
            lambda: ceph.json(["df", "detail"]),
            lambda: ceph.try_json(["osd", "pool", "stats"], []),
            lambda: ceph.try_json(["osd", "crush", "rule", "dump"], []),
        )
        rows = pool_rows(detail or [], df or {}, stats, rules)
        if pool:
            rows = [r for r in rows if r["name"] == pool]
            if not rows:
                raise UsageError(f"no pool named {pool!r}")
        return limited(rows, 1000, what="pools")

    @app.tool(server, title="List placement groups")
    async def pg_list(
        states: Annotated[
            list[str] | None,
            Field(
                description="Only PGs whose state includes all of these, e.g. "
                "['degraded'], ['inactive'], ['backfilling'], ['inconsistent'], "
                "['active', 'clean']."
            ),
        ] = None,
        pool: Annotated[str | None, Field(description="Only PGs of this pool.")] = None,
        osd: Annotated[
            int | None, Field(description="Only PGs that have a copy on this OSD.")
        ] = None,
        stuck: Annotated[
            Literal["inactive", "unclean", "stale", "undersized", "degraded"] | None,
            Field(description="Only PGs stuck in this condition for longer than the threshold."),
        ] = None,
        limit: Annotated[int, Field(ge=1, le=1000)] = 50,
    ) -> dict[str, Any]:
        """Placement groups with their state, up and acting OSD sets, object and
        degraded/misplaced/unfound counts, and scrub times.

        Without arguments returns only the count of PGs per state. Give a
        filter (states, pool, osd, stuck) to list individual PGs. To inspect
        one PG in depth, run ceph_command with ['pg', '<pgid>', 'query'].
        """
        for state in states or []:
            word(state, "PG state")
        if stuck:
            data = await ceph.json(["pg", "dump_stuck", stuck])
            rows = pg_rows((data or {}).get("stuck_pg_stats") or [])
            if states:
                rows = [r for r in rows if all(s in r["state"] for s in states)]
            return limited(rows, limit, what="PGs")
        if not states and pool is None and osd is None:
            stat = await ceph.json(["pg", "stat"])
            summary = (stat or {}).get("pg_summary") or stat or {}
            return {
                "total": summary.get("num_pgs"),
                "by_state": {s["name"]: s["num"] for s in summary.get("num_pg_by_state") or []},
                "hint": "pass states, pool, osd or stuck to list individual PGs",
            }
        if osd is not None:
            args = ["pg", "ls-by-osd", f"osd.{osd_id(osd)}"]
        elif pool:
            args = ["pg", "ls-by-pool", pool_name(pool)]
        else:
            args = ["pg", "ls"]
        data = await ceph.json([*args, *(states or [])], timeout=120)
        rows = pg_rows((data or {}).get("pg_stats") or [])
        if pool and osd is not None:
            pools = await ceph.json(["osd", "pool", "ls", "detail"])
            ids = {str(p.get("pool_id")) for p in pools or [] if p.get("pool_name") == pool}
            rows = [r for r in rows if r["pgid"].split(".")[0] in ids]
        return limited(rows, limit, what="PGs")
