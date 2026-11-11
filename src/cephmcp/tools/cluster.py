"""Cluster-wide state: status, health, daemons, logs, crashes."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from ..app import App
from ..ceph import gather
from ..render import compact, fmt_bytes, fmt_rate, limited, pct
from ..timeline import audit_message
from .common import short_time, word

LOG_WINDOW = 10000  # mon_log_max, the size of the monitor's log buffer


def digest_health(health: dict[str, Any], max_detail: int = 0) -> dict[str, Any]:
    checks = []
    for code, check in (health.get("checks") or {}).items():
        entry: dict[str, Any] = {
            "code": code,
            "severity": check.get("severity", "").removeprefix("HEALTH_"),
            "summary": (check.get("summary") or {}).get("message"),
            "muted": check.get("muted") or None,
        }
        detail = [d.get("message") for d in check.get("detail") or []]
        if max_detail and detail:
            entry["detail"] = detail[:max_detail]
            if len(detail) > max_detail:
                entry["detail_omitted"] = len(detail) - max_detail
        checks.append(entry)
    return {
        "status": health.get("status"),
        "checks": checks,
        "mutes": [m.get("code") for m in health.get("mutes") or []],
    }


CORE_DAEMONS = {"mon", "mgr", "osd", "mds"}


def service_daemons(servicemap: dict[str, Any] | None) -> list[tuple[str, dict[str, Any]]]:
    """(kind, {key: daemon}) for the gateway-type services in the service map:
    rgw, rbd-mirror, nfs and so on. Core daemons sometimes leave a stub entry
    there; those are reported elsewhere and skipped."""
    out = []
    for kind, service in ((servicemap or {}).get("services") or {}).items():
        daemons = {
            key: d
            for key, d in (service.get("daemons") or {}).items()
            if isinstance(d, dict) and d.get("metadata")
        }
        if kind not in CORE_DAEMONS and daemons:
            out.append((kind, daemons))
    return out


def digest_status(status: dict[str, Any]) -> dict[str, Any]:
    pgmap = status.get("pgmap") or {}
    osdmap = status.get("osdmap") or {}
    mgrmap = status.get("mgrmap") or {}
    monmap = status.get("monmap") or {}
    fsmap = status.get("fsmap") or {}
    quorum = status.get("quorum_names") or []
    total = pgmap.get("bytes_total")

    def ratio(key: str) -> float | None:
        value = pgmap.get(key)
        return round(100 * value, 2) if value else None

    services = {kind: len(daemons) for kind, daemons in service_daemons(status.get("servicemap"))}
    progress = [
        f"{event.get('message')} ({round(100 * event.get('progress', 0))}%)"
        for event in (status.get("progress_events") or {}).values()
    ]
    return compact(
        {
            "fsid": status.get("fsid"),
            "health": digest_health(status.get("health") or {}),
            "mon": {
                "count": monmap.get("num_mons"),
                "quorum": quorum,
                "out_of_quorum": (monmap.get("num_mons") or len(quorum)) - len(quorum) or None,
                "release": monmap.get("min_mon_release_name"),
            },
            "mgr": {
                "available": mgrmap.get("available"),
                "standbys": mgrmap.get("num_standbys"),
                "modules": mgrmap.get("modules"),
            },
            "osd": {
                "total": osdmap.get("num_osds"),
                "up": osdmap.get("num_up_osds"),
                "in": osdmap.get("num_in_osds"),
                "remapped_pgs": osdmap.get("num_remapped_pgs") or None,
            },
            "mds": {
                "ranks": [
                    f"{r.get('name')}: {r.get('status')}" for r in fsmap.get("by_rank") or []
                ],
                "standby": fsmap.get("up:standby") or None,
            },
            "services": services,
            "pgs": {
                "total": pgmap.get("num_pgs"),
                "by_state": {s["state_name"]: s["count"] for s in pgmap.get("pgs_by_state") or []},
                "inactive_pct": ratio("inactive_pgs_ratio"),
                "degraded_objects_pct": ratio("degraded_ratio"),
                "misplaced_objects_pct": ratio("misplaced_ratio"),
                "unfound_objects": pgmap.get("unfound_objects") or None,
            },
            "data": {
                "pools": pgmap.get("num_pools"),
                "objects": pgmap.get("num_objects"),
                "stored": fmt_bytes(pgmap.get("data_bytes")),
                "raw_used": fmt_bytes(pgmap.get("bytes_used")),
                "raw_avail": fmt_bytes(pgmap.get("bytes_avail")),
                "raw_total": fmt_bytes(total),
                "raw_used_pct": pct(pgmap.get("bytes_used"), total),
            },
            "client_io": {
                "read": fmt_rate(pgmap.get("read_bytes_sec"))
                if pgmap.get("read_bytes_sec")
                else None,
                "write": fmt_rate(pgmap.get("write_bytes_sec"))
                if pgmap.get("write_bytes_sec")
                else None,
                "read_ops_per_sec": pgmap.get("read_op_per_sec") or None,
                "write_ops_per_sec": pgmap.get("write_op_per_sec") or None,
            },
            "recovery_io": {
                "rate": fmt_rate(pgmap.get("recovering_bytes_per_sec"))
                if pgmap.get("recovering_bytes_per_sec")
                else None,
                "objects_per_sec": pgmap.get("recovering_objects_per_sec") or None,
                "keys_per_sec": pgmap.get("recovering_keys_per_sec") or None,
            },
            "progress": progress,
        }
    )


def register(server: MCPServer, app: App) -> None:
    ceph = app.ceph

    @app.tool(server, title="Cluster status")
    async def cluster_status() -> dict[str, Any]:
        """Overall cluster state: health checks, mon quorum, mgr, OSD up/in counts,
        PG states, capacity, and current client and recovery I/O rates.

        Start here. Follow up with health_detail for the specifics of a health
        check, osd_list / pool_list / pg_list for per-object detail, and
        perf_overview for performance.
        """
        return digest_status(await ceph.json(["status"]))

    @app.tool(server, title="Health detail")
    async def health_detail(
        max_detail: Annotated[
            int, Field(ge=1, le=500, description="Detail lines to return per health check.")
        ] = 20,
    ) -> dict[str, Any]:
        """Active health checks with their detail lines (which OSDs, PGs or pools
        are affected), plus muted checks and crashes not yet acknowledged."""
        health, crashes = await gather(
            lambda: ceph.json(["health", "detail"]),
            lambda: ceph.try_json(["crash", "ls-new"], []),
        )
        out = digest_health(health, max_detail)
        if crashes:
            out["new_crashes"] = len(crashes)
        return compact(out)

    @app.tool(server, title="Daemons and versions")
    async def daemon_list() -> dict[str, Any]:
        """Monitors, managers, metadata servers and gateways: name, host, role and
        state, plus the Ceph version each daemon type runs. OSDs are only
        counted per host here; use osd_list for them."""
        quorum, mon_meta, mgr_stat, mgr_meta, fs, services, versions, nodes = await gather(
            lambda: ceph.json(["quorum_status"]),
            lambda: ceph.try_json(["mon", "metadata"], []),
            lambda: ceph.try_json(["mgr", "stat"], {}),
            lambda: ceph.try_json(["mgr", "metadata"], []),
            lambda: ceph.try_json(["fs", "status"], {}),
            lambda: ceph.try_json(["service", "dump"], {}),
            lambda: ceph.try_json(["versions"], {}),
            lambda: ceph.try_json(["node", "ls"], {}),
        )
        mon_hosts = {m.get("name"): m.get("hostname") for m in mon_meta or []}
        in_quorum = set(quorum.get("quorum_names") or [])
        leader = quorum.get("quorum_leader_name")
        mons = [
            {
                "name": m["name"],
                "host": mon_hosts.get(m["name"]),
                "addr": (m.get("public_addr") or "").split("/")[0],
                "state": "leader"
                if m["name"] == leader
                else "peon"
                if m["name"] in in_quorum
                else "OUT OF QUORUM",
            }
            for m in (quorum.get("monmap") or {}).get("mons") or []
        ]
        active = (mgr_stat or {}).get("active_name")
        mgrs = [
            {
                "name": m.get("name"),
                "host": m.get("hostname"),
                "state": "active" if m.get("name") == active else "standby",
            }
            for m in mgr_meta or []
        ]
        mds = [
            {
                "name": m.get("name"),
                "state": m.get("state"),
                "rank": m.get("rank"),
                "fs": m.get("fs"),
            }
            for m in (fs or {}).get("mdsmap") or []
        ]
        other = {
            kind: [
                {
                    "name": d["metadata"].get("id", key),
                    "host": d["metadata"].get("hostname"),
                    "frontend": d["metadata"].get("frontend_config#0"),
                }
                for key, d in daemons.items()
            ]
            for kind, daemons in service_daemons(services)
        }
        version_table = {
            kind: {v.split("(")[0].replace("ceph version", "").strip(): n for v, n in vs.items()}
            for kind, vs in (versions or {}).items()
            if kind != "overall"
        }
        return compact(
            {
                "mon": limited(mons, 50),
                "mgr": limited(mgrs, 50),
                "mds": limited(mds, 100) if mds else None,
                "other_services": other,
                "osds_per_host": {
                    host: len(ids) for host, ids in ((nodes or {}).get("osd") or {}).items()
                },
                "versions": version_table,
                "mixed_versions": len((versions or {}).get("overall") or {}) > 1 or None,
            }
        )

    @app.tool(server, title="Cluster log")
    async def cluster_log(
        lines: Annotated[int, Field(ge=1, le=500, description="Number of recent entries.")] = 30,
        level: Annotated[
            Literal["debug", "info", "sec", "warn", "error"],
            Field(description="Minimum severity."),
        ] = "info",
        channel: Annotated[
            str,
            Field(
                description="Log channel: 'cluster' (events and health changes), "
                "'audit' (commands that were run), 'cephadm', or '*' for all."
            ),
        ] = "cluster",
    ) -> dict[str, Any]:
        """Recent entries of the central cluster log, newest last. The 'cluster'
        channel shows health transitions, OSDs going down, slow ops and the like;
        'audit' shows which administrative commands were run and by whom."""
        if channel != "*":
            word(channel, "log channel")
        # The monitor takes the last N entries and only then filters by level,
        # so ask for its whole buffer and keep the tail of what matches.
        entries = await ceph.json(["log", "last", str(LOG_WINDOW), level, channel]) or []
        entries = entries[-lines:]
        secrets = app.settings.allow_secrets
        rows = [
            {
                "time": short_time(e.get("stamp")),
                "channel": e.get("channel"),
                "level": (e.get("priority") or "").strip("[]"),
                "from": e.get("name"),
                # Audit entries quote whole commands, values of secret options included.
                "message": audit_message(str(e.get("message") or ""), secrets=secrets)
                if e.get("channel") == "audit"
                else e.get("message"),
            }
            for e in entries
        ]
        return limited(rows, lines, what="entries")

    @app.tool(server, title="Daemon crashes")
    async def crash_list(
        crash_id: Annotated[
            str | None,
            Field(description="Return the full report of this crash, including the backtrace."),
        ] = None,
        limit: Annotated[int, Field(ge=1, le=200)] = 30,
    ) -> dict[str, Any]:
        """Daemon crash reports collected by the cluster, newest first. Without
        arguments lists them; pass crash_id for one report with its backtrace."""
        if crash_id:
            info = await ceph.json(["crash", "info", word(crash_id, "crash id")])
            keep = (
                "crash_id",
                "timestamp",
                "entity_name",
                "ceph_version",
                "process_name",
                "utsname_hostname",
                "assert_condition",
                "assert_func",
                "assert_file",
                "assert_line",
                "assert_msg",
                "archived",
            )
            out = {k: info.get(k) for k in keep}
            out["backtrace"] = (info.get("backtrace") or [])[:40]
            return compact(out)
        crashes, new = await gather(
            lambda: ceph.json(["crash", "ls"]),
            lambda: ceph.try_json(["crash", "ls-new"], []),
        )
        unseen = {c.get("crash_id") for c in new or []}
        rows = [
            {
                "crash_id": c.get("crash_id"),
                "time": short_time(c.get("timestamp")),
                "daemon": c.get("entity_name"),
                "new": c.get("crash_id") in unseen,
            }
            for c in sorted(crashes or [], key=lambda c: c.get("timestamp", ""), reverse=True)
        ]
        return limited(rows, limit, what="crashes")
