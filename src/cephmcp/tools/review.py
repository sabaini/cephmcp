"""Cluster review: one call that looks for weaknesses and says how to fix them."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from .. import review
from ..app import App
from ..ceph import CephClient, gather
from ..render import compact, fmt_bytes, pct
from ..topology import build

Severity = Literal["critical", "high", "medium", "low", "info"]


# What the checks read beside the layout: command, and what stands in for it
# when it cannot be read.
OPTIONAL: tuple[tuple[tuple[str, ...], Any], ...] = (
    (("osd", "crush", "rule", "dump"), []),
    (("df", "detail"), {}),
    (("status",), {}),
    (("health", "detail"), {}),
    (("config", "dump"), []),
    (("versions",), {}),
    (("osd", "metadata"), []),
    (("mon", "metadata"), []),
    (("crash", "ls-new"), []),
    (("balancer", "status"), {}),
    (("osd", "pool", "autoscale-status"), []),
    (("osd", "perf"), {}),
    (("fs", "status"), {}),
    (("config", "get", "osd", "osd_memory_target"), review.DEFAULT_OSD_MEMORY_TARGET),
)


async def collect(ceph: CephClient) -> tuple[review.Inputs, list[str]]:
    """Everything the checks look at, and the commands that gave no answer.
    Only the layout is required; whatever else cannot be read (an old
    release, a restricted identity) is left out, and the checks that need it
    stay silent. The caller says so: a review with gaps is not a clean bill."""
    df_tree, osd_dump, *rest = await gather(
        lambda: ceph.json(["osd", "df", "tree"]),
        lambda: ceph.json(["osd", "dump"]),
        *[lambda args=args: ceph.try_json(list(args)) for args, _ in OPTIONAL],
    )
    unread = [
        " ".join(args) for (args, _), value in zip(OPTIONAL, rest, strict=True) if value is None
    ]
    (
        rules,
        df,
        status,
        health,
        config,
        versions,
        osd_metadata,
        mon_metadata,
        new_crashes,
        balancer,
        autoscale,
        osd_perf,
        fs,
        memory_target,
    ) = [
        default if value is None else value
        for (_, default), value in zip(OPTIONAL, rest, strict=True)
    ]
    try:
        memory_target = int(memory_target)
    except (TypeError, ValueError):
        memory_target = review.DEFAULT_OSD_MEMORY_TARGET
    inputs = review.Inputs(
        topology=build(df_tree, osd_dump, rules, df),
        status=status,
        health=health,
        config=config,
        versions=versions,
        osd_metadata=osd_metadata,
        mon_metadata=mon_metadata,
        new_crashes=new_crashes,
        balancer=balancer,
        autoscale=autoscale,
        osd_perf=osd_perf,
        fs=fs,
        osd_memory_target=memory_target,
    )
    return inputs, unread


def report(
    inputs: review.Inputs, min_severity: str = "low", unread: list[str] | None = None
) -> dict[str, Any]:
    findings = review.run(inputs)
    cutoff = review.SEVERITIES.index(min_severity)
    shown = [f for f in findings if review.SEVERITIES.index(f.severity) <= cutoff]
    counts = {
        s: sum(1 for f in findings if f.severity == s)
        for s in review.SEVERITIES
        if any(f.severity == s for f in findings)
    }
    topo = inputs.topology
    osds = [o for o in topo.osds.values() if o.in_]
    total, used = sum(o.size for o in osds), sum(o.used for o in osds)
    if shown:
        worst = [f"{n} {s}" for s, n in counts.items() if review.SEVERITIES.index(s) <= cutoff]
        summary = ", ".join(worst)
    else:
        summary = f"nothing found at severity '{min_severity}' or above"
    hidden = len(findings) - len(shown)
    return compact(
        {
            "summary": summary,
            "cluster": {
                "health": (inputs.health or inputs.status.get("health") or {}).get("status"),
                "hosts": len(topo.hosts()),
                "osds": len(topo.osds),
                "pools": len(topo.pools),
                "raw_total": fmt_bytes(total),
                "raw_used_pct": pct(used, total),
            },
            "findings": [f.as_dict() for f in shown],
            "not_shown": f"{hidden} below '{min_severity}'" if hidden else None,
            "could_not_read": unread or None,
            "incomplete": "The checks that need what could not be read were skipped: "
            "finding nothing there does not mean there is nothing."
            if unread
            else None,
            "checked": [check.__name__.replace("_", " ") for check in review.CHECKS]
            + ["health checks"],
        }
    )


def register(server: MCPServer, app: App) -> None:
    @app.tool(server, title="Review the cluster")
    async def cluster_review(
        min_severity: Annotated[
            Severity,
            Field(description="Leave out findings below this severity."),
        ] = "low",
    ) -> dict[str, Any]:
        """Audit the cluster for weaknesses, most serious first: pools that can
        lose data or stall on a single failure, CRUSH rules that keep copies on
        one host, flags left set, too little free space to recover from a
        failure, monitors and managers without redundancy, OSDs sharing a
        device or outgrowing their host's memory, slow disks, mixed versions,
        crashes nobody looked at, and risky configuration.

        Each finding says what is wrong, what it means and how to fix it. Many
        of these are conditions Ceph itself reports as HEALTH_OK. Use it for
        "is this cluster set up well", before going to production, and as the
        first step of a periodic check-up.
        """
        inputs, unread = await collect(app.ceph)
        return report(inputs, min_severity, unread)
