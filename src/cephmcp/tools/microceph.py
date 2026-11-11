"""MicroCeph: the deployment layer around the Ceph cluster.

Registered only when the microceph command is present. MicroCeph manages
which services and disks run on which node; everything inside the cluster is
ordinary Ceph and handled by the other tools.
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from mcp.server.mcpserver import Context, MCPServer
from pydantic import Field

from .. import guard
from ..app import App
from ..ceph import CephError, gather
from ..policy import classify_microceph
from ..render import compact, fmt_bytes
from .raw import check_args


def parse_status(text: str) -> list[dict[str, Any]]:
    """Parse `microceph status` into one record per node."""
    nodes: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("- "):
            name, _, address = line[2:].partition(" (")
            nodes.append({"node": name.strip(), "address": address.rstrip(")") or None})
        elif nodes and line.startswith("Services:"):
            nodes[-1]["services"] = [s.strip() for s in line[9:].split(",") if s.strip()]
        elif nodes and line.startswith("Disks:"):
            count = line[6:].strip()
            nodes[-1]["disks"] = int(count) if count.isdigit() else count
    return nodes


def register(server: MCPServer, app: App) -> None:
    microceph = app.microceph

    async def try_json(args: list[str]) -> Any:
        try:
            result = await microceph.run(args)
            return json.loads(result.stdout)
        except (CephError, ValueError):
            return None

    @app.tool(server, title="MicroCeph deployment")
    async def microceph_status() -> dict[str, Any]:
        """How this MicroCeph cluster is laid out: the nodes, which services
        (mon, mgr, mds, rgw, osd, ...) each runs, the disks configured as OSDs,
        and unused disks that could be added. For the state of the Ceph cluster
        itself use cluster_status."""
        status, members, disks = await gather(
            lambda: microceph.run(["status"]),
            lambda: try_json(["cluster", "list", "--format=json"]),
            lambda: try_json(["disk", "list", "--json"]),
        )
        nodes = parse_status(status.stdout)
        roles = {
            m.get("name"): {"role": m.get("role"), "status": m.get("status")}
            for m in members or []
            if isinstance(m, dict)
        }
        for node in nodes:
            node.update(roles.get(node["node"], {}))
        out: dict[str, Any] = {"nodes": nodes}
        if isinstance(disks, dict):
            out["osd_disks"] = [
                {"osd": d.get("osd"), "node": d.get("location"), "path": d.get("path")}
                for d in disks.get("ConfiguredDisks") or []
            ]
            out["available_disks"] = [
                {
                    "path": d.get("Path"),
                    "model": d.get("Model"),
                    "size": d.get("Size")
                    if isinstance(d.get("Size"), str)
                    else fmt_bytes(d.get("Size")),
                    "type": d.get("Type"),
                }
                for d in disks.get("AvailableDisks") or []
            ]
        if not nodes:
            out["raw"] = status.stdout.strip()
        return compact(out)

    if not app.settings.admin:
        note = "This server is read-only: only commands that inspect are allowed."
    elif app.settings.allow_destructive:
        note = "This server is in admin mode, destructive commands included."
    else:
        note = (
            "This server is in admin mode: adding disks, enabling services and changing "
            "settings is allowed; removing disks or nodes is not."
        )

    @app.tool(
        server,
        title="Run a microceph command",
        read_only=not app.settings.admin,
        destructive=app.settings.allow_destructive,
        note=note,
    )
    async def microceph_command(
        ctx: Context,
        args: Annotated[
            list[str],
            Field(
                description="Command words without the leading 'microceph', one per element: "
                "['disk', 'list'], ['cluster', 'config', 'list'], ['enable', 'rgw', "
                "'--target=node2'], ['disk', 'add', 'loop,4G,1'], ['cluster', 'maintenance', "
                "'enter', 'node2', '--dry-run']. Options go last, as single --name=value tokens."
            ),
        ],
        timeout: Annotated[
            float | None, Field(ge=1, le=1800, description="Seconds to wait; default 30.")
        ] = None,
    ) -> str:
        """Run a `microceph` command: manage the nodes' disks (OSDs) and services
        (mon, mgr, mds, rgw, nfs, rbd-mirror), cluster-level settings and
        maintenance mode. Use `disk add` here rather than raw Ceph commands to
        create OSDs, and `enable`/`disable` to place services. Commands act on
        the node the server runs on unless they take --target."""
        check_args(args)
        verdict = classify_microceph(args)
        app.policy.require(verdict.level, f"`microceph {verdict.command}`")
        command = "microceph " + " ".join(args)
        client = guard.client_name(ctx)
        try:
            result = await microceph.run(args, timeout=timeout)
        except CephError as exc:
            if verdict.level != "read":
                await app.journal(
                    "microceph_command",
                    command[:200],
                    commands=[command],
                    outcome=f"failed: {str(exc)[:300]}",
                    client=client,
                )
            raise
        if verdict.level != "read":
            await app.journal("microceph_command", command[:200], commands=[command], client=client)
        out, notes = result.stdout.strip(), result.stderr.strip()
        if out and notes:
            return f"{out}\n[stderr] {notes[-2000:]}"
        return out or notes or "(no output; the command succeeded)"
