"""Assemble the MCP server."""

from __future__ import annotations

import importlib.util
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Literal

import httpx2
from mcp.server.mcpserver import MCPServer

from . import __version__
from .app import App
from .config import Settings
from .runner import Runner, run_command
from .tools import (
    admin,
    cluster,
    daemons,
    history,
    iotop,
    journal,
    maintenance,
    microceph,
    perf,
    raw,
    review,
    runbooks,
    simulate,
    storage,
)

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def charts_available() -> bool:
    """Whether Pillow, which draws the charts, is installed (the 'charts' extra)."""
    return importlib.util.find_spec("PIL") is not None


def instructions(app: App) -> str:
    s = app.settings
    if not s.admin:
        mode = (
            "The server is READ-ONLY: it can inspect everything but change nothing. "
            "If a change is needed, say what should be done and let the operator do it "
            "or restart the server in admin mode."
        )
    elif s.allow_destructive:
        mode = (
            "The server is in ADMIN mode with destructive operations enabled. Changes "
            "take effect on a live storage cluster immediately; confirm intent with the "
            "user before anything that removes data or redundancy."
        )
    else:
        mode = (
            "The server is in ADMIN mode: tools can change the live cluster (flags, OSD "
            "state and weights, pools, configuration). Operations that destroy data or "
            "redundancy are disabled."
        )
    microceph_note = (
        " This cluster is deployed with MicroCeph: use microceph_status and "
        "microceph_command for nodes, disks and services; there is no orchestrator "
        "(`ceph orch`)."
        if app.microceph.available
        else ""
    )
    pictures = (
        "\n- Pictures: chart draws history, the OSDs by host, or the capacity projection."
        if charts_available()
        else ""
    )
    changing = (
        "\n- Changes: host maintenance goes through maintenance_enter and maintenance_exit, "
        "which check, ask for approval and verify. Every change is journaled (change_log) "
        "and most can be reverted (change_undo)."
        if s.admin
        else ""
    )
    return f"""Tools to inspect, measure and administer a Ceph storage cluster.{microceph_note}

{mode}

Where to start:
- For a task that has a runbook (triage, slowness, host maintenance, incident report, \
capacity plan, check-up, upgrade, disk replacement), fetch it with runbook and follow it.
- State and health: cluster_status, then health_detail; osd_list, pool_list, pg_list for detail.
- Is the cluster set up well: cluster_review finds weaknesses that HEALTH_OK does not show.
- What happened: what_changed gives a timeline of events, commands and metric shifts; \
history_query, capacity_forecast and osd_outliers read the server's own recorded history.
- What would happen: simulate answers "can I reboot this host", "what if this disk dies", \
"how much moves if I add disks", without touching anything.
- Performance: perf_overview first, io_top for who causes the load. Then metrics_rate for \
rates and latencies over an interval, metrics_query for current values, daemon_perf and \
daemon_ops for one daemon. metrics_search finds metric names; metrics_sources explains \
what is available.{pictures}{changing}
- Anything else: ceph_command runs any ceph CLI command the server mode allows.

Conventions: results are JSON; lists are {{"columns": [...], "rows": [[...]]}}. Sizes are \
strings with binary units ("1.5 GiB"); latencies are milliseconds; times are UTC. Empty \
fields are left out. Keys and passwords are masked."""


def build_server(
    settings: Settings,
    *,
    runner: Runner = run_command,
    transport: httpx2.AsyncBaseTransport | None = None,
    clock: Callable[[], float] = time.time,
    log_level: LogLevel = "WARNING",
) -> tuple[MCPServer, App]:
    """Create the server with the tools that the settings permit.

    runner, transport and clock exist so that tests can substitute the
    command line, HTTP and time.
    """
    app = App.create(settings, runner, transport, clock)

    @asynccontextmanager
    async def lifespan(_: MCPServer) -> AsyncIterator[App]:
        # The app is handed to the handlers of a connection as its context.
        try:
            yield app
        finally:
            await app.http.aclose()

    server = MCPServer(
        "cephmcp",
        title="Ceph",
        description="Administration and performance metrics for Ceph and MicroCeph clusters",
        instructions=instructions(app),
        version=__version__,
        lifespan=lifespan,
        log_level=log_level,
    )
    cluster.register(server, app)
    review.register(server, app)
    storage.register(server, app)
    daemons.register(server, app)
    perf.register(server, app)
    history.register(server, app)
    simulate.register(server, app)
    iotop.register(server, app)
    raw.register(server, app)
    runbooks.register(server, app)
    if charts_available():
        from .tools import charts

        charts.register(server, app)
    if app.microceph.available:
        microceph.register(server, app)
    journal.register(server, app)
    if settings.admin:
        admin.register(server, app)
        maintenance.register(server, app)
    return server, app
