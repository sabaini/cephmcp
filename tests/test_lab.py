"""Smoke test against a live cluster. Skipped unless CEPHMCP_LAB_CMD is set to
a command line that starts the server on stdio, for example:

    CEPHMCP_LAB_CMD='ssh lxd-host lxc exec cephmcp-1 -- cephmcp' pytest -m lab

It only reads. Every read-only tool must answer without an error.
"""

from __future__ import annotations

import json
import os
import shlex

import pytest
from mcp import Client, StdioServerParameters

LAB_CMD = os.environ.get("CEPHMCP_LAB_CMD", "")

pytestmark = [
    pytest.mark.lab,
    pytest.mark.anyio,
    pytest.mark.skipif(not LAB_CMD, reason="CEPHMCP_LAB_CMD is not set"),
]

CALLS: list[tuple[str, dict[str, object]]] = [
    ("cluster_status", {}),
    ("health_detail", {}),
    ("daemon_list", {}),
    ("cluster_log", {"lines": 5}),
    ("crash_list", {}),
    ("osd_list", {}),
    ("pool_list", {}),
    ("pg_list", {}),
    ("pg_list", {"states": ["active"], "limit": 3}),
    ("config_get", {}),
    ("config_get", {"who": "osd", "option": "osd_max_backfills"}),
    ("metrics_sources", {}),
    ("perf_overview", {"seconds": 0}),
    ("ceph_command", {"args": ["osd", "tree"]}),
    ("cluster_review", {}),
    ("runbook", {}),
    ("runbook", {"name": "triage", "subject": "slow"}),
    ("what_changed", {"since": "-2h"}),
    ("history_query", {}),
    ("capacity_forecast", {}),
    ("osd_outliers", {}),
    ("io_top", {"seconds": 2}),
    ("change_log", {}),
]


async def test_read_only_tools_answer() -> None:
    command, *args = shlex.split(LAB_CMD)
    async with Client(StdioServerParameters(command=command, args=args)) as client:
        offered = {tool.name for tool in (await client.list_tools()).tools}
        # OSD ids differ between deployments (MicroCeph starts at 1): ask for one.
        osds = await client.call_tool("osd_list", {"limit": 1})
        table = json.loads(osds.content[0].text)
        osd = f"osd.{table['rows'][0][table['columns'].index('id')]}"
        per_daemon: list[tuple[str, dict[str, object]]] = [
            ("daemon_perf", {"daemon": osd}),
            ("daemon_ops", {"daemon": osd, "kind": "historic"}),
            ("ceph_command", {"args": ["tell", osd, "version"]}),
        ]
        failures = []
        for tool, arguments in [*CALLS, *per_daemon]:
            result = await client.call_tool(tool, arguments)
            text = "\n".join(c.text for c in result.content if c.type == "text")
            if result.is_error or not text:
                failures.append(f"{tool} {json.dumps(arguments)}: {text[:300]}")
        assert not failures, "\n".join(failures)

        # A read-only server must refuse to change anything.
        refused = await client.call_tool("ceph_command", {"args": ["osd", "set", "noout"]})
        assert refused.is_error
        assert "cluster_flags" not in offered

        # The first host of the cluster: can it be rebooted? Either answer is fine.
        hosts = await client.call_tool("osd_list", {"limit": 1})
        host = table["rows"][0][table["columns"].index("host")]
        verdict = await client.call_tool("simulate", {"scenario": "reboot", "hosts": [host]})
        assert not verdict.is_error and not hosts.is_error, verdict.content
        assert json.loads(verdict.content[0].text)["verdict"] in (
            "safe",
            "safe, with caveats",
            "unsafe",
        )
        assert "maintenance_enter" not in offered and "change_undo" not in offered

        if "chart" in offered:
            picture = await client.call_tool("chart", {"kind": "osds"})
            assert not picture.is_error, picture.content
            assert [c.type for c in picture.content] == ["image", "text"]

        prompts = {p.name for p in (await client.list_prompts()).prompts}
        assert {"triage", "pre_maintenance", "incident_report"} <= prompts

        if "microceph_status" in offered:
            status = await client.call_tool("microceph_status", {})
            assert not status.is_error, status.content
