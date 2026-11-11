"""Test doubles: a fake ceph command line, fake metric endpoints, a fake clock.

The fixtures under tests/fixtures are real output captured from a Ceph 20.2
(Tentacle) cluster: three OSDs on one host, one healthy replicated pool and
one erasure-coded pool that cannot place its PGs (so health is WARN).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp import Client

from cephmcp.config import Settings
from cephmcp.runner import CommandResult
from cephmcp.server import build_server

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


# ceph arguments -> fixture file, or literal output
COMMANDS: dict[str, str] = {
    "-h": "ceph_help.txt",
    "status": "status.json",
    "health detail": "health_detail.json",
    "df detail": "df_detail.json",
    "osd df tree": "osd_df_tree.json",
    "osd perf": "osd_perf.json",
    "osd pool ls detail": "osd_pool_ls_detail.json",
    "osd pool stats": "osd_pool_stats.json",
    "osd crush rule dump": "osd_crush_rule_dump.json",
    "osd metadata": "osd_metadata.json",
    "pg stat": "pg_stat.json",
    "pg ls": "pg_ls.json",
    "pg ls incomplete": "pg_ls_incomplete.json",
    "pg ls-by-osd osd.0": "pg_ls.json",
    "pg ls-by-pool testpool": "pg_ls.json",
    "quorum_status": "quorum_status.json",
    "mon metadata": "mon_metadata.json",
    "mgr stat": "mgr_stat.json",
    "mgr metadata": "mgr_metadata.json",
    "mgr services": "mgr_services.json",
    "mgr module ls": "mgr_module_ls.json",
    "fs status": "fs_status.json",
    "service dump": "service_dump.json",
    "versions": "versions.json",
    "node ls": "node_ls.json",
    "crash ls": "[]",
    "crash ls-new": "[]",
    "log last 10000 info cluster": "log_last.json",
    "config dump": "config_dump.json",
    "config help osd_max_backfills": "config_help.json",
    "config get osd osd_max_backfills": "1",
    "config get osd": "config_get_osd.json",
    "config show osd.0": "config_show_osd0.json",
    "config get mgr mgr/prometheus/scrape_interval": "15.0",
    "config get mgr mgr/prometheus/exclude_perf_counters": "true",
    "tell osd.0 perf dump": "tell_osd0_perf_dump.json",
    "tell osd.0 perf schema": "tell_osd0_perf_schema.json",
    "tell osd.0 dump_historic_ops": "tell_osd0_historic.json",
    "orch host ls": "orch_host_ls.json",
    "orch ps --daemon_type ceph-exporter": "orch_ps_exporter.json",
    "orch ps --daemon_type prometheus": "orch_ps_prometheus.json",
    "fsid": "1ea4e1cc-ddcc-11f1-9f64-00163e578a2b",
}

# Complete snapshots of two other clusters, under fixtures/<scenario>/:
#   microceph  three hosts with two OSDs each, six 3x pools, HEALTH_OK
#   cephadm    one host with three OSDs, two 2x pools with min_size 1 and an
#              erasure-coded pool that cannot place its PGs, HEALTH_WARN
SCENARIO_COMMANDS: dict[str, str] = {
    "status": "status.json",
    "health detail": "health_detail.json",
    "df detail": "df_detail.json",
    "osd df tree": "osd_df_tree.json",
    "osd perf": "osd_perf.json",
    "osd dump": "osd_dump.json",
    "osd metadata": "osd_metadata.json",
    "osd pool ls detail": "osd_pool_ls_detail.json",
    "osd pool stats": "osd_pool_stats.json",
    "osd pool autoscale-status": "osd_pool_autoscale.json",
    "osd crush rule dump": "osd_crush_rule_dump.json",
    "pg dump pgs_brief": "pg_dump_pgs_brief.json",
    "pg stat": "pg_stat.json",
    "mon dump": "mon_dump.json",
    "mon metadata": "mon_metadata.json",
    "quorum_status": "quorum_status.json",
    "mgr stat": "mgr_stat.json",
    "mgr metadata": "mgr_metadata.json",
    "versions": "versions.json",
    "fs status": "fs_status.json",
    "mds metadata": "mds_metadata.json",
    "service dump": "service_dump.json",
    "node ls": "node_ls.json",
    "config dump": "config_dump.json",
    "config log 200": "config_log.json",
    "crash ls": "crash_ls.json",
    "crash ls-new": "crash_ls_new.json",
    "balancer status": "balancer_status.json",
    "progress json": "progress.json",
    "log last 10000 info cluster": "log_cluster.json",
    "log last 10000 info audit": "log_audit.json",
    "config get osd osd_memory_target": "4294967296",
}

GLOBAL_OPTIONS = {"--conf", "--keyring", "--name", "--connect-timeout", "--format"}


class FakeCli:
    """Stands in for runner.run_command: answers ceph commands from fixtures
    and records what was run."""

    def __init__(self) -> None:
        self.responses: dict[str, str | Callable[[], str]] = dict(COMMANDS)
        self.microceph: dict[str, str] = {}
        self.failures: dict[str, tuple[int, str, str]] = {}
        self.calls: list[str] = []
        self.inputs: dict[str, bytes | None] = {}
        self.formats: dict[str, str | None] = {}

    def fail(self, command: str, message: str, code: int = 2, output: str = "") -> None:
        """Make a command exit non-zero, with this on stderr (and stdout)."""
        self.failures[command] = (code, message, output)

    def use(self, scenario: str) -> None:
        """Answer from the snapshot of another cluster."""
        for command, name in SCENARIO_COMMANDS.items():
            if not name.endswith(".json"):
                self.responses[command] = name
            elif (FIXTURES / scenario / name).exists():
                self.responses[command] = f"{scenario}/{name}"
            else:
                self.responses.pop(command, None)

    def set_json(self, command: str, data: Any) -> None:
        self.responses[command] = json.dumps(data)

    def load(self, command: str) -> Any:
        """The scripted JSON answer to a command, for a test to modify."""
        answer = self.responses[command]
        assert isinstance(answer, str)
        return json.loads(fixture(answer) if answer.endswith(".json") else answer)

    async def __call__(
        self, argv: list[str], *, timeout: float = 30.0, input: bytes | None = None
    ) -> CommandResult:
        tool, args = argv[0], list(argv[1:])
        fmt = None
        while len(args) >= 2 and args[0] in GLOBAL_OPTIONS:
            if args[0] == "--format":
                fmt = args[1]
            args = args[2:]
        command = " ".join(args)
        table = self.microceph if tool == "microceph" else self.responses
        self.calls.append(command if tool == "ceph" else f"{tool} {command}")
        self.inputs[command] = input
        self.formats[command] = fmt
        if command in self.failures:
            code, message, output = self.failures[command]
            return CommandResult(tuple(argv), code, output, message, 0.0)
        if command not in table:
            return CommandResult(
                tuple(argv), 22, "", f"Error EINVAL: not scripted in the test: {command}", 0.0
            )
        answer = table[command]
        if callable(answer):
            answer = answer()
        elif answer.endswith((".json", ".txt")):
            answer = fixture(answer)
        return CommandResult(tuple(argv), 0, answer, "", 0.0)

    @property
    def changes(self) -> list[str]:
        """Commands run, minus the read-only lookups the tools make around them."""
        return [
            c
            for c in self.calls
            if c not in COMMANDS and c not in SCENARIO_COMMANDS and not c.startswith(LOOKUPS)
        ]


# Read-only commands that tools run with arguments, so they are in no table above.
LOOKUPS = (
    "config get ",
    "osd pool get ",
    "osd pool get-quota ",
    "osd ok-to-stop ",
    "mon ok-to-stop ",
    "orch status",
    "orch host ls",
)


class Clock:
    """Time that moves only when something sleeps."""

    def __init__(self) -> None:
        self.now = 1_800_000_000.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


MGR_URL = "http://10.165.218.74:9283/metrics"
EXPORTER_URL = "http://10.165.218.74:9926/metrics"
PROMETHEUS_URL = "http://10.165.218.74:9095"


class FakeEndpoints:
    """Stands in for the network: serves metric endpoints and a Prometheus API."""

    def __init__(self) -> None:
        self.pages: dict[str, str | Callable[[], str]] = {
            MGR_URL: fixture("mgr_metrics.txt"),
            EXPORTER_URL: fixture("exporter_metrics.txt"),
        }
        self.prometheus: dict[str, Any] = json.loads(fixture("prom_query.json"))
        self.prometheus_status = 200
        self.requests: list[httpx2.Request] = []
        self.transport = httpx2.MockTransport(self._handle)

    def _handle(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        url = str(request.url).split("?")[0]
        if url.startswith(PROMETHEUS_URL + "/api/v1/"):
            return httpx2.Response(self.prometheus_status, json=self.prometheus)
        page = self.pages.get(url)
        if page is None:
            raise httpx2.ConnectError("connection refused", request=request)
        return httpx2.Response(200, text=page() if callable(page) else page)


class Harness:
    """A server wired to the fakes, with an in-process MCP client."""

    def __init__(
        self,
        settings: Settings,
        cli: FakeCli,
        net: FakeEndpoints,
        clock: Clock,
        client_options: dict[str, Any] | None = None,
    ):
        self.cli, self.net, self.clock = cli, net, clock
        self.client_options = client_options or {}
        self.server, self.app = build_server(
            settings, runner=cli, transport=net.transport, clock=clock
        )
        self._client: Client | None = None

    async def __aenter__(self) -> Harness:
        self._client = Client(self.server, **self.client_options)
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._client is not None
        await self._client.__aexit__(None, None, None)

    async def tools(self) -> dict[str, Any]:
        assert self._client is not None
        return {t.name: t for t in (await self._client.list_tools()).tools}

    async def prompts(self) -> dict[str, Any]:
        assert self._client is not None
        return {p.name: p for p in (await self._client.list_prompts()).prompts}

    async def prompt(self, name: str, **arguments: str) -> str:
        assert self._client is not None
        result = await self._client.get_prompt(name, arguments)
        return "\n".join(m.content.text for m in result.messages)

    async def content(self, tool: str, **arguments: Any) -> list[Any]:
        """Everything a tool returned, pictures included."""
        assert self._client is not None
        result = await self._client.call_tool(tool, arguments)
        assert not result.is_error, result.content
        return list(result.content)

    async def raw(self, tool: str, **arguments: Any) -> tuple[bool, str]:
        assert self._client is not None
        result = await self._client.call_tool(tool, arguments)
        text = "\n".join(c.text for c in result.content if c.type == "text")
        return bool(result.is_error), text

    async def call(self, tool: str, **arguments: Any) -> Any:
        """Call a tool that must succeed; JSON results are decoded."""
        failed, text = await self.raw(tool, **arguments)
        assert not failed, text
        try:
            return json.loads(text)
        except ValueError:
            return text

    async def error(self, tool: str, **arguments: Any) -> str:
        """Call a tool that must fail; returns the message."""
        failed, text = await self.raw(tool, **arguments)
        assert failed, f"expected an error, got: {text[:300]}"
        return text


def rows(table: dict[str, Any]) -> list[dict[str, Any]]:
    """Turn a {"columns", "rows"} table back into records."""
    return [dict(zip(table["columns"], row, strict=True)) for row in table["rows"]]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def no_local_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests must not find a ceph or microceph installed on the machine."""
    monkeypatch.setattr("cephmcp.ceph.shutil.which", lambda name: None)
    monkeypatch.setattr("cephmcp.ceph.os.access", lambda path, mode: False)


@pytest.fixture
def cli() -> FakeCli:
    return FakeCli()


@pytest.fixture
def net() -> FakeEndpoints:
    return FakeEndpoints()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock()
    monkeypatch.setattr("cephmcp.tools.perf.sleep", clock.sleep)
    monkeypatch.setattr("cephmcp.tools.daemons.sleep", clock.sleep)
    monkeypatch.setattr("cephmcp.tools.maintenance.sleep", clock.sleep)
    monkeypatch.setattr("cephmcp.tools.iotop.sleep", clock.sleep)
    return clock


@pytest.fixture
def harness(cli: FakeCli, net: FakeEndpoints, clock: Clock) -> Callable[..., Harness]:
    def make(client: dict[str, Any] | None = None, **settings: Any) -> Harness:
        """`client` are options for the MCP client, e.g. an elicitation_callback."""
        settings.setdefault("ceph_cmd", ("ceph",))
        return Harness(Settings(**settings), cli, net, clock, client)

    return make
