"""Settings, the command runner, the CLI client and the HTTP front door."""

from __future__ import annotations

import sys
from typing import Any

import pytest
from conftest import FakeCli

from cephmcp import cli as cli_module
from cephmcp import runner
from cephmcp.ceph import CephClient, CephError, MicroCephClient
from cephmcp.cli import BearerAuth, build_parser, is_loopback, main, settings_from
from cephmcp.config import Settings, env_values, validate
from cephmcp.runner import CommandError, run_command


class TestSettings:
    def parse(self, *argv: str, env: dict[str, str] | None = None) -> Settings:
        return settings_from(build_parser().parse_args(argv), env or {})

    def test_defaults_are_the_safe_ones(self) -> None:
        s = self.parse()
        assert s.mode == "read-only" and not s.admin and not s.destructive
        assert not s.allow_secrets and s.tls_verify and s.discover_exporters

    def test_environment_then_flags(self) -> None:
        env = {
            "CEPHMCP_MODE": "admin",
            "CEPHMCP_CEPH_CMD": "ssh node1 sudo ceph",
            "CEPHMCP_CEPH_EXPORTER_URLS": "http://a:9926, http://b:9926",
            "CEPHMCP_COMMAND_TIMEOUT": "5",
            "CEPHMCP_ALLOW_SECRETS": "yes",
            "UNRELATED": "x",
        }
        s = self.parse(env=env)
        assert s.admin and s.allow_secrets and s.command_timeout == 5.0
        assert s.ceph_cmd == ("ssh", "node1", "sudo", "ceph")
        assert s.ceph_exporter_urls == ("http://a:9926", "http://b:9926")
        flags = self.parse("--mode", "read-only", "--ceph-cmd", "lxc exec vm -- ceph", env=env)
        assert not flags.admin and flags.ceph_cmd == ("lxc", "exec", "vm", "--", "ceph")

    def test_flags(self) -> None:
        s = self.parse(
            "--mode", "admin", "--allow-destructive", "--insecure-tls", "--no-discover",
            "--ceph-exporter-url", "h1:9926", "--ceph-exporter-url", "h2:9926",
            "--prometheus-url", "http://prom:9090", "--ceph-name", "client.mcp",
        )  # fmt: skip
        assert s.destructive and not s.tls_verify and not s.discover_exporters
        assert s.ceph_exporter_urls == ("h1:9926", "h2:9926")
        assert s.prometheus_url == "http://prom:9090" and s.ceph_name == "client.mcp"

    def test_destructive_requires_admin(self) -> None:
        with pytest.raises(ValueError, match="requires mode 'admin'"):
            self.parse("--allow-destructive")
        with pytest.raises(ValueError, match="mode must be"):
            validate(Settings(mode="yolo"))  # type: ignore[arg-type]

    def test_bad_environment_values_name_the_variable(self) -> None:
        with pytest.raises(ValueError, match="CEPHMCP_COMMAND_TIMEOUT"):
            env_values({"CEPHMCP_COMMAND_TIMEOUT": "soon"})


@pytest.mark.anyio
class TestRunner:
    async def test_output_and_exit_status(self) -> None:
        ok = await run_command(
            [sys.executable, "-c", "print('hi'); import sys; print('warn', file=sys.stderr)"]
        )
        assert (ok.returncode, ok.stdout, ok.stderr) == (0, "hi\n", "warn\n")
        bad = await run_command([sys.executable, "-c", "raise SystemExit(3)"])
        assert bad.returncode == 3

    async def test_stdin(self) -> None:
        result = await run_command(
            [sys.executable, "-c", "import sys; print(sys.stdin.read().upper())"], input=b"spec"
        )
        assert result.stdout == "SPEC\n"

    async def test_timeout_kills_the_command(self) -> None:
        with pytest.raises(CommandError, match=r"timed out after 0\.3s"):
            await run_command([sys.executable, "-c", "import time; time.sleep(30)"], timeout=0.3)

    async def test_missing_binary(self) -> None:
        with pytest.raises(CommandError, match="command not found: /no/such/ceph"):
            await run_command(["/no/such/ceph", "status"])

    async def test_runaway_output_is_capped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(runner, "MAX_STDOUT", 1000)
        result = await run_command([sys.executable, "-c", "print('x' * 100000)"])
        assert len(result.stdout) == 1000 and result.truncated and result.returncode == 0


@pytest.mark.anyio
class TestCephClient:
    async def test_global_options_come_first(self, cli: FakeCli) -> None:
        seen: list[list[str]] = []

        async def spy(argv: list[str], **kwargs: Any) -> Any:
            seen.append(list(argv))
            return await cli(argv, **kwargs)

        s = Settings(ceph_cmd=("ceph",), ceph_conf="/c", ceph_keyring="/k", ceph_name="client.mcp")
        await CephClient(s, spy).json(["pg", "stat"])
        assert seen[0] == [
            "ceph", "--conf", "/c", "--keyring", "/k", "--name", "client.mcp",
            "--connect-timeout", "10", "--format", "json", "pg", "stat",
        ]  # fmt: skip

    async def test_ssh_prefix_quotes_arguments_for_the_remote_shell(self) -> None:
        s = Settings(ceph_cmd=("ssh", "node1", "sudo", "ceph"))
        argv = CephClient(s).build_argv(["osd", "pool", "get", "x; rm -rf /", "size"])
        assert argv[:4] == ["ssh", "node1", "sudo", "ceph"]
        assert argv[4:] == ["osd", "pool", "get", "'x; rm -rf /'", "size"]
        local = CephClient(Settings(ceph_cmd=("lxc", "exec", "vm", "--", "ceph")))
        assert local.build_argv(["a b"])[-1] == "a b"  # argv is passed through as is

    async def test_autodetect_prefers_ceph_then_microceph(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        found = {"microceph.ceph": "/snap/bin/microceph.ceph"}
        monkeypatch.setattr("cephmcp.ceph.shutil.which", found.get)
        assert CephClient(Settings()).prefix() == ["/snap/bin/microceph.ceph"]
        found["ceph"] = "/usr/bin/ceph"
        assert CephClient(Settings()).prefix() == ["/usr/bin/ceph"]

    async def test_snap_paths_are_found_without_PATH(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "cephmcp.ceph.os.access", lambda path, mode: path.startswith("/snap/bin/")
        )
        assert CephClient(Settings()).prefix() == ["/snap/bin/ceph"]
        assert MicroCephClient(Settings()).available

    async def test_nothing_installed(self) -> None:
        with pytest.raises(CephError, match="--ceph-cmd"):
            CephClient(Settings()).prefix()
        assert not MicroCephClient(Settings()).available

    async def test_failures(self, cli: FakeCli) -> None:
        client = CephClient(Settings(ceph_cmd=("ceph",)), cli)
        cli.fail("osd tree", "noise\n\nError EACCES: access denied", 13)
        with pytest.raises(
            CephError, match=r"(?s)`ceph osd tree` failed \(exit 13\).*access denied"
        ) as err:
            await client.json(["osd", "tree"])
        assert err.value.returncode == 13
        assert await client.try_json(["osd", "tree"], "fallback") == "fallback"
        cli.responses["status"] = "definitely not json"
        with pytest.raises(CephError, match="did not return JSON"):
            await client.json(["status"])
        cli.responses["status"] = ""
        assert await client.json(["status"]) is None


class TestHttpFrontDoor:
    def test_loopback(self) -> None:
        assert is_loopback("127.0.0.1") and is_loopback("localhost") and is_loopback("::1")
        assert (
            not is_loopback("0.0.0.0") and not is_loopback("10.0.0.5") and not is_loopback("node1")
        )

    def test_refuses_to_expose_the_cluster_without_a_token(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("CEPHMCP_AUTH_TOKEN", raising=False)
        with pytest.raises(SystemExit) as exit_info:
            main(["--transport", "http", "--host", "0.0.0.0"])
        assert exit_info.value.code == 2
        assert "without authentication" in capsys.readouterr().err

    def test_tls_needs_both_files(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit):
            main(["--transport", "http", "--tls-cert", "cert.pem"])
        assert "--tls-cert and --tls-key" in capsys.readouterr().err

    def test_invalid_settings_exit_with_a_message(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit):
            main(["--allow-destructive"])
        assert "requires mode 'admin'" in capsys.readouterr().err

    @pytest.mark.anyio
    async def test_bearer_token(self) -> None:
        reached: list[str] = []

        async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
            reached.append(scope["type"])

        async def request(headers: list[tuple[bytes, bytes]]) -> list[dict[str, Any]]:
            sent: list[dict[str, Any]] = []

            async def send(message: dict[str, Any]) -> None:
                sent.append(message)

            await BearerAuth(app, "s3cret")({"type": "http", "headers": headers}, None, send)
            return sent

        assert await request([(b"authorization", b"Bearer s3cret")]) == []
        assert reached == ["http"]
        for headers in ([], [(b"authorization", b"Bearer wrong")], [(b"authorization", b"s3cret")]):
            sent = await request(headers)
            assert sent[0]["status"] == 401
        assert reached == ["http"]
        # startup and shutdown events are not requests
        await BearerAuth(app, "s3cret")({"type": "lifespan"}, None, None)
        assert reached == ["http", "lifespan"]


@pytest.mark.anyio
async def test_check_reports_what_is_reachable(
    cli: FakeCli, net: Any, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    real = cli_module.build_server
    monkeypatch.setattr(
        cli_module,
        "build_server",
        lambda settings: real(settings, runner=cli, transport=net.transport),
    )
    assert await cli_module.check(Settings(ceph_cmd=("ceph",))) == 0
    out = capsys.readouterr().out
    assert "HEALTH_WARN" in out and "mode read-only" in out
    assert "mgr-prometheus" in out and "ceph-exporter" in out and "tools (" in out
    assert "cluster_status, " in out and "ceph_command" in out

    cli.fail("status", "[errno 2] RADOS object not found (error connecting to the cluster)", 1)
    assert await cli_module.check(Settings(ceph_cmd=("ceph",))) == 1
    assert "FAILED" in capsys.readouterr().out
