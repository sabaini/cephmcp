"""Command-line entry point."""

from __future__ import annotations

import argparse
import contextlib
import hmac
import ipaddress
import logging
import os
import shlex
import signal
import sys
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import anyio
from anyio.abc import TaskStatus

from . import __version__
from .ceph import CephError
from .config import ENV_PREFIX, Settings, default_state_dir, env_values, validate
from .server import build_server, charts_available

DESCRIPTION = """\
MCP server for Ceph and MicroCeph: lets AI agents inspect, measure and
administer a cluster.

By default it serves MCP on stdin/stdout and is read-only. The permission,
cluster access and metrics options can also be set through the environment:
--ceph-cmd is CEPHMCP_CEPH_CMD.
"""

EPILOG = """\
examples:
  cephmcp                                  read-only, stdio, local ceph CLI
  cephmcp --mode admin                     allow changes to the cluster
  cephmcp --ceph-cmd 'ssh node1 sudo ceph' reach a cluster over ssh
  cephmcp --transport http --port 8000     serve streamable HTTP on localhost
  cephmcp --check                          test the setup and exit
"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cephmcp",
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--version", action="version", version=f"cephmcp {__version__}")
    p.add_argument(
        "--check", action="store_true", help="test cluster access and metric sources, then exit"
    )

    g = p.add_argument_group("permissions")
    g.add_argument("--mode", choices=["read-only", "admin"], help="read-only (default) or admin")
    g.add_argument(
        "--allow-destructive",
        action="store_true",
        default=None,
        help="admin mode only: also allow operations that can lose data",
    )
    g.add_argument(
        "--allow-secrets",
        action="store_true",
        default=None,
        help="do not mask keys and passwords in results",
    )

    g.add_argument(
        "--approval",
        choices=["auto", "required", "off"],
        help="who confirms host maintenance and pool deletion: auto (default) asks a "
        "person through the client where it can, else the agent confirms with a code; "
        "required refuses without a person; off asks nobody",
    )

    g = p.add_argument_group("cluster access")
    g.add_argument(
        "--ceph-cmd",
        metavar="CMD",
        help="how to run ceph, e.g. 'ssh node sudo ceph' (default: autodetect)",
    )
    g.add_argument(
        "--microceph-cmd", metavar="CMD", help="how to run microceph (default: autodetect)"
    )
    g.add_argument("--ceph-conf", metavar="PATH", help="ceph.conf to use")
    g.add_argument("--ceph-keyring", metavar="PATH", help="keyring to use")
    g.add_argument(
        "--ceph-name", metavar="NAME", help="client name, e.g. client.mcp (default: client.admin)"
    )
    g.add_argument(
        "--command-timeout", type=float, metavar="SEC", help="per-command timeout (default 30)"
    )

    g = p.add_argument_group("metrics")
    g.add_argument(
        "--mgr-prometheus-url",
        metavar="URL",
        help="mgr prometheus module endpoint (default: discover)",
    )
    g.add_argument(
        "--ceph-exporter-url",
        dest="ceph_exporter_urls",
        action="append",
        metavar="URL",
        help="ceph-exporter endpoint; repeatable (default: discover)",
    )
    g.add_argument(
        "--ceph-exporter-port",
        type=int,
        metavar="PORT",
        help="port probed for ceph-exporter (default 9926)",
    )
    g.add_argument(
        "--no-discover",
        dest="discover_exporters",
        action="store_false",
        default=None,
        help="use only the metric endpoints given explicitly",
    )
    g.add_argument("--prometheus-url", metavar="URL", help="Prometheus server for PromQL queries")
    g.add_argument(
        "--insecure-tls",
        dest="tls_verify",
        action="store_false",
        default=None,
        help="do not verify TLS certificates of metric endpoints",
    )

    g = p.add_argument_group("history")
    g.add_argument(
        "--state-dir",
        metavar="DIR",
        help="where the history and the change journal are kept "
        "(default: $XDG_STATE_HOME/cephmcp, i.e. ~/.local/state/cephmcp)",
    )
    g.add_argument(
        "--no-state",
        action="store_true",
        help="keep nothing on disk: history and journal last as long as the process",
    )
    g.add_argument(
        "--no-record",
        dest="record",
        action="store_false",
        default=None,
        help="do not sample the cluster in the background",
    )
    g.add_argument(
        "--record-only",
        action="store_true",
        help="only record history and serve nothing: run this as a service to have "
        "history from times when no client is connected",
    )
    g.add_argument(
        "--record-interval", type=float, metavar="SEC", help="seconds between samples (default 60)"
    )
    g.add_argument(
        "--history-days", type=float, metavar="N", help="days of history to keep (default 90)"
    )

    g = p.add_argument_group("output")
    g.add_argument(
        "--max-output-chars",
        type=int,
        metavar="N",
        help="longest result a tool may return (default 40000)",
    )

    g = p.add_argument_group("transport")
    g.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="stdio (default) or streamable HTTP",
    )
    g.add_argument("--host", default="127.0.0.1", help="HTTP bind address (default 127.0.0.1)")
    g.add_argument("--port", type=int, default=8000, help="HTTP port (default 8000)")
    g.add_argument(
        "--auth-token",
        metavar="TOKEN",
        help=f"require this bearer token over HTTP (or {ENV_PREFIX}AUTH_TOKEN)",
    )
    g.add_argument(
        "--allow-unauthenticated",
        action="store_true",
        help="permit HTTP on a non-loopback address without a token",
    )
    g.add_argument("--tls-cert", metavar="PATH", help="serve HTTPS with this certificate")
    g.add_argument("--tls-key", metavar="PATH", help="private key for --tls-cert")
    g.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="log level on stderr (default WARNING)",
    )
    return p


_ARG_FIELDS = (
    "mode",
    "allow_destructive",
    "allow_secrets",
    "approval",
    "ceph_conf",
    "ceph_keyring",
    "ceph_name",
    "command_timeout",
    "mgr_prometheus_url",
    "ceph_exporter_port",
    "discover_exporters",
    "prometheus_url",
    "tls_verify",
    "max_output_chars",
    "state_dir",
    "record",
    "record_interval",
    "history_days",
)


def settings_from(args: argparse.Namespace, environ: dict[str, str] | None = None) -> Settings:
    """Environment first, command-line flags on top."""
    values: dict[str, Any] = env_values(environ)
    for field in _ARG_FIELDS:
        value = getattr(args, field)
        if value is not None:
            values[field] = value
    if args.ceph_cmd:
        values["ceph_cmd"] = tuple(shlex.split(args.ceph_cmd))
    if args.microceph_cmd:
        values["microceph_cmd"] = tuple(shlex.split(args.microceph_cmd))
    if args.ceph_exporter_urls:
        values["ceph_exporter_urls"] = tuple(args.ceph_exporter_urls)
    # Unlike the bare Settings, the command line records by default.
    values.setdefault("record", True)
    if args.no_state:
        values["state_dir"] = None
    else:
        values.setdefault("state_dir", default_state_dir(environ))
    return validate(Settings(**values))


def is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class BearerAuth:
    """ASGI middleware: require `Authorization: Bearer <token>` on every request."""

    def __init__(self, app: Any, token: str):
        self.app = app
        self.expected = f"Bearer {token}".encode()

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        given = dict(scope.get("headers") or []).get(b"authorization", b"")
        if hmac.compare_digest(given, self.expected):
            await self.app(scope, receive, send)
            return
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [(b"www-authenticate", b"Bearer"), (b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"unauthorized\n"})


async def check(settings: Settings) -> int:
    """Print what the server can reach. Returns a process exit status."""
    server, app = build_server(settings)
    ok = True
    print(
        f"cephmcp {__version__}, mode {settings.mode}"
        + (" +destructive" if settings.allow_destructive else "")
    )
    try:
        print(f"ceph command:      {' '.join(app.ceph.prefix())}")
        status = await app.ceph.json(["status"])
        print(f"cluster:           {status.get('fsid')} {status.get('health', {}).get('status')}")
    except CephError as exc:
        ok = False
        print(f"cluster:           FAILED - {exc}")
    prefix = app.microceph.locate()
    print(f"microceph command: {' '.join(prefix) if prefix else 'not found'}")
    if ok:
        found = await app.metrics.discover()
        for source in found.sources:
            scrape = await app.metrics.scrape(source)
            state = scrape.error or f"{len(scrape.metrics)} metrics, {scrape.series} series"
            print(f"metrics:           {source.kind} {source.url} - {state}")
        if not found.sources:
            print("metrics:           no endpoint found")
        print(f"prometheus server: {found.prometheus_url or 'none'}")
        for note in found.notes:
            print(f"note:              {note}")
    if settings.state_dir:
        recording = "on" if settings.record else "off (--no-record)"
        print(f"history:           {settings.state_dir}, recording {recording}")
    else:
        print("history:           in memory only (--no-state)")
    print(
        "charts:            "
        + ("available" if charts_available() else "not installed (pip install 'cephmcp[charts]')")
    )
    tools = await server.list_tools()
    print(f"tools ({len(tools)}):        {', '.join(t.name for t in tools)}")
    await app.http.aclose()
    return 0 if ok else 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        settings = settings_from(args)
    except ValueError as exc:
        parser.error(str(exc))

    if args.check:
        return anyio.run(check, settings)

    # Building the server also sets up logging, on stderr.
    server, app = build_server(settings, log_level=args.log_level)
    if args.log_level != "DEBUG":
        # One line per scrape of a metric endpoint is too much for INFO.
        for noisy in ("httpx2", "httpx", "httpcore2", "httpcore"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    async def run(serve: Callable[[], Awaitable[Any]] | None, stops_itself: bool) -> None:
        """Serve, with the recorder beside it for as long as the process lives.
        Without something to serve, record only.

        A service manager stops us with SIGTERM. We leave in order, so that
        the recorder gives up its lease instead of letting it run out.
        `stops_itself` says that what we serve shuts down on the signal by
        itself (uvicorn does, and then raises the signal again: receiving it
        here keeps that second delivery from ending the process before the
        recorder has finished)."""

        async def on_sigterm(
            scope: anyio.CancelScope,
            *,
            task_status: TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
        ) -> None:
            with anyio.open_signal_receiver(signal.SIGTERM) as signals:
                task_status.started()
                async for _ in signals:
                    if not stops_itself:
                        scope.cancel()

        try:
            async with anyio.create_task_group() as tasks:
                # In place before anything is served: uvicorn then puts its own
                # handler on top and hands back to this one when it is done.
                await tasks.start(on_sigterm, tasks.cancel_scope)
                if serve is None:
                    await app.recorder.run()
                elif settings.record:
                    tasks.start_soon(app.recorder.run)
                if serve is not None:
                    await serve()
                tasks.cancel_scope.cancel()
        finally:
            app.history.close()

    if args.record_only:
        print(
            f"cephmcp recording every {settings.record_interval:g}s into "
            f"{settings.state_dir or 'memory'}",
            file=sys.stderr,
        )
        _run(run, None, False)
        return 0

    if args.transport == "stdio":
        _run(run, server.run_stdio_async, False)
        return 0

    token = args.auth_token or os.environ.get(f"{ENV_PREFIX}AUTH_TOKEN")
    if not token and not is_loopback(args.host) and not args.allow_unauthenticated:
        parser.error(
            f"refusing to serve on {args.host} without authentication: anyone who can reach "
            "the port could operate the cluster. Set --auth-token (or "
            f"{ENV_PREFIX}AUTH_TOKEN), bind to 127.0.0.1, or pass --allow-unauthenticated."
        )
    if bool(args.tls_cert) != bool(args.tls_key):
        parser.error("--tls-cert and --tls-key must be given together")

    import uvicorn

    asgi: Any = server.streamable_http_app(host=args.host)
    if token:
        asgi = BearerAuth(asgi, token)
    config = uvicorn.Config(
        asgi,
        host=args.host,
        port=args.port,
        log_level=args.log_level.lower(),
        ssl_certfile=args.tls_cert,
        ssl_keyfile=args.tls_key,
    )
    scheme = "https" if args.tls_cert else "http"
    print(
        f"cephmcp serving {scheme}://{args.host}:{args.port}/mcp ({settings.mode})", file=sys.stderr
    )
    _run(run, uvicorn.Server(config).serve, True)
    return 0


def _run(
    run: Callable[..., Awaitable[None]],
    serve: Callable[[], Awaitable[Any]] | None,
    stops_itself: bool,
) -> None:
    with contextlib.suppress(KeyboardInterrupt):
        anyio.run(run, serve, stops_itself)


if __name__ == "__main__":
    sys.exit(main())
