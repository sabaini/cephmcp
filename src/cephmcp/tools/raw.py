"""The escape hatch: run any ceph command the server's mode permits."""

from __future__ import annotations

import contextlib
import json
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import Context, MCPServer
from pydantic import Field

from .. import guard
from ..app import App, UsageError
from ..ceph import CephError


def check_args(args: list[str]) -> None:
    if not args:
        raise UsageError("args is empty; give the command words, e.g. ['osd', 'tree']")
    for arg in args:
        if not arg or len(arg) > 4096 or "\x00" in arg or "\n" in arg:
            raise UsageError("each argument must be a non-empty single line")
    if args[0] == "ceph":
        raise UsageError("leave out the leading 'ceph'")


def register(server: MCPServer, app: App) -> None:
    if not app.settings.admin:
        scope = "This server is read-only: commands that change the cluster are refused."
    elif app.settings.allow_destructive:
        scope = "This server is in admin mode, destructive commands included."
    else:
        scope = (
            "This server is in admin mode: commands that change the cluster are allowed, "
            "except destructive ones (deleting pools, purging OSDs, removing file "
            "systems, anything that needs a --yes-i-really flag)."
        )

    @app.tool(
        server,
        title="Run a ceph command",
        read_only=not app.settings.admin,
        destructive=app.settings.allow_destructive,
        note=scope,
    )
    async def ceph_command(
        ctx: Context,
        args: Annotated[
            list[str],
            Field(
                description="Command words without the leading 'ceph', one per element: "
                "['osd', 'pool', 'autoscale-status'], ['tell', 'osd.3', 'config', 'show'], "
                "['pg', '2.1f', 'query']. Options go last, as single --name=value tokens."
            ),
        ],
        format: Annotated[
            Literal["plain", "json"],
            Field(description="plain: the human-readable table or text. json: full detail."),
        ] = "plain",
        # Plain `str`, not `str | None`: the SDK decodes JSON-looking strings
        # for every other annotation, which would mangle a JSON document here.
        input: Annotated[
            str,
            Field(
                description="Text for commands that read a file with -i, e.g. a service "
                "spec for ['orch', 'apply']. It is passed on stdin; do not add -i yourself."
            ),
        ] = "",
        timeout: Annotated[
            float | None, Field(ge=1, le=900, description="Seconds to wait; default 30.")
        ] = None,
    ) -> Any:
        """Run a `ceph` CLI command and return its output. Use this for anything
        the other tools do not cover, e.g. ['fs', 'status'], ['orch', 'ps'],
        ['osd', 'crush', 'rule', 'dump'], ['device', 'ls'], ['balancer', 'status'],
        ['osd', 'pool', 'autoscale-status'], ['tell', 'osd.3', 'bench'].

        Secrets in the output are masked. Output is cut off when very long, so
        prefer the plain format and specific commands over full dumps.
        """
        check_args(args)
        verdict = await app.policy.check_ceph(args)
        argv = list(args)
        stdin = None
        if input:
            app.policy.require("write", "passing input to a command")
            argv += ["-i", "-"]
            stdin = input.encode()
        command = "ceph " + " ".join(args)
        try:
            result = await app.ceph.run(
                argv, fmt="json" if format == "json" else None, timeout=timeout, input=stdin
            )
        except CephError as exc:
            if verdict.level != "read":
                await app.journal(
                    "ceph_command",
                    command[:200],
                    commands=[command],
                    outcome=f"failed: {str(exc)[:300]}",
                    client=guard.client_name(ctx),
                )
            raise
        if verdict.level != "read":
            # A change made by hand: on record, but with nothing known to undo it.
            await app.journal(
                "ceph_command", command[:200], commands=[command], client=guard.client_name(ctx)
            )
        out = result.stdout.strip()
        if format == "json" and out:
            # Returned as data, so that secrets are masked by field as well.
            with contextlib.suppress(ValueError):
                return json.loads(out)
        notes = result.stderr.strip()
        if out and notes:
            return f"{out}\n[stderr] {notes[-2000:]}"
        return out or notes or "(no output; the command succeeded)"
