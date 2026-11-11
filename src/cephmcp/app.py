"""Shared state for the tools: settings, cluster access, metrics, policy."""

from __future__ import annotations

import functools
import inspect
import logging
import sqlite3
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

import httpx2
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.utilities.types import Image
from mcp_types import ToolAnnotations

from . import render
from .ceph import CephClient, CephError, MicroCephClient
from .config import Settings
from .http import Http, HttpError
from .metrics import MetricsHub
from .policy import Denied, Policy
from .recorder import History, Recorder
from .redact import redact_obj, redact_text
from .runner import Runner, run_command

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])
log = logging.getLogger("cephmcp")


class UsageError(Exception):
    """The arguments of a tool call do not make sense. Shown to the model."""


@dataclass
class App:
    settings: Settings
    ceph: CephClient
    microceph: MicroCephClient
    http: Http
    metrics: MetricsHub
    policy: Policy
    history: History
    recorder: Recorder
    clock: Callable[[], float]
    extras: dict[str, Any] = field(default_factory=dict)
    """State that single tool modules keep per server, under their own key."""

    @classmethod
    def create(
        cls,
        settings: Settings,
        runner: Runner = run_command,
        transport: httpx2.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.time,
    ) -> App:
        ceph = CephClient(settings, runner)
        http = Http(settings, transport)
        metrics = MetricsHub(settings, ceph, http, clock)
        history = History(settings, ceph)
        return cls(
            settings=settings,
            ceph=ceph,
            microceph=MicroCephClient(settings, runner),
            http=http,
            metrics=metrics,
            policy=Policy(settings, ceph),
            history=history,
            recorder=Recorder(settings, ceph, metrics, history, clock),
            clock=clock,
        )

    async def journal(
        self,
        tool: str,
        summary: str,
        *,
        arguments: Mapping[str, Any] | None = None,
        commands: Sequence[str] = (),
        undo: Sequence[Sequence[str]] = (),
        outcome: str = "done",
        client: str = "",
        undoes: int | None = None,
    ) -> int | None:
        """Record a change made through this server. Returns its number, or
        None if the journal cannot be written (which must not stop the work)."""
        try:
            store = await self.history.store()
            return await self.history.call(
                store.log_change,
                self.clock(),
                tool,
                summary,
                dict(arguments or {}),
                list(commands),
                [list(step) for step in undo],
                client,
                outcome,
                undoes,
            )
        except (CephError, sqlite3.Error, OSError) as exc:
            log.warning("change not journaled: %s", exc)
            return None

    def scrub(self, text: str) -> str:
        return text if self.settings.allow_secrets else redact_text(text)

    def render(self, result: Any) -> str:
        """Turn a tool's return value into the text the model reads."""
        if isinstance(result, str):
            text = self.scrub(result)
        else:
            if not self.settings.allow_secrets:
                result = redact_obj(result)
            text = render.dumps(result)
        return render.clip(text, self.settings.max_output_chars)

    def tool(
        self,
        server: MCPServer,
        *,
        title: str,
        read_only: bool = True,
        destructive: bool = False,
        idempotent: bool | None = None,
        note: str = "",
    ) -> Callable[[F], F]:
        """Register an async function as an MCP tool.

        The docstring is the description the model sees, followed by `note`
        if given. Expected failures become tool errors with a readable message;
        results are rendered as compact JSON, redacted and length-limited.
        """

        def decorator(fn: F) -> F:
            @functools.wraps(fn)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    result = await fn(*args, **kwargs)
                except (CephError, HttpError, Denied, UsageError) as exc:
                    raise ToolError(self.scrub(str(exc))) from None
                if isinstance(result, list) and any(isinstance(r, Image) for r in result):
                    # Pictures pass through; what comes with them is rendered as usual.
                    return [r if isinstance(r, Image) else self.render(r) for r in result]
                return self.render(result)

            description = inspect.cleandoc(fn.__doc__ or "")
            if note:
                description = f"{description}\n\n{note}"
            server.add_tool(
                wrapper,
                name=fn.__name__,
                title=title,
                description=description,
                annotations=ToolAnnotations(
                    read_only_hint=read_only,
                    destructive_hint=destructive if not read_only else None,
                    idempotent_hint=True if read_only else idempotent,
                    open_world_hint=False,
                ),
                structured_output=False,
            )
            return fn

        return decorator
