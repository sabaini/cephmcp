"""Access to the cluster through the ceph and microceph command-line tools.

The CLI is the one interface that every deployment has (MicroCeph, cephadm,
packages, Rook) and that needs nothing installed beside the cluster itself.
The command is an argv prefix, so it can also be reached indirectly:
`ssh node sudo ceph`, `lxc exec vm -- ceph`, `cephadm shell -- ceph`.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import sys
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

import anyio

from .config import Settings
from .runner import CommandError, CommandResult, Runner, run_command

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import BaseExceptionGroup  # anyio depends on it there

T = TypeVar("T")

MAX_CONCURRENT_COMMANDS = 8


class CephError(Exception):
    """A ceph or microceph command failed. The message is safe to show to the model."""

    def __init__(self, message: str, *, returncode: int | None = None):
        super().__init__(message)
        self.returncode = returncode


def _tail(text: str, lines: int = 8, chars: int = 1500) -> str:
    kept = [ln for ln in text.strip().splitlines() if ln.strip()][-lines:]
    return "\n".join(kept)[-chars:]


async def gather(*calls: Callable[[], Awaitable[Any]]) -> list[Any]:
    """Run zero-argument async callables concurrently; results in call order.

    If any of them fails, the others are cancelled and the first failure is
    raised as itself, not wrapped in the task group's exception group, so
    callers can catch CephError as usual.
    """
    results: list[Any] = [None] * len(calls)

    async def one(i: int, call: Callable[[], Awaitable[Any]]) -> None:
        results[i] = await call()

    try:
        async with anyio.create_task_group() as tg:
            for i, call in enumerate(calls):
                tg.start_soon(one, i, call)
    except BaseExceptionGroup as group:
        failure: BaseException = group
        while isinstance(failure, BaseExceptionGroup):
            failure = failure.exceptions[0]
        raise failure from None
    return results


class _Cli:
    """An external CLI reached through an argv prefix."""

    tool = ""
    candidates: tuple[str, ...] = ()
    fallbacks: tuple[str, ...] = ()
    hint = ""

    def __init__(self, prefix: Sequence[str], settings: Settings, runner: Runner):
        self.settings = settings
        self._runner = runner
        self._prefix: list[str] | None = list(prefix) or None
        self._limiter: anyio.CapacityLimiter | None = None

    def locate(self) -> list[str] | None:
        """The argv prefix, autodetected if it was not configured. None if absent."""
        if self._prefix is None:
            for name in self.candidates:
                path = shutil.which(name)
                if path:
                    self._prefix = [path]
                    break
            else:
                for path in self.fallbacks:
                    if os.access(path, os.X_OK):
                        self._prefix = [path]
                        break
        return self._prefix

    def prefix(self) -> list[str]:
        prefix = self.locate()
        if prefix is None:
            raise CephError(f"{self.tool} command not found. {self.hint}")
        return prefix

    def build_argv(self, args: Sequence[str]) -> list[str]:
        prefix = self.prefix()
        tail = list(args)
        # ssh hands its arguments to a remote shell as one string, so they need
        # quoting there; every other prefix passes argv through untouched.
        if os.path.basename(prefix[0]) == "ssh":
            tail = [shlex.quote(a) for a in tail]
        return prefix + tail

    async def _exec(
        self,
        args: Sequence[str],
        *,
        timeout: float | None,
        input: bytes | None,
        check: bool,
        options: Sequence[str] = (),
    ) -> CommandResult:
        """Run `<tool> <options> <args>`. Error messages name only the args."""
        argv = self.build_argv([*options, *args])
        if self._limiter is None:
            self._limiter = anyio.CapacityLimiter(MAX_CONCURRENT_COMMANDS)
        try:
            async with self._limiter:
                result = await self._runner(
                    argv, timeout=timeout or self.settings.command_timeout, input=input
                )
        except CommandError as exc:
            raise CephError(str(exc)) from None
        if check and result.returncode != 0:
            detail = _tail(result.stderr) or _tail(result.stdout) or "no error output"
            raise CephError(
                f"`{self.tool} {' '.join(args)}` failed (exit {result.returncode}): {detail}",
                returncode=result.returncode,
            )
        return result


class CephClient(_Cli):
    tool = "ceph"
    candidates = ("ceph", "microceph.ceph")
    fallbacks = ("/snap/bin/ceph", "/snap/bin/microceph.ceph")
    hint = (
        "Install ceph-common or MicroCeph on this host, or point --ceph-cmd at a way "
        "to run it, e.g. 'ssh node sudo ceph'."
    )

    def __init__(self, settings: Settings, runner: Runner = run_command):
        super().__init__(settings.ceph_cmd, settings, runner)

    def _options(self, fmt: str | None) -> list[str]:
        s = self.settings
        opts: list[str] = []
        if s.ceph_conf:
            opts += ["--conf", s.ceph_conf]
        if s.ceph_keyring:
            opts += ["--keyring", s.ceph_keyring]
        if s.ceph_name:
            opts += ["--name", s.ceph_name]
        opts += ["--connect-timeout", str(s.connect_timeout)]
        if fmt:
            opts += ["--format", fmt]
        return opts

    async def run(
        self,
        args: Sequence[str],
        *,
        fmt: str | None = None,
        timeout: float | None = None,
        input: bytes | None = None,
        check: bool = True,
    ) -> CommandResult:
        """Run `ceph <args>`. Raises CephError on failure unless check=False."""
        return await self._exec(
            args, timeout=timeout, input=input, check=check, options=self._options(fmt)
        )

    async def json(self, args: Sequence[str], *, timeout: float | None = None) -> Any:
        """Run `ceph <args> --format json` and decode the output."""
        result = await self.run(args, fmt="json", timeout=timeout)
        text = result.stdout.strip()
        if not text:
            return None
        if result.truncated:
            raise CephError(f"`ceph {' '.join(args)}` produced too much output to process")
        try:
            return json.loads(text)
        except ValueError:
            raise CephError(f"`ceph {' '.join(args)}` did not return JSON: {text[:300]}") from None

    async def try_json(
        self, args: Sequence[str], default: T | None = None, *, timeout: float | None = None
    ) -> Any | T | None:
        """Like json(), but returns `default` if the command fails."""
        try:
            return await self.json(args, timeout=timeout)
        except CephError:
            return default


class MicroCephClient(_Cli):
    tool = "microceph"
    candidates = ("microceph",)
    fallbacks = ("/snap/bin/microceph",)
    hint = "This host does not run MicroCeph, or --microceph-cmd needs to be set."

    def __init__(self, settings: Settings, runner: Runner = run_command):
        super().__init__(settings.microceph_cmd, settings, runner)

    @property
    def available(self) -> bool:
        return self.locate() is not None

    async def run(
        self, args: Sequence[str], *, timeout: float | None = None, check: bool = True
    ) -> CommandResult:
        return await self._exec(args, timeout=timeout, input=None, check=check)
