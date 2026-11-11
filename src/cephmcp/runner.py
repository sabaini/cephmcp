"""Run external commands with a timeout and bounded output."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

import anyio
from anyio.abc import ByteReceiveStream

# Hard caps on captured output. `ceph pg dump` on a large cluster is tens of MB;
# anything beyond this is not something a tool should hold in memory.
MAX_STDOUT = 64 * 1024 * 1024
MAX_STDERR = 256 * 1024


class CommandError(Exception):
    """A command could not be run at all (missing binary, timeout)."""


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    duration: float
    truncated: bool = False


# Signature of run_command, so tests can substitute a fake.
Runner = Callable[..., Awaitable[CommandResult]]


async def _drain(stream: ByteReceiveStream | None, buf: bytearray, cap: int) -> bool:
    """Read a stream to EOF, keeping at most `cap` bytes. Returns True if cut."""
    truncated = False
    if stream is None:
        return truncated
    async for chunk in stream:
        room = cap - len(buf)
        if room >= len(chunk):
            buf += chunk
        else:
            buf += chunk[: max(room, 0)]
            truncated = True  # keep reading so the child does not block on a full pipe
    return truncated


async def run_command(
    argv: Sequence[str],
    *,
    timeout: float = 30.0,
    input: bytes | None = None,
) -> CommandResult:
    """Run argv (no shell) and capture its output.

    Raises CommandError if the binary is missing or the timeout expires; a
    non-zero exit status is reported through CommandResult.returncode.
    """
    start = time.monotonic()
    out, err = bytearray(), bytearray()
    cut = [False]

    async def pump_out(stream: ByteReceiveStream | None) -> None:
        cut[0] = await _drain(stream, out, MAX_STDOUT)

    try:
        with anyio.fail_after(timeout):
            process = await anyio.open_process(
                list(argv),
                stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            # Leaving this block on timeout or cancellation kills the child.
            async with process:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(pump_out, process.stdout)
                    tg.start_soon(_drain, process.stderr, err, MAX_STDERR)
                    if input is not None and process.stdin is not None:
                        await process.stdin.send(input)
                        await process.stdin.aclose()
                await process.wait()
            returncode = process.returncode
    except TimeoutError:
        raise CommandError(f"timed out after {timeout:g}s: {' '.join(argv)}") from None
    except FileNotFoundError:
        raise CommandError(f"command not found: {argv[0]}") from None
    except PermissionError:
        raise CommandError(f"permission denied running {argv[0]}") from None

    return CommandResult(
        argv=tuple(argv),
        returncode=-1 if returncode is None else returncode,
        stdout=out.decode("utf-8", "replace"),
        stderr=err.decode("utf-8", "replace"),
        duration=time.monotonic() - start,
        truncated=cut[0],
    )
