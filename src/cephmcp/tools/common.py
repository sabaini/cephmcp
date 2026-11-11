"""Argument checks shared by the tools.

Arguments come from a model and are passed to a command line, so every
free-form value is checked against the shape it is supposed to have. In
particular nothing may start with '-', where it would be read as an option.
"""

from __future__ import annotations

import json
import re
from typing import Any

from ..app import UsageError
from ..ceph import CephClient, gather
from ..topology import Topology, build

_POOL = re.compile(r"^[A-Za-z0-9_.][A-Za-z0-9_.-]{0,127}$")
_DAEMON = re.compile(r"^(osd|mon|mgr|mds|rgw|client)\.[A-Za-z0-9_.-]{1,128}$")
_WHO = re.compile(r"^[a-z]+(\.[A-Za-z0-9_.*-]+)?(/[a-z_]+:[A-Za-z0-9_.-]+)*$")
_OPTION = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_/.:-]{0,127}$")
_WORD = re.compile(r"^[A-Za-z0-9_.][A-Za-z0-9_.:+-]{0,127}$")


# The ceph CLI cuts its argument list at this word, wherever it stands.
RESERVED = frozenset(["injectargs"])


def _check(pattern: re.Pattern[str], value: str, what: str, example: str) -> str:
    if value in RESERVED:
        raise UsageError(f"{value!r} cannot be used as a {what}: the ceph command line reserves it")
    if not pattern.match(value):
        raise UsageError(f"{value!r} is not a valid {what} (expected something like {example})")
    return value


def pool_name(value: str) -> str:
    return _check(_POOL, value, "pool name", "'rbd' or 'default.rgw.log'")


def daemon_name(value: str) -> str:
    return _check(_DAEMON, value, "daemon name", "'osd.3', 'mon.a' or 'mgr.x'")


def config_who(value: str) -> str:
    return _check(_WHO, value, "config target", "'global', 'osd', 'osd.3' or 'osd/class:ssd'")


def config_option(value: str) -> str:
    return _check(_OPTION, value, "option name", "'osd_max_backfills'")


def word(value: str, what: str) -> str:
    return _check(_WORD, value, what, "a plain name")


def osd_id(value: int | str) -> int:
    """Accept 3, '3' or 'osd.3'."""
    text = str(value).removeprefix("osd.")
    if not text.isdigit():
        raise UsageError(f"{value!r} is not an OSD id (expected a number or 'osd.<n>')")
    return int(text)


def short_time(stamp: str | None) -> str | None:
    """'2026-11-11T12:40:39.647000+0000' -> '2026-11-11 12:40:39' (UTC as given)."""
    if not stamp:
        return None
    return stamp.replace("T", " ")[:19]


async def load_topology(ceph: CephClient) -> Topology:
    """The cluster's layout as it is now."""
    df_tree, osd_dump, rules, df = await gather(
        lambda: ceph.json(["osd", "df", "tree"]),
        lambda: ceph.json(["osd", "dump"]),
        lambda: ceph.try_json(["osd", "crush", "rule", "dump"], []),
        lambda: ceph.try_json(["df", "detail"], {}),
    )
    return build(df_tree, osd_dump, rules, df)


async def config_value(ceph: CephClient, who: str, option: str, *, check: bool = True) -> Any:
    """The value of an option as `ceph config get` gives it: a number or a
    boolean where it is one, otherwise the text. Ceph prints the bare value
    whatever format is asked for, so `mclock_scheduler` is not JSON. With
    check=False a failed lookup gives None."""
    result = await ceph.run(["config", "get", who, option], check=check)
    if result.returncode != 0:
        return None
    text = result.stdout.strip()
    try:
        value = json.loads(text)
    except ValueError:
        return text or None
    return value if isinstance(value, int | float | bool) else text
