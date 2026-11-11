"""The background recorder: samples the cluster into the history store.

Once per interval it reads a handful of cluster-wide commands (and the metric
exporters, where there are any), stores the figures as time series, and
compares the cluster's state with the previous sample to record what changed.
It only ever reads from the cluster.
"""

from __future__ import annotations

import logging
import os
import socket
import sqlite3
from collections.abc import Awaitable, Callable, Mapping
from functools import partial
from typing import Any, TypeVar

import anyio

from .ceph import CephClient, CephError, gather
from .config import Settings
from .history import Event, Store, database_path
from .http import HttpError
from .metrics import Collection, MetricsHub, rates

log = logging.getLogger("cephmcp.recorder")
T = TypeVar("T")

KIB = 1024
HEALTH_LEVEL = {"HEALTH_OK": 0, "HEALTH_WARN": 1, "HEALTH_ERR": 2}
COMPACT_EVERY = 3600.0
STATE_KEY = "fingerprint"
LATENCIES = (("op_w", "ceph_osd_op_w_latency"), ("op_r", "ceph_osd_op_r_latency"))


class History:
    """Async access to the history store of the cluster the server talks to."""

    def __init__(self, settings: Settings, ceph: CephClient):
        self._settings = settings
        self._ceph = ceph
        self._store: Store | None = None
        self._lock: anyio.Lock | None = None

    async def store(self) -> Store:
        if self._store is not None:
            return self._store
        if self._lock is None:
            self._lock = anyio.Lock()
        async with self._lock:
            if self._store is None:
                fsid = ""
                if self._settings.state_dir:
                    # One file per cluster, so that pointing the server at another
                    # cluster does not mix their histories.
                    fsid = (await self._ceph.run(["fsid"])).stdout.strip()
                path = database_path(self._settings.state_dir, fsid)
                try:
                    self._store = await anyio.to_thread.run_sync(Store, path)
                except (OSError, sqlite3.Error) as exc:
                    raise CephError(f"cannot open the history file {path}: {exc}") from None
        return self._store

    async def call(self, method: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a Store method (bound, e.g. store.read) off the event loop."""
        return await anyio.to_thread.run_sync(partial(method, *args, **kwargs))

    def close(self) -> None:
        if self._store is not None:
            self._store.close()
            self._store = None


# ------------------------------------------------------------------ sampling


def cluster_values(
    status: Mapping[str, Any],
    df_tree: Mapping[str, Any] | None,
    perf: Mapping[str, Any] | None,
    df: Mapping[str, Any] | None,
    pool_stats: list[Mapping[str, Any]] | None,
    *,
    per_osd: bool = True,
) -> dict[str, float]:
    """The figures of one sample, keyed by series name.

    Names are kind/entity/metric: cluster/write_bytes_sec, pool/<name>/stored_bytes,
    osd/<id>/commit_ms, host/<name>/used_pct. The suffix gives the unit.
    """
    pgmap = status.get("pgmap") or {}
    osdmap = status.get("osdmap") or {}
    health = (status.get("health") or {}).get("status")
    clean = sum(
        s.get("count", 0)
        for s in pgmap.get("pgs_by_state") or []
        if s.get("state_name") == "active+clean"
    )
    values: dict[str, float | None] = {
        "cluster/health": HEALTH_LEVEL.get(health),
        "cluster/used_bytes": pgmap.get("bytes_used"),
        "cluster/total_bytes": pgmap.get("bytes_total"),
        "cluster/stored_bytes": pgmap.get("data_bytes"),
        "cluster/objects": pgmap.get("num_objects"),
        "cluster/read_bytes_sec": pgmap.get("read_bytes_sec", 0),
        "cluster/write_bytes_sec": pgmap.get("write_bytes_sec", 0),
        "cluster/read_ops_sec": pgmap.get("read_op_per_sec", 0),
        "cluster/write_ops_sec": pgmap.get("write_op_per_sec", 0),
        "cluster/recovery_bytes_sec": pgmap.get("recovering_bytes_per_sec", 0),
        "cluster/degraded_pct": 100 * pgmap.get("degraded_ratio", 0),
        "cluster/misplaced_pct": 100 * pgmap.get("misplaced_ratio", 0),
        "cluster/pgs_not_clean": (pgmap.get("num_pgs") or 0) - clean,
        "cluster/osds": osdmap.get("num_osds"),
        "cluster/osds_up": osdmap.get("num_up_osds"),
        "cluster/osds_in": osdmap.get("num_in_osds"),
    }
    for pool in (df or {}).get("pools") or []:
        stats, name = pool.get("stats") or {}, pool.get("name")
        values[f"pool/{name}/stored_bytes"] = stats.get("stored")
        values[f"pool/{name}/used_bytes"] = stats.get("bytes_used")
        values[f"pool/{name}/objects"] = stats.get("objects")
        values[f"pool/{name}/max_avail_bytes"] = stats.get("max_avail")
    for pool in pool_stats or []:
        rate, name = pool.get("client_io_rate") or {}, pool.get("pool_name")
        values[f"pool/{name}/read_bytes_sec"] = rate.get("read_bytes_sec", 0)
        values[f"pool/{name}/write_bytes_sec"] = rate.get("write_bytes_sec", 0)
        values[f"pool/{name}/read_ops_sec"] = rate.get("read_op_per_sec", 0)
        values[f"pool/{name}/write_ops_sec"] = rate.get("write_op_per_sec", 0)

    latency = {
        p["id"]: p.get("perf_stats") or {}
        for p in ((perf or {}).get("osdstats") or {}).get("osd_perf_infos") or []
    }
    commits = [float(v.get("commit_latency_ms") or 0) for v in latency.values()]
    if commits:
        values["cluster/commit_ms_avg"] = sum(commits) / len(commits)
        values["cluster/commit_ms_max"] = max(commits)

    nodes = (df_tree or {}).get("nodes") or []
    by_id = {n.get("id"): n for n in nodes}
    for node in nodes:
        if node.get("type") == "host":
            members = [by_id[c] for c in node.get("children") or [] if c in by_id]
            osds = [m for m in members if m.get("type") == "osd"]
            name = node.get("name")
            values[f"host/{name}/used_pct"] = node.get("utilization")
            values[f"host/{name}/osds_up"] = sum(1 for o in osds if o.get("status") == "up")
            host_commits = [
                float(latency[o["id"]].get("commit_latency_ms") or 0)
                for o in osds
                if o["id"] in latency
            ]
            if host_commits:
                values[f"host/{name}/commit_ms_max"] = max(host_commits)
        elif node.get("type") == "osd" and per_osd:
            osd = node["id"]
            values[f"osd/{osd}/used_pct"] = node.get("utilization")
            values[f"osd/{osd}/pgs"] = node.get("pgs")
            if osd in latency:
                values[f"osd/{osd}/commit_ms"] = latency[osd].get("commit_latency_ms")
                values[f"osd/{osd}/apply_ms"] = latency[osd].get("apply_latency_ms")
    return {k: float(v) for k, v in values.items() if isinstance(v, int | float)}


def exporter_values(before: Collection, after: Collection, *, per_osd: bool) -> dict[str, float]:
    """Operation rates and average latencies between two scrapes of the exporters."""
    values: dict[str, float] = {}
    for short, base in LATENCIES:
        sums = {lb: v for _, lb, v in rates(before, after, base + "_sum")}
        total_sum = total_count = 0.0
        for _, labels, count_rate in rates(before, after, base + "_count"):
            if labels not in sums:
                continue
            total_sum += sums[labels]
            total_count += count_rate
            daemon = dict(labels).get("ceph_daemon", "")
            if per_osd and count_rate > 0 and daemon.startswith("osd."):
                osd = daemon[4:]
                values[f"osd/{osd}/{short}_latency_ms"] = 1000 * sums[labels] / count_rate
                values[f"osd/{osd}/{short}_per_sec"] = count_rate
        if sums:
            values[f"cluster/{short}_per_sec"] = total_count
            if total_count > 0:
                values[f"cluster/{short}_latency_ms"] = 1000 * total_sum / total_count
    return values


def fingerprint(
    status: Mapping[str, Any],
    osd_dump: Mapping[str, Any] | None,
    df_tree: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """The parts of the cluster's state whose changes are worth an event."""
    health = status.get("health") or {}
    checks = {
        code: (check.get("summary") or {}).get("message", "")
        for code, check in (health.get("checks") or {}).items()
    }
    nodes = (df_tree or {}).get("nodes") or []
    host_of: dict[int, str] = {}
    for node in nodes:
        if node.get("type") == "host":
            for child in node.get("children") or []:
                host_of[child] = node.get("name")
    weights = {n["id"]: n.get("crush_weight") for n in nodes if n.get("type") == "osd"}
    osds = {
        str(o.get("osd")): {
            "up": bool(o.get("up")),
            "in": bool(o.get("in")),
            "up_from": o.get("up_from"),
            "reweight": o.get("weight"),
            "crush_weight": weights.get(o.get("osd")),
            "host": host_of.get(o.get("osd")),
        }
        for o in (osd_dump or {}).get("osds") or []
    }
    pools = {
        str(p.get("pool_name")): {
            "size": p.get("size"),
            "min_size": p.get("min_size"),
            "pg_num": p.get("pg_num"),
            "crush_rule": p.get("crush_rule"),
            "autoscale": p.get("pg_autoscale_mode"),
        }
        for p in (osd_dump or {}).get("pools") or []
    }
    routine = {"sortbitwise", "recovery_deletes", "purged_snapdirs", "pglog_hardlimit"}
    return {
        "health": {"status": health.get("status"), "checks": checks},
        "osds": osds,
        "pools": pools,
        "flags": sorted(set((osd_dump or {}).get("flags_set") or []) - routine),
        "quorum": sorted(status.get("quorum_names") or []),
        "have_osd_dump": bool(osd_dump),
    }


def _weight(value: Any) -> str:
    return f"{value:g}" if isinstance(value, int | float) else str(value)


def diff(old: Mapping[str, Any], new: Mapping[str, Any], ts: float) -> list[Event]:
    """Events for what differs between two fingerprints."""
    events: list[Event] = []

    def add(kind: str, subject: str, message: str) -> None:
        events.append(Event(ts, kind, subject, message))

    old_health, new_health = old.get("health") or {}, new.get("health") or {}
    if old_health.get("status") != new_health.get("status"):
        add("health", "cluster", f"{old_health.get('status')} -> {new_health.get('status')}")
    old_checks, new_checks = old_health.get("checks") or {}, new_health.get("checks") or {}
    for code in sorted(new_checks.keys() - old_checks.keys()):
        add("health", code, f"health check raised: {new_checks[code]} ({code})")
    for code in sorted(old_checks.keys() - new_checks.keys()):
        add("health", code, f"health check cleared: {code} (was: {old_checks[code]})")

    if old.get("have_osd_dump") and new.get("have_osd_dump"):
        old_osds, new_osds = old.get("osds") or {}, new.get("osds") or {}
        for osd in sorted(new_osds.keys() - old_osds.keys(), key=int):
            add("osd", f"osd.{osd}", f"osd.{osd} added on host {new_osds[osd].get('host')}")
        for osd in sorted(old_osds.keys() - new_osds.keys(), key=int):
            add("osd", f"osd.{osd}", f"osd.{osd} removed from the cluster")
        for osd in sorted(new_osds.keys() & old_osds.keys(), key=int):
            a, b = old_osds[osd], new_osds[osd]
            name = f"osd.{osd}"
            if a.get("up") != b.get("up"):
                add("osd", name, f"{name} went {'up' if b.get('up') else 'down'}")
            elif b.get("up") and a.get("up_from") != b.get("up_from"):
                add("osd", name, f"{name} restarted")
            if a.get("in") != b.get("in"):
                add("osd", name, f"{name} marked {'in' if b.get('in') else 'out'}")
            for key, label in (("crush_weight", "CRUSH weight"), ("reweight", "reweight")):
                if a.get("in") == b.get("in") and a.get(key) != b.get(key):
                    add(
                        "osd",
                        name,
                        f"{name} {label} {_weight(a.get(key))} -> {_weight(b.get(key))}",
                    )
        old_pools, new_pools = old.get("pools") or {}, new.get("pools") or {}
        for pool in sorted(new_pools.keys() - old_pools.keys()):
            add("pool", pool, f"pool {pool} created ({new_pools[pool].get('size')} copies)")
        for pool in sorted(old_pools.keys() - new_pools.keys()):
            add("pool", pool, f"pool {pool} deleted")
        for pool in sorted(new_pools.keys() & old_pools.keys()):
            for key, value in new_pools[pool].items():
                if old_pools[pool].get(key) != value:
                    add("pool", pool, f"pool {pool} {key} {old_pools[pool].get(key)} -> {value}")
        old_flags, new_flags = set(old.get("flags") or []), set(new.get("flags") or [])
        for flag in sorted(new_flags - old_flags):
            add("flag", flag, f"flag {flag} set")
        for flag in sorted(old_flags - new_flags):
            add("flag", flag, f"flag {flag} cleared")

    old_quorum, new_quorum = set(old.get("quorum") or []), set(new.get("quorum") or [])
    for mon in sorted(old_quorum - new_quorum):
        add("mon", f"mon.{mon}", f"mon.{mon} left the quorum")
    for mon in sorted(new_quorum - old_quorum):
        add("mon", f"mon.{mon}", f"mon.{mon} joined the quorum")
    return events


class Recorder:
    def __init__(
        self,
        settings: Settings,
        ceph: CephClient,
        hub: MetricsHub,
        history: History,
        clock: Callable[[], float],
        sleep: Callable[[float], Awaitable[None]] = anyio.sleep,
    ):
        self._settings = settings
        self._ceph = ceph
        self._hub = hub
        self._history = history
        self._clock = clock
        self._sleep = sleep
        self._previous: Collection | None = None
        self._compacted = 0.0
        self.owner = f"{socket.gethostname()}:{os.getpid()}"
        self.samples = 0
        self.last_error = ""
        self.running = False

    async def sample(self) -> dict[str, float]:
        """Take one sample and store it. Returns the values written."""
        ceph = self._ceph
        status, df_tree, perf, df, pool_stats, osd_dump = await gather(
            lambda: ceph.json(["status"]),
            lambda: ceph.try_json(["osd", "df", "tree"], {}),
            lambda: ceph.try_json(["osd", "perf"], {}),
            lambda: ceph.try_json(["df", "detail"], {}),
            lambda: ceph.try_json(["osd", "pool", "stats"], []),
            lambda: ceph.try_json(["osd", "dump"], {}),
        )
        now = self._clock()
        osd_count = (status.get("osdmap") or {}).get("num_osds") or 0
        per_osd = osd_count <= self._settings.history_max_osds
        values = cluster_values(status, df_tree, perf, df, pool_stats, per_osd=per_osd)
        values.update(await self._exporters(per_osd))

        store = await self._history.store()
        state = fingerprint(status, osd_dump, df_tree)
        saved = await self._history.call(store.get_state, STATE_KEY)
        events: list[Event] = []
        if saved is None:
            events.append(Event(now, "recorder", "recorder", "recording started"))
        else:
            gap = now - float(saved.get("at") or now)
            found = diff(saved.get("state") or {}, state, now)
            if found and gap > 3 * self._settings.record_interval:
                # Nobody was watching: the changes happened some time in the gap.
                note = f" (noticed after {_span(gap)} without recording)"
                found = [Event(e.ts, e.kind, e.subject, e.message + note) for e in found]
            events.extend(found)
        await self._history.call(store.write, now, values, events)
        await self._history.call(store.set_state, STATE_KEY, {"at": now, "state": state})
        if now - self._compacted >= COMPACT_EVERY:
            await self._history.call(store.compact, now, self._settings.history_days)
            self._compacted = now
        self.samples += 1
        return values

    async def _exporters(self, per_osd: bool) -> dict[str, float]:
        try:
            found = await self._hub.discover()
            if not found.sources:
                return {}
            current = await self._hub.collect(fresh=True)
        except (CephError, HttpError) as exc:
            log.debug("metric exporters not sampled: %s", exc)
            return {}
        previous, self._previous = self._previous, current
        if previous is None or not current.metrics:
            return {}
        if current.time - previous.time > 5 * self._settings.record_interval:
            return {}
        return exporter_values(previous, current, per_osd=per_osd)

    async def run(self) -> None:
        """Sample forever. Holds the store's recording lease, so that of
        several servers on one cluster only one records."""
        interval = self._settings.record_interval
        self.running = True
        try:
            while True:
                started = self._clock()
                try:
                    store = await self._history.store()
                    mine = await self._history.call(
                        store.acquire, self.owner, started, 2.5 * interval
                    )
                    if mine:
                        await self.sample()
                        self.last_error = ""
                except (CephError, HttpError, sqlite3.Error, OSError) as exc:
                    if str(exc) != self.last_error:
                        log.warning("history sample failed: %s", exc)
                    self.last_error = str(exc)
                await self._sleep(max(1.0, interval - (self._clock() - started)))
        finally:
            self.running = False
            with anyio.CancelScope(shield=True):
                try:
                    store = await self._history.store()
                    await self._history.call(store.release, self.owner)
                except (CephError, sqlite3.Error, OSError):
                    pass


def _span(seconds: float) -> str:
    """90 -> '90s', 5400 -> '1.5h', 200000 -> '2.3d'."""
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds / size:.3g}{unit}"
    return f"{seconds:.0f}s"
