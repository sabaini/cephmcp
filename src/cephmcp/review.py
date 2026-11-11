"""Cluster review: look for weaknesses that Ceph's own health checks do not
flag, or flag without saying what to do.

Every check is a function from the collected state to findings. A finding
says what is wrong, what it means in practice and how to fix it. Checks are
deliberately conservative: each one describes a condition that an experienced
operator would want to know about, not a matter of taste.
"""

from __future__ import annotations

import re
import statistics
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .render import fmt_bytes
from .topology import Osd, Pool, Topology

SEVERITIES = ("critical", "high", "medium", "low", "info")
GIB = 2**30
DEFAULT_OSD_MEMORY_TARGET = 4 * GIB
MAX_HEADROOM_HOSTS = 40

DATA, AVAILABILITY, CAPACITY, PERFORMANCE, OPERATIONS, CONFIG = (
    "data safety",
    "availability",
    "capacity",
    "performance",
    "operations",
    "configuration",
)


@dataclass
class Finding:
    severity: str
    area: str
    code: str
    finding: str
    detail: str = ""
    fix: str = ""
    health_codes: tuple[str, ...] = ()
    """Ceph health checks that report the same condition."""

    def as_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "area": self.area,
            "code": self.code,
            "finding": self.finding,
            "detail": self.detail or None,
            "fix": self.fix or None,
        }


@dataclass
class Inputs:
    topology: Topology
    status: Mapping[str, Any] = field(default_factory=dict)
    health: Mapping[str, Any] = field(default_factory=dict)
    config: list[Mapping[str, Any]] = field(default_factory=list)
    versions: Mapping[str, Any] = field(default_factory=dict)
    osd_metadata: list[Mapping[str, Any]] = field(default_factory=list)
    mon_metadata: list[Mapping[str, Any]] = field(default_factory=list)
    new_crashes: list[Mapping[str, Any]] = field(default_factory=list)
    balancer: Mapping[str, Any] = field(default_factory=dict)
    autoscale: list[Mapping[str, Any]] = field(default_factory=list)
    osd_perf: Mapping[str, Any] = field(default_factory=dict)
    fs: Mapping[str, Any] = field(default_factory=dict)
    osd_memory_target: int = DEFAULT_OSD_MEMORY_TARGET

    def option(self, name: str, sections: Iterable[str] = ("global", "mon", "osd", "mgr")) -> Any:
        """The value of an option in the configuration database, if it is set
        for one of the sections (not for a single daemon or a mask)."""
        wanted = set(sections)
        for entry in self.config:
            unmasked = not entry.get("mask")
            if unmasked and entry.get("name") == name and entry.get("section") in wanted:
                return entry.get("value")
        return None


def _names(items: Iterable[Any], limit: int = 8) -> str:
    items = [str(i) for i in items]
    if len(items) <= limit:
        return ", ".join(items)
    return ", ".join(items[:limit]) + f" and {len(items) - limit} more"


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "on"}


# ------------------------------------------------------------------ pools


def pool_redundancy(inp: Inputs) -> Iterable[Finding]:
    groups: dict[str, list[Pool]] = {}
    for pool in inp.topology.pools.values():
        if pool.erasure:
            if pool.k is None or pool.m is None:
                continue
            if pool.min_size <= pool.k:
                groups.setdefault("ec_min_k", []).append(pool)
            elif pool.min_size >= pool.size:
                groups.setdefault("min_eq_size", []).append(pool)
            if pool.m == 1:
                groups.setdefault("ec_m1", []).append(pool)
        elif pool.size <= 1:
            groups.setdefault("size1", []).append(pool)
        elif pool.min_size <= 1:
            groups.setdefault("min1_size2" if pool.size == 2 else "min1", []).append(pool)
        elif pool.size == 2:
            groups.setdefault("size2", []).append(pool)
        elif pool.min_size >= pool.size:
            groups.setdefault("min_eq_size", []).append(pool)

    def listing(pools: list[Pool]) -> str:
        return _names(f"{p.name} ({p.redundancy}, min_size {p.min_size})" for p in pools)

    if pools := groups.get("size1"):
        yield Finding(
            "critical",
            DATA,
            "POOL_NO_REDUNDANCY",
            f"{_plural(len(pools), 'pool')} keep a single copy of the data",
            f"{listing(pools)}. Any disk failure loses data.",
            "Raise the replica count: `ceph osd pool set <pool> size 3`, then min_size 2.",
            ("POOL_NO_REDUNDANCY",),
        )
    if pools := groups.get("min1_size2"):
        yield Finding(
            "high",
            DATA,
            "POOL_SIZE2_MIN1",
            f"{_plural(len(pools), 'pool')} keep 2 copies and accept writes with only 1",
            f"{listing(pools)}. With one OSD down, new writes exist in a single place; if "
            "that OSD fails or the first one returns with stale data, acknowledged writes "
            "are lost or PGs go inconsistent. Two copies also cannot tell which one is right.",
            "Use 3 copies with min_size 2: `ceph osd pool set <pool> size 3` and "
            "`ceph osd pool set <pool> min_size 2`. This needs a third failure domain and "
            "50% more raw space.",
        )
    if pools := groups.get("min1"):
        yield Finding(
            "high",
            DATA,
            "POOL_MIN_SIZE_1",
            f"{_plural(len(pools), 'pool')} accept writes with a single copy left",
            f"{listing(pools)}. During a double failure, writes land on one OSD only and "
            "are lost if it fails before recovery.",
            "Set min_size to 2: `ceph osd pool set <pool> min_size 2`.",
        )
    if pools := groups.get("ec_min_k"):
        yield Finding(
            "high",
            DATA,
            "EC_MIN_SIZE_K",
            f"{_plural(len(pools), 'erasure-coded pool')} accept writes with no parity left",
            f"{listing(pools)}. min_size equals k, so writes continue when every spare "
            "shard is gone; one more failure then loses them.",
            "Set min_size to k+1: `ceph osd pool set <pool> min_size <k+1>`.",
        )
    if pools := groups.get("size2"):
        yield Finding(
            "medium",
            DATA,
            "POOL_SIZE_2",
            f"{_plural(len(pools), 'pool')} keep only 2 copies",
            f"{listing(pools)}. One failure leaves no redundancy, and I/O stops until "
            "recovery because min_size is 2.",
            "Use 3 copies where the data matters: `ceph osd pool set <pool> size 3`.",
        )
    if pools := groups.get("ec_m1"):
        yield Finding(
            "medium",
            DATA,
            "EC_SINGLE_PARITY",
            f"{_plural(len(pools), 'erasure-coded pool')} tolerate a single failure only",
            f"{listing(pools)}. With m=1 a second failure during recovery loses data.",
            "Erasure-code profiles cannot be changed in place: create a pool with m>=2 "
            "and migrate.",
        )
    if pools := groups.get("min_eq_size"):
        yield Finding(
            "medium",
            AVAILABILITY,
            "POOL_MIN_SIZE_EQUALS_SIZE",
            f"{_plural(len(pools), 'pool')} stop serving I/O when any one OSD is down",
            f"{listing(pools)}. min_size equals size, so every copy must be up.",
            "Lower min_size by one, or add redundancy.",
        )


def pool_placement(inp: Inputs) -> Iterable[Finding]:
    topo = inp.topology
    hosts = topo.hosts()
    same_host: dict[str, list[str]] = {}
    cannot_place: list[str] = []
    no_spare: list[str] = []
    mixed: dict[str, list[str]] = {}
    for pool in topo.pools.values():
        rule = topo.rule_of(pool)
        if rule is None:
            continue
        domains = topo.domains(rule)
        if rule.failure_domain == "osd" and len(hosts) > 1:
            same_host.setdefault(rule.name, []).append(pool.name)
        if len(domains) < pool.size:
            cannot_place.append(
                f"{pool.name} needs {pool.size} {rule.failure_domain}s, {len(domains)} available"
            )
        elif len(domains) == pool.size and rule.failure_domain != "osd":
            no_spare.append(pool.name)
        classes = {o.device_class for o in topo.eligible(rule) if o.device_class}
        if not rule.device_class and len(classes) > 1:
            mixed.setdefault(f"{rule.name} ({'+'.join(sorted(classes))})", []).append(pool.name)

    for rule_name, pools in same_host.items():
        yield Finding(
            "high",
            DATA,
            "CRUSH_OSD_FAILURE_DOMAIN",
            f"CRUSH rule '{rule_name}' may put all copies of a PG on one host",
            f"Its failure domain is 'osd', and the cluster has {len(hosts)} hosts. Pools "
            f"using it: {_names(pools)}. Losing one host can make their data unavailable "
            "or lose it.",
            "Move the pools to a rule that separates hosts: "
            "`ceph osd crush rule create-replicated by-host default host`, then "
            "`ceph osd pool set <pool> crush_rule by-host`. This moves data.",
        )
    if len(hosts) == 1 and topo.pools:
        yield Finding(
            "medium",
            AVAILABILITY,
            "SINGLE_HOST",
            "All OSDs are on one host",
            f"Host {next(iter(hosts))} holds every copy of the data: the cluster does not "
            "survive the loss of that host, and is offline while it reboots.",
            "Add hosts and switch the pools to a host-level CRUSH rule if this cluster "
            "is meant to be highly available.",
        )
    if cannot_place:
        yield Finding(
            "high",
            DATA,
            "POOL_CANNOT_PLACE",
            f"{_plural(len(cannot_place), 'pool')} cannot place all copies",
            f"{_names(cannot_place)}. Their PGs stay undersized or inactive.",
            "Add failure domains (hosts), or use a pool layout that fits the cluster.",
        )
    if no_spare:
        yield Finding(
            "low",
            AVAILABILITY,
            "NO_SPARE_FAILURE_DOMAIN",
            "No spare host to recover onto",
            f"{_plural(len(no_spare), 'pool')} ({_names(no_spare)}) use every failure "
            "domain there is. After losing one, data stays degraded until it is replaced; "
            "the cluster cannot heal itself.",
            "Normal for three-node clusters. One more host than the replica count gives "
            "self-healing.",
        )
    for rule_name, pools in mixed.items():
        yield Finding(
            "medium",
            PERFORMANCE,
            "MIXED_DEVICE_CLASSES",
            f"CRUSH rule {rule_name} mixes device classes",
            f"Pools {_names(pools)} spread over disks of different speed, so latency "
            "follows the slowest copy.",
            "Create per-class rules (`ceph osd crush rule create-replicated fast default "
            "host ssd`) and assign each pool to one.",
        )


def pool_housekeeping(inp: Inputs) -> Iterable[Finding]:
    topo = inp.topology
    untagged = [p.name for p in topo.pools.values() if not p.applications]
    if untagged:
        yield Finding(
            "low",
            OPERATIONS,
            "POOL_NO_APPLICATION",
            f"{_plural(len(untagged), 'pool')} without an application tag",
            _names(untagged),
            "`ceph osd pool application enable <pool> rbd|cephfs|rgw`.",
            ("POOL_APP_NOT_ENABLED",),
        )
    near_quota = []
    for pool in topo.pools.values():
        if pool.quota_bytes and pool.stored >= 0.8 * pool.quota_bytes:
            near_quota.append(f"{pool.name} at {100 * pool.stored / pool.quota_bytes:.0f}%")
        elif pool.quota_objects and pool.objects >= 0.8 * pool.quota_objects:
            near_quota.append(f"{pool.name} at {100 * pool.objects / pool.quota_objects:.0f}%")
    if near_quota:
        yield Finding(
            "medium",
            CAPACITY,
            "POOL_NEAR_QUOTA",
            f"{_plural(len(near_quota), 'pool')} close to the quota",
            f"{_names(near_quota)} of the quota. Writes fail when it is reached.",
            "Raise the quota (`ceph osd pool set-quota`) or free space.",
        )
    wrong_pgs = [
        f"{a.get('pool_name')} has {a.get('pg_num_target')}, should have {a.get('pg_num_final')}"
        for a in inp.autoscale
        if a.get("would_adjust") and a.get("pg_autoscale_mode") != "on"
    ]
    if wrong_pgs:
        yield Finding(
            "low",
            PERFORMANCE,
            "PG_NUM_SUBOPTIMAL",
            f"{_plural(len(wrong_pgs), 'pool')} with a PG count the autoscaler would change",
            _names(wrong_pgs),
            "`ceph osd pool set <pool> pg_autoscale_mode on`, or set pg_num by hand.",
            ("POOL_TOO_FEW_PGS", "POOL_TOO_MANY_PGS"),
        )


# ------------------------------------------------------------------ flags

_FLAG_NOTES = {
    "pauserd": ("critical", "all reads are paused"),
    "pausewr": ("critical", "all writes are paused"),
    "norecover": ("high", "recovery is switched off: degraded data is not repaired"),
    "nobackfill": ("high", "backfill is switched off: data does not move to where it belongs"),
    "norebalance": ("medium", "rebalancing is switched off"),
    "nodown": ("high", "failed OSDs are not marked down, so I/O to them hangs"),
    "noup": ("high", "OSDs that start are not marked up"),
    "noin": ("medium", "new or returning OSDs are not taken into use"),
    "noout": ("medium", "stopped OSDs are never marked out, so lost copies are not re-created"),
    "noscrub": ("medium", "scrubbing is off: inconsistencies go unnoticed"),
    "nodeep-scrub": ("medium", "deep scrubbing is off: silent corruption goes unnoticed"),
    "notieragent": ("low", "the cache tiering agent is paused"),
}


def flags(inp: Inputs) -> Iterable[Finding]:
    topo = inp.topology
    present = sorted(f for f in topo.flags if f in _FLAG_NOTES)
    if present:
        worst = min(present, key=lambda f: SEVERITIES.index(_FLAG_NOTES[f][0]))
        yield Finding(
            _FLAG_NOTES[worst][0],
            OPERATIONS,
            "FLAGS_SET",
            f"Cluster flags are set: {', '.join(present)}",
            "; ".join(f"{f}: {_FLAG_NOTES[f][1]}" for f in present)
            + ". Flags are meant for the duration of a maintenance and are easily forgotten.",
            "If no maintenance is under way, clear them: `ceph osd unset <flag>` "
            "(cluster_flags in admin mode).",
            ("OSDMAP_FLAGS",),
        )
    if topo.scoped_flags:
        listing = _names(f"{t}: {','.join(f)}" for t, f in sorted(topo.scoped_flags.items()))
        yield Finding(
            "medium",
            OPERATIONS,
            "SCOPED_FLAGS_SET",
            f"Flags are set on {_plural(len(topo.scoped_flags), 'OSD or host')}",
            f"{listing}. Their OSDs are exempt from the normal handling of failures.",
            "If the maintenance is over: `ceph osd unset-group <flag> <target>`.",
            ("OSD_FLAGS",),
        )


# ------------------------------------------------------------------- OSDs


def osd_state(inp: Inputs) -> Iterable[Finding]:
    osds = inp.topology.osds.values()
    idle = [o for o in osds if o.up and not o.in_]
    if idle:
        yield Finding(
            "low",
            OPERATIONS,
            "OSD_OUT_BUT_UP",
            f"{_plural(len(idle), 'OSD')} running but marked out",
            f"{_names(f'osd.{o.id}' for o in idle)} hold no data. Either a drain was never "
            "followed by the removal, or they were meant to come back in.",
            "`ceph osd in <id>` to use them, or remove them.",
        )
    overridden = [o for o in osds if o.in_ and o.reweight < 0.999]
    if overridden:
        yield Finding(
            "low",
            CAPACITY,
            "OSD_REWEIGHT_OVERRIDE",
            f"{_plural(len(overridden), 'OSD')} with a reduced override weight",
            f"{_names(f'osd.{o.id} ({o.reweight:g})' for o in overridden)}. Leftovers of "
            "manual balancing reduce usable capacity and fight the balancer.",
            "With the upmap balancer active, reset them: `ceph osd reweight <id> 1`.",
        )


def osd_devices(inp: Inputs) -> Iterable[Finding]:
    by_device: dict[tuple[str, str], list[int]] = {}
    for meta in inp.osd_metadata:
        device = meta.get("bluestore_bdev_devices") or meta.get("devices")
        host = meta.get("hostname")
        if device and host and meta.get("id") is not None:
            by_device.setdefault((str(host), str(device)), []).append(int(meta["id"]))
    shared = {key: ids for key, ids in by_device.items() if len(ids) > 1}
    if not shared:
        return
    topo = inp.topology
    osd_domain = any(
        (rule := topo.rule_of(p)) is not None and rule.failure_domain == "osd"
        for p in topo.pools.values()
    )
    listing = _names(
        f"{host}:{device} carries {_names((f'osd.{i}' for i in sorted(ids)), 6)}"
        for (host, device), ids in sorted(shared.items())
    )
    yield Finding(
        "high" if osd_domain else "low",
        DATA if osd_domain else OPERATIONS,
        "OSDS_SHARE_DEVICE",
        f"Several OSDs share one physical device on {_plural(len(shared), 'host')}",
        f"{listing}. One device failure takes all of them down at once."
        + (
            " Pools with failure domain 'osd' can have all their copies on that device."
            if osd_domain
            else " Deliberate for split NVMe drives; for loop files or partitions of a "
            "system disk it is a test setup."
        ),
        "Give each OSD its own device, or make sure every pool separates hosts.",
    )


def osd_memory(inp: Inputs) -> Iterable[Finding]:
    def target(osd_id: int, host: str, device_class: str | None) -> int:
        best, rank = inp.osd_memory_target, -1
        for entry in inp.config:
            if entry.get("name") != "osd_memory_target":
                continue
            section, mask = entry.get("section"), entry.get("mask") or ""
            if section == f"osd.{osd_id}":
                r = 4
            elif section == "osd" and mask == f"host:{host}":
                r = 3
            elif section == "osd" and device_class and mask == f"class:{device_class}":
                r = 2
            elif section in ("osd", "global") and not mask:
                r = 1 if section == "osd" else 0
            else:
                continue
            if r > rank:
                try:
                    best, rank = int(entry.get("value")), r  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
        return best

    classes = {o.id: o.device_class for o in inp.topology.osds.values()}
    wanted: dict[str, int] = {}
    total: dict[str, int] = {}
    count: dict[str, int] = {}
    low: list[str] = []
    for meta in inp.osd_metadata:
        host, osd_id = meta.get("hostname"), meta.get("id")
        try:
            memory = int(meta.get("mem_total_kb") or 0) * 1024
        except (TypeError, ValueError):
            continue
        if not host or osd_id is None or not memory:
            continue
        t = target(int(osd_id), str(host), classes.get(int(osd_id)))
        if t < 2 * GIB:
            low.append(f"osd.{osd_id} ({fmt_bytes(t)})")
        wanted[host] = wanted.get(host, 0) + t
        total[host] = memory
        count[host] = count.get(host, 0) + 1
    over = sorted(
        ((wanted[h] / total[h], h) for h in wanted if wanted[h] > 0.8 * total[h]), reverse=True
    )
    if over:
        worst = over[0][0]
        listing = _names(
            f"{h}: {count[h]} OSDs x target = {fmt_bytes(wanted[h])} of {fmt_bytes(total[h])} RAM"
            for _, h in over
        )
        yield Finding(
            "medium" if worst > 1.0 else "low",
            PERFORMANCE,
            "OSD_MEMORY_OVERCOMMIT",
            f"OSD memory targets exceed what {_plural(len(over), 'host')} can give",
            f"{listing}. OSDs grow towards osd_memory_target as data and load grow, and "
            "mons, mgrs and the kernel need memory too; the result is swapping or OSDs "
            "killed by the kernel under recovery load.",
            "Lower osd_memory_target for these hosts (`ceph config set osd/host:<host> "
            "osd_memory_target <bytes>`; not below 2 GiB), or add RAM.",
        )
    if low:
        yield Finding(
            "medium",
            PERFORMANCE,
            "OSD_MEMORY_LOW",
            f"{_plural(len(low), 'OSD')} with a memory target under 2 GiB",
            f"{_names(low)}. BlueStore caches too little below that and gets slow.",
            "Set osd_memory_target to at least 2 GiB, better 4 GiB.",
        )


def slow_osds(inp: Inputs) -> Iterable[Finding]:
    infos = (inp.osd_perf.get("osdstats") or {}).get("osd_perf_infos") or []
    latency = {
        int(i["id"]): float((i.get("perf_stats") or {}).get("commit_latency_ms") or 0)
        for i in infos
        if "id" in i
    }
    by_class: dict[str | None, list[Osd]] = {}
    for osd in inp.topology.osds.values():
        if osd.id in latency and osd.up:
            by_class.setdefault(osd.device_class, []).append(osd)
    outliers = []
    for peers in by_class.values():
        if len(peers) < 3:
            continue
        median = statistics.median(latency[o.id] for o in peers)
        for osd in peers:
            value = latency[osd.id]
            if value >= 50 and value > 3 * max(median, 1.0):
                outliers.append((value, osd, median))
    if outliers:
        outliers.sort(key=lambda r: -r[0])
        listing = _names(
            f"osd.{o.id} on {o.host}: {v:g} ms (peers: {m:g} ms)" for v, o, m in outliers
        )
        yield Finding(
            "medium",
            PERFORMANCE,
            "SLOW_OSD",
            f"{_plural(len(outliers), 'OSD')} much slower than its peers",
            f"Commit latency: {listing}. A disk that is failing or overloaded slows every "
            "PG it takes part in.",
            "Check the disk (`ceph device ls`, SMART, kernel log) and daemon_ops / "
            "daemon_perf for that OSD; mark it out if the disk is failing.",
        )


# --------------------------------------------------------------- capacity


def capacity(inp: Inputs) -> Iterable[Finding]:
    topo = inp.topology
    osds = [o for o in topo.osds.values() if o.in_ and o.size]
    if not osds:
        return
    nearfull = 100 * topo.nearfull_ratio
    # At the limit itself Ceph raises its own health warning; this is the stretch before.
    filling = sorted(
        (o for o in osds if nearfull - 10 <= o.used_pct < nearfull), key=lambda o: -o.used_pct
    )
    if filling:
        yield Finding(
            "medium",
            CAPACITY,
            "OSD_FILLING",
            f"{_plural(len(filling), 'OSD')} close to the nearfull limit ({nearfull:.0f}%)",
            f"{_names(f'osd.{o.id} {o.used_pct:.0f}%' for o in filling)}. At "
            f"{100 * topo.backfillfull_ratio:.0f}% recovery onto an OSD stops, at "
            f"{100 * topo.full_ratio:.0f}% the cluster stops accepting writes.",
            "Add capacity or delete data. capacity_forecast estimates when the limits "
            "are reached; simulate tells what adding disks changes.",
        )
    total, used = sum(o.size for o in osds), sum(o.used for o in osds)
    if total and not filling and 0.70 <= used / total < topo.nearfull_ratio:
        yield Finding(
            "medium",
            CAPACITY,
            "CLUSTER_FILLING",
            f"Raw capacity is {100 * used / total:.0f}% used",
            "Above about 70% there is little room left to re-create the data of a failed "
            "host or disk.",
            "Plan capacity now. capacity_forecast estimates the date the limits are hit.",
        )
    by_class: dict[str | None, list[Osd]] = {}
    for osd in osds:
        by_class.setdefault(osd.device_class, []).append(osd)
    for device_class, peers in by_class.items():
        if len(peers) < 2:
            continue
        low, high = min(peers, key=lambda o: o.used_pct), max(peers, key=lambda o: o.used_pct)
        if high.used_pct >= 50 and high.used_pct - low.used_pct >= 20:
            yield Finding(
                "medium",
                CAPACITY,
                "UNBALANCED",
                f"Unevenly filled {device_class or ''} OSDs".replace("  ", " "),
                f"osd.{high.id} is {high.used_pct:.0f}% full, osd.{low.id} {low.used_pct:.0f}%. "
                "The fullest OSD decides when the cluster is full, so space is wasted.",
                "Check `ceph balancer status`; enable the upmap balancer if it is off.",
            )
    if inp.balancer and (
        not inp.balancer.get("active") or inp.balancer.get("mode") in (None, "none")
    ):
        yield Finding(
            "low",
            CAPACITY,
            "BALANCER_OFF",
            "The balancer is switched off",
            "Nothing evens out how full the OSDs are.",
            "`ceph balancer mode upmap` and `ceph balancer on`.",
        )


def headroom(inp: Inputs) -> Iterable[Finding]:
    """Could the cluster re-create the data of its fullest host, or OSD?"""
    topo = inp.topology
    hosts = topo.hosts()
    before = topo.expected_bytes()
    limit = topo.backfillfull_ratio

    def worst_after(gone: list[Osd]) -> tuple[float, Osd] | None:
        ids = {o.id for o in gone}
        after = topo.without(ids).expected_bytes()
        worst: tuple[float, Osd] | None = None
        for osd in topo.osds.values():
            if osd.id in ids or not osd.in_ or not osd.size:
                continue
            projected = (osd.used + max(0.0, after[osd.id] - before[osd.id])) / osd.size
            if worst is None or projected > worst[0]:
                worst = (projected, osd)
        return worst

    # Only where the cluster would actually re-replicate: there must be a
    # failure domain left to take the data.
    def can_heal(gone: list[Osd]) -> bool:
        ids = {o.id for o in gone}
        return all(
            len(topo.domains(topo.rule_of(p), ids)) >= p.size
            for p in topo.pools.values()
            if p.raw_used
        )

    # Candidates: every host, and the fullest OSD of every host. Capped, so
    # that the check stays cheap on very large clusters.
    by_used = sorted(hosts.items(), key=lambda kv: -sum(o.used for o in kv[1]))[:MAX_HEADROOM_HOSTS]
    cases: list[tuple[str, list[Osd]]] = []
    for name, osds in by_used:
        if len(hosts) > 1:
            cases.append((f"host {name}", osds))
        active = [o for o in osds if o.in_]
        if active:
            fullest = max(active, key=lambda o: o.used)
            cases.append((f"osd.{fullest.id}", [fullest]))
    stuck = []
    for label, gone in cases:
        if not any(o.in_ and o.used for o in gone) or not can_heal(gone):
            continue
        worst = worst_after(gone)
        if worst and worst[0] >= limit:
            stuck.append((worst[0], label, worst[1]))
    if stuck:
        stuck.sort(key=lambda r: -r[0])
        projected, label, osd = stuck[0]
        single_osd = any(lb.startswith("osd.") for _, lb, _ in stuck)
        yield Finding(
            "high" if single_osd else "medium",
            CAPACITY,
            "NO_HEADROOM_FOR_FAILURE",
            "Not enough free space to recover from a failure",
            f"Losing {label} would fill osd.{osd.id} to about {100 * projected:.0f}%, past "
            f"the backfillfull limit ({100 * limit:.0f}%): recovery would stall and leave "
            "data degraded. "
            + (
                "Even a single OSD failing would do this."
                if single_osd
                else "A single OSD failing is still covered."
            ),
            "Add capacity, or free space. `simulate` shows the outcome of a specific failure.",
        )


# ---------------------------------------------------------------- daemons


def monitors(inp: Inputs) -> Iterable[Finding]:
    monmap = inp.status.get("monmap") or {}
    count = monmap.get("num_mons") or len(inp.mon_metadata)
    hosts = len(inp.topology.hosts())
    if not count:
        return
    if count == 1:
        yield Finding(
            "high" if hosts >= 3 else "medium",
            AVAILABILITY,
            "SINGLE_MON",
            "There is only one monitor",
            "The whole cluster stops when it is down, and losing its disk loses the "
            "cluster maps." + (f" The cluster has {hosts} hosts." if hosts >= 3 else ""),
            "Run 3 monitors on different hosts.",
        )
    elif count % 2 == 0:
        yield Finding(
            "medium",
            AVAILABILITY,
            "EVEN_MON_COUNT",
            f"There are {count} monitors",
            f"A quorum needs {count // 2 + 1} of them, so {count} monitors tolerate no more "
            f"failures than {count - 1} would, with more that can fail.",
            f"Run {count + 1} or {count - 1} monitors.",
        )
    by_host: dict[str, list[str]] = {}
    for mon in inp.mon_metadata:
        if mon.get("hostname"):
            by_host.setdefault(str(mon["hostname"]), []).append(str(mon.get("name")))
    crowded = {h: names for h, names in by_host.items() if len(names) > 1}
    if crowded and len(by_host) >= 1 and count > 1:
        yield Finding(
            "medium",
            AVAILABILITY,
            "MONS_SHARE_HOST",
            "Several monitors run on the same host",
            _names(f"{h}: {', '.join(n)}" for h, n in crowded.items())
            + ". That host failing can cost the quorum.",
            "Spread the monitors over different hosts.",
        )


def managers(inp: Inputs) -> Iterable[Finding]:
    mgrmap = inp.status.get("mgrmap") or {}
    if mgrmap and mgrmap.get("available") and not mgrmap.get("num_standbys"):
        yield Finding(
            "medium" if len(inp.topology.hosts()) > 1 else "low",
            AVAILABILITY,
            "NO_STANDBY_MGR",
            "There is no standby manager",
            "If the active mgr fails, metrics, the dashboard, the orchestrator and the "
            "balancer stop until someone restarts it. Client I/O continues.",
            "Run a second mgr on another host.",
        )
    ranks = [m for m in inp.fs.get("mdsmap") or [] if m.get("state") == "active"]
    standbys = [m for m in inp.fs.get("mdsmap") or [] if "standby" in str(m.get("state"))]
    if ranks and not standbys:
        yield Finding(
            "medium",
            AVAILABILITY,
            "NO_STANDBY_MDS",
            "CephFS has no standby metadata server",
            f"{_plural(len(ranks), 'active MDS')} and none in reserve: the file system "
            "hangs when one fails.",
            "Run at least one standby MDS.",
            ("MDS_INSUFFICIENT_STANDBY",),
        )


def versions(inp: Inputs) -> Iterable[Finding]:
    overall = inp.versions.get("overall") or {}
    if len(overall) <= 1:
        return

    def short(version: str) -> str:
        m = re.search(r"ceph version (\S+)", version)
        return m.group(1) if m else version[:40]

    parts = []
    for kind, counts in inp.versions.items():
        if kind != "overall" and len(counts) > 1:
            parts.append(f"{kind}: " + ", ".join(f"{n}x {short(v)}" for v, n in counts.items()))
    yield Finding(
        "medium",
        OPERATIONS,
        "MIXED_VERSIONS",
        f"Daemons run {len(overall)} different Ceph versions",
        (_names(parts) or ", ".join(f"{n}x {short(v)}" for v, n in overall.items()))
        + ". Fine during an upgrade; left like this, new features stay disabled and "
        "bugs fixed in the newer version remain.",
        "Finish the upgrade (`ceph orch upgrade status` on cephadm; `snap refresh` "
        "node by node on MicroCeph).",
        ("DAEMON_OLD_VERSION",),
    )


def crashes(inp: Inputs) -> Iterable[Finding]:
    if not inp.new_crashes:
        return
    daemons: dict[str, int] = {}
    for crash in inp.new_crashes:
        name = str(crash.get("entity_name", "?"))
        daemons[name] = daemons.get(name, 0) + 1
    latest = max(str(c.get("timestamp", "")) for c in inp.new_crashes)
    repeat = [d for d, n in daemons.items() if n > 1]
    yield Finding(
        "high" if repeat else "medium",
        OPERATIONS,
        "NEW_CRASHES",
        f"{_plural(len(inp.new_crashes), 'daemon crash')} nobody has looked at",
        f"{_names(f'{d} ({n}x)' for d, n in sorted(daemons.items()))}; latest "
        f"{latest[:19].replace('T', ' ')}."
        + (f" Crashing repeatedly: {_names(repeat)}." if repeat else ""),
        "Read them with crash_list (crash_id gives the backtrace), then acknowledge "
        "with `ceph crash archive-all`.",
        ("RECENT_CRASH", "RECENT_MGR_MODULE_CRASH"),
    )


# ---------------------------------------------------------- configuration


def configuration(inp: Inputs) -> Iterable[Finding]:
    for name in ("auth_cluster_required", "auth_service_required", "auth_client_required"):
        if str(inp.option(name) or "").strip().lower() == "none":
            yield Finding(
                "critical",
                CONFIG,
                "AUTH_DISABLED",
                f"Authentication is switched off ({name} = none)",
                "Anyone who can reach the cluster network can read and change everything.",
                f"`ceph config rm global {name}` to return to cephx.",
            )
            break
    if _truthy(inp.option("mon_allow_pool_size_one")):
        yield Finding(
            "medium",
            CONFIG,
            "POOL_SIZE_ONE_ALLOWED",
            "Pools without redundancy are permitted (mon_allow_pool_size_one)",
            "The guard against setting a pool to one copy is off.",
            "`ceph config rm global mon_allow_pool_size_one`.",
        )
    if _truthy(inp.option("mon_allow_pool_delete")):
        yield Finding(
            "low",
            CONFIG,
            "POOL_DELETE_ALLOWED",
            "Pool deletion is enabled (mon_allow_pool_delete)",
            "One command deletes a pool with all its data. The option is meant to be "
            "switched on only for the moment a pool is removed.",
            "`ceph config rm mon mon_allow_pool_delete` (or wherever it is set).",
        )
    size = inp.option("osd_pool_default_size")
    if size is not None and str(size).isdigit() and int(size) < 3:
        yield Finding(
            "medium",
            CONFIG,
            "DEFAULT_POOL_SIZE_LOW",
            f"New pools get {size} {'copy' if int(size) == 1 else 'copies'} by default",
            f"osd_pool_default_size is {size}. Pools created by services (RGW, CephFS, "
            "the mgr) inherit it without anyone choosing.",
            "`ceph config set global osd_pool_default_size 3` once there are enough hosts.",
        )
    if str(inp.option("osd_pool_default_min_size") or "") == "1":
        yield Finding(
            "medium",
            CONFIG,
            "DEFAULT_MIN_SIZE_1",
            "New pools accept writes with a single copy by default",
            "osd_pool_default_min_size is 1.",
            "`ceph config rm global osd_pool_default_min_size`.",
        )
    interval = inp.option("mon_osd_down_out_interval")
    if interval is not None and str(interval).strip() in ("0", "0.0"):
        yield Finding(
            "medium",
            CONFIG,
            "NO_AUTO_OUT",
            "Failed OSDs are never marked out automatically",
            "mon_osd_down_out_interval is 0, so the data of a dead OSD is not re-created "
            "until someone marks it out by hand.",
            "`ceph config rm mon mon_osd_down_out_interval` (default: 10 minutes).",
        )
    profile = inp.option("osd_mclock_profile")
    if profile and str(profile) == "high_recovery_ops":
        yield Finding(
            "low",
            CONFIG,
            "RECOVERY_PRIORITISED",
            "Recovery is prioritised over client I/O (osd_mclock_profile)",
            "The high_recovery_ops profile is meant for the duration of a recovery.",
            "`ceph config rm osd osd_mclock_profile` to return to the balanced default.",
        )
    noisy = []
    per_daemon = []
    for entry in inp.config:
        name, section = str(entry.get("name", "")), str(entry.get("section", ""))
        if name.startswith("debug_"):
            level = re.match(r"\d+", str(entry.get("value", "")))
            if level and int(level.group()) >= 10:
                noisy.append(f"{section} {name}={entry.get('value')}")
        if re.fullmatch(r"(osd|mon|mgr|mds)\.[^/]+", section) and not name.startswith("mgr/"):
            per_daemon.append(f"{section} {name}")
    if noisy:
        yield Finding(
            "low",
            CONFIG,
            "DEBUG_LOGGING",
            f"Verbose debug logging is on ({_plural(len(noisy), 'setting')})",
            f"{_names(noisy)}. It costs CPU and fills log volumes.",
            "Remove with `ceph config rm <who> <option>` once the investigation is over.",
        )
    if per_daemon:
        yield Finding(
            "info",
            CONFIG,
            "PER_DAEMON_OVERRIDES",
            f"{_plural(len(per_daemon), 'option')} set on individual daemons",
            f"{_names(per_daemon)}. Often leftovers of troubleshooting that make daemons "
            "behave differently from their peers.",
            "Review; remove with `ceph config rm <daemon> <option>`.",
        )


# ----------------------------------------------------------------- health

_SERIOUS = {
    "PG_AVAILABILITY",
    "PG_DEGRADED",
    "PG_DAMAGED",
    "OSD_DOWN",
    "OSD_HOST_DOWN",
    "MON_DOWN",
    "MGR_DOWN",
    "MDS_ALL_DOWN",
    "FS_DEGRADED",
    "SLOW_OPS",
    "OSD_NEARFULL",
    "OSD_BACKFILLFULL",
    "POOL_NEARFULL",
    "OBJECT_UNFOUND",
    "CEPHADM_HOST_CHECK_FAILED",
    "CEPHADM_FAILED_DAEMON",
}


def health_checks(inp: Inputs, covered: set[str]) -> Iterable[Finding]:
    muted = []
    for code, check in (inp.health.get("checks") or {}).items():
        if check.get("muted"):
            muted.append(code)
            continue
        if code in covered:
            continue
        if check.get("severity") == "HEALTH_ERR":
            severity = "critical"
        else:
            severity = "high" if code in _SERIOUS else "medium"
        detail = [
            str(d["message"]).split(" (", 1)[0][:160]
            for d in check.get("detail") or []
            if d.get("message")
        ]
        yield Finding(
            severity,
            "health",
            f"HEALTH:{code}",
            str((check.get("summary") or {}).get("message") or code),
            _names(detail, 3),
            "health_detail lists everything affected; the triage runbook walks through it.",
        )
    muted += [m.get("code") for m in inp.health.get("mutes") or [] if m.get("code") not in muted]
    if muted:
        yield Finding(
            "low",
            OPERATIONS,
            "HEALTH_MUTED",
            f"{_plural(len(muted), 'health check')} muted",
            f"{_names(muted)}. A muted check hides the problem from HEALTH_OK.",
            "`ceph health unmute <code>` once the reason for muting is gone.",
        )


CHECKS: tuple[Callable[[Inputs], Iterable[Finding]], ...] = (
    pool_redundancy,
    pool_placement,
    pool_housekeeping,
    flags,
    osd_state,
    osd_devices,
    osd_memory,
    slow_osds,
    capacity,
    headroom,
    monitors,
    managers,
    versions,
    crashes,
    configuration,
)


def run(inp: Inputs) -> list[Finding]:
    """All findings, most severe first."""
    findings: list[Finding] = []
    for check in CHECKS:
        findings.extend(check(inp))
    covered = {code for f in findings for code in f.health_codes}
    findings.extend(health_checks(inp, covered))
    findings.sort(key=lambda f: SEVERITIES.index(f.severity))
    return findings
