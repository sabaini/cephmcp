"""What-if analysis: the consequences of stopping, losing, draining or adding
OSDs and hosts, worked out from the cluster's current state without touching it.

Availability is exact: it follows from where the copies of every PG are right
now. Where data would go, and how full OSDs get, is the weight model of
`topology`, good to within a few percent on a balanced cluster.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .render import fmt_bytes, num, pct, table
from .topology import Osd, Outage, Topology

SAFE, CAVEATS, UNSAFE, DATA_LOSS = "safe", "safe, with caveats", "unsafe", "data loss"
_ORDER = (SAFE, CAVEATS, UNSAFE, DATA_LOSS)
_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGTP])(i?B?)?\s*$", re.IGNORECASE)
_POWERS = {"K": 1, "M": 2, "G": 3, "T": 4, "P": 5}


def parse_size(text: str) -> int:
    """'4T', '960G', '1.8TiB' -> bytes. Binary units, as Ceph reports sizes."""
    match = _SIZE.match(text)
    if not match:
        raise ValueError(f"{text!r} is not a size; write it like '4T', '960G' or '1.8TiB'")
    return int(float(match.group(1)) * 1024 ** _POWERS[match.group(2).upper()])


@dataclass
class Services:
    """Which daemons other than OSDs run where."""

    mons: dict[str, str | None] = field(default_factory=dict)
    quorum: list[str] = field(default_factory=list)
    mgr_active: str | None = None
    mgrs: dict[str, str | None] = field(default_factory=dict)
    mds: list[dict[str, Any]] = field(default_factory=list)
    other: dict[str, list[tuple[str, str | None]]] = field(default_factory=dict)

    def hosts(self) -> set[str]:
        found = {h for h in [*self.mons.values(), *self.mgrs.values()] if h}
        found |= {m["host"] for m in self.mds if m.get("host")}
        found |= {h for daemons in self.other.values() for _, h in daemons if h}
        return found


def services_from(
    status: Mapping[str, Any] | None,
    mon_metadata: Iterable[Mapping[str, Any]] | None,
    mgr_stat: Mapping[str, Any] | None,
    mgr_metadata: Iterable[Mapping[str, Any]] | None,
    fs_status: Mapping[str, Any] | None,
    mds_metadata: Iterable[Mapping[str, Any]] | None,
    service_dump: Mapping[str, Any] | None,
) -> Services:
    mds_hosts = {m.get("name"): m.get("hostname") for m in mds_metadata or []}
    other: dict[str, list[tuple[str, str | None]]] = {}
    for kind, service in ((service_dump or {}).get("services") or {}).items():
        if kind in ("mon", "mgr", "osd", "mds"):
            continue
        for key, daemon in (service.get("daemons") or {}).items():
            meta = daemon.get("metadata") if isinstance(daemon, dict) else None
            if meta:
                other.setdefault(kind, []).append((str(meta.get("id", key)), meta.get("hostname")))
    return Services(
        mons={str(m.get("name")): m.get("hostname") for m in mon_metadata or []},
        quorum=list((status or {}).get("quorum_names") or []),
        mgr_active=(mgr_stat or {}).get("active_name"),
        mgrs={str(m.get("name")): m.get("hostname") for m in mgr_metadata or []},
        mds=[
            {
                "name": m.get("name"),
                "state": m.get("state"),
                "fs": m.get("fs"),
                "host": mds_hosts.get(m.get("name")),
            }
            for m in (fs_status or {}).get("mdsmap") or []
        ],
        other=other,
    )


@dataclass
class Result:
    scenario: str
    verdict: str = SAFE
    notes: list[str] = field(default_factory=list)
    sections: dict[str, Any] = field(default_factory=dict)
    _reasons: list[tuple[int, str]] = field(default_factory=list)

    def worsen(self, verdict: str, reason: str) -> None:
        level = _ORDER.index(verdict)
        if level > _ORDER.index(self.verdict):
            self.verdict = verdict
        self._reasons.append((level, reason))

    @property
    def reasons(self) -> list[str]:
        """Why it is not simply safe: what breaks, most serious first."""
        return [reason for _, reason in sorted(self._reasons, key=lambda r: -r[0])]

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "verdict": self.verdict,
            "reasons": self.reasons,
            **self.sections,
            "notes": self.notes,
        }


def _names(items: Iterable[Any], limit: int = 6) -> str:
    items = [str(i) for i in items]
    if len(items) <= limit:
        return ", ".join(items)
    return ", ".join(items[:limit]) + f" and {len(items) - limit} more"


def describe_targets(topo: Topology, hosts: Collection[str], osd_ids: Collection[int]) -> str:
    parts = []
    for host in hosts:
        ids = sorted(o.id for o in topo.osds_of(host))
        inside = _names((f"osd.{i}" for i in ids), 8) if ids else "no OSDs"
        parts.append(f"host {host} ({inside})")
    loose = sorted(set(osd_ids) - {o.id for h in hosts for o in topo.osds_of(h)})
    if loose:
        parts.append(_names((f"osd.{i}" for i in loose), 12))
    return " and ".join(parts)


# -------------------------------------------------------------- availability


def data_section(topo: Topology, outage: Outage, result: Result, *, permanent: bool) -> None:
    """What happens to the data while the OSDs are away."""
    rows = []
    for name, pool in sorted(outage.pools.items()):
        if not pool.degraded and not pool.already_inactive:
            continue
        spec = next((p for p in topo.pools.values() if p.name == name), None)
        rows.append(
            {
                "pool": name,
                "redundancy": f"{spec.redundancy}, min_size {spec.min_size}" if spec else None,
                "pgs": pool.pgs,
                "lose_a_copy": pool.degraded,
                "stop_serving_io": pool.inactive or None,
                "left_without_redundancy": pool.last_copy or None,
                "all_copies_gone": pool.lost or None,
                "examples": _names(
                    pool.examples.get("lost") or pool.examples.get("inactive") or [], 5
                )
                or None,
            }
        )
    lost, inactive = outage.total("lost"), outage.total("inactive")
    stalled, last = outage.total("stalled"), outage.total("last_copy")
    degraded = outage.total("degraded")

    def pools_with(what: str) -> str:
        return _names(sorted(n for n, p in outage.pools.items() if getattr(p, what)))

    if permanent:
        if lost:
            result.worsen(
                DATA_LOSS,
                f"{lost} PGs in {pools_with('lost')} would have too few copies left to "
                "rebuild from: their data is lost.",
            )
        if stalled:
            result.worsen(
                UNSAFE,
                f"{stalled} PGs in {pools_with('stalled')} drop below min_size and stop "
                "serving I/O until recovery has restored enough copies.",
            )
    elif inactive:
        cut_off = outage.total("cut_off")
        every = f" ({cut_off} of them have every copy on these OSDs)" if cut_off else ""
        result.worsen(
            UNSAFE,
            f"{inactive} PGs in {pools_with('inactive')} drop below min_size and stop "
            f"serving I/O for the duration{every}.",
        )
    if last:
        result.worsen(
            CAVEATS,
            f"{last} PGs in {pools_with('last_copy')} keep running on their last copy: one more "
            "failure meanwhile loses data.",
        )
    already = outage.total("already_inactive")
    if already:
        result.notes.append(f"{already} PGs are not active right now, independent of this.")
    result.sections["data"] = {
        "pgs_total": outage.total("pgs"),
        "pgs_losing_a_copy": degraded,
        "pgs_that_stop_serving_io": inactive,
        "pgs_left_without_redundancy": last or None,
        "pgs_with_all_copies_gone": lost or None,
        "by_pool": table(rows) if rows else None,
    }


def services_section(
    services: Services, hosts: Collection[str], result: Result, *, permanent: bool
) -> None:
    """What happens to monitors, managers, metadata servers and gateways."""
    if not hosts:
        return
    gone = set(hosts)
    out: dict[str, str] = {}
    mons_here = sorted(n for n, h in services.mons.items() if h in gone)
    if mons_here:
        total = len(services.mons)
        in_quorum = set(services.quorum) or set(services.mons)
        remaining = len(in_quorum - set(mons_here))
        needed = total // 2 + 1
        if remaining >= needed:
            out["mon"] = (
                f"{_names(mons_here)} stop; {remaining} of {total} monitors remain, "
                f"quorum ({needed}) holds"
            )
            if remaining == needed:
                result.notes.append("No further monitor may fail meanwhile: quorum is exact.")
        else:
            out["mon"] = f"{_names(mons_here)} stop; {remaining} of {total} remain: no quorum"
            result.worsen(
                UNSAFE,
                f"The monitors lose their quorum ({remaining} of {total} left, {needed} "
                "needed): the whole cluster stops.",
            )
    mgrs_here = sorted(n for n, h in services.mgrs.items() if h in gone)
    if mgrs_here:
        elsewhere = sorted(n for n, h in services.mgrs.items() if h not in gone)
        if services.mgr_active in mgrs_here:
            if elsewhere:
                out["mgr"] = f"the active mgr is here; standby {elsewhere[0]} takes over"
            else:
                out["mgr"] = "the only mgr is here"
                result.worsen(
                    CAVEATS,
                    "No mgr remains: metrics, dashboard, orchestrator and balancer stop "
                    "(client I/O continues).",
                )
        else:
            out["mgr"] = f"standby {_names(mgrs_here)} stops; the active mgr is elsewhere"
    active_here = [m for m in services.mds if m.get("host") in gone and m.get("state") == "active"]
    if active_here:
        spare = [
            m
            for m in services.mds
            if m.get("host") not in gone and "standby" in str(m.get("state"))
        ]
        names = _names(m["name"] for m in active_here)
        if spare:
            out["mds"] = f"active MDS {names} stops; standby {spare[0]['name']} takes over"
        else:
            out["mds"] = f"active MDS {names} stops and there is no standby elsewhere"
            result.worsen(UNSAFE, "CephFS loses its metadata server: the file system hangs.")
    for kind, daemons in sorted(services.other.items()):
        here = [name for name, host in daemons if host in gone]
        if not here:
            continue
        left = len(daemons) - len(here)
        if left:
            out[kind] = f"{len(here)} of {len(daemons)} {kind} daemons stop; {left} remain"
        else:
            out[kind] = f"the only {kind} daemon{'s' if len(here) > 1 else ''} run here"
            result.worsen(
                CAVEATS,
                f"The {kind} service is down "
                + ("until it is redeployed elsewhere." if permanent else "for the duration."),
            )
    if out:
        result.sections["services"] = out


# ------------------------------------------------------------------ recovery


def _projection(
    topo: Topology, after: Topology, skip: Collection[int] = ()
) -> list[dict[str, Any]]:
    """Per OSD: how full it is, and how full the model expects it to become."""
    before_bytes, after_bytes = topo.expected_bytes(), after.expected_bytes()
    rows = []
    for osd in after.osds.values():
        if osd.id in skip or not osd.in_ or not osd.size:
            continue
        now = topo.osds.get(osd.id)
        used = now.used if now else 0
        delta = after_bytes.get(osd.id, 0.0) - before_bytes.get(osd.id, 0.0)
        rows.append(
            {
                "osd": osd,
                "delta": delta,
                "now_pct": now.used_pct if now else None,
                "after_pct": 100.0 * max(0.0, used + delta) / osd.size,
            }
        )
    return rows


def _osd_table(rows: list[dict[str, Any]], limit: int = 12) -> dict[str, Any]:
    shown = sorted(rows, key=lambda r: -r["after_pct"])[:limit]
    out = table(
        [
            {
                "osd": f"osd.{r['osd'].id}",
                "host": r["osd"].host,
                "used_pct_now": num(r["now_pct"], 1) if r["now_pct"] is not None else "new",
                "used_pct_after": num(r["after_pct"], 1),
                "change": fmt_bytes(r["delta"]),
            }
            for r in shown
        ]
    )
    if len(rows) > limit:
        out["note"] = f"the {limit} fullest of {len(rows)} OSDs afterwards"
    return out


def recovery_section(
    topo: Topology, gone: Collection[int], result: Result, *, planned: bool
) -> None:
    """Whether the cluster can re-create what the OSDs held, and where."""
    after = topo.without(gone)
    stuck, blocked = [], []
    for pool in topo.pools.values():
        can = after.placeable(pool)
        rule = topo.rule_of(pool)
        domain = rule.failure_domain if rule else "host"
        if can < pool.min_size:
            blocked.append(
                f"{pool.name} ({can} of {pool.size} {domain}s left, min_size {pool.min_size})"
            )
        elif can < pool.size:
            stuck.append(f"{pool.name} ({can} of {pool.size} {domain}s left)")
    if blocked:
        result.worsen(
            UNSAFE,
            f"Too few failure domains remain for {_names(blocked)}: these pools cannot "
            "serve I/O until hardware is added.",
        )
    if stuck:
        result.worsen(
            CAVEATS,
            f"Full redundancy cannot be restored for {_names(stuck)}: there is no other "
            "failure domain to put the missing copies on. They stay degraded until the "
            "hardware is replaced.",
        )
    rows = _projection(topo, after, skip=gone)
    moving = sum(max(0.0, r["delta"]) for r in rows)
    held = sum(topo.osds[i].used for i in gone if i in topo.osds)
    section: dict[str, Any] = {
        "can_restore_full_redundancy": not stuck and not blocked,
        "data_to_move" if planned else "data_to_recreate": fmt_bytes(moving),
        "held_by_these_osds": fmt_bytes(held),
    }
    if rows:
        worst = max(rows, key=lambda r: r["after_pct"])
        section["fullest_osd_afterwards"] = (
            f"osd.{worst['osd'].id} at {worst['after_pct']:.0f}% (now {worst['now_pct']:.0f}%)"
        )
        ratio = worst["after_pct"] / 100
        if ratio >= topo.full_ratio:
            result.worsen(
                UNSAFE,
                f"Not enough space: osd.{worst['osd'].id} would reach "
                f"{worst['after_pct']:.0f}%, past the full limit "
                f"({100 * topo.full_ratio:.0f}%). The cluster would stop accepting writes.",
            )
        elif ratio >= topo.backfillfull_ratio:
            result.worsen(
                UNSAFE if planned else CAVEATS,
                f"Not enough space to finish: osd.{worst['osd'].id} would reach "
                f"{worst['after_pct']:.0f}%, past the backfillfull limit "
                f"({100 * topo.backfillfull_ratio:.0f}%). Recovery would stall part-way.",
            )
        elif ratio >= topo.nearfull_ratio:
            result.worsen(
                CAVEATS,
                f"osd.{worst['osd'].id} would reach {worst['after_pct']:.0f}%, past the "
                f"nearfull limit ({100 * topo.nearfull_ratio:.0f}%).",
            )
        section["osds_afterwards"] = _osd_table([r for r in rows if r["delta"] > 0] or rows)
    total = sum(o.size for o in after.osds.values() if o.in_)
    used = sum(o.used for o in topo.osds.values() if o.in_)
    section["raw_capacity"] = (
        f"{fmt_bytes(sum(o.size for o in topo.osds.values() if o.in_))} -> {fmt_bytes(total)}"
    )
    section["raw_used_pct_afterwards"] = pct(used - held + moving, total)
    result.sections["recovery"] = section
    if any(not r.simple for r in topo.rules.values()):
        result.notes.append(
            "Some CRUSH rules have several steps; for them the space figures are approximate."
        )


def expansion_section(
    topo: Topology, host: str, sizes: list[int], device_class: str | None, result: Result
) -> None:
    after = topo.with_new(host, sizes, device_class)
    new_ids = set(after.osds) - set(topo.osds)
    rows = _projection(topo, after)
    arriving = sum(max(0.0, r["delta"]) for r in rows)
    onto_new = sum(max(0.0, r["delta"]) for r in rows if r["osd"].id in new_ids)
    stored = sum(p.raw_used for p in topo.pools.values())
    before_total = sum(o.size for o in topo.osds.values() if o.in_)
    after_total = sum(o.size for o in after.osds.values() if o.in_)
    used = sum(o.used for o in topo.osds.values() if o.in_)
    old_worst = max(
        (o for o in topo.osds.values() if o.in_ and o.size), key=lambda o: o.used_pct, default=None
    )
    new_worst = max(rows, key=lambda r: r["after_pct"], default=None)
    is_new_host = host not in topo.hosts()
    unused = [
        pool.name
        for pool in topo.pools.values()
        if not any(i in new_ids for i in after.shares(pool))
    ]
    section: dict[str, Any] = {
        "adds": f"{len(sizes)} x {fmt_bytes(sizes[0])}"
        if len(set(sizes)) == 1
        else ", ".join(fmt_bytes(s) or "" for s in sizes),
        "on": f"{host} ({'a new host' if is_new_host else 'existing host'})",
        "raw_capacity": f"{fmt_bytes(before_total)} -> {fmt_bytes(after_total)}",
        "raw_used_pct": f"{pct(used, before_total)} -> {pct(used, after_total)}",
        "data_that_moves": fmt_bytes(arriving),
        "share_of_all_data": f"{pct(arriving, stored)}%" if stored else None,
        "onto_the_new_osds": fmt_bytes(onto_new),
        "fullest_osd": (
            f"osd.{old_worst.id} at {old_worst.used_pct:.0f}% -> "
            f"osd.{new_worst['osd'].id} at {new_worst['after_pct']:.0f}%"
        )
        if old_worst and new_worst
        else None,
        "osds_afterwards": _osd_table(rows),
    }
    result.sections["expansion"] = section
    domains_gained = []
    for pool in topo.pools.values():
        was, now = topo.placeable(pool), after.placeable(pool)
        if now > was:
            domains_gained.append(f"{pool.name} ({was} -> {now} of {pool.size} copies)")
    if domains_gained:
        result.notes.append(
            f"This lets these pools place more of their copies: {_names(domains_gained)}."
        )
    if unused and len(unused) < len(topo.pools):
        result.notes.append(
            f"Pools {_names(unused)} would not use the new OSDs (their CRUSH rule selects "
            "another root or device class)."
        )
    elif unused:
        result.worsen(
            CAVEATS,
            "No pool would use the new OSDs: their device class or location matches no "
            "CRUSH rule in use.",
        )
    if not is_new_host:
        peers = [sum(o.weight for o in osds) for h, osds in topo.hosts().items() if h != host]
        mine = sum(o.weight for o in after.osds.values() if o.host == host)
        if peers and mine > 1.5 * max(peers):
            result.notes.append(
                f"{host} would hold much more capacity than any other host. With one copy "
                "per host, capacity beyond what the other hosts can mirror stays unused."
            )
    result.notes.append(
        "Data moves in the background at the pace of backfill; clients keep working. "
        "The figures are estimates from CRUSH weights."
    )


def worst(osds: Iterable[Osd]) -> Osd | None:
    return max((o for o in osds if o.in_ and o.size), key=lambda o: o.used_pct, default=None)
