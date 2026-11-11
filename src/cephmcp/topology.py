"""A model of the cluster's layout: OSDs in their CRUSH tree, pools, rules.

Built from `osd df tree`, `osd dump`, `osd crush rule dump` and `df detail`.
It answers two kinds of question without touching the cluster:

- exactly, from the PGs' current acting sets: what stops working if these
  OSDs go away (`outage`);
- approximately, from CRUSH weights: where data would settle after OSDs are
  lost or added (`expected_bytes`). CRUSH places replicas in proportion to
  weight, at most one per failure domain; the model computes that steady
  state rather than replaying CRUSH for every PG.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

KIB = 1024
CRUSH_ITEM_NONE = 0x7FFFFFFF
REPLICATED, ERASURE = "replicated", "erasure"
_POOL_TYPES = {1: REPLICATED, 3: ERASURE}
_CHOOSE_OPS = ("choose_firstn", "choose_indep", "chooseleaf_firstn", "chooseleaf_indep")
# Flags that are always set on a current cluster and say nothing about its state.
ROUTINE_FLAGS = {"sortbitwise", "recovery_deletes", "purged_snapdirs", "pglog_hardlimit"}


@dataclass(frozen=True)
class Osd:
    id: int
    up: bool
    reweight: float
    crush_weight: float
    size: int
    used: int
    pgs: int = 0
    device_class: str | None = None
    ancestors: tuple[tuple[str, str], ...] = ()
    """(bucket type, bucket name) from the OSD upward: host first, root last."""

    @property
    def in_(self) -> bool:
        return self.reweight > 0

    @property
    def weight(self) -> float:
        """The weight CRUSH places data by."""
        return self.crush_weight * self.reweight

    @property
    def host(self) -> str | None:
        return self.bucket("host")

    @property
    def used_pct(self) -> float:
        return 100.0 * self.used / self.size if self.size else 0.0

    def bucket(self, kind: str) -> str | None:
        if kind == "osd":
            return f"osd.{self.id}"
        return next((name for t, name in self.ancestors if t == kind), None)

    def under(self, bucket: str) -> bool:
        return any(name == bucket for _, name in self.ancestors)


@dataclass(frozen=True)
class Rule:
    id: int
    name: str
    root: str | None
    device_class: str | None
    failure_domain: str
    """Bucket type across which copies are spread: 'osd', 'host', 'rack', ..."""
    simple: bool
    """False for rules with several take or choose steps, which the weight
    model only approximates."""


@dataclass(frozen=True)
class Pool:
    id: int
    name: str
    kind: str
    size: int
    min_size: int
    rule_id: int
    pg_num: int
    k: int | None = None
    m: int | None = None
    profile: str | None = None
    stored: int = 0
    raw_used: int = 0
    max_avail: int | None = None
    applications: tuple[str, ...] = ()
    autoscale: str | None = None
    flags: tuple[str, ...] = ()
    quota_bytes: int = 0
    quota_objects: int = 0
    objects: int = 0

    @property
    def erasure(self) -> bool:
        return self.kind == ERASURE

    @property
    def survivable(self) -> int:
        """How many copies or shards may be lost without losing data."""
        if self.erasure and self.m is not None:
            return self.m
        return self.size - 1

    @property
    def needed(self) -> int:
        """Copies or shards needed to read the data at all."""
        return self.size - self.survivable

    @property
    def redundancy(self) -> str:
        if self.erasure:
            return f"EC {self.k}+{self.m}" if self.k else f"EC size {self.size}"
        return f"{self.size}x"


@dataclass
class Topology:
    osds: dict[int, Osd] = field(default_factory=dict)
    pools: dict[int, Pool] = field(default_factory=dict)
    rules: dict[int, Rule] = field(default_factory=dict)
    flags: set[str] = field(default_factory=set)
    scoped_flags: dict[str, list[str]] = field(default_factory=dict)
    """Flags set on single OSDs, CRUSH buckets or device classes: target -> flags."""
    full_ratio: float = 0.95
    backfillfull_ratio: float = 0.90
    nearfull_ratio: float = 0.85

    # ------------------------------------------------------------- lookups

    def hosts(self) -> dict[str, list[Osd]]:
        out: dict[str, list[Osd]] = {}
        for osd in self.osds.values():
            if osd.host:
                out.setdefault(osd.host, []).append(osd)
        return out

    def osds_of(self, bucket: str) -> list[Osd]:
        """OSDs under a CRUSH bucket (usually a host) or a single 'osd.N'."""
        return [o for o in self.osds.values() if o.under(bucket) or bucket == f"osd.{o.id}"]

    def rule_of(self, pool: Pool) -> Rule | None:
        return self.rules.get(pool.rule_id)

    def eligible(self, rule: Rule | None, exclude: Collection[int] = ()) -> list[Osd]:
        """OSDs a rule can place data on: in, weighted, under its root, of its class."""
        out = []
        for osd in self.osds.values():
            if osd.id in exclude or not osd.in_ or osd.weight <= 0:
                continue
            if rule is not None:
                if rule.root and not osd.under(rule.root):
                    continue
                if rule.device_class and osd.device_class != rule.device_class:
                    continue
            out.append(osd)
        return out

    def domains(self, rule: Rule | None, exclude: Collection[int] = ()) -> dict[str, list[Osd]]:
        """Failure domain -> eligible OSDs in it."""
        kind = rule.failure_domain if rule else "host"
        out: dict[str, list[Osd]] = {}
        for osd in self.eligible(rule, exclude):
            out.setdefault(osd.bucket(kind) or f"osd.{osd.id}", []).append(osd)
        return out

    # ------------------------------------------------------- what-if changes

    def without(self, osd_ids: Iterable[int]) -> Topology:
        """The cluster with these OSDs gone for good (marked out)."""
        gone = set(osd_ids)
        osds = {
            i: replace(o, reweight=0.0, up=False) if i in gone else o for i, o in self.osds.items()
        }
        return replace(self, osds=osds)

    def with_new(self, host: str, sizes: Iterable[int], device_class: str | None) -> Topology:
        """The cluster with empty OSDs of the given sizes (bytes) added to a host."""
        template = next((o for o in self.osds.values() if o.host == host), None)
        if template is not None:
            ancestors = template.ancestors
        else:
            # A new host goes where its peers are: under the same parents.
            peer = next((o for o in self.osds.values() if o.host), None)
            above = tuple(a for a in peer.ancestors if a[0] != "host") if peer else ()
            ancestors = (("host", host), *above)
        if device_class is None:
            device_class = template.device_class if template else self._common_class()
        osds = dict(self.osds)
        next_id = max(osds, default=-1) + 1
        for size in sizes:
            osds[next_id] = Osd(
                id=next_id,
                up=True,
                reweight=1.0,
                crush_weight=size / 2**40,  # CRUSH weight is the size in TiB
                size=size,
                used=0,
                device_class=device_class,
                ancestors=ancestors,
            )
            next_id += 1
        return replace(self, osds=osds)

    def _common_class(self) -> str | None:
        counts: dict[str, int] = {}
        for osd in self.osds.values():
            if osd.device_class:
                counts[osd.device_class] = counts.get(osd.device_class, 0) + 1
        return max(counts, key=lambda c: counts[c]) if counts else None

    # ----------------------------------------------------- placement model

    def shares(self, pool: Pool) -> dict[int, float]:
        """OSD id -> the number of copies (or shards) of an average PG of the
        pool that the OSD holds in the steady state. Sums to the pool's size
        when there are enough failure domains, to less otherwise."""
        domains = self.domains(self.rule_of(pool))
        weights = {name: sum(o.weight for o in osds) for name, osds in domains.items()}
        copies = dict.fromkeys(weights, 0.0)
        remaining = float(pool.size)
        open_ = {name for name, w in weights.items() if w > 0}
        # A domain holds at most one copy of a PG. Give every domain its
        # weight-proportional part; cap those that would exceed one copy and
        # spread what is left over the others.
        while open_ and remaining > 1e-9:
            total = sum(weights[name] for name in open_)
            capped = {name for name in open_ if remaining * weights[name] / total >= 1.0}
            if not capped:
                for name in open_:
                    copies[name] = remaining * weights[name] / total
                break
            for name in capped:
                copies[name] = 1.0
            open_ -= capped
            remaining -= len(capped)
        shares: dict[int, float] = {}
        for name, osds in domains.items():
            for osd in osds:
                shares[osd.id] = copies[name] * osd.weight / weights[name] if weights[name] else 0.0
        return shares

    def expected_bytes(self) -> dict[int, float]:
        """OSD id -> the pool data the model expects on it."""
        out = dict.fromkeys(self.osds, 0.0)
        for pool in self.pools.values():
            if not pool.raw_used or not pool.size:
                continue
            per_copy = pool.raw_used / pool.size
            for osd_id, share in self.shares(pool).items():
                out[osd_id] += share * per_copy
        return out

    def placeable(self, pool: Pool) -> int:
        """How many copies of the pool's PGs can be placed at all."""
        return min(pool.size, len(self.domains(self.rule_of(pool))))

    # ------------------------------------------------------------- outage

    def outage(self, pgs: Iterable[Mapping[str, Any]], down: Collection[int]) -> Outage:
        """What happens to the PGs if the OSDs in `down` stop."""
        down = set(down)
        result = Outage()
        for pg in pgs:
            pgid = str(pg.get("pgid", ""))
            pool = self.pools.get(_pool_of(pgid))
            if pool is None:
                continue
            acting = [o for o in pg.get("acting") or [] if o != CRUSH_ITEM_NONE]
            alive = [o for o in acting if o not in down]
            stats = result.pools.setdefault(pool.name, PoolOutage())
            stats.pgs += 1
            # A PG with enough copies counts as serving I/O even if it is peering
            # at this moment: the question is what the outage itself takes away.
            was_active = len(acting) >= pool.min_size
            if "active" not in str(pg.get("state", "")):
                stats.already_inactive += 1
            if len(alive) == len(acting):
                continue
            stats.degraded += 1
            if len(alive) < pool.needed:
                stats.lost += 1
                stats.inactive += was_active
                stats.cut_off += was_active
                stats.examples.setdefault("lost", []).append(pgid)
            elif len(alive) < pool.min_size:
                stats.inactive += was_active
                stats.stalled += was_active
                stats.examples.setdefault("inactive", []).append(pgid)
            elif len(alive) == pool.needed:
                stats.last_copy += 1
        return result


@dataclass
class PoolOutage:
    pgs: int = 0
    degraded: int = 0
    """PGs that lose at least one copy."""
    inactive: int = 0
    """PGs that are active now and would drop below min_size: I/O stops."""
    stalled: int = 0
    """Of those, the ones that still have enough copies to recover from."""
    cut_off: int = 0
    """Of those, the ones with too few copies left to read the data."""
    already_inactive: int = 0
    last_copy: int = 0
    """PGs that stay active but with no redundancy left."""
    lost: int = 0
    """PGs left with too few copies to read the data at all."""
    examples: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class Outage:
    pools: dict[str, PoolOutage] = field(default_factory=dict)

    def total(self, what: str) -> int:
        return sum(getattr(p, what) for p in self.pools.values())


def _pool_of(pgid: str) -> int:
    head = pgid.split(".", 1)[0]
    return int(head) if head.isdigit() else -1


# ---------------------------------------------------------------- building


def parse_rule(rule: Mapping[str, Any]) -> Rule:
    steps = rule.get("steps") or []
    takes = [s for s in steps if s.get("op") == "take"]
    chooses = [s for s in steps if s.get("op") in _CHOOSE_OPS]
    root, device_class = None, None
    if takes:
        root, _, device_class = str(takes[0].get("item_name") or "").partition("~")
    leaf = [s for s in chooses if str(s.get("op", "")).startswith("chooseleaf")]
    if leaf:
        domain = leaf[-1].get("type")
    elif chooses:
        # `choose host, then choose 1 osd` spreads across hosts as well.
        spreading = [s for s in chooses if s.get("num") != 1]
        domain = (spreading[-1] if spreading else chooses[0]).get("type")
    else:
        domain = "osd"
    return Rule(
        id=int(rule.get("rule_id", -1)),
        name=str(rule.get("rule_name", "")),
        root=root or None,
        device_class=device_class or None,
        failure_domain=str(domain or "osd"),
        simple=len(takes) == 1 and len(chooses) == 1,
    )


def _osds(df_tree: Mapping[str, Any]) -> dict[int, Osd]:
    nodes = list(df_tree.get("nodes") or [])
    parent: dict[int, Mapping[str, Any]] = {}
    for node in nodes:
        for child in node.get("children") or []:
            parent[child] = node
    osds: dict[int, Osd] = {}
    for node in [*nodes, *(df_tree.get("stray") or [])]:
        if node.get("type") != "osd":
            continue
        ancestors = []
        at, hops = node["id"], 0
        while at in parent and hops < 32:
            bucket = parent[at]
            ancestors.append((str(bucket.get("type")), str(bucket.get("name"))))
            at, hops = bucket["id"], hops + 1
        osds[node["id"]] = Osd(
            id=node["id"],
            up=node.get("status") == "up",
            reweight=float(node.get("reweight") or 0),
            crush_weight=float(node.get("crush_weight") or 0),
            size=int(node.get("kb") or 0) * KIB,
            used=int(node.get("kb_used") or 0) * KIB,
            pgs=int(node.get("pgs") or 0),
            device_class=node.get("device_class"),
            ancestors=tuple(ancestors),
        )
    return osds


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build(
    df_tree: Mapping[str, Any] | None,
    osd_dump: Mapping[str, Any] | None,
    rules: Iterable[Mapping[str, Any]] | None,
    df: Mapping[str, Any] | None = None,
) -> Topology:
    osd_dump = osd_dump or {}
    usage = {p.get("id"): p.get("stats") or {} for p in (df or {}).get("pools") or []}
    profiles = osd_dump.get("erasure_code_profiles") or {}
    pools: dict[int, Pool] = {}
    for p in osd_dump.get("pools") or []:
        pid = p.get("pool", p.get("pool_id"))
        if pid is None:
            continue
        kind = _POOL_TYPES.get(p.get("type"), str(p.get("type")))
        profile = profiles.get(p.get("erasure_code_profile") or "") or {}
        use = usage.get(pid, {})
        pools[pid] = Pool(
            id=pid,
            name=str(p.get("pool_name")),
            kind=kind,
            size=int(p.get("size") or 0),
            min_size=int(p.get("min_size") or 0),
            rule_id=int(p.get("crush_rule", -1)),
            pg_num=int(p.get("pg_num") or 0),
            k=_int(profile.get("k")) if kind == ERASURE else None,
            m=_int(profile.get("m")) if kind == ERASURE else None,
            profile=p.get("erasure_code_profile") or None,
            stored=int(use.get("stored") or 0),
            raw_used=int(use.get("bytes_used") or 0),
            max_avail=use.get("max_avail"),
            applications=tuple((p.get("application_metadata") or {}).keys()),
            autoscale=p.get("pg_autoscale_mode"),
            flags=tuple(str(p.get("flags_names") or "").split(",")),
            quota_bytes=int(p.get("quota_max_bytes") or 0),
            quota_objects=int(p.get("quota_max_objects") or 0),
            objects=int(use.get("objects") or 0),
        )
    osds = _osds(df_tree or {})
    scoped: dict[str, list[str]] = {}
    for osd in osd_dump.get("osds") or []:
        # Per-OSD flags show up among the state words: ["exists", "up", "noout"].
        flags = [s for s in osd.get("state") or [] if s.startswith("no")]
        if flags:
            scoped[f"osd.{osd.get('osd')}"] = flags
    for key in ("crush_node_flags", "device_class_flags"):
        for target, flags in (osd_dump.get(key) or {}).items():
            if flags:
                scoped[str(target)] = list(flags)
    names = {str(node.get("id")): node.get("name") for node in (df_tree or {}).get("nodes") or []}
    scoped = {str(names.get(target, target)): flags for target, flags in scoped.items()}
    topology = Topology(
        osds=osds,
        pools=pools,
        rules={r.id: r for r in map(parse_rule, rules or [])},
        flags=set(osd_dump.get("flags_set") or []) - ROUTINE_FLAGS,
        scoped_flags=scoped,
    )
    for name in ("full_ratio", "backfillfull_ratio", "nearfull_ratio"):
        if osd_dump.get(name):
            setattr(topology, name, float(osd_dump[name]))
    return topology
