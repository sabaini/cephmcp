"""Hand-made cluster layouts for tests, in the shape of the ceph commands'
JSON output."""

from __future__ import annotations

from typing import Any

from cephmcp.topology import Topology, build

GIB = 2**30


def tree(hosts: dict[str, list[dict[str, Any]]], root: str = "default") -> dict[str, Any]:
    """`osd df tree` for {host: [{"id": 0, "size": GiB, "used": GiB, ...}]}.

    Per OSD, optional: "class" (default hdd), "up" (default True), "reweight"
    (default 1), "pgs".
    """
    nodes: list[dict[str, Any]] = []
    host_ids = []
    for n, (host, osds) in enumerate(hosts.items()):
        host_id = -2 - n
        host_ids.append(host_id)
        bucket: dict[str, Any] = {"id": host_id, "name": host, "type": "host", "children": []}
        nodes.append(bucket)
        for osd in osds:
            size = osd.get("size", 100)
            bucket["children"].append(osd["id"])
            nodes.append(
                {
                    "id": osd["id"],
                    "name": f"osd.{osd['id']}",
                    "type": "osd",
                    "device_class": osd.get("class", "hdd"),
                    "crush_weight": size / 1024,
                    "reweight": osd.get("reweight", 1.0),
                    "kb": size * GIB // 1024,
                    "kb_used": int(osd.get("used", 0) * GIB) // 1024,
                    "utilization": 100.0 * osd.get("used", 0) / size if size else 0,
                    "pgs": osd.get("pgs", 0),
                    "status": "up" if osd.get("up", True) else "down",
                }
            )
    nodes.insert(0, {"id": -1, "name": root, "type": "root", "children": host_ids})
    return {"nodes": nodes, "stray": []}


def rule(rule_id: int, name: str, domain: str = "host", take: str = "default") -> dict[str, Any]:
    op = "choose_firstn" if domain == "osd" else "chooseleaf_firstn"
    return {
        "rule_id": rule_id,
        "rule_name": name,
        "type": 1,
        "steps": [
            {"op": "take", "item": -1, "item_name": take},
            {"op": op, "num": 0, "type": domain},
            {"op": "emit"},
        ],
    }


def pool(
    pool_id: int,
    name: str,
    size: int = 3,
    min_size: int = 2,
    rule_id: int = 0,
    *,
    used: float = 0,
    profile: str = "",
    application: str | None = "rbd",
    **extra: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """(entry for `osd dump`, entry for `df detail`); `used` is raw GiB."""
    dump = {
        "pool": pool_id,
        "pool_name": name,
        "type": 3 if profile else 1,
        "size": size,
        "min_size": min_size,
        "crush_rule": rule_id,
        "pg_num": 32,
        "pg_autoscale_mode": "on",
        "erasure_code_profile": profile,
        "flags_names": "hashpspool",
        "application_metadata": {application: {}} if application else {},
        **extra,
    }
    raw = int(used * GIB)
    df = {
        "id": pool_id,
        "name": name,
        "stats": {"bytes_used": raw, "stored": raw // max(size, 1), "objects": 0},
    }
    return dump, df


def cluster(
    hosts: dict[str, list[dict[str, Any]]],
    pools: list[tuple[dict[str, Any], dict[str, Any]]],
    rules: list[dict[str, Any]] | None = None,
    **dump: Any,
) -> dict[str, Any]:
    """The four command outputs a Topology is built from."""
    return {
        "osd df tree": tree(hosts),
        "osd dump": {
            "pools": [p for p, _ in pools],
            "osds": [],
            "flags_set": ["sortbitwise", "recovery_deletes", "purged_snapdirs", "pglog_hardlimit"],
            "full_ratio": 0.95,
            "backfillfull_ratio": 0.90,
            "nearfull_ratio": 0.85,
            "erasure_code_profiles": {"ec42": {"k": "4", "m": "2"}, "ec21": {"k": "2", "m": "1"}},
            **dump,
        },
        "osd crush rule dump": rules if rules is not None else [rule(0, "by-host")],
        "df detail": {"pools": [d for _, d in pools]},
    }


def topology(*args: Any, **kwargs: Any) -> Topology:
    c = cluster(*args, **kwargs)
    return build(c["osd df tree"], c["osd dump"], c["osd crush rule dump"], c["df detail"])


def three_hosts(used: float = 10, size: int = 100, per_host: int = 2) -> dict[str, list[dict]]:
    """Hosts a, b, c with `per_host` OSDs each, ids from 0."""
    ids = iter(range(100))
    return {
        host: [{"id": next(ids), "size": size, "used": used} for _ in range(per_host)]
        for host in ("a", "b", "c")
    }


def pgs(pool_id: int, acting_sets: list[list[int]], state: str = "active+clean") -> list[dict]:
    return [
        {"pgid": f"{pool_id}.{i:x}", "state": state, "up": acting, "acting": acting}
        for i, acting in enumerate(acting_sets)
    ]
