"""Who is using the cluster: pools, RBD images, clients, gateways, CephFS clients.

Ceph keeps no per-client statistics. What every OSD does know is which
operations it is working on right now, and for whom. Sampling that a few times
across the OSDs gives each client's share of the load, the way a sampling
profiler finds where a program spends its time. It needs no configuration
change and leaves nothing behind.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Annotated, Any

from anyio import sleep
from mcp.server.mcpserver import MCPServer
from pydantic import Field

from ..app import App
from ..ceph import CephError, gather
from ..metrics import Collection, rates
from ..render import compact, fmt_rate, num, pct, table

MAX_SAMPLED_OSDS = 24
RBD_STATS = ("write_ops", "read_ops", "write_bytes", "read_bytes", "write_latency", "read_latency")

# osd_op(client.20211.0:10452 2.0 2:02a97620:::rbd_data.4e7b98d26791.0000000000000117:head
#        [write 475136~4096 [fadvise_random] in=4096b] snapc 0=[] ondisk+write+known_if_... e310)
# and, for an operation not looked at yet:
# osd_op(client.20208.0:17509 2.0 2.6aea6f60 (undecoded) ondisk+write+known_if_redirected e310)
_OP = re.compile(
    r"^osd_op\((?P<client>client\.\d+)\.\d+:\d+ (?P<pool>\d+)\.[0-9a-f]+ (?P<rest>.*)$"
)
_OBJECT = re.compile(r"^\d+:[0-9a-f]+:[^:]*:[^:]*:(?P<name>.*?):(?:head|snapdir|[0-9a-f]+) ")
_CEPHFS = re.compile(r"^[0-9a-f]{6,16}\.[0-9a-f]{8}$")
_RGW = re.compile(r"^([0-9a-f-]{36}\.\d+\.\d+)_")


@dataclass
class Op:
    client: str
    pool: int
    write: bool
    target: str
    """What the operation touches, as far as the object name tells."""


def classify_object(name: str) -> str:
    if name.startswith(("rbd_data.", "rbd_header.", "rbd_object_map.", "journal_data.")):
        parts = name.split(".")
        return f"rbd image id {parts[1]}" if len(parts) > 1 else "rbd"
    if name.startswith("benchmark_data_"):
        return "rados bench"
    if _CEPHFS.match(name):
        return "cephfs file data"
    match = _RGW.match(name)
    if match:
        return f"rgw bucket marker {match.group(1)}"
    return ""


def parse_op(description: str) -> Op | None:
    """A client operation from the OSD's op tracker; None for anything else
    (replication between OSDs, peering, scrubbing)."""
    match = _OP.match(description)
    if not match:
        return None
    rest = match["rest"]
    name = _OBJECT.match(rest)
    return Op(
        client=match["client"],
        pool=int(match["pool"]),
        write="+write" in rest or " write+" in rest,
        target=classify_object(name["name"]) if name else "",
    )


def address_of(client_addr: str) -> str:
    """'10.0.0.5:0/123' or 'v1:10.0.0.5:0/123' or '[fd00::5]:0/123' -> the IP address."""
    text = re.sub(r"^v[12]:", "", client_addr)
    if text.startswith("["):
        return text[1:].split("]", 1)[0]
    return text.split(":", 1)[0]


@dataclass
class ClientLoad:
    ops: int = 0
    writes: int = 0
    address: str = ""
    targets: Counter[str] = field(default_factory=Counter)


def tally(
    dumps: list[dict[str, Any] | None], pools: dict[int, str]
) -> tuple[dict[str, ClientLoad], int]:
    """Per client, the operations seen in flight. Also returns how many
    operations of any kind were looked at."""
    clients: dict[str, ClientLoad] = {}
    seen = 0
    for dump in dumps:
        for op in (dump or {}).get("ops") or []:
            seen += 1
            parsed = parse_op(str(op.get("description") or ""))
            if parsed is None:
                continue
            load = clients.setdefault(parsed.client, ClientLoad())
            load.ops += 1
            load.writes += parsed.write
            info = (op.get("type_data") or {}).get("client_info") or {}
            load.address = load.address or address_of(str(info.get("client_addr") or ""))
            pool = pools.get(parsed.pool, f"pool {parsed.pool}")
            load.targets[f"{pool}: {parsed.target}" if parsed.target else pool] += 1
    return clients, seen


def rbd_rows(stats: dict[str, Any] | None) -> list[dict[str, Any]]:
    """`ceph rbd perf image stats` as rows, busiest image first."""
    if not stats:
        return []
    names = list(stats.get("stat_descriptors") or RBD_STATS)
    pools = stats.get("pool_descriptors") or {}
    rows = []
    for entry in stats.get("stats") or []:
        for key, values in entry.items():
            pool_index, _, image = key.partition("/")
            v = dict(zip(names, values, strict=False))
            rows.append(
                {
                    "image": f"{pools.get(pool_index, pool_index)}/{image}",
                    "write_ops_s": num(float(v.get("write_ops", 0)), 1),
                    "read_ops_s": num(float(v.get("read_ops", 0)), 1),
                    "write": fmt_rate(v.get("write_bytes")) if v.get("write_bytes") else None,
                    "read": fmt_rate(v.get("read_bytes")) if v.get("read_bytes") else None,
                    # Latencies come in nanoseconds.
                    "write_ms": num(v["write_latency"] / 1e6, 2)
                    if v.get("write_latency")
                    else None,
                    "read_ms": num(v["read_latency"] / 1e6, 2) if v.get("read_latency") else None,
                }
            )
    rows.sort(key=lambda r: -((r["write_ops_s"] or 0) + (r["read_ops_s"] or 0)))
    return rows


def pool_rows(pool_stats: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows = []
    for pool in pool_stats or []:
        rate = pool.get("client_io_rate") or {}
        ops = (rate.get("read_op_per_sec") or 0) + (rate.get("write_op_per_sec") or 0)
        volume = (rate.get("read_bytes_sec") or 0) + (rate.get("write_bytes_sec") or 0)
        if not ops and not volume:
            continue
        rows.append(
            {
                "pool": pool.get("pool_name"),
                "read_ops_s": rate.get("read_op_per_sec") or 0,
                "write_ops_s": rate.get("write_op_per_sec") or 0,
                "read": fmt_rate(rate["read_bytes_sec"]) if rate.get("read_bytes_sec") else None,
                "write": fmt_rate(rate["write_bytes_sec"]) if rate.get("write_bytes_sec") else None,
                "_ops": ops,
            }
        )
    total = sum(r["_ops"] for r in rows)
    rows.sort(key=lambda r: -r["_ops"])
    for row in rows:
        row["share_pct"] = pct(row.pop("_ops"), total, 0)
    return rows


def cephfs_rows(sessions: list[dict[str, Any]] | None, mds: str) -> list[dict[str, Any]]:
    rows = []
    for session in sessions or []:
        meta = session.get("client_metadata") or {}
        rows.append(
            {
                "client": f"client.{session.get('id')}",
                "host": meta.get("hostname"),
                "mount": meta.get("mount_point") or meta.get("root"),
                "user": (session.get("auth_name") or {}).get("id"),
                "requests_load": num(float(session.get("request_load_avg") or 0), 1),
                "in_flight": session.get("requests_in_flight") or None,
                "caps": session.get("num_caps"),
                "mds": mds,
            }
        )
    rows.sort(key=lambda r: -(r["requests_load"] or 0))
    return rows


def rgw_rows(before: Collection, after: Collection) -> list[dict[str, Any]]:
    """Request rates per gateway between two scrapes of its counters."""
    measured: dict[str, dict[str, float]] = {}
    for short, metric in (
        ("requests_s", "ceph_rgw_req"),
        ("failed_s", "ceph_rgw_failed_req"),
        ("put_ops_s", "ceph_rgw_op_put_obj_ops"),
        ("get_ops_s", "ceph_rgw_op_get_obj_ops"),
        ("put_bytes", "ceph_rgw_op_put_obj_bytes"),
        ("get_bytes", "ceph_rgw_op_get_obj_bytes"),
    ):
        for _, labels, value in rates(before, after, metric):
            lookup = dict(labels)
            gateway = lookup.get("instance_id") or lookup.get("ceph_daemon") or "rgw"
            measured.setdefault(gateway, {})[short] = value
    rows = []
    for gateway, m in measured.items():
        rows.append(
            {
                "gateway": gateway,
                "requests_s": num(m.get("requests_s", 0.0), 1),
                "failed_s": num(m["failed_s"], 1) if m.get("failed_s") else None,
                "put_ops_s": num(m.get("put_ops_s", 0.0), 1),
                "get_ops_s": num(m.get("get_ops_s", 0.0), 1),
                "upload": fmt_rate(m["put_bytes"]) if m.get("put_bytes") else None,
                "download": fmt_rate(m["get_bytes"]) if m.get("get_bytes") else None,
            }
        )
    rows.sort(key=lambda r: -(r["requests_s"] or 0))
    return rows


def register(server: MCPServer, app: App) -> None:
    ceph = app.ceph

    @app.tool(server, title="Who is using the cluster")
    async def io_top(
        seconds: Annotated[
            float,
            Field(
                ge=2,
                le=120,
                description="How long to watch. Longer catches more clients and evens out bursts.",
            ),
        ] = 10,
        limit: Annotated[int, Field(ge=1, le=100, description="Rows per section.")] = 10,
    ) -> dict[str, Any]:
        """Find out where the load on the cluster comes from, right now:

        - pools: client reads and writes per pool, and each pool's share;
        - rbd_images: the busiest RBD images with their I/O rates and latencies
          (from the mgr; the first call starts the collection, which needs
          about 20 seconds before it has numbers);
        - clients: which clients the OSDs are working for, by sampling the
          operations in flight on the OSDs several times: each client's share
          of them, its address and identity, how much of it is writes, and
          what it touches (pool, RBD image, CephFS data, S3 bucket);
        - cephfs_clients: sessions on the metadata servers, busiest first;
        - gateways: request rates per RGW daemon, where metrics export them.

        The tool for "who is hammering the cluster" and "which VM is the noisy
        neighbour". Client shares are a statistical sample, good for telling
        the heavy users from the rest, not exact accounting. This call takes
        `seconds` to return.
        """
        notes: list[str] = []
        status, pool_stats, detail, fs_status = await gather(
            lambda: ceph.json(["status"]),
            lambda: ceph.try_json(["osd", "pool", "stats"], []),
            lambda: ceph.try_json(["osd", "pool", "ls", "detail"], []),
            lambda: ceph.try_json(["fs", "status"], {}),
        )
        pools = {p.get("pool_id", p.get("pool")): p.get("pool_name") for p in detail or []}
        has_rbd = any("rbd" in (p.get("application_metadata") or {}) for p in detail or [])
        # The first request makes the mgr start collecting per-image statistics.
        rbd_first = await ceph.try_json(["rbd", "perf", "image", "stats"]) if has_rbd else None
        found = await app.metrics.discover()
        before: Collection | None = None
        if found.sources:
            before = await app.metrics.collect()
            if "ceph_rgw_req" not in before.metrics:
                before = None

        dump = await ceph.try_json(["osd", "dump"])
        if not isinstance(dump, dict):
            notes.append("The OSDs could not be listed, so clients could not be sampled.")
            dump = {}
        up = sorted(o["osd"] for o in dump.get("osds") or [] if o.get("up"))
        rounds = max(2, min(6, int(seconds // 2)))
        dumps: list[dict[str, Any] | None] = []
        denied = False
        offset = 0
        started = app.clock()
        for i in range(rounds):
            chosen = [up[(offset + k) % len(up)] for k in range(min(len(up), MAX_SAMPLED_OSDS))]
            offset += len(chosen)

            async def in_flight(osd: int) -> dict[str, Any] | None:
                nonlocal denied
                try:
                    return await ceph.json(["tell", f"osd.{osd}", "dump_ops_in_flight"])
                except CephError as exc:
                    denied = denied or "EACCES" in str(exc)
                    return None

            dumps.extend(await gather(*[lambda o=o: in_flight(o) for o in chosen]))
            if i < rounds - 1:
                # Looks are spread over the period; asking the OSDs takes time too.
                await sleep(max(0.0, started + (i + 1) * seconds / rounds - app.clock()))
        clients, seen = tally(dumps, pools)

        # Who the clients are: the monitors know the identity behind each id.
        identities: dict[str, dict[str, Any]] = {}
        if clients:
            sessions = await gather(
                *[
                    lambda m=m: ceph.try_json(["tell", f"mon.{m}", "sessions"], [])
                    for m in (status.get("quorum_names") or [])[:5]
                ]
            )
            for session in (s for group in sessions for s in group or []):
                if isinstance(session, dict) and session.get("global_id") is not None:
                    identities[f"client.{session['global_id']}"] = session
        total = sum(c.ops for c in clients.values())
        client_rows = []
        for name, load in sorted(clients.items(), key=lambda kv: -kv[1].ops)[:limit]:
            who = identities.get(name, {})
            client_rows.append(
                {
                    "client": name,
                    "share_pct": pct(load.ops, total, 0),
                    "address": load.address or None,
                    "host": who.get("remote_host") or None,
                    "identity": who.get("entity_name"),
                    "writes_pct": pct(load.writes, load.ops, 0),
                    "touches": ", ".join(
                        f"{target} ({pct(n, load.ops, 0):.0f}%)"
                        for target, n in load.targets.most_common(3)
                    ),
                }
            )

        out: dict[str, Any] = {"watched_s": seconds}
        pgmap = status.get("pgmap") or {}
        out["cluster"] = {
            "read": fmt_rate(pgmap["read_bytes_sec"]) if pgmap.get("read_bytes_sec") else None,
            "write": fmt_rate(pgmap["write_bytes_sec"]) if pgmap.get("write_bytes_sec") else None,
            "read_ops_s": pgmap.get("read_op_per_sec") or None,
            "write_ops_s": pgmap.get("write_op_per_sec") or None,
            "recovery": fmt_rate(pgmap["recovering_bytes_per_sec"])
            if pgmap.get("recovering_bytes_per_sec")
            else None,
        }
        by_pool = pool_rows(pool_stats)
        out["pools"] = table(by_pool[:limit]) if by_pool else None

        if has_rbd:
            images = rbd_rows(await ceph.try_json(["rbd", "perf", "image", "stats"]))
            if images:
                out["rbd_images"] = table(images[:limit])
            elif rbd_first is None:
                notes.append("RBD image statistics are not available (mgr rbd_support module).")
            elif any(r["pool"] and r.get("share_pct") for r in by_pool):
                notes.append(
                    "No RBD image shows I/O yet. The mgr starts collecting per-image "
                    "statistics on the first request: call again in about 20 seconds."
                )

        if client_rows:
            out["clients"] = table(client_rows)
            out["clients"]["sampled"] = (
                f"{total} client operations in flight, seen in {rounds} looks at "
                f"{min(len(up), MAX_SAMPLED_OSDS)} of {len(up)} OSDs"
            )
            if len(clients) > limit:
                out["clients"]["more"] = f"{len(clients) - limit} more clients with smaller shares"
        elif denied:
            notes.append(
                "Clients cannot be told apart: the server's Ceph identity may not query the "
                "OSDs (that needs osd 'allow *')."
            )
        elif up:
            notes.append(
                f"No client operation was in flight in {rounds} looks at the OSDs ({seen} "
                "operations of other kinds): the cluster is idle or nearly so."
            )

        active = [
            m["name"] for m in (fs_status or {}).get("mdsmap") or [] if m.get("state") == "active"
        ]
        if active:
            fs_rows: list[dict[str, Any]] = []
            for mds in active[:8]:
                sessions = await ceph.try_json(["tell", f"mds.{mds}", "session", "ls"], [])
                fs_rows.extend(cephfs_rows(sessions, mds))
            fs_rows.sort(key=lambda r: -(r["requests_load"] or 0))
            if fs_rows:
                out["cephfs_clients"] = table(fs_rows[:limit])
                out["cephfs_clients"]["note"] = (
                    "requests_load is the MDS's moving average of metadata requests per second"
                )

        if before is not None:
            after = await app.metrics.collect(fresh=True)
            gateways = rgw_rows(before, after)
            if any(r["requests_s"] for r in gateways):
                out["gateways"] = table(gateways[:limit])
            elif gateways and after.time - before.time <= found.mgr_scrape_interval:
                notes.append(
                    "RGW request rates need a longer look on this cluster: its metrics "
                    f"refresh every {found.mgr_scrape_interval:g}s. Use seconds >= "
                    f"{found.mgr_scrape_interval + 5:g}."
                )
        out["notes"] = notes
        return compact(out)
