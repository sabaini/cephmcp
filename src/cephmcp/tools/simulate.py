"""What-if questions: can I reboot this host, what if this disk dies, how
much data moves if I add disks. Nothing here changes the cluster."""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from pydantic import Field

from .. import whatif
from ..app import App, UsageError
from ..ceph import CephClient, gather
from ..render import compact
from ..topology import Topology, build
from .common import osd_id, word

Scenario = Literal["reboot", "fail", "drain", "add"]


class State:
    """Everything a simulation looks at, read from the cluster in one go."""

    def __init__(
        self,
        topology: Topology,
        pgs: list[dict[str, Any]],
        services: whatif.Services,
        status: dict[str, Any],
    ):
        self.topology = topology
        self.pgs = pgs
        self.services = services
        self.status = status

    @property
    def hosts(self) -> set[str]:
        return set(self.topology.hosts()) | self.services.hosts()


async def load_state(ceph: CephClient) -> State:
    """Read what an assessment rests on. Every one of these commands has to
    answer: a monitor list that could not be read is not a cluster without
    monitors, and "safe" must not come out of a gap."""
    (
        df_tree,
        osd_dump,
        rules,
        df,
        pg_dump,
        status,
        mon_metadata,
        mgr_stat,
        mgr_metadata,
        fs_status,
        mds_metadata,
        service_dump,
    ) = await gather(
        lambda: ceph.json(["osd", "df", "tree"]),
        lambda: ceph.json(["osd", "dump"]),
        lambda: ceph.json(["osd", "crush", "rule", "dump"]),
        lambda: ceph.json(["df", "detail"]),
        lambda: ceph.json(["pg", "dump", "pgs_brief"], timeout=120),
        lambda: ceph.json(["status"]),
        lambda: ceph.json(["mon", "metadata"]),
        lambda: ceph.json(["mgr", "stat"]),
        lambda: ceph.json(["mgr", "metadata"]),
        lambda: ceph.json(["fs", "status"]),
        lambda: ceph.json(["mds", "metadata"]),
        lambda: ceph.json(["service", "dump"]),
    )
    pgs = pg_dump.get("pg_stats") if isinstance(pg_dump, dict) else pg_dump
    return State(
        build(df_tree, osd_dump, rules, df),
        list(pgs or []),
        whatif.services_from(
            status, mon_metadata, mgr_stat, mgr_metadata, fs_status, mds_metadata, service_dump
        ),
        status or {},
    )


def resolve(state: State, hosts: list[str] | None, osds: list[int] | None) -> set[int]:
    """The OSD ids a scenario is about; complains about names it does not know."""
    topo = state.topology
    ids: set[int] = set()
    for host in hosts or []:
        word(host, "host name")
        if host not in state.hosts:
            raise UsageError(
                f"no host named {host!r}; the hosts are: {', '.join(sorted(state.hosts))}"
            )
        ids |= {o.id for o in topo.osds_of(host)}
    for osd in osds or []:
        n = osd_id(osd)
        if n not in topo.osds:
            raise UsageError(f"there is no osd.{n}")
        ids.add(n)
    return ids


async def ceph_opinion(
    ceph: CephClient, state: State, hosts: list[str], ids: set[int], result: whatif.Result
) -> None:
    """Ask Ceph's own safety checks, and take the more cautious answer."""
    opinion: dict[str, Any] = {}
    if ids:
        answer = await ceph.run(
            ["osd", "ok-to-stop", *[str(i) for i in sorted(ids)]], fmt="json", check=False
        )
        try:
            said = json.loads(answer.stdout)
        except ValueError:
            said = {}
        if "ok_to_stop" in said:
            opinion["osd ok-to-stop"] = (
                "yes"
                if said["ok_to_stop"]
                else f"no: {said.get('num_not_ok_pgs', '?')} PGs are or would become inactive"
            )
            if not said["ok_to_stop"] and result.verdict in (whatif.SAFE, whatif.CAVEATS):
                # Ceph also counts PGs that are inactive already; do not contradict it.
                result.worsen(
                    whatif.UNSAFE,
                    "`ceph osd ok-to-stop` refuses: "
                    + (answer.stderr.strip().splitlines() or ["no reason given"])[-1],
                )
    mons = sorted(n for n, h in state.services.mons.items() if h in set(hosts))
    if mons:
        answer = await ceph.run(["mon", "ok-to-stop", *mons], check=False)
        opinion["mon ok-to-stop"] = "yes" if answer.returncode == 0 else "no"
        if answer.returncode != 0 and result.verdict in (whatif.SAFE, whatif.CAVEATS):
            result.worsen(
                whatif.UNSAFE,
                "`ceph mon ok-to-stop` refuses: "
                + (answer.stderr.strip().splitlines() or ["no reason given"])[-1],
            )
    if opinion:
        result.sections["ceph_agrees"] = opinion


def current_state(state: State, result: whatif.Result) -> None:
    health = (state.status.get("health") or {}).get("status")
    unclean = sum(1 for pg in state.pgs if pg.get("state") != "active+clean")
    down = sorted(o.id for o in state.topology.osds.values() if not o.up and o.in_)
    if (health and health != "HEALTH_OK") or unclean or down:
        result.sections["cluster_now"] = compact(
            {
                "health": health,
                "pgs_not_active_clean": unclean or None,
                "osds_down": [f"osd.{i}" for i in down] or None,
            }
        )
    if unclean or down:
        result.worsen(
            whatif.CAVEATS,
            "The cluster is not fully healthy now ("
            + ", ".join(
                x
                for x in (
                    f"{unclean} PGs not active+clean" if unclean else "",
                    f"{len(down)} OSDs down" if down else "",
                )
                if x
            )
            + "): redundancy is already reduced, and the outcome above assumes nothing "
            "else changes.",
        )


async def run(
    ceph: CephClient,
    scenario: str,
    hosts: list[str] | None = None,
    osds: list[int] | None = None,
    add_host: str = "",
    add_count: int = 1,
    add_size: str = "",
    add_class: str = "",
) -> whatif.Result:
    state = await load_state(ceph)
    topo = state.topology
    if scenario == "add":
        if hosts or osds:
            raise UsageError("scenario 'add' takes add_host, add_count and add_size")
        if not add_host or not add_size:
            raise UsageError("scenario 'add' needs add_host and add_size, e.g. 'node4' and '4T'")
        word(add_host, "host name")
        if add_class:
            word(add_class, "device class")
        try:
            size = whatif.parse_size(add_size)
        except ValueError as exc:
            raise UsageError(str(exc)) from None
        result = whatif.Result(f"add {add_count} OSD(s) of {add_size} to {add_host}")
        whatif.expansion_section(topo, add_host, [size] * add_count, add_class or None, result)
        return result

    ids = resolve(state, hosts, osds)
    if not hosts and not osds:
        raise UsageError(f"scenario {scenario!r} needs hosts or osds")
    targets = whatif.describe_targets(topo, hosts or [], ids)
    if scenario == "reboot":
        result = whatif.Result(f"{targets} stop for a while and come back")
        outage = topo.outage(state.pgs, ids)
        whatif.data_section(topo, outage, result, permanent=False)
        whatif.services_section(state.services, hosts or [], result, permanent=False)
        await ceph_opinion(ceph, state, hosts or [], ids, result)
        current_state(state, result)
        if result.verdict != whatif.UNSAFE:
            scope = " ".join(hosts) if hosts else " ".join(f"osd.{i}" for i in sorted(ids))
            result.sections["how"] = (
                f"Set noout for the duration (`ceph osd set-group noout {scope}`) so that "
                "nothing is re-replicated, and clear it afterwards. maintenance_enter and "
                "maintenance_exit do this with checks, when the server runs in admin mode."
            )
    elif scenario == "fail":
        result = whatif.Result(f"{targets} are lost for good, without warning")
        outage = topo.outage(state.pgs, ids)
        whatif.data_section(topo, outage, result, permanent=True)
        if ids:
            whatif.recovery_section(topo, ids, result, planned=False)
        whatif.services_section(state.services, hosts or [], result, permanent=True)
        current_state(state, result)
    else:
        if not ids:
            raise UsageError("nothing to drain: these hosts have no OSDs")
        result = whatif.Result(f"{targets} are drained: marked out, emptied, then removed")
        down = sorted(i for i in ids if not topo.osds[i].up)
        if down:
            result.notes.append(
                f"{', '.join(f'osd.{i}' for i in down)} are down already and cannot serve "
                "as a source: for them this is scenario 'fail'."
            )
        whatif.recovery_section(topo, ids, result, planned=True)
        whatif.services_section(state.services, hosts or [], result, permanent=True)
        current_state(state, result)
        result.notes.append(
            "While draining, every PG keeps all its copies; only when all PGs are "
            "active+clean again (and `ceph osd safe-to-destroy` agrees) is it safe to remove "
            "the OSDs."
        )
    if not ids and scenario != "drain":
        result.notes.append("These hosts hold no OSDs: only the services on them are affected.")
    return result


def register(server: MCPServer, app: App) -> None:
    @app.tool(server, title="What-if simulation")
    async def simulate(
        scenario: Annotated[
            Scenario,
            Field(
                description="reboot: hosts or OSDs go away for a while and come back with "
                "their data (maintenance, restart). fail: they are lost for good, without "
                "warning. drain: they are marked out, emptied and removed in an orderly "
                "way. add: new OSDs join (uses add_host, add_count, add_size)."
            ),
        ],
        hosts: Annotated[
            list[str] | None, Field(description="Hosts the scenario happens to, e.g. ['node2'].")
        ] = None,
        osds: Annotated[
            list[int] | None, Field(description="OSD ids the scenario happens to, e.g. [3, 7].")
        ] = None,
        add_host: Annotated[
            str, Field(description="For 'add': the host that gets the OSDs, existing or new.")
        ] = "",
        add_count: Annotated[int, Field(ge=1, le=200, description="For 'add': how many OSDs.")] = 1,
        add_size: Annotated[
            str, Field(description="For 'add': the size of each, e.g. '4T', '960G', '1.8TiB'.")
        ] = "",
        add_class: Annotated[
            str,
            Field(description="For 'add': device class (hdd, ssd, nvme). Default: as its host."),
        ] = "",
    ) -> dict[str, Any]:
        """Work out what would happen, without changing anything:

        - "Can I reboot node2 now?" scenario='reboot', hosts=['node2']
        - "What if osd.3 dies tonight?" scenario='fail', osds=[3]
        - "What if we lose a whole host?" scenario='fail', hosts=['node2']
        - "Can I take osd.7 out of the cluster?" scenario='drain', osds=[7]
        - "How much data moves if I add two 4 TB disks to node4?" scenario='add',
          add_host='node4', add_count=2, add_size='4T'

        The answer starts with a verdict (safe; safe, with caveats; unsafe; data
        loss) and the reasons. Then: which PGs lose copies, stop serving I/O or
        are left without redundancy, per pool; what happens to monitors,
        managers, metadata servers and gateways on those hosts; whether the
        cluster can re-create the data, where it would go and how full the OSDs
        get; and for 'reboot', what Ceph's own ok-to-stop checks say.

        PG outcomes are exact, from where every PG's copies are now. Space
        figures are estimates from CRUSH weights.
        """
        result = await run(
            app.ceph, scenario, hosts, osds, add_host, add_count, add_size, add_class
        )
        return compact(result.as_dict())
