"""Host maintenance as one guarded operation: check that the host can go,
have it approved, apply it the way the deployment wants it done, wait until
the cluster has settled, and verify. Leaving maintenance is the reverse.

How a host is put into maintenance depends on what manages the daemons:
MicroCeph and cephadm have a command for it that also stops the daemons;
elsewhere the server sets noout for the host and the operator stops them.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import Annotated, Any

from anyio import sleep
from mcp.server.elicitation import ElicitationResult
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.resolve import Elicit, Resolve
from pydantic import Field

from .. import guard, whatif
from ..app import App, UsageError
from ..ceph import CephError
from ..guard import Approval
from ..http import HttpError
from ..policy import Denied
from ..render import compact
from . import simulate
from .common import word

MICROCEPH, CEPHADM, PLAIN = "MicroCeph", "cephadm", "no orchestrator"
PLAN_TTL = 5.0
"""A plan is reused within one call (the approval question and the tool body
must look at the same one), never across the time a person takes to answer."""
POLL = 3.0
Step = list[str]


@dataclass
class Plan:
    host: str
    deployment: str
    result: whatif.Result
    osds: list[int]
    steps: list[Step]
    undo: list[Step]
    stops_daemons: bool
    forced: bool
    code: str = ""
    made: float = 0.0
    not_active_before: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def safe(self) -> bool:
        return self.result.verdict in (whatif.SAFE, whatif.CAVEATS)

    def commands(self) -> list[str]:
        return [" ".join(step) for step in self.steps]

    def question(self) -> str:
        """What the operator is asked. Must read the same for the same plan."""
        lines = [f"Put host {self.host} into maintenance?", f"Assessment: {self.result.verdict}."]
        lines += [f"- {reason}" for reason in self.result.reasons]
        data = self.result.sections.get("data") or {}
        if self.osds:
            names = ", ".join(f"osd.{i}" for i in self.osds)
            lines.append(
                f"Its {len(self.osds)} OSDs go down ({names}): "
                f"{data.get('pgs_losing_a_copy', 0)} PGs lose a copy, "
                f"{data.get('pgs_that_stop_serving_io', 0)} stop serving I/O."
            )
        lines += [
            f"{kind}: {text}" for kind, text in (self.result.sections.get("services") or {}).items()
        ]
        lines.append("This runs: " + "; ".join(self.commands() or ["nothing (no flags needed)"]))
        if not self.stops_daemons:
            lines.append("The daemons keep running until you stop them or reboot the host.")
        return "\n".join(lines)

    def identity(self) -> str:
        """A code for everything the operator is shown about this plan: what
        it affects, how it is assessed and what it runs. An approval counts
        for the plan with this code and no other."""
        return guard.code_for(self.question(), *self.commands(), *self.notes)

    def summary(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "managed_by": self.deployment,
            "assessment": self.result.verdict,
            "reasons": self.result.reasons,
            "osds": [f"osd.{i}" for i in self.osds],
            "data": {
                k: v for k, v in (self.result.sections.get("data") or {}).items() if k != "by_pool"
            },
            "services": self.result.sections.get("services"),
            "would_run": self.commands(),
            "notes": self.notes + self.result.notes,
        }


class Planner:
    """Works out, and briefly remembers, the plan for a host. The approval
    question and the tool itself must look at the same plan."""

    def __init__(self, app: App):
        self.app = app
        self._deployment: str | None = None
        self._plans: dict[tuple[str, bool, bool], Plan] = {}
        self.asked: dict[tuple[str, bool, bool], str] = {}
        """The plan each pending question to an operator was about, by its code."""

    async def deployment(self) -> str:
        if self._deployment is None:
            if self.app.microceph.available:
                self._deployment = MICROCEPH
            else:
                status = await self.app.ceph.try_json(["orch", "status"], {}) or {}
                managed = status.get("available") and status.get("backend") == "cephadm"
                self._deployment = CEPHADM if managed else PLAIN
        return self._deployment

    async def plan(self, host: str, stop_daemons: bool, force: bool) -> Plan:
        key = (host, stop_daemons, force)
        cached = self._plans.get(key)
        if cached and self.app.clock() - cached.made < PLAN_TTL:
            return cached
        word(host, "host name")
        ceph = self.app.ceph
        state = await simulate.load_state(ceph)
        ids = simulate.resolve(state, [host], None)
        result = whatif.Result(f"host {host} goes down for maintenance")
        outage = state.topology.outage(state.pgs, ids)
        whatif.data_section(state.topology, outage, result, permanent=False)
        whatif.services_section(state.services, [host], result, permanent=False)
        await simulate.ceph_opinion(ceph, state, [host], ids, result)
        simulate.current_state(state, result)

        deployment = await self.deployment()
        notes: list[str] = []
        steps: list[Step] = []
        services = state.services
        active_here = services.mgrs.get(services.mgr_active or "") == host
        standby = any(h != host for n, h in services.mgrs.items() if n != services.mgr_active)
        if active_here and standby and deployment != MICROCEPH:
            # Hand the mgr over now instead of having it noticed missing later.
            steps.append(["ceph", "mgr", "fail"])
        if deployment == MICROCEPH:
            enter = ["microceph", "cluster", "maintenance", "enter", host]
            if stop_daemons:
                enter.append("--stop-osds")
            if force:
                enter.append("--ignore-check")
            steps.append(enter)
            undo = [["microceph", "cluster", "maintenance", "exit", host]]
            notes.append(
                "MicroCeph sets noout for the whole cluster while a node is in maintenance."
            )
            stops = stop_daemons and bool(ids)
        elif deployment == CEPHADM:
            enter = ["ceph", "orch", "host", "maintenance", "enter", host]
            if force:
                enter.append("--force")
            steps.append(enter)
            undo = [["ceph", "orch", "host", "maintenance", "exit", host]]
            stops = bool(ids)
        else:
            undo = []
            if ids:
                steps.append(["ceph", "osd", "set-group", "noout", host])
                undo = [["ceph", "osd", "unset-group", "noout", host]]
            notes.append(
                "Nothing manages the daemons here: this sets the flags, and the operator "
                "stops the daemons or reboots the host."
            )
            stops = False
        plan = Plan(
            host=host,
            deployment=deployment,
            result=result,
            osds=sorted(ids),
            steps=steps,
            undo=undo,
            stops_daemons=stops,
            forced=force,
            made=self.app.clock(),
            not_active_before=outage.total("already_inactive"),
            notes=notes,
        )
        plan.code = plan.identity()
        self._plans[key] = plan
        return plan

    def forget(self) -> None:
        self._plans.clear()


def planner(app: App) -> Planner:
    return app.extras.setdefault("maintenance", Planner(app))


async def approve_maintenance(
    ctx: Context, host: str, stop_daemons: bool, force: bool
) -> Elicit[Approval] | str:
    """Ask the operator, showing what the server found and what it will run."""
    app = guard.app_of(ctx)
    if not app.settings.admin or app.settings.approval == "off":
        return guard.NOT_NEEDED
    try:
        plan = await planner(app).plan(host, stop_daemons, force)
    except (CephError, HttpError, Denied, UsageError) as exc:
        raise ToolError(app.scrub(str(exc))) from None
    if not plan.safe and not (force and app.settings.destructive):
        # The tool refuses this anyway; no need to trouble anyone.
        return guard.ASK_AGENT
    asking = guard.request(ctx, plan.question())
    if isinstance(asking, Elicit):
        # Remember which plan the question is about: the answer counts for that one.
        planner(app).asked[(host, stop_daemons, force)] = plan.code
    return asking


def register(server: MCPServer, app: App) -> None:
    ceph, policy = app.ceph, app.policy
    plans = planner(app)

    async def run_steps(steps: list[Step]) -> tuple[list[str], list[str]]:
        ran, outputs = [], []
        for step in steps:
            ran.append(" ".join(step))
            if step[0] == "microceph":
                result = await app.microceph.run(step[1:], timeout=900)
            else:
                result = await ceph.run(step[1:], timeout=600)
            output = (result.stdout.strip() + "\n" + result.stderr.strip()).strip()
            if output:
                outputs.append(output[-1500:])
        return ran, outputs

    async def observe(osds: list[int]) -> dict[str, Any] | None:
        """Where the host's OSDs and the cluster's PGs stand; None if the
        cluster did not say (which is not the same as "all down")."""
        status = await ceph.try_json(["status"])
        dump = await ceph.try_json(["osd", "dump"])
        if not isinstance(status, dict) or not isinstance(dump, dict):
            return None
        up = {o.get("osd") for o in dump.get("osds") or [] if o.get("up")}
        states = {
            s.get("state_name", ""): s.get("count", 0)
            for s in (status.get("pgmap") or {}).get("pgs_by_state") or []
        }
        return {
            "health": (status.get("health") or {}).get("status"),
            "down": sorted(i for i in osds if i not in up),
            "up": sorted(i for i in osds if i in up),
            "pgs": sum(states.values()),
            "not_active": sum(n for s, n in states.items() if "active" not in s),
            "in_transition": sum(
                n for s, n in states.items() if "peering" in s or "activating" in s
            ),
            "not_clean": sum(n for s, n in states.items() if s != "active+clean"),
            "degraded": sum(n for s, n in states.items() if "degraded" in s or "undersized" in s),
            "flags": sorted(dump.get("flags_set") or []),
        }

    async def wait_until(
        ctx: Context[Any, Any], osds: list[int], done: Any, seconds: float, what: str
    ) -> tuple[dict[str, Any] | None, float, bool]:
        """Poll until done(observation) holds or the time is up. The last
        observation is None if the cluster did not answer."""
        waited = 0.0
        seen = await observe(osds)
        while (seen is None or not done(seen)) and waited < seconds:
            # Progress is a courtesy to the client, never a reason to fail.
            with contextlib.suppress(Exception):
                await ctx.report_progress(waited, seconds, what)
            await sleep(POLL)
            waited += POLL
            seen = await observe(osds)
        return seen, waited, seen is not None and bool(done(seen))

    UNVERIFIED = (
        "applied, but the result could not be verified: the cluster did not answer. "
        "Check with cluster_status before relying on it"
    )

    @app.tool(server, title="Put a host into maintenance", read_only=False)
    async def maintenance_enter(
        ctx: Context,
        host: Annotated[str, Field(description="The host to take down.")],
        approval: Annotated[ElicitationResult[Approval], Resolve(approve_maintenance)],
        stop_daemons: Annotated[
            bool,
            Field(
                description="Also stop the host's OSDs, where the deployment can (MicroCeph, "
                "cephadm). Otherwise only flags are set."
            ),
        ] = True,
        force: Annotated[
            bool,
            Field(
                description="Go ahead although the assessment is 'unsafe': data becomes "
                "unavailable. Needs --allow-destructive."
            ),
        ] = False,
        wait: Annotated[
            float, Field(ge=0, le=900, description="Seconds to wait for the cluster to settle.")
        ] = 180,
        confirm: Annotated[
            str,
            Field(description="The confirmation code from a previous call that returned the plan."),
        ] = "",
    ) -> dict[str, Any]:
        """Prepare a host for a reboot or hardware work, as one guarded operation:

        1. Check that it can go: every PG stays active, the monitors keep their
           quorum, and Ceph's own ok-to-stop agrees (the same assessment that
           simulate scenario='reboot' gives). If not, nothing is changed.
        2. Get approval. Where the client can ask its user, the operator sees
           the assessment and the commands, and says yes or no. Otherwise this
           tool returns the plan with a confirmation code; show the plan to the
           operator and call again with confirm=<code> to apply it.
        3. Apply it the way the deployment wants: MicroCeph and cephadm have a
           maintenance mode that sets noout and stops the daemons; elsewhere
           noout is set for the host and the operator stops the daemons.
        4. Wait until the host's OSDs are down and the PGs have settled, and
           verify that all of them still serve I/O.

        The change is journaled; maintenance_exit (or change_undo) reverses it.
        """
        policy.require("write", "putting a host into maintenance")
        plan = await plans.plan(host, stop_daemons, force)
        if not plan.safe:
            if not force:
                return compact(
                    {
                        "applied": False,
                        "status": "refused: taking this host down is not safe",
                        **plan.summary(),
                    }
                )
            policy.require("destructive", "forcing a host into maintenance against the assessment")
        if not guard.granted(approval, confirm, plan.code):
            return compact(
                {
                    "applied": False,
                    "status": "confirmation needed",
                    **plan.summary(),
                    "confirm": plan.code,
                    "next": "Show this plan to the operator. To go ahead, call maintenance_enter "
                    f"again with the same arguments and confirm='{plan.code}'.",
                }
            )
        if guard.by_person(approval):
            # The operator answered a question about one particular plan. If the
            # cluster has changed since it was put, this is a different plan.
            shown = plans.asked.pop((host, stop_daemons, force), None)
            if shown != plan.code:
                raise ToolError(
                    "Not applied: the cluster changed while the approval was pending, so "
                    "the plan is no longer the one that was approved. Call maintenance_enter "
                    "again to have the current plan approved."
                )
        plans.forget()
        client = guard.client_name(ctx)
        arguments = {"host": host, "stop_daemons": stop_daemons, "force": force}
        summary = f"host {host} into maintenance ({plan.deployment})"
        try:
            ran, outputs = await run_steps(plan.steps)
        except CephError as exc:
            await app.journal(
                "maintenance_enter",
                summary,
                arguments=arguments,
                commands=plan.commands(),
                outcome=f"failed: {str(exc)[:300]}",
                client=client,
            )
            raise
        change = await app.journal(
            "maintenance_enter",
            summary,
            arguments=arguments,
            commands=ran,
            undo=plan.undo,
            client=client,
        )

        expected = (plan.result.sections.get("data") or {}).get("pgs_losing_a_copy", 0)

        def settled(seen: dict[str, Any]) -> bool:
            if not plan.stops_daemons:
                return not seen["in_transition"]
            # PG states trail the OSDs by a few seconds: wait until they show the loss.
            return not seen["up"] and not seen["in_transition"] and seen["degraded"] >= expected

        seen, waited, ok = await wait_until(
            ctx, plan.osds, settled, wait, f"waiting for {host} to settle"
        )
        if seen is None:
            return compact(
                {
                    "applied": True,
                    "status": UNVERIFIED,
                    "managed_by": plan.deployment,
                    "ran": ran,
                    "output": "\n".join(outputs) or None,
                    "change": change,
                    "afterwards": f"maintenance_exit host='{host}' when the host is back",
                }
            )
        newly_inactive = max(0, seen["not_active"] - plan.not_active_before)
        problems = []
        if plan.stops_daemons and seen["up"]:
            problems.append(
                f"{len(seen['up'])} of the host's OSDs are still up after {waited:.0f}s"
            )
        if newly_inactive and plan.safe:
            problems.append(
                f"{newly_inactive} PGs are not active: leave maintenance and investigate"
            )
        if plan.stops_daemons:
            state = "its OSDs are down; the host can be powered off or rebooted"
        elif plan.osds:
            state = "noout is set for it; stop its daemons or reboot it now"
        else:
            state = "it holds no OSDs; it can go down"
        return compact(
            {
                "applied": True,
                "status": f"{host} is in maintenance: {state}"
                if not problems
                else "applied, but not as expected: " + "; ".join(problems),
                "managed_by": plan.deployment,
                "ran": ran,
                "output": "\n".join(outputs) or None,
                "verified": {
                    "osds_down": [f"osd.{i}" for i in seen["down"]],
                    "osds_still_up": [f"osd.{i}" for i in seen["up"]]
                    if plan.stops_daemons
                    else None,
                    "pgs_serving_io": f"{seen['pgs'] - seen['not_active']} of {seen['pgs']}",
                    "pgs_degraded": seen["degraded"],
                    "health": seen["health"],
                    "settled_after_s": waited if ok else None,
                },
                "change": change,
                "afterwards": f"maintenance_exit host='{host}' when the host is back",
                "notes": plan.notes
                + (
                    ["While the host is away nothing is re-replicated: do not leave it for long."]
                    if plan.osds
                    else []
                ),
            }
        )

    @app.tool(server, title="Take a host out of maintenance", read_only=False, idempotent=True)
    async def maintenance_exit(
        ctx: Context,
        host: Annotated[str, Field(description="The host that is back.")],
        wait: Annotated[
            float,
            Field(ge=0, le=1800, description="Seconds to wait for its OSDs and for recovery."),
        ] = 300,
        even_if_down: Annotated[
            bool,
            Field(
                description="Clear the flags although the host's OSDs are not up, e.g. because "
                "the host is not coming back. The cluster then starts re-creating its data."
            ),
        ] = False,
    ) -> dict[str, Any]:
        """End a host's maintenance: start its daemons where the deployment does
        that (MicroCeph, cephadm), wait for its OSDs to be up, clear the noout
        flags, wait for recovery to finish, and verify that the cluster is back
        to where it was. If the OSDs do not come up, the flags stay set (unless
        even_if_down) and the result says what is missing."""
        policy.require("write", "taking a host out of maintenance")
        word(host, "host name")
        state = await simulate.load_state(ceph)
        osds = sorted(simulate.resolve(state, [host], None))
        deployment = await plans.deployment()
        plans.forget()
        client = guard.client_name(ctx)
        arguments = {"host": host, "even_if_down": even_if_down}
        ran: list[str] = []
        outputs: list[str] = []
        waited = 0.0

        async def step(steps: list[Step]) -> None:
            done, out = await run_steps(steps)
            ran.extend(done)
            outputs.extend(out)

        try:
            if deployment == MICROCEPH:
                await step([["microceph", "cluster", "maintenance", "exit", host]])
            elif deployment == CEPHADM:
                await step([["ceph", "orch", "host", "maintenance", "exit", host]])
            seen, spent, up = await wait_until(
                ctx, osds, lambda s: not s["down"], wait, f"waiting for the OSDs of {host}"
            )
            waited += spent
            if deployment == PLAIN and osds:
                if not up and not even_if_down:
                    # Not known to be up (down, or the cluster did not say): the
                    # flag is what keeps their data from being re-created.
                    down = seen["down"] if seen else osds
                    return compact(
                        {
                            "applied": False,
                            "status": (
                                f"{len(down)} OSDs of {host} are still down"
                                if seen
                                else f"the state of the OSDs of {host} could not be read"
                            )
                            + "; noout stays set",
                            "osds_down": [f"osd.{i}" for i in down] if seen else None,
                            "next": "Start the host's Ceph daemons and call maintenance_exit "
                            "again, or pass even_if_down=true if the host is not coming back.",
                        }
                    )
                await step([["ceph", "osd", "unset-group", "noout", host]])
        except CephError as exc:
            await app.journal(
                "maintenance_exit",
                f"host {host} out of maintenance",
                arguments=arguments,
                commands=ran,
                outcome=f"failed: {str(exc)[:300]}",
                client=client,
            )
            raise
        # Leaving is the undo of entering: mark the entry it closes.
        store = await app.history.store()
        entered = next(
            (
                c.id
                for c in await app.history.call(store.changes)
                if c.tool == "maintenance_enter"
                and c.arguments.get("host") == host
                and c.outcome == "done"
                and not c.undone_by
            ),
            None,
        )
        change = await app.journal(
            "maintenance_exit",
            f"host {host} out of maintenance ({deployment})",
            arguments=arguments,
            commands=ran,
            client=client,
            undoes=entered,
        )
        seen, spent, clean = await wait_until(
            ctx,
            osds,
            lambda s: not s["down"] and not s["not_clean"],
            max(0.0, wait - waited),
            "waiting for recovery",
        )
        waited += spent
        if seen is None:
            return compact(
                {
                    "applied": True,
                    "status": UNVERIFIED,
                    "managed_by": deployment,
                    "ran": ran,
                    "output": "\n".join(outputs) or None,
                    "change": change,
                }
            )
        leftover = [f for f in seen["flags"] if f == "noout"]
        if seen["down"]:
            status = f"flags cleared, but {len(seen['down'])} OSDs of {host} are down"
        elif clean:
            status = f"{host} is back in service: all PGs active+clean"
        else:
            status = (
                f"{host} is back; recovery is still running ({seen['not_clean']} PGs not "
                "clean). cluster_status shows its progress"
            )
        return compact(
            {
                "applied": True,
                "status": status,
                "managed_by": deployment,
                "ran": ran,
                "output": "\n".join(outputs) or None,
                "verified": {
                    "osds_up": [f"osd.{i}" for i in seen["up"]],
                    "osds_down": [f"osd.{i}" for i in seen["down"]],
                    "pgs_not_clean": seen["not_clean"],
                    "health": seen["health"],
                    "waited_s": waited,
                },
                "change": change,
                "notes": [
                    "The cluster-wide noout flag is still set; clear it with cluster_flags if "
                    "no other maintenance is under way."
                ]
                if leftover
                else [],
            }
        )
