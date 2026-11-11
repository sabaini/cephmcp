"""State-changing tools. Registered only when the server runs in admin mode.

Every change is written to the journal with the commands that were run and,
where the previous state is known, the commands that restore it (change_log,
change_undo).
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from mcp.server.elicitation import ElicitationResult
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.resolve import Elicit, Resolve
from pydantic import Field

from .. import guard
from ..app import App, UsageError
from ..ceph import CephError, gather
from ..guard import Approval
from ..redact import REDACTED, is_sensitive_name
from ..render import compact
from ..topology import build
from .common import config_option, config_value, config_who, osd_id, pool_name, word
from .storage import osd_rows, pool_rows

ClusterFlag = Literal[
    "noout",
    "noin",
    "nodown",
    "noup",
    "norebalance",
    "norecover",
    "nobackfill",
    "noscrub",
    "nodeep-scrub",
]
GROUP_FLAGS = ("noout", "noin", "nodown", "noup")
QUOTAS = {"quota_max_bytes": "max_bytes", "quota_max_objects": "max_objects"}
Step = list[str]
"""One command as argv, starting with the tool: ['ceph', 'osd', 'unset', 'noout']."""


async def apply(
    app: App,
    ctx: Context[Any, Any] | None,
    tool: str,
    summary: str,
    arguments: dict[str, Any],
    commands: list[list[str]],
    undo: list[Step] | None = None,
    *,
    timeout: float | None = None,
    undoes: int | None = None,
    note: str = "",
) -> dict[str, Any]:
    """Run ceph commands as one journaled change. The journal keeps the
    commands as they were run; what is returned to the model is masked."""
    ran: list[str] = []
    outputs: list[str] = []
    client = guard.client_name(ctx)
    try:
        for args in commands:
            ran.append("ceph " + " ".join(args))
            result = await app.ceph.run(args, timeout=timeout)
            output = (result.stdout.strip() + "\n" + result.stderr.strip()).strip()
            if output:
                outputs.append(output)
    except CephError as exc:
        await app.journal(
            tool,
            summary,
            arguments=arguments,
            commands=ran,
            outcome=f"failed: {str(exc)[:300]}",
            client=client,
            undoes=None,
        )
        raise
    change = await app.journal(
        tool,
        summary,
        arguments=arguments,
        commands=ran,
        undo=undo or [],
        client=client,
        undoes=undoes,
    )
    shown = [app.scrub(command) for command in ran]
    result = {
        "ran": shown[0] if len(shown) == 1 else shown,
        "output": app.scrub("\n".join(outputs)) or None,
        "change": change,
        "undo": f"change_undo change_id={change}" if undo and change else None,
        "note": note or None,
    }
    return {key: value for key, value in result.items() if value is not None}


UNKNOWN_BEFORE = "The state before this change could not be read, so it has no automatic undo."


def group_flags(df_tree: Any, osd_dump: Any) -> dict[str, list[str]] | None:
    """Flags set on single OSDs, CRUSH buckets and device classes, by name;
    None if the cluster did not say."""
    if not isinstance(df_tree, dict) or not isinstance(osd_dump, dict):
        return None
    return build(df_tree, osd_dump, [], None).scoped_flags


async def approve_pool_delete(ctx: Context, pool: str) -> Elicit[Approval] | str:
    """Ask the operator before a pool goes."""
    return guard.request(ctx, f"Delete pool '{pool}' and all data in it? This cannot be undone.")


def register(server: MCPServer, app: App) -> None:
    ceph = app.ceph
    policy = app.policy

    @app.tool(server, title="Set or clear OSD flags", read_only=False, idempotent=True)
    async def cluster_flags(
        ctx: Context,
        action: Literal["set", "unset"],
        flag: Annotated[
            ClusterFlag,
            Field(
                description="noout: do not mark stopped OSDs out (no data movement during "
                "maintenance). norebalance, nobackfill, norecover: hold back data movement. "
                "noscrub, nodeep-scrub: pause scrubbing. noin, noup, nodown: freeze OSD state."
            ),
        ],
        targets: Annotated[
            list[str] | None,
            Field(
                description="Limit the flag to these OSDs ('osd.3') or CRUSH buckets such as "
                "host names. Only for noout, noin, nodown and noup. Default: whole cluster."
            ),
        ] = None,
    ) -> dict[str, Any]:
        """Set or clear a cluster flag, e.g. noout before taking a host down for
        maintenance and unset afterwards. Prefer limiting noout to the affected
        host with `targets`, or use maintenance_enter, which also checks that
        the host can go. Flags stay set until cleared and raise a health
        warning while set."""
        policy.require("write", "changing cluster flags")
        arguments = {"action": action, "flag": flag, "targets": targets}
        reverse = "unset" if action == "set" else "set"
        if targets:
            if flag not in GROUP_FLAGS:
                raise UsageError(f"targets is only supported for {', '.join(GROUP_FLAGS)}")
            names = [word(t, "OSD or CRUSH bucket name") for t in targets]
            df_tree, osd_dump = await gather(
                lambda: ceph.try_json(["osd", "df", "tree"]),
                lambda: ceph.try_json(["osd", "dump"]),
            )
            state = group_flags(df_tree, osd_dump)
            summary = f"{action} {flag} on {', '.join(names)}"
            undo: list[Step] = []
            if state is not None:
                # Undo must leave alone what was already as asked: a flag that
                # somebody else set stays theirs to clear.
                def has(target: str) -> bool:
                    key = f"osd.{target}" if target.isdigit() else target
                    return flag in state.get(key, [])

                changed = [t for t in names if has(t) != (action == "set")]
                unchanged = [t for t in names if t not in changed]
                if changed:
                    undo = [["ceph", "osd", f"{reverse}-group", flag, *changed]]
                if unchanged:
                    was = "had" if action == "set" else "lacked"
                    summary += f" ({', '.join(unchanged)} already {was} it)"
            return await apply(
                app,
                ctx,
                "cluster_flags",
                summary,
                arguments,
                [["osd", f"{action}-group", flag, *names]],
                undo,
                note=UNKNOWN_BEFORE if state is None else "",
            )
        before = await ceph.try_json(["osd", "dump"])
        if not isinstance(before, dict):
            return await apply(
                app,
                ctx,
                "cluster_flags",
                f"{action} {flag}",
                arguments,
                [["osd", action, flag]],
                note=UNKNOWN_BEFORE,
            )
        was_set = flag in (before.get("flags_set") or [])
        changes = was_set != (action == "set")
        return await apply(
            app,
            ctx,
            "cluster_flags",
            f"{action} {flag}" + ("" if changes else " (it already was)"),
            arguments,
            [["osd", action, flag]],
            [["ceph", "osd", reverse, flag]] if changes else [],
        )

    @app.tool(server, title="Change an OSD", read_only=False, idempotent=True)
    async def osd_set(
        ctx: Context,
        osd: Annotated[int, Field(ge=0, description="OSD id, e.g. 3 for osd.3.")],
        action: Annotated[
            Literal["in", "out", "reweight", "crush-reweight", "primary-affinity"],
            Field(
                description="out: stop placing data on the OSD (its data migrates away). "
                "in: bring it back. reweight: override weight 0-1 for balancing. "
                "crush-reweight: CRUSH weight, normally the size in TiB. "
                "primary-affinity: 0-1, how often the OSD acts as primary."
            ),
        ],
        value: Annotated[
            float | None, Field(ge=0, description="New value for the weight actions.")
        ] = None,
    ) -> dict[str, Any]:
        """Mark an OSD in or out, or change its weights. Marking out and changing
        weights move data and cause recovery load; simulate scenario='drain'
        tells beforehand where the data goes and whether there is room. Before
        stopping or removing an OSD, check ceph_command ['osd', 'ok-to-stop',
        '<id>'] and ['osd', 'safe-to-destroy', '<id>']."""
        policy.require("write", "changing an OSD")
        n = osd_id(osd)
        if action not in ("in", "out"):
            if value is None:
                raise UsageError(f"{action} needs a value")
            if action != "crush-reweight" and value > 1:
                raise UsageError(f"{action} takes a value between 0 and 1")
        tree, dump = await gather(
            lambda: ceph.try_json(["osd", "df", "tree"]),
            lambda: ceph.try_json(["osd", "dump"]),
        )
        node = next(
            (x for x in (tree or {}).get("nodes") or [] if x.get("type") == "osd" and x["id"] == n),
            None,
        )
        state = next((o for o in (dump or {}).get("osds") or [] if o.get("osd") == n), None)
        undo: list[Step] = []
        if action in ("in", "out"):
            commands = [["osd", action, str(n)]]
            was_in = bool(state.get("in")) if state else None
            if was_in is not None and was_in != (action == "in"):
                undo = [["ceph", "osd", "in" if was_in else "out", str(n)]]
            summary = f"mark osd.{n} {action}"
        else:
            assert value is not None
            if action == "crush-reweight":
                commands = [["osd", "crush", "reweight", f"osd.{n}", f"{value:g}"]]
                previous = node.get("crush_weight") if node else None
                if previous is not None:
                    undo = [["ceph", "osd", "crush", "reweight", f"osd.{n}", f"{previous:.6g}"]]
            else:
                commands = [["osd", action, str(n), f"{value:g}"]]
                key = "weight" if action == "reweight" else "primary_affinity"
                previous = state.get(key) if state else None
                if previous is not None:
                    undo = [["ceph", "osd", action, str(n), f"{previous:.6g}"]]
            was = f" (was {previous:g})" if isinstance(previous, int | float) else ""
            summary = f"osd.{n} {action} {value:g}{was}"
        done = await apply(
            app,
            ctx,
            "osd_set",
            summary,
            {"osd": n, "action": action, "value": value},
            commands,
            undo,
            note="" if state is not None and node is not None else UNKNOWN_BEFORE,
        )
        after = await ceph.try_json(["osd", "df", "tree"], {})
        done["osd"] = next((r for r in osd_rows(after or {}, None) if r["id"] == n), None)
        return compact(done)

    @app.tool(server, title="Create a pool", read_only=False)
    async def pool_create(
        ctx: Context,
        name: Annotated[str, Field(description="Name of the new pool.")],
        application: Annotated[
            str,
            Field(description="What will use the pool: 'rbd', 'cephfs', 'rgw' or a custom name."),
        ],
        pool_type: Literal["replicated", "erasure"] = "replicated",
        size: Annotated[
            int | None,
            Field(
                ge=1,
                le=10,
                description="Number of replicas (replicated pools). Default: cluster default.",
            ),
        ] = None,
        erasure_code_profile: Annotated[
            str | None, Field(description="Erasure-code profile (erasure pools).")
        ] = None,
        crush_rule: Annotated[
            str | None, Field(description="CRUSH rule name (replicated pools).")
        ] = None,
        pg_num: Annotated[
            int | None,
            Field(
                ge=1, le=65536, description="Initial PG count. Default: let the autoscaler decide."
            ),
        ] = None,
        bulk: Annotated[
            bool, Field(description="Expect a large pool: start with many PGs.")
        ] = False,
    ) -> dict[str, Any]:
        """Create a storage pool and tag it with its application."""
        policy.require("write", "creating a pool")
        pool_name(name)
        word(application, "application name")
        args = ["osd", "pool", "create", name, f"--pool_type={pool_type}"]
        if pg_num:
            args.append(f"--pg_num={pg_num}")
        if pool_type == "erasure":
            if size or crush_rule:
                raise UsageError("size and crush_rule do not apply to erasure-coded pools")
            if erasure_code_profile:
                args.append(f"--erasure_code_profile={word(erasure_code_profile, 'profile name')}")
        else:
            if erasure_code_profile:
                raise UsageError("erasure_code_profile needs pool_type='erasure'")
            if size == 1:
                policy.require("destructive", "creating a pool without redundancy (size 1)")
            if size:
                args.append(f"--size={size}")
            if crush_rule:
                args.append(f"--rule={word(crush_rule, 'CRUSH rule name')}")
        if bulk:
            args.append("--bulk")
        done = await apply(
            app,
            ctx,
            "pool_create",
            f"create {pool_type} pool {name} for {application}",
            {"name": name, "application": application, "pool_type": pool_type, "size": size},
            [args, ["osd", "pool", "application", "enable", name, application]],
        )
        detail, rules = await gather(
            lambda: ceph.try_json(["osd", "pool", "ls", "detail"], []),
            lambda: ceph.try_json(["osd", "crush", "rule", "dump"], []),
        )
        mine = [p for p in detail or [] if p.get("pool_name") == name]
        row = pool_rows(mine, {}, None, rules)
        done["pool"] = compact(row[0]) if row else None
        return compact(done)

    @app.tool(server, title="Change a pool setting", read_only=False, idempotent=True)
    async def pool_set(
        ctx: Context,
        pool: Annotated[str, Field(description="Pool name.")],
        option: Annotated[
            str,
            Field(
                description="Setting to change, e.g. size, min_size, pg_num, pg_autoscale_mode "
                "(on/off/warn), crush_rule, compression_mode, bulk, nodelete, "
                "quota_max_bytes, quota_max_objects (0 removes a quota), or "
                "'application' to tag the pool."
            ),
        ],
        value: Annotated[str, Field(description="New value.")],
    ) -> dict[str, Any]:
        """Change one setting of a pool. Changing size, crush_rule or pg_num moves
        data. Ceph refuses a size of 1 unless explicitly forced, and so does
        this tool."""
        policy.require("write", "changing a pool")
        pool_name(pool)
        word(option, "pool option")
        if not value or "\n" in value or (value.startswith("-") and not value[1:].isdigit()):
            raise UsageError("value must be a single plain value")
        arguments = {"pool": pool, "option": option, "value": value}
        if option == "application":
            command = ["osd", "pool", "application", "enable", pool, word(value, "name")]
            return compact(
                await apply(
                    app, ctx, "pool_set", f"tag pool {pool} as {value}", arguments, [command]
                )
            )
        undo: list[Step] = []
        previous: Any = None
        if option in QUOTAS:
            quota = await ceph.try_json(["osd", "pool", "get-quota", pool])
            previous = quota.get(option) if isinstance(quota, dict) else None
            if previous is not None:
                undo = [["ceph", "osd", "pool", "set-quota", pool, QUOTAS[option], str(previous)]]
            commands = [["osd", "pool", "set-quota", pool, QUOTAS[option], value]]
        else:
            current = await ceph.try_json(["osd", "pool", "get", pool, option])
            previous = current.get(option) if isinstance(current, dict) else None
            if isinstance(previous, bool):
                previous = "true" if previous else "false"
            if previous is not None and not (option == "size" and str(previous) == "1"):
                undo = [["ceph", "osd", "pool", "set", pool, option, f"--val={previous}"]]
            if option == "size" and value == "1":
                policy.require("destructive", "removing a pool's redundancy (size 1)")
                commands = [["osd", "pool", "set", pool, "size", "1", "--yes-i-really-mean-it"]]
            else:
                commands = [["osd", "pool", "set", pool, option, f"--val={value}"]]
        was = f" (was {previous})" if previous is not None else ""
        done = await apply(
            app,
            ctx,
            "pool_set",
            f"pool {pool}: {option} = {value}{was}",
            arguments,
            commands,
            undo,
            note="" if previous is not None else UNKNOWN_BEFORE,
        )
        done["previous"] = previous
        return compact(done)

    @app.tool(server, title="Change configuration", read_only=False, idempotent=True)
    async def config_set(
        ctx: Context,
        who: Annotated[
            str,
            Field(
                description="Scope: 'global', a daemon type ('osd', 'mon', 'mgr', 'mds', "
                "'client'), one daemon ('osd.3'), or a type with a mask ('osd/class:ssd')."
            ),
        ],
        option: Annotated[str, Field(description="Option name, e.g. 'osd_max_backfills'.")],
        value: Annotated[str, Field(description="New value.")] = "",
        remove: Annotated[
            bool,
            Field(description="Remove the setting instead, returning the option to its default."),
        ] = False,
    ) -> dict[str, Any]:
        """Set or remove an option in the cluster configuration database. Most
        options apply at runtime; config_get on the option tells whether a
        daemon restart is needed. Returns the previous and the new value, so
        check that the change took: some options are managed by Ceph itself
        and revert."""
        policy.require("write", "changing configuration")
        config_who(who)
        config_option(option)
        if remove == bool(value):
            raise UsageError("give either a value to set, or remove=true")
        if "\n" in value:
            raise UsageError("value must be a single line")
        scope, _, mask = who.partition("/")
        before, dump = await gather(
            lambda: config_value(ceph, scope, option, check=False),
            lambda: ceph.try_json(["config", "dump"]),
        )
        known = isinstance(dump, list)
        # What undo restores is the entry in the database for exactly this
        # scope, which is not the same as the value in effect.
        stored = next(
            (
                e.get("value")
                for e in dump or []
                if e.get("section") == scope
                and (e.get("mask") or "") == mask
                and e.get("name") == option
            ),
            None,
        )
        if not known:
            # Whether an entry existed is unknown: removing it on undo could
            # uncover a different value than the one that was in force.
            undo: list[Step] = []
        elif stored is not None:
            undo = [["ceph", "config", "set", who, option, f"--value={stored}"]]
        else:
            undo = [] if remove else [["ceph", "config", "rm", who, option]]
        if remove:
            commands, summary = [["config", "rm", who, option]], f"remove {option} from {who}"
        else:
            commands = [["config", "set", who, option, f"--value={value}"]]
            written = REDACTED if is_sensitive_name(option) else value
            summary = f"set {option} = {written} for {who}"
        done = await apply(
            app,
            ctx,
            "config_set",
            summary,
            {"who": who, "option": option, "value": value, "remove": remove},
            commands,
            undo,
            note="" if known else UNKNOWN_BEFORE,
        )
        now = await config_value(ceph, scope, option, check=False)
        if is_sensitive_name(option) and not app.settings.allow_secrets:
            # The option holds a secret: neither the old nor the new value is shown.
            before = REDACTED if before is not None else None
            now = REDACTED if now is not None else None
        done["previous"] = before
        done["now"] = now
        return done

    if not app.settings.allow_destructive:
        return

    @app.tool(server, title="Delete a pool", read_only=False, destructive=True)
    async def pool_delete(
        ctx: Context,
        pool: Annotated[str, Field(description="Pool to delete.")],
        confirm: Annotated[
            str, Field(description="The pool name again, to confirm that all its data may go.")
        ],
        approval: Annotated[ElicitationResult[Approval], Resolve(approve_pool_delete)],
    ) -> dict[str, Any]:
        """Delete a pool and all data in it. This cannot be undone. Where the
        client can ask its user, the server asks for approval first."""
        policy.require("destructive", "deleting a pool")
        pool_name(pool)
        if confirm != pool:
            raise UsageError("confirm must repeat the pool name exactly")
        # Without a person to ask, the repeated pool name is the confirmation.
        guard.granted(approval, confirm, pool)
        try:
            return compact(
                await apply(
                    app,
                    ctx,
                    "pool_delete",
                    f"delete pool {pool}",
                    {"pool": pool},
                    [["osd", "pool", "delete", pool, pool, "--yes-i-really-really-mean-it"]],
                )
            )
        except CephError as exc:
            if "mon_allow_pool_delete" in str(exc):
                raise CephError(
                    f"{exc}\nPool deletion is switched off cluster-wide. Enable it with "
                    "config_set who='mon' option='mon_allow_pool_delete' value='true', "
                    "and consider switching it off again afterwards."
                ) from None
            raise
