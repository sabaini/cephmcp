"""The journal of changes made through this server, and undoing them."""

from __future__ import annotations

from typing import Annotated, Any

from mcp.server.mcpserver import Context, MCPServer
from pydantic import Field

from .. import guard, timeline
from ..app import App, UsageError
from ..ceph import CephError
from ..history import Change
from ..policy import classify_microceph
from ..redact import redact_text
from ..render import compact, table
from .history import window

# The arguments that say what a change was made to, as opposed to what it set.
SUBJECT = ("pool", "name", "osd", "who", "option", "flag", "targets", "host")


def subject(change: Change) -> tuple[Any, ...]:
    return (change.tool, *[str(change.arguments.get(k)) for k in SUBJECT])


def register(server: MCPServer, app: App) -> None:
    history = app.history

    def shown(command: str) -> str:
        return command if app.settings.allow_secrets else redact_text(command)

    def undo_state(change: Change) -> str:
        if change.undone_by:
            return f"undone by change {change.undone_by}"
        if change.undoes:
            return f"is the undo of change {change.undoes}"
        if change.outcome != "done":
            return "nothing to undo"
        return "available" if change.undo else "not automatic"

    @app.tool(server, title="Changes made through this server")
    async def change_log(
        change_id: Annotated[
            int | None,
            Field(description="Show this change in full: arguments, commands, undo commands."),
        ] = None,
        since: Annotated[
            str, Field(description="How far back to list, e.g. '-1d' or '-30d'.")
        ] = "-30d",
        limit: Annotated[int, Field(ge=1, le=500)] = 30,
    ) -> dict[str, Any]:
        """The journal of what this server changed on the cluster, newest first:
        when, through which tool, for which client, the commands that were run,
        whether it worked, and whether it can be undone with change_undo.

        It covers changes made through this server only. For everything that
        happened to the cluster, whoever did it, use what_changed.
        """
        store = await history.store()
        if change_id is not None:
            change = await history.call(store.change, change_id)
            if change is None:
                raise UsageError(f"there is no change {change_id}")
            return compact(
                {
                    "change": change.id,
                    "time": timeline.fmt_time(change.ts),
                    "tool": change.tool,
                    "summary": shown(change.summary),
                    "outcome": shown(change.outcome),
                    "client": change.client,
                    "arguments": change.arguments,
                    "commands": [shown(c) for c in change.commands],
                    "undo": undo_state(change),
                    "undo_commands": [shown(" ".join(step)) for step in change.undo],
                }
            )
        start, end = window(since, "now", app.clock())
        changes = await history.call(store.changes, start, end, limit + 1)
        rows = [
            {
                "change": c.id,
                "time": timeline.fmt_time(c.ts),
                "tool": c.tool,
                "summary": shown(c.summary),
                "outcome": shown(c.outcome),
                "undo": undo_state(c),
                "client": c.client or None,
            }
            for c in changes
        ]
        out = table(rows[:limit])
        out["count"] = min(len(rows), limit)
        if len(rows) > limit:
            # One more than asked for was fetched, to know whether there is more.
            out["truncated"] = f"the newest {limit} are shown; raise limit or shorten since"
        if not rows:
            out["note"] = (
                "No changes on record"
                + ("" if app.settings.state_dir else " (the journal is kept in memory only)")
                + "."
            )
        return out

    if not app.settings.admin:
        return

    @app.tool(server, title="Undo a change", read_only=False)
    async def change_undo(
        ctx: Context,
        change_id: Annotated[int, Field(description="The change to undo, from change_log.")],
    ) -> dict[str, Any]:
        """Undo a change made through this server, by running the commands that
        were recorded for restoring the previous state: the previous value of
        a pool or configuration setting, the previous weight of an OSD, the
        opposite flag or in/out state, leaving maintenance.

        Changes are undone newest first: a change cannot be undone while a
        later change to the same thing stands. Creating or deleting a pool, and
        free-form commands, have no automatic undo. The undo is journaled itself.
        """
        app.policy.require("write", "undoing a change")
        store = await history.store()
        change = await history.call(store.change, change_id)
        if change is None:
            raise UsageError(f"there is no change {change_id}; change_log lists them")
        if change.outcome != "done":
            raise UsageError(f"change {change_id} did not succeed ({change.outcome})")
        if change.undone_by:
            raise UsageError(f"change {change_id} was already undone, by change {change.undone_by}")
        if not change.undo:
            raise UsageError(
                f"no automatic undo is recorded for change {change_id} ({change.tool}: "
                f"{change.summary})"
            )
        later = [
            c
            for c in await history.call(store.changes, change.ts, float("inf"), 1000)
            if c.id > change.id
            and c.outcome == "done"
            and not c.undone_by
            and not c.undoes
            and subject(c) == subject(change)
        ]
        if later:
            raise UsageError(
                f"change {later[0].id} was made later to the same thing ({later[0].summary}); "
                "undo that one first"
            )
        ran = []
        outputs = []
        client = guard.client_name(ctx)
        summary = f"undo of change {change_id}: {change.summary}"
        try:
            for step in change.undo:
                tool, args = step[0], list(step[1:])
                ran.append(" ".join(step))
                if tool == "microceph":
                    verdict = classify_microceph(args)
                    app.policy.require(verdict.level, f"`microceph {verdict.command}`")
                    result = await app.microceph.run(args, timeout=600)
                else:
                    await app.policy.check_ceph(args)
                    result = await app.ceph.run(args, timeout=300)
                output = (result.stdout.strip() + "\n" + result.stderr.strip()).strip()
                if output:
                    outputs.append(output)
        except CephError as exc:
            await app.journal(
                "change_undo",
                summary,
                commands=ran,
                outcome=f"failed: {str(exc)[:300]}",
                client=client,
            )
            raise
        number = await app.journal(
            "change_undo", summary, commands=ran, client=client, undoes=change_id
        )
        return compact(
            {
                "undone": change_id,
                "ran": [shown(c) for c in ran],
                "output": "\n".join(outputs) or None,
                "change": number,
            }
        )
