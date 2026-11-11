"""Runbooks as MCP prompts (for people) and as a tool (for agents)."""

from __future__ import annotations

from typing import Annotated, Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.prompts.base import Prompt, PromptArgument
from pydantic import Field

from ..app import App, UsageError
from ..runbooks import BY_NAME, RUNBOOKS, Runbook, Situation


def situation(app: App) -> Situation:
    return Situation(
        admin=app.settings.admin,
        destructive=app.settings.destructive,
        microceph=app.microceph.available,
    )


def register(server: MCPServer, app: App) -> None:
    where = situation(app)

    def prompt_for(runbook: Runbook) -> Prompt:
        def render(**arguments: Any) -> str:
            return runbook.render(where, str(arguments.get(runbook.argument) or ""))

        arguments = (
            [PromptArgument(name=runbook.argument, description=runbook.argument_help)]
            if runbook.argument
            else []
        )
        return Prompt(
            name=runbook.name,
            title=runbook.title,
            description=runbook.description,
            arguments=arguments,
            fn=render,
        )

    for runbook in RUNBOOKS:
        server.add_prompt(prompt_for(runbook))

    @app.tool(server, title="Runbooks")
    async def runbook(
        name: Annotated[
            str,
            Field(
                description="Which runbook: "
                + ", ".join(r.name for r in RUNBOOKS)
                + ". Leave empty for the list."
            ),
        ] = "",
        subject: Annotated[
            str,
            Field(
                description="What the runbook is about: the symptom, host, OSD id, time "
                "span or version it takes as its argument."
            ),
        ] = "",
    ) -> Any:
        """Step-by-step procedures for common situations: triaging a problem,
        investigating slowness, taking a host down for maintenance, writing an
        incident report, planning capacity, a routine check-up, checking
        upgrade readiness and replacing a disk.

        A runbook says which tools to call in which order and how to report
        the result. Fetch the matching one before starting such a task and
        follow it. The same runbooks are offered to people as prompts.
        """
        if not name:
            return {
                "runbooks": [
                    {
                        "name": r.name,
                        "purpose": r.description,
                        "subject": r.argument_help or None,
                    }
                    for r in RUNBOOKS
                ]
            }
        found = BY_NAME.get(name.strip().lower().replace("-", "_"))
        if found is None:
            raise UsageError(f"no runbook named {name!r}; there are: {', '.join(BY_NAME)}")
        return found.render(where, subject)
