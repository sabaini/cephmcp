"""Guard rails for changes: who is asking, and who has to approve.

A guarded operation shows a person what is about to happen and waits for a
yes. Where the client can put a question to its user (MCP elicitation), the
server asks through it. Where it cannot, the tool returns its plan together
with a confirmation code, and applies the change only when called again with
that code: the agent has then at least seen the plan, and the client's own
permission prompt covers the second call.
"""

from __future__ import annotations

import hashlib
import secrets
from typing import TYPE_CHECKING, Any

from mcp.server.elicitation import (
    AcceptedElicitation,
    CancelledElicitation,
    DeclinedElicitation,
    ElicitationResult,
)
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.resolve import Elicit
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from .app import App

ASK_AGENT = "confirm"
"""Resolver outcome: no person can be asked; fall back to the confirmation code."""
NOT_NEEDED = "off"
_SALT = secrets.token_bytes(16)


class Approval(BaseModel):
    approve: bool = Field(description="Apply this change to the cluster?")


def app_of(ctx: Context[Any, Any]) -> App:
    """The App behind a request (handed out by the server's lifespan)."""
    return ctx.request_context.lifespan_context


def client_name(ctx: Context[Any, Any] | None) -> str:
    """'claude-code 2.1' or similar; '' when the client did not introduce itself."""
    if ctx is None:
        return ""
    try:
        params = ctx.session.client_params
    except (AttributeError, LookupError, ValueError):
        return ""
    info = getattr(params, "client_info", None)
    if info is None:
        return ""
    return f"{info.name} {info.version or ''}".strip()


def can_ask(ctx: Context[Any, Any]) -> bool:
    """Whether the client declared that it can put a form question to its user."""
    try:
        capabilities = ctx.client_capabilities
    except (AttributeError, LookupError, ValueError):
        return False
    elicitation = capabilities.elicitation if capabilities is not None else None
    return elicitation is not None and (elicitation.form is not None or elicitation.url is None)


def code_for(*parts: str) -> str:
    """A confirmation code bound to a plan: it changes when the plan does, and
    cannot be guessed without having been shown it by this server process."""
    digest = hashlib.sha256(_SALT)
    for part in parts:
        digest.update(b"\0" + part.encode())
    return digest.hexdigest()[:8]


def request(ctx: Context[Any, Any], question: str) -> Elicit[Approval] | str:
    """What an approval resolver returns: the question for the person, or a
    marker for the tool when no person is to be asked."""
    app = app_of(ctx)
    mode = app.settings.approval
    if mode == "off":
        return NOT_NEEDED
    if can_ask(ctx):
        return Elicit(question, Approval)
    if mode == "required":
        raise ToolError(
            "This server requires a person to approve this operation, and the client "
            "cannot ask one (it does not support MCP elicitation). Use a client that "
            "does, or let the operator run it."
        )
    return ASK_AGENT


def by_person(decision: ElicitationResult[Any]) -> bool:
    """Whether the decision is a person's answer, as opposed to a marker
    saying that nobody was asked."""
    return isinstance(decision, AcceptedElicitation) and isinstance(decision.data, Approval)


def granted(decision: ElicitationResult[Any], confirm: str, code: str) -> bool:
    """Whether a guarded operation may go ahead.

    False means: show the plan and ask the caller to repeat the call with the
    confirmation code. A person saying no raises instead.
    """
    if isinstance(decision, DeclinedElicitation | CancelledElicitation):
        raise ToolError("Not applied: the operator did not approve it.")
    answer = decision.data if isinstance(decision, AcceptedElicitation) else decision
    if isinstance(answer, Approval):
        if not answer.approve:
            raise ToolError("Not applied: the operator did not approve it.")
        return True
    if answer == NOT_NEEDED:
        return True
    return bool(confirm) and secrets.compare_digest(confirm, code)
