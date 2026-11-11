"""Server settings, from environment variables and command-line flags."""

from __future__ import annotations

import os
import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

Mode = Literal["read-only", "admin"]
Approval = Literal["auto", "required", "off"]

ENV_PREFIX = "CEPHMCP_"


@dataclass(frozen=True)
class Settings:
    # --- what agents are allowed to do ---
    mode: Mode = "read-only"
    """read-only: inspection and metrics only. admin: also state-changing tools."""
    allow_destructive: bool = False
    """Admin mode only: also allow operations that can lose data or redundancy."""
    allow_secrets: bool = False
    """Return keys and other secrets unredacted, and allow commands that print them."""
    approval: Approval = "auto"
    """Who confirms guarded operations (host maintenance, deleting a pool).
    auto: a person, asked through the client where it can ask; otherwise the
    agent repeats the call with a confirmation code after showing the plan.
    required: a person, or the operation is refused. off: nobody."""

    # --- how to reach the cluster ---
    ceph_cmd: tuple[str, ...] = ()
    """argv prefix for the ceph CLI; empty means autodetect (ceph, microceph.ceph)."""
    microceph_cmd: tuple[str, ...] = ()
    """argv prefix for the microceph CLI; empty means autodetect."""
    ceph_conf: str | None = None
    ceph_keyring: str | None = None
    ceph_name: str | None = None
    command_timeout: float = 30.0
    connect_timeout: int = 10

    # --- metrics ---
    mgr_prometheus_url: str | None = None
    """Override for the mgr prometheus module endpoint; default is discovery."""
    ceph_exporter_urls: tuple[str, ...] = ()
    """Explicit ceph-exporter endpoints; default is discovery."""
    ceph_exporter_port: int = 9926
    discover_exporters: bool = True
    prometheus_url: str | None = None
    """Prometheus server for PromQL; default is discovery through the orchestrator."""
    prometheus_username: str | None = None
    prometheus_password: str | None = None
    http_timeout: float = 10.0
    tls_verify: bool = True

    # --- history ---
    state_dir: str | None = None
    """Directory for the history file and the change journal; None keeps them
    in memory for the life of the process."""
    record: bool = False
    """Sample the cluster in the background while the server runs."""
    record_interval: float = 60.0
    history_days: float = 90.0
    """How long hourly averages are kept. Full resolution is kept for two days."""
    history_max_osds: int = 500
    """Above this many OSDs, per-OSD series are left out (per-host ones remain)."""

    # --- output ---
    max_output_chars: int = 40_000
    """Upper bound for one tool result. Keeps a single call from flooding the context."""

    @property
    def admin(self) -> bool:
        return self.mode == "admin"

    @property
    def destructive(self) -> bool:
        return self.admin and self.allow_destructive


def _flag(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _argv(value: str) -> tuple[str, ...]:
    return tuple(shlex.split(value))


def _urls(value: str) -> tuple[str, ...]:
    return tuple(u for u in value.replace(",", " ").split() if u)


# Environment variable suffix -> (Settings field, parser)
_ENV = {
    "MODE": ("mode", str),
    "ALLOW_DESTRUCTIVE": ("allow_destructive", _flag),
    "ALLOW_SECRETS": ("allow_secrets", _flag),
    "APPROVAL": ("approval", str),
    "CEPH_CMD": ("ceph_cmd", _argv),
    "MICROCEPH_CMD": ("microceph_cmd", _argv),
    "CEPH_CONF": ("ceph_conf", str),
    "CEPH_KEYRING": ("ceph_keyring", str),
    "CEPH_NAME": ("ceph_name", str),
    "COMMAND_TIMEOUT": ("command_timeout", float),
    "CONNECT_TIMEOUT": ("connect_timeout", int),
    "MGR_PROMETHEUS_URL": ("mgr_prometheus_url", str),
    "CEPH_EXPORTER_URLS": ("ceph_exporter_urls", _urls),
    "CEPH_EXPORTER_PORT": ("ceph_exporter_port", int),
    "DISCOVER_EXPORTERS": ("discover_exporters", _flag),
    "PROMETHEUS_URL": ("prometheus_url", str),
    "PROMETHEUS_USERNAME": ("prometheus_username", str),
    "PROMETHEUS_PASSWORD": ("prometheus_password", str),
    "HTTP_TIMEOUT": ("http_timeout", float),
    "TLS_VERIFY": ("tls_verify", _flag),
    "MAX_OUTPUT_CHARS": ("max_output_chars", int),
    "STATE_DIR": ("state_dir", str),
    "RECORD": ("record", _flag),
    "RECORD_INTERVAL": ("record_interval", float),
    "HISTORY_DAYS": ("history_days", float),
    "HISTORY_MAX_OSDS": ("history_max_osds", int),
}


def default_state_dir(environ: Mapping[str, str] | None = None) -> str:
    """$XDG_STATE_HOME/cephmcp, by default ~/.local/state/cephmcp."""
    environ = os.environ if environ is None else environ
    base = environ.get("XDG_STATE_HOME") or os.path.join(
        environ.get("HOME") or os.path.expanduser("~"), ".local", "state"
    )
    return os.path.join(base, "cephmcp")


def env_values(environ: Mapping[str, str] | None = None) -> dict[str, object]:
    """Settings fields given through CEPHMCP_* environment variables."""
    environ = os.environ if environ is None else environ
    values: dict[str, object] = {}
    for suffix, (field, parse) in _ENV.items():
        raw = environ.get(ENV_PREFIX + suffix)
        if raw is None or raw == "":
            continue
        try:
            values[field] = parse(raw)
        except ValueError as exc:
            raise ValueError(f"invalid {ENV_PREFIX}{suffix}={raw!r}: {exc}") from exc
    return values


def validate(settings: Settings) -> Settings:
    if settings.mode not in ("read-only", "admin"):
        raise ValueError(f"mode must be 'read-only' or 'admin', not {settings.mode!r}")
    if settings.approval not in ("auto", "required", "off"):
        raise ValueError(f"approval must be auto, required or off, not {settings.approval!r}")
    if settings.allow_destructive and not settings.admin:
        raise ValueError("allow-destructive requires mode 'admin'")
    if settings.command_timeout <= 0 or settings.http_timeout <= 0:
        raise ValueError("timeouts must be positive")
    if settings.record_interval < 5:
        raise ValueError("record-interval must be at least 5 seconds")
    if settings.history_days < 1:
        raise ValueError("history-days must be at least 1")
    if settings.max_output_chars < 1000:
        raise ValueError("max-output-chars must be at least 1000")
    return settings
