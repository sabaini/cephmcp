"""What an agent may run, depending on the server mode.

Three levels:

  read         allowed always
  write        needs --mode admin
  destructive  needs --mode admin --allow-destructive

Typed tools carry their level in code. This module classifies the free-form
commands of the `ceph_command` and `microceph_command` tools, where the level
has to be worked out from the arguments.

Ceph publishes a required permission per command, but it is no guide to what
a command does: `balancer execute`, `alerts send` and `orch device replace` are
all marked read-only there, and `auth print-key` is too. So read-only commands
are listed here by hand (starting from Ceph's own list, minus everything that
acts or reveals a secret), and anything not listed needs admin mode.

To tell `health` from `health mute`, a command line is matched against the
longest command the live cluster knows (`ceph -h`). That list leaves out
deprecated aliases and some commands that only the mgr registers; those are
simply not recognised and so are refused in read-only mode. Checked against
the full command table of Ceph 20.2: no unlisted command extends a read-only
one. For a hard guarantee, give the server a cephx identity with read-only
caps (--ceph-name); this module is the convenience layer on top.

Options are the other half. The ceph CLI reads an argument list in three
layers: its own options (argparse, which also takes abbreviations and attached
values: `--out-f=x`, `-ox`), then the client library, which takes any Ceph
configuration option in any spelling (`--log-file x`, `--log_file=x`), and
only what is left goes to the cluster as the command. The first two layers
write local files and choose the identity and the cluster, whatever the
command is. So options are not filtered by a list of bad names, which those
layers would outnumber, but admitted by a list of good ones: an option must be
written `--name` or `--name=value`, and `name` must be an argument that the
command itself declares in the cluster's help, and must not be one that the
first two layers would take for themselves.
"""

from __future__ import annotations

import fnmatch
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from .ceph import CephClient, CephError
from .config import Settings

Level = Literal["read", "write", "destructive"]


class Denied(Exception):
    """The server's mode does not permit the request. Shown to the model."""


# --------------------------------------------------------------------- ceph

# Read-only mon and mgr commands, by literal command prefix (glob patterns).
READ_ONLY = frozenset(
    """
status | health | df | fsid | features | versions | version | report | node ls
quorum_status | mon_status | time-sync-status | log last | insights
progress | progress json | healthcheck history ls | prometheus file_sd_config
ops | sessions | dump_ops_in_flight | dump_historic_ops
dump_historic_ops_by_duration | dump_historic_slow_ops | connection scores dump
auth ls | auth list | auth get | auth export | auth get-key | auth print-key
auth print_key | config-key get | config-key dump | orch certmgr key get
rgw realm tokens | cephadm get-ssh-config | dashboard sso show saml2
balancer dump | balancer eval | balancer eval-verbose | balancer ls
balancer pool ls | balancer show | balancer status | balancer status detail
cephadm config-check ls | cephadm config-check status | cephadm get-extra-ceph-conf
cephadm get-pub-key | cephadm get-user | cephadm systemd-unit ls
config dump | config get | config help | config log | config ls | config show
config show-with-defaults | config generate-minimal-conf
config-key exists | config-key list | config-key ls
crash info | crash json_report | crash ls | crash ls-new | crash stat
dashboard get-* | dashboard ac-role-show | dashboard ac-user-show | dashboard sso status
dashboard iscsi-gateway-list | dashboard nvmeof-gateway-list | dashboard motd get
device info | device ls | device ls-by-daemon | device ls-by-host | device ls-lights
device get-health-metrics | device predict-life-expectancy
device query-daemon-health-metrics
fs compat show | fs dump | fs feature ls | fs get | fs ls | fs lsflags | fs perf stats
fs status | fs clone status | fs snap-schedule list | fs snap-schedule status
fs snapshot mirror daemon status | fs snapshot mirror dirmap | fs snapshot mirror ls
fs snapshot mirror peer_list | fs snapshot mirror show distribution
fs subvolume authorized_list | fs subvolume charmap get | fs subvolume earmark get
fs subvolume exist | fs subvolume getpath | fs subvolume info | fs subvolume ls
fs subvolume metadata get | fs subvolume metadata ls | fs subvolume snapshot getpath
fs subvolume snapshot info | fs subvolume snapshot ls
fs subvolume snapshot metadata get | fs subvolume snapshot metadata ls
fs subvolumegroup charmap get | fs subvolumegroup exist | fs subvolumegroup getpath
fs subvolumegroup info | fs subvolumegroup ls | fs subvolumegroup snapshot ls
fs volume info | fs volume ls
k8sevents ceph | k8sevents ls | k8sevents status
mds compat show | mds count-metadata | mds last-seen | mds metadata | mds ok-to-stop
mds stat | mds versions
mgr count-metadata | mgr dump | mgr metadata | mgr module ls | mgr services | mgr stat
mgr versions
mon count-metadata | mon dump | mon feature ls | mon metadata | mon ok-to-add-offline
mon ok-to-rm | mon ok-to-stop | mon stat | mon versions
nfs cluster config get | nfs cluster info | nfs cluster ls | nfs export get
nfs export info | nfs export ls | nvme-gw show
orch certmgr cert check | orch certmgr cert get | orch certmgr cert ls
orch certmgr entity ls | orch certmgr key ls | orch client-keyring ls | orch device ls
orch hardware status | orch host ls | orch host ok-to-stop | orch ls | orch ps
orch osd rm status | orch status | orch tuned-profile ls | orch upgrade ls
orch upgrade status
osd blacklist ls | osd blocklist ls | osd blocked-by | osd count-metadata
osd crush class ls | osd crush class ls-osd | osd crush dump
osd crush get-device-class | osd crush get-tunable | osd crush ls | osd crush rule dump
osd crush rule list | osd crush rule ls | osd crush rule ls-by-class
osd crush show-tunables | osd crush tree | osd crush weight-set dump
osd crush weight-set ls | osd df | osd dump | osd erasure-code-profile get
osd erasure-code-profile ls | osd find | osd get-require-min-compat-client
osd getmaxosd | osd info | osd last-stat-seq | osd ls | osd ls-tree | osd lspools
osd map | osd metadata | osd numa-status | osd ok-to-stop | osd perf
osd perf counters get | osd pool application get | osd pool autoscale-status
osd pool availability-status | osd pool get | osd pool get noautoscale
osd pool get-quota | osd pool ls | osd pool stats | osd pool stretch show
osd safe-to-destroy | osd stat | osd status | osd test-reweight-by-pg
osd test-reweight-by-utilization | osd tree | osd tree-from | osd utilization
osd versions
pg debug | pg dump | pg dump_json | pg dump_pools_json | pg dump_stuck | pg ls
pg ls-by-osd | pg ls-by-pool | pg ls-by-primary | pg map | pg stat
rbd mirror snapshot schedule list | rbd mirror snapshot schedule status
rbd perf image counters | rbd perf image stats | rbd task list
rbd trash purge schedule list | rbd trash purge schedule status
service dump | service status | smb cluster ls | smb share ls | smb show
telemetry channel ls | telemetry collection ls | telemetry diff | telemetry preview
telemetry preview-all | telemetry preview-device | telemetry show | telemetry show-all
telemetry show-device | telemetry status
""".replace("\n", "|").split("|")
)
READ_ONLY_PATTERNS = tuple(sorted(p.strip() for p in READ_ONLY if p.strip()))

# Commands that print a secret as their whole purpose; they need --allow-secrets
# whatever the mode. Other secrets (keys in `auth get`, passwords in JSON) are
# masked in the output instead.
SECRET_PATTERNS = (
    "auth get-key",
    "auth print-key",
    "auth print_key",
    "auth get-or-create*",
    "config-key get",
    "config-key dump",
    "orch certmgr key get",
    "orch *get-credentials",
    "rgw realm tokens",
    "fs authorize",
    "dashboard sso show*",
    "cephadm get-ssh-config",
)

# Commands that can lose data or redundancy, or are hard to undo.
DESTRUCTIVE_PATTERNS = (
    "osd pool delete",
    "osd pool rm",
    "osd pool rmsnap",
    "osd purge*",
    "osd destroy*",
    "osd rm",
    "osd lost",
    "osd force-create-pg",
    "osd crush rm",
    "osd crush remove",
    "osd crush rule rm",
    "osd crush weight-set rm*",
    "osd erasure-code-profile rm",
    "osd tier remove*",
    "osd setcrushmap",
    "osd rm-pg-upmap*",
    "pg force_create_pg",
    "fs rm",
    "fs reset",
    "fs volume rm",
    "fs subvolume rm",
    "fs subvolume snapshot rm",
    "fs subvolumegroup rm",
    "fs subvolumegroup snapshot rm",
    "fs rm_data_pool",
    "mds rm",
    "mds rmfailed",
    "mon rm",
    "mon remove",
    "auth rm",
    "auth del",
    "auth import",
    "config-key rm",
    "config-key del",
    "config reset",
    "config assimilate-conf",
    "orch rm",
    "orch daemon rm",
    "orch osd rm",
    "orch host rm",
    "orch host drain",
    "orch device zap",
    "orch device replace",
    "orch upgrade start",
    "orch apply*",
    "nfs cluster rm",
    "nfs export rm",
    "smb cluster rm",
    "smb share rm",
    "rbd task add remove",
    "rbd task add trash remove",
    "rbd trash purge*",
    "nvme-gw delete",
    "mgr self-test*",
)

# Daemon commands (`ceph tell <daemon> ...`) that only read state.
READ_ONLY_TELL = (
    "version",
    "git_version",
    "help",
    "status",
    "mon_status",
    "quorum_status",
    "perf dump",
    "perf schema",
    "perf histogram dump",
    "perf histogram schema",
    "counter dump",
    "counter schema",
    "config show",
    "config diff",
    "config get",
    "config help",
    "ops",
    "dump_ops_in_flight",
    "dump_blocked_ops",
    "dump_historic_ops",
    "dump_historic_ops_by_duration",
    "dump_historic_slow_ops",
    "dump_mempools",
    "dump_osd_network",
    "dump_pgstate_history",
    "dump_pg_recovery_stats",
    "dump_recovery_reservations",
    "dump_scrub_reservations",
    "dump_scrubs",
    "dump_watchers",
    "dump_blocklist",
    "dump_op_pq_state",
    "dump_reservations",
    "dump_objectstore_kv_stats",
    "list_devices",
    "list_unfound",
    "objecter_requests",
    "sessions",
    "session ls",
    "client ls",
    "cache status",
    "heap stats",
    "bluefs stats",
    "bluestore bluefs device info",
    "bluestore allocator score block",
    "bluestore allocator fragmentation block",
    "get_mapped_pools",
    "smart",
    "damage ls",
    "dirfrag ls",
    "get subtrees",
    "dump loads",
    "dump snaps",
    "openfiles ls",
    "scrub status",
    "query",
)
DESTRUCTIVE_TELL = ("mark_unfound_lost", "cache drop", "flush_journal", "scrub abort")

# The ceph CLI's own options (ceph.in, Ceph 20.2 and main), dashes as
# underscores. An option name that begins any of them is refused: argparse
# takes unambiguous abbreviations, and these redirect the client to another
# cluster or identity, read and write local files, or never return.
CEPH_CLI_OPTIONS = (
    "completion",
    "help",
    "conf",
    "in_file",
    "out_file",
    "setuser",
    "setgroup",
    "id",
    "user",
    "name",
    "cluster",
    "admin_daemon",
    "status",
    "watch",
    "watch_debug",
    "watch_info",
    "watch_sec",
    "watch_warn",
    "watch_error",
    "watch_channel",
    "version",
    "verbose",
    "concise",
    "daemon_output_file",
    "format",
    "connect_timeout",
    "block",
    "period",
)
# Names that the client library takes as its own configuration before the
# command sees them. Any Ceph configuration option works there; these are the
# ones with an effect on the server host, plus the two that commands of Ceph
# 20.2 also use as argument names (host, key), which can therefore only be
# given positionally.
CEPH_CLIENT_OPTIONS = frozenset(
    [
        "host",
        "key",
        "keyfile",
        "keyring",
        "mon_host",
        "monmap",
        "bind",
        "fsid",
        "admin_socket",
        "log_file",
        "log_to_file",
        "log_to_syslog",
        "log_to_graylog",
        "log_graylog_host",
        "pid_file",
        "run_dir",
        "crash_dir",
        "plugin_dir",
        "erasure_code_dir",
        "heartbeat_file",
        "show_conf",
        "show_config",
        "show_config_value",
        "no_mon_config",
        "mon_config",
        "foreground",
        "client_mountpoint",
    ]
)
OPTION = re.compile(r"^--([A-Za-z][A-Za-z0-9_-]*)(?:=(.*))?$", re.DOTALL)
CONFIRM = "yes_i_really"
"""Confirmation flags (--yes-i-really-mean-it and its kin) make a command destructive."""
NEGATIVE_NUMBER = re.compile(r"^-\d+(\.\d+)?$")
SECRET_WORD = re.compile(r"(?i)(passw|secret|token|credential|private|[-_]key$)")
PGID = re.compile(r"^\d+\.[0-9a-f]+$")
DAEMON = re.compile(r"^(osd|mon|mgr|mds|client|rgw)(\.[A-Za-z0-9_.*-]+)?$")
KNOWN_TTL = 3600.0


@dataclass(frozen=True)
class Verdict:
    level: Level
    command: str
    """What the arguments were recognised as, for messages."""
    secret: bool = False
    verified: bool = True
    """False if the cluster's command list was not available to check against."""


def _match(command: str, patterns: Sequence[str]) -> bool:
    return any(fnmatch.fnmatchcase(command, p) for p in patterns)


def is_read_only(command: str) -> bool:
    """Whether a command (its words up to the first argument, as in the audit
    log's "prefix") is one of those known only to read."""
    return _match(command, READ_ONLY_PATTERNS)


def _starts(words: Sequence[str], patterns: Sequence[str]) -> str | None:
    """The first pattern that the leading words match, comparing whole words."""
    for pattern in patterns:
        n = len(pattern.split())
        if len(words) >= n and fnmatch.fnmatchcase(" ".join(words[:n]), pattern):
            return pattern
    return None


HELP_SIGNATURE_WIDTH = 40


_ARGUMENT = re.compile(r"<([A-Za-z_][A-Za-z0-9_-]*)")
_FLAG = re.compile(r"--([A-Za-z][A-Za-z0-9_-]*)")


def parse_help(text: str) -> dict[str, frozenset[str]]:
    """Commands from `ceph -h` output: literal prefix -> declared argument names.

    Each command starts in column 0 of the monitor command section, with its
    signature in a 40-column field that wraps onto indented lines, sometimes
    in the middle of a word. The prefix is the words before the first
    <argument>, [optional] or --flag; the argument names are what stands in
    <name> or <name:type> and after --, with dashes as underscores.
    """
    commands: dict[str, set[str]] = {}
    signature = ""
    in_commands = False

    def finish() -> None:
        words = []
        for word in signature.split():
            if word[0] in "<[{-":
                break
            words.append(word)
        if words:
            names = _ARGUMENT.findall(signature) + _FLAG.findall(signature)
            commands.setdefault(" ".join(words), set()).update(
                name.replace("-", "_") for name in names
            )

    for line in text.splitlines():
        if not in_commands:
            in_commands = line.strip().lower().startswith("monitor commands")
            continue
        if not line.strip() or line.strip().startswith("="):
            continue
        part = line[:HELP_SIGNATURE_WIDTH].strip()
        if not line[0].isspace():
            finish()
            signature = part
        elif part:
            # A line that ends in '-' or '_' was cut inside a word.
            signature += part if signature.endswith(("-", "_")) else " " + part
    finish()
    return {prefix: frozenset(names) for prefix, names in commands.items()}


@dataclass(frozen=True)
class Option:
    name: str
    """As it is compared: without the dashes in front, inner dashes as underscores."""
    value: str | None
    """None for a bare --flag."""
    token: str


def split_args(args: Sequence[str], tool: str) -> tuple[list[str], list[Option]]:
    """Split a command line into positional words and options.

    One spelling of an option is accepted: `--name` or `--name=value`, after
    all positional words, each name once. Everything else that begins with a
    dash is refused (negative numbers are words). The CLIs accept more: short
    options with the value attached (`-o/tmp/x`), abbreviations, `--name
    value`, the same option twice with the last one winning. Each of those is
    a way for the CLI to see a different command than the one classified
    here, e.g. `auth --entity client.x rm`.
    """
    words: list[str] = []
    options: list[Option] = []
    seen: set[str] = set()
    for arg in args:
        if arg in ("-h", "--help"):
            option = Option("help", None, arg)
        elif arg.startswith("-") and arg != "-" and not NEGATIVE_NUMBER.match(arg):
            match = OPTION.match(arg)
            if arg == "--" or not match:
                raise Denied(
                    f"{arg!r} is not accepted: write options as --name or --name=value. "
                    f"Short options and other spellings can mean something else to the "
                    f"{tool} command line than they appear to."
                )
            option = Option(match.group(1).replace("-", "_"), match.group(2), arg)
        elif options:
            raise Denied(
                f"write options after all positional arguments and as one token, "
                f"e.g. --name=value (got {arg!r} after {options[-1].token!r}). "
                f"This keeps the server's reading of a {tool} command unambiguous."
            )
        else:
            words.append(arg)
            continue
        if option.name in seen:
            raise Denied(f"option --{option.name} is given twice; give it once")
        seen.add(option.name)
        options.append(option)
    return words, options


def check_ceph_options(
    options: Sequence[Option],
    command: str,
    declared: frozenset[str] | None,
    *,
    verbatim: bool = False,
) -> None:
    """Raise Denied for any option that the command does not declare, or that
    the ceph CLI or the client library would take for itself. `declared` is
    None when the command's arguments are not known (tell commands, commands
    missing from the cluster's help). `verbatim` is for `tell ... injectargs`,
    whose options are settings for the daemon: the client library leaves them
    alone, the CLI's own option parser does not."""
    for option in options:
        name = option.name
        if name == "help" or name.startswith(CONFIRM):
            continue
        if "format".startswith(name):
            raise Denied(f"use the tool's format parameter instead of {option.token}")
        taken_by_cli = any(own.startswith(name) for own in CEPH_CLI_OPTIONS)
        if verbatim and not taken_by_cli:
            continue
        if taken_by_cli or name in CEPH_CLIENT_OPTIONS:
            raise Denied(
                f"option --{name} is not allowed: the ceph client takes it as a setting of "
                "its own (connection, identity, files), not as part of the command. If it "
                "is an argument of the command, give it positionally."
            )
        if declared is None:
            raise Denied(
                f"options cannot be checked for `ceph {command}`, whose arguments this "
                f"server does not know; give its arguments positionally (got {option.token})"
            )
        if name not in declared:
            takes = ", ".join(f"--{n}" for n in sorted(declared)) or "none"
            raise Denied(f"`ceph {command}` has no option --{name}; it takes: {takes}")


class Policy:
    def __init__(self, settings: Settings, ceph: CephClient):
        self._settings = settings
        self._ceph = ceph
        self._known: dict[str, frozenset[str]] = {}
        self._known_at = 0.0

    # ---------------------------------------------------------------- checks

    def require(self, level: Level, what: str) -> None:
        """Raise Denied unless the server mode permits an action of this level."""
        s = self._settings
        if level == "read":
            return
        if not s.admin:
            raise Denied(
                f"{what} is not a read-only operation, and this server runs in read-only "
                "mode. The operator can restart it with --mode admin."
            )
        if level == "destructive" and not s.allow_destructive:
            raise Denied(
                f"{what} is destructive or hard to undo, and such operations are "
                "disabled. The operator can restart the server with --allow-destructive."
            )

    def require_secrets(self, what: str) -> None:
        if not self._settings.allow_secrets:
            raise Denied(
                f"{what} reveals a secret, which this server does not hand out. "
                "The operator can restart it with --allow-secrets."
            )

    # ------------------------------------------------------------ ceph commands

    async def known_commands(self) -> dict[str, frozenset[str]]:
        """The live cluster's commands with their argument names, from `ceph -h`."""
        if self._known and time.monotonic() - self._known_at < KNOWN_TTL:
            return self._known
        try:
            result = await self._ceph.run(["-h"], check=False)
        except CephError:
            return self._known
        known = parse_help(result.stdout)
        if known:
            self._known, self._known_at = known, time.monotonic()
        return self._known

    async def classify_ceph(self, args: Sequence[str]) -> Verdict:
        """Work out what a `ceph <args>` invocation does."""
        words, options = split_args(args, "ceph")
        dangerous_flag = any(o.name.startswith(CONFIRM) for o in options)
        if not words:
            raise Denied("no command given")

        # The CLI cuts the argument list at this word, wherever it stands, and
        # hands the rest to the daemon unparsed. It has one legitimate place.
        injecting = words[:1] == ["tell"] and words[2:3] == ["injectargs"]
        if "injectargs" in words and not (injecting and words.count("injectargs") == 1):
            raise Denied(
                "'injectargs' is only accepted as `tell <daemon> injectargs ...`; "
                "to change a setting use config_set or `config set`"
            )
        if words[0] in ("tell", "daemon", "daemonperf"):
            if words[0] != "tell":
                raise Denied(
                    f"`{words[0]}` talks to a daemon's local socket; use `tell`, which "
                    "works from any host (daemon_perf for counters)"
                )
            if len(words) < 3 or (not DAEMON.match(words[1]) and not PGID.match(words[1])):
                raise Denied("usage: tell <daemon, e.g. osd.3 or mon.a> <command...>")
            check_ceph_options(options, f"tell {words[1]} {words[2]}", None, verbatim=injecting)
            return self._classify_tell(words[2:], f"tell {words[1]}", dangerous_flag)
        if words[0] == "pg" and len(words) >= 3 and PGID.match(words[1]):
            check_ceph_options(options, f"pg {words[1]} {words[2]}", None)
            return self._classify_tell(words[2:], f"pg {words[1]}", dangerous_flag)

        known = await self.known_commands()
        command = ""
        for n in range(min(len(words), 6), 0, -1):
            candidate = " ".join(words[:n])
            if candidate in known:
                command = candidate
                break
        check_ceph_options(options, command or " ".join(words[:3]), known.get(command))
        if _starts(words, DESTRUCTIVE_PATTERNS) or dangerous_flag:
            return Verdict("destructive", command or " ".join(words[:3]))
        secret = bool(_starts(words, SECRET_PATTERNS)) or any(
            SECRET_WORD.search(w) for w in (command or " ".join(words[:3])).split()[1:]
        )
        if command and _match(command, READ_ONLY_PATTERNS):
            # Reading an option or key that is named like a secret reveals it.
            secret = secret or any(SECRET_WORD.search(w) for w in words[1:])
            return Verdict("read", command, secret)
        # Unknown to the cluster or not on the read-only list: assume it writes.
        return Verdict("write", command or " ".join(words[:3]), secret, verified=bool(known))

    def _classify_tell(self, words: Sequence[str], target: str, dangerous: bool) -> Verdict:
        label = f"{target} {' '.join(words[:2])}"
        if dangerous or _starts(words, DESTRUCTIVE_TELL):
            return Verdict("destructive", label)
        if _starts(words, READ_ONLY_TELL):
            return Verdict("read", label)
        return Verdict("write", label)

    async def check_ceph(self, args: Sequence[str]) -> Verdict:
        """Classify a ceph command and raise Denied if the mode forbids it."""
        verdict = await self.classify_ceph(args)
        what = f"`ceph {verdict.command}`"
        if verdict.secret:
            self.require_secrets(what)
        if not verdict.verified and not self._settings.admin:
            # Without the cluster's command list nothing can be shown to be
            # read-only: `health mute` would look like `health`.
            raise Denied(
                "cannot verify the command against the cluster's command list "
                "(`ceph -h` gave none); the cluster may be unreachable"
            )
        self.require(verdict.level, what)
        return verdict


# ----------------------------------------------------------------- microceph

MICROCEPH_READ = (
    "status",
    "version",
    "cluster list",
    "cluster config list",
    "cluster config get",
    "disk list",
    "client config list",
    "client config get",
    "pool list",
    "log get-level",
    "remote list",
    "replication list*",
    "replication status*",
)
MICROCEPH_WRITE = (
    "disk add",
    "enable *",
    "disable *",
    "cluster config set",
    "cluster config reset",
    "client config set",
    "client config reset",
    "cluster maintenance enter",
    "cluster maintenance exit",
    "pool set-rf",
    "log set-level",
    "certificate *",
    "waitready",
)
# Everything else (disk remove, cluster remove/join/bootstrap/migrate/sql/export,
# replication changes, remote import/remove, cluster add, init) is destructive
# or hands out credentials.
MICROCEPH_DENIED = ("init", "cluster sql", "cluster add", "cluster export", "cluster join")
MICROCEPH_DENIED_OPTIONS = frozenset(["state_dir"])
# Go's strconv.ParseBool, which MicroCeph's flags are read with.
GO_TRUE = frozenset(["1", "t", "T", "TRUE", "true", "True"])
GO_FALSE = frozenset(["0", "f", "F", "FALSE", "false", "False"])


def go_bool(option: Option) -> bool:
    """The value of a boolean flag as MicroCeph will read it."""
    if option.value is None or option.value in GO_TRUE:
        return True
    if option.value in GO_FALSE:
        return False
    raise Denied(f"{option.token}: --{option.name} takes true or false")


def classify_microceph(args: Sequence[str]) -> Verdict:
    words, options = split_args(args, "microceph")
    given = {o.name: o for o in options}
    for name in MICROCEPH_DENIED_OPTIONS & given.keys():
        raise Denied(f"option --{name.replace('_', '-')} is not allowed")
    if not words:
        raise Denied("no command given")

    def on(name: str) -> bool:
        return name in given and go_bool(given[name])

    label = " ".join(words[:3])
    if _starts(words, MICROCEPH_DENIED):
        raise Denied(
            f"`microceph {label}` is interactive, hands out credentials or bypasses "
            "MicroCeph's own safeguards; run it by hand on the node"
        )
    if _starts(words, MICROCEPH_READ):
        return Verdict("read", label)
    if _starts(words, MICROCEPH_WRITE):
        # Preflight checks and dry runs change nothing: but only when the
        # flag is really on. `--dry-run=0` is a real run.
        if words[:2] == ["cluster", "maintenance"] and (on("dry_run") or on("check_only")):
            return Verdict("read", label)
        if words[:2] == ["disk", "add"] and on("dry_run"):
            return Verdict("read", label)
        return Verdict("write", label)
    return Verdict("destructive", label)
