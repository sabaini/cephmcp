"""What the server lets an agent run in each mode."""

from __future__ import annotations

import argparse
import ast
import os
import shutil
from pathlib import Path
from typing import ClassVar

import pytest
from conftest import FakeCli, fixture

from cephmcp.ceph import CephClient
from cephmcp.config import Settings
from cephmcp.policy import Denied, Policy, classify_microceph, parse_help


def policy(cli: FakeCli, **settings: object) -> Policy:
    s = Settings(ceph_cmd=("ceph",), **settings)  # type: ignore[arg-type]
    return Policy(s, CephClient(s, cli))


def test_parse_help_finds_command_prefixes() -> None:
    known = parse_help(fixture("ceph_help.txt")).keys()
    assert {"health", "health mute", "health unmute", "status", "df"} <= known
    assert {"osd pool create", "osd pool rm", "osd pool ls", "osd pool get"} <= known
    assert {"auth get", "auth get-key", "config get", "config set", "pg ls"} <= known
    # a signature wrapped in the middle of a word, and one wrapped between words
    assert "dashboard get-grafana-api-ssl-verify" in known
    assert "balancer status detail" in known
    # arguments, options, help text and the heading's underline are not prefixes
    assert not any(set("<[=") & set(k) or "--" in k for k in known)
    assert "show cluster health" not in " ".join(known)
    # deprecated aliases are hidden from the listing
    assert "osd pool delete" not in known


def test_parse_help_finds_the_arguments_of_each_command() -> None:
    known = parse_help(fixture("ceph_help.txt"))
    assert known["status"] == frozenset()
    assert known["config set"] == {"who", "name", "value", "force"}
    # wrapped over six lines, with names cut at an underscore and flags among them
    assert known["osd pool create"] >= {
        "pool", "pg_num", "pool_type", "erasure_code_profile", "expected_num_objects",
        "size", "bulk", "yes_i_really_mean_it",
    }  # fmt: skip
    assert {"daemon_type", "service_name", "hostname"} <= known["orch ps"]
    assert all(name == name.replace("-", "_") for names in known.values() for name in names)


READ = [
    ["status"],
    ["health"],
    ["health", "detail"],
    ["osd", "tree"],
    ["osd", "pool", "ls", "detail"],
    ["osd", "pool", "get", "rbd", "size"],
    ["osd", "df", "tree", "--filter_by=class"],
    ["pg", "ls", "2", "degraded"],
    ["config", "get", "osd.1", "osd_max_backfills"],
    ["auth", "ls"],
    ["orch", "ps", "--daemon_type=osd"],
    ["tell", "osd.3", "perf", "dump"],
    ["tell", "mon.a", "mon_status"],
    ["tell", "osd.*", "version"],
    ["pg", "2.1f", "query"],
]
WRITE = [
    ["osd", "set", "noout"],
    ["osd", "out", "3"],
    ["osd", "pool", "create", "x"],
    ["osd", "pool", "set", "x", "size", "3"],
    ["config", "set", "osd", "osd_max_backfills", "2"],
    ["health", "mute", "OSD_DOWN"],
    ["mgr", "module", "enable", "prometheus"],
    ["balancer", "execute", "plan"],  # Ceph itself marks this one read-only
    ["alerts", "send"],  # and this
    ["tell", "osd.3", "injectargs", "--debug_ms=1"],
    ["tell", "osd.3", "bench"],
    ["no", "such", "command"],
]
DESTRUCTIVE = [
    ["osd", "pool", "delete", "x", "x", "--yes-i-really-really-mean-it"],
    ["osd", "pool", "rm", "x", "x"],
    ["osd", "purge", "3", "--force"],
    ["osd", "destroy", "3"],
    ["osd", "rm", "3"],
    ["auth", "rm", "client.x"],
    ["fs", "rm", "cephfs", "--yes-i-really-mean-it"],
    ["orch", "rm", "rgw.default"],
    ["orch", "apply", "osd", "--all-available-devices"],
    ["osd", "pool", "set", "x", "size", "1", "--yes-i-really-mean-it"],  # any such flag
    ["pg", "2.1f", "mark_unfound_lost", "delete"],
    ["tell", "mds.a", "cache", "drop"],
]


@pytest.mark.anyio
@pytest.mark.parametrize("args", READ)
async def test_read_commands(cli: FakeCli, args: list[str]) -> None:
    assert (await policy(cli).check_ceph(args)).level == "read"


@pytest.mark.anyio
@pytest.mark.parametrize("args", WRITE)
async def test_write_commands_need_admin(cli: FakeCli, args: list[str]) -> None:
    with pytest.raises(Denied, match="read-only"):
        await policy(cli).check_ceph(args)
    assert (await policy(cli, mode="admin").check_ceph(args)).level == "write"


@pytest.mark.anyio
@pytest.mark.parametrize("args", DESTRUCTIVE)
async def test_destructive_commands_need_the_extra_switch(cli: FakeCli, args: list[str]) -> None:
    with pytest.raises(Denied, match="read-only"):
        await policy(cli).check_ceph(args)
    with pytest.raises(Denied, match="--allow-destructive"):
        await policy(cli, mode="admin").check_ceph(args)
    verdict = await policy(cli, mode="admin", allow_destructive=True).check_ceph(args)
    assert verdict.level == "destructive"


@pytest.mark.anyio
@pytest.mark.parametrize(
    "args",
    [
        ["auth", "get-key", "client.admin"],
        ["auth", "print-key", "client.admin"],
        ["config-key", "get", "mgr/dashboard/key"],
        ["config-key", "dump"],
        ["dashboard", "get-grafana-api-password"],
        # reading an option that is named like a secret reveals it
        ["config", "get", "client.rgw", "rgw_keystone_admin_password"],
    ],
)
async def test_secret_revealing_commands(cli: FakeCli, args: list[str]) -> None:
    for mode in ("read-only", "admin"):
        with pytest.raises(Denied, match="--allow-secrets"):
            await policy(cli, mode=mode).check_ceph(args)
    assert (await policy(cli, allow_secrets=True).check_ceph(args)).secret


@pytest.mark.anyio
@pytest.mark.parametrize(
    "args",
    [
        # `--opt value` could make the CLI see a different command than we do:
        # here `auth rm client.x` and `osd pool delete`.
        ["auth", "--entity", "client.x", "rm"],
        ["osd", "pool", "--pool", "ls", "delete", "x"],
        ["config", "--who", "get", "set", "debug_ms", "20"],
        ["osd", "tree", "--", "osd", "out", "1"],
    ],
)
async def test_option_smuggling_is_refused_in_every_mode(cli: FakeCli, args: list[str]) -> None:
    for settings in ({}, {"mode": "admin"}, {"mode": "admin", "allow_destructive": True}):
        with pytest.raises(Denied):
            await policy(cli, **settings).check_ceph(args)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "option",
    ["-i", "--in-file=/etc/shadow", "-o", "--conf=/tmp/x", "--keyring=/x", "--name=client.x",
     "-m", "--cluster=other", "--admin-daemon=/run/x.asok", "-w", "--watch", "--format=json", "-f"],
)  # fmt: skip
async def test_options_that_redirect_the_client_are_refused(cli: FakeCli, option: str) -> None:
    with pytest.raises(Denied):
        await policy(cli, mode="admin", allow_destructive=True).check_ceph(["status", option])


@pytest.mark.anyio
async def test_setting_a_secret_is_not_revealing_one(cli: FakeCli) -> None:
    verdict = await policy(cli, mode="admin").check_ceph(
        ["config", "set", "client.rgw", "rgw_keystone_admin_password", "new"]
    )
    assert verdict.level == "write" and not verdict.secret


@pytest.mark.anyio
async def test_negative_numbers_are_arguments_not_options(cli: FakeCli) -> None:
    verdict = await policy(cli, mode="admin").check_ceph(
        ["osd", "pool", "set", "x", "recovery_priority", "-1"]
    )
    assert verdict.level == "write"


@pytest.mark.anyio
async def test_read_only_needs_the_live_command_list(cli: FakeCli) -> None:
    """Without it, `health mute` could not be told from `health`."""
    cli.responses["-h"] = "usage: ceph ...\n"
    with pytest.raises(Denied, match="cannot verify"):
        await policy(cli).check_ceph(["health"])
    # in admin mode nothing depends on read-only verification
    assert (await policy(cli, mode="admin").check_ceph(["health"])).level == "write"


@pytest.mark.anyio
async def test_daemon_socket_and_malformed_tell(cli: FakeCli) -> None:
    for args in (["daemon", "osd.0", "perf", "dump"], ["tell", "osd.0"], ["tell", "/x", "version"]):
        with pytest.raises(Denied):
            await policy(cli, mode="admin").check_ceph(args)


class TestMicroceph:
    @pytest.mark.parametrize(
        "args",
        [
            ["status"],
            ["disk", "list", "--json"],
            ["cluster", "list"],
            ["cluster", "config", "get", "cluster_network"],
            ["cluster", "maintenance", "enter", "node2", "--dry-run"],
            ["cluster", "maintenance", "exit", "node2", "--check-only"],
            ["disk", "add", "--osd-match=eq(@type,'nvme')", "--dry-run"],
        ],
    )
    def test_read(self, args: list[str]) -> None:
        assert classify_microceph(args).level == "read"

    @pytest.mark.parametrize(
        "args",
        [
            ["disk", "add", "loop,4G,1"],
            ["disk", "add", "/dev/sdb", "--wipe"],
            ["enable", "rgw", "--target=node2"],
            ["disable", "rgw"],
            ["cluster", "config", "set", "cluster_network", "10.0.0.0/24"],
            ["cluster", "maintenance", "enter", "node2"],
            ["cluster", "maintenance", "enter", "node2", "--dry-run=false"],
            ["pool", "set-rf", "rbd", "--size=3"],
        ],
    )
    def test_write(self, args: list[str]) -> None:
        assert classify_microceph(args).level == "write"

    @pytest.mark.parametrize(
        "args",
        [["disk", "remove", "3"], ["cluster", "remove", "node2"], ["cluster", "bootstrap"],
         ["cluster", "migrate", "a", "b"], ["replication", "promote"], ["unheard", "of"]],
    )  # fmt: skip
    def test_destructive(self, args: list[str]) -> None:
        assert classify_microceph(args).level == "destructive"

    @pytest.mark.parametrize(
        "args",
        [
            ["init"],
            ["cluster", "sql", "select 1"],
            ["cluster", "add", "node4"],
            ["status", "--state-dir=/tmp/x"],
            # cobra would read "list" as the value of --timeout and run `disk remove`
            ["disk", "--timeout", "list", "remove", "3"],
        ],
    )
    def test_refused_outright(self, args: list[str]) -> None:
        with pytest.raises(Denied):
            classify_microceph(args)


# What the ceph CLI's own argument parser accepts (ceph.in, Ceph 20.2): option
# strings, and whether the option takes a value.
CEPH_CLI_PARSER = [
    (["--completion"], False),
    (["-h", "--help"], False),
    (["-c", "--conf"], True),
    (["-i", "--in-file"], True),
    (["-o", "--out-file"], True),
    (["--setuser"], True),
    (["--setgroup"], True),
    (["--id", "--user"], True),
    (["--name", "-n"], True),
    (["--cluster"], True),
    (["--admin-daemon"], True),
    (["-s", "--status"], False),
    (["-w", "--watch"], False),
    (["--watch-debug"], False),
    (["--watch-info"], False),
    (["--watch-sec"], False),
    (["--watch-warn"], False),
    (["--watch-error"], False),
    (["-W", "--watch-channel"], True),
    (["--version", "-v"], False),
    (["--verbose"], False),
    (["--concise"], False),
    (["--daemon-output-file"], True),
    (["-f", "--format"], True),
    (["--connect-timeout"], True),
    (["--block"], False),
    (["--period", "-p"], True),
]


def ceph_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    for strings, takes_value in CEPH_CLI_PARSER:
        if takes_value:
            parser.add_argument(*strings, dest=strings[-1])
        else:
            parser.add_argument(*strings, dest=strings[-1], action="store_true")
    return parser


def spellings() -> list[str]:
    """Every way to write each of the CLI's options as one token."""
    out: set[str] = set()
    for strings, takes_value in CEPH_CLI_PARSER:
        for string in strings:
            if string.startswith("--"):
                # argparse takes any unambiguous abbreviation of a long option
                forms = [string[:n] for n in range(3, len(string) + 1)]
            else:
                forms = [string]
            for form in forms:
                out.add(form)
                if takes_value:
                    out.add(form + "=/tmp/x")
                    if not form.startswith("--"):
                        out.add(form + "/tmp/x")  # -o/tmp/x
    return sorted(out - {"-h", "--help"})


@pytest.mark.anyio
@pytest.mark.parametrize("token", spellings())
async def test_whatever_the_cli_takes_for_itself_is_refused(cli: FakeCli, token: str) -> None:
    """The contract with the real parser: a token that argparse would consume
    (or choke on) instead of passing it to the cluster never gets through."""
    parser = ceph_cli_parser()
    try:
        _, extras = parser.parse_known_args(["osd", "tree", token])
        consumed = extras != ["osd", "tree", token]
    except (argparse.ArgumentError, SystemExit):
        consumed = True  # ambiguous or malformed: the CLI would stop
    refused = False
    try:
        await policy(cli, mode="admin", allow_destructive=True).check_ceph(["osd", "tree", token])
    except Denied:
        refused = True
    assert refused, f"{token} was let through"
    # Nothing is refused here for being harmless: each of these is the CLI's.
    assert consumed or token.replace("-", "_").lstrip("_") in ("format",)


def test_the_parser_table_matches_the_policy() -> None:
    from cephmcp.policy import CEPH_CLI_OPTIONS

    long_names = {
        string[2:].replace("-", "_")
        for strings, _ in CEPH_CLI_PARSER
        for string in strings
        if string.startswith("--")
    }
    assert long_names == set(CEPH_CLI_OPTIONS)


def test_the_parser_table_matches_the_installed_cli() -> None:
    """Against the real thing, where there is one: the `ceph` script on PATH,
    or the file CEPH_CLI_SOURCE names (ceph.in from a source tree)."""
    source = os.environ.get("CEPH_CLI_SOURCE") or shutil.which("ceph")
    if not source:
        pytest.skip("no ceph CLI here; set CEPH_CLI_SOURCE to check against one")
    tree = ast.parse(Path(source).read_text())
    function = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "parse_cmdargs"
    )
    real = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "add_argument":
            real.add(frozenset(a.value for a in node.args if isinstance(a, ast.Constant)))
    assert real == {frozenset(strings) for strings, _ in CEPH_CLI_PARSER}


@pytest.mark.anyio
@pytest.mark.parametrize(
    "option",
    [
        # Ceph configuration options: the client library takes any of them, in
        # either spelling, before the command sees them.
        "--log-file=/etc/cron.d/x",
        "--log_file=/etc/cron.d/x",
        "--log-to-file=true",
        "--debug_ms=1",
        "--admin_socket=/run/x.asok",
        "--keyring=/tmp/k",
        "--mon_host=10.0.0.9",
        "--rados_mon_op_timeout=1",
        # not an option of `osd tree` at all
        "--pool=rbd",
        # other spellings
        "-ox",
        "-",
        "--",
        "--=x",
        "--9lives",
        "---x",
        "-filter_by=class",
        # the same option twice: the CLI would take the last one
        "--filter_by=class --filter_by=name",
    ],
)
async def test_only_options_the_command_declares_get_through(cli: FakeCli, option: str) -> None:
    args = ["osd", "df", "tree", *option.split()] if option != "-" else ["osd", "df", "-o", "-"]
    for settings in ({}, {"mode": "admin", "allow_destructive": True}):
        with pytest.raises(Denied):
            await policy(cli, **settings).check_ceph(args)


@pytest.mark.anyio
async def test_declared_options_in_either_spelling(cli: FakeCli) -> None:
    admin = policy(cli, mode="admin")
    create = ["osd", "pool", "create", "x", "--pg_num=8", "--pool-type=replicated", "--bulk"]
    assert (await admin.check_ceph(create)).level == "write"
    assert (
        await policy(cli).check_ceph(["osd", "df", "tree", "--filter-by=class"])
    ).level == "read"
    assert (await policy(cli).check_ceph(["osd", "pool", "ls", "--help"])).level == "read"
    with pytest.raises(Denied, match=r"has no option --size; it takes: --filter, --filter_by"):
        await admin.check_ceph(["osd", "df", "tree", "--size=3"])


@pytest.mark.anyio
async def test_arguments_the_client_would_take_must_be_positional(cli: FakeCli) -> None:
    """`config set` declares <name>, `config-key set` <key>, `orch host add`
    <hostname>: as --name, --key, --host they never reach the command."""
    admin = policy(cli, mode="admin")
    for args in (
        ["config", "set", "osd", "--name=debug_ms", "--value=1"],
        ["osd", "crush", "rule", "create-replicated", "--name=r", "--root=default", "--type=host"],
        ["osd", "purge", "--id=3"],
        ["osd", "set", "--key=noout"],
    ):
        with pytest.raises(Denied, match="takes it as a setting of its own"):
            await admin.check_ceph(args)
    assert (
        await admin.check_ceph(["config", "set", "osd", "debug_ms", "--value=1"])
    ).level == "write"


@pytest.mark.anyio
async def test_options_of_commands_the_server_does_not_know(cli: FakeCli) -> None:
    admin = policy(cli, mode="admin", allow_destructive=True)
    for args in (["no", "such", "command", "--force"], ["tell", "osd.3", "bench", "--count=5"]):
        with pytest.raises(Denied, match="options cannot be checked"):
            await admin.check_ceph(args)
    # Confirmation flags are no setting of anything; they make it destructive.
    verdict = await admin.check_ceph(["no", "such", "command", "--yes-i-really-mean-it"])
    assert verdict.level == "destructive"


@pytest.mark.anyio
async def test_injectargs_has_one_place(cli: FakeCli) -> None:
    admin = policy(cli, mode="admin")
    # After `tell <daemon> injectargs` the options are settings for the daemon.
    inject = ["tell", "osd.3", "injectargs", "--debug_ms=1", "--osd_max_backfills=2"]
    assert (await admin.check_ceph(inject)).level == "write"
    with pytest.raises(Denied, match="read-only"):
        await policy(cli).check_ceph(inject)
    # The CLI's own parser still reads them first.
    for option in ("--out-file=/tmp/x", "--out-f=/tmp/x", "--conf=/tmp/c", "-o/tmp/x"):
        with pytest.raises(Denied):
            await admin.check_ceph(["tell", "osd.3", "injectargs", option])
    # Anywhere else the CLI would cut the command off at that word.
    for args in (
        ["status", "injectargs"],
        ["osd", "pool", "ls", "injectargs", "--log_file=/tmp/x"],
        ["tell", "osd.3", "injectargs", "injectargs"],
        ["injectargs", "--debug_ms=1"],
    ):
        with pytest.raises(Denied, match="only accepted as"):
            await admin.check_ceph(args)


@pytest.mark.anyio
async def test_a_value_that_begins_with_a_dash(cli: FakeCli) -> None:
    admin = policy(cli, mode="admin")
    with pytest.raises(Denied, match="write options as --name or --name=value"):
        await admin.check_ceph(["config", "set", "osd", "some_option", "-x"])
    assert (await admin.check_ceph(["config", "set", "osd", "some_option", "--value=-x"])).level


class TestMicrocephFlags:
    """MicroCeph reads its boolean flags with Go's strconv.ParseBool."""

    ENTER: ClassVar[list[str]] = ["cluster", "maintenance", "enter", "node2", "--stop-osds"]

    @pytest.mark.parametrize("value", ["", "=1", "=t", "=T", "=TRUE", "=true", "=True"])
    def test_a_dry_run_that_is_on_changes_nothing(self, value: str) -> None:
        assert classify_microceph([*self.ENTER, f"--dry-run{value}"]).level == "read"
        assert classify_microceph([*self.ENTER, f"--check-only{value}"]).level == "read"
        add = ["disk", "add", "--osd-match=eq(@type,'nvme')", f"--dry-run{value}"]
        assert classify_microceph(add).level == "read"

    @pytest.mark.parametrize("value", ["=0", "=f", "=F", "=FALSE", "=false", "=False"])
    def test_a_dry_run_that_is_off_is_the_real_thing(self, value: str) -> None:
        assert classify_microceph([*self.ENTER, f"--dry-run{value}"]).level == "write"
        assert classify_microceph([*self.ENTER, f"--check-only{value}"]).level == "write"
        assert classify_microceph(["disk", "add", "/dev/sdb", f"--dry-run{value}"]).level == "write"
        # one of the two on is enough, as for MicroCeph itself
        both = [*self.ENTER, f"--dry-run{value}", "--check-only"]
        assert classify_microceph(both).level == "read"

    @pytest.mark.parametrize(
        "flags",
        [
            ["--dry-run", "--dry-run=false"],  # the last one would win
            ["--dry-run=false", "--dry-run"],
            ["--check-only", "--check-only=0"],
            ["--dry-run=maybe"],
            ["--dry-run=yes"],
            ["--dry-run="],
            ["-d"],
        ],
    )
    def test_ambiguous_flags_are_refused(self, flags: list[str]) -> None:
        with pytest.raises(Denied):
            classify_microceph([*self.ENTER, *flags])

    def test_other_spellings_of_the_state_directory(self) -> None:
        for option in ("--state-dir=/tmp/x", "--state_dir=/tmp/x", "--state-dir"):
            with pytest.raises(Denied):
                classify_microceph(["status", option])
