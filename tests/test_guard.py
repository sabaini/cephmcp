"""The change journal, undo, approval, and guarded host maintenance."""

from __future__ import annotations

import json
from typing import Any

import mcp_types as types
import pytest
from conftest import FakeCli, rows

from cephmcp import guard
from cephmcp.cli import build_parser, settings_from

pytestmark = pytest.mark.anyio


class Operator:
    """Stands in for the person at the client: answers approval questions."""

    def __init__(self, answer: str = "accept", approve: bool = True):
        self.answer, self.approve = answer, approve
        self.asked: list[str] = []

    async def __call__(self, context: Any, params: Any) -> types.ElicitResult:
        self.asked.append(params.message)
        if self.answer != "accept":
            return types.ElicitResult(action=self.answer)
        return types.ElicitResult(action="accept", content={"approve": self.approve})

    @property
    def client(self) -> dict[str, Any]:
        return {"elicitation_callback": self}


def script(cli: FakeCli, *commands: str) -> None:
    for command in commands:
        cli.responses[command] = ""


def osd_dump(cli: FakeCli, **content: Any) -> None:
    """What `ceph osd dump` says about flags (the default fixtures have none)."""
    cli.set_json("osd dump", {"flags_set": ["sortbitwise"], "osds": [], **content})


class TestJournal:
    async def test_changes_are_recorded_with_their_undo(self, harness: Any, cli: FakeCli) -> None:
        script(cli, "osd set noout", "osd set-group noout node1", "osd pool set rbd size --val=2")
        cli.set_json("osd pool get rbd size", {"pool": "rbd", "size": 3})
        osd_dump(cli)
        async with harness(mode="admin") as h:
            first = await h.call("cluster_flags", action="set", flag="noout")
            assert first["change"] == 1 and first["undo"] == "change_undo change_id=1"
            await h.call("cluster_flags", action="set", flag="noout", targets=["node1"])
            sized = await h.call("pool_set", pool="rbd", option="size", value="2")
            assert sized["previous"] == 3
            log = rows(await h.call("change_log"))
            assert [(c["change"], c["tool"], c["summary"]) for c in log] == [
                (3, "pool_set", "pool rbd: size = 2 (was 3)"),
                (2, "cluster_flags", "set noout on node1"),
                (1, "cluster_flags", "set noout"),
            ]
            assert all(c["outcome"] == "done" and c["undo"] == "available" for c in log)
            assert log[0]["client"].startswith("mcp")
            detail = await h.call("change_log", change_id=3)
            assert detail["commands"] == ["ceph osd pool set rbd size --val=2"]
            assert detail["undo_commands"] == ["ceph osd pool set rbd size --val=3"]
            assert detail["arguments"] == {"pool": "rbd", "option": "size", "value": "2"}
            assert "there is no change 9" in await h.error("change_log", change_id=9)
            newest = await h.call("change_log", limit=2)
            assert [r["change"] for r in rows(newest)] == [3, 2] and newest["count"] == 2
            assert newest["truncated"].startswith("the newest 2 are shown")
            assert "truncated" not in await h.call("change_log", limit=3)

    async def test_no_undo_for_what_changed_nothing(self, harness: Any, cli: FakeCli) -> None:
        script(cli, "osd set noout")
        osd_dump(cli, flags_set=["sortbitwise", "noout"])
        async with harness(mode="admin") as h:
            await h.call("cluster_flags", action="set", flag="noout")
            entry = rows(await h.call("change_log"))[0]
        # Clearing it on undo would take away a flag somebody else had set.
        assert entry["summary"] == "set noout (it already was)" and entry["undo"] == "not automatic"

    async def test_failures_are_recorded_too(self, harness: Any, cli: FakeCli) -> None:
        cli.fail("osd pool set rbd size --val=99", "Error EINVAL: size must be between 1 and 10")
        cli.set_json("osd pool get rbd size", {"size": 3})
        async with harness(mode="admin") as h:
            await h.error("pool_set", pool="rbd", option="size", value="99")
            entry = rows(await h.call("change_log"))[0]
            assert entry["outcome"].startswith("failed: ") and "EINVAL" in entry["outcome"]
            assert entry["undo"] == "nothing to undo"
            assert "did not succeed" in await h.error("change_undo", change_id=1)

    async def test_free_form_commands_are_on_record(self, harness: Any, cli: FakeCli) -> None:
        script(cli, "osd pool rename rbd vms")
        async with harness(mode="admin") as h:
            await h.call("ceph_command", args=["health", "detail"])  # a read: not a change
            await h.call("ceph_command", args=["osd", "pool", "rename", "rbd", "vms"])
            log = rows(await h.call("change_log"))
            assert [(c["tool"], c["summary"], c["undo"]) for c in log] == [
                ("ceph_command", "ceph osd pool rename rbd vms", "not automatic")
            ]
            assert "no automatic undo" in await h.error("change_undo", change_id=1)

    async def test_secrets_in_the_journal_are_masked(self, harness: Any, cli: FakeCli) -> None:
        script(cli, "config set mgr mgr/dashboard/jwt_secret --value=hunter2")
        async with harness(mode="admin") as h:
            await h.call(
                "config_set", who="mgr", option="mgr/dashboard/jwt_secret", value="hunter2"
            )
            failed, text = await h.raw("change_log", change_id=1)
            assert not failed and "hunter2" not in text and "jwt_secret" in text
            failed, listing = await h.raw("change_log")
            assert "hunter2" not in listing

    async def test_secrets_stay_out_of_the_reply_to_the_change_itself(
        self, harness: Any, cli: FakeCli
    ) -> None:
        option = "mgr/dashboard/jwt_secret"
        cli.responses[f"config get mgr {option}"] = "old-secret-value\n"
        cli.set_json(
            "config dump", [{"section": "mgr", "name": option, "value": "old-secret-value"}]
        )
        script(cli, f"config set mgr {option} --value=new-secret-value")
        async with harness(mode="admin") as h:
            failed, text = await h.raw(
                "config_set", who="mgr", option=option, value="new-secret-value"
            )
            assert not failed
            assert "new-secret-value" not in text and "old-secret-value" not in text
            done = json.loads(text)
            assert done["previous"] == done["now"] == "<redacted>"
            assert done["ran"] == f"ceph config set mgr {option} <redacted>"
            assert done["undo"] == "change_undo change_id=1"  # the journal still knows how
            # A command that fails quotes itself in the error: masked as well.
            cli.fail(f"config set mgr {option} --value=another-secret", "Error EINVAL: no")
            message = await h.error("config_set", who="mgr", option=option, value="another-secret")
            assert "another-secret" not in message and "old-secret-value" not in message
        async with harness(mode="admin", allow_secrets=True) as h:
            done = await h.call("config_set", who="mgr", option=option, value="new-secret-value")
            assert done["previous"] == "old-secret-value" and "new-secret-value" in done["ran"]

    async def test_an_ordinary_option_is_shown_as_before(self, harness: Any, cli: FakeCli) -> None:
        script(cli, "config set osd osd_max_backfills --value=3")
        async with harness(mode="admin") as h:
            done = await h.call("config_set", who="osd", option="osd_max_backfills", value="3")
        assert done["ran"] == "ceph config set osd osd_max_backfills --value=3"
        assert done["previous"] == 1 and "note" not in done

    async def test_options_whose_value_is_a_word(self, harness: Any, cli: FakeCli) -> None:
        """`ceph config get` prints the bare value, which for a word is not JSON."""
        cli.responses["config get osd osd_op_queue"] = "mclock_scheduler\n"
        script(cli, "config set osd osd_op_queue --value=wpq")
        async with harness(mode="admin") as h:
            read = await h.call("config_get", who="osd", option="osd_op_queue")
            assert read["value"] == "mclock_scheduler"
            done = await h.call("config_set", who="osd", option="osd_op_queue", value="wpq")
            assert done["previous"] == "mclock_scheduler"
            assert (await h.call("config_get", who="osd", option="osd_max_backfills"))["value"] == 1

    async def test_read_only_server_shows_but_cannot_undo(self, harness: Any) -> None:
        async with harness() as h:
            assert "No changes on record" in (await h.call("change_log"))["note"]
            assert "change_undo" not in await h.tools()

    async def test_changes_show_in_the_timeline(
        self, harness: Any, cli: FakeCli, clock: Any
    ) -> None:
        script(cli, "osd set noout")
        osd_dump(cli)
        async with harness(mode="admin") as h:
            await h.call("cluster_flags", action="set", flag="noout")
            clock.now += 60
            events = rows((await h.call("what_changed", since="-1h"))["events"])
        assert events[-1]["source"] == "cephmcp"
        assert events[-1]["what"] == "cluster_flags: set noout [change 1, done]"


class TestUndo:
    async def test_flags(self, harness: Any, cli: FakeCli) -> None:
        script(cli, "osd set noout", "osd unset noout")
        osd_dump(cli)
        async with harness(mode="admin") as h:
            await h.call("cluster_flags", action="set", flag="noout")
            undone = await h.call("change_undo", change_id=1)
            assert undone == {"undone": 1, "ran": ["ceph osd unset noout"], "change": 2}
            log = rows(await h.call("change_log"))
            assert log[0]["summary"] == "undo of change 1: set noout"
            assert log[0]["undo"] == "is the undo of change 1"
            assert log[1]["undo"] == "undone by change 2"
            assert "already undone, by change 2" in await h.error("change_undo", change_id=1)
            assert "no automatic undo" in await h.error("change_undo", change_id=2)
            assert "there is no change 7" in await h.error("change_undo", change_id=7)
        assert cli.changes == ["osd set noout", "osd unset noout"]

    async def test_a_flag_that_was_already_there_is_not_ours_to_clear(
        self, harness: Any, cli: FakeCli
    ) -> None:
        cli.use("microceph")
        script(
            cli, "osd set-group noout cephmcp-2", "osd set-group noout cephmcp-1 cephmcp-2 osd.5 6"
        )
        dump = cli.load("osd dump")
        dump["crush_node_flags"] = {"-3": ["noout"]}  # host cephmcp-2, set by somebody else
        next(o for o in dump["osds"] if o["osd"] == 6)["state"] = ["exists", "up", "noout"]
        cli.set_json("osd dump", dump)
        async with harness(mode="admin") as h:
            same = await h.call("cluster_flags", action="set", flag="noout", targets=["cephmcp-2"])
            assert "undo" not in same
            entry = await h.call("change_log", change_id=1)
            assert entry["summary"] == "set noout on cephmcp-2 (cephmcp-2 already had it)"
            assert entry["undo"] == "not automatic" and "undo_commands" not in entry
            # Several targets, some of which had the flag: undo restores exactly that.
            mixed = await h.call(
                "cluster_flags",
                action="set",
                flag="noout",
                targets=["cephmcp-1", "cephmcp-2", "osd.5", "6"],
            )
            assert mixed["undo"] == "change_undo change_id=2"
            entry = await h.call("change_log", change_id=2)
            assert entry["undo_commands"] == ["ceph osd unset-group noout cephmcp-1 osd.5"]
            assert entry["summary"].endswith("(cephmcp-2, 6 already had it)")

    async def test_clearing_a_flag_that_was_not_set_has_nothing_to_undo(
        self, harness: Any, cli: FakeCli
    ) -> None:
        cli.use("microceph")
        script(cli, "osd unset-group noout cephmcp-1 cephmcp-2")
        dump = cli.load("osd dump")
        dump["crush_node_flags"] = {"-3": ["noout"]}
        cli.set_json("osd dump", dump)
        async with harness(mode="admin") as h:
            await h.call(
                "cluster_flags", action="unset", flag="noout", targets=["cephmcp-1", "cephmcp-2"]
            )
            entry = await h.call("change_log", change_id=1)
        assert entry["undo_commands"] == ["ceph osd set-group noout cephmcp-2"]

    @pytest.mark.parametrize("targets", [None, ["cephmcp-2"]])
    async def test_no_undo_when_the_state_before_is_unknown(
        self, harness: Any, cli: FakeCli, targets: list[str] | None
    ) -> None:
        cli.use("microceph")
        script(cli, "osd set noout", "osd set-group noout cephmcp-2")
        cli.fail("osd dump", "Error ETIMEDOUT: timed out", 110)
        async with harness(mode="admin") as h:
            done = await h.call("cluster_flags", action="set", flag="noout", targets=targets)
            assert "undo" not in done and "could not be read" in done["note"]
            assert "no automatic undo" in await h.error("change_undo", change_id=1)

    async def test_a_failed_lookup_is_not_an_empty_configuration(
        self, harness: Any, cli: FakeCli
    ) -> None:
        script(cli, "config set osd osd_max_backfills --value=3")
        cli.fail("config dump", "temporarily unavailable")
        async with harness(mode="admin") as h:
            done = await h.call("config_set", who="osd", option="osd_max_backfills", value="3")
            # An override may have existed: removing it on undo could bring back another value.
            assert "undo" not in done and "could not be read" in done["note"]
            assert (await h.call("change_log", change_id=1))["undo"] == "not automatic"
            assert "no automatic undo" in await h.error("change_undo", change_id=1)

    async def test_other_changes_without_a_known_before(self, harness: Any, cli: FakeCli) -> None:
        script(
            cli, "osd out 1", "osd pool set rbd size --val=2", "osd pool set-quota rbd max_bytes 5"
        )
        cli.fail("osd dump", "Error ETIMEDOUT: timed out", 110)
        async with harness(mode="admin") as h:
            out = await h.call("osd_set", osd=1, action="out")
            sized = await h.call("pool_set", pool="rbd", option="size", value="2")
            quota = await h.call("pool_set", pool="rbd", option="quota_max_bytes", value="5")
            for done in (out, sized, quota):
                assert "undo" not in done and "could not be read" in done["note"]

    async def test_osd_state_and_weights(self, harness: Any, cli: FakeCli) -> None:
        script(
            cli,
            "osd out 1",
            "osd in 1",
            "osd reweight 2 0.8",
            "osd reweight 2 1",
            "osd crush reweight osd.0 2",
            "osd primary-affinity 0 0.5",
        )
        dump = {
            "osds": [
                {"osd": 0, "in": 1, "up": 1, "weight": 1, "primary_affinity": 1},
                {"osd": 1, "in": 1, "up": 1, "weight": 1, "primary_affinity": 1},
                {"osd": 2, "in": 1, "up": 1, "weight": 1, "primary_affinity": 1},
            ]
        }
        cli.set_json("osd dump", dump)
        async with harness(mode="admin") as h:
            await h.call("osd_set", osd=1, action="out")
            await h.call("osd_set", osd=2, action="reweight", value=0.8)
            await h.call("osd_set", osd=0, action="crush-reweight", value=2)
            await h.call("osd_set", osd=0, action="primary-affinity", value=0.5)
            undo = [
                (await h.call("change_log", change_id=i))["undo_commands"] for i in (1, 2, 3, 4)
            ]
            assert undo == [
                ["ceph osd in 1"],
                ["ceph osd reweight 2 1"],
                ["ceph osd crush reweight osd.0 0.00979614"],
                ["ceph osd primary-affinity 0 1"],
            ]
            assert rows(await h.call("change_log"))[2]["summary"] == "osd.2 reweight 0.8 (was 1)"
            await h.call("change_undo", change_id=2)
            await h.call("change_undo", change_id=1)
        assert cli.changes[-2:] == ["osd reweight 2 1", "osd in 1"]

    async def test_configuration(self, harness: Any, cli: FakeCli) -> None:
        script(
            cli,
            "config set osd osd_max_backfills --value=3",
            "config rm osd osd_max_backfills",
            "config set osd/class:hdd osd_scrub_sleep --value=0.5",
            "config set osd/class:hdd osd_scrub_sleep --value=0.2",
            "config rm global osd_pool_default_size",
            "config set global osd_pool_default_size --value=2",
        )
        stored = cli.load("config dump")
        stored.append(
            {"section": "osd", "mask": "class:hdd", "name": "osd_scrub_sleep", "value": "0.2"}
        )
        cli.set_json("config dump", stored)
        async with harness(mode="admin") as h:
            # Not set before: undo removes it again.
            await h.call("config_set", who="osd", option="osd_max_backfills", value="3")
            # Set before, for exactly this scope and mask: undo restores that value.
            await h.call("config_set", who="osd/class:hdd", option="osd_scrub_sleep", value="0.5")
            # Removed: undo puts it back.
            await h.call("config_set", who="global", option="osd_pool_default_size", remove=True)
            undo = [(await h.call("change_log", change_id=i))["undo_commands"] for i in (1, 2, 3)]
            assert undo == [
                ["ceph config rm osd osd_max_backfills"],
                ["ceph config set osd/class:hdd osd_scrub_sleep --value=0.2"],
                ["ceph config set global osd_pool_default_size --value=2"],
            ]
            await h.call("change_undo", change_id=3)
            await h.call("change_undo", change_id=1)
        assert cli.changes[-2:] == [
            "config set global osd_pool_default_size --value=2",
            "config rm osd osd_max_backfills",
        ]

    async def test_pool_settings_and_quota(self, harness: Any, cli: FakeCli) -> None:
        script(
            cli,
            "osd pool set rbd nodelete --val=true",
            "osd pool set rbd nodelete --val=false",
            "osd pool set-quota rbd max_bytes 1073741824",
            "osd pool set-quota rbd max_bytes 0",
            "osd pool application enable rbd rbd",
        )
        cli.set_json("osd pool get rbd nodelete", {"pool": "rbd", "nodelete": False})
        cli.set_json("osd pool get-quota rbd", {"quota_max_bytes": 0, "quota_max_objects": 0})
        async with harness(mode="admin") as h:
            await h.call("pool_set", pool="rbd", option="nodelete", value="true")
            await h.call("pool_set", pool="rbd", option="quota_max_bytes", value="1073741824")
            await h.call("pool_set", pool="rbd", option="application", value="rbd")
            assert "no automatic undo" in await h.error("change_undo", change_id=3)
            await h.call("change_undo", change_id=2)
            await h.call("change_undo", change_id=1)
        assert cli.changes[-2:] == [
            "osd pool set-quota rbd max_bytes 0",
            "osd pool set rbd nodelete --val=false",
        ]

    async def test_newest_first(self, harness: Any, cli: FakeCli) -> None:
        script(
            cli,
            "osd pool set rbd size --val=2",
            "osd pool set rbd size --val=4",
            "osd pool set rbd size --val=3",
            "osd pool set rbd min_size --val=1",
        )
        cli.set_json("osd pool get rbd size", {"size": 3})
        cli.set_json("osd pool get rbd min_size", {"min_size": 2})
        async with harness(mode="admin") as h:
            await h.call("pool_set", pool="rbd", option="size", value="2")
            cli.set_json("osd pool get rbd size", {"size": 2})
            await h.call("pool_set", pool="rbd", option="size", value="4")
            await h.call("pool_set", pool="rbd", option="min_size", value="1")
            message = await h.error("change_undo", change_id=1)
            assert "change 2 was made later to the same thing" in message
            # Another option of the same pool does not stand in the way.
            await h.call("change_undo", change_id=2)
            await h.call("change_undo", change_id=1)
        assert cli.changes[-2:] == [
            "osd pool set rbd size --val=2",
            "osd pool set rbd size --val=3",
        ]

    async def test_undo_obeys_the_mode(self, harness: Any, cli: FakeCli) -> None:
        async with harness(mode="admin") as h:
            store = await h.app.history.store()
            store.log_change(
                h.clock.now,
                "ceph_command",
                "something drastic",
                {},
                [],
                [["ceph", "osd", "pool", "delete", "rbd", "rbd", "--yes-i-really-really-mean-it"]],
                "x",
                "done",
            )
            assert "--allow-destructive" in await h.error("change_undo", change_id=1)
        assert cli.changes == []


class TestApproval:
    def settings(self, *argv: str, **environ: str) -> Any:
        return settings_from(build_parser().parse_args(argv), {"HOME": "/h", **environ})

    def test_setting(self) -> None:
        assert self.settings().approval == "auto"
        assert self.settings("--approval", "required").approval == "required"
        assert self.settings(CEPHMCP_APPROVAL="off").approval == "off"
        with pytest.raises(ValueError, match="approval must be"):
            self.settings(CEPHMCP_APPROVAL="sometimes")

    def test_codes_follow_the_plan(self) -> None:
        assert guard.code_for("a", "b") == guard.code_for("a", "b")
        assert guard.code_for("a", "b") != guard.code_for("a", "c")
        assert guard.code_for("ab") != guard.code_for("a", "b")
        assert len(guard.code_for("x")) == 8

    @pytest.mark.parametrize("mode", [None, "legacy"])
    async def test_the_operator_is_asked(
        self, harness: Any, cli: FakeCli, mode: str | None
    ) -> None:
        script(cli, "osd pool delete rbd rbd --yes-i-really-really-mean-it")
        operator = Operator()
        options = {**operator.client, **({"mode": mode} if mode else {})}
        async with harness(client=options, mode="admin", allow_destructive=True) as h:
            schema = (await h.tools())["pool_delete"].input_schema
            assert set(schema["properties"]) == {"pool", "confirm"}  # approval is not an argument
            done = await h.call("pool_delete", pool="rbd", confirm="rbd")
        assert operator.asked == ["Delete pool 'rbd' and all data in it? This cannot be undone."]
        assert done["change"] == 1 and len(cli.changes) == 1

    @pytest.mark.parametrize(
        "operator", [Operator("decline"), Operator("cancel"), Operator(approve=False)]
    )
    async def test_no_means_no(self, harness: Any, cli: FakeCli, operator: Operator) -> None:
        script(cli, "osd pool delete rbd rbd --yes-i-really-really-mean-it")
        async with harness(client=operator.client, mode="admin", allow_destructive=True) as h:
            message = await h.error("pool_delete", pool="rbd", confirm="rbd")
            assert "the operator did not approve" in message
            assert (await h.call("change_log"))["count"] == 0
        assert len(operator.asked) == 1 and cli.changes == []

    async def test_a_client_that_cannot_ask(self, harness: Any, cli: FakeCli) -> None:
        script(cli, "osd pool delete rbd rbd --yes-i-really-really-mean-it")
        async with harness(mode="admin", allow_destructive=True) as h:
            # The repeated pool name is then the confirmation, as before.
            assert "confirm must repeat" in await h.error("pool_delete", pool="rbd", confirm="x")
            await h.call("pool_delete", pool="rbd", confirm="rbd")
        async with harness(mode="admin", allow_destructive=True, approval="required") as h:
            message = await h.error("pool_delete", pool="rbd", confirm="rbd")
            assert "requires a person to approve" in message
        assert len(cli.changes) == 1

    async def test_approval_switched_off(self, harness: Any, cli: FakeCli) -> None:
        script(cli, "osd pool delete rbd rbd --yes-i-really-really-mean-it")
        operator = Operator("decline")
        options = {"mode": "admin", "allow_destructive": True, "approval": "off"}
        async with harness(client=operator.client, **options) as h:
            await h.call("pool_delete", pool="rbd", confirm="rbd")
        assert operator.asked == [] and len(cli.changes) == 1


OK_56 = {"ok_to_stop": True, "osds": [5, 6], "num_ok_pgs": 161, "num_not_ok_pgs": 0}
ENTER_3 = "cluster maintenance enter cephmcp-3 --stop-osds"
EXIT_3 = "cluster maintenance exit cephmcp-3"


def set_osds(cli: FakeCli, ids: tuple[int, ...], up: bool) -> None:
    """Make the fake cluster show these OSDs up or down, with matching PG states."""
    dump = cli.load("osd dump")
    for osd in dump["osds"]:
        if osd["osd"] in ids:
            osd["up"] = int(up)
    cli.set_json("osd dump", dump)
    status = cli.load("status")
    total = status["pgmap"]["num_pgs"]
    state = "active+clean" if up else "active+undersized+degraded"
    status["pgmap"]["pgs_by_state"] = [{"state_name": state, "count": total}]
    status["health"]["status"] = "HEALTH_OK" if up else "HEALTH_WARN"
    cli.set_json("status", status)


def microceph_lab(cli: FakeCli) -> None:
    """The three-node MicroCeph cluster, with a maintenance mode that works."""
    cli.use("microceph")
    cli.set_json("osd ok-to-stop 5 6", OK_56)
    cli.responses["mon ok-to-stop cephmcp-3"] = ""

    def enter() -> str:
        set_osds(cli, (5, 6), up=False)
        return "node cephmcp-3 is in maintenance"

    def leave() -> str:
        set_osds(cli, (5, 6), up=True)
        return "node cephmcp-3 is back"

    cli.microceph.update({ENTER_3: enter, EXIT_3: leave})


def another_osd(cli: FakeCli) -> None:
    """Host cephmcp-3 gains osd.7, which holds no data yet."""
    tree = cli.load("osd df tree")
    new = dict(next(n for n in tree["nodes"] if n.get("id") == 6), id=7, name="osd.7")
    tree["nodes"].append(new)
    next(n for n in tree["nodes"] if n.get("name") == "cephmcp-3")["children"].append(7)
    cli.set_json("osd df tree", tree)
    cli.set_json("osd ok-to-stop 5 6 7", {**OK_56, "osds": [5, 6, 7]})


class TestMaintenance:
    async def test_plan_then_confirm_then_apply(self, harness: Any, cli: FakeCli) -> None:
        microceph_lab(cli)
        async with harness(mode="admin", microceph_cmd=("microceph",)) as h:
            plan = await h.call("maintenance_enter", host="cephmcp-3")
            assert plan["applied"] is False and plan["status"] == "confirmation needed"
            assert plan["assessment"] == "safe" and plan["managed_by"] == "MicroCeph"
            assert plan["osds"] == ["osd.5", "osd.6"]
            assert plan["data"]["pgs_losing_a_copy"] == 161
            assert plan["data"]["pgs_that_stop_serving_io"] == 0
            assert plan["would_run"] == [f"microceph {ENTER_3}"]
            assert f"confirm='{plan['confirm']}'" in plan["next"]
            assert cli.changes == []  # nothing has been touched yet

            wrong = await h.call("maintenance_enter", host="cephmcp-3", confirm="00000000")
            assert wrong["applied"] is False and cli.changes == []

            done = await h.call("maintenance_enter", host="cephmcp-3", confirm=plan["confirm"])
            assert done["applied"] is True
            assert done["status"] == (
                "cephmcp-3 is in maintenance: its OSDs are down; the host can be powered off "
                "or rebooted"
            )
            assert done["ran"] == [f"microceph {ENTER_3}"]
            assert done["verified"] == {
                "osds_down": ["osd.5", "osd.6"],
                "pgs_serving_io": "161 of 161",
                "pgs_degraded": 161,
                "health": "HEALTH_WARN",
                "settled_after_s": 0,
            }
            assert done["afterwards"] == "maintenance_exit host='cephmcp-3' when the host is back"
            entry = await h.call("change_log", change_id=done["change"])
            assert entry["tool"] == "maintenance_enter"
            assert entry["undo_commands"] == [f"microceph {EXIT_3}"]
        assert cli.changes == [f"microceph {ENTER_3}"]

    async def test_the_operator_approves_at_the_client(self, harness: Any, cli: FakeCli) -> None:
        microceph_lab(cli)
        operator = Operator()
        async with harness(client=operator.client, mode="admin", microceph_cmd=("microceph",)) as h:
            done = await h.call("maintenance_enter", host="cephmcp-3")
        assert done["applied"] is True
        question = operator.asked[0].splitlines()
        assert question[0] == "Put host cephmcp-3 into maintenance?"
        assert question[1] == "Assessment: safe."
        assert question[2] == (
            "Its 2 OSDs go down (osd.5, osd.6): 161 PGs lose a copy, 0 stop serving I/O."
        )
        assert "mon: cephmcp-3 stop; 2 of 3 monitors remain, quorum (2) holds" in question
        assert question[-1] == f"This runs: microceph {ENTER_3}"

    async def test_the_operator_declines(self, harness: Any, cli: FakeCli) -> None:
        microceph_lab(cli)
        operator = Operator("decline")
        async with harness(client=operator.client, mode="admin", microceph_cmd=("microceph",)) as h:
            assert "did not approve" in await h.error("maintenance_enter", host="cephmcp-3")
        assert cli.changes == []

    async def test_unsafe_is_refused_before_anyone_is_asked(
        self, harness: Any, cli: FakeCli
    ) -> None:
        cli.use("cephadm")
        cli.fail("osd ok-to-stop 0 1 2", "Error EBUSY: unsafe", 16, '{"ok_to_stop": false}')
        cli.fail("mon ok-to-stop cephmcp-adm", "Error EBUSY: not enough monitors", 16)
        cli.set_json("orch status", {"available": True, "backend": "cephadm"})
        operator = Operator()
        async with harness(client=operator.client, mode="admin") as h:
            out = await h.call("maintenance_enter", host="cephmcp-adm")
            assert out["applied"] is False
            assert out["status"] == "refused: taking this host down is not safe"
            assert out["assessment"] == "unsafe" and out["managed_by"] == "cephadm"
            assert any("lose their quorum" in r for r in out["reasons"])
            assert "confirm" not in out
            # Forcing it takes data offline: that needs the destructive switch.
            message = await h.error("maintenance_enter", host="cephmcp-adm", force=True)
            assert "--allow-destructive" in message
        assert operator.asked == [] and cli.changes == []

    async def test_leaving_maintenance(self, harness: Any, cli: FakeCli) -> None:
        microceph_lab(cli)
        async with harness(mode="admin", microceph_cmd=("microceph",), approval="off") as h:
            await h.call("maintenance_enter", host="cephmcp-3")
            back = await h.call("maintenance_exit", host="cephmcp-3")
            assert back["applied"] is True
            assert back["status"] == "cephmcp-3 is back in service: all PGs active+clean"
            assert back["ran"] == [f"microceph {EXIT_3}"]
            assert back["verified"]["osds_up"] == ["osd.5", "osd.6"]
            assert back["verified"]["pgs_not_clean"] == 0
            log = rows(await h.call("change_log"))
            assert [c["tool"] for c in log] == ["maintenance_exit", "maintenance_enter"]
            assert log[1]["undo"] == "undone by change 2"  # nothing is left to undo
        assert cli.changes == [f"microceph {ENTER_3}", f"microceph {EXIT_3}"]

    async def test_undo_leaves_maintenance(self, harness: Any, cli: FakeCli) -> None:
        microceph_lab(cli)
        async with harness(mode="admin", microceph_cmd=("microceph",), approval="off") as h:
            done = await h.call("maintenance_enter", host="cephmcp-3")
            undone = await h.call("change_undo", change_id=done["change"])
        assert undone["ran"] == [f"microceph {EXIT_3}"]

    async def test_waits_for_the_osds_to_stop(self, harness: Any, cli: FakeCli, clock: Any) -> None:
        microceph_lab(cli)
        # The daemons take a while: they show as down only 9 seconds after the command.
        cli.microceph[ENTER_3] = "ok"
        started = clock.now
        up = (json.dumps(cli.load("osd dump")), json.dumps(cli.load("status")))
        set_osds(cli, (5, 6), up=False)
        down = (cli.responses["osd dump"], cli.responses["status"])
        cli.responses["osd dump"] = lambda: down[0] if clock.now - started >= 9 else up[0]
        cli.responses["status"] = lambda: down[1] if clock.now - started >= 9 else up[1]
        async with harness(mode="admin", microceph_cmd=("microceph",), approval="off") as h:
            done = await h.call("maintenance_enter", host="cephmcp-3", wait=60)
        assert done["verified"]["osds_down"] == ["osd.5", "osd.6"]
        assert done["verified"]["settled_after_s"] == 9

    async def test_reports_when_the_osds_do_not_stop(self, harness: Any, cli: FakeCli) -> None:
        microceph_lab(cli)
        cli.microceph[ENTER_3] = "ok"  # the command returns, but nothing stops
        async with harness(mode="admin", microceph_cmd=("microceph",), approval="off") as h:
            done = await h.call("maintenance_enter", host="cephmcp-3", wait=6)
        assert done["applied"] is True
        assert done["status"] == (
            "applied, but not as expected: 2 of the host's OSDs are still up after 6s"
        )
        assert done["verified"]["osds_still_up"] == ["osd.5", "osd.6"]

    async def test_without_an_orchestrator_only_flags_are_set(
        self, harness: Any, cli: FakeCli
    ) -> None:
        cli.use("microceph")
        cli.set_json("osd ok-to-stop 5 6", OK_56)
        script(
            cli,
            "mon ok-to-stop cephmcp-3",
            "osd set-group noout cephmcp-3",
            "osd unset-group noout cephmcp-3",
        )
        async with harness(mode="admin", approval="off") as h:
            done = await h.call("maintenance_enter", host="cephmcp-3")
            assert done["managed_by"] == "no orchestrator"
            assert done["ran"] == ["ceph osd set-group noout cephmcp-3"]
            assert done["status"] == (
                "cephmcp-3 is in maintenance: noout is set for it; stop its daemons or "
                "reboot it now"
            )
            # The operator has stopped the daemons. Leaving while they are down keeps the flag.
            set_osds(cli, (5, 6), up=False)
            early = await h.call("maintenance_exit", host="cephmcp-3", wait=0)
            assert early["applied"] is False
            assert early["status"] == "2 OSDs of cephmcp-3 are still down; noout stays set"
            assert "osd unset-group noout cephmcp-3" not in cli.changes
            # The host is back.
            set_osds(cli, (5, 6), up=True)
            back = await h.call("maintenance_exit", host="cephmcp-3")
            assert back["ran"] == ["ceph osd unset-group noout cephmcp-3"]
            assert back["status"] == "cephmcp-3 is back in service: all PGs active+clean"
            # Or it never comes back: clear the flag so that the cluster heals.
            set_osds(cli, (5, 6), up=False)
            gone = await h.call("maintenance_exit", host="cephmcp-3", wait=0, even_if_down=True)
            assert gone["status"] == "flags cleared, but 2 OSDs of cephmcp-3 are down"

    async def test_cephadm_hands_the_mgr_over_first(self, harness: Any, cli: FakeCli) -> None:
        cli.use("microceph")  # three hosts; the active mgr is on cephmcp-1
        cli.set_json("orch status", {"available": True, "backend": "cephadm"})
        cli.set_json("osd ok-to-stop 1 2", {**OK_56, "osds": [1, 2]})
        script(cli, "mon ok-to-stop cephmcp-1", "mgr fail")
        cli.responses["orch host maintenance enter cephmcp-1"] = lambda: (
            set_osds(cli, (1, 2), up=False) or "ok"
        )
        async with harness(mode="admin") as h:
            plan = await h.call("maintenance_enter", host="cephmcp-1")
            assert plan["managed_by"] == "cephadm"
            assert plan["would_run"] == [
                "ceph mgr fail",
                "ceph orch host maintenance enter cephmcp-1",
            ]
            assert plan["services"]["mgr"] == "the active mgr is here; standby cephmcp-2 takes over"
            done = await h.call("maintenance_enter", host="cephmcp-1", confirm=plan["confirm"])
            assert done["applied"] is True and done["verified"]["osds_down"] == ["osd.1", "osd.2"]
            entry = await h.call("change_log", change_id=done["change"])
            assert entry["undo_commands"] == ["ceph orch host maintenance exit cephmcp-1"]

    async def test_a_plan_that_changed_needs_a_new_confirmation(
        self, harness: Any, cli: FakeCli, clock: Any
    ) -> None:
        microceph_lab(cli)
        async with harness(mode="admin", microceph_cmd=("microceph",)) as h:
            plan = await h.call("maintenance_enter", host="cephmcp-3")
            # Meanwhile an OSD elsewhere goes down: the same host is now a different question.
            clock.now += 60
            pgs = cli.load("pg dump pgs_brief")
            for pg in pgs["pg_stats"]:
                pg["acting"] = [o for o in pg["acting"] if o != 1]
            cli.set_json("pg dump pgs_brief", pgs)
            again = await h.call("maintenance_enter", host="cephmcp-3", confirm=plan["confirm"])
        assert again["applied"] is False and cli.changes == []
        assert again["status"] == "refused: taking this host down is not safe"

    async def test_a_code_is_for_the_plan_it_came_with(
        self, harness: Any, cli: FakeCli, clock: Any
    ) -> None:
        """The assessment and the commands stay the same when the host gains an
        OSD, but it is not what the operator was shown."""
        microceph_lab(cli)
        async with harness(mode="admin", microceph_cmd=("microceph",)) as h:
            plan = await h.call("maintenance_enter", host="cephmcp-3")
            clock.now += 30
            another_osd(cli)
            again = await h.call("maintenance_enter", host="cephmcp-3", confirm=plan["confirm"])
            assert again["applied"] is False and again["status"] == "confirmation needed"
            assert again["osds"] == ["osd.5", "osd.6", "osd.7"]
            assert again["confirm"] != plan["confirm"] and cli.changes == []
            done = await h.call("maintenance_enter", host="cephmcp-3", confirm=again["confirm"])
            assert done["applied"] is True

    @pytest.mark.parametrize("mode", [None, "legacy"])
    async def test_approval_is_for_the_plan_that_was_shown(
        self, harness: Any, cli: FakeCli, clock: Any, mode: str | None
    ) -> None:
        """The operator takes a while to answer, and meanwhile the host gains an
        OSD. What was approved is not what would now be done."""
        microceph_lab(cli)
        asked: list[str] = []

        async def slow_yes(context: Any, params: Any) -> types.ElicitResult:
            asked.append(params.message)
            if len(asked) == 1:
                clock.now += 30
                another_osd(cli)
            return types.ElicitResult(action="accept", content={"approve": True})

        options = {"elicitation_callback": slow_yes, **({"mode": mode} if mode else {})}
        async with harness(client=options, mode="admin", microceph_cmd=("microceph",)) as h:
            failed, text = await h.raw("maintenance_enter", host="cephmcp-3", wait=0)
            assert "Its 2 OSDs go down (osd.5, osd.6)" in asked[0]
            if mode == "legacy":
                # The question cannot be put again within the same call: refuse.
                assert failed and "the cluster changed while the approval was pending" in text
                assert cli.changes == []
                # Asked again, the operator sees the plan as it is now.
                done = await h.call("maintenance_enter", host="cephmcp-3", wait=0)
            else:
                # The newer protocol asks again by itself when the question has changed.
                assert not failed
                done = json.loads(text)
            assert "Its 3 OSDs go down (osd.5, osd.6, osd.7)" in asked[1]
            assert done["applied"] is True and len(asked) == 2

    async def test_nothing_is_done_when_the_assessment_cannot_be_made(
        self, harness: Any, cli: FakeCli
    ) -> None:
        microceph_lab(cli)
        cli.fail("mon metadata", "Error ETIMEDOUT: timed out", 110)
        operator = Operator()
        async with harness(client=operator.client, mode="admin", microceph_cmd=("microceph",)) as h:
            assert "`ceph mon metadata` failed" in await h.error(
                "maintenance_enter", host="cephmcp-3"
            )
        assert operator.asked == [] and cli.changes == []

    async def test_says_so_when_the_result_cannot_be_verified(
        self, harness: Any, cli: FakeCli
    ) -> None:
        microceph_lab(cli)

        def enter() -> str:
            cli.fail("status", "Error ETIMEDOUT: timed out", 110)  # the cluster stops answering
            return "ok"

        cli.microceph[ENTER_3] = enter
        async with harness(mode="admin", microceph_cmd=("microceph",), approval="off") as h:
            done = await h.call("maintenance_enter", host="cephmcp-3", wait=6)
            assert done["applied"] is True and "verified" not in done
            assert done["status"].startswith("applied, but the result could not be verified")
            assert done["change"] == 1  # on record, and can be undone

    async def test_flags_stay_when_the_osds_cannot_be_seen(
        self, harness: Any, cli: FakeCli
    ) -> None:
        cli.use("microceph")
        cli.set_json("osd ok-to-stop 5 6", OK_56)
        script(cli, "mon ok-to-stop cephmcp-3", "osd set-group noout cephmcp-3")
        async with harness(mode="admin", approval="off") as h:
            await h.call("maintenance_enter", host="cephmcp-3")
            dump = json.dumps(cli.load("osd dump"))
            calls = len(cli.calls)
            # Leaving: the layout is still read, then the cluster stops answering.
            cli.responses["osd dump"] = lambda: (
                dump if cli.calls[calls:].count("osd dump") <= 1 else "not json"
            )
            out = await h.call("maintenance_exit", host="cephmcp-3", wait=3)
        assert out["applied"] is False
        assert (
            out["status"] == "the state of the OSDs of cephmcp-3 could not be read; noout stays set"
        )
        assert "osd unset-group noout cephmcp-3" not in cli.calls

    async def test_failure_of_the_command_is_reported_and_journaled(
        self, harness: Any, cli: FakeCli
    ) -> None:
        microceph_lab(cli)
        del cli.microceph[ENTER_3]
        async with harness(mode="admin", microceph_cmd=("microceph",), approval="off") as h:
            message = await h.error("maintenance_enter", host="cephmcp-3")
            assert "microceph cluster maintenance enter" in message
            assert rows(await h.call("change_log"))[0]["outcome"].startswith("failed")

    async def test_arguments(self, harness: Any, cli: FakeCli) -> None:
        cli.use("microceph")
        async with harness(mode="admin") as h:
            assert "the hosts are: cephmcp-1" in await h.error("maintenance_enter", host="nope")
            assert "the hosts are: cephmcp-1" in await h.error("maintenance_exit", host="nope")
        async with harness() as h:
            tools = await h.tools()
            assert "maintenance_enter" not in tools and "maintenance_exit" not in tools
