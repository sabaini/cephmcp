"""What-if simulation."""

from __future__ import annotations

import json
from typing import Any

import pytest
import synth
from conftest import rows

from cephmcp import whatif
from cephmcp.whatif import CAVEATS, DATA_LOSS, SAFE, UNSAFE, Services

GIB = 2**30


def three_by_two(used: float = 10, pool_used: float = 30, **pool: Any) -> Any:
    return synth.topology(
        synth.three_hosts(used=used), [synth.pool(1, "rbd", used=pool_used, **pool)]
    )


def spread() -> list[dict[str, Any]]:
    """PGs of a 3x pool on hosts a (0,1), b (2,3), c (4,5): one copy per host."""
    return synth.pgs(1, [[0, 2, 4], [1, 3, 5], [0, 3, 4], [1, 2, 5]])


class TestSizes:
    def test_parse(self) -> None:
        assert whatif.parse_size("4T") == 4 * 2**40
        assert whatif.parse_size("960G") == 960 * GIB
        assert whatif.parse_size("1.5TiB") == int(1.5 * 2**40)
        assert whatif.parse_size(" 500 gb ") == 500 * GIB
        for bad in ("4", "big", "4X", "-1T", ""):
            with pytest.raises(ValueError, match="is not a size"):
                whatif.parse_size(bad)


class TestData:
    def outcome(self, topo: Any, pgs: Any, down: set[int], permanent: bool) -> whatif.Result:
        result = whatif.Result("x")
        whatif.data_section(topo, topo.outage(pgs, down), result, permanent=permanent)
        return result

    def test_one_host_of_three_is_safe(self) -> None:
        result = self.outcome(three_by_two(), spread(), {0, 1}, permanent=False)
        assert result.verdict == SAFE and result.reasons == []
        data = result.sections["data"]
        assert data["pgs_losing_a_copy"] == 4 and data["pgs_that_stop_serving_io"] == 0
        assert rows(data["by_pool"])[0]["redundancy"] == "3x, min_size 2"

    def test_two_hosts_stop_io_but_lose_nothing(self) -> None:
        topo = three_by_two()
        away = self.outcome(topo, spread(), {0, 1, 2, 3}, permanent=False)
        assert away.verdict == UNSAFE
        assert (
            "4 PGs in rbd drop below min_size and stop serving I/O for the duration"
            in away.reasons[0]
        )
        gone = self.outcome(topo, spread(), {0, 1, 2, 3}, permanent=True)
        assert (
            gone.verdict == UNSAFE
            and "until recovery has restored enough copies" in gone.reasons[0]
        )

    def test_losing_every_copy_is_data_loss(self) -> None:
        topo = three_by_two()
        gone = self.outcome(topo, spread(), set(range(6)), permanent=True)
        assert gone.verdict == DATA_LOSS and "their data is lost" in gone.reasons[0]
        away = self.outcome(topo, spread(), set(range(6)), permanent=False)
        assert away.verdict == UNSAFE
        assert "(4 of them have every copy on these OSDs)" in away.reasons[0]

    def test_two_copies_run_on_the_last_one(self) -> None:
        topo = three_by_two(size=2, min_size=1)
        result = self.outcome(topo, synth.pgs(1, [[0, 2], [1, 4], [3, 5]]), {0, 1}, permanent=False)
        assert result.verdict == CAVEATS
        assert "2 PGs in rbd keep running on their last copy" in result.reasons[0]
        assert result.sections["data"]["pgs_left_without_redundancy"] == 2

    def test_pgs_that_are_inactive_already_are_not_blamed_on_the_scenario(self) -> None:
        topo = three_by_two()
        pgs = synth.pgs(1, [[0, 2, 4]]) + synth.pgs(1, [[1]], state="down+incomplete")
        result = self.outcome(topo, pgs, {5}, permanent=False)
        assert result.verdict == SAFE
        # A PG that is only peering at this moment does count: it has its copies.
        peering = synth.pgs(1, [[0, 2, 4]], state="peering")
        assert self.outcome(topo, peering, {0, 2}, permanent=False).verdict == UNSAFE
        assert result.notes == ["1 PGs are not active right now, independent of this."]


class TestServices:
    services = Services(
        mons={"a": "a", "b": "b", "c": "c"},
        quorum=["a", "b", "c"],
        mgr_active="x",
        mgrs={"x": "a", "y": "b"},
        mds=[
            {"name": "m1", "state": "active", "host": "a"},
            {"name": "m2", "state": "standby", "host": "b"},
        ],
        other={"rgw": [("gw", "c")], "nfs": [("n1", "a"), ("n2", "b")]},
    )

    def outcome(self, hosts: list[str], services: Services | None = None) -> whatif.Result:
        result = whatif.Result("x")
        whatif.services_section(services or self.services, hosts, result, permanent=False)
        return result

    def test_host_with_everything_on_it(self) -> None:
        result = self.outcome(["a"])
        assert result.verdict == SAFE
        assert result.sections["services"] == {
            "mon": "a stop; 2 of 3 monitors remain, quorum (2) holds",
            "mgr": "the active mgr is here; standby y takes over",
            "mds": "active MDS m1 stops; standby m2 takes over",
            "nfs": "1 of 2 nfs daemons stop; 1 remain",
        }
        assert result.notes == ["No further monitor may fail meanwhile: quorum is exact."]

    def test_quorum(self) -> None:
        result = self.outcome(["a", "b"])
        assert result.verdict == UNSAFE
        assert "lose their quorum (1 of 3 left, 2 needed)" in result.reasons[0]
        # A monitor that is out of the quorum already does not count as remaining.
        weak = Services(mons={"a": "a", "b": "b", "c": "c"}, quorum=["a", "b"])
        assert self.outcome(["a"], weak).verdict == UNSAFE

    def test_singletons(self) -> None:
        assert self.outcome(["c"]).reasons == ["The rgw service is down for the duration."]
        assert self.outcome(["c"]).verdict == CAVEATS
        lone = Services(
            mgr_active="x", mgrs={"x": "a"}, mds=[{"name": "m", "state": "active", "host": "a"}]
        )
        result = self.outcome(["a"], lone)
        assert result.verdict == UNSAFE
        assert result.reasons == [  # the most serious first
            "CephFS loses its metadata server: the file system hangs.",
            "No mgr remains: metrics, dashboard, orchestrator and balancer stop (client "
            "I/O continues).",
        ]

    def test_uninvolved_host(self) -> None:
        assert "services" not in self.outcome(["elsewhere"]).sections


class TestRecovery:
    def outcome(self, topo: Any, gone: set[int], planned: bool = False) -> whatif.Result:
        result = whatif.Result("x")
        whatif.recovery_section(topo, gone, result, planned=planned)
        return result

    def test_three_hosts_cannot_replace_a_host(self) -> None:
        result = self.outcome(three_by_two(), {0, 1})
        assert result.verdict == CAVEATS
        assert "Full redundancy cannot be restored for rbd (2 of 3 hosts left)" in result.reasons[0]
        assert result.sections["recovery"]["can_restore_full_redundancy"] is False

    def test_an_osd_is_taken_over_by_its_neighbour(self) -> None:
        result = self.outcome(three_by_two(), {0})
        recovery = result.sections["recovery"]
        assert result.verdict == SAFE and recovery["can_restore_full_redundancy"]
        assert recovery["data_to_recreate"] == "5 GiB"
        assert recovery["fullest_osd_afterwards"] == "osd.1 at 15% (now 10%)"
        assert rows(recovery["osds_afterwards"]) == [
            {
                "osd": "osd.1",
                "host": "a",
                "used_pct_now": 10,
                "used_pct_after": 15,
                "change": "5 GiB",
            }
        ]
        assert recovery["raw_capacity"] == "600 GiB -> 500 GiB"

    def test_no_room(self) -> None:
        full = self.outcome(three_by_two(used=60, pool_used=360), {0})
        assert full.verdict == UNSAFE and "past the full limit (95%)" in full.reasons[0]
        stall = self.outcome(three_by_two(used=46, pool_used=276), {0})
        assert stall.verdict == CAVEATS and "Recovery would stall part-way" in stall.reasons[0]
        planned = self.outcome(three_by_two(used=46, pool_used=276), {0}, planned=True)
        assert planned.verdict == UNSAFE  # a drain that cannot finish should not be started
        assert "data_to_move" in planned.sections["recovery"]
        near = self.outcome(three_by_two(used=43, pool_used=258), {0})
        assert near.verdict == CAVEATS and "past the nearfull limit (85%)" in near.reasons[0]

    def test_too_few_domains_to_serve_io(self) -> None:
        topo = synth.topology(
            synth.three_hosts(per_host=1),
            [
                synth.pool(1, "strict", size=3, min_size=3),
                synth.pool(2, "ec", size=3, min_size=3, profile="ec21"),
            ],
        )
        result = self.outcome(topo, {0})
        assert result.verdict == UNSAFE
        assert "strict (2 of 3 hosts left, min_size 3)" in result.reasons[0]


class TestExpansion:
    def test_new_host(self) -> None:
        topo = three_by_two()
        result = whatif.Result("x")
        whatif.expansion_section(topo, "d", [100 * GIB, 100 * GIB], None, result)
        grow = result.sections["expansion"]
        assert result.verdict == SAFE
        assert grow["adds"] == "2 x 100 GiB" and grow["on"] == "d (a new host)"
        assert grow["raw_capacity"] == "600 GiB -> 800 GiB"
        assert grow["raw_used_pct"] == "10.0 -> 7.5"
        # A quarter of the data moves to the new quarter of the cluster.
        assert grow["data_that_moves"] == "7.5 GiB" and grow["share_of_all_data"] == "25.0%"
        assert grow["fullest_osd"].startswith("osd.0 at 10% -> osd.")
        new = [r for r in rows(grow["osds_afterwards"]) if r["used_pct_now"] == "new"]
        assert len(new) == 2 and new[0]["host"] == "d" and new[0]["change"] == "3.75 GiB"

    def test_more_disks_in_one_host_do_not_help_with_one_copy_per_host(self) -> None:
        topo = three_by_two()
        result = whatif.Result("x")
        whatif.expansion_section(topo, "a", [400 * GIB], None, result)
        grow = result.sections["expansion"]
        # Host a still holds exactly one copy of everything: data only moves within it.
        assert grow["onto_the_new_osds"] == "6.67 GiB"
        assert any("much more capacity than any other host" in n for n in result.notes)

    def test_a_class_no_rule_uses(self) -> None:
        topo = synth.topology(
            synth.three_hosts(),
            [synth.pool(1, "rbd", used=30)],
            [synth.rule(0, "hdd-only", take="default~hdd")],
        )
        result = whatif.Result("x")
        whatif.expansion_section(topo, "d", [100 * GIB], "nvme", result)
        assert result.verdict == CAVEATS and "No pool would use the new OSDs" in result.reasons[0]
        assert result.sections["expansion"]["data_that_moves"] == "0 B"

    def test_a_fourth_host_lets_a_wide_pool_place_more(self) -> None:
        topo = synth.topology(
            synth.three_hosts(), [synth.pool(1, "wide", size=4, min_size=3, profile="ec21")]
        )
        result = whatif.Result("x")
        whatif.expansion_section(topo, "d", [100 * GIB], None, result)
        assert any("wide (3 -> 4 of 4 copies)" in n for n in result.notes)


OK_56 = {"ok_to_stop": True, "osds": [5, 6], "num_ok_pgs": 161, "num_not_ok_pgs": 0}


@pytest.mark.anyio
class TestTool:
    async def test_reboot_of_one_host_of_three(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        cli.set_json("osd ok-to-stop 5 6", OK_56)
        cli.responses["mon ok-to-stop cephmcp-3"] = ""
        async with harness() as h:
            out = await h.call("simulate", scenario="reboot", hosts=["cephmcp-3"])
        assert out["scenario"] == "host cephmcp-3 (osd.5, osd.6) stop for a while and come back"
        assert out["verdict"] == "safe" and "reasons" not in out
        assert out["data"]["pgs_total"] == 161 and out["data"]["pgs_losing_a_copy"] == 161
        assert out["data"]["pgs_that_stop_serving_io"] == 0
        assert out["services"]["mon"] == "cephmcp-3 stop; 2 of 3 monitors remain, quorum (2) holds"
        assert out["services"]["mgr"] == "standby cephmcp-3 stops; the active mgr is elsewhere"
        assert out["ceph_agrees"] == {"osd ok-to-stop": "yes", "mon ok-to-stop": "yes"}
        assert "ceph osd set-group noout cephmcp-3" in out["how"]
        assert {"osd ok-to-stop 5 6", "mon ok-to-stop cephmcp-3"} <= set(cli.calls)
        assert cli.changes == []

    async def test_the_host_with_the_only_gateway(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        cli.set_json("osd ok-to-stop 3 4", {**OK_56, "osds": [3, 4]})
        cli.responses["mon ok-to-stop cephmcp-2"] = ""
        async with harness() as h:
            out = await h.call("simulate", scenario="reboot", hosts=["cephmcp-2"])
        assert out["verdict"] == "safe, with caveats"
        assert out["reasons"] == ["The rgw service is down for the duration."]

    async def test_two_hosts_of_three(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        refusal = (
            "Error EBUSY: unsafe to stop osd(s) at this time (161 PGs are or would become offline)"
        )
        cli.fail(
            "osd ok-to-stop 3 4 5 6",
            refusal,
            16,
            json.dumps({"ok_to_stop": False, "num_not_ok_pgs": 161}),
        )
        cli.fail("mon ok-to-stop cephmcp-2 cephmcp-3", "Error EBUSY: not enough monitors", 16)
        async with harness() as h:
            out = await h.call("simulate", scenario="reboot", hosts=["cephmcp-2", "cephmcp-3"])
        assert out["verdict"] == "unsafe"
        assert "161 PGs in" in out["reasons"][0] and "stop serving I/O" in out["reasons"][0]
        assert "lose their quorum (1 of 3 left, 2 needed)" in out["reasons"][1]
        assert out["ceph_agrees"]["osd ok-to-stop"] == "no: 161 PGs are or would become inactive"
        assert "how" not in out

    async def test_ceph_has_the_last_word(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        cli.fail(
            "osd ok-to-stop 1",
            "Error EBUSY: unsafe to stop osd(s) at this time (2 PGs are or would become offline)",
            16,
            json.dumps({"ok_to_stop": False, "num_not_ok_pgs": 2}),
        )
        async with harness() as h:
            out = await h.call("simulate", scenario="reboot", osds=[1])
        assert out["verdict"] == "unsafe"
        assert out["reasons"] == [
            "`ceph osd ok-to-stop` refuses: Error EBUSY: unsafe to stop osd(s) at this time "
            "(2 PGs are or would become offline)"
        ]

    async def test_single_host_cluster_cannot_be_rebooted(self, harness: Any, cli: Any) -> None:
        cli.use("cephadm")
        cli.fail("osd ok-to-stop 0 1 2", "Error EBUSY: unsafe", 16, '{"ok_to_stop": false}')
        cli.fail("mon ok-to-stop cephmcp-adm", "Error EBUSY: not enough monitors", 16)
        async with harness() as h:
            out = await h.call("simulate", scenario="reboot", hosts=["cephmcp-adm"])
        assert out["verdict"] == "unsafe"
        assert "33 PGs in .mgr, testpool drop below min_size" in out["reasons"][0]
        assert "(33 of them have every copy on these OSDs)" in out["reasons"][0]
        assert any("lose their quorum (0 of 1 left" in r for r in out["reasons"])
        assert out["cluster_now"]["health"] == "HEALTH_WARN"
        assert out["cluster_now"]["pgs_not_active_clean"] == 16
        assert out["notes"][0] == "16 PGs are not active right now, independent of this."

    async def test_losing_an_osd(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            out = await h.call("simulate", scenario="fail", osds=[5])
        assert out["scenario"] == "osd.5 are lost for good, without warning"
        assert out["verdict"] == "safe"
        assert out["recovery"]["can_restore_full_redundancy"] is True
        # Three hosts for three copies: the other OSD of that host takes it all.
        assert [r["osd"] for r in rows(out["recovery"]["osds_afterwards"])] == ["osd.6"]
        assert out["recovery"]["raw_capacity"] == "48 GiB -> 40 GiB"
        assert "ceph_agrees" not in out and cli.changes == []

    async def test_losing_a_host(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            out = await h.call("simulate", scenario="fail", hosts=["cephmcp-2"])
        assert out["verdict"] == "safe, with caveats"
        assert "Full redundancy cannot be restored for" in out["reasons"][0]
        assert "The rgw service is down until it is redeployed elsewhere." in out["reasons"]

    async def test_losing_what_holds_all_copies(self, harness: Any, cli: Any) -> None:
        cli.use("cephadm")
        async with harness() as h:
            out = await h.call("simulate", scenario="fail", osds=[0, 1])
        assert out["verdict"] == "data loss"
        assert "would have too few copies left to rebuild from" in out["reasons"][0]

    async def test_drain(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            out = await h.call("simulate", scenario="drain", osds=[1])
            assert out["verdict"] == "safe" and "data" not in out
            assert out["recovery"]["data_to_move"] and "data_to_recreate" not in out["recovery"]
            assert any("safe-to-destroy" in n for n in out["notes"])
            host = await h.call("simulate", scenario="drain", hosts=["cephmcp-1"])
            assert host["verdict"] == "safe, with caveats"
            assert "mon" in host["services"]

    async def test_add(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            out = await h.call(
                "simulate", scenario="add", add_host="cephmcp-4", add_count=2, add_size="8G"
            )
        assert out["scenario"] == "add 2 OSD(s) of 8G to cephmcp-4"
        assert out["verdict"] == "safe"
        grow = out["expansion"]
        assert grow["on"] == "cephmcp-4 (a new host)" and grow["adds"] == "2 x 8 GiB"
        assert grow["raw_capacity"] == "48 GiB -> 64 GiB"
        assert grow["share_of_all_data"] == "25.0%"

    @pytest.mark.parametrize(
        "command",
        ["mon metadata", "mgr metadata", "fs status", "service dump", "df detail", "status"],
    )
    async def test_no_verdict_from_a_gap(self, harness: Any, cli: Any, command: str) -> None:
        """A monitor list that could not be read is not a cluster without
        monitors: "safe" must not be the answer to a failed lookup."""
        cli.use("microceph")
        cli.set_json("osd ok-to-stop 5 6", OK_56)
        cli.responses["mon ok-to-stop cephmcp-3"] = ""
        cli.fail(command, "Error ETIMEDOUT: timed out", 110)
        async with harness() as h:
            message = await h.error("simulate", scenario="reboot", hosts=["cephmcp-3"])
        assert f"`ceph {command}` failed" in message

    async def test_arguments(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            assert "the hosts are: cephmcp-1, cephmcp-2, cephmcp-3" in await h.error(
                "simulate", scenario="reboot", hosts=["nope"]
            )
            assert "there is no osd.42" in await h.error("simulate", scenario="fail", osds=[42])
            assert "needs hosts or osds" in await h.error("simulate", scenario="reboot")
            assert "needs add_host and add_size" in await h.error("simulate", scenario="add")
            assert "is not a size" in await h.error(
                "simulate", scenario="add", add_host="x", add_size="lots"
            )
            assert "takes add_host" in await h.error(
                "simulate", scenario="add", hosts=["cephmcp-1"], add_host="x", add_size="1T"
            )
            # The ceph command line cuts its arguments at this word.
            assert "reserves it" in await h.error(
                "simulate", scenario="reboot", hosts=["injectargs"]
            )
            assert "reserves it" in await h.error("pool_list", pool="injectargs")
