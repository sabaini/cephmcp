"""The cluster model, the review checks and the runbooks."""

from __future__ import annotations

import json
from typing import Any

import pytest
import synth
from conftest import FIXTURES

from cephmcp import review
from cephmcp.runbooks import RUNBOOKS, Situation
from cephmcp.topology import CRUSH_ITEM_NONE, build, parse_rule

GIB = 2**30


def codes(inputs: review.Inputs) -> dict[str, review.Finding]:
    return {f.code: f for f in review.run(inputs)}


def inputs(*args: Any, **extra: Any) -> review.Inputs:
    known = {f for f in review.Inputs.__dataclass_fields__}
    fields = {k: extra.pop(k) for k in list(extra) if k in known}
    return review.Inputs(topology=synth.topology(*args, **extra), **fields)


def healthy(**extra: Any) -> review.Inputs:
    """Four hosts, 3x pool, plenty of room: nothing to find."""
    hosts = synth.three_hosts()
    hosts["d"] = [{"id": 6, "used": 10}, {"id": 7, "used": 10}]
    return inputs(hosts, [synth.pool(1, "rbd", used=60)], **extra)


class TestTopology:
    def test_rules(self) -> None:
        by_host = parse_rule(synth.rule(0, "r", "host"))
        assert (by_host.failure_domain, by_host.root, by_host.device_class) == (
            "host",
            "default",
            None,
        )
        by_class = parse_rule(synth.rule(1, "fast", "rack", take="default~ssd"))
        assert (by_class.failure_domain, by_class.root, by_class.device_class) == (
            "rack",
            "default",
            "ssd",
        )
        assert parse_rule(synth.rule(2, "flat", "osd")).failure_domain == "osd"
        stretch = parse_rule(
            {
                "rule_id": 3,
                "rule_name": "stretch",
                "steps": [
                    {"op": "take", "item_name": "default"},
                    {"op": "choose_firstn", "num": 0, "type": "datacenter"},
                    {"op": "chooseleaf_firstn", "num": 2, "type": "host"},
                    {"op": "emit"},
                ],
            }
        )
        assert stretch.failure_domain == "host" and not stretch.simple
        two_step = parse_rule(
            {
                "rule_id": 4,
                "rule_name": "two",
                "steps": [
                    {"op": "take", "item_name": "default"},
                    {"op": "choose_firstn", "num": 0, "type": "host"},
                    {"op": "choose_firstn", "num": 1, "type": "osd"},
                    {"op": "emit"},
                ],
            }
        )
        assert two_step.failure_domain == "host"

    def test_tree(self) -> None:
        topo = synth.topology(synth.three_hosts(), [synth.pool(1, "rbd")])
        assert sorted(topo.hosts()) == ["a", "b", "c"]
        assert topo.osds[3].host == "b" and topo.osds[3].under("default")
        assert [o.id for o in topo.osds_of("c")] == [4, 5]
        assert [o.id for o in topo.osds_of("osd.2")] == [2]
        assert topo.osds[0].used_pct == pytest.approx(10.0)

    def test_shares_follow_weight_within_the_failure_domain(self) -> None:
        topo = synth.topology(synth.three_hosts(), [synth.pool(1, "rbd", used=30)])
        # Three hosts, three copies: one copy per host, split between its two OSDs.
        assert topo.shares(topo.pools[1]) == {i: pytest.approx(0.5) for i in range(6)}
        assert sum(topo.expected_bytes().values()) == pytest.approx(30 * GIB)

    def test_a_big_host_holds_at_most_one_copy(self) -> None:
        topo = synth.topology(
            {"a": [{"id": 0, "size": 1000}], "b": [{"id": 1}], "c": [{"id": 2}]},
            [synth.pool(1, "rbd", used=30)],
        )
        assert topo.shares(topo.pools[1]) == {0: 1.0, 1: 1.0, 2: 1.0}

    def test_losing_an_osd_moves_its_data(self) -> None:
        hosts = synth.three_hosts()
        topo = synth.topology(hosts, [synth.pool(1, "rbd", used=30)])
        after = topo.without([0]).expected_bytes()
        # With as many hosts as copies, the neighbour on the same host takes it all.
        assert after[0] == 0 and after[1] == pytest.approx(10 * GIB)
        assert after[2] == pytest.approx(5 * GIB)
        hosts["d"] = [{"id": 6}, {"id": 7}]
        topo = synth.topology(hosts, [synth.pool(1, "rbd", used=40)])
        spread = topo.without([0, 1]).expected_bytes()
        assert spread[2] == pytest.approx(40 * GIB / 6)

    def test_adding_osds(self) -> None:
        topo = synth.topology(synth.three_hosts(), [synth.pool(1, "rbd", used=30)])
        bigger = topo.with_new("d", [100 * GIB, 100 * GIB], None)
        assert bigger.osds[6].host == "d" and bigger.osds[6].device_class == "hdd"
        assert bigger.osds[6].under("default")
        assert bigger.expected_bytes()[6] == pytest.approx(30 * GIB / 8)
        same_host = topo.with_new("a", [100 * GIB], "ssd")
        assert same_host.osds[6].host == "a" and same_host.osds[6].device_class == "ssd"

    def test_device_class_rules_only_use_their_class(self) -> None:
        hosts = {
            h: [{"id": 2 * i, "class": "hdd"}, {"id": 2 * i + 1, "class": "ssd"}]
            for i, h in enumerate("abc")
        }
        topo = synth.topology(
            hosts,
            [synth.pool(1, "fast", used=3, rule_id=1)],
            [synth.rule(0, "any"), synth.rule(1, "fast", take="default~ssd")],
        )
        assert sorted(o.id for o in topo.eligible(topo.rules[1])) == [1, 3, 5]
        assert topo.expected_bytes()[0] == 0

    def test_outage(self) -> None:
        topo = synth.topology(
            synth.three_hosts(),
            [synth.pool(1, "rbd"), synth.pool(2, "two", size=2, min_size=1)],
        )
        pgs = synth.pgs(1, [[0, 2, 4], [1, 3, 5], [0, 3, 5]]) + synth.pgs(2, [[0, 2], [1, 3]])
        one = topo.outage(pgs, {0})
        assert one.pools["rbd"].degraded == 2 and one.pools["rbd"].inactive == 0
        assert one.pools["two"].last_copy == 1
        host = topo.outage(pgs, {0, 1, 2, 3})
        # 3x pool with min_size 2: one copy left is not enough to stay active.
        assert host.pools["rbd"].inactive == 3 and host.pools["rbd"].lost == 0
        assert host.pools["two"].lost == 2
        assert host.total("inactive") == 5

    def test_outage_counts_missing_shards_of_erasure_coded_pools(self) -> None:
        topo = synth.topology(
            synth.three_hosts(per_host=2),
            [synth.pool(1, "ec", size=6, min_size=5, profile="ec42")],
        )
        pgs = synth.pgs(1, [[0, 1, 2, 3, 4, 5]])
        assert topo.outage(pgs, {0}).pools["ec"].inactive == 0
        two = topo.outage(pgs, {0, 1}).pools["ec"]
        assert two.inactive == 1 and two.lost == 0
        assert topo.outage(pgs, {0, 1, 2}).pools["ec"].lost == 1
        holes = synth.pgs(1, [[0, CRUSH_ITEM_NONE, 2, 3, 4, 5]])
        assert topo.outage(holes, {0}).pools["ec"].inactive == 1

    def test_scoped_flags(self) -> None:
        c = synth.cluster(synth.three_hosts(), [synth.pool(1, "rbd")])
        c["osd dump"]["osds"] = [{"osd": 3, "state": ["exists", "up", "noout"]}]
        c["osd dump"]["crush_node_flags"] = {"-2": ["noout", "noin"]}
        c["osd dump"]["flags_set"] += ["noout"]
        topo = build(c["osd df tree"], c["osd dump"], c["osd crush rule dump"], c["df detail"])
        assert topo.flags == {"noout"}
        assert topo.scoped_flags == {"osd.3": ["noout"], "a": ["noout", "noin"]}


class TestChecks:
    def test_a_well_built_cluster_has_no_findings(self) -> None:
        assert review.run(healthy()) == []

    @pytest.mark.parametrize(
        ("size", "min_size", "code", "severity"),
        [
            (1, 1, "POOL_NO_REDUNDANCY", "critical"),
            (2, 1, "POOL_SIZE2_MIN1", "high"),
            (3, 1, "POOL_MIN_SIZE_1", "high"),
            (2, 2, "POOL_SIZE_2", "medium"),
            (3, 3, "POOL_MIN_SIZE_EQUALS_SIZE", "medium"),
        ],
    )
    def test_replicated_pools(self, size: int, min_size: int, code: str, severity: str) -> None:
        hosts = synth.three_hosts()
        hosts["d"] = [{"id": 6}]
        found = codes(inputs(hosts, [synth.pool(1, "vms", size=size, min_size=min_size)]))
        assert found[code].severity == severity
        assert "vms" in found[code].detail

    def test_erasure_coded_pools(self) -> None:
        hosts = {h: [{"id": i}] for i, h in enumerate("abcdefgh")}
        found = codes(
            inputs(
                hosts,
                [
                    synth.pool(1, "weak", size=6, min_size=4, profile="ec42"),
                    synth.pool(2, "fine", size=6, min_size=5, profile="ec42"),
                    synth.pool(3, "thin", size=3, min_size=3, profile="ec21"),
                ],
            )
        )
        assert "weak" in found["EC_MIN_SIZE_K"].detail
        assert "fine" not in found["EC_MIN_SIZE_K"].detail
        assert "thin" in found["EC_SINGLE_PARITY"].detail
        assert "thin" in found["POOL_MIN_SIZE_EQUALS_SIZE"].detail

    def test_copies_on_one_host(self) -> None:
        found = codes(
            inputs(synth.three_hosts(), [synth.pool(1, "rbd")], [synth.rule(0, "flat", "osd")])
        )
        assert found["CRUSH_OSD_FAILURE_DOMAIN"].severity == "high"
        assert "flat" in found["CRUSH_OSD_FAILURE_DOMAIN"].finding
        single = codes(
            inputs(
                {"only": [{"id": 0}, {"id": 1}, {"id": 2}]},
                [synth.pool(1, "rbd")],
                [synth.rule(0, "flat", "osd")],
            )
        )
        # On a single host that rule is the only one that works; the host is the finding.
        assert "CRUSH_OSD_FAILURE_DOMAIN" not in single
        assert single["SINGLE_HOST"].severity == "medium"

    def test_too_few_failure_domains(self) -> None:
        found = codes(
            inputs(
                synth.three_hosts(),
                [synth.pool(1, "wide", size=6, min_size=5, profile="ec42"), synth.pool(2, "rbd")],
            )
        )
        assert "wide needs 6 hosts, 3 available" in found["POOL_CANNOT_PLACE"].detail
        assert (
            "rbd"
            in found["NO_SPARE_FAILURE_DOMAIN"].finding + found["NO_SPARE_FAILURE_DOMAIN"].detail
        )

    def test_mixed_device_classes(self) -> None:
        hosts = {
            h: [{"id": 2 * i, "class": "hdd"}, {"id": 2 * i + 1, "class": "ssd"}]
            for i, h in enumerate("abcd")
        }
        found = codes(inputs(hosts, [synth.pool(1, "rbd")]))
        assert "hdd+ssd" in found["MIXED_DEVICE_CLASSES"].finding
        per_class = inputs(
            hosts,
            [synth.pool(1, "rbd", rule_id=1)],
            [synth.rule(1, "fast", take="default~ssd")],
        )
        assert "MIXED_DEVICE_CLASSES" not in codes(per_class)

    def test_pool_housekeeping(self) -> None:
        dump, df = synth.pool(1, "bare", application=None, quota_max_bytes=10 * GIB)
        df["stats"]["stored"] = 9 * GIB
        found = codes(
            inputs(
                synth.three_hosts(),
                [(dump, df)],
                autoscale=[
                    {
                        "pool_name": "bare",
                        "would_adjust": True,
                        "pg_autoscale_mode": "warn",
                        "pg_num_target": 8,
                        "pg_num_final": 64,
                    }
                ],
            )
        )
        assert found["POOL_NO_APPLICATION"].detail == "bare"
        assert "bare at 90%" in found["POOL_NEAR_QUOTA"].detail
        assert "has 8, should have 64" in found["PG_NUM_SUBOPTIMAL"].detail

    def test_flags(self) -> None:
        found = codes(
            healthy(
                flags_set=["sortbitwise", "noout", "norecover"],
                osds=[{"osd": 2, "state": ["exists", "up", "noout"]}],
            )
        )
        assert found["FLAGS_SET"].severity == "high"  # norecover outweighs noout
        assert "noout, norecover" in found["FLAGS_SET"].finding
        assert "osd.2: noout" in found["SCOPED_FLAGS_SET"].detail
        assert codes(healthy(flags_set=["pauserd", "pausewr"]))["FLAGS_SET"].severity == "critical"

    def test_osd_state(self) -> None:
        hosts = synth.three_hosts()
        hosts["d"] = [{"id": 6, "reweight": 0.0}, {"id": 7, "reweight": 0.8}]
        found = codes(inputs(hosts, [synth.pool(1, "rbd")]))
        assert "osd.6" in found["OSD_OUT_BUT_UP"].detail
        assert "osd.7 (0.8)" in found["OSD_REWEIGHT_OVERRIDE"].detail

    def test_shared_devices(self) -> None:
        meta = [{"id": i, "hostname": "a", "bluestore_bdev_devices": "nvme0n1"} for i in (0, 1)] + [
            {"id": 2, "hostname": "b", "devices": "sda"}
        ]
        found = codes(healthy(osd_metadata=meta))
        assert found["OSDS_SHARE_DEVICE"].severity == "low"
        assert "a:nvme0n1 carries osd.0, osd.1" in found["OSDS_SHARE_DEVICE"].detail
        flat = inputs(
            synth.three_hosts(),
            [synth.pool(1, "rbd")],
            [synth.rule(0, "flat", "osd")],
            osd_metadata=meta,
        )
        assert codes(flat)["OSDS_SHARE_DEVICE"].severity == "high"

    def test_memory(self) -> None:
        meta = [
            {"id": i, "hostname": "a" if i < 4 else "b", "mem_total_kb": str(16 * 2**20)}
            for i in range(6)
        ]
        found = codes(healthy(osd_metadata=meta))
        # 4 OSDs at the 4 GiB default on a 16 GiB host; 2 on the other.
        assert found["OSD_MEMORY_OVERCOMMIT"].severity == "low"
        assert "a: 4 OSDs" in found["OSD_MEMORY_OVERCOMMIT"].detail
        assert "b:" not in found["OSD_MEMORY_OVERCOMMIT"].detail
        tuned = healthy(
            osd_metadata=meta,
            config=[
                {
                    "section": "osd",
                    "mask": "host:a",
                    "name": "osd_memory_target",
                    "value": "2147483648",
                },
                {
                    "section": "osd.5",
                    "mask": "",
                    "name": "osd_memory_target",
                    "value": "1073741824",
                },
            ],
        )
        found = codes(tuned)
        assert "OSD_MEMORY_OVERCOMMIT" not in found
        assert "osd.5 (1 GiB)" in found["OSD_MEMORY_LOW"].detail

    def test_slow_osd(self) -> None:
        perf = {
            "osdstats": {
                "osd_perf_infos": [
                    {"id": i, "perf_stats": {"commit_latency_ms": 400 if i == 3 else 12}}
                    for i in range(8)
                ]
            }
        }
        found = codes(healthy(osd_perf=perf))
        assert "osd.3 on b: 400 ms (peers: 12 ms)" in found["SLOW_OSD"].detail
        calm = {
            "osdstats": {
                "osd_perf_infos": [
                    {"id": i, "perf_stats": {"commit_latency_ms": 30}} for i in range(8)
                ]
            }
        }
        assert "SLOW_OSD" not in codes(healthy(osd_perf=calm))

    def test_capacity(self) -> None:
        hosts = synth.three_hosts(used=40)
        hosts["d"] = [{"id": 6, "used": 80}, {"id": 7, "used": 40}]
        found = codes(inputs(hosts, [synth.pool(1, "rbd")], balancer={"active": False}))
        assert "osd.6 80%" in found["OSD_FILLING"].detail
        assert "osd.6 is 80% full" in found["UNBALANCED"].detail
        assert "BALANCER_OFF" in found
        even = synth.three_hosts(used=72)
        even["d"] = [{"id": 6, "used": 72}, {"id": 7, "used": 72}]
        assert "72%" in codes(inputs(even, [synth.pool(1, "rbd")]))["CLUSTER_FILLING"].finding

    def test_headroom(self) -> None:
        # Four hosts at 70%: losing one puts its data on the other three.
        hosts = synth.three_hosts(used=70)
        hosts["d"] = [{"id": 6, "used": 70}, {"id": 7, "used": 70}]
        tight = codes(inputs(hosts, [synth.pool(1, "rbd", used=540)]))
        assert tight["NO_HEADROOM_FOR_FAILURE"].severity == "medium"
        assert "Losing host" in tight["NO_HEADROOM_FOR_FAILURE"].detail
        # With as many hosts as copies a lost host cannot be re-created at all, but a
        # lost OSD is: by its neighbour on the same host, which here has no room for it.
        three = codes(inputs(synth.three_hosts(used=70), [synth.pool(1, "rbd", used=400)]))
        assert three["NO_HEADROOM_FOR_FAILURE"].severity == "high"
        assert "Even a single OSD failing" in three["NO_HEADROOM_FOR_FAILURE"].detail
        # One OSD per host: nothing can take over, so space is not the issue.
        alone = synth.three_hosts(used=70, per_host=1)
        assert "NO_HEADROOM_FOR_FAILURE" not in codes(
            inputs(alone, [synth.pool(1, "rbd", used=200)])
        )
        assert "NO_HEADROOM_FOR_FAILURE" not in codes(healthy())

    def test_monitors(self) -> None:
        one = codes(healthy(status={"monmap": {"num_mons": 1}}))
        assert one["SINGLE_MON"].severity == "high"
        two = codes(healthy(status={"monmap": {"num_mons": 2}}))
        assert "tolerate no more failures than 1" in two["EVEN_MON_COUNT"].detail
        crowded = healthy(
            status={"monmap": {"num_mons": 3}},
            mon_metadata=[
                {"name": "x", "hostname": "a"},
                {"name": "y", "hostname": "a"},
                {"name": "z", "hostname": "b"},
            ],
        )
        assert "a: x, y" in codes(crowded)["MONS_SHARE_HOST"].detail

    def test_managers_and_metadata_servers(self) -> None:
        found = codes(
            healthy(
                status={"mgrmap": {"available": True, "num_standbys": 0}},
                fs={"mdsmap": [{"name": "m", "state": "active"}]},
            )
        )
        assert found["NO_STANDBY_MGR"].severity == "medium"
        assert "NO_STANDBY_MDS" in found
        fine = healthy(
            status={"mgrmap": {"available": True, "num_standbys": 1}},
            fs={"mdsmap": [{"name": "m", "state": "active"}, {"name": "s", "state": "standby"}]},
        )
        assert review.run(fine) == []

    def test_versions(self) -> None:
        v = {
            "osd": {
                "ceph version 20.2.0 (abc) tentacle (stable)": 5,
                "ceph version 20.2.1 (def) tentacle (stable)": 3,
            },
            "mon": {"ceph version 20.2.1 (def) tentacle (stable)": 3},
            "overall": {
                "ceph version 20.2.0 (abc) tentacle (stable)": 5,
                "ceph version 20.2.1 (def) tentacle (stable)": 6,
            },
        }
        found = codes(healthy(versions=v))
        assert "osd: 5x 20.2.0, 3x 20.2.1" in found["MIXED_VERSIONS"].detail

    def test_crashes(self) -> None:
        crashes = [
            {"entity_name": "osd.3", "timestamp": "2026-11-10T08:00:00.0Z"},
            {"entity_name": "osd.3", "timestamp": "2026-11-11T09:30:00.0Z"},
            {"entity_name": "mgr.x", "timestamp": "2026-11-09T09:30:00.0Z"},
        ]
        found = codes(healthy(new_crashes=crashes))
        assert found["NEW_CRASHES"].severity == "high"
        assert "osd.3 (2x)" in found["NEW_CRASHES"].detail
        assert "2026-11-11 09:30:00" in found["NEW_CRASHES"].detail
        assert codes(healthy(new_crashes=crashes[2:]))["NEW_CRASHES"].severity == "medium"

    def test_configuration(self) -> None:
        def opt(section: str, name: str, value: str, mask: str = "") -> dict[str, str]:
            return {"section": section, "name": name, "value": value, "mask": mask}

        found = codes(
            healthy(
                config=[
                    opt("global", "auth_client_required", "none"),
                    opt("mon", "mon_allow_pool_delete", "true"),
                    opt("global", "mon_allow_pool_size_one", "true"),
                    opt("global", "osd_pool_default_size", "2"),
                    opt("global", "osd_pool_default_min_size", "1"),
                    opt("mon", "mon_osd_down_out_interval", "0"),
                    opt("osd", "osd_mclock_profile", "high_recovery_ops"),
                    opt("osd", "debug_osd", "20/20"),
                    opt("osd", "debug_ms", "1/5"),
                    opt("osd.3", "osd_max_backfills", "8"),
                    opt("mgr.x", "mgr/dashboard/server_addr", "10.0.0.1"),
                ]
            )
        )
        assert found["AUTH_DISABLED"].severity == "critical"
        for code in (
            "POOL_DELETE_ALLOWED",
            "POOL_SIZE_ONE_ALLOWED",
            "DEFAULT_POOL_SIZE_LOW",
            "DEFAULT_MIN_SIZE_1",
            "NO_AUTO_OUT",
            "RECOVERY_PRIORITISED",
        ):
            assert code in found, code
        assert found["DEBUG_LOGGING"].detail.startswith("osd debug_osd=20/20.")
        assert found["PER_DAEMON_OVERRIDES"].detail.startswith("osd.3 osd_max_backfills.")

    def test_health_checks(self) -> None:
        health = {
            "status": "HEALTH_ERR",
            "checks": {
                "OSDMAP_FLAGS": {
                    "severity": "HEALTH_WARN",
                    "summary": {"message": "noout flag(s) set"},
                },
                "PG_DAMAGED": {
                    "severity": "HEALTH_ERR",
                    "summary": {"message": "Possible data damage: 1 pg inconsistent"},
                    "detail": [{"message": "pg 2.1f is active+clean+inconsistent"}],
                },
                "SLOW_OPS": {"severity": "HEALTH_WARN", "summary": {"message": "3 slow ops"}},
                "MON_DISK_LOW": {
                    "severity": "HEALTH_WARN",
                    "summary": {"message": "low"},
                    "muted": True,
                },
                "TELEMETRY_CHANGED": {"severity": "HEALTH_WARN", "summary": {"message": "changed"}},
            },
            "mutes": [{"code": "MON_DISK_LOW"}],
        }
        found = review.run(healthy(health=health, flags_set=["noout"]))
        by_code = {f.code: f for f in found}
        # The flags finding says the same as OSDMAP_FLAGS, with advice: no duplicate.
        assert "HEALTH:OSDMAP_FLAGS" not in by_code and "FLAGS_SET" in by_code
        assert by_code["HEALTH:PG_DAMAGED"].severity == "critical"
        assert "2.1f" in by_code["HEALTH:PG_DAMAGED"].detail
        assert by_code["HEALTH:SLOW_OPS"].severity == "high"
        assert by_code["HEALTH:TELEMETRY_CHANGED"].severity == "medium"
        assert by_code["HEALTH_MUTED"].detail.startswith("MON_DISK_LOW.")
        assert [f.severity for f in found] == sorted(
            (f.severity for f in found), key=review.SEVERITIES.index
        )


@pytest.mark.anyio
class TestReviewTool:
    async def test_single_host_cluster(self, harness: Any, cli: Any) -> None:
        cli.use("cephadm")
        async with harness() as h:
            out = await h.call("cluster_review")
        found = {f["code"]: f for f in out["findings"]}
        assert out["summary"] == "3 high, 4 medium"
        assert out["cluster"] == {
            "health": "HEALTH_WARN",
            "hosts": 1,
            "osds": 3,
            "pools": 3,
            "raw_total": "30 GiB",
            "raw_used_pct": 7.9,
        }
        assert (
            ".mgr (2x, min_size 1), testpool (2x, min_size 1)" in found["POOL_SIZE2_MIN1"]["detail"]
        )
        assert "ecpool needs 4 hosts, 1 available" in found["POOL_CANNOT_PLACE"]["detail"]
        assert found["HEALTH:PG_AVAILABILITY"]["severity"] == "high"
        assert (
            "3 OSDs x target = 12 GiB of 7.71 GiB RAM" in found["OSD_MEMORY_OVERCOMMIT"]["detail"]
        )
        assert {"SINGLE_HOST", "SINGLE_MON", "DEFAULT_POOL_SIZE_LOW"} <= set(found)
        assert all(f["fix"] for f in out["findings"])
        assert out["findings"][0]["severity"] == "high"

    async def test_healthy_cluster_still_has_findings(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            out = await h.call("cluster_review")
            assert out["cluster"]["health"] == "HEALTH_OK"
            assert [f["code"] for f in out["findings"]] == [
                "OSD_MEMORY_OVERCOMMIT",
                "NO_SPARE_FAILURE_DOMAIN",
                "OSDS_SHARE_DEVICE",
            ]
            high = await h.call("cluster_review", min_severity="high")
        assert high["summary"] == "nothing found at severity 'high' or above"
        assert "findings" not in high and high["not_shown"] == "3 below 'high'"

    async def test_review_needs_only_the_layout(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        for command in ("config dump", "osd metadata", "balancer status", "versions", "status"):
            cli.fail(command, "Error EACCES: access denied", 13)
        async with harness() as h:
            out = await h.call("cluster_review")
            assert [f["code"] for f in out["findings"]] == ["NO_SPARE_FAILURE_DOMAIN"]
            # ... and it says what it could not look at, so that silence is not read as health.
            assert out["could_not_read"] == [
                "status",
                "config dump",
                "versions",
                "osd metadata",
                "balancer status",
            ]
            assert "does not mean there is nothing" in out["incomplete"]
            cli.fail("osd dump", "Error EACCES: access denied", 13)
            assert "osd dump" in await h.error("cluster_review")

    async def test_review_changes_nothing(self, harness: Any, cli: Any) -> None:
        cli.use("cephadm")
        async with harness(mode="admin", allow_destructive=True) as h:
            out = await h.call("cluster_review")
        assert cli.changes == [] and "could_not_read" not in out and "incomplete" not in out


@pytest.mark.anyio
class TestRunbooks:
    async def test_offered_as_prompts(self, harness: Any) -> None:
        async with harness() as h:
            prompts = await h.prompts()
            assert list(prompts) == [r.name for r in RUNBOOKS]
            assert prompts["triage"].title == "Triage a problem"
            assert [a.name for a in prompts["pre_maintenance"].arguments] == ["host"]
            assert not prompts["pre_maintenance"].arguments[0].required
            assert not prompts["checkup"].arguments
            text = await h.prompt("pre_maintenance", host="node2")
            assert "simulate scenario='reboot' hosts=['node2']" in text
            assert "This server is read-only" in text
            assert "ceph osd set-group noout node2" in text
            assert "cluster_review" in await h.prompt("checkup")
            assert 'The reported symptom: "VMs hang"' in await h.prompt(
                "triage", symptom="VMs hang"
            )

    async def test_adapt_to_the_server(self, harness: Any, cli: Any, monkeypatch: Any) -> None:
        async with harness(mode="admin") as h:
            text = await h.prompt("pre_maintenance", host="node2")
            assert "maintenance_enter host='node2'" in text
            assert "maintenance_exit host='node2'" in text
            assert "not permitted by this server" in await h.prompt("replace_osd", osd="4")
        async with harness(mode="admin", allow_destructive=True, microceph_cmd=("microceph",)) as h:
            text = await h.prompt("replace_osd", osd="4")
            assert "microceph disk remove 4" in text and "not permitted" not in text
            assert "snap refresh microceph" in await h.prompt("upgrade_readiness")

    async def test_tool(self, harness: Any) -> None:
        async with harness() as h:
            listing = await h.call("runbook")
            assert [r["name"] for r in listing["runbooks"]] == [r.name for r in RUNBOOKS]
            text = await h.call("runbook", name="incident-report", subject="-6h")
            assert text.startswith("Write a post-incident report")
            assert "what_changed since='-6h'" in text
            assert text == await h.prompt("incident_report", since="-6h")
            assert "there are: triage" in await h.error("runbook", name="nope")

    def test_lines_fit_a_terminal(self) -> None:
        for situation in (Situation(), Situation(admin=True), Situation(True, True, True)):
            for runbook in RUNBOOKS:
                for line in runbook.render(situation, "x").splitlines():
                    assert len(line) <= 100, (runbook.name, line)


def test_scenario_fixtures_are_complete() -> None:
    for scenario in ("microceph", "cephadm"):
        for name in ("osd_df_tree", "osd_dump", "osd_crush_rule_dump", "df_detail", "status"):
            assert json.loads((FIXTURES / scenario / f"{name}.json").read_text())


@pytest.mark.anyio
async def test_runbooks_only_name_tools_that_exist(harness: Any) -> None:
    """A runbook that sends the model to a tool the server does not have is worse than none."""
    import re

    async with harness(mode="admin", allow_destructive=True, microceph_cmd=("microceph",)) as h:
        offered = set(await h.tools())
    mentioned: set[str] = set()
    for situation in (Situation(), Situation(True, True, True)):
        for runbook in RUNBOOKS:
            text = runbook.render(situation, "x")
            mentioned |= set(re.findall(r"\b[a-z]+_[a-z_]+\b", text))
    # Words that begin like a tool name but are an option or an argument.
    not_tools = {"osd_mclock_profile", "crash_id"}
    prefixes = (
        "cluster", "osd", "pool", "pg", "daemon", "metrics", "history", "capacity", "change",
        "maintenance", "what", "io", "health", "crash", "config", "perf", "ceph",
    )  # fmt: skip
    unknown = {word for word in mentioned - offered - not_tools if word.split("_")[0] in prefixes}
    assert not unknown, unknown
    assert {"cluster_review", "what_changed", "io_top", "osd_outliers", "change_log"} <= mentioned
