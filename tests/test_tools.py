"""The tools, called through MCP against the fake cluster."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from conftest import FakeCli, Harness, fixture, rows

pytestmark = pytest.mark.anyio

READ_TOOLS = {
    "cluster_status",
    "health_detail",
    "daemon_list",
    "cluster_log",
    "crash_list",
    "osd_list",
    "pool_list",
    "pg_list",
    "config_get",
    "daemon_perf",
    "daemon_ops",
    "metrics_sources",
    "metrics_search",
    "metrics_query",
    "metrics_rate",
    "perf_overview",
    "prometheus_query",
    "ceph_command",
    "cluster_review",
    "runbook",
    "what_changed",
    "history_query",
    "capacity_forecast",
    "osd_outliers",
    "simulate",
    "change_log",
    "io_top",
    "chart",
}
ADMIN_TOOLS = {
    "cluster_flags",
    "osd_set",
    "pool_create",
    "pool_set",
    "config_set",
    "change_undo",
    "maintenance_enter",
    "maintenance_exit",
}
KEY = "AQ" + "B" * 36 + "=="

Make = Callable[..., Harness]


class TestRegistration:
    async def test_read_only_mode_offers_no_way_to_change_anything(self, harness: Make) -> None:
        async with harness() as h:
            tools = await h.tools()
        assert set(tools) == READ_TOOLS
        assert all(t.annotations.read_only_hint for t in tools.values())
        assert "read-only" in tools["ceph_command"].description

    async def test_admin_mode_adds_the_changing_tools(self, harness: Make) -> None:
        async with harness(mode="admin") as h:
            tools = await h.tools()
        assert set(tools) == READ_TOOLS | ADMIN_TOOLS
        changing = {n for n, t in tools.items() if not t.annotations.read_only_hint}
        assert changing == ADMIN_TOOLS | {"ceph_command"}
        assert "pool_delete" not in tools

    async def test_destructive_tools_need_the_extra_switch(self, harness: Make) -> None:
        async with harness(mode="admin", allow_destructive=True) as h:
            tools = await h.tools()
        assert set(tools) == READ_TOOLS | ADMIN_TOOLS | {"pool_delete"}
        assert tools["pool_delete"].annotations.destructive_hint

    async def test_microceph_tools_appear_only_with_microceph(self, harness: Make) -> None:
        async with harness(microceph_cmd=("microceph",)) as h:
            tools = await h.tools()
            assert set(tools) == READ_TOOLS | {"microceph_status", "microceph_command"}
            assert "MicroCeph" in h.server.instructions

    async def test_every_tool_has_a_clean_description(self, harness: Make) -> None:
        async with harness(mode="admin", allow_destructive=True, microceph_cmd=("m",)) as h:
            tools = await h.tools()
        for name, tool in tools.items():
            assert len(tool.description) > 30, name
            assert "\n    " not in tool.description, f"{name}: docstring indentation leaked"


class TestCluster:
    async def test_status(self, harness: Make) -> None:
        async with harness() as h:
            out = await h.call("cluster_status")
        assert out["health"]["status"] == "HEALTH_WARN"
        assert out["health"]["checks"][0]["code"] == "PG_AVAILABILITY"
        assert out["osd"] == {"total": 3, "up": 3, "in": 3}
        assert out["mon"]["quorum"] == ["cephmcp-adm"]
        assert out["pgs"]["by_state"] == {"active+clean": 33, "creating+incomplete": 16}
        assert out["data"]["raw_total"] == "30 GiB"

    async def test_status_of_a_full_cluster(self, harness: Make, cli: FakeCli) -> None:
        cli.responses["status"] = "full_status.json"
        cli.responses["health detail"] = "full_health_detail.json"
        async with harness() as h:
            status = await h.call("cluster_status")
            detail = await h.call("health_detail", max_detail=1)
        assert status["health"]["status"] == "HEALTH_ERR"
        assert status["data"]["raw_used_pct"] > 80
        by_code = {c["code"]: c for c in detail["checks"]}
        assert by_code["OSD_FULL"]["severity"] == "ERR"
        assert by_code["OSD_FULL"]["detail"] == ["osd.0 is full"]
        assert by_code["OSD_FULL"]["detail_omitted"] == 1

    async def test_health_detail_limits_lines_per_check(self, harness: Make) -> None:
        async with harness() as h:
            out = await h.call("health_detail", max_detail=3)
        check = out["checks"][0]
        assert len(check["detail"]) == 3 and check["detail_omitted"] == 13
        assert check["detail"][0].startswith("pg ")

    async def test_daemon_list(self, harness: Make) -> None:
        async with harness() as h:
            out = await h.call("daemon_list")
        assert rows(out["mon"]) == [
            {
                "name": "cephmcp-adm",
                "host": "cephmcp-adm",
                "addr": "10.165.218.74:6789",
                "state": "leader",
            }
        ]
        assert {m["state"] for m in rows(out["mgr"])} == {"active", "standby"}
        assert out["osds_per_host"] == {"cephmcp-adm": 3}
        assert out["versions"]["osd"] == {"20.2.0": 3}
        assert "mixed_versions" not in out

    async def test_gateways_come_from_the_service_map(self, harness: Make, cli: FakeCli) -> None:
        """As seen on MicroCeph with RGW enabled: the map also holds a stub for an OSD."""
        servicemap = {
            "services": {
                "osd": {"daemons": {"summary": "", "7": {"gid": 0, "metadata": {}}}},
                "rgw": {
                    "daemons": {
                        "summary": "",
                        "24919": {
                            "gid": 24919,
                            "metadata": {
                                "id": "radosgw.gateway",
                                "hostname": "node2",
                                "frontend_config#0": "beast port=8080",
                            },
                        },
                    }
                },
            }
        }
        status = json.loads(fixture("status.json"))
        status["servicemap"] = servicemap
        cli.responses["service dump"] = json.dumps(servicemap)
        cli.responses["status"] = json.dumps(status)
        async with harness() as h:
            daemons = await h.call("daemon_list")
            overview = await h.call("cluster_status")
        assert daemons["other_services"] == {
            "rgw": [{"name": "radosgw.gateway", "host": "node2", "frontend": "beast port=8080"}]
        }
        assert overview["services"] == {"rgw": 1}

    async def test_cluster_log_returns_the_most_recent_entries(self, harness: Make) -> None:
        async with harness() as h:
            out = await h.call("cluster_log", lines=3)
        entries = rows(out)
        assert len(entries) == 3
        assert entries[-1]["time"] > entries[0]["time"] or entries[-1]["time"] == entries[0]["time"]
        assert {e["level"] for e in entries} <= {"INF", "WRN", "ERR"}

    async def test_crashes(self, harness: Make, cli: FakeCli) -> None:
        crash = {"crash_id": "2026-11-11T10:00:00.1Z_abc", "timestamp": "2026-11-11T10:00:00.1Z"}
        cli.responses["crash ls"] = json.dumps([{**crash, "entity_name": "osd.1"}])
        cli.responses["crash ls-new"] = json.dumps([crash])
        cli.responses[f"crash info {crash['crash_id']}"] = json.dumps(
            {**crash, "entity_name": "osd.1", "backtrace": ["a", "b"], "assert_msg": "boom"}
        )
        async with harness() as h:
            listed = rows(await h.call("crash_list"))
            info = await h.call("crash_list", crash_id=crash["crash_id"])
            health = await h.call("health_detail")
        assert listed == [
            {
                "crash_id": crash["crash_id"],
                "time": "2026-11-11 10:00:00",
                "daemon": "osd.1",
                "new": True,
            }
        ]
        assert info["backtrace"] == ["a", "b"] and info["assert_msg"] == "boom"
        assert health["new_crashes"] == 1


class TestFailures:
    """A failing command must come back as a readable tool error, also when
    it was one of several commands run concurrently."""

    async def test_cluster_unreachable(self, harness: Make, cli: FakeCli) -> None:
        down = "[errno 110] RADOS timed out (error connecting to the cluster)"
        for command in (
            "status",
            "health detail",
            "osd df tree",
            "osd pool ls detail",
            "quorum_status",
        ):
            cli.fail(command, down, 1)
        async with harness() as h:
            for tool in ("cluster_status", "health_detail", "osd_list", "pool_list", "daemon_list"):
                message = await h.error(tool)
                assert "error connecting to the cluster" in message, tool
                assert message.startswith(f"Error executing tool {tool}: `ceph "), message

    async def test_daemon_query_without_permission(self, harness: Make, cli: FakeCli) -> None:
        denied = "Error EACCES: problem getting command descriptions from osd.0"
        cli.fail("tell osd.0 perf dump", denied, 13)
        cli.fail("tell osd.0 dump_ops_in_flight", denied, 13)
        async with harness() as h:
            for tool in ("daemon_perf", "daemon_ops"):
                message = await h.error(tool, daemon="osd.0")
                assert "EACCES" in message and "metrics_rate" in message, tool

    async def test_optional_information_may_be_missing(self, harness: Make, cli: FakeCli) -> None:
        for command in ("osd perf", "osd pool stats", "osd crush rule dump", "crash ls-new"):
            cli.fail(command, "Error EACCES: access denied", 13)
        async with harness() as h:
            osds = rows(await h.call("osd_list"))
            pools = rows(await h.call("pool_list"))
            await h.call("health_detail")
        assert len(osds) == 3 and "commit_ms" not in osds[0]
        assert {p["crush_rule"] for p in pools} == {0, 1}  # ids when the names are unavailable


class TestStorage:
    async def test_osd_list(self, harness: Make) -> None:
        async with harness() as h:
            out = await h.call("osd_list")
        osds = rows(out)
        assert [o["id"] for o in osds] == [0, 1, 2]
        assert {o["host"] for o in osds} == {"cephmcp-adm"}
        assert all(o["up"] and o["in"] and o["class"] == "hdd" for o in osds)
        assert all(o["commit_ms"] is not None for o in osds)
        assert out["summary"]["osds"] == 3 and out["summary"]["raw_total"] == "30 GiB"

    async def test_osd_list_filters_and_sorts(self, harness: Make, cli: FakeCli) -> None:
        cli.responses["osd df tree"] = "full_osd_df_tree.json"
        async with harness() as h:
            fullest = rows(await h.call("osd_list", sort_by="used_pct", limit=1))
            down = await h.call("osd_list", state="down")
            elsewhere = await h.call("osd_list", host="no-such-host")
        assert fullest[0]["used_pct"] > 95
        assert down["count"] == 0 and elsewhere["count"] == 0
        assert down["summary"]["up"] == 3  # the summary covers the cluster, not the filter

    async def test_pool_list(self, harness: Make) -> None:
        async with harness() as h:
            pools = {p["name"]: p for p in rows(await h.call("pool_list"))}
        assert set(pools) == {".mgr", "testpool", "ecpool"}
        assert pools["testpool"]["redundancy"] == "2x"
        assert pools["testpool"]["crush_rule"] == "replicated_rule"
        assert pools["testpool"]["application"] == "rbd"
        assert pools["ecpool"]["redundancy"] == "ec:default size 4"
        assert pools["ecpool"]["min_size"] == 3
        assert pools[".mgr"]["stored"].endswith("KiB")

    async def test_pool_list_one_pool(self, harness: Make) -> None:
        async with harness() as h:
            assert (await h.call("pool_list", pool="ecpool"))["count"] == 1
            assert "no pool named" in await h.error("pool_list", pool="nope")

    async def test_pg_summary_then_detail(self, harness: Make) -> None:
        async with harness() as h:
            summary = await h.call("pg_list")
            incomplete = rows(await h.call("pg_list", states=["incomplete"]))
        assert summary["total"] == 49
        assert summary["by_state"]["creating+incomplete"] == 16
        assert len(incomplete) == 3
        # CRUSH's "no OSD" placeholder (2147483647) is shown as null
        assert incomplete[0]["acting"][1:] == [None, None, None]
        assert isinstance(incomplete[0]["acting"][0], int)

    async def test_pg_list_by_osd_pool_and_stuck(self, harness: Make, cli: FakeCli) -> None:
        stuck = json.loads(fixture("pg_ls_incomplete.json"))["pg_stats"]
        cli.responses["pg dump_stuck inactive"] = json.dumps({"stuck_pg_stats": stuck})
        async with harness() as h:
            by_osd = await h.call("pg_list", osd=0)
            by_pool = await h.call("pg_list", pool="testpool", limit=2)
            stuck_out = await h.call("pg_list", stuck="inactive")
        assert by_osd["count"] == 4
        assert len(by_pool["rows"]) == 2 and "truncated" in by_pool
        assert stuck_out["count"] == 3
        assert "pg ls-by-osd osd.0" in cli.calls and "pg ls-by-pool testpool" in cli.calls

    @pytest.mark.parametrize(
        ("tool", "arguments"),
        [
            ("pool_list", {"pool": "--yes-i-really-mean-it"}),
            ("pg_list", {"pool": "a b"}),
            ("pg_list", {"states": ["clean; rm -rf /"]}),
            ("daemon_perf", {"daemon": "osd.0 --debug"}),
            ("daemon_ops", {"daemon": "-h"}),
            ("config_get", {"who": "osd", "option": "--force"}),
        ],
    )
    async def test_arguments_that_are_not_names_never_reach_the_cli(
        self, harness: Make, cli: FakeCli, tool: str, arguments: dict[str, Any]
    ) -> None:
        async with harness() as h:
            await h.error(tool, **arguments)
        assert cli.calls == []


class TestDaemons:
    async def test_config_get_modes(self, harness: Make) -> None:
        async with harness() as h:
            dump = rows(await h.call("config_get"))
            doc = await h.call("config_get", option="osd_max_backfills")
            value = await h.call("config_get", who="osd", option="osd_max_backfills")
            applied = rows(await h.call("config_get", who="osd"))
            running = rows(await h.call("config_get", who="osd.0", running=True))
            needs_daemon = await h.error("config_get", who="osd", running=True)
        assert {"who": "global", "option": "osd_pool_default_size", "value": "2"} in dump
        assert doc["type"] == "uint" and doc["default"] == 1 and doc["can_update_at_runtime"]
        assert value["value"] == 1 and value["desc"].startswith("Maximum number")
        assert any(o["option"] == "osd_memory_target_autotune" for o in applied)
        assert {"option": "daemonize", "value": "false", "source": "override"} in running
        assert "one daemon" in needs_daemon

    async def test_daemon_perf_defaults_to_the_interesting_counters(self, harness: Make) -> None:
        async with harness() as h:
            out = await h.call("daemon_perf", daemon="osd.0")
        counters = {r["counter"]: r for r in rows(out["counters"])}
        averages = {r["counter"]: r for r in rows(out["averages"])}
        assert counters["osd.op"]["value"] > 0
        assert averages["osd.op_latency"]["avg_ms"] > 0
        assert "osd.op_r" not in counters  # lower priority: only shown when asked for

    async def test_daemon_perf_over_an_interval(self, harness: Make, cli: FakeCli) -> None:
        first = json.loads(fixture("tell_osd0_perf_dump.json"))
        second = json.loads(fixture("tell_osd0_perf_dump.json"))
        second["osd"]["op_w"] += 500
        second["osd"]["op_w_latency"]["avgcount"] += 500
        second["osd"]["op_w_latency"]["sum"] += 2.0  # 500 ops in 2 s of latency: 4 ms each
        dumps = iter([json.dumps(first), json.dumps(second)])
        cli.responses["tell osd.0 perf dump"] = lambda: next(dumps)
        async with harness() as h:
            out = await h.call(
                "daemon_perf", daemon="osd.0", match=r"^osd\.op_w(_latency)?$", seconds=5
            )
        assert rows(out["counters"])[0]["per_sec"] > 0
        latency = rows(out["averages"])[0]
        assert latency["counter"] == "osd.op_w_latency"
        assert latency["interval_avg_ms"] == 4
        assert latency["avg_ms"] != 4  # the lifetime average is a different number

    async def test_daemon_perf_bad_pattern(self, harness: Make) -> None:
        async with harness() as h:
            assert "invalid regular expression" in await h.error(
                "daemon_perf", daemon="osd.0", match="("
            )

    async def test_daemon_ops(self, harness: Make) -> None:
        async with harness() as h:
            out = await h.call("daemon_ops", daemon="osd.0", kind="historic")
        ops = rows(out)
        assert len(ops) == 2 and ops[0]["duration_s"] >= ops[1]["duration_s"]
        assert ops[0]["description"].startswith("osd_")
        assert "ms" in ops[0]["slowest_steps"]


class TestCephCommand:
    async def test_plain_and_json(self, harness: Make, cli: FakeCli) -> None:
        cli.responses["osd tree"] = "ID  CLASS  WEIGHT\n-1  0.03  root default\n"
        async with harness() as h:
            plain = await h.call("ceph_command", args=["osd", "tree"])
            assert cli.formats["osd tree"] is None
            compact = await h.raw("ceph_command", args=["pg", "stat"], format="json")
        assert plain.startswith("ID  CLASS")
        assert cli.formats["pg stat"] == "json"
        assert compact[1].startswith('{"pg_ready":true,')  # re-encoded without whitespace

    async def test_read_only_mode_refuses_changes(self, harness: Make, cli: FakeCli) -> None:
        async with harness() as h:
            message = await h.error("ceph_command", args=["osd", "set", "noout"])
            await h.error("ceph_command", args=["osd", "pool", "ls"], input="x")
        assert "read-only" in message and "--mode admin" in message
        assert cli.changes == []

    async def test_keys_never_come_back(self, harness: Make, cli: FakeCli) -> None:
        cli.responses["auth get client.admin"] = f"[client.admin]\n\tkey = {KEY}\n"
        cli.fail("osd pool get x size", f"auth: bad key {KEY}")
        async with harness() as h:
            out = await h.call("ceph_command", args=["auth", "get", "client.admin"])
            err = await h.error("ceph_command", args=["osd", "pool", "get", "x", "size"])
        assert KEY not in out and "<redacted>" in out
        assert KEY not in err and "<redacted>" in err

    async def test_secret_options_are_masked_in_every_format(
        self, harness: Make, cli: FakeCli
    ) -> None:
        dump = [
            {"section": "client.rgw", "name": "rgw_keystone_admin_password", "value": "hunter2"},
            {"section": "osd", "name": "osd_max_backfills", "value": "3"},
        ]
        cli.responses["config dump"] = lambda: (
            json.dumps(dump)
            if cli.formats.get("config dump") == "json"
            else "WHO  OPTION  VALUE\nclient.rgw  rgw_keystone_admin_password  hunter2\n"
        )
        async with harness() as h:
            plain = await h.call("ceph_command", args=["config", "dump"])
            as_json = await h.raw("ceph_command", args=["config", "dump"], format="json")
            typed = await h.raw("config_get")
        for text in (plain, as_json[1], typed[1]):
            assert "hunter2" not in text and "<redacted>" in text
        assert '"osd_max_backfills","value":"3"' in as_json[1]

    async def test_secrets_on_request(self, harness: Make, cli: FakeCli) -> None:
        cli.responses["auth get-key client.admin"] = KEY
        async with harness() as h:
            assert "--allow-secrets" in await h.error(
                "ceph_command", args=["auth", "get-key", "client.admin"]
            )
        async with harness(allow_secrets=True) as h:
            assert await h.call("ceph_command", args=["auth", "get-key", "client.admin"]) == KEY

    async def test_failures_are_reported_with_cephs_message(
        self, harness: Make, cli: FakeCli
    ) -> None:
        cli.fail("osd pool get nope size", "Error ENOENT: unrecognized pool 'nope'")
        async with harness() as h:
            message = await h.error("ceph_command", args=["osd", "pool", "get", "nope", "size"])
        assert "unrecognized pool 'nope'" in message
        assert "--connect-timeout" not in message  # internal options are not the caller's business

    async def test_long_output_is_cut_with_a_note(self, harness: Make, cli: FakeCli) -> None:
        cli.responses["osd dump"] = "x" * 50_000
        async with harness(max_output_chars=2000) as h:
            out = await h.call("ceph_command", args=["osd", "dump"])
        assert len(out) < 2200 and "output truncated: 2000 of 50000" in out

    @pytest.mark.parametrize("args", [[], ["ceph", "status"], ["osd", ""], ["a\nb"]])
    async def test_malformed_arguments(self, harness: Make, args: list[str]) -> None:
        async with harness() as h:
            await h.error("ceph_command", args=args)

    async def test_stdin_input_in_admin_mode(self, harness: Make, cli: FakeCli) -> None:
        # A JSON document must arrive as text; the SDK would decode it into a
        # dict for any parameter not declared as a plain string.
        document = '{"export_id": 1, "path": "/"}\n'
        cli.responses["nfs export apply mynfs -i -"] = "ok"
        async with harness(mode="admin") as h:
            await h.call("ceph_command", args=["nfs", "export", "apply", "mynfs"], input=document)
        assert cli.inputs["nfs export apply mynfs -i -"] == document.encode()


class TestAdmin:
    async def test_cluster_flags(self, harness: Make, cli: FakeCli) -> None:
        cli.responses.update({"osd set noout": "", "osd unset-group noout host1 osd.3": ""})
        async with harness(mode="admin") as h:
            done = await h.call("cluster_flags", action="set", flag="noout")
            await h.call("cluster_flags", action="unset", flag="noout", targets=["host1", "osd.3"])
            await h.error("cluster_flags", action="set", flag="noscrub", targets=["host1"])
            await h.error("cluster_flags", action="set", flag="pause")
        assert done["ran"] == "ceph osd set noout"
        assert cli.changes == ["osd set noout", "osd unset-group noout host1 osd.3"]

    async def test_osd_set(self, harness: Make, cli: FakeCli) -> None:
        cli.responses.update(
            {
                "osd out 2": "marked out osd.2.",
                "osd reweight 2 0.8": "",
                "osd crush reweight osd.2 1.5": "",
            }
        )
        async with harness(mode="admin") as h:
            done = await h.call("osd_set", osd=2, action="out")
            await h.call("osd_set", osd=2, action="reweight", value=0.8)
            await h.call("osd_set", osd=2, action="crush-reweight", value=1.5)
            await h.error("osd_set", osd=2, action="reweight")  # no value
            await h.error("osd_set", osd=2, action="primary-affinity", value=2)
        assert done["output"] == "marked out osd.2." and done["osd"]["id"] == 2
        assert cli.changes == ["osd out 2", "osd reweight 2 0.8", "osd crush reweight osd.2 1.5"]

    async def test_pool_create(self, harness: Make, cli: FakeCli) -> None:
        create = "osd pool create vms --pool_type=replicated --pg_num=64 --size=3 --rule=ssd --bulk"
        cli.responses.update(
            {
                create: "pool 'vms' created",
                "osd pool application enable vms rbd": "",
                "osd pool create cold --pool_type=erasure --erasure_code_profile=k4m2": "",
                "osd pool application enable cold rgw": "",
            }
        )
        async with harness(mode="admin") as h:
            await h.call(
                "pool_create",
                name="vms",
                application="rbd",
                size=3,
                crush_rule="ssd",
                pg_num=64,
                bulk=True,
            )
            await h.call(
                "pool_create",
                name="cold",
                application="rgw",
                pool_type="erasure",
                erasure_code_profile="k4m2",
            )
            no_redundancy = await h.error("pool_create", name="tmp", application="rbd", size=1)
            await h.error("pool_create", name="x", application="rbd", pool_type="erasure", size=3)
        assert "--allow-destructive" in no_redundancy
        assert cli.changes == [
            create,
            "osd pool application enable vms rbd",
            "osd pool create cold --pool_type=erasure --erasure_code_profile=k4m2",
            "osd pool application enable cold rgw",
        ]

    async def test_pool_set(self, harness: Make, cli: FakeCli) -> None:
        cli.responses.update(
            {
                "osd pool set rbd pg_autoscale_mode --val=off": "",
                "osd pool set-quota rbd max_bytes 1073741824": "",
                "osd pool application enable rbd rbd": "",
                "osd pool set rbd recovery_priority --val=-1": "",
            }
        )
        async with harness(mode="admin") as h:
            await h.call("pool_set", pool="rbd", option="pg_autoscale_mode", value="off")
            await h.call("pool_set", pool="rbd", option="quota_max_bytes", value="1073741824")
            await h.call("pool_set", pool="rbd", option="application", value="rbd")
            await h.call("pool_set", pool="rbd", option="recovery_priority", value="-1")
            assert "--allow-destructive" in await h.error(
                "pool_set", pool="rbd", option="size", value="1"
            )
            await h.error("pool_set", pool="rbd", option="size", value="--yes-i-really-mean-it")
        assert len(cli.changes) == 4

    async def test_config_set_reports_before_and_after(self, harness: Make, cli: FakeCli) -> None:
        values = iter(["0.0", "0.2", "0.2", "0.0"])
        cli.responses["config get osd osd_scrub_sleep"] = lambda: next(values)
        cli.responses["config set osd/class:hdd osd_scrub_sleep --value=0.2"] = ""
        cli.responses["config rm osd/class:hdd osd_scrub_sleep"] = ""
        async with harness(mode="admin") as h:
            done = await h.call(
                "config_set", who="osd/class:hdd", option="osd_scrub_sleep", value="0.2"
            )
            undone = await h.call(
                "config_set", who="osd/class:hdd", option="osd_scrub_sleep", remove=True
            )
            await h.error("config_set", who="osd", option="osd_scrub_sleep")  # neither
            await h.error("config_set", who="osd", option="osd_scrub_sleep", value="1", remove=True)
        assert (done["previous"], done["now"]) == (0.0, 0.2)
        assert (undone["previous"], undone["now"]) == (0.2, 0.0)
        assert cli.changes == [
            "config set osd/class:hdd osd_scrub_sleep --value=0.2",
            "config rm osd/class:hdd osd_scrub_sleep",
        ]

    async def test_pool_delete(self, harness: Make, cli: FakeCli) -> None:
        delete = "osd pool delete tmp tmp --yes-i-really-really-mean-it"
        cli.fail(
            delete,
            "Error EPERM: pool deletion is disabled; you must first set the "
            "mon_allow_pool_delete config option to true",
            1,
        )
        async with harness(mode="admin", allow_destructive=True) as h:
            assert "confirm must repeat" in await h.error("pool_delete", pool="tmp", confirm="yes")
            assert cli.changes == []
            hint = await h.error("pool_delete", pool="tmp", confirm="tmp")
            del cli.failures[delete]
            cli.responses[delete] = "pool 'tmp' removed"
            done = await h.call("pool_delete", pool="tmp", confirm="tmp")
        assert "config_set" in hint and "mon_allow_pool_delete" in hint
        assert done["output"] == "pool 'tmp' removed"

    async def test_ceph_command_levels(self, harness: Make, cli: FakeCli) -> None:
        cli.responses["osd set noout"] = "noout is set"
        cli.responses["osd pool rm tmp tmp --yes-i-really-really-mean-it"] = "pool 'tmp' removed"
        async with harness(mode="admin") as h:
            assert await h.call("ceph_command", args=["osd", "set", "noout"]) == "noout is set"
            refused = await h.error(
                "ceph_command",
                args=["osd", "pool", "rm", "tmp", "tmp", "--yes-i-really-really-mean-it"],
            )
        assert "--allow-destructive" in refused
        async with harness(mode="admin", allow_destructive=True) as h:
            await h.call(
                "ceph_command",
                args=["osd", "pool", "rm", "tmp", "tmp", "--yes-i-really-really-mean-it"],
            )

    @pytest.mark.parametrize(
        "option",
        [
            "-o/tmp/out",  # the value attached to a short option
            "--out-f=/tmp/out",  # an abbreviation of --out-file
            "--log-file=/tmp/log",  # a Ceph configuration option, taken by the client
            "--log_to_file=true",
            "--daemon-output-file=/tmp/out",
        ],
    )
    async def test_no_spelling_of_a_client_option_reaches_the_command_line(
        self, harness: Make, cli: FakeCli, option: str
    ) -> None:
        for settings in ({}, {"mode": "admin", "allow_destructive": True}):
            async with harness(**settings) as h:
                await h.error("ceph_command", args=["status", option])
        assert not [c for c in cli.calls if c.startswith("status ")]


STATUS = """MicroCeph deployment summary:
- node1 (10.0.0.1)
  Services: mds, mgr, mon, osd
  Disks: 2
- node2 (10.0.0.2)
  Services: mon, rgw
  Disks: 0
"""
DISKS = {
    "ConfiguredDisks": [{"osd": 1, "location": "node1", "path": "/dev/sdb"}],
    "AvailableDisks": [{"Model": "QEMU", "Size": "10.00GiB", "Type": "scsi", "Path": "/dev/sdc"}],
}


class TestMicroceph:
    async def test_status(self, harness: Make, cli: FakeCli) -> None:
        cli.microceph = {
            "status": STATUS,
            "cluster list --format=json": json.dumps(
                [{"name": "node1", "role": "voter", "status": "ONLINE"}]
            ),
            "disk list --json": json.dumps(DISKS),
        }
        async with harness(microceph_cmd=("microceph",)) as h:
            out = await h.call("microceph_status")
        assert out["nodes"][0] == {
            "node": "node1",
            "address": "10.0.0.1",
            "services": ["mds", "mgr", "mon", "osd"],
            "disks": 2,
            "role": "voter",
            "status": "ONLINE",
        }
        assert out["nodes"][1]["services"] == ["mon", "rgw"] and out["nodes"][1]["disks"] == 0
        assert out["osd_disks"] == [{"osd": 1, "node": "node1", "path": "/dev/sdb"}]
        assert out["available_disks"][0]["path"] == "/dev/sdc"

    async def test_status_survives_missing_json_support(self, harness: Make, cli: FakeCli) -> None:
        cli.microceph = {"status": STATUS}  # older MicroCeph: the JSON listings fail
        async with harness(microceph_cmd=("microceph",)) as h:
            out = await h.call("microceph_status")
        assert [n["node"] for n in out["nodes"]] == ["node1", "node2"]

    async def test_command_levels(self, harness: Make, cli: FakeCli) -> None:
        cli.microceph = {
            "disk list": "Disks configured in MicroCeph:",
            "disk add loop,4G,1": "",
            "disk remove 3": "",
        }
        async with harness(microceph_cmd=("microceph",)) as h:
            assert "Disks configured" in await h.call("microceph_command", args=["disk", "list"])
            assert "read-only" in await h.error(
                "microceph_command", args=["disk", "add", "loop,4G,1"]
            )
        async with harness(microceph_cmd=("microceph",), mode="admin") as h:
            await h.call("microceph_command", args=["disk", "add", "loop,4G,1"])
            assert "--allow-destructive" in await h.error(
                "microceph_command", args=["disk", "remove", "3"]
            )
            await h.error("microceph_command", args=["cluster", "sql", "select 1"])
        async with harness(microceph_cmd=("microceph",), mode="admin", allow_destructive=True) as h:
            await h.call("microceph_command", args=["disk", "remove", "3"])
            await h.error("microceph_command", args=["cluster", "sql", "select 1"])  # never
        assert [c for c in cli.calls if c.startswith("microceph")] == [
            "microceph disk list",
            "microceph disk add loop,4G,1",
            "microceph disk remove 3",
        ]

    @pytest.mark.parametrize(
        "flags",
        [
            ["--dry-run=0"],
            ["--dry-run=False"],
            ["--dry-run", "--dry-run=false"],
            ["--check-only=f"],
        ],
    )
    async def test_a_dry_run_switched_off_is_a_change(
        self, harness: Make, cli: FakeCli, flags: list[str]
    ) -> None:
        """MicroCeph reads `--dry-run=0` as "not a dry run": the node would
        really go into maintenance and its OSDs would really stop."""
        command = ["cluster", "maintenance", "enter", "node2", "--stop-osds", *flags]
        cli.microceph[" ".join(command)] = "operation executed"
        async with harness(microceph_cmd=("microceph",)) as h:
            await h.error("microceph_command", args=command)
            assert (await h.call("change_log"))["count"] == 0
        assert not [c for c in cli.calls if c.startswith("microceph cluster maintenance")]

    async def test_a_real_dry_run_is_allowed_and_a_change_is_journaled(
        self, harness: Make, cli: FakeCli
    ) -> None:
        dry = ["cluster", "maintenance", "enter", "node2", "--dry-run=true"]
        real = ["cluster", "maintenance", "enter", "node2", "--dry-run=0"]
        cli.microceph = {" ".join(dry): "would do this", " ".join(real): "done"}
        async with harness(microceph_cmd=("microceph",)) as h:
            assert await h.call("microceph_command", args=dry) == "would do this"
        async with harness(mode="admin", microceph_cmd=("microceph",)) as h:
            assert await h.call("microceph_command", args=real) == "done"
            log = rows(await h.call("change_log"))
            assert log[0]["tool"] == "microceph_command" and "--dry-run=0" in log[0]["summary"]
