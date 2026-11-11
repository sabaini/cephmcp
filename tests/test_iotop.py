"""Who is using the cluster."""

from __future__ import annotations

import json
from typing import Any

import pytest
from conftest import FIXTURES, FakeCli, rows

from cephmcp.tools import iotop

RBD_WRITE = (
    "osd_op(client.20211.0:10452 2.0 2:02a97620:::rbd_data.4e7b98d26791.0000000000000117:head "
    "[write 475136~4096 [fadvise_random] in=4096b] snapc 0=[] "
    "ondisk+write+known_if_redirected+supports_pool_eio e310)"
)
UNDECODED = (
    "osd_op(client.20208.0:17509 2.0 2.6aea6f60 (undecoded) "
    "ondisk+write+known_if_redirected+supports_pool_eio e310)"
)
BENCH_READ = (
    "osd_op(client.19692.0:370 2.15 2:ab70c1e0:::benchmark_data_cephmcp-1_56018_object368:head "
    "[read 0~16384] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e271)"
)


def load(name: str) -> Any:
    return json.loads((FIXTURES / "iotop" / f"{name}.json").read_text())


class TestParsing:
    def test_operations(self) -> None:
        write = iotop.parse_op(RBD_WRITE)
        assert write == iotop.Op("client.20211", 2, True, "rbd image id 4e7b98d26791")
        waiting = iotop.parse_op(UNDECODED)
        assert waiting == iotop.Op("client.20208", 2, True, "")
        read = iotop.parse_op(BENCH_READ)
        assert read == iotop.Op("client.19692", 2, False, "rados bench")
        # Replication between OSDs is not a client's operation.
        assert (
            iotop.parse_op(
                "osd_repop(client.20211.0:10452 2.0 e310/308 2:02a97620:::x:head v 310'5)"
            )
            is None
        )
        assert iotop.parse_op("pg_scrub(pg: 2.1f)") is None and iotop.parse_op("") is None

    def test_objects(self) -> None:
        assert iotop.classify_object("rbd_header.4e7b98d26791") == "rbd image id 4e7b98d26791"
        assert iotop.classify_object("10000000001.00000003") == "cephfs file data"
        marker = "c0ffee00-1234-5678-9abc-def012345678.4567.1"
        assert iotop.classify_object(f"{marker}_photos/cat.jpg") == f"rgw bucket marker {marker}"
        assert iotop.classify_object("notify.6") == ""

    def test_addresses(self) -> None:
        assert iotop.address_of("10.165.218.116:0/2515822689") == "10.165.218.116"
        assert iotop.address_of("v1:10.165.218.116:0/3362389085") == "10.165.218.116"
        assert iotop.address_of("[fd00::5]:0/12") == "fd00::5"
        assert iotop.address_of("") == ""

    def test_tally(self) -> None:
        dumps = [
            {
                "ops": [
                    {
                        "description": RBD_WRITE,
                        "type_data": {"client_info": {"client_addr": "10.0.0.5:0/1"}},
                    },
                    {"description": UNDECODED},
                    {"description": "osd_repop(client.20211.0:1 2.0 e310/308)"},
                ]
            },
            None,  # an OSD that did not answer
            {
                "ops": [
                    {"description": RBD_WRITE.replace("[write", "[read").replace("+write", "+read")}
                ]
            },
        ]
        clients, seen = iotop.tally(dumps, {2: "vms"})
        assert seen == 4 and set(clients) == {"client.20211", "client.20208"}
        heavy = clients["client.20211"]
        assert (heavy.ops, heavy.writes, heavy.address) == (2, 1, "10.0.0.5")
        assert heavy.targets == {"vms: rbd image id 4e7b98d26791": 2}
        assert clients["client.20208"].targets == {"vms": 1}

    def test_rbd_statistics(self) -> None:
        images = iotop.rbd_rows(load("rbd_perf"))
        assert images[0] == {
            "image": "testpool/vm-disk-1",
            "write_ops_s": 3512.4,
            "read_ops_s": 0,
            "write": "55 MiB/s",
            "read": None,
            "write_ms": 76.75,
            "read_ms": None,
        }
        assert images[1]["image"] == "testpool/vm-disk-2"
        assert iotop.rbd_rows(None) == [] and iotop.rbd_rows({"stats": []}) == []

    def test_cephfs_sessions(self) -> None:
        sessions = iotop.cephfs_rows(load("mds_sessions"), "cephmcp-3")
        assert sessions[0]["host"] == "cephmcp-1" and sessions[0]["mount"] == "/"
        assert sessions[0]["user"] == "admin" and sessions[0]["mds"] == "cephmcp-3"


def busy(cli: FakeCli) -> None:
    """The MicroCeph cluster with two RBD images, a CephFS mount and a bench running."""
    cli.use("microceph")
    for name in ("osd_pool_ls_detail", "osd_pool_stats", "fs_status"):
        cli.responses[
            name.replace("osd_pool_ls_detail", "osd pool ls detail")
            .replace("osd_pool_stats", "osd pool stats")
            .replace("fs_status", "fs status")
        ] = f"iotop/{name}.json"
    for osd in range(1, 7):
        cli.responses[f"tell osd.{osd} dump_ops_in_flight"] = f"iotop/inflight_osd{osd}.json"
    cli.responses["rbd perf image stats"] = "iotop/rbd_perf.json"
    cli.responses["tell mds.cephmcp-3 session ls"] = "iotop/mds_sessions.json"
    for mon in ("cephmcp-1", "cephmcp-2", "cephmcp-3"):
        cli.responses[f"tell mon.{mon} sessions"] = "iotop/mon_sessions.json"


@pytest.mark.anyio
class TestTool:
    async def test_a_busy_cluster(self, harness: Any, cli: FakeCli) -> None:
        busy(cli)
        async with harness() as h:
            out = await h.call("io_top", seconds=6)
        pools = rows(out["pools"])
        assert pools[0]["pool"] == "testpool" and pools[0]["share_pct"] == 99
        assert [r["image"] for r in rows(out["rbd_images"])] == [
            "testpool/vm-disk-1",
            "testpool/vm-disk-2",
        ]
        clients = rows(out["clients"])
        assert clients[0]["identity"] == "client.admin" and clients[0]["host"] == "cephmcp-1"
        assert clients[0]["address"] == "10.165.218.116" and clients[0]["writes_pct"] == 100
        assert "testpool: rbd image id " in clients[0]["touches"]
        assert sum(c["share_pct"] for c in clients) == pytest.approx(100, abs=3)
        assert (
            "client operations in flight, seen in 3 looks at 6 of 6 OSDs"
            in out["clients"]["sampled"]
        )
        assert rows(out["cephfs_clients"])[0]["client"] == "client.20472"
        assert out["watched_s"] == 6
        # Everything it ran only reads: status commands, the op trackers, session lists.
        reads = ("rbd perf image stats", "dump_ops_in_flight", " sessions", " session ls")
        assert all(c.endswith(reads) for c in cli.changes), cli.changes

    async def test_limit(self, harness: Any, cli: FakeCli) -> None:
        busy(cli)
        async with harness() as h:
            out = await h.call("io_top", seconds=2, limit=2)
        assert len(out["clients"]["rows"]) == 2 and "more clients" in out["clients"]["more"]
        assert len(out["pools"]["rows"]) == 2

    async def test_an_idle_cluster(self, harness: Any, cli: FakeCli) -> None:
        cli.use("microceph")
        for osd in range(1, 7):
            cli.set_json(f"tell osd.{osd} dump_ops_in_flight", {"ops": [], "num_ops": 0})
        cli.set_json("rbd perf image stats", {"stats": [], "pool_descriptors": {}})
        async with harness() as h:
            out = await h.call("io_top", seconds=2)
        assert "clients" not in out and "rbd_images" not in out and "cephfs_clients" not in out
        assert any("idle or nearly so" in n for n in out["notes"])

    async def test_rbd_statistics_need_a_second_call(self, harness: Any, cli: FakeCli) -> None:
        busy(cli)
        cli.set_json("rbd perf image stats", {"stats": [], "pool_descriptors": {}})
        async with harness() as h:
            out = await h.call("io_top", seconds=2)
        assert "rbd_images" not in out
        assert any("call again in about 20 seconds" in n for n in out["notes"])

    async def test_identity_without_access_to_the_osds(self, harness: Any, cli: FakeCli) -> None:
        busy(cli)
        for osd in range(1, 7):
            cli.fail(f"tell osd.{osd} dump_ops_in_flight", "Error EACCES: access denied", 13)
        async with harness() as h:
            out = await h.call("io_top", seconds=2)
        assert "clients" not in out and "pools" in out and "rbd_images" in out
        assert any("may not query the OSDs" in n for n in out["notes"])

    async def test_gateway_request_rates(
        self, harness: Any, cli: FakeCli, net: Any, clock: Any
    ) -> None:
        busy(cli)
        url = "http://10.165.218.74:9283/metrics"
        base = "\n".join(
            f'{metric}{{instance_id="24919"}} {value}'
            for metric, value in (
                ("ceph_rgw_req", 1000),
                ("ceph_rgw_failed_req", 0),
                ("ceph_rgw_op_put_obj_ops", 400),
                ("ceph_rgw_op_get_obj_ops", 600),
                ("ceph_rgw_op_put_obj_bytes", 0),
                ("ceph_rgw_op_get_obj_bytes", 0),
            )
        )
        started = clock.now
        # Six looks spread over 30 seconds: the last one is 25 seconds after the first.
        later = base.replace(" 1000", " 1500").replace(" 400", " 650").replace(" 600", " 850")
        later = later.replace(
            'put_obj_bytes{instance_id="24919"} 0', 'put_obj_bytes{instance_id="24919"} 52428800'
        )
        net.pages[url] = lambda: (later if clock.now - started >= 25 else base) + chr(10)
        async with harness(mgr_prometheus_url=url, discover_exporters=False) as h:
            out = await h.call("io_top", seconds=30)
        assert rows(out["gateways"]) == [
            {
                "gateway": "24919",
                "requests_s": 20,
                "put_ops_s": 10,
                "get_ops_s": 10,
                "upload": "2 MiB/s",
            }
        ]
