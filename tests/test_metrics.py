"""Metric discovery, queries, rates and PromQL."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import parse_qs

import pytest
from conftest import (
    EXPORTER_URL,
    MGR_URL,
    PROMETHEUS_URL,
    FakeCli,
    FakeEndpoints,
    Harness,
    fixture,
    rows,
)

from cephmcp.metrics import EXPORTER, MGR, Scrape, Source, merge
from cephmcp.promtext import parse
from cephmcp.tools.perf import parse_time

pytestmark = pytest.mark.anyio

Make = Callable[..., Harness]
NO_ORCHESTRATOR = "Error ENOENT: No orchestrator configured (try `ceph orch set backend`)"


def without_orchestrator(cli: FakeCli) -> None:
    """Behave like MicroCeph: there is no `ceph orch`."""
    for command in (
        "orch ps --daemon_type ceph-exporter",
        "orch ps --daemon_type prometheus",
        "orch host ls",
    ):
        cli.fail(command, NO_ORCHESTRATOR)


def exporter_page(ops: int, latency_sum: float) -> str:
    """Two OSDs; osd.1 does twice the writes at twice the latency."""
    return (
        "# TYPE ceph_osd_op_w counter\n"
        f'ceph_osd_op_w{{ceph_daemon="osd.0"}} {ops}\n'
        f'ceph_osd_op_w{{ceph_daemon="osd.1"}} {2 * ops}\n'
        "# TYPE ceph_osd_op_w_latency_sum gauge\n"
        f'ceph_osd_op_w_latency_sum{{ceph_daemon="osd.0"}} {latency_sum}\n'
        f'ceph_osd_op_w_latency_sum{{ceph_daemon="osd.1"}} {4 * latency_sum}\n'
        "# TYPE ceph_osd_op_w_latency_count counter\n"
        f'ceph_osd_op_w_latency_count{{ceph_daemon="osd.0"}} {ops}\n'
        f'ceph_osd_op_w_latency_count{{ceph_daemon="osd.1"}} {2 * ops}\n'
    )


def changing(net: FakeEndpoints, url: str, pages: list[str]) -> None:
    """Serve the pages in turn, then keep serving the last one."""
    remaining: Iterator[str] = iter(pages)
    last = [pages[0]]

    def page() -> str:
        last[0] = next(remaining, last[0])
        return last[0]

    net.pages[url] = page


class TestDiscovery:
    async def test_with_an_orchestrator(self, harness: Make, cli: FakeCli) -> None:
        async with harness() as h:
            out = await h.call("metrics_sources")
        sources = {s["kind"]: s for s in rows(out["sources"])}
        assert sources[MGR]["url"] == MGR_URL and sources[MGR]["found_by"] == "ceph mgr services"
        # the orchestrator knows the daemon by host name; the address comes from `orch host ls`
        assert sources[EXPORTER]["url"] == EXPORTER_URL
        assert sources[EXPORTER]["host"] == "cephmcp-adm"
        assert all(s["ok"] and s["series"] > 0 for s in sources.values())
        assert out["prometheus_server"] == PROMETHEUS_URL
        assert out["mgr_scrape_interval_s"] == 15
        assert "notes" not in out
        assert "osd metadata" not in cli.calls  # no need to probe

    async def test_without_orchestrator_hosts_are_probed(self, harness: Make, cli: FakeCli) -> None:
        without_orchestrator(cli)
        async with harness() as h:
            out = await h.call("metrics_sources")
        sources = {s["kind"]: s for s in rows(out["sources"])}
        assert sources[EXPORTER]["found_by"] == "probe"
        assert sources[EXPORTER]["url"] == EXPORTER_URL
        assert "prometheus_server" not in out
        assert any("No Prometheus server" in n for n in out["notes"])

    async def test_microceph_default_explains_missing_perf_counters(
        self, harness: Make, cli: FakeCli, net: FakeEndpoints
    ) -> None:
        """MicroCeph has no ceph-exporter, and the mgr module leaves the
        per-daemon counters out unless told otherwise."""
        without_orchestrator(cli)
        del net.pages[EXPORTER_URL]
        async with harness() as h:
            out = await h.call("metrics_sources")
        assert [s["kind"] for s in rows(out["sources"])] == [MGR]
        note = next(n for n in out["notes"] if "exclude_perf_counters" in n)
        assert "ceph config set mgr mgr/prometheus/exclude_perf_counters false" in note
        assert "daemon_perf" in note

    async def test_disabled_prometheus_module(
        self, harness: Make, cli: FakeCli, net: FakeEndpoints
    ) -> None:
        without_orchestrator(cli)
        del net.pages[EXPORTER_URL]
        cli.responses["mgr services"] = "{}"
        cli.responses["mgr module ls"] = json.dumps({"enabled_modules": ["iostat"]})
        async with harness() as h:
            out = await h.call("metrics_sources")
            failure = await h.error("metrics_query", metric="ceph_health_status")
        assert "sources" not in out
        assert any("ceph mgr module enable prometheus" in n for n in out["notes"])
        assert "No metrics available" in failure and "mgr module enable prometheus" in failure

    async def test_discovery_is_cached_until_refreshed(self, harness: Make, cli: FakeCli) -> None:
        async with harness() as h:
            await h.call("metrics_sources")
            first = cli.calls.count("mgr services")
            await h.call("metrics_sources")
            assert cli.calls.count("mgr services") == first
            await h.call("metrics_sources", refresh=True)
            assert cli.calls.count("mgr services") == first + 1

    async def test_configured_endpoints_skip_discovery(self, harness: Make, cli: FakeCli) -> None:
        async with harness(
            mgr_prometheus_url="http://10.165.218.74:9283",
            ceph_exporter_urls=("10.165.218.74:9926",),
            prometheus_url=PROMETHEUS_URL + "/",
            discover_exporters=False,
        ) as h:
            out = await h.call("metrics_sources")
        assert {s["found_by"] for s in rows(out["sources"])} == {"configured"}
        assert {s["url"] for s in rows(out["sources"])} == {MGR_URL, EXPORTER_URL}
        assert out["prometheus_server"] == PROMETHEUS_URL
        assert not any(c.startswith(("mgr services", "orch")) for c in cli.calls)

    async def test_an_unreachable_endpoint_is_reported_not_fatal(
        self, harness: Make, net: FakeEndpoints
    ) -> None:
        del net.pages[EXPORTER_URL]
        async with harness() as h:
            out = await h.call("metrics_sources")
            health = await h.call("metrics_query", metric="ceph_health_status")
        sources = {s["kind"]: s for s in rows(out["sources"])}
        assert sources[EXPORTER]["ok"] is False and "cannot reach" in sources[EXPORTER]["error"]
        assert rows(health) == [{"metric": "ceph_health_status", "value": 1}]


def test_merge_takes_perf_counters_from_the_exporter_only() -> None:
    """With exclude_perf_counters off, the mgr exports them too; they must not count twice."""
    mgr = Scrape(
        Source(MGR, "m"),
        0,
        parse('ceph_osd_op_w{ceph_daemon="osd.0"} 5\nceph_osd_up{ceph_daemon="osd.0"} 1\n'),
    )
    exporter = Scrape(
        Source(EXPORTER, "e", "node1"), 0, parse('ceph_osd_op_w{ceph_daemon="osd.0"} 7\n')
    )
    dead = Scrape(Source(EXPORTER, "x", "node2"), 0, error="down")
    merged = merge([mgr, exporter, dead])
    assert [(s.labels, s.value) for s in merged["ceph_osd_op_w"].samples] == [
        ((("ceph_daemon", "osd.0"), ("instance", "node1")), 7.0)
    ]
    assert len(merged["ceph_osd_up"].samples) == 1


class TestQuery:
    async def test_search(self, harness: Make) -> None:
        async with harness() as h:
            overview = await h.call("metrics_search")
            found = rows(await h.call("metrics_search", pattern="OSD_OP_W_LAT"))
            by_help = rows(await h.call("metrics_search", pattern="blocklist"))
            await h.error("metrics_search", pattern="(")
        assert overview["families"]["ceph_pg"] > 20 and "hint" in overview
        assert [m["name"] for m in found] == [
            "ceph_osd_op_w_latency_count",
            "ceph_osd_op_w_latency_sum",
        ]
        assert found[0]["labels"] == "ceph_daemon,instance" and found[0]["series"] == 3
        assert by_help and "blocklist" in by_help[0]["name"]

    async def test_values_get_pool_and_host_names(self, harness: Make) -> None:
        async with harness() as h:
            pools = await h.call("metrics_query", metric="ceph_pool_stored")
            latency = await h.call("metrics_query", metric="ceph_osd_commit_latency_ms")
        assert {r["pool"] for r in rows(pools)} == {".mgr", "testpool", "ecpool"}
        assert pools["common"] == {"metric": "ceph_pool_stored"}
        # labels shared by every row are stated once
        assert latency["common"]["hostname"] == "cephmcp-adm"
        assert latency["common"]["device_class"] == "hdd"
        assert latency["columns"] == ["ceph_daemon", "value"]

    async def test_filter_aggregate_and_top(self, harness: Make) -> None:
        async with harness() as h:
            one = await h.call(
                "metrics_query", metric="ceph_pool_.*", labels={"pool": "test.*"}, limit=500
            )
            by_host = await h.call(
                "metrics_query", metric="ceph_osd_up", aggregate="sum", group_by=["hostname"]
            )
            top = await h.call(
                "metrics_query", metric="ceph_pg_.*", aggregate="sum", group_by=[], top=2
            )
        assert one["common"]["pool"] == "testpool" and one["count"] > 10
        assert rows(by_host) == [{"metric": "ceph_osd_up", "hostname": "cephmcp-adm", "value": 3}]
        assert [r["metric"] for r in rows(top)] == ["ceph_pg_total", "ceph_pg_active"]

    async def test_mistakes_get_useful_errors(self, harness: Make) -> None:
        async with harness() as h:
            assert "metrics_search" in await h.error("metrics_query", metric="ceph_nope")
            assert "invalid regular expression" in await h.error("metrics_query", metric="ceph_(")
            assert "invalid" in await h.error(
                "metrics_query", metric="ceph_osd_up", labels={"x": "("}
            )
            assert "aggregate" in await h.error(
                "metrics_query", metric="ceph_osd_up", group_by=["hostname"]
            )


class TestRate:
    async def test_counter_rates(self, harness: Make, net: FakeEndpoints) -> None:
        changing(net, EXPORTER_URL, [exporter_page(1000, 1.0), exporter_page(1100, 1.5)])
        async with harness() as h:
            out = await h.call("metrics_rate", metric="ceph_osd_op_w", seconds=10)
            total = await h.call(
                "metrics_rate", metric="ceph_osd_op_w", seconds=10, aggregate="sum", group_by=[]
            )
        assert out["interval_s"] == 10
        assert {r["ceph_daemon"]: r["per_sec"] for r in rows(out)} == {"osd.0": 10, "osd.1": 20}
        assert total["rows"] == [["ceph_osd_op_w", 0]]  # the counters did not move again

    async def test_the_shortest_interval_still_takes_two_samples(
        self, harness: Make, net: FakeEndpoints
    ) -> None:
        """A scrape is reused for two seconds; the sample that ends a
        one-second measurement must not be the one that began it."""
        changing(net, EXPORTER_URL, [exporter_page(1000, 1.0), exporter_page(1100, 1.5)])
        async with harness() as h:
            out = await h.call("metrics_rate", metric="ceph_osd_op_w", seconds=1)
        assert out["interval_s"] == 1
        assert {r["ceph_daemon"]: r["per_sec"] for r in rows(out)} == {"osd.0": 100, "osd.1": 200}
        assert len([r for r in net.requests if str(r.url) == EXPORTER_URL]) == 2

    async def test_a_sample_warmed_by_another_caller_is_dated_when_it_was_taken(
        self, harness: Make, net: FakeEndpoints, clock: Any
    ) -> None:
        changing(net, EXPORTER_URL, [exporter_page(1000, 1.0), exporter_page(1300, 1.5)])
        async with harness() as h:
            await h.call("metrics_query", metric="ceph_osd_op_w")  # somebody scrapes
            clock.now += 1.5  # ... and a moment later a rate is asked for
            out = await h.call("metrics_rate", metric="ceph_osd_op_w", seconds=1.5)
        # The first sample is the reused one, 1.5 s old when the measurement begins:
        # 300 more operations over the 3 s between the two samples, not over 1.5 s.
        assert out["interval_s"] == 3
        assert {r["ceph_daemon"]: r["per_sec"] for r in rows(out)} == {"osd.0": 100, "osd.1": 200}

    async def test_latency_is_the_average_over_the_interval(
        self, harness: Make, net: FakeEndpoints
    ) -> None:
        changing(net, EXPORTER_URL, [exporter_page(1000, 1.0), exporter_page(1100, 1.5)])
        async with harness() as h:
            out = await h.call("metrics_rate", metric="ceph_osd_op_w_latency", seconds=10)
        per_osd = {r["ceph_daemon"]: r for r in rows(out)}
        assert per_osd["osd.0"]["ops_per_sec"] == 10 and per_osd["osd.0"]["avg_ms"] == 5
        assert per_osd["osd.1"]["ops_per_sec"] == 20 and per_osd["osd.1"]["avg_ms"] == 10
        assert out["rows"][0][0] == "osd.1"  # slowest first

    async def test_grouped_latency_is_weighted_by_operations(
        self, harness: Make, net: FakeEndpoints
    ) -> None:
        changing(net, EXPORTER_URL, [exporter_page(1000, 1.0), exporter_page(1100, 1.5)])
        async with harness() as h:
            out = await h.call(
                "metrics_rate", metric="ceph_osd_op_w_latency", seconds=10, group_by=[]
            )
        # 100 ops at 5 ms and 200 ops at 10 ms: 8.33 ms, not the 7.5 ms mean of the two
        assert rows(out) == [
            {"metric": "ceph_osd_op_w_latency", "ops_per_sec": 30, "avg_ms": 8.333}
        ]

    async def test_idle_daemons_have_no_latency(self, harness: Make, net: FakeEndpoints) -> None:
        net.pages[EXPORTER_URL] = exporter_page(1000, 1.0)
        async with harness() as h:
            out = await h.call("metrics_rate", metric="ceph_osd_op_w_latency", seconds=5)
        assert out["count"] == 2
        assert all(r["ops_per_sec"] == 0 and r.get("avg_ms") is None for r in rows(out))

    async def test_restarted_daemons_are_left_out(self, harness: Make, net: FakeEndpoints) -> None:
        changing(net, EXPORTER_URL, [exporter_page(1000, 1.0), exporter_page(10, 0.1)])
        async with harness() as h:
            out = await h.call("metrics_rate", metric="ceph_osd_op_w", seconds=10)
        assert out["count"] == 0

    async def test_mgr_metrics_need_a_longer_interval(
        self, harness: Make, net: FakeEndpoints
    ) -> None:
        before = fixture("mgr_metrics.txt")
        line = next(ln for ln in before.splitlines() if ln.startswith("ceph_pool_wr_bytes{"))
        name, value = line.rsplit(" ", 1)
        after = before.replace(line, f"{name} {float(value) + 16 * 1024}")
        changing(net, MGR_URL, [before, after])
        async with harness() as h:
            out = await h.call("metrics_rate", metric="ceph_pool_wr_bytes", seconds=3)
        assert out["interval_s"] == 16 and "every 15s" in out["note"]
        assert max(r["per_sec"] for r in rows(out)) == 1024

    async def test_unknown_metric(self, harness: Make) -> None:
        async with harness() as h:
            assert "metrics_search" in await h.error("metrics_rate", metric="ceph_nope")


class TestOverview:
    async def test_combines_cluster_state_and_measured_latency(
        self, harness: Make, net: FakeEndpoints
    ) -> None:
        changing(net, EXPORTER_URL, [exporter_page(1000, 1.0), exporter_page(1100, 1.5)])
        async with harness() as h:
            out = await h.call("perf_overview", seconds=10, top=1)
        assert out["health"] == "HEALTH_WARN"
        assert out["osd_commit_latency_ms"]["max"] >= out["osd_commit_latency_ms"]["avg"]
        ops = out["osd_ops"]
        assert ops["source"] == "ceph-exporter" and ops["interval_s"] == 10
        assert ops["write"] == {"ops_per_sec": 30, "avg_ms": 8.333}
        slowest = rows(ops["slowest_osds_write"])
        assert slowest == [{"osd": "osd.1", "host": "cephmcp-adm", "ops_per_sec": 20, "avg_ms": 10}]

    async def test_without_sampling_returns_at_once(
        self, harness: Make, net: FakeEndpoints
    ) -> None:
        async with harness() as h:
            out = await h.call("perf_overview", seconds=0)
        assert "osd_ops" not in out and out["health"] == "HEALTH_WARN"
        assert net.requests == []

    async def test_works_without_any_exporter(
        self, harness: Make, cli: FakeCli, net: FakeEndpoints
    ) -> None:
        without_orchestrator(cli)
        net.pages.clear()
        cli.responses["mgr services"] = "{}"
        cli.responses["mgr module ls"] = json.dumps({"enabled_modules": []})
        async with harness() as h:
            out = await h.call("perf_overview")
        assert "osd_commit_latency_ms" in out and "osd_ops" not in out
        assert any("mgr module enable prometheus" in n for n in out["notes"])


class TestPromQL:
    def posted(self, net: FakeEndpoints) -> tuple[str, dict[str, list[str]]]:
        request = next(r for r in reversed(net.requests) if "/api/v1/" in str(r.url))
        return str(request.url), parse_qs(request.content.decode())

    async def test_instant_query(self, harness: Make, net: FakeEndpoints) -> None:
        async with harness() as h:
            out = await h.call("prometheus_query", query="ceph_health_status")
        url, form = self.posted(net)
        assert url == PROMETHEUS_URL + "/api/v1/query" and form == {"query": ["ceph_health_status"]}
        assert rows(out)[0]["value"] == 1 and rows(out)[0]["metric"] == "ceph_health_status"

    async def test_range_query(self, harness: Make, net: FakeEndpoints, clock: Any) -> None:
        net.prometheus = json.loads(fixture("prom_range.json"))
        async with harness() as h:
            out = await h.call(
                "prometheus_query", query="sum(rate(ceph_osd_op_w[1m]))", start="-1h"
            )
        url, form = self.posted(net)
        assert url.endswith("/api/v1/query_range")
        assert float(form["end"][0]) == clock.now
        assert float(form["start"][0]) == clock.now - 3600
        assert form["step"] == ["60"]
        assert len(out["times"]) == 5 and out["times"][0].endswith("Z")
        assert out["rows"][0][-1][-1] == pytest.approx(1211.7036)
        assert "notes" in out  # Prometheus' own remarks are passed on

    async def test_errors(self, harness: Make, net: FakeEndpoints, cli: FakeCli) -> None:
        net.prometheus, net.prometheus_status = {"status": "error", "error": "parse error: x"}, 400
        async with harness() as h:
            assert "parse error: x" in await h.error("prometheus_query", query="sum((")
        without_orchestrator(cli)
        async with harness() as h:
            message = await h.error("prometheus_query", query="up")
        assert "--prometheus-url" in message and "metrics_rate" in message

    async def test_credentials_are_sent_when_configured(
        self, harness: Make, net: FakeEndpoints
    ) -> None:
        async with harness(prometheus_username="mcp", prometheus_password="pw") as h:
            await h.call("prometheus_query", query="up")
        request = next(r for r in net.requests if "/api/v1/" in str(r.url))
        assert request.headers["authorization"].startswith("Basic ")


def test_parse_time() -> None:
    now = 1_000_000.0
    assert parse_time("now", now) == now
    assert parse_time("-15m", now) == now - 900
    assert parse_time("-2h", now) == now - 7200
    assert parse_time("-1d", now) == now - 86400
    assert parse_time("1700000000", now) == 1_700_000_000.0
    assert parse_time("2026-11-11T12:00:00Z", now) == "2026-11-11T12:00:00Z"
    assert parse_time(None, now) is None
