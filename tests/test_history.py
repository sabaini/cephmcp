"""History: the store, the statistics, the recorder, the timeline, the tools."""

from __future__ import annotations

import json
import math
import random
import stat
from pathlib import Path
from typing import Any

import pytest
from conftest import FIXTURES, rows

from cephmcp import analysis, timeline
from cephmcp.cli import build_parser, settings_from
from cephmcp.config import default_state_dir
from cephmcp.history import RAW_KEEP, Event, Store, database_path
from cephmcp.metrics import MetricsHub
from cephmcp.policy import is_read_only
from cephmcp.recorder import Recorder, cluster_values, diff, fingerprint

HOUR, DAY = 3600.0, 86400.0
T0 = 1_794_400_000.0  # 2026-11-11 12:26:40 UTC, a round hour boundary is not needed


def load(scenario: str, name: str) -> Any:
    return json.loads((FIXTURES / scenario / f"{name}.json").read_text())


def at(text: str) -> float:
    ts = timeline.parse_stamp(text)
    assert ts is not None
    return ts


class TestStore:
    def test_samples_round_trip(self) -> None:
        store = Store()
        store.write(T0, {"cluster/used_bytes": 100.0, "osd/1/commit_ms": 4.0})
        store.write(
            T0 + 60, {"cluster/used_bytes": 110.0, "osd/1/commit_ms": 6.0, "osd/2/commit_ms": 1.0}
        )
        assert store.names() == ["cluster/used_bytes", "osd/1/commit_ms", "osd/2/commit_ms"]
        assert store.names("osd/*/commit_ms") == ["osd/1/commit_ms", "osd/2/commit_ms"]
        got = store.read("osd/*", T0, T0 + 60)
        assert got["osd/1/commit_ms"] == [(T0, 4.0, 4.0, 4.0), (T0 + 60, 6.0, 6.0, 6.0)]
        assert got["osd/2/commit_ms"] == [(T0 + 60, 1.0, 1.0, 1.0)]
        assert store.read("osd/*", T0 + 61, T0 + 120) == {}
        assert store.latest("cluster/*") == {"cluster/used_bytes": (T0 + 60, 110.0)}
        cover = store.coverage()
        assert (cover.first, cover.last, cover.samples, cover.series) == (T0, T0 + 60, 2, 3)

    def test_nan_is_not_stored(self) -> None:
        store = Store()
        store.write(T0, {"a": math.nan, "b": 1.0})
        assert store.read("*", 0, T0) == {"b": [(T0, 1.0, 1.0, 1.0)]}

    def test_old_samples_become_hourly_averages(self) -> None:
        store = Store()
        start = (T0 // HOUR) * HOUR
        for minute in range(180):  # three hours, a sample a minute
            store.write(start + 60 * minute, {"x": float(minute // 60), "y": float(minute)})
        now = start + RAW_KEEP + 2 * HOUR + 1  # the first two hours are older than two days
        store.compact(now, keep_days=90)
        got = store.read("x", 0, now)["x"]
        assert got[:2] == [(start + 1800, 0.0, 0.0, 0.0), (start + HOUR + 1800, 1.0, 1.0, 1.0)]
        assert len(got) == 2 + 60 and got[2] == (start + 2 * HOUR, 2.0, 2.0, 2.0)
        first_hour = store.read("y", 0, now)["y"][0]
        assert first_hour == (start + 1800, 29.5, 0.0, 59.0)
        assert store.coverage().first == start
        # Beyond the retention everything goes, series names included.
        store.compact(now + 100 * DAY, keep_days=90)
        assert store.read("*", 0, now + 200 * DAY) == {} and store.names() == []

    def test_compacting_twice_changes_nothing(self) -> None:
        store = Store()
        start = (T0 // HOUR) * HOUR
        for minute in range(120):
            store.write(start + 60 * minute, {"x": float(minute)})
        now = start + RAW_KEEP + 2 * HOUR + 1
        store.compact(now, 90)
        once = store.read("x", 0, now)
        store.compact(now + 30, 90)
        assert store.read("x", 0, now) == once

    def test_only_one_recorder(self) -> None:
        store = Store()
        assert store.recorder(T0) is None
        assert store.acquire("a", T0, ttl=150)
        assert not store.acquire("b", T0 + 100, ttl=150)
        assert store.recorder(T0 + 100) == "a"
        assert store.acquire("a", T0 + 100, ttl=150)  # renewal
        assert store.acquire("b", T0 + 300, ttl=150)  # a is gone: its lease ran out
        store.release("b")
        assert store.recorder(T0 + 301) is None

    def test_events_and_state(self) -> None:
        store = Store()
        store.write(T0, {}, [Event(T0, "osd", "osd.3", "osd.3 went down")])
        store.write(T0 + 60, {}, [Event(T0 + 60, "osd", "osd.3", "osd.3 went up")])
        assert [e.message for e in store.events(T0, T0 + 30)] == ["osd.3 went down"]
        assert len(store.events(0, T0 + 60)) == 2
        assert store.get_state("nothing") is None
        store.set_state("k", {"a": [1, 2]})
        store.set_state("k", {"a": [3]})
        assert store.get_state("k") == {"a": [3]}

    def test_journal(self) -> None:
        store = Store()
        first = store.log_change(
            T0,
            "cluster_flags",
            "set noout",
            {"flag": "noout"},
            ["osd set noout"],
            [["osd", "unset", "noout"]],
            "claude-code",
            "done",
        )
        second = store.log_change(
            T0 + 5,
            "change_undo",
            "undo of change 1",
            {},
            ["osd unset noout"],
            [],
            "x",
            "done",
            undoes=first,
        )
        entry = store.change(first)
        assert entry is not None and entry.undo == [["osd", "unset", "noout"]]
        assert entry.undone_by == second and entry.arguments == {"flag": "noout"}
        assert [c.id for c in store.changes()] == [second, first]
        assert [c.id for c in store.changes(T0 + 1, T0 + 10)] == [second]
        assert store.change(99) is None

    def test_file_is_private_and_named_after_the_cluster(self, tmp_path: Path) -> None:
        path = database_path(str(tmp_path / "state"), "f30c0449-39f6-42bb")
        assert path == str(tmp_path / "state" / "f30c0449-39f6-42bb.db")
        assert database_path(None, "x") == ":memory:"
        assert database_path(str(tmp_path), "../../etc/passwd").endswith("/etcpasswd.db")
        store = Store(path)
        store.write(T0, {"a": 1.0})
        store.close()
        assert stat.S_IMODE(Path(path).stat().st_mode) == 0o600
        assert stat.S_IMODE(Path(path).parent.stat().st_mode) == 0o700
        again = Store(path)
        assert again.read("a", 0, T0) == {"a": [(T0, 1.0, 1.0, 1.0)]}
        assert again.coverage().size_bytes


def series(values: list[float], step: float = 60.0) -> list[tuple[float, float]]:
    return [(T0 + i * step, v) for i, v in enumerate(values)]


class TestAnalysis:
    def test_a_step_is_found_where_it_happens(self) -> None:
        rng = random.Random(1)
        values = [5 + rng.gauss(0, 0.4) for _ in range(60)] + [
            14 + rng.gauss(0, 0.6) for _ in range(40)
        ]
        shift = analysis.find_shift(series(values))
        assert shift is not None
        assert shift.at == T0 + 60 * 60
        assert shift.before == pytest.approx(5, abs=0.3) and shift.after == pytest.approx(
            14, abs=0.3
        )
        assert shift.ratio == pytest.approx(2.8, abs=0.3)

    def test_noise_and_small_changes_are_not_shifts(self) -> None:
        rng = random.Random(2)
        flat = [10 + rng.gauss(0, 1) for _ in range(100)]
        assert analysis.find_shift(series(flat)) is None
        small = [10 + rng.gauss(0, 0.1) for _ in range(50)] + [
            11 + rng.gauss(0, 0.1) for _ in range(50)
        ]
        assert analysis.find_shift(series(small)) is None  # 10% is below the 20% default
        assert analysis.find_shift(series(small), min_relative=0.05) is not None
        assert analysis.find_shift(series(small), min_relative=0.05, min_absolute=2) is None
        assert analysis.find_shift(series([1, 9, 1, 9])) is None  # too short to tell
        assert analysis.find_shift(series([0.0] * 50)) is None

    def test_a_clean_step_without_noise(self) -> None:
        shift = analysis.find_shift(series([0.0] * 30 + [3.0] * 30))
        assert shift is not None and shift.at == T0 + 30 * 60 and shift.change == 3.0

    def test_trend(self) -> None:
        trend = analysis.fit([(T0 + i * HOUR, 100 + 2 * i) for i in range(48)])
        assert trend is not None
        assert trend.slope * HOUR == pytest.approx(2) and trend.r2 == pytest.approx(1)
        assert trend.span == 47 * HOUR and trend.points == 48
        now = T0 + 47 * HOUR
        assert trend.at(now) == pytest.approx(194)
        assert trend.reaches(294, now) == pytest.approx(now + 50 * HOUR)
        assert trend.reaches(204, now, current=200) == pytest.approx(now + 2 * HOUR)
        assert trend.reaches(100, now) == now  # already there
        falling = analysis.fit([(T0 + i, 100 - i) for i in range(10)])
        assert falling is not None and falling.reaches(200, T0) is None
        assert analysis.fit([(T0, 1.0)]) is None
        assert analysis.fit([(T0, 1.0), (T0, 2.0)]) is None

    def test_buckets(self) -> None:
        points = [(T0 + 60 * i, float(i), float(i), float(i)) for i in range(10)]
        got = analysis.buckets(points, T0, T0 + 600, 5)
        assert [b[1] for b in got] == [0.5, 2.5, 4.5, 6.5, 8.5]
        assert got[0] == (T0, 0.5, 0.0, 1.0)
        sparse = analysis.buckets(points[:2], T0, T0 + 600, 5)
        assert [b[1] for b in sparse] == [0.5, None, None, None, None]
        # A point exactly at the end belongs to the last interval.
        edge = analysis.buckets([(T0 + 600, 7.0, 7.0, 7.0)], T0, T0 + 600, 5)
        assert edge[-1][1] == 7.0

    @staticmethod
    def group(values: dict[str, list[float]]) -> dict[str, list[analysis.Point]]:
        return {
            name: [(T0 + 60 * i, v, v, v) for i, v in enumerate(vs)] for name, vs in values.items()
        }

    def test_outlier_among_peers(self) -> None:
        peers = {f"osd.{i}": [10.0] * 20 for i in range(5)}
        peers["osd.9"] = [45.0] * 20
        found, median = analysis.outliers(self.group(peers), min_excess=5)
        assert median == 10.0
        assert [(o.name, o.ratio, o.drifting) for o in found] == [("osd.9", 4.5, False)]
        assert analysis.outliers(self.group({f"osd.{i}": [10.0] * 20 for i in range(5)}))[0] == []
        assert analysis.outliers(self.group({"a": [1.0], "b": [9.0]})) == ([], None)

    def test_drift_is_reported_before_it_is_an_outlier(self) -> None:
        peers = {f"osd.{i}": [10.0] * 20 for i in range(5)}
        peers["osd.9"] = [10.0] * 10 + [17.0] * 10  # was like the others, now 1.7 times
        found, _ = analysis.outliers(self.group(peers), min_excess=5)
        assert len(found) == 1 and found[0].drifting
        assert found[0].earlier_ratio == 1.0 and found[0].ratio == pytest.approx(1.7)

    def test_a_disk_that_is_slow_whenever_it_is_used(self) -> None:
        # Load comes and goes: two samples in three are taken while nothing is written.
        peers = {f"osd.{i}": [0.0, 0.0, 2.0] * 10 for i in range(5)}
        peers["osd.9"] = [0.0, 0.0, 110.0] * 10
        found, _ = analysis.outliers(self.group(peers), min_excess=5, floor=1)
        assert [o.name for o in found] == ["osd.9"]
        # One hiccup does not make a disk an outlier.
        peers["osd.9"] = [0.0, 0.0, 2.0] * 9 + [0.0, 0.0, 500.0]
        assert analysis.outliers(self.group(peers), min_excess=5, floor=1)[0] == []

    def test_an_idle_group_has_no_outliers(self) -> None:
        idle = {f"osd.{i}": [0.0] * 20 for i in range(5)}
        idle["osd.9"] = [3.0] * 20  # three times nothing is still nothing
        assert analysis.outliers(self.group(idle), min_excess=5, floor=1)[0] == []


class TestSampling:
    def test_values_of_a_sample(self) -> None:
        values = cluster_values(
            load("microceph", "status"),
            load("microceph", "osd_df_tree"),
            load("microceph", "osd_perf"),
            load("microceph", "df_detail"),
            load("microceph", "osd_pool_stats"),
        )
        assert values["cluster/health"] == 0
        assert values["cluster/osds_up"] == 6 and values["cluster/pgs_not_clean"] == 0
        assert values["cluster/total_bytes"] == 51539607552
        assert values["pool/testpool/max_avail_bytes"] == 13968059392
        assert "pool/default.rgw.log/write_ops_sec" in values
        assert values["osd/1/used_pct"] == pytest.approx(10.25, abs=0.01)
        assert values["osd/1/pgs"] == 87 and values["osd/1/commit_ms"] == 0
        assert values["host/cephmcp-1/osds_up"] == 2
        assert all(isinstance(v, float) for v in values.values())
        lean = cluster_values(
            load("microceph", "status"),
            load("microceph", "osd_df_tree"),
            None,
            None,
            None,
            per_osd=False,
        )
        assert not any(name.startswith(("osd/", "pool/")) for name in lean)
        assert "host/cephmcp-1/used_pct" in lean

    @staticmethod
    def state(**changes: Any) -> dict[str, Any]:
        status = load("microceph", "status")
        dump = load("microceph", "osd_dump")
        for key, value in changes.items():
            if key == "status":
                status.update(value)
            else:
                dump[key] = value
        return fingerprint(status, dump, load("microceph", "osd_df_tree"))

    def test_no_events_when_nothing_changed(self) -> None:
        assert diff(self.state(), self.state(), T0) == []

    def test_events(self) -> None:
        before = self.state()
        dump = load("microceph", "osd_dump")
        osds = dump["osds"]
        osds[0].update(up=0)  # osd.1 down
        osds[1].update(up_from=999)  # osd.2 restarted
        osds[2].update(**{"in": 0, "weight": 0})  # osd.3 marked out
        osds.pop()  # osd.6 gone
        pools = dump["pools"]
        pools[1].update(size=2)
        gone = pools.pop()["pool_name"]
        pools.append({**pools[0], "pool_name": "new", "pool": 99})
        after = self.state(
            osds=osds,
            pools=pools,
            flags_set=["sortbitwise", "noout"],
            status={
                "health": {
                    "status": "HEALTH_WARN",
                    "checks": {"OSD_DOWN": {"summary": {"message": "1 osds down"}}},
                },
                "quorum_names": ["cephmcp-1", "cephmcp-2"],
            },
        )
        messages = [e.message for e in diff(before, after, T0)]
        assert messages == [
            "HEALTH_OK -> HEALTH_WARN",
            "health check raised: 1 osds down (OSD_DOWN)",
            "osd.6 removed from the cluster",
            "osd.1 went down",
            "osd.2 restarted",
            "osd.3 marked out",
            "pool new created (3 copies)",
            f"pool {gone} deleted",
            "pool testpool size 3 -> 2",
            "flag noout set",
            "mon.cephmcp-3 left the quorum",
        ]
        back = [e.message for e in diff(after, before, T0)]
        assert "health check cleared: OSD_DOWN (was: 1 osds down)" in back
        assert "osd.1 went up" in back and "flag noout cleared" in back
        assert "osd.6 added on host cephmcp-3" in back

    def test_a_failed_osd_dump_is_not_a_mass_removal(self) -> None:
        before = self.state()
        without = fingerprint(load("microceph", "status"), None, None)
        assert diff(before, without, T0) == [] and diff(without, before, T0) == []


@pytest.mark.anyio
class TestRecorder:
    async def test_sample_stores_series_and_events(
        self, harness: Any, cli: Any, clock: Any
    ) -> None:
        cli.use("microceph")
        async with harness(mgr_prometheus_url="http://10.165.218.74:9283") as h:
            recorder, store = h.app.recorder, await h.app.history.store()
            values = await recorder.sample()
            assert values["cluster/osds_up"] == 6
            assert [e.message for e in store.events(0, clock.now)] == ["recording started"]
            # A minute later osd.1 is down.
            clock.now += 60
            dump = cli.load("osd dump")
            dump["osds"][0]["up"] = 0
            cli.set_json("osd dump", dump)
            await recorder.sample()
            assert [e.message for e in store.events(0, clock.now)][1:] == ["osd.1 went down"]
            assert len(store.read("cluster/used_bytes", 0, clock.now)["cluster/used_bytes"]) == 2
            assert recorder.samples == 2 and cli.changes == []

    async def test_changes_found_after_a_gap_say_so(
        self, harness: Any, cli: Any, clock: Any
    ) -> None:
        cli.use("microceph")
        async with harness() as h:
            await h.app.recorder.sample()
            clock.now += 5 * HOUR
            dump = cli.load("osd dump")
            dump["flags_set"].append("noout")
            cli.set_json("osd dump", dump)
            await h.app.recorder.sample()
            store = await h.app.history.store()
            assert store.events(0, clock.now)[-1].message == (
                "flag noout set (noticed after 5h without recording)"
            )

    async def test_exporter_latencies(self, harness: Any, cli: Any, net: Any, clock: Any) -> None:
        async with harness() as h:
            first = await h.app.recorder.sample()
            assert not any("latency" in name for name in first)
            clock.now += 60
            # The same counters a minute later, with 600 more writes taking 3 s in total.
            page = net.pages["http://10.165.218.74:9926/metrics"]
            bumped = []
            for line in page.splitlines():
                if line.startswith("ceph_osd_op_w_latency_sum{") and 'ceph_daemon="osd.0"' in line:
                    name, value = line.rsplit(" ", 1)
                    line = f"{name} {float(value) + 3.0}"
                elif (
                    line.startswith("ceph_osd_op_w_latency_count{")
                    and 'ceph_daemon="osd.0"' in line
                ):
                    name, value = line.rsplit(" ", 1)
                    line = f"{name} {float(value) + 600}"
                bumped.append(line)
            net.pages["http://10.165.218.74:9926/metrics"] = "\n".join(bumped) + "\n"
            second = await h.app.recorder.sample()
            assert second["osd/0/op_w_latency_ms"] == pytest.approx(5.0)
            assert second["osd/0/op_w_per_sec"] == pytest.approx(10.0)
            assert second["cluster/op_w_latency_ms"] == pytest.approx(5.0)
            assert "osd/1/op_w_latency_ms" not in second  # no writes there: no latency to report

    async def test_large_clusters_record_no_per_osd_series(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness(history_max_osds=5) as h:
            values = await h.app.recorder.sample()
        assert not any(name.startswith("osd/") for name in values)
        assert "host/cephmcp-2/used_pct" in values

    async def test_run_records_until_stopped_and_holds_the_lease(
        self, harness: Any, cli: Any, clock: Any
    ) -> None:
        cli.use("microceph")

        class Stop(Exception):
            pass

        async with harness(record_interval=60) as h:
            app = h.app
            store = await app.history.store()
            naps = 0

            async def nap(seconds: float) -> None:
                nonlocal naps
                naps += 1
                assert store.recorder(clock.now) == recorder.owner
                if naps == 3:
                    raise Stop
                clock.now += seconds

            recorder = Recorder(app.settings, app.ceph, app.metrics, app.history, clock, nap)
            with pytest.raises(Stop):
                await recorder.run()
            assert recorder.samples == 3 and not recorder.running
            assert store.recorder(clock.now) is None  # released on the way out
            # Somebody else holds the lease: nothing is sampled.
            store.acquire("another-server", clock.now, ttl=10_000)
            naps = 2

            async def nap_once(seconds: float) -> None:
                raise Stop

            idle = Recorder(app.settings, app.ceph, app.metrics, app.history, clock, nap_once)
            with pytest.raises(Stop):
                await idle.run()
            assert idle.samples == 0

    async def test_a_failing_cluster_does_not_stop_the_recorder(
        self, harness: Any, cli: Any, clock: Any
    ) -> None:
        cli.fail("status", "Error ETIMEDOUT: cluster unreachable", 110)

        class Stop(Exception):
            pass

        async with harness() as h:
            naps = 0

            async def nap(seconds: float) -> None:
                nonlocal naps
                naps += 1
                if naps == 2:
                    raise Stop
                cli.failures.clear()

            app = h.app
            recorder = Recorder(app.settings, app.ceph, app.metrics, app.history, clock, nap)
            with pytest.raises(Stop):
                await recorder.run()
            assert recorder.samples == 1 and recorder.last_error == ""


class TestTimeline:
    def test_stamps(self) -> None:
        expected = 1794406595.464323
        assert timeline.parse_stamp("2026-11-11T14:16:35.464323+0000") == pytest.approx(expected)
        assert timeline.parse_stamp("2026-11-11T14:16:35.464323Z") == pytest.approx(expected)
        assert timeline.parse_stamp("2026-11-11T14:16:35Z") == int(expected)
        assert timeline.parse_stamp("2026-11-11 14:16:35.464323") == int(expected)
        assert timeline.parse_stamp(12.5) == 12.5
        assert timeline.parse_stamp("yesterday") is None and timeline.parse_stamp(None) is None
        assert timeline.fmt_time(expected) == "2026-11-11 14:16:35"

    def test_cluster_log_without_the_chatter(self) -> None:
        entries = timeline.cluster_entries(load("microceph", "log_cluster"))
        texts = [e.text for e in entries]
        assert not any(t.startswith("overall HEALTH") or "OSD bench result" in t for t in texts)
        assert not any("continuing backfill" in t for t in texts)
        assert "osd.1 boot" in texts  # the address list is gone
        assert "Client client.admin marked osd.7 out, while it was still marked up" in texts
        warn = [e for e in entries if e.level == "warn"]
        assert any("Health check failed" in e.text for e in warn)

    def test_audit_log_keeps_what_changed_the_cluster(self) -> None:
        entries = timeline.audit_entries(load("cephadm", "log_audit"), is_read_only)
        texts = [e.text for e in entries]
        assert "client.admin ran: osd pool create pool=testpool pg_num=32" in texts
        assert "client.admin ran: osd set-group flags=noout who=cephmcp-adm" in texts
        # Each command once (it is logged when dispatched and when finished) ...
        assert texts.count("client.admin ran: osd reweight id=2 weight=0.9") == 1
        # ... reads are no events, attempts that were refused never finished ...
        assert not any(" auth ls" in t or " auth get " in t for t in texts)
        assert sum("osd pool delete pool=testpool" in t for t in texts) == 1
        # ... configuration changes come from the config log, and the daemons'
        # own housekeeping is left out, except what moves data.
        assert not any(" config " in t for t in texts)
        assert not any(
            "osd crush create-or-move" in t or "auth get-or-create entity=osd" in t for t in texts
        )
        assert any(
            t.startswith("mgr.cephmcp-adm.ziovuj ran: osd pool set pool=testpool var=pg_num")
            for t in texts
        )

    def test_commands_are_rendered_without_secrets(self) -> None:
        render = timeline.render_command
        plain = {
            "prefix": "osd pool set",
            "pool": "vms",
            "var": "size",
            "val": "2",
            "format": "json",
        }
        assert render(plain) == "osd pool set pool=vms var=size val=2"
        secret = {"prefix": "config-key set", "key": "mgr/dashboard/jwt_secret", "val": "hunter2"}
        assert render(secret) == "config-key set key=mgr/dashboard/jwt_secret val=<redacted>"
        assert "hunter2" in render(secret, secrets=True)
        key = "AQ" + "B" * 36 + "=="
        imported = {
            "prefix": "auth add",
            "entity": "client.x",
            "caps": ["mon", "allow r"],
            "blob": key,
        }
        assert key not in render(imported) and "caps=mon,allow r" in render(imported)
        assert render({"prefix": "dashboard set-rgw-credentials", "password": "pw"}).endswith(
            "password=<redacted>"
        )

    def test_config_log(self) -> None:
        log = [
            {
                "timestamp": "2026-11-11T12:56:32.0+0000",
                "changes": [{"name": "osd/osd_max_backfills", "new_value": "2"}],
            },
            {
                "timestamp": "2026-11-11T12:56:33.0+0000",
                "changes": [
                    {"name": "osd/osd_max_backfills", "previous_value": "2"},
                    {"name": "global/x", "previous_value": "1", "new_value": "3"},
                    {"name": "mgr/mgr/dashboard/jwt_secret", "new_value": "hunter2"},
                ],
            },
        ]
        assert [e.text for e in timeline.config_entries(log)] == [
            "osd/osd_max_backfills set to 2",
            "osd/osd_max_backfills removed (was 2)",
            "global/x changed from 1 to 3",
            "mgr/mgr/dashboard/jwt_secret set to <redacted>",
        ]
        assert "hunter2" in timeline.config_entries(log, secrets=True)[-1].text

    def test_crashes_and_progress(self) -> None:
        crash = timeline.crash_entries(
            [{"timestamp": "2026-11-11T09:30:00.123456Z", "entity_name": "osd.3", "crash_id": "c1"}]
        )
        assert crash[0].text.startswith("osd.3 crashed") and crash[0].level == "error"
        done = timeline.progress_entries(load("microceph", "progress"))
        assert done[0].text == "PG autoscaler increasing pool 2 PGs from 1 to 32 (took 0s)"
        running = timeline.progress_entries(
            {"events": [{"started_at": T0, "message": "Rebalancing", "progress": 0.416}]}
        )
        assert running[0].text == "Rebalancing (42% so far)"

    def test_runs_collapse(self) -> None:
        def entry(ts: float, text: str, key: str, source: str = timeline.CLUSTER) -> timeline.Entry:
            return timeline.Entry(ts, source, text, key=key)

        merged = timeline.collapse(
            [
                entry(3, "osd.2 boot", "osd.N boot"),
                entry(1, "osd.0 boot", "osd.N boot"),
                entry(2, "osd.1 boot", "osd.N boot"),
                entry(4, "noout set", "", timeline.AUDIT),  # another source: does not break the run
                entry(5, "osd.3 boot", "osd.N boot"),
                entry(6, "Cluster is now healthy", "Cluster is now healthy"),
                entry(7, "osd.4 boot", "osd.N boot"),
            ]
        )
        assert [timeline.describe(e) for e in merged] == [
            "osd.0 boot (and 3 more like it, until 00:00:05)",
            "noout set",
            "Cluster is now healthy",
            "osd.4 boot",
        ]
        updates = timeline.collapse(
            [
                entry(1, "Health check update: 5 pgs degraded (PG_DEGRADED)", "update:PG_DEGRADED"),
                entry(2, "Health check update: 2 pgs degraded (PG_DEGRADED)", "update:PG_DEGRADED"),
            ]
        )
        # For a counter that keeps changing, the last state is the one to show.
        assert timeline.describe(updates[0]).startswith("Health check update: 2 pgs degraded")


def fill(store: Store, name: str, start: float, values: list[float], step: float = 60.0) -> None:
    for i, value in enumerate(values):
        store.write(start + i * step, {name: value})


@pytest.mark.anyio
class TestWhatChanged:
    async def test_timeline_of_a_day(self, harness: Any, cli: Any, clock: Any) -> None:
        cli.use("cephadm")
        clock.now = at("2026-11-11T16:00:00Z")
        async with harness() as h:
            out = await h.call("what_changed", since="-6h", limit=400)
        events = rows(out["events"])
        assert out["period"] == {"from": "2026-11-11 10:00:00", "to": "2026-11-11 16:00:00"}
        assert [e["time"] for e in events] == sorted(e["time"] for e in events)
        what = {e["what"]: e for e in events}
        assert what["Health check failed: 1 full osd(s) (OSD_FULL)"]["source"] == "cluster log"
        assert what["client.admin ran: osd set key=noout"]["time"] == "2026-11-11 12:56:17"
        assert what["mon/mon_allow_pool_delete set to true"]["source"] == "config"
        assert what["osd/osd_max_backfills removed (was 2)"]["time"] == "2026-11-11 12:56:33"
        assert any(e["source"] == "progress" and "PG autoscaler" in e["what"] for e in events)
        assert out["coverage"]["cluster log"] == "since 2026-11-11 12:34:09"
        assert out["coverage"]["recorded metrics"] == "nothing"
        assert any("begin after the start of the period" in n for n in out["notes"])
        assert any("No metrics have been recorded" in n for n in out["notes"])

    async def test_period_and_limit(self, harness: Any, cli: Any, clock: Any) -> None:
        cli.use("cephadm")
        clock.now = at("2026-11-11T16:00:00Z")
        async with harness() as h:
            out = await h.call(
                "what_changed", since="2026-11-11T12:56:00Z", until="2026-11-11T12:57:00Z"
            )
            events = rows(out["events"])
            assert events[0]["what"] == "Health check failed: noout flag(s) set (OSDMAP_FLAGS)"
            assert all("2026-11-11 12:56" in e["time"] for e in events)
            few = await h.call("what_changed", since="-6h", limit=5)
            assert len(few["events"]["rows"]) == 5 and "latest 5 of" in few["events"]["truncated"]
            assert rows(few["events"])[-1]["what"] == "client.admin ran: auth rm entity=client.mcp"
            quiet = await h.call("what_changed", since="-10m")
            assert quiet["events"] == "nothing on record in this period"
            assert "before the end" in await h.error("what_changed", since="-1h", until="-2h")
            assert "times must be" in await h.error("what_changed", since="last week")

    async def test_recorder_fills_in_where_the_logs_end(
        self, harness: Any, cli: Any, clock: Any
    ) -> None:
        cli.use("cephadm")
        clock.now = at("2026-11-11T16:00:00Z")
        async with harness() as h:
            store = await h.app.history.store()
            early, late = at("2026-11-11T09:00:00Z"), at("2026-11-11T13:00:00Z")
            store.write(early, {}, [Event(early, "osd", "osd.2", "osd.2 went down")])
            store.write(late, {}, [Event(late, "osd", "osd.2", "osd.2 went up")])
            store.log_change(
                late + 60, "cluster_flags", "set noout on cephmcp-adm", {}, [], [], "c", "done"
            )
            events = rows((await h.call("what_changed", since="-12h", limit=400))["events"])
        recorded = [e for e in events if e["source"] == "recorder"]
        # The cluster log starts at 12:34: before that the recorder is the witness,
        # after that the log tells it itself.
        assert [(e["time"], e["what"]) for e in recorded] == [
            ("2026-11-11 09:00:00", "osd.2 went down")
        ]
        own = [e for e in events if e["source"] == "cephmcp"]
        assert own[0]["what"] == "cluster_flags: set noout on cephmcp-adm [change 1, done]"

    async def test_metric_shifts(self, harness: Any, cli: Any, clock: Any) -> None:
        cli.use("cephadm")
        clock.now = at("2026-11-11T16:00:00Z")
        async with harness() as h:
            store = await h.app.history.store()
            start = clock.now - 4 * HOUR
            fill(store, "cluster/op_w_latency_ms", start, [4.0] * 150 + [31.0] * 90)
            fill(store, "osd/1/commit_ms", start, [3.0] * 150 + [80.0] * 90)
            fill(store, "osd/2/commit_ms", start, [3.0] * 240)
            fill(store, "cluster/used_bytes", start, [1e9 + 1e6 * i for i in range(240)])
            out = await h.call("what_changed", since="-3h")
        shifts = rows(out["metric_shifts"])
        assert [s["series"] for s in shifts] == ["osd/1/commit_ms", "cluster/op_w_latency_ms"]
        assert shifts[1] == {
            "series": "cluster/op_w_latency_ms",
            "at": "2026-11-11 14:30:00",
            "before": "4 ms",
            "after": "31 ms",
        }
        assert out["coverage"]["recorded metrics"] == "since 2026-11-11 12:00:00"

    async def test_the_same_shift_on_many_osds_is_one_event(
        self, harness: Any, cli: Any, clock: Any
    ) -> None:
        cli.use("cephadm")
        clock.now = at("2026-11-11T16:00:00Z")
        async with harness() as h:
            store = await h.app.history.store()
            start = clock.now - 4 * HOUR
            for osd in range(8):
                fill(
                    store, f"osd/{osd}/op_w_latency_ms", start, [4.0 + osd / 10] * 150 + [30.0] * 90
                )
            fill(store, "osd/9/op_w_latency_ms", start, [4.0] * 60 + [90.0] * 180)
            out = await h.call("what_changed", since="-210m")
        shifts = rows(out["metric_shifts"])
        assert shifts == [
            {
                "series": "osd/*/op_w_latency_ms",
                "at": "2026-11-11 14:30:00",
                "before": "4.35 ms",
                "after": "30 ms",
                "seen_on": "8 osds (medians)",
            },
            {
                "series": "osd/9/op_w_latency_ms",
                "at": "2026-11-11 13:00:00",
                "before": "4 ms",
                "after": "90 ms",
                "seen_on": None,
            },
        ]

    async def test_secrets_in_the_logs_stay_masked(
        self, harness: Any, cli: Any, clock: Any
    ) -> None:
        cli.use("cephadm")
        clock.now = at("2026-11-11T16:00:00Z")
        secret = (
            "from='client.? ' entity='client.admin' cmd='[{\"prefix\": \"config-key set\", "
            '"key": "mgr/dashboard/jwt_secret", "val": "hunter2"}]\': finished'
        )
        log = cli.load("log last 10000 info audit")
        log.append({"stamp": "2026-11-11T15:00:00.0+0000", "message": secret})
        cli.set_json("log last 10000 info audit", log)
        async with harness() as h:
            failed, text = await h.raw("what_changed", since="-2h")
            assert not failed and "hunter2" not in text and "jwt_secret val=<redacted>" in text
        async with harness(allow_secrets=True) as h:
            assert "hunter2" in (await h.raw("what_changed", since="-2h"))[1]


@pytest.mark.anyio
class TestHistoryQuery:
    async def test_without_arguments_says_what_is_recorded(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            empty = await h.call("history_query")
            assert empty["history"]["recorder"] == "not running"
            assert empty["history"]["file"] == "in memory"
            assert any("Nothing is recording" in n for n in empty["history"]["notes"])
            await h.app.recorder.sample()
            out = await h.call("history_query")
            assert out["history"]["samples"] == 1
            assert out["recorded"]["cluster"].startswith("cluster/{")
            assert "commit_ms" in out["recorded"]["osd"] and "osd/<name>/" in out["recorded"]["osd"]

    async def test_values_and_shift(self, harness: Any, clock: Any) -> None:
        async with harness() as h:
            store = await h.app.history.store()
            start = clock.now - 2 * HOUR
            fill(store, "cluster/op_w_latency_ms", start, [4.0] * 60 + [20.0] * 61)
            fill(store, "cluster/write_bytes_sec", start, [1e6] * 121)
            out = await h.call("history_query", series="cluster/*", since="-2h", points=4)
            got = {r["series"]: r for r in rows(out)}
            latency = got["cluster/op_w_latency_ms"]
            assert latency["values"] == [4, 4, 20, 20]
            assert (latency["min"], latency["max"], latency["last"]) == (4, 20, 20)
            assert latency["shift"].startswith("rose from 4 ms to 20 ms at ")
            assert got["cluster/write_bytes_sec"]["shift"] is None
            assert out["interval"] == "30m" and len(out["times"]) == 4
            peak = await h.call(
                "history_query", series="cluster/op_w*", since="-2h", points=2, stat="max"
            )
            assert rows(peak)[0]["values"] == [4, 20]

    async def test_errors_explain(self, harness: Any, clock: Any) -> None:
        async with harness() as h:
            assert "nothing recorded matches 'osd/*/x'" in await h.error(
                "history_query", series="osd/*/x"
            )
            store = await h.app.history.store()
            store.write(clock.now - 10 * DAY, {"cluster/health": 0.0})
            message = await h.error("history_query", series="cluster/health", since="-1h")
            assert "has no samples in that period; history covers" in message
            assert "glob pattern" in await h.error("history_query", series="x; DROP TABLE")

    async def test_limit(self, harness: Any, clock: Any) -> None:
        async with harness() as h:
            store = await h.app.history.store()
            store.write(clock.now - 60, {f"osd/{i}/commit_ms": float(i) for i in range(30)})
            out = await h.call("history_query", series="osd/*/commit_ms", limit=5)
            assert len(out["rows"]) == 5 and out["count"] == 30
            assert "showing 5 of 30 series" in out["truncated"]


@pytest.mark.anyio
class TestCapacityForecast:
    async def test_needs_some_history(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            out = await h.call("capacity_forecast")
        assert out["now"]["raw_total"] == "48 GiB" and out["now"]["used_pct"] == 8
        assert out["now"]["fullest_osd"] == "osd.4 at 13.7%"
        assert out["forecast"].startswith("not enough history for a trend (have none")
        assert out["history"]["recorder"] == "not running"

    async def test_dates(self, harness: Any, cli: Any, clock: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            store = await h.app.history.store()
            used = cli.load("df detail")["stats"]["total_used_raw_bytes"]
            gib = 2**30
            # Ten days at 1 GiB a day, a sample an hour, ending at today's usage.
            for hour in range(241):
                when = clock.now - (240 - hour) * HOUR
                store.write(
                    when,
                    {
                        "cluster/used_bytes": used - (240 - hour) * gib / 24,
                        "pool/testpool/stored_bytes": 5 * gib + hour * gib / 96,
                    },
                )
            out = await h.call("capacity_forecast", days=30)
        assert out["growth_per_day"] == "1 GiB"
        assert out["basis"] == {
            "source": "recorded history",
            "history": "10d",
            "samples": 241,
            "confidence": "good",
        }
        # 48 GiB raw, 3.84 GiB used, fullest OSD at 13.7% against 8% on average:
        # nearfull (85%) is reached when the average is at about 49.7%.
        nearfull = out["limits_reached"]["nearfull (85%)"]
        assert nearfull["in_days"] == pytest.approx(20.0, abs=0.5)
        assert out["limits_reached"]["full (95%)"]["in_days"] > nearfull["in_days"]
        assert len(nearfull["at"]) == 10 and nearfull["at"].startswith("2027-02-0")  # a date
        assert any("fullest OSD is 1.7" in n for n in out["notes"])
        pool = next(p for p in rows(out["pools"]) if p["pool"] == "testpool")
        assert pool["growth_per_day"] == "256 MiB" and pool["full_in_days"] == pytest.approx(
            52.0, abs=0.2
        )

    async def test_no_growth(self, harness: Any, cli: Any, clock: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            store = await h.app.history.store()
            for hour in range(48):
                store.write(clock.now - hour * HOUR, {"cluster/used_bytes": 4e9 + hour * 1e6})
            out = await h.call("capacity_forecast")
        assert out["forecast"] == "raw usage is not growing; no limit is being approached"
        assert "limits_reached" not in out

    async def test_prometheus_is_used_when_it_knows_more(
        self, harness: Any, cli: Any, net: Any, clock: Any
    ) -> None:
        cli.use("microceph")
        used = cli.load("df detail")["stats"]["total_used_raw_bytes"]
        net.prometheus = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {},
                        "values": [
                            [clock.now - (20 - d) * DAY, str(used - (20 - d) * 2 * 2**30)]
                            for d in range(21)
                        ],
                    }
                ],
            },
        }
        async with harness(prometheus_url="http://10.165.218.74:9095") as h:
            store = await h.app.history.store()
            store.write(clock.now - 600, {"cluster/used_bytes": float(used)})
            out = await h.call("capacity_forecast")
            assert out["basis"]["source"] == "Prometheus" and out["basis"]["history"] == "20d"
            assert out["growth_per_day"] == "2 GiB"
            net.prometheus_status, net.prometheus = 500, {"status": "error", "error": "boom"}
            broken = await h.call("capacity_forecast")
            assert broken["forecast"].startswith("not enough history")
            assert any("Prometheus could not be asked" in n for n in broken["notes"])


@pytest.mark.anyio
class TestOsdOutliers:
    async def test_from_history(self, harness: Any, cli: Any, clock: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            store = await h.app.history.store()
            start = clock.now - 2 * HOUR
            for osd in range(1, 7):
                slow = [9.0] * 60 + [9.0 if osd != 4 else 30.0] * 60
                fill(store, f"osd/{osd}/commit_ms", start, slow if osd != 2 else [70.0] * 120)
            out = await h.call("osd_outliers", since="-2h")
        assert out["basis"] == "recorded history"
        assert out["compared"] == "6 OSDs in 1 device class group(s)"
        found = rows(out["outliers"])
        assert [(r["osd"], r["host"], r["verdict"]) for r in found] == [
            ("osd.2", "cephmcp-1", "outlier"),
            ("osd.4", "cephmcp-2", "drifting away from its peers"),
        ]
        assert found[0]["commit_ms"] == 70 and found[0]["times_peers"] == 7.8
        assert found[1]["earlier"] == 1 and found[1]["times_peers"] == 3.3

    async def test_without_history_compares_current_values(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        perf = cli.load("osd perf")
        for info in perf["osdstats"]["osd_perf_infos"]:
            info["perf_stats"]["commit_latency_ms"] = 250 if info["id"] == 3 else 8
        cli.set_json("osd perf", perf)
        async with harness() as h:
            out = await h.call("osd_outliers")
            assert out["basis"].startswith("current values only")
            assert [r["osd"] for r in rows(out["outliers"])] == ["osd.3"]
            cli.use("microceph")
            calm = await h.call("osd_outliers")
            assert calm["result"] == "no OSD stands out from its peers"
            full = await h.call("osd_outliers", metric="used_pct")
            assert full["result"] == "no OSD stands out from its peers"
            message = await h.error("osd_outliers", metric="op_w_latency_ms")
            assert "metric exporters" in message

    async def test_too_few_peers(self, harness: Any, cli: Any) -> None:
        cli.use("cephadm")
        tree = cli.load("osd df tree")
        for node in tree["nodes"]:
            if node["type"] == "osd":
                node["device_class"] = "ssd" if node["id"] == 0 else "hdd"
        cli.set_json("osd df tree", tree)
        async with harness() as h:
            out = await h.call("osd_outliers")
        assert out["result"] == "fewer than 3 OSDs per device class: nothing to compare with"


class TestSettings:
    def parse(self, *argv: str, **environ: str) -> Any:
        return settings_from(build_parser().parse_args(argv), {"HOME": "/home/op", **environ})

    def test_the_command_line_records_into_the_state_directory(self) -> None:
        settings = self.parse()
        assert settings.record and settings.state_dir == "/home/op/.local/state/cephmcp"
        assert settings.record_interval == 60 and settings.history_days == 90

    def test_switches(self) -> None:
        assert not self.parse("--no-record").record
        assert self.parse("--no-state").state_dir is None
        assert self.parse("--state-dir", "/var/lib/cephmcp").state_dir == "/var/lib/cephmcp"
        tuned = self.parse("--record-interval", "30", "--history-days", "7")
        assert (tuned.record_interval, tuned.history_days) == (30, 7)
        env = self.parse(CEPHMCP_RECORD="0", CEPHMCP_STATE_DIR="/srv/x", CEPHMCP_HISTORY_DAYS="14")
        assert (env.record, env.state_dir, env.history_days) == (False, "/srv/x", 14)
        assert self.parse(XDG_STATE_HOME="/xdg").state_dir == "/xdg/cephmcp"
        assert default_state_dir({"HOME": "/root"}) == "/root/.local/state/cephmcp"

    def test_limits(self) -> None:
        with pytest.raises(ValueError, match="record-interval"):
            self.parse("--record-interval", "1")
        with pytest.raises(ValueError, match="history-days"):
            self.parse("--history-days", "0")


def test_hub_is_shared_with_the_recorder(harness: Any) -> None:
    h = harness()
    assert isinstance(h.app.metrics, MetricsHub) and h.app.recorder is not None


SECRET_COMMAND = '{"prefix": "config-key set", "key": "mgr/dashboard/jwt_secret", "val": "hunter2"}'


def test_audit_lines_are_readable_and_masked() -> None:
    line = f"from='client.? 10.0.0.1:0/1' entity='client.admin' cmd={SECRET_COMMAND} : dispatch"
    assert timeline.audit_message(line) == (
        "client.admin dispatch: config-key set key=mgr/dashboard/jwt_secret val=<redacted>"
    )
    assert "hunter2" in timeline.audit_message(line, secrets=True)
    # A line that cannot be parsed (cut off here) still goes through the general masking.
    cut = 'from=\'x\' entity=\'client.admin\' cmd=[{"prefix": "x", "password": "hunter2", "n'
    assert "hunter2" not in timeline.audit_message(cut)


@pytest.mark.anyio
async def test_the_audit_channel_of_the_cluster_log_is_masked(harness: Any, cli: Any) -> None:
    message = f"from='client.? ' entity='client.admin' cmd='[{SECRET_COMMAND}]': finished"
    entry = {"stamp": "2026-11-11T15:00:00.0+0000", "channel": "audit", "message": message}
    cli.set_json("log last 10000 info audit", [entry])
    async with harness() as h:
        out = await h.call("cluster_log", channel="audit")
        assert rows(out)[0]["message"] == (
            "client.admin finished: config-key set key=mgr/dashboard/jwt_secret val=<redacted>"
        )
    async with harness(allow_secrets=True) as h:
        shown = rows(await h.call("cluster_log", channel="audit"))[0]["message"]
        assert "val=hunter2" in shown
