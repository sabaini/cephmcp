"""Charts: the helpers, the renderers and the tool."""

from __future__ import annotations

import base64
import io
import json
from typing import Any

import pytest
from PIL import Image

from cephmcp import charts
from cephmcp.tools.charts import entity_of, panel_title

HOUR, DAY = 3600.0, 86400.0
T0 = 1_794_412_800.0  # 2026-11-11 16:00:00 UTC


def decode(png: bytes) -> Image.Image:
    assert png.startswith(b"\x89PNG\r\n\x1a\n")
    return Image.open(io.BytesIO(png)).convert("RGB")


def colours(image: Image.Image) -> set[tuple[int, int, int]]:
    return {colour for _, colour in image.getcolors(maxcolors=1 << 24)}


def rgb(colour: str) -> tuple[int, int, int]:
    return charts._rgb(colour)


def has_hue(image: Image.Image, colour: str, tolerance: int = 45) -> bool:
    """Whether something close to a colour is in the picture. Thin lines are
    blended with their surroundings when the picture is scaled down, so they
    never show the exact colour."""
    want = rgb(colour)
    return any(
        sum(abs(a - b) for a, b in zip(c, want, strict=True)) <= tolerance for c in colours(image)
    )


class TestFigures:
    def test_units_follow_the_series_name(self) -> None:
        assert charts.unit_of("cluster/write_bytes_sec") == "bytes/s"
        assert charts.unit_of("pool/vms/stored_bytes") == "bytes"
        assert charts.unit_of("osd/3/commit_ms") == "ms"
        assert charts.unit_of("host/a/commit_ms_max") == "ms"
        assert charts.unit_of("cluster/degraded_pct") == "%"
        assert charts.unit_of("cluster/op_w_per_sec") == "/s"
        assert charts.unit_of("osd/3/pgs") == ""

    def test_formatting(self) -> None:
        assert charts.fmt(0, "bytes") == "0 B"
        assert charts.fmt(1536, "bytes") == "1.5 KiB"
        assert charts.fmt(3 * 2**30, "bytes/s") == "3 GiB/s"
        assert charts.fmt(45.83, "ms") == "45.8 ms"
        assert charts.fmt(33.333, "%") == "33.3%"
        assert charts.fmt(1234.0, "/s") == "1234/s"
        assert charts.fmt(87.0, "") == "87"

    def test_value_ticks_are_round(self) -> None:
        assert charts.nice_ticks(92, "ms") == [0, 25, 50, 75, 100]
        assert charts.nice_ticks(0.7, "%") == [0, 0.2, 0.4, 0.6, 0.8]
        assert charts.nice_ticks(0, "ms") == [0, 1]
        # Bytes are rounded in the binary unit their labels use.
        gib = 2**30
        assert charts.nice_ticks(26 * gib, "bytes") == [0, 10 * gib, 20 * gib, 30 * gib]
        assert all(t % (5 * 2**20) == 0 for t in charts.nice_ticks(14.2 * 2**20, "bytes/s"))

    def test_time_ticks_fall_on_round_times(self) -> None:
        ticks, pattern = charts.time_ticks(T0 + 400, T0 + 400 + 45 * 60)
        assert pattern == "%H:%M" and all(t % 600 == 0 for t in ticks)
        assert [charts.clock(t, pattern) for t in ticks] == [
            "16:10",
            "16:20",
            "16:30",
            "16:40",
            "16:50",
        ]
        ticks, pattern = charts.time_ticks(T0, T0 + 30 * DAY)
        assert pattern == "%b %d" and len(ticks) <= 8
        _, pattern = charts.time_ticks(T0, T0 + 3 * DAY)
        assert pattern == "%b %d %H:%M"

    def test_labels_are_spread_but_stay_in_order(self) -> None:
        assert charts.spread([100.0], 13) == [100.0]
        assert charts.spread([10.0, 50.0, 90.0], 13) == [10.0, 50.0, 90.0]
        close = charts.spread([100.0, 102.0, 104.0], 13)
        assert close == [89.0, 102.0, 115.0]
        assert charts.spread([104.0, 100.0], 13) == [108.5, 95.5]

    def test_gaps_split_a_line(self) -> None:
        points = [(0.0, 1.0), (60.0, 1.0), (120.0, 1.0), (900.0, 2.0), (960.0, 2.0)]
        assert [len(s) for s in charts._segments(points, 200)] == [3, 2]

    def test_only_what_stands_out_keeps_its_colour(self) -> None:
        lines = [charts.Line(f"osd.{i}", [(0.0, 10.0), (1.0, 10.0 + i / 10)]) for i in range(8)]
        lines[5].points[-1] = (1.0, 90.0)
        assert charts.emphasise(lines) == 5
        assert not lines[5].context and sum(ln.context for ln in lines) == 5
        few = [charts.Line(f"osd.{i}", [(0.0, 1.0)]) for i in range(4)]
        assert charts.emphasise(few) == 0 and not any(ln.context for ln in few)

    def test_text_inside_a_fill_is_readable(self) -> None:
        assert charts.luminance("#ffffff") == pytest.approx(1.0)
        assert (
            charts.luminance(charts.LIGHT.ramp[0]) > 0.36 > charts.luminance(charts.LIGHT.ramp[-1])
        )

    def test_names(self) -> None:
        assert entity_of("osd/3/commit_ms") == "osd.3"
        assert entity_of("pool/default.rgw.log/stored_bytes") == "pool default.rgw.log"
        assert entity_of("cluster/health") == "cluster"
        assert panel_title(["cluster/health"]) == "cluster/health"
        assert panel_title(["osd/1/commit_ms", "osd/2/commit_ms"]) == "osd/*/commit_ms"


class TestRendering:
    def lines(self) -> list[charts.Line]:
        return [
            charts.Line(
                f"osd/{i}/commit_ms",
                [(T0 + 60 * m, 5.0 + i + (80 if i == 2 and m > 30 else 0)) for m in range(60)],
                entity=f"osd.{i}",
            )
            for i in range(3)
        ]

    def test_line_chart(self) -> None:
        panel = charts.Panel("osd/*/commit_ms", "ms", self.lines())
        image = decode(charts.line_chart("Commit latency", "one hour", [panel], T0, T0 + HOUR))
        assert image.size == (880 * charts.SCALE, (106 + 190 + 34) * charts.SCALE)
        seen = colours(image)
        assert rgb(charts.LIGHT.surface) in seen
        assert {rgb(c) for c in charts.LIGHT.series[:3]} <= seen  # three lines, three hues
        assert rgb(charts.LIGHT.series[3]) not in seen

    def test_dark_theme_and_several_panels(self) -> None:
        panels = [
            charts.Panel("osd/*/commit_ms", "ms", self.lines()),
            charts.Panel(
                "cluster/write_bytes_sec",
                "bytes/s",
                [charts.Line("w", [(T0, 0.0), (T0 + HOUR, 5e6)])],
            ),
        ]
        image = decode(charts.line_chart("t", "s", panels, T0, T0 + HOUR, charts.DARK))
        assert image.size[1] == (106 + 2 * 190 + 34 + 34) * charts.SCALE
        seen = colours(image)
        assert rgb(charts.DARK.surface) in seen and rgb(charts.DARK.series[0]) in seen

    def test_context_lines_are_grey(self) -> None:
        lines = [
            charts.Line(f"osd/{i}/x_ms", [(T0, 10.0), (T0 + HOUR, 10.0 + i)], entity=f"osd.{i}")
            for i in range(9)
        ]
        lines[4].points[-1] = (T0 + HOUR, 300.0)
        charts.emphasise(lines)
        image = decode(charts.line_chart("t", "s", [charts.Panel("p", "ms", lines)], T0, T0 + HOUR))
        seen = colours(image)
        assert rgb(charts.LIGHT.other) in seen and rgb(charts.LIGHT.series[0]) in seen

    def test_projection_and_limits(self) -> None:
        measured = charts.Line(
            "used", [(T0 + 600 * i, 1e9 + 1e7 * i) for i in range(30)], entity="used"
        )
        projected = charts.Line(
            "p", [(T0 + 5 * HOUR, 1.3e9), (T0 + 9 * HOUR, 2e9)], entity="used", projected=True
        )
        panel = charts.Panel(
            "raw used", "bytes", [measured, projected], [charts.Mark(1.9e9, "nearfull (85%)")]
        )
        image = decode(
            charts.line_chart("Capacity", "s", [panel], T0, T0 + 10 * HOUR, now=T0 + 5 * HOUR)
        )
        assert has_hue(image, charts.LIGHT.warning)

    def test_empty_series_do_not_break(self) -> None:
        panel = charts.Panel("p", "ms", [charts.Line("a", []), charts.Line("b", [(T0, 1.0)])])
        decode(charts.line_chart("t", "s", [panel], T0, T0 + HOUR))

    def test_grid(self) -> None:
        rows = [
            ("node1", [charts.Cell("osd.0", 12.0), charts.Cell("osd.1", 91.0, flagged=True)]),
            ("node2", [charts.Cell("osd.2", 40.0), charts.Cell("osd.3 down", None)]),
        ]
        image = decode(
            charts.osd_grid("How full", "4 OSDs", rows, "%", (0, 100), flag_note="above nearfull")
        )
        seen = colours(image)
        assert rgb(charts.LIGHT.warning) in seen  # the mark on the flagged cell
        assert rgb(charts.LIGHT.ramp[-2]) in seen and rgb(charts.LIGHT.ramp[1]) in seen
        plain = decode(charts.osd_grid("How full", "4 OSDs", rows[1:], "%", (0, 100)))
        assert rgb(charts.LIGHT.warning) not in colours(plain)
        assert plain.size[1] < image.size[1]

    def test_grid_wraps_hosts_with_many_osds(self) -> None:
        cells = [charts.Cell(f"osd.{i}", float(i)) for i in range(30)]
        one = decode(charts.osd_grid("t", "s", [("big", cells[:12])], "%", (0, 100)))
        three = decode(charts.osd_grid("t", "s", [("big", cells)], "%", (0, 100)))
        assert three.size[0] == one.size[0] and three.size[1] > one.size[1]


def picture(content: list[Any]) -> tuple[Image.Image, dict[str, Any]]:
    images = [c for c in content if c.type == "image"]
    texts = [c for c in content if c.type == "text"]
    assert len(images) == 1 and images[0].mime_type == "image/png" and len(texts) == 1
    return decode(base64.b64decode(images[0].data)), json.loads(texts[0].text)


@pytest.mark.anyio
class TestTool:
    async def test_history(self, harness: Any, clock: Any) -> None:
        async with harness() as h:
            store = await h.app.history.store()
            for minute in range(120):
                slow = 80.0 if minute >= 90 else 4.0
                store.write(
                    clock.now - (120 - minute) * 60,
                    {
                        "cluster/op_w_latency_ms": slow,
                        "cluster/write_bytes_sec": 2e6,
                        **{f"osd/{i}/commit_ms": 3.0 + (slow if i == 4 else 0) for i in range(8)},
                    },
                )
            image, summary = picture(
                await h.content("chart", series="osd/*/commit_ms, cluster/*", since="-2h")
            )
            assert image.size[0] == 1760 and summary["chart"].startswith("lines over time")
            names = [row[0] for row in summary["series"]["rows"]]
            assert names[:2] == ["cluster/op_w_latency_ms", "cluster/write_bytes_sec"]
            latency = dict(
                zip(summary["series"]["columns"], summary["series"]["rows"][0], strict=True)
            )
            assert latency["last"] == "80 ms" and latency["shift"].startswith(
                "rose from 4 ms to 80 ms"
            )
            assert rgb(charts.LIGHT.other) in colours(image)  # eight OSDs: most are context
            dark, _ = picture(
                await h.content("chart", series="cluster/*", since="-2h", theme="dark")
            )
            assert rgb(charts.DARK.surface) in colours(dark)

    async def test_history_errors(self, harness: Any) -> None:
        async with harness() as h:
            assert "nothing recorded matches" in await h.error("chart", series="osd/*/commit_ms")
            assert "is not a series name" in await h.error("chart", series="a b; c")
            assert "series is empty" in await h.error("chart", series=" , ")

    async def test_osd_grid(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            image, summary = picture(await h.content("chart", kind="osds"))
            assert summary["by_host"]["cephmcp-1"] == {"osd.1": 10.3, "osd.2": 5.4}
            assert "flagged" not in summary and image.size[0] >= 1120
            perf = cli.load("osd perf")
            for info in perf["osdstats"]["osd_perf_infos"]:
                info["perf_stats"]["commit_latency_ms"] = 240 if info["id"] == 3 else 6
            cli.set_json("osd perf", perf)
            slow, summary = picture(await h.content("chart", kind="osds", metric="commit_ms"))
            assert summary["flagged"] == ["osd.3"] and summary["median"] == 6
            assert rgb(charts.LIGHT.warning) in colours(slow)

    async def test_down_osds_show_as_down(self, harness: Any, cli: Any) -> None:
        cli.use("microceph")
        tree = cli.load("osd df tree")
        for node in tree["nodes"]:
            if node["id"] == 5:
                node["status"] = "down"
        cli.set_json("osd df tree", tree)
        async with harness() as h:
            _, summary = picture(await h.content("chart", kind="osds", metric="pgs"))
        assert summary["by_host"]["cephmcp-3"]["osd.5 down"] == "down"

    async def test_capacity(self, harness: Any, cli: Any, clock: Any) -> None:
        cli.use("microceph")
        async with harness() as h:
            assert "not enough history" in await h.error("chart", kind="capacity")
            store = await h.app.history.store()
            used = cli.load("df detail")["stats"]["total_used_raw_bytes"]
            for hour in range(241):
                when = clock.now - (240 - hour) * HOUR
                store.write(when, {"cluster/used_bytes": used - (240 - hour) * 2**30 / 24})
            image, summary = picture(await h.content("chart", kind="capacity"))
        assert (
            summary["growth_per_day"] == "1 GiB" and summary["basis"] == "10d of recorded history"
        )
        assert list(summary["limits_reached"]) == [
            "nearfull (85%)",
            "backfillfull (90%)",
            "full (95%)",
        ]
        assert has_hue(image, charts.LIGHT.warning)

    async def test_without_pillow_there_is_no_chart_tool(
        self, harness: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("cephmcp.server.charts_available", lambda: False)
        async with harness() as h:
            assert "chart" not in await h.tools()
