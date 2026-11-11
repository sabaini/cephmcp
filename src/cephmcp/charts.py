"""Charts as PNG images, drawn with Pillow (an optional dependency).

Three forms, chosen by what the reader has to do:

- lines over time, one panel per unit of measurement (never two scales on one
  plot); with more than four series in a panel, the few that stand out are
  drawn in colour and the rest recede into grey;
- a grid of OSDs by host, shaded by one hue from light to dark, for "where is
  it full" and "which disk is slow";
- usage over time with its projection and the limits it is heading for.

Colours are a fixed, colour-blind-checked palette; text is never drawn in a
series colour; every chart comes with its figures as text beside it.
"""

from __future__ import annotations

import io
import itertools
import math
import time
from dataclasses import dataclass, field
from typing import Any

from PIL import Image, ImageDraw, ImageFont

SCALE = 2
"""Output pixels per layout unit: sharp on dense screens."""
SUPER = 2
"""Extra resolution while drawing, averaged away on output, for smooth lines."""
K = SCALE * SUPER


@dataclass(frozen=True)
class Theme:
    surface: str
    ink: str
    secondary: str
    muted: str
    grid: str
    axis: str
    series: tuple[str, ...]
    """Categorical hues, used in this order and never cycled."""
    other: str
    """De-emphasis grey for series that are context."""
    ramp: tuple[str, ...]
    """One hue from "little" to "much"."""
    warning: str


_BLUES = (
    "#cde2fb",
    "#b7d3f6",
    "#9ec5f4",
    "#86b6ef",
    "#6da7ec",
    "#5598e7",
    "#3987e5",
    "#2a78d6",
    "#256abf",
    "#1c5cab",
    "#184f95",
    "#104281",
    "#0d366b",
)
LIGHT = Theme(
    surface="#fcfcfb",
    ink="#0b0b0b",
    secondary="#52514e",
    muted="#898781",
    grid="#e1e0d9",
    axis="#c3c2b7",
    series=("#2a78d6", "#eb6834", "#1baf7a", "#eda100"),
    other="#c3c2b7",
    ramp=_BLUES,
    warning="#d03b3b",
)
DARK = Theme(
    surface="#1a1a19",
    ink="#f0efec",
    secondary="#c3c2b7",
    muted="#898781",
    grid="#2c2c2a",
    axis="#383835",
    series=("#3987e5", "#d95926", "#199e70", "#c98500"),
    other="#52514e",
    # On a dark surface "little" recedes into the dark end of the hue.
    ramp=tuple(reversed(_BLUES)),
    warning="#d03b3b",
)
THEMES = {"light": LIGHT, "dark": DARK}
MAX_COLOURED = len(LIGHT.series)


# ------------------------------------------------------------------ figures

_IEC = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB")


def unit_of(series: str) -> str:
    """The unit a series name implies, as a key for formatting and grouping."""
    for suffix, unit in (
        ("_bytes_sec", "bytes/s"),
        ("_bytes", "bytes"),
        ("_pct", "%"),
        ("_ops_sec", "/s"),
        ("_per_sec", "/s"),
    ):
        if series.endswith(suffix):
            return unit
    if series.endswith("_ms") or "_ms_" in series:
        return "ms"
    return ""


def _trim(value: float, digits: int = 3) -> str:
    text = f"{value:.{digits}g}"
    return text if "e" not in text else f"{value:.0f}"


def fmt(value: float, unit: str) -> str:
    if unit in ("bytes", "bytes/s"):
        n, i = abs(float(value)), 0
        while n >= 1024 and i < len(_IEC) - 1:
            n /= 1024
            i += 1
        text = f"{'-' if value < 0 else ''}{_trim(n)} {_IEC[i]}"
        return text + "/s" if unit == "bytes/s" else text
    if unit == "%":
        return f"{_trim(value)}%"
    if unit == "ms":
        return f"{_trim(value)} ms"
    if unit == "/s":
        return f"{_trim(value)}/s"
    return _trim(value)


def nice_ticks(top: float, unit: str, count: int = 4) -> list[float]:
    """Round tick values from zero to at least `top`."""
    if top <= 0:
        return [0.0, 1.0]
    base = 1.0
    if unit in ("bytes", "bytes/s"):
        # Round in the binary unit the labels will use.
        while top / base >= 1024:
            base *= 1024
    raw = top / base / count
    power = 10 ** math.floor(math.log10(raw))
    step = next(m for m in (1, 2, 2.5, 5, 10) if m * power >= raw) * power * base
    # Rounded, so that 3 * 0.2 is 0.6 and not 0.6000000000000001.
    ticks = [round(i * step, 10) for i in range(math.ceil(top / step - 1e-9) + 1)]
    return ticks if len(ticks) > 1 else [0.0, step]


_TIME_STEPS = (
    60,
    120,
    300,
    600,
    900,
    1800,
    3600,
    7200,
    3 * 3600,
    6 * 3600,
    12 * 3600,
    86400,
    2 * 86400,
    7 * 86400,
    14 * 86400,
    30 * 86400,
    90 * 86400,
)


def time_ticks(start: float, end: float, most: int = 7) -> tuple[list[float], str]:
    """Tick times on round boundaries (UTC), and the strftime format for them."""
    span = max(end - start, 1.0)
    step = next((s for s in _TIME_STEPS if span / s <= most), _TIME_STEPS[-1])
    first = math.ceil(start / step) * step
    ticks = [first + i * step for i in range(int((end - first) // step) + 1)]
    if step >= 86400:
        pattern = "%b %d"
    elif span > 86400:
        pattern = "%b %d %H:%M"
    else:
        pattern = "%H:%M"
    return ticks, pattern


def clock(ts: float, pattern: str = "%Y-%m-%d %H:%M") -> str:
    return time.strftime(pattern, time.gmtime(ts))


# ------------------------------------------------------------------- canvas


def _rgb(colour: str) -> tuple[int, int, int]:
    colour = colour.lstrip("#")
    return int(colour[0:2], 16), int(colour[2:4], 16), int(colour[4:6], 16)


def luminance(colour: str) -> float:
    def channel(c: int) -> float:
        v = c / 255
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4

    r, g, b = (channel(c) for c in _rgb(colour))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


class Canvas:
    """Drawing in layout units, with the scaling to pixels in one place."""

    def __init__(self, width: float, height: float, theme: Theme):
        self.width, self.height, self.theme = width, height, theme
        self.image = Image.new("RGB", (round(width * K), round(height * K)), theme.surface)
        self.draw = ImageDraw.Draw(self.image)
        self._fonts: dict[float, Any] = {}

    def font(self, size: float) -> Any:
        if size not in self._fonts:
            self._fonts[size] = ImageFont.load_default(size=size * K)
        return self._fonts[size]

    def text_width(self, text: str, size: float) -> float:
        return float(self.draw.textlength(text, font=self.font(size))) / K

    def text(
        self, x: float, y: float, text: str, size: float, colour: str, anchor: str = "ls"
    ) -> None:
        """anchor: Pillow's two letters, e.g. 'ls' left/baseline, 'rm' right/middle."""
        self.draw.text((x * K, y * K), text, font=self.font(size), fill=colour, anchor=anchor)

    def line(self, points: list[tuple[float, float]], colour: str, width: float = 1.0) -> None:
        if len(points) >= 2:
            self.draw.line(
                [(x * K, y * K) for x, y in points],
                fill=colour,
                width=max(1, round(width * K)),
                joint="curve",
            )
            if width >= 1.5:
                # Round caps: Pillow ends lines square.
                r = width * K / 2
                for x, y in (points[0], points[-1]):
                    self.draw.ellipse((x * K - r, y * K - r, x * K + r, y * K + r), fill=colour)

    def dashes(
        self, points: list[tuple[float, float]], colour: str, width: float, on: float, off: float
    ) -> None:
        """A dashed line along a path: reads as "projected", not measured."""
        carry, drawing = 0.0, True
        for (x0, y0), (x1, y1) in itertools.pairwise(points):
            length = math.hypot(x1 - x0, y1 - y0)
            at = 0.0
            while at < length:
                left = (on if drawing else off) - carry
                step = min(left, length - at)
                if drawing:
                    a, b = at / length, (at + step) / length
                    self.line(
                        [
                            (x0 + (x1 - x0) * a, y0 + (y1 - y0) * a),
                            (x0 + (x1 - x0) * b, y0 + (y1 - y0) * b),
                        ],
                        colour,
                        width,
                    )
                at += step
                carry += step
                if carry >= (on if drawing else off) - 1e-9:
                    carry, drawing = 0.0, not drawing

    def rect(
        self, x0: float, y0: float, x1: float, y1: float, colour: str, radius: float = 0
    ) -> None:
        box = (x0 * K, y0 * K, x1 * K, y1 * K)
        if radius:
            self.draw.rounded_rectangle(box, radius=radius * K, fill=colour)
        else:
            self.draw.rectangle(box, fill=colour)

    def dot(self, x: float, y: float, radius: float, colour: str) -> None:
        """A marker with a ring in the surface colour, so it stays legible on a line."""
        for r, fill in ((radius + 2, self.theme.surface), (radius, colour)):
            self.draw.ellipse(((x - r) * K, (y - r) * K, (x + r) * K, (y + r) * K), fill=fill)

    def png(self) -> bytes:
        size = (round(self.width * SCALE), round(self.height * SCALE))
        out = io.BytesIO()
        self.image.resize(size, Image.Resampling.LANCZOS).save(out, format="PNG", optimize=True)
        return out.getvalue()


# -------------------------------------------------------------- line charts


@dataclass
class Line:
    name: str
    points: list[tuple[float, float]]
    entity: str = ""
    """What the line belongs to; one entity keeps one colour across panels."""
    context: bool = False
    """Drawn in grey behind the others."""
    projected: bool = False


@dataclass
class Mark:
    value: float
    label: str


@dataclass
class Panel:
    title: str
    unit: str
    lines: list[Line]
    marks: list[Mark] = field(default_factory=list)
    note: str = ""


def emphasise(lines: list[Line], keep: int = 3) -> int:
    """With more lines than there are colours, keep the ones that stand out
    furthest from the group and turn the others into context. Returns how
    many became context."""
    if len(lines) <= MAX_COLOURED:
        return 0
    peaks = sorted(max((v for _, v in ln.points), default=0.0) for ln in lines)
    middle = peaks[len(peaks) // 2]

    def distance(line: Line) -> float:
        return abs(max((v for _, v in line.points), default=0.0) - middle)

    standing_out = {id(ln) for ln in sorted(lines, key=distance, reverse=True)[:keep]}
    for line in lines:
        line.context = id(line) not in standing_out
    return len(lines) - keep


def spread(positions: list[float], least: float) -> list[float]:
    """Move positions apart until neighbours are at least `least` from each
    other, keeping their order and, as far as possible, their centre."""
    if len(positions) < 2:
        return list(positions)
    order = sorted(range(len(positions)), key=lambda i: positions[i])
    placed = [positions[i] for i in order]
    for i in range(1, len(placed)):
        placed[i] = max(placed[i], placed[i - 1] + least)
    shift = (sum(placed) - sum(positions[i] for i in order)) / len(placed)
    out = [0.0] * len(positions)
    for rank, i in enumerate(order):
        out[i] = placed[rank] - shift
    return out


def _segments(points: list[tuple[float, float]], gap: float) -> list[list[tuple[float, float]]]:
    """Split a series where samples are missing, so that gaps stay gaps."""
    out: list[list[tuple[float, float]]] = []
    for point in points:
        if out and point[0] - out[-1][-1][0] <= gap:
            out[-1].append(point)
        else:
            out.append([point])
    return out


def line_chart(
    title: str,
    subtitle: str,
    panels: list[Panel],
    start: float,
    end: float,
    theme: Theme = LIGHT,
    *,
    now: float | None = None,
    width: float = 880,
) -> bytes:
    """One panel per unit, stacked on a shared time axis."""
    left, right, bottom = 68.0, 150.0, 34.0
    panel_height, gap = 190.0, 34.0
    entities: list[str] = []
    for panel in panels:
        for line in panel.lines:
            if not line.context and (line.entity or line.name) not in entities:
                entities.append(line.entity or line.name)
    entities = entities[:MAX_COLOURED]
    colours = {entity: theme.series[i] for i, entity in enumerate(entities)}
    legend = len(entities) > 1 or any(ln.context for p in panels for ln in p.lines)
    # Title, subtitle, then the legend row if there is one, then the first panel's title.
    top = 106.0 if legend else 84.0
    height = top + len(panels) * panel_height + (len(panels) - 1) * gap + bottom
    c = Canvas(width, height, theme)
    plot_width = width - left - right

    def x_at(t: float) -> float:
        return left + plot_width * (t - start) / max(end - start, 1e-9)

    c.text(left - 44, 30, title, 17, theme.ink)
    c.text(left - 44, 50, subtitle, 12, theme.secondary)
    if legend:
        x = left - 44
        for entity in entities:
            c.line([(x, 70), (x + 16, 70)], colours[entity], 2)
            c.text(x + 22, 74, entity, 12, theme.secondary)
            x += 22 + c.text_width(entity, 12) + 18
        if any(
            ln.context or (ln.entity or ln.name) not in colours for p in panels for ln in p.lines
        ):
            c.line([(x, 70), (x + 16, 70)], theme.other, 1.5)
            c.text(x + 22, 74, "the others", 12, theme.secondary)

    ticks, pattern = time_ticks(start, end)
    for index, panel in enumerate(panels):
        y0 = top + index * (panel_height + gap)
        y1 = y0 + panel_height
        values = [v for ln in panel.lines for _, v in ln.points] + [m.value for m in panel.marks]
        y_ticks = nice_ticks(max(values, default=1.0) * 1.05, panel.unit)
        peak = y_ticks[-1]

        def y_at(v: float, y1: float = y1, peak: float = peak) -> float:
            return y1 - panel_height * max(0.0, v) / peak

        c.text(left, y0 - 9, panel.title, 12, theme.secondary)
        if panel.note:
            c.text(left + plot_width, y0 - 9, panel.note, 11, theme.muted, anchor="rs")
        for tick in y_ticks:
            y = y_at(tick)
            c.line([(left, y), (left + plot_width, y)], theme.axis if tick == 0 else theme.grid, 1)
            c.text(left - 8, y, fmt(tick, panel.unit), 11, theme.muted, anchor="rm")
        for t in ticks:
            c.line([(x_at(t), y1), (x_at(t), y1 + 4)], theme.axis, 1)
            if index == len(panels) - 1:
                c.text(x_at(t), y1 + 18, clock(t, pattern), 11, theme.muted, anchor="ms")
        if now is not None and start < now < end:
            c.line([(x_at(now), y0), (x_at(now), y1)], theme.axis, 1)
            if index == 0:
                c.text(x_at(now) + 5, y1 - 7, "now", 11, theme.muted)
        # Limits that lie close together keep their lines but get their labels spread.
        mark_ys = spread([y_at(m.value) for m in panel.marks], 13.0)
        for mark, label_y in zip(panel.marks, mark_ys, strict=True):
            y = y_at(mark.value)
            c.line([(left, y), (left + plot_width, y)], theme.warning, 1)
            c.text(left + plot_width + 8, label_y, mark.label, 11, theme.secondary, anchor="lm")

        steps = sorted(b[0] - a[0] for ln in panel.lines for a, b in itertools.pairwise(ln.points))
        gap_limit = 3.5 * steps[len(steps) // 2] if steps else float("inf")
        # Context first, so that the coloured lines lie on top.
        ordered = sorted(
            panel.lines, key=lambda ln: not (ln.context or (ln.entity or ln.name) not in colours)
        )
        labels: list[tuple[float, str, str]] = []
        for line in ordered:
            entity = line.entity or line.name
            # A line without a colour of its own (more entities than hues) is context too.
            context = line.context or entity not in colours
            colour = theme.other if context else colours[entity]
            if line.projected:
                c.dashes([(x_at(t), y_at(v)) for t, v in line.points], colour, 2, 6, 5)
            else:
                for segment in _segments(line.points, gap_limit):
                    path = [(x_at(t), y_at(v)) for t, v in segment]
                    c.line(path, colour, 1.5 if context else 2)
            if line.points and not context:
                t, v = line.points[-1]
                if not line.projected:
                    c.dot(x_at(t), y_at(v), 4, colour)
                name = "" if len(entities) < 2 else f"{entity}  "
                labels.append((y_at(v), f"{name}{fmt(v, panel.unit)}", colour))
        # Labels at the line ends, where they do not run into each other or a limit.
        taken = list(mark_ys)
        for y, text, _ in sorted(labels, key=lambda item: item[0]):
            if all(abs(y - other) >= 13 for other in taken):
                c.text(left + plot_width + 10, y, text, 11.5, theme.ink, anchor="lm")
                taken.append(y)
    return c.png()


# ---------------------------------------------------------------- OSD grid


@dataclass
class Cell:
    label: str
    value: float | None
    flagged: bool = False


def osd_grid(
    title: str,
    subtitle: str,
    rows: list[tuple[str, list[Cell]]],
    unit: str,
    domain: tuple[float, float],
    theme: Theme = LIGHT,
    *,
    flag_note: str = "",
    per_line: int = 12,
) -> bytes:
    """Hosts down, their OSDs across, each cell shaded by its value."""
    cell_w, cell_h, gap = 58.0, 40.0, 2.0
    left = 44.0 + max((_label_width(name) for name, _ in rows), default=40.0)
    top, right = 74.0, 30.0
    bottom = 78.0 if flag_note else 58.0
    lines = [max(1, math.ceil(len(cells) / per_line)) for _, cells in rows]
    widest = max((min(len(cells), per_line) for _, cells in rows), default=1)
    width = max(left + widest * (cell_w + gap) + right, left + 420.0, 560.0)
    height = top + sum(n * (cell_h + gap) for n in lines) + (len(rows) - 1) * 8 + bottom
    c = Canvas(width, height, theme)
    c.text(22, 30, title, 17, theme.ink)
    c.text(22, 50, subtitle, 12, theme.secondary)
    low, high = domain
    span = max(high - low, 1e-9)

    def shade(value: float) -> str:
        position = min(1.0, max(0.0, (value - low) / span))
        return theme.ramp[round(position * (len(theme.ramp) - 1))]

    y = top
    for (name, cells), count in zip(rows, lines, strict=True):
        c.text(left - 12, y + cell_h / 2, name, 12, theme.secondary, anchor="rm")
        for i, cell in enumerate(cells):
            x0 = left + (i % per_line) * (cell_w + gap)
            y0 = y + (i // per_line) * (cell_h + gap)
            fill = theme.grid if cell.value is None else shade(cell.value)
            c.rect(x0, y0, x0 + cell_w, y0 + cell_h, fill, radius=4)
            inside = "#ffffff" if luminance(fill) < 0.36 else "#0b0b0b"
            c.text(x0 + 7, y0 + 14, cell.label, 10, inside)
            shown = "-" if cell.value is None else fmt(cell.value, unit)
            c.text(x0 + 7, y0 + 31, shown, 12.5, inside)
            if cell.flagged:
                # A status mark: shape and legend text, not colour alone.
                c.dot(x0 + cell_w - 9, y0 + 10, 4, theme.warning)
        y += count * (cell_h + gap) + 8

    # The scale: what light and dark mean.
    scale_y = height - (50 if flag_note else 30)
    block = 16.0
    c.text(left, scale_y + 12, fmt(low, unit), 11, theme.muted, anchor="rs")
    for i, colour in enumerate(theme.ramp):
        c.rect(left + 8 + i * block, scale_y, left + 8 + (i + 1) * block - 1, scale_y + 12, colour)
    end = left + 8 + len(theme.ramp) * block
    c.text(end + 8, scale_y + 12, fmt(high, unit), 11, theme.muted)
    if flag_note:
        c.dot(left + 12, scale_y + 30, 4, theme.warning)
        c.text(left + 24, scale_y + 34, flag_note, 11, theme.secondary)
    return c.png()


def _label_width(text: str) -> float:
    return 7.0 * len(text)
