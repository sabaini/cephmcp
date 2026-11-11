"""Small, dependency-free statistics for the recorded history: level shifts,
trends and outliers among peers. Deliberately simple and explainable; every
result names the numbers it rests on."""

from __future__ import annotations

import itertools
import math
import statistics
from collections.abc import Sequence
from dataclasses import dataclass

Point = tuple[float, float, float, float]
"""(time, average, minimum, maximum)."""


@dataclass(frozen=True)
class Shift:
    """A lasting change of level in a series."""

    at: float
    before: float
    after: float
    score: float

    @property
    def change(self) -> float:
        return self.after - self.before

    @property
    def ratio(self) -> float | None:
        return self.after / self.before if self.before else None


@dataclass(frozen=True)
class Trend:
    slope: float
    """Change per second."""
    intercept: float
    r2: float
    span: float
    """Seconds between the first and the last point."""
    points: int

    def at(self, t: float) -> float:
        return self.intercept + self.slope * t

    def reaches(self, level: float, now: float, current: float | None = None) -> float | None:
        """When the trend reaches `level`, as a time; None if it never does.
        Starts from `current` if given, which is more recent than the fit."""
        if self.slope <= 0:
            return None
        value = self.at(now) if current is None else current
        if value >= level:
            return now
        return now + (level - value) / self.slope


def noise(values: Sequence[float]) -> float:
    """Typical point-to-point variation, estimated from successive
    differences so that a shift in level does not count as noise."""
    if len(values) < 3:
        return 0.0
    diffs = [abs(b - a) for a, b in itertools.pairwise(values)]
    return 1.4826 * statistics.median(diffs) / math.sqrt(2)


def find_shift(
    points: Sequence[tuple[float, float]],
    *,
    min_score: float = 5.0,
    min_relative: float = 0.2,
    min_absolute: float = 0.0,
) -> Shift | None:
    """The single most significant change of level in (time, value) points,
    if there is one worth reporting.

    Tries every split of the series into "before" and "after" and scores the
    difference of the two averages against the noise. A shift counts when it
    stands out from the noise (`min_score`), amounts to at least
    `min_relative` of the larger level, and exceeds `min_absolute`.
    """
    n = len(points)
    if n < 8:
        return None
    values = [v for _, v in points]
    margin = max(3, n // 20)
    sigma = noise(values)
    scale = max(abs(v) for v in values)
    if scale == 0:
        return None
    sigma = max(sigma, 1e-6 * scale)
    prefix = [0.0]
    for v in values:
        prefix.append(prefix[-1] + v)
    total = prefix[-1]
    best: tuple[float, int, float, float] | None = None
    for i in range(margin, n - margin + 1):
        left = prefix[i] / i
        right = (total - prefix[i]) / (n - i)
        score = abs(right - left) * math.sqrt(i * (n - i) / n) / sigma
        if best is None or score > best[0]:
            best = (score, i, left, right)
    if best is None:
        return None
    score, i, left, right = best
    delta = abs(right - left)
    if score < min_score or delta <= min_absolute:
        return None
    if delta < min_relative * max(abs(left), abs(right)):
        return None
    return Shift(at=points[i][0], before=left, after=right, score=score)


def fit(points: Sequence[tuple[float, float]]) -> Trend | None:
    """Least-squares line through (time, value) points."""
    n = len(points)
    if n < 2:
        return None
    t0 = points[0][0]
    xs = [t - t0 for t, _ in points]
    ys = [v for _, v in points]
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    if sxx == 0:
        return None
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True))
    syy = sum((y - mean_y) ** 2 for y in ys)
    slope = sxy / sxx
    r2 = (sxy * sxy) / (sxx * syy) if syy > 0 else 1.0
    return Trend(
        slope=slope,
        intercept=mean_y - slope * mean_x - slope * t0,
        r2=r2,
        span=xs[-1] - xs[0],
        points=n,
    )


def buckets(
    points: Sequence[Point], start: float, end: float, count: int
) -> list[tuple[float, float | None, float | None, float | None]]:
    """Resample to `count` equal intervals: (start of interval, average,
    minimum, maximum), with None where an interval has no data."""
    width = (end - start) / count
    sums = [0.0] * count
    hits = [0] * count
    lows: list[float | None] = [None] * count
    highs: list[float | None] = [None] * count
    for t, avg, low, high in points:
        i = int((t - start) / width) if width > 0 else 0
        if i == count and t <= end:
            i = count - 1
        if not 0 <= i < count:
            continue
        sums[i] += avg
        hits[i] += 1
        lows[i] = low if lows[i] is None else min(lows[i], low)  # type: ignore[type-var]
        highs[i] = high if highs[i] is None else max(highs[i], high)  # type: ignore[type-var]
    return [
        (start + i * width, sums[i] / hits[i] if hits[i] else None, lows[i], highs[i])
        for i in range(count)
    ]


@dataclass(frozen=True)
class Outlier:
    name: str
    value: float
    peers: float
    """Median of the group."""
    ratio: float | None
    earlier_ratio: float | None
    """The same ratio in the first half of the window, when history allows."""

    @property
    def drifting(self) -> bool:
        return (
            self.ratio is not None
            and self.earlier_ratio is not None
            and self.ratio >= 1.5
            and self.ratio >= 1.5 * self.earlier_ratio
        )


def typical(points: Sequence[Point]) -> float | None:
    """What a series is like when it matters: its upper quartile.

    Under load that comes and goes, most samples of a latency are taken while
    nothing happens, and a median would call a disk fine that is slow every
    time it is used. A single spike, on the other hand, does not move the
    upper quartile."""
    if not points:
        return None
    values = [p[1] for p in points]
    return statistics.quantiles(values, n=4)[2] if len(values) >= 4 else max(values)


def outliers(
    series: dict[str, Sequence[Point]],
    *,
    min_ratio: float = 2.0,
    min_excess: float = 0.0,
    floor: float = 0.0,
) -> tuple[list[Outlier], float | None]:
    """Members of a group that sit well above the group's median.

    A member is reported when its typical value (see `typical`) is at least
    `min_ratio` times the median of the group and at least `min_excess` above it, or when it has
    drifted: its ratio to the group grew by half between the first and the
    second half of the window. `floor` is the smallest median to divide by, so
    that a group idling near zero does not make everyone an outlier.
    """
    levels = {name: m for name, pts in series.items() if (m := typical(pts)) is not None}
    if len(levels) < 3:
        return [], None
    middle = statistics.median(levels.values())
    base = max(middle, floor)

    def halves(name: str) -> tuple[float | None, float | None]:
        pts = series[name]
        if len(pts) < 8:
            return None, None
        half = len(pts) // 2
        return typical(pts[:half]), typical(pts[half:])

    first_halves = {n: halves(n)[0] for n in levels}
    early = [v for v in first_halves.values() if v is not None]
    early_base = max(statistics.median(early), floor) if len(early) >= 3 else None
    found = []
    for name, value in levels.items():
        ratio = value / base if base > 0 else None
        _, late = halves(name)
        late_ratio = late / base if late is not None and base > 0 else None
        first = first_halves[name]
        earlier = first / early_base if first is not None and early_base else None
        high = ratio is not None and ratio >= min_ratio and value - middle >= min_excess
        recent = Outlier(name, late if late is not None else value, middle, late_ratio, earlier)
        if recent.drifting and recent.value - middle >= min_excess:
            # Show where it stands now, not the average over its way there.
            found.append(recent)
        elif high:
            found.append(Outlier(name, value, middle, ratio, earlier))
    found.sort(key=lambda o: -(o.ratio or 0))
    return found, middle
