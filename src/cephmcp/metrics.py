"""Metric sources: the mgr prometheus module and ceph-exporter daemons.

Both expose the Prometheus text format over HTTP. The mgr module carries
cluster-wide state (health, pools, PGs, OSD state); ceph-exporter, one per
host, carries the per-daemon performance counters. Where no ceph-exporter
runs (MicroCeph), the mgr module can export those counters too.
"""

from __future__ import annotations

import contextlib
import ipaddress
import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit

import anyio

from . import promtext
from .ceph import CephClient, gather
from .config import Settings
from .http import Http, HttpError
from .promtext import Metric, Sample

MGR = "mgr-prometheus"
EXPORTER = "ceph-exporter"

DISCOVERY_TTL = 300.0
SCRAPE_TTL = 2.0
PROBE_TIMEOUT = 2.0
MAX_PROBE_HOSTS = 256


@dataclass(frozen=True)
class Source:
    kind: str
    url: str
    host: str = ""
    found_by: str = ""


@dataclass
class Scrape:
    source: Source
    time: float
    metrics: dict[str, Metric] = field(default_factory=dict)
    error: str = ""

    @property
    def series(self) -> int:
        return sum(len(m.samples) for m in self.metrics.values())


@dataclass
class Discovery:
    sources: list[Source] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    prometheus_url: str | None = None
    mgr_scrape_interval: float = 15.0
    when: float = 0.0

    def kinds(self) -> set[str]:
        return {s.kind for s in self.sources}


Labels = tuple[tuple[str, str], ...]


@dataclass
class Collection:
    """Metrics merged from all sources at one point in time."""

    time: float
    metrics: dict[str, Metric]
    scrapes: list[Scrape]

    @property
    def errors(self) -> list[str]:
        return [s.error for s in self.scrapes if s.error]

    def kinds_of(self, name: str) -> set[str]:
        """Which kinds of source provide this metric."""
        return {s.source.kind for s in self.scrapes if name in s.metrics}

    def enricher(self) -> Enricher:
        return Enricher(self.metrics)


class Enricher:
    """Adds the labels people join on in PromQL: a pool's name next to its
    pool_id, and an OSD's host and device class next to its ceph_daemon."""

    def __init__(self, metrics: Mapping[str, Metric]):
        pools = metrics.get("ceph_pool_metadata")
        osds = metrics.get("ceph_osd_metadata")
        self.pools = {s.label("pool_id"): s.label("name") for s in pools.samples} if pools else {}
        self.osds = (
            {
                s.label("ceph_daemon"): (s.label("hostname"), s.label("device_class"))
                for s in osds.samples
            }
            if osds
            else {}
        )

    def __call__(self, labels: Labels) -> Labels:
        if not self.pools and not self.osds:
            return labels
        have = dict(labels)
        extra: list[tuple[str, str]] = []
        pool = self.pools.get(have.get("pool_id", ""))
        if pool and "pool" not in have and "name" not in have:
            extra.append(("pool", pool))
        osd = self.osds.get(have.get("ceph_daemon", ""))
        if osd:
            if osd[0] and "hostname" not in have:
                extra.append(("hostname", osd[0]))
            if osd[1] and "device_class" not in have:
                extra.append(("device_class", osd[1]))
        return labels + tuple(extra) if extra else labels


def metrics_url(url: str) -> str:
    """Normalise an endpoint to its /metrics URL."""
    if "://" not in url:
        url = "http://" + url
    parts = urlsplit(url)
    path = parts.path.rstrip("/")
    if not path.endswith("/metrics"):
        path += "/metrics"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def addr_host(addr: str) -> str | None:
    """'10.0.0.1:6800/123' or '[fd00::1]:6789/0' -> the IP address."""
    addr = addr.strip()
    if not addr:
        return None
    host = addr[1:].split("]", 1)[0] if addr.startswith("[") else addr.split(":", 1)[0]
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return None
    return host


def _url_for(host: str, port: int) -> str:
    return f"http://[{host}]:{port}/metrics" if ":" in host else f"http://{host}:{port}/metrics"


class MetricsHub:
    def __init__(
        self,
        settings: Settings,
        ceph: CephClient,
        http: Http,
        clock: Callable[[], float] = time.time,
    ):
        self._settings = settings
        self._ceph = ceph
        self._http = http
        self._clock = clock
        self._discovery: Discovery | None = None
        self._scrapes: dict[str, Scrape] = {}

    def now(self) -> float:
        return self._clock()

    # ---------------------------------------------------------------- discovery

    async def discover(self, refresh: bool = False) -> Discovery:
        cached = self._discovery
        if cached and not refresh and self._clock() - cached.when < DISCOVERY_TTL:
            return cached
        found = Discovery(when=self._clock())
        await gather(
            lambda: self._find_mgr(found),
            lambda: self._find_exporters(found),
            lambda: self._find_prometheus(found),
        )
        await self._check_perf_counters(found)
        self._discovery = found
        return found

    async def _find_mgr(self, found: Discovery) -> None:
        s = self._settings
        if s.mgr_prometheus_url:
            found.sources.append(Source(MGR, metrics_url(s.mgr_prometheus_url), "", "configured"))
            return
        if not s.discover_exporters:
            return
        services = await self._ceph.try_json(["mgr", "services"], {}) or {}
        url = services.get("prometheus")
        if url:
            found.sources.append(
                Source(MGR, metrics_url(url), urlsplit(url).hostname or "", "ceph mgr services")
            )
            interval = await self._ceph.try_json(
                ["config", "get", "mgr", "mgr/prometheus/scrape_interval"]
            )
            with contextlib.suppress(TypeError, ValueError):
                found.mgr_scrape_interval = float(interval)
            return
        modules = await self._ceph.try_json(["mgr", "module", "ls"], {}) or {}
        if "prometheus" in modules.get("enabled_modules", []):
            found.notes.append(
                "The mgr prometheus module is enabled but not serving yet; "
                "the active mgr may still be starting."
            )
        elif modules:
            found.notes.append(
                "The mgr prometheus module is disabled, so cluster metrics are not exported. "
                "Enable it with `ceph mgr module enable prometheus` "
                "(ceph_command in admin mode), then call metrics_sources with refresh=true."
            )

    async def _find_exporters(self, found: Discovery) -> None:
        s = self._settings
        if s.ceph_exporter_urls:
            for url in s.ceph_exporter_urls:
                url = metrics_url(url)
                found.sources.append(
                    Source(EXPORTER, url, urlsplit(url).hostname or "", "configured")
                )
            return
        if not s.discover_exporters:
            return
        endpoints = await self._orch_endpoints("ceph-exporter", s.ceph_exporter_port)
        if endpoints is not None:
            found.sources.extend(
                Source(EXPORTER, _url_for(address, port), name, "orchestrator")
                for address, name, port in endpoints
            )
            return
        # No orchestrator (MicroCeph, package installs): probe the cluster's hosts.
        hosts = await self._cluster_hosts()
        candidates = [
            Source(EXPORTER, _url_for(ip, s.ceph_exporter_port), name, "probe")
            for ip, name in list(hosts.items())[:MAX_PROBE_HOSTS]
        ]
        alive = await gather(*[lambda c=c: self._probe(c) for c in candidates])
        found.sources.extend(c for c, ok in zip(candidates, alive, strict=True) if ok)

    async def _cluster_hosts(self) -> dict[str, str]:
        """IP -> hostname of every host running a mon or an OSD."""
        osds, mons = await gather(
            lambda: self._ceph.try_json(["osd", "metadata"], []),
            lambda: self._ceph.try_json(["mon", "metadata"], []),
        )
        hosts: dict[str, str] = {}
        for daemon in [*(mons or []), *(osds or [])]:
            name = daemon.get("hostname", "")
            for key in ("front_addr", "addrs"):
                for addr in re.findall(r"v[12]:([^,\]\s]+(?:\][^,\s]*)?)", daemon.get(key, "")) or [
                    daemon.get(key, "")
                ]:
                    ip = addr_host(addr)
                    if ip:
                        hosts.setdefault(ip, name)
        return hosts

    async def _probe(self, source: Source) -> bool:
        try:
            text = await self._http.get_text(source.url, timeout=PROBE_TIMEOUT)
        except HttpError:
            return False
        return "ceph_" in text[:65536]

    async def _find_prometheus(self, found: Discovery) -> None:
        s = self._settings
        if s.prometheus_url:
            found.prometheus_url = s.prometheus_url.rstrip("/")
            return
        if not s.discover_exporters:
            return
        for address, _name, port in await self._orch_endpoints("prometheus", 9095) or []:
            found.prometheus_url = _url_for(address, port).removesuffix("/metrics")
            return

    async def _orch_endpoints(
        self, daemon_type: str, default_port: int
    ) -> list[tuple[str, str, int]] | None:
        """(address, hostname, port) of the running daemons of one type, as the
        orchestrator knows them. None when there is no orchestrator."""
        daemons, hosts = await gather(
            lambda: self._ceph.try_json(["orch", "ps", "--daemon_type", daemon_type]),
            lambda: self._ceph.try_json(["orch", "host", "ls"], []),
        )
        if daemons is None:
            return None
        addresses = {h.get("hostname"): h.get("addr") for h in hosts or []}
        endpoints = []
        for daemon in daemons:
            if daemon.get("status_desc") not in (None, "running"):
                continue
            name = daemon.get("hostname") or ""
            address = daemon.get("ip") or addresses.get(name) or name
            if address:
                endpoints.append((address, name, (daemon.get("ports") or [default_port])[0]))
        return endpoints

    async def _check_perf_counters(self, found: Discovery) -> None:
        """Say so when per-daemon performance counters are not exported anywhere."""
        kinds = found.kinds()
        if MGR not in kinds or EXPORTER in kinds:
            return
        excluded = await self._ceph.try_json(
            ["config", "get", "mgr", "mgr/prometheus/exclude_perf_counters"]
        )
        if str(excluded).lower() == "true":
            found.notes.append(
                "Per-daemon performance counters (ceph_osd_op_*, ceph_bluestore_*, ...) are "
                "not exported: the mgr prometheus module leaves them to ceph-exporter, and no "
                "ceph-exporter was found. To have the mgr export them, run `ceph config set "
                "mgr mgr/prometheus/exclude_perf_counters false`. Until then use daemon_perf."
            )

    # ------------------------------------------------------------------ scraping

    async def scrape(self, source: Source, *, fresh: bool = False) -> Scrape:
        """Read an endpoint. A result less than two seconds old is reused, so
        that calls in quick succession do not each fetch it, unless `fresh`:
        a measurement over an interval needs a sample from the end of it."""
        cached = self._scrapes.get(source.url)
        if cached and not fresh and not cached.error and self._clock() - cached.time < SCRAPE_TTL:
            return cached
        now = self._clock()
        try:
            text = await self._http.get_text(source.url)
        except HttpError as exc:
            return Scrape(source, now, error=str(exc))
        metrics = await anyio.to_thread.run_sync(promtext.parse, text)
        result = Scrape(source, now, metrics)
        self._scrapes[source.url] = result
        return result

    async def collect(self, kind: str | None = None, *, fresh: bool = False) -> Collection:
        """Scrape all sources (or those of one kind) and merge the result.

        The collection is dated when its samples were taken, which for a
        reused scrape is earlier than now: rates are computed from that."""
        found = await self.discover()
        sources = [s for s in found.sources if kind is None or s.kind == kind]
        scrapes: list[Scrape] = await gather(
            *[lambda s=s: self.scrape(s, fresh=fresh) for s in sources]
        )
        taken = [s.time for s in scrapes if not s.error]
        return Collection(max(taken) if taken else self._clock(), merge(scrapes), scrapes)


def merge(scrapes: Iterable[Scrape]) -> dict[str, Metric]:
    """Merge scrapes into one metric set.

    ceph-exporter samples get an `instance` label naming their host. A metric
    that ceph-exporter provides is taken from there only, so that counters the
    mgr module also exports are not counted twice.
    """
    scrapes = [s for s in scrapes if not s.error]
    from_exporters = {name for s in scrapes if s.source.kind == EXPORTER for name in s.metrics}
    merged: dict[str, Metric] = {}
    for scrape in scrapes:
        is_exporter = scrape.source.kind == EXPORTER
        extra = (("instance", scrape.source.host),) if is_exporter and scrape.source.host else ()
        for name, metric in scrape.metrics.items():
            if not is_exporter and name in from_exporters:
                continue
            target = merged.get(name)
            if target is None:
                target = merged[name] = Metric(name, metric.type, metric.help)
            if extra:
                target.samples.extend(Sample(s.labels + extra, s.value) for s in metric.samples)
            else:
                target.samples.extend(metric.samples)
    return merged


# ------------------------------------------------------------------ selection


def compile_pattern(pattern: str, what: str) -> re.Pattern[str]:
    if len(pattern) > 300:
        raise ValueError(f"{what} is too long")
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"invalid regular expression for {what}: {exc}") from None


def select(
    metrics: Mapping[str, Metric],
    name: str,
    labels: Mapping[str, str] | None = None,
) -> list[tuple[str, Sample]]:
    """Samples whose metric name fully matches `name` (a regex) and whose
    labels fully match the given regexes. A missing label matches only ''."""
    name_re = compile_pattern(name, "metric name")
    label_res = {k: compile_pattern(v, f"label {k}") for k, v in (labels or {}).items()}
    out: list[tuple[str, Sample]] = []
    exact = metrics.get(name)
    names = [name] if exact is not None else [n for n in metrics if name_re.fullmatch(n)]
    for metric_name in names:
        for sample in metrics[metric_name].samples:
            if all(rx.fullmatch(sample.label(k)) for k, rx in label_res.items()):
                out.append((metric_name, sample))
    return out


AGGREGATIONS = {
    "sum": sum,
    "avg": lambda v: sum(v) / len(v),
    "min": min,
    "max": max,
    "count": len,
}


def aggregate(
    rows: Iterable[tuple[str, tuple[tuple[str, str], ...], float]],
    how: str,
    by: Iterable[str],
) -> list[tuple[str, tuple[tuple[str, str], ...], float]]:
    """Aggregate (metric, labels, value) rows over all labels except `by`."""
    by = list(by)
    groups: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = {}
    for name, labels, value in rows:
        if value != value:  # NaN
            continue
        lookup = dict(labels)
        key = (name, tuple((b, lookup.get(b, "")) for b in by))
        groups.setdefault(key, []).append(value)
    fn = AGGREGATIONS[how]
    return [(name, labels, float(fn(values))) for (name, labels), values in groups.items()]


def rates(
    before: Collection, after: Collection, name: str, labels: Mapping[str, str] | None = None
) -> list[tuple[str, tuple[tuple[str, str], ...], float]]:
    """Per-second increase of matching counters between two collections.

    Series that are missing from either side, or that went backwards (a daemon
    restarted), are left out.
    """
    elapsed = after.time - before.time
    if elapsed <= 0:
        return []
    old = {(n, s.labels): s.value for n, s in select(before.metrics, name, labels)}
    out = []
    for metric_name, sample in select(after.metrics, name, labels):
        prev = old.get((metric_name, sample.labels))
        if prev is None or sample.value < prev:
            continue
        out.append((metric_name, sample.labels, (sample.value - prev) / elapsed))
    return out
