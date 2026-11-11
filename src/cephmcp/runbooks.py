"""Runbooks: proven procedures for common situations, built on the tools.

They reach a client in two ways: as MCP prompts, which a person starts (in
Claude Code as a slash command), and through the `runbook` tool, which lets
an agent look one up by itself. A runbook does not do anything on its own;
it tells the model which tools to use, in which order, and what to conclude.
"""

from __future__ import annotations

import textwrap
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class Situation:
    """What a runbook adapts to."""

    admin: bool = False
    destructive: bool = False
    microceph: bool = False

    @property
    def changes(self) -> str:
        if not self.admin:
            text = (
                "This server is read-only. Where a step changes the cluster, give the "
                "exact command and leave running it to the operator."
            )
        else:
            text = (
                "This server may change the cluster. Before any change, say what you are "
                "about to do and why, and prefer the guarded tools (maintenance_enter, "
                "cluster_flags, osd_set, pool_set, config_set) over ceph_command: they "
                "record what they did and how to undo it (change_log, change_undo)."
            )
        return textwrap.fill(text, 78)


@dataclass(frozen=True)
class Runbook:
    name: str
    title: str
    description: str
    argument: str
    """Name of the optional argument, or '' if the runbook takes none."""
    argument_help: str
    body: Callable[[Situation, str], str]

    def render(self, situation: Situation, value: str = "") -> str:
        return self.body(situation, value.strip()).strip() + "\n"


REPORT = """\
Report in this shape, and keep it short:
1. What is wrong, in one sentence, and who is affected.
2. The evidence: the specific daemons, PGs, pools or numbers, with times in UTC.
3. The cause, or the two most likely causes and what would tell them apart.
4. What to do, most urgent first, as exact commands or tool calls.
5. What not to do (anything that would make it worse).
State what you could not check. Do not present a guess as a finding."""


def _triage(s: Situation, symptom: str) -> str:
    about = f' The reported symptom: "{symptom}".' if symptom else ""
    return f"""\
Find out what is wrong with this Ceph cluster and what to do about it.{about}

{s.changes}

1. cluster_status, then health_detail. Note every health check, and whether
   data is unavailable (inactive PGs), at risk (degraded, undersized) or only
   redundancy and capacity warnings.
2. what_changed for the last few hours: most problems follow a change. Look at
   what happened just before the first health check appeared.
3. Follow the checks you found:
   - OSDs down: osd_list state=down. One host or scattered? cluster_log for the
     moment they went down; crash_list for a crash of those daemons.
   - PGs inactive, incomplete or stuck: pg_list with states or stuck; for one PG
     ceph_command ['pg', '<pgid>', 'query'] and read "recovery_state".
   - Degraded or misplaced only: is recovery progressing? cluster_status shows
     the rate; flags like norecover or nobackfill stop it (cluster_review).
   - Slow ops: perf_overview, then daemon_ops kind=historic_slow on the OSDs
     named in health_detail; osd_outliers for a failing disk; io_top for the load.
   - Nearfull or full: osd_list sort_by=used_pct, pool_list, capacity_forecast.
   - Monitors out of quorum or clock skew: daemon_list, cluster_log.
   - Crashes: crash_list, then crash_list crash_id=<id> for the backtrace.
4. If health is OK but the complaint stands, run perf_overview and io_top, and
   cluster_review for weaknesses that Ceph does not warn about.

{REPORT}"""


def _slow(s: Situation, symptom: str) -> str:
    about = f' The complaint: "{symptom}".' if symptom else ""
    return f"""\
Find out why this Ceph cluster is slow, and where.{about}

{s.changes}

1. perf_overview. Is client I/O competing with recovery or backfill? What are
   the read and write latencies, and is one host or OSD far above the others?
2. Narrow it down:
   - One OSD or one host stands out: osd_outliers compares every OSD with its
     peers, over time if history exists. Then daemon_ops kind=historic_slow and
     daemon_perf seconds=10 on that OSD. Long "queued_for_pg" or "waiting for
     subops" points at another OSD or the network; long commit or kv times
     point at this disk.
   - Everything is slow: io_top shows which pools, images and clients produce
     the load. Compare with yesterday using history_query series='cluster.*'.
   - Recovery is running: cluster_status for its progress; it ends on its own.
     If clients suffer, osd_mclock_profile=high_client_ops shifts priority.
3. what_changed: when did latency start to rise, and what happened then?
4. Check for standing causes with cluster_review: mixed device classes behind
   one rule, OSDs sharing a device, memory targets above the host's RAM.

{REPORT}"""


def _pre_maintenance(s: Situation, host: str) -> str:
    target = host or "<host>"
    ask = "" if host else "Ask which host is meant if it was not said.\n\n"
    if s.admin:
        enter = (
            f"maintenance_enter host='{target}' does steps 1 to 3 as one guarded operation,\n"
            "   asks the operator to approve, and records how to undo it."
        )
        leave = (
            f"maintenance_exit host='{target}' waits for the OSDs and for recovery,\n"
            "   and clears the flags."
        )
    else:
        enter = f"give the operator `ceph osd set-group noout {target}`"
        enter += (
            f"\n   (MicroCeph: `microceph cluster maintenance enter {target}`)."
            if s.microceph
            else "."
        )
        leave = (
            "wait until its OSDs are up, clear the flag with\n"
            f"   `ceph osd unset-group noout {target}`, and watch recovery finish."
        )
    return f"""\
Prepare host {target} for maintenance (reboot, hardware or OS work) without
disturbing the cluster's clients.

{ask}{s.changes}

1. simulate scenario='reboot' hosts=['{target}']. It answers whether every PG
   stays active, whether the monitors keep a quorum and which services run only
   there. If the verdict is "unsafe", stop and explain why; do not work around it.
2. cluster_status: the cluster should be HEALTH_OK with all PGs active+clean
   before a host is taken down. If it is already degraded, wait or say why not.
3. Entering maintenance: {enter}
4. Tell the operator the host can go down, and what to expect: its OSDs show as
   down, PGs as degraded and undersized, health as WARN. Nothing is re-replicated
   while noout is set, so the host should not stay away for long.
5. When the host is back: {leave}
6. Finish with cluster_status: HEALTH_OK, all PGs active+clean, no flags left.

Say clearly at each point whether it is safe to proceed."""


def _incident_report(s: Situation, since: str) -> str:
    window = since or "-24h"
    return f"""\
Write a post-incident report for this Ceph cluster, covering {window} until now.

1. what_changed since='{window}' gives the merged timeline: health checks
   appearing and clearing, OSDs going down and up, commands that were run,
   configuration changes, crashes, and shifts in the recorded metrics.
2. Fill gaps: cluster_log (channel 'cluster' and 'audit') around the key
   moments, crash_list for backtraces, history_query for the metrics that
   moved (latency, client I/O, degraded PGs), chart for a picture of them.
3. cluster_status and cluster_review for the state now: is it fully recovered,
   and is the weakness that allowed the incident still there?

Write the report with these sections:
- Summary: what happened and for how long, in two sentences.
- Impact: what clients experienced (unavailable or slow I/O, which pools).
- Timeline: times in UTC, one line per event, facts only.
- Root cause: what failed, and why that had this effect. Say so if it is not known.
- Contributing factors: what made it worse or delayed recovery.
- What worked.
- Actions: each one specific, with the command or change, ordered by value.

Every statement must trace back to something a tool returned. Where the
records do not reach back far enough, say that instead of filling the gap."""


def _capacity(s: Situation, horizon: str) -> str:
    target = horizon or "the next 6 months"
    return f"""\
Plan the capacity of this Ceph cluster for {target}.

1. capacity_forecast: growth per day, and the dates on which the cluster and
   each pool reach the nearfull and full limits. Note how much history the
   forecast rests on.
2. pool_list and osd_list sort_by=used_pct: which pools grow, and how evenly
   the OSDs are filled. The fullest OSD decides when the cluster is full.
3. cluster_review: does the cluster have room to re-create the data of a failed
   host today? That, not 100%, is the real limit.
4. simulate scenario='add' for the expansion you would propose: how much data
   moves, and how full the OSDs are afterwards.

Conclude with: the date by which capacity must be added, how much, on which
hosts (keeping hosts equal in size), and what it changes. Give the reasoning in
numbers. Mention cheaper options if they exist (rebalancing, removing unused
pools, compression) and what each would buy."""


def _checkup(s: Situation, _: str) -> str:
    return """\
Give this Ceph cluster a routine check-up.

1. cluster_review: weaknesses in the setup, most serious first.
2. cluster_status and perf_overview seconds=10: state and performance now.
3. what_changed since='-7d': what happened since the last look.
4. capacity_forecast: when space runs out.
5. osd_outliers: disks drifting away from their peers.

Report on one page: a verdict in one sentence, then at most five things to
do, each with its reason and the command, ordered by how much they matter.
Then one line each for what was checked and found in order. Do not list
findings of no consequence."""


def _upgrade(s: Situation, version: str) -> str:
    target = f" to {version}" if version else ""
    how = (
        "MicroCeph upgrades one node at a time: for each node, simulate scenario='reboot',\n"
        "   enter maintenance, `snap refresh microceph --channel <track>/stable`, exit\n"
        "   maintenance and wait for HEALTH_OK before the next node."
        if s.microceph
        else "cephadm: `ceph orch upgrade start --ceph-version <version>`, then follow\n"
        "   `ceph orch upgrade status`. It restarts mons, mgrs, OSDs, MDSs and gateways\n"
        "   in the right order and waits for each. Package installs: the same order by hand."
    )
    return f"""\
Check whether this Ceph cluster is ready to be upgraded{target}, and plan it.

{s.changes}

1. cluster_status: an upgrade starts from HEALTH_OK with all PGs active+clean.
   Any warning must be understood first; say which ones block.
2. daemon_list: versions now. Mixed versions mean an earlier upgrade was never
   finished; finish that first.
3. cluster_review: redundancy weaknesses matter now, because every daemon gets
   restarted. Pools that stall when one OSD is down will stall during the upgrade.
4. simulate scenario='reboot' for each host: can every host be restarted alone?
5. crash_list: unexplained crashes on the current version are a reason to wait.
6. Space: recovery during the upgrade needs room. osd_list sort_by=used_pct.
7. How: {how}
8. Afterwards: daemon_list shows one version everywhere; `ceph osd
   require-osd-release <name>` finalises it; cluster_status is HEALTH_OK.

Conclude with go or no-go, the blockers if any, and the plan step by step."""


def _replace_osd(s: Situation, osd: str) -> str:
    target = osd or "<id>"
    if s.microceph:
        remove = (
            f"`microceph disk remove {target}`, swap the disk,\n"
            "   then `microceph disk add <device>` on that node."
        )
    else:
        remove = (
            f"cephadm: `ceph orch osd rm {target} --replace` drains first\n"
            "   and keeps the id for the new disk. By hand: mark it out, wait, then\n"
            f"   `ceph osd purge {target} --yes-i-really-mean-it`, swap the disk and create\n"
            "   the OSD again."
        )
    permission = "Removing an OSD is destructive"
    if not s.destructive:
        permission += " and not permitted by this server: give the commands to the operator"
    permission = textwrap.fill(
        permission + ". Never remove an OSD that safe-to-destroy does not clear, unless the "
        "disk is dead and the data is confirmed to exist elsewhere (all PGs active).",
        78,
    )
    return f"""\
Replace the disk behind osd.{target} without putting data at risk.

{s.changes}

1. osd_list and health_detail: is the OSD down, slow or still healthy? For a
   suspected disk: daemon_ops kind=historic_slow, osd_outliers, and
   ceph_command ['device', 'ls-by-daemon', 'osd.{target}'].
2. simulate scenario='drain' osds=[{target}]: where its data goes, whether the
   other OSDs have room for it, and whether redundancy can be restored at all.
   If not, the replacement disk must go in before the old one is removed.
3. If the OSD still runs, mark it out and let the data move away while it can
   still serve as a source: osd_set action='out'. Watch cluster_status until all
   PGs are active+clean again.
4. ceph_command ['osd', 'safe-to-destroy', '{target}'] must agree before the
   OSD is removed.
5. Remove and replace: {remove}
6. Verify: osd_list shows the new OSD up and in, backfill runs, and the cluster
   returns to HEALTH_OK.

{permission}"""


RUNBOOKS: tuple[Runbook, ...] = (
    Runbook(
        "triage",
        "Triage a problem",
        "Find out what is wrong with the cluster and what to do about it.",
        "symptom",
        "What was noticed, e.g. 'HEALTH_WARN since this morning' or 'VMs hang'.",
        _triage,
    ),
    Runbook(
        "slow",
        "Investigate slowness",
        "Find out why the cluster is slow, and where.",
        "symptom",
        "What is slow, e.g. 'writes to pool vms' or 'everything since 14:00'.",
        _slow,
    ),
    Runbook(
        "pre_maintenance",
        "Prepare a host for maintenance",
        "Check that a host can be taken down safely, and walk through doing it.",
        "host",
        "The host to take down.",
        _pre_maintenance,
    ),
    Runbook(
        "incident_report",
        "Write an incident report",
        "Reconstruct what happened from the records and write a post-incident report.",
        "since",
        "How far back to look, e.g. '-6h' or '-2d'. Default: -24h.",
        _incident_report,
    ),
    Runbook(
        "capacity_plan",
        "Plan capacity",
        "Work out when space runs out and what to add.",
        "horizon",
        "The period to plan for, e.g. '12 months'.",
        _capacity,
    ),
    Runbook(
        "checkup",
        "Routine check-up",
        "Review setup, state, performance, recent changes and capacity on one page.",
        "",
        "",
        _checkup,
    ),
    Runbook(
        "upgrade_readiness",
        "Check upgrade readiness",
        "Decide whether the cluster is ready for an upgrade, and plan it.",
        "version",
        "The target version or release, e.g. '20.2.2' or 'tentacle'.",
        _upgrade,
    ),
    Runbook(
        "replace_osd",
        "Replace a disk",
        "Replace the disk behind an OSD without putting data at risk.",
        "osd",
        "The OSD id, e.g. '7'.",
        _replace_osd,
    ),
)

BY_NAME = {r.name: r for r in RUNBOOKS}
