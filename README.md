# cephmcp

An [MCP](https://modelcontextprotocol.io) server that lets AI agents inspect,
measure and administer a Ceph cluster. It works with MicroCeph, cephadm and
plain package installs, because it talks to the cluster the way an operator
does: through the `ceph` (and `microceph`) command line, plus the Prometheus
endpoints of the mgr `prometheus` module and of `ceph-exporter` where they exist.

Beyond passing commands through, it knows things an operator knows:

- what is weak about a cluster that reports `HEALTH_OK` (`cluster_review`);
- what happened, and when (`what_changed`, from logs, the audit trail and its
  own recorded history, which needs no Prometheus);
- what would happen (`simulate`: reboot a host, lose a disk, add capacity);
- how to do risky things safely (`maintenance_enter`: check, approval, apply,
  wait, verify, with a journal and undo);
- who is causing the load (`io_top`);
- and how to go about common tasks (runbooks, offered as slash commands).

It is read-only unless you say otherwise.

## Quick start

On a cluster node (for MicroCeph: any node, as root):

```sh
uv tool install '.[charts]'   # or: pipx install '.[charts]'   (from a checkout)
sudo cephmcp --check          # shows what the server can reach, then exits
```

```
cephmcp 0.1.0, mode read-only
ceph command:      /usr/bin/ceph
cluster:           1ea4e1cc-ddcc-11f1-9f64-00163e578a2b HEALTH_OK
microceph command: not found
metrics:           ceph-exporter http://10.0.0.5:9926/metrics - 454 metrics, 1136 series
metrics:           mgr-prometheus http://10.0.0.5:9283/metrics - 99 metrics, 333 series
prometheus server: http://10.0.0.5:9095
history:           /root/.local/state/cephmcp, recording on
charts:            available
tools (28):        cluster_status, health_detail, ...
```

Then point an MCP client at it. The server speaks MCP on stdin/stdout, so the
client can start it locally or over ssh:

```json
{
  "mcpServers": {
    "ceph": { "command": "ssh", "args": ["root@node1", "cephmcp"] }
  }
}
```

With Claude Code: `claude mcp add ceph -- ssh root@node1 cephmcp`.

The `charts` extra adds Pillow, which draws the pictures; without it everything
else works and the `chart` tool is not offered.

## What agents get

**State**

| Tool | Purpose |
| --- | --- |
| `cluster_status` | Health, quorum, OSD and PG state, capacity, client and recovery I/O |
| `health_detail` | Health checks with the affected OSDs, PGs and pools; unacknowledged crashes |
| `cluster_review` | An audit: weaknesses in the setup, most serious first, each with its fix |
| `daemon_list` | Mons, mgrs, MDSs, gateways: host, role, state, versions |
| `osd_list`, `pool_list`, `pg_list` | Per OSD, per pool, per PG |
| `config_get` | Configuration database, per-daemon running values, option documentation |
| `cluster_log`, `crash_list` | The cluster and audit logs; crash reports and backtraces |

**History and what-if**

| Tool | Purpose |
| --- | --- |
| `what_changed` | Timeline of a period: health checks, OSDs down and up, commands run and by whom, configuration changes, crashes, and shifts in metrics with their onset |
| `history_query` | How a recorded figure developed; when its level changed |
| `capacity_forecast` | Growth per day and the dates the nearfull and full limits are reached |
| `osd_outliers` | Disks that are slower than their peers, or drifting away from them |
| `simulate` | Consequences of rebooting or losing hosts and OSDs, draining them, or adding disks |

**Performance**

| Tool | Purpose |
| --- | --- |
| `perf_overview` | One-call picture: throughput, slowest OSDs, op latencies |
| `io_top` | Where the load comes from: pools, RBD images, clients, CephFS clients, gateways |
| `daemon_perf`, `daemon_ops` | A daemon's perf counters; its in-flight, blocked and slow operations |
| `metrics_sources`, `metrics_search`, `metrics_query`, `metrics_rate` | The exporters' metrics: what exists, current values, rates and latencies over an interval |
| `prometheus_query` | PromQL against a Prometheus server, including history |
| `chart` | A PNG: history as lines, the OSDs as a grid by host, or the capacity projection |

**Guidance and everything else**

| Tool | Purpose |
| --- | --- |
| `runbook` | Step-by-step procedures (see below) |
| `change_log` | What this server changed on the cluster |
| `ceph_command` | Any other `ceph` command the mode allows |
| `microceph_status`, `microceph_command` | Nodes, disks and services of a MicroCeph deployment (only when `microceph` is present) |

**In admin mode, additionally:** `maintenance_enter` and `maintenance_exit`
(a host into and out of maintenance, guarded), `cluster_flags`, `osd_set`,
`pool_create`, `pool_set`, `config_set`, `change_undo`, and with
`--allow-destructive` also `pool_delete`.

Results are compact JSON; lists come as `{"columns": [...], "rows": [[...]]}`.
Output is capped (`--max-output-chars`) and says so when it was cut.

## Cluster review

`cluster_review` reads the cluster's layout and configuration and reports what
an experienced operator would want to know, including conditions Ceph itself
reports as healthy:

- pools that keep too few copies, or accept writes with a single copy left;
- CRUSH rules that can put all copies on one host; pools that need more
  failure domains than exist;
- not enough free space to re-create the data of a failed host or disk;
- flags left set, OSDs out but running, leftover reweights;
- a single monitor, an even number of monitors, no standby mgr or MDS;
- several OSDs on one device; OSD memory targets beyond the host's RAM;
- a disk much slower than its peers;
- mixed versions, crashes nobody looked at, muted health checks;
- risky settings: authentication off, pool deletion enabled, default pool size
  below 3, verbose debug logging, per-daemon overrides.

Each finding has a severity, what it means in practice, and the command that
fixes it.

## Runbooks

Eight procedures, offered to people as MCP prompts (in Claude Code:
`/mcp__ceph__triage` and so on) and to agents through the `runbook` tool:

| Runbook | For |
| --- | --- |
| `triage` | Something is wrong: find out what, and what to do |
| `slow` | The cluster is slow: find out why, and where |
| `pre_maintenance` | Take a host down safely, and bring it back |
| `incident_report` | Reconstruct an incident from the records and write it up |
| `capacity_plan` | When does space run out, and what to add |
| `checkup` | Setup, state, performance, changes and capacity on one page |
| `upgrade_readiness` | Is the cluster ready for an upgrade; the plan |
| `replace_osd` | Replace a disk without putting data at risk |

A runbook tells the model which tools to use, in which order, and how to
report. They adapt to the server: read-only or admin, MicroCeph or not.

## History

Ceph keeps no history beyond a short log, and MicroCeph ships no Prometheus.
The server therefore records the cluster itself: once a minute, a few dozen
figures for the cluster, each pool, each OSD and each host (capacity, client
and recovery throughput, latencies, degraded data, health), and an event for
every change of state it sees. That is what `history_query`, `osd_outliers`,
`capacity_forecast`, the metric shifts in `what_changed` and `chart` read.

It goes into one SQLite file per cluster under `~/.local/state/cephmcp`
(`--state-dir`): a minute apart for two days, hourly averages for 90 days
(`--history-days`). A six-OSD cluster needs about 20 MB. Above 500 OSDs the
per-OSD series are left out and per-host ones remain.

A server started by a client records only while that client is connected. For
continuous history, run the recorder as a service on one node:

```ini
# /etc/systemd/system/cephmcp-record.service
[Service]
ExecStart=/usr/local/bin/cephmcp --record-only
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Any number of servers can share the file; one of them records at a time.
`--no-record` switches recording off, `--no-state` keeps nothing on disk.

`what_changed` does not depend on the recorder: it merges the cluster log, the
audit log, the configuration history, crash reports and the progress module,
and uses recorded events only for the time before those logs begin.

## What-if

`simulate` works out consequences from the cluster's current state, without
changing anything:

| Scenario | Question |
| --- | --- |
| `reboot` | Hosts or OSDs go away for a while: does every PG stay active, do the monitors keep quorum, which services run only there? |
| `fail` | They are lost for good: is data lost or unavailable, can the cluster re-create it, where, and is there room? |
| `drain` | They are taken out in an orderly way: where does the data go, is there room? |
| `add` | New disks join: how much data moves, how full is everything afterwards? |

Which PGs stop serving I/O is exact: it follows from where the copies of every
PG are. Where data would go is computed from CRUSH weights and failure domains
rather than by replaying CRUSH per PG, which is accurate to a few percent on a
balanced cluster. For `reboot` the server also asks Ceph's own `ok-to-stop`
checks and takes the more cautious answer. If any of the lookups an assessment
rests on fails, there is no assessment: a monitor list that could not be read
is not taken for a cluster without monitors.

## Changing the cluster

| Mode | Flag | What agents can do |
| --- | --- | --- |
| read-only | (default) | Inspect, measure, simulate. Tools that change state are not offered at all. |
| admin | `--mode admin` | Also host maintenance, flags, OSDs in/out and weights, pools, configuration, and other state-changing `ceph` and `microceph` commands. |
| destructive | `--mode admin --allow-destructive` | Also delete pools, purge OSDs, remove file systems, use `--yes-i-really-…` flags, remove MicroCeph disks and nodes, force maintenance against the assessment. |

**Maintenance.** `maintenance_enter host=node2` is one guarded operation:

1. It checks that the host can go (the `simulate` assessment). If not, it
   changes nothing and says why.
2. It gets approval (see below).
3. It applies the change the way the deployment wants it: MicroCeph and
   cephadm have a maintenance mode that sets `noout` and stops the daemons;
   elsewhere `noout` is set for the host and the operator stops the daemons.
4. It waits until the host's OSDs are down and the PGs have settled, and
   verifies that all of them still serve I/O.

`maintenance_exit` reverses it, waits for the OSDs and for recovery, and
verifies that the cluster is back to all PGs active+clean.

**Approval.** Host maintenance and pool deletion are confirmed before they
run (`--approval`):

- `auto` (default): where the client can put a question to its user (MCP
  elicitation), the operator is shown the assessment, the OSDs affected and
  the commands, and answers yes or no. Where it cannot, the tool returns its
  plan with a confirmation code and applies it only when called again with
  that code.
- `required`: a person approves, or the operation is refused.
- `off`: nobody is asked (for unattended use).

An approval is for the plan that was shown. The plan is worked out again
before it is applied; if the cluster has changed in the meantime (another OSD
on the host, a different assessment, other commands), the old yes or the old
code does not count and approval is asked for again.

**Journal and undo.** Every change made through the server is recorded with
its commands, the client that asked for it and its outcome (`change_log`).
Where the previous state is known, the commands that restore it are recorded
too, and `change_undo` runs them: the previous value of a pool or
configuration setting, an OSD's previous weight, the opposite flag, leaving
maintenance. An undo covers what the change actually altered: a flag that was
already set on a host is not cleared by undoing a call that set it again. If
the state before a change could not be read, the change is made, the result
says so, and no undo is recorded rather than a guessed one. Changes are undone
newest first. Creating or deleting a pool and free-form commands are journaled
without an undo.

**Free-form commands.** `ceph_command` and `microceph_command` accept any
arguments, so the server classifies each command before running it:

- Read-only commands are listed explicitly. Ceph's own per-command permission
  is not usable for this: it marks `balancer execute`, `alerts send` and
  `auth print-key` as "read". A command line is matched against the commands
  the live cluster knows, so `health` and `health mute` are told apart; what is
  not recognised as read-only needs admin mode.
- Options are admitted, not filtered. The `ceph` command line reads its
  arguments in three layers: its own options (which also take abbreviations
  and attached values, `--out-f=x`, `-ox`), then the client library, which
  takes any Ceph configuration option in any spelling (`--log-file x`), and
  only then the command. The first two write local files and choose identity
  and cluster, whatever the command is. So an option gets through only if it is
  written `--name` or `--name=value`, after the positional words, once, and
  `name` is an argument that the command itself declares in the cluster's
  help and that neither lower layer would take for itself. Everything else
  that begins with a dash is refused. For `microceph`, boolean flags are read
  as MicroCeph reads them: `--dry-run=0` is a real run, not a dry run.
- Commands that read a file take their input through the tool's `input`
  parameter.
- Keys and passwords are masked in everything the server returns, including
  commands quoted from the audit log, and commands whose purpose is to print a
  secret are refused, unless `--allow-secrets`.

This is a convenience layer, not a security boundary: the server usually holds
the admin keyring. For a hard guarantee, give it its own identity:

```sh
ceph auth get-or-create client.mcp mon 'allow r' mgr 'allow r' osd 'allow r' \
    -o /etc/ceph/ceph.client.mcp.keyring
chmod 600 /etc/ceph/ceph.client.mcp.keyring
cephmcp --ceph-name client.mcp --ceph-keyring /etc/ceph/ceph.client.mcp.keyring
```

Ceph then refuses every change itself, whatever mode the server is in. What
queries daemons directly stops working with such an identity, because Ceph
demands full caps for it: `daemon_perf`, `daemon_ops`, the client part of
`io_top`, and listing auth entities.

## Who is using the cluster

`io_top` answers "who is hammering the cluster":

- per pool, from the cluster's own rates;
- per RBD image, with latencies, from the mgr (the first call starts the
  collection; numbers appear about 20 seconds later);
- per client. Ceph keeps no per-client statistics, but every OSD knows which
  operations it is working on and for whom. The server samples that a few
  times across the OSDs, the way a profiler samples a program: each client's
  share of the operations in flight, its address, host and Ceph identity, how
  much of it is writes, and what it touches (pool, RBD image, CephFS data,
  S3 bucket). It changes nothing and needs no module;
- per CephFS client, from the metadata servers' sessions;
- per RGW daemon, where its request counters are exported.

## Metrics

| Source | What it has | Found by |
| --- | --- | --- |
| mgr `prometheus` module | Cluster, pool, PG and OSD state; health | `ceph mgr services` |
| `ceph-exporter` (one per host) | Per-daemon perf counters: op counts and latencies, BlueStore, RocksDB, ... | the orchestrator, else by probing the cluster's hosts on port 9926 |
| Prometheus server | History and PromQL | the orchestrator, or `--prometheus-url` |

Every source is optional and can be set explicitly. The exporters only expose
counters, so `metrics_rate` and `perf_overview` read them twice and report the
difference: ops/s, bytes/s and the average latency of the operations completed
in between. Samples are enriched with the labels one would otherwise join on:
`pool` next to `pool_id`, `hostname` and `device_class` next to `ceph_daemon`.

**MicroCeph** runs no `ceph-exporter` and no Prometheus, and the mgr module is
off by default. To get metrics there:

```sh
ceph mgr module enable prometheus
# the mgr leaves per-daemon perf counters to ceph-exporter unless told otherwise
ceph config set mgr mgr/prometheus/exclude_perf_counters false
```

`metrics_sources` tells an agent exactly this when something is missing. The
recorded history, `daemon_perf`, `daemon_ops`, `osd_list`, `io_top` and
`perf_overview` work without any exporter.

## Charts

`chart` returns a PNG image together with the figures it shows:

- `kind='history'`: recorded series as lines over time, one panel per unit of
  measurement. With many series of one kind (a latency per OSD), the few that
  stand out are coloured and the rest are grey.
- `kind='osds'`: one cell per OSD, grouped by host, shaded by how full it is
  or by its latency; outliers are marked.
- `kind='capacity'`: usage so far, the projection, and the limits.

Light and dark variants; the palette is checked for colour-blind separation.
Clients that show images in tool results show the picture; the model sees it
either way.

## Reaching the cluster

The server runs `ceph`, which it finds on `PATH` (as `ceph` or
`microceph.ceph`) or under `/snap/bin`. `--ceph-cmd` replaces that with any
command prefix:

```sh
cephmcp --ceph-cmd 'ssh node1 sudo ceph'            # from a workstation
cephmcp --ceph-cmd 'lxc exec node1 -- ceph'
cephmcp --ceph-cmd 'cephadm shell -- ceph'          # always matches the cluster version
cephmcp --ceph-conf /etc/ceph/other.conf --ceph-keyring /etc/ceph/other.keyring
```

Metric endpoints are fetched over HTTP from where the server runs, so a server
on a workstation needs a route to the cluster network for metrics (everything
else keeps working without).

On cephadm clusters the host's `ceph` package can be older than the cluster in
its containers, and then fails to parse the keyring
(`error setting modifier for [client.admin] type=key ... Malformed input`).
Use `cephadm shell -- ceph` as above, or install a matching `ceph-common`.

## Transports

stdio is the default and needs no network port. For a long-running service
there is streamable HTTP, which also records history for as long as it runs:

```sh
CEPHMCP_AUTH_TOKEN=$(openssl rand -hex 24) \
  cephmcp --transport http --host 0.0.0.0 --port 8000 --tls-cert cert.pem --tls-key key.pem
```

Clients send `Authorization: Bearer <token>` to `https://host:8000/mcp`. The
server refuses to listen on a non-loopback address without a token unless
`--allow-unauthenticated` is given.

## Configuration

The permission, cluster access, metrics and history options can also be set
through the environment: `--ceph-cmd` is `CEPHMCP_CEPH_CMD`. See
`cephmcp --help` for the full list.

| Flag | Default | |
| --- | --- | --- |
| `--mode` | `read-only` | `read-only` or `admin` |
| `--allow-destructive` | off | admin mode: also allow operations that can lose data |
| `--approval` | `auto` | who confirms guarded operations: `auto`, `required`, `off` |
| `--allow-secrets` | off | do not mask keys and passwords |
| `--ceph-cmd`, `--microceph-cmd` | autodetect | command prefix to run the CLI |
| `--ceph-conf`, `--ceph-keyring`, `--ceph-name` | Ceph's defaults | client configuration |
| `--command-timeout` | 30 | seconds per command |
| `--mgr-prometheus-url`, `--ceph-exporter-url`, `--prometheus-url` | discover | metric endpoints |
| `--no-discover` | | use only the endpoints given explicitly |
| `--insecure-tls` | | do not verify certificates of metric endpoints |
| `--state-dir` | `~/.local/state/cephmcp` | history and change journal |
| `--no-state`, `--no-record` | | keep nothing on disk; do not sample the cluster |
| `--record-only` | | record history and serve nothing (for a service) |
| `--record-interval`, `--history-days` | 60, 90 | seconds between samples; days kept |
| `--max-output-chars` | 40000 | longest result a tool may return |
| `--transport`, `--host`, `--port` | `stdio` | `http` serves on `127.0.0.1:8000` |
| `--auth-token`, `--tls-cert`, `--tls-key` | | HTTP only |

## Why not a mgr module

A mgr module would have direct access to cluster state, but:

- MicroCeph ships Ceph inside a read-only snap; there is no supported way to
  add a mgr module to it.
- ceph-mgr runs each module in a Python sub-interpreter. The MCP SDK depends on
  pydantic-core, a Rust extension that does not load there.
- A standalone process can be restarted, upgraded and confined to a read-only
  cephx identity without touching the cluster.

## Development

```sh
uv sync --all-extras
uv run pytest            # replays output captured from Ceph 20.2 clusters
uv run ruff check
```

`scripts/lab-microceph.sh` and `scripts/lab-cephadm.sh` bring up disposable
clusters in LXD VMs (a three-node MicroCeph cluster, and a single-node cephadm
cluster with ceph-exporter and Prometheus). Run them on the LXD host:

```sh
ssh lxd-host bash -s < scripts/lab-cephadm.sh
ssh lxd-host bash -s < scripts/lab-microceph.sh
```

Sizes, names and versions are environment variables at the top of each script.
Where the snap store is out of reach, `SNAP_DIR` makes the MicroCeph script
install from snaps downloaded elsewhere.

With the server installed on a node, the same read-only smoke test runs against
either cluster:

```sh
CEPHMCP_LAB_CMD='ssh lxd-host lxc exec cephmcp-1 -- cephmcp' uv run pytest -m lab
```

## Limits

- Tested against Ceph 20.2 (Tentacle): a three-node MicroCeph cluster
  (`tentacle/stable`) and a single-node cephadm cluster, on Ubuntu 26.04.
  Host maintenance was exercised end to end on MicroCeph; the cephadm and
  plain variants are covered by replayed tests only, because the cephadm lab
  has a single host, which cannot go into maintenance.
- `simulate` estimates space from CRUSH weights. Rules with several choose
  steps (stretch clusters), upmap exceptions and uneven PG sizes make the
  figures approximate; the PG availability verdict is not affected.
- `io_top` attributes load to clients by sampling. It tells heavy users from
  light ones; it is not accounting. RBD images are named in the image section;
  in the client section they appear by image id, since mapping ids to names
  needs the `rbd` tool. S3 load is shown per gateway, not per bucket or user.
- The capacity forecast is a straight line through past usage. It is as good
  as the history behind it, which the result states.
- RBD images, RGW users and buckets, and CephFS subvolumes have no dedicated
  tools; the mgr-side commands are reachable through `ceph_command`
  (`fs subvolume ls`, `rbd perf image stats`, ...), the `rbd` and
  `radosgw-admin` binaries are not.
- The mgr `prometheus` module refreshes every 15 s by default; rates of metrics
  that come only from it are measured over at least that long.
- In full admin mode the server offers about 40 tools, whose descriptions take
  roughly 12,000 tokens of a model's context.
