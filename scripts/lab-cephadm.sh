#!/usr/bin/env bash
# Bring up a single-node cephadm cluster in an LXD VM for integration testing.
# Unlike MicroCeph, cephadm deploys ceph-exporter and a Prometheus server, so
# this is the environment that exercises those metric sources.
#
# Run on the LXD host, e.g.:
#   ssh <lxd-host> bash -s < scripts/lab-cephadm.sh
#
# Idempotent: re-running skips steps that are already done.
set -euo pipefail

NAME=${NAME:-cephmcp-adm}
IMAGE=${IMAGE:-ubuntu:26.04}
POOL=${POOL:-default}
CPU=${CPU:-4}
MEM=${MEM:-8GiB}
DISK=${DISK:-40GiB}
OSDS=${OSDS:-3}
OSD_SIZE=${OSD_SIZE:-10GiB}

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*"; }
# lxc reads stdin; keep it from swallowing this script when run via `bash -s`.
lxc() { command lxc "$@" </dev/null; }
vm() { lxc exec "$NAME" -- "$@"; }

if ! lxc info "$NAME" >/dev/null 2>&1; then
  log "creating $NAME"
  lxc init -q "$IMAGE" "$NAME" --vm \
    -c limits.cpu="$CPU" -c limits.memory="$MEM" -d root,size="$DISK"
  for i in $(seq 1 "$OSDS"); do
    vol="$NAME-osd$i"
    lxc storage volume show "$POOL" "$vol" >/dev/null 2>&1 ||
      lxc storage volume create -q "$POOL" "$vol" --type=block size="$OSD_SIZE"
    lxc config device add -q "$NAME" "osd$i" disk pool="$POOL" source="$vol"
  done
  lxc start "$NAME"
fi

for _ in $(seq 1 120); do
  vm true 2>/dev/null && break
  sleep 2
done
vm cloud-init status --wait >/dev/null 2>&1 || true
log "VM ready"

if ! vm sh -c 'which cephadm podman ceph >/dev/null 2>&1' 2>/dev/null; then
  log "installing cephadm, podman, ceph-common"
  vm sh -c 'export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq && apt-get install -y -qq cephadm podman ceph-common >/var/log/lab-apt.log 2>&1' ||
    { vm tail -20 /var/log/lab-apt.log; exit 1; }
fi
log "packages: $(vm sh -c "dpkg-query -W -f '\${Package} \${Version}  ' cephadm ceph-common")"

if ! vm test -f /etc/ceph/ceph.conf; then
  ip=$(vm sh -c "ip -4 route get 1.1.1.1 | sed -n 's/.* src \([0-9.]*\).*/\1/p'")
  # Pin the container to the version of the host's ceph CLI. cephadm would
  # otherwise pull the newest point release, and a newer cluster writes a
  # keyring that an older CLI cannot parse.
  image=${CEPH_IMAGE:-quay.io/ceph/ceph:v$(vm ceph --version | awk '{print $3}')}
  log "bootstrapping $image on $ip"
  vm sh -c "cephadm --image $image bootstrap --mon-ip $ip --single-host-defaults \
    --allow-fqdn-hostname >/var/log/lab-bootstrap.log 2>&1" ||
    { vm tail -30 /var/log/lab-bootstrap.log; exit 1; }
fi
vm ceph versions

if [ "$(vm ceph osd ls | wc -l)" -lt "$OSDS" ]; then
  log "deploying OSDs"
  vm ceph orch apply osd --all-available-devices >/dev/null
  for _ in $(seq 1 90); do
    [ "$(vm ceph osd ls | wc -l)" -ge "$OSDS" ] && break
    sleep 10
  done
fi

# Three OSDs at the default 4 GiB memory target do not fit this VM beside the
# monitoring stack: under load it runs out of memory. (cephmcp's cluster_review
# reports exactly that when this line is left out.)
vm ceph config set osd osd_memory_target "${OSD_MEMORY_TARGET:-1610612736}"

vm ceph -s
vm ceph orch ps
log "DONE cephadm lab"
