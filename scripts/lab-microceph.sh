#!/usr/bin/env bash
# Bring up a MicroCeph cluster in LXD VMs for integration testing.
#
# Run on the LXD host, e.g.:
#   ssh <lxd-host> bash -s < scripts/lab-microceph.sh
#
# Idempotent: re-running skips steps that are already done. The script waits
# for the snap store to become reachable, because it is not always reachable
# from lab networks.
#
# Without store access, download the snaps elsewhere and point SNAP_DIR at a
# directory on the LXD host that holds them with their .assert files:
#   snap download microceph --channel=tentacle/stable
#   snap download core24          # the base that this microceph revision needs
#   snap download snapd           # needed too if the VM has no snaps yet
# A snap without an .assert file is installed with --dangerous.
set -euo pipefail

PREFIX=${PREFIX:-cephmcp}
NODES=${NODES:-3}
IMAGE=${IMAGE:-ubuntu:26.04}
CHANNEL=${CHANNEL:-tentacle/stable}
CPU=${CPU:-4}
MEM=${MEM:-8GiB}
DISK=${DISK:-40GiB}
OSDS_PER_NODE=${OSDS_PER_NODE:-2}
OSD_SIZE=${OSD_SIZE:-8G}
STORE_WAIT=${STORE_WAIT:-110}        # minutes to wait for the snap store
SNAP_DIR=${SNAP_DIR:-}               # install from these files instead of the store

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*"; }
# lxc reads stdin; keep it from swallowing this script when run via `bash -s`.
lxc() { command lxc "$@" </dev/null; }

names=()
for i in $(seq 1 "$NODES"); do names+=("$PREFIX-$i"); done
first=${names[0]}

for n in "${names[@]}"; do
  if ! lxc info "$n" >/dev/null 2>&1; then
    log "launching $n"
    lxc launch -q "$IMAGE" "$n" --vm \
      -c limits.cpu="$CPU" -c limits.memory="$MEM" -d root,size="$DISK"
  fi
done

for n in "${names[@]}"; do
  for _ in $(seq 1 120); do
    lxc exec "$n" -- true 2>/dev/null && break
    sleep 2
  done
  lxc exec "$n" -- cloud-init status --wait >/dev/null 2>&1 || true
done
log "VMs ready"

if [ -z "$SNAP_DIR" ] && ! lxc exec "$first" -- snap list microceph >/dev/null 2>&1; then
  tries=0
  until lxc exec "$first" -- timeout 40 snap info microceph >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge "$STORE_WAIT" ]; then
      log "snap store still unreachable after $tries tries, giving up"
      exit 1
    fi
    [ $((tries % 10)) -eq 1 ] && log "snap store unreachable (try $tries), waiting"
    sleep 60
  done
  log "snap store reachable"
fi

for n in "${names[@]}"; do
  if ! lxc exec "$n" -- snap list microceph >/dev/null 2>&1; then
    if [ -n "$SNAP_DIR" ]; then
      lxc exec "$n" -- mkdir -p /root/snaps
      for f in "$SNAP_DIR"/*.assert "$SNAP_DIR"/*.snap; do
        [ -e "$f" ] && lxc file push -q "$f" "$n/root/snaps/"
      done
      # Assertions first, then snapd, bases, and microceph itself.
      lxc exec "$n" -- sh -c 'set -e
        cd /root/snaps
        for a in *.assert; do [ -e "$a" ] && snap ack "$a"; done
        for s in snapd_*.snap core*.snap microceph_*.snap; do
          [ -e "$s" ] || continue
          snap list "${s%%_*}" >/dev/null 2>&1 && continue
          if [ -e "${s%.snap}.assert" ]; then snap install "./$s"; else snap install --dangerous "./$s"; fi
        done'
    else
      lxc exec "$n" -- snap install microceph --channel "$CHANNEL"
    fi
    lxc exec "$n" -- snap refresh --hold microceph >/dev/null 2>&1 || true
  fi
  log "$n: $(lxc exec "$n" -- snap list microceph | tail -1)"
done

if ! lxc exec "$first" -- microceph status >/dev/null 2>&1; then
  log "bootstrapping on $first"
  lxc exec "$first" -- microceph cluster bootstrap
fi

for n in "${names[@]:1}"; do
  if ! lxc exec "$first" -- microceph cluster list 2>/dev/null | grep -qw "$n"; then
    log "joining $n"
    token=$(lxc exec "$first" -- microceph cluster add "$n")
    lxc exec "$n" -- microceph cluster join "$token"
  fi
done

for n in "${names[@]}"; do
  have=$(lxc exec "$n" -- sh -c 'ls -d /var/snap/microceph/common/data/osd/ceph-* 2>/dev/null | wc -l')
  if [ "$have" -lt "$OSDS_PER_NODE" ]; then
    log "adding $((OSDS_PER_NODE - have)) loop OSDs on $n"
    lxc exec "$n" -- microceph disk add "loop,$OSD_SIZE,$((OSDS_PER_NODE - have))"
  fi
done

lxc exec "$first" -- microceph status
lxc exec "$first" -- ceph -s
log "DONE microceph lab"
