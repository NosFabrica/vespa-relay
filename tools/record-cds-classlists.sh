#!/usr/bin/env bash
# Re-records relay/cds.classlist and sync/cds.classlist, the classes each process loads in a real
# boot, which the Dockerfile archives (AppCDS) when it builds the image. Run it from the repository
# root against a compose stack that is up, with Vespa holding data and the sync's sources reachable,
# after a dependency bump or a change to what the boot does; then rebuild the image and commit both
# files. A dev stack, not production: it restarts the relay and the sync twice.
#
#   tools/record-cds-classlists.sh [seconds to let them run, default 90]
#
# COMPOSE overrides the compose command, files included, for a stack started with its own (an
# --env-file, an override): COMPOSE="docker compose --env-file my.env -f docker-compose.yml -f my.yml"
set -euo pipefail
SECS=${1:-90}
STATE=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/var/lib/vespa-relay"}}{{.Source}}{{end}}{{end}}' vespa-relay)
[ -n "$STATE" ] || { echo "no running vespa-relay container with a /var/lib/vespa-relay mount" >&2; exit 1; }
OVR=$(mktemp --suffix=.yml)
trap 'rm -f "$OVR"' EXIT
# Recording replaces the archive: classes mapped from one would be missing from the list.
cat > "$OVR" <<YML
services:
  relay:
    environment:
      VESPA_RELAY_OPTS: "-XX:DumpLoadedClassList=/var/lib/vespa-relay/relay.classlist"
  sync:
    environment:
      VESPA_SYNC_OPTS: "-XX:DumpLoadedClassList=/var/lib/vespa-relay/sync.classlist"
YML
BASE=${COMPOSE:-docker compose -f docker-compose.yml}
DC="$BASE -f $OVR --profile sync"
$DC up -d --no-deps --force-recreate relay sync
echo "recording for ${SECS}s"
sleep "$SECS"
$DC stop relay sync
for app in relay sync; do
  {
    echo "# Classes a $app boot loads, recorded from a running stack by tools/record-cds-classlists.sh and"
    echo "# archived when the image is built (Dockerfile). Regenerate after a dependency bump; a stale"
    echo "# entry is skipped, a missing one is only loaded the slow way."
    # The ids only matter for classes from custom loaders, which this list has none of.
    grep -v '^#' "$STATE/$app.classlist" | sed 's/ id: [0-9]*$//'
  } > "$app/cds.classlist"
  echo "$app/cds.classlist: $(grep -vc '^#' "$app/cds.classlist") entries"
done
# Back to the stack as configured.
$BASE --profile sync up -d --no-deps --force-recreate relay sync
