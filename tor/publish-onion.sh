#!/bin/sh
# Entrypoint for the hidden-service container: point Tor at the relay, start
# it, and publish the address it mints where the relay can read it.
#
# The relay needs that address for one thing — NIP-42. A Tor client signs the
# address it dialled, so a relay that does not know its own .onion rejects
# every auth event arriving through it, and here a rejected AUTH is not a
# locked door but a silent downgrade to unranked search. Handing the address
# over in a file, rather than an env var an operator pastes in after the fact,
# is what keeps that from depending on someone noticing.
#
# Runs as root so it can hand the key directory to the tor user; the daemon
# drops to `User tor` from the torrc and never runs as root itself.
set -eu

HS_DIR=/var/lib/tor/onion
OUT=${ONION_HOSTNAME_FILE:-/var/lib/onion/hostname}
TARGET_HOST=${ONION_TARGET_HOST:-relay}
TARGET_PORT=${ONION_TARGET_PORT:-7777}
GENERATED=/tmp/torrc-onion

# Both are named volumes, and a fresh one arrives owned by root. Tor refuses to
# start on a HiddenServiceDir it cannot write — and, being stricter than most
# daemons, on one whose mode is looser than 0700.
mkdir -p "$HS_DIR" "$(dirname "$OUT")"
chown tor:tor /var/lib/tor "$HS_DIR"
chmod 700 /var/lib/tor "$HS_DIR"

# The compose service name, as an IP. Tor's HiddenServicePort takes an address
# and NOT a name — a hostname there fails config parsing outright — so this is
# resolved here, once, and appended to a generated torrc.
resolve() {
    found=$(getent hosts "$TARGET_HOST" 2>/dev/null | awk 'NR==1 { print $1 }')
    if [ -z "$found" ]; then
        # busybox's nslookup, for an image whose getent is missing: skip the
        # resolver's own address (it carries a :53) and take the answer.
        found=$(
            nslookup "$TARGET_HOST" 2>/dev/null |
                awk '/^Address/ { addr = $NF } END { if (addr !~ /:/) print addr }'
        )
    fi
    printf '%s' "$found"
}

waited=0
target=$(resolve)
while [ -z "$target" ]; do
    if [ "$waited" -ge 120 ]; then
        echo "onion: $TARGET_HOST still does not resolve after ${waited}s — is the relay service on this network?" >&2
        exit 1
    fi
    waited=$((waited + 1))
    sleep 1
    target=$(resolve)
done

cp /etc/tor/torrc-onion "$GENERATED"
echo "HiddenServicePort 80 $target:$TARGET_PORT" >> "$GENERATED"
echo "onion: forwarding to $TARGET_HOST ($target:$TARGET_PORT)"

# Publish the address once Tor has minted (or reloaded) the key, and watch the
# target for the one change Tor cannot follow on its own. Backgrounded so `tor`
# stays the container's main process and keeps receiving its signals.
(
    # A Tor that never writes a hostname is a broken config, and a loop that
    # waits on it forever in silence is how that goes unnoticed.
    waited=0
    while [ ! -s "$HS_DIR/hostname" ]; do
        if [ "$waited" -ge 300 ]; then
            echo "onion: no hostname under $HS_DIR after ${waited}s — see the tor log above" >&2
            exit 1
        fi
        waited=$((waited + 1))
        sleep 1
    done

    address=$(cat "$HS_DIR/hostname")
    # Written, then moved into place, so a relay reading concurrently sees the
    # whole address or no file at all — never half of one.
    printf '%s\n' "$address" > "$OUT.tmp"
    chmod 644 "$OUT.tmp"
    mv "$OUT.tmp" "$OUT"

    echo "onion: this relay is reachable at ws://$address (published to $OUT)"

    # Docker hands a re-created container a new IP, and the address above was
    # frozen into the config at startup: from then on Tor would forward the
    # world to whatever now answers at the old one, or to nothing. Nothing in
    # Tor re-resolves it, so this restarts the container instead — the key is
    # on a volume, so the .onion address survives and only the descriptor
    # blinks.
    while sleep 30; do
        current=$(resolve)
        if [ -n "$current" ] && [ "$current" != "$target" ]; then
            echo "onion: $TARGET_HOST moved from $target to $current — restarting so Tor re-resolves it"
            kill -TERM 1
            exit 0
        fi
    done
) &

exec tor -f "$GENERATED"
