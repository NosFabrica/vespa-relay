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
EXTRA=${ONION_EXTRA_TORRC:-/etc/tor/onion.extra.conf}

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
    # An IPv4 answer, from whichever lookup this image's libc actually carries.
    # `getent hosts` returns only the FIRST entry, so on a dual-stack name it
    # can hand back the IPv6 one — and the relay's Ktor server binds 0.0.0.0,
    # so forwarding there would publish an onion that never answers. `ahostsv4`
    # is glibc's spelling, `ahosts` is the one musl also has, `hosts` is the
    # floor; a lookup this image lacks simply answers nothing and falls through.
    for lookup in ahostsv4 ahosts hosts; do
        found=$(getent "$lookup" "$TARGET_HOST" 2>/dev/null | awk '$1 !~ /:/ { print $1; exit }')
        if [ -n "$found" ]; then
            printf '%s' "$found"
            return 0
        fi
    done

    # busybox's nslookup, for an image with no getent at all. The resolver's
    # own address line carries a :53, which the same test skips.
    found=$(
        nslookup "$TARGET_HOST" 2>/dev/null |
            awk '/^Address/ && $NF !~ /:/ { addr = $NF } END { print addr }'
    )
    if [ -n "$found" ]; then
        printf '%s' "$found"
        return 0
    fi

    # IPv6-only network: there or nowhere. as_target brackets it, because
    # `fd00::2:7777` unbracketed still PARSES — as the address fd00::2:7777 on
    # the default port — so the service would come up healthy and forward the
    # world to nothing. Verified against tor 0.4.8: both spellings are "valid".
    getent hosts "$TARGET_HOST" 2>/dev/null | awk 'NR == 1 { print $1; exit }'
}

# An IPv6 literal needs brackets before the `:port`; an IPv4 address must not
# have them.
as_target() {
    case "$1" in
        *:*) printf '[%s]' "$1" ;;
        *) printf '%s' "$1" ;;
    esac
}

# The relay may still be starting; docker's DNS only answers once its container
# exists. Bounded, because a name that never resolves is a compose file with the
# wrong service name and waiting on it forever says nothing.
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
echo "HiddenServicePort 80 $(as_target "$target"):$TARGET_PORT" >> "$GENERATED"

# Anything the operator wants this service to carry that the bundled torrc does
# not — more introduction points, tor's onion-service proof-of-work defenses,
# single-hop mode for a relay whose address is public anyway. Mounted, so it
# needs no rebuild; appended last, so it wins on any option tor takes once.
# tor/onion.extra.conf.example is the file, and carries the stanzas.
#
# Comments and blank lines do not count as content: the example file is mounted
# by DEFAULT, and "appended 40 lines" on a deployment that changed nothing is a
# log line that trains operators to ignore this one.
if [ -s "$EXTRA" ] && grep -qv '^[[:space:]]*\(#.*\)\?$' "$EXTRA"; then
    printf '\n# --- from %s ---\n' "$EXTRA" >> "$GENERATED"
    cat "$EXTRA" >> "$GENERATED"
    echo "onion: appended $(wc -l < "$EXTRA") line(s) of extra config from $EXTRA"
fi

# Readable by the tor user: tor parses this as root before dropping privileges,
# but a reload after the drop would have to read it again.
chmod 644 "$GENERATED"
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
    while sleep 10; do
        current=$(resolve)
        if [ -n "$current" ] && [ "$current" != "$target" ]; then
            echo "onion: $TARGET_HOST moved from $target to $current — restarting so Tor re-resolves it"
            kill -TERM 1
            exit 0
        fi
    done
) &

exec tor -f "$GENERATED"
