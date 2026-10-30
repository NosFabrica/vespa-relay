#!/usr/bin/env bash
# Whether connections leave anything behind: ROUNDS rounds of FEEDS feeds opened and closed, then the live count
# (after a full GC) of the classes one connection makes. A count or a heap that grows round on round is a leak.
#
#   churn.sh [VAR=value ...]       FEEDS (2000), ROUNDS (3)
source "$(dirname "$0")/lib.sh"
FEEDS=${FEEDS:-2000}
ROUNDS=${ROUNDS:-3}
ensure_vespa || exit 1
relay_start "$@" || exit 1
live() { jcmd "$RELAY_PID" GC.class_histogram 2>/dev/null | awk -v c="$1" '$4 == c {print $2; f = 1} END {if (!f) print 0}'; }
for round in $(seq 1 "$ROUNDS"); do
  STEPS=$FEEDS RATE=0 HOLD=1 CHILDREN=2 timeout 300 feeds > /dev/null 2>&1
  sleep 8
  echo "round $round: RelaySession $(live com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession)," \
    "NativeWebSocketSession $(live com.nosfabrica.vespa.relay.server.NativeWebSocketSession)," \
    "NettyHttp1Handler $(live io.ktor.server.netty.http1.NettyHttp1Handler)," \
    "live heap $(jcmd "$RELAY_PID" GC.class_histogram 2>/dev/null | awk '/^Total/ {printf "%.0f MB", $3 / 1048576}')"
done
relay_stop
