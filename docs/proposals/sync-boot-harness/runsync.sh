#!/bin/bash
# $1 = install dir, $2 = label. Boots the sync against the tarpit for 75s and probes the pages.
DIST=$1; LABEL=$2
export VESPA_URL=http://localhost:18080 AUTO_DEPLOY=false RELAY_URL=ws://localhost:7777/
export RELAY_NSEC=${RELAY_NSEC:?an nsec1 for the throwaway monitor identity}
export SYNC_STATUS_PORT=7778 MONITOR_STATUS_PORT=7779
export SYNC_STATUS_FILE=$PWD/$LABEL-status.json MONITOR_STATUS_FILE=$PWD/$LABEL-monitor.json
export SYNC_CONFIG=$'streams {\n  probe {\n    dir = "down"\n    filter = { "kinds": [0] }\n    urls = [ "ws://127.0.0.1:1/" ]\n  }\n}'
timeout 75 $DIST/bin/vespa-sync > $LABEL.log 2>&1 &
PID=$!
for t in 15 30 45 60; do
  sleep 15
  S=$(curl -s -o /dev/null -w '%{http_code}' -m 3 http://localhost:7778/ || echo refused)
  M=$(curl -s -o /dev/null -w '%{http_code}' -m 3 http://localhost:7779/ || echo refused)
  echo "$LABEL t=${t}s  :7778 -> $S   :7779 -> $M"
done
wait $PID
echo "---- $LABEL boot lines:"; grep -n -E 'status page on|monitor page on|down stream|monitor passes gated|sync identity|store call SLOW|retire' $LABEL.log | head -12
