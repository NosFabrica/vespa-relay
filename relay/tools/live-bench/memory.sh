#!/usr/bin/env bash
# Live heap and kernel memory per idle feed: a live-object histogram (a full GC first) before and after FEEDS idle
# feeds open, the difference per feed with the classes that make it up, and the TCP slab's growth per connection.
# Loopback holds both ends of a connection in one kernel, so the slab growth is halved.
#
#   memory.sh [VAR=value ...]      FEEDS (12000) and PER_SOCKET (1) shape the feeds.
#
# Two runs at PER_SOCKET 1 and 5 separate the cost per socket from the cost per subscription:
#   per feed at 1 = S + F;  per feed at 5 = S/5 + F.
source "$(dirname "$0")/lib.sh"
FEEDS=${FEEDS:-12000}
ensure_vespa || exit 1
relay_start "$@" || exit 1
sleep 5
slab() { awk '$1 == "TCP" {print $2 * $4}' /proc/slabinfo; }
jcmd "$RELAY_PID" GC.class_histogram > "$BENCH_OUT/heap-before.txt" 2>/dev/null
slab_before=$(slab)
log="$BENCH_OUT/memory-load.txt"
LINGER=120 STEPS=$FEEDS RATE=0 HOLD=1 CHILDREN=${CHILDREN:-4} feeds > "$log" 2>&1 &
for _ in $(seq 1 120); do grep -q "holding" "$log" && break; sleep 2; done
sleep 10
jcmd "$RELAY_PID" GC.class_histogram > "$BENCH_OUT/heap-after.txt" 2>/dev/null
slab_after=$(slab)
relay_stop
pkill -f "[l]ive-feeds.mjs" 2>/dev/null
python3 - "$BENCH_OUT/heap-before.txt" "$BENCH_OUT/heap-after.txt" "$FEEDS" "$slab_before" "$slab_after" <<'EOF'
import re, sys
def load(path):
    rows, total = {}, 0
    for line in open(path):
        m = re.match(r"\s*\d+:\s+(\d+)\s+(\d+)\s+(\S+)", line)
        if m: rows[m[3]] = (int(m[1]), int(m[2]))
        t = re.match(r"Total\s+(\d+)\s+(\d+)", line)
        if t: total = int(t[2])
    return rows, total
before, tb = load(sys.argv[1]); after, ta = load(sys.argv[2]); n = int(sys.argv[3])
print(f"live heap +{(ta - tb) / n:.0f} bytes per idle feed ({(ta - tb) / 2**20:.0f} MB for {n}); "
      f"kernel TCP slab +{(int(sys.argv[5]) - int(sys.argv[4])) / 2 / n:.0f} bytes per connection")
grow = sorted(((after.get(k, (0, 0))[1] - before.get(k, (0, 0))[1], after.get(k, (0, 0))[0] - before.get(k, (0, 0))[0], k)
               for k in set(before) | set(after)), reverse=True)
for size, count, name in grow[:15]:
    if size <= 0: break
    print(f"  {size / n:6.0f} B  {count / n:5.1f} objects per feed  {name}")
EOF
