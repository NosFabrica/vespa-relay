# JFR execution samples (`jfr print --json --events jdk.ExecutionSample`) as shares of the live fan-out path: each
# sample goes to the first area its stack passes through, the rest by their top frame. Shares of Java samples only.
import collections
import json
import sys

AREAS = [
    ("fan-out: subscription index", "FilterIndex.candidatesFor"),
    ("fan-out: filter match", "Filter.match"),
    ("fan-out: replay/live dedupe", "SeenIds.firstSighting"),
    ("fan-out: trust gate", "ObserverBackend"),
    ("fan-out: hand-off to sockets", "NativeWebSocketSession.offer"),
    ("fan-out: other", "LiveEventStore.fanout"),
    ("loop: frame head encode", "encodeLiveFrameHead"),
    ("loop: shared event body", "sharedBody"),
    ("loop: socket write and flush", "AbstractChannelHandlerContext.flush"),
    ("loop: pipeline write", "AbstractChannelHandlerContext.write"),
    ("loop: drain", "NativeWebSocketSession.drain"),
    ("loop: keepalive tick", "NativeWebSocketSession.tick"),
    ("loop: task queue", "pollTaskFrom"),
    ("loop: select", "Selector"),
    ("ktor websocket", "io.ktor.websocket"),
    ("vespa client", "ai.vespa"),
    ("http client", "okhttp3"),
]

events = json.load(open(sys.argv[1]))["recording"]["events"]
areas, threads, total = collections.Counter(), collections.Counter(), 0
for event in events:
    stack = event["values"]["stackTrace"]
    if not stack:
        continue
    total += 1
    threads[event["values"]["sampledThread"]["javaName"].rstrip("0123456789-")] += 1
    frames = [f["method"]["type"]["name"].replace("/", ".") + "." + f["method"]["name"] for f in stack["frames"]]
    joined = " ".join(frames)
    for name, key in AREAS:
        if key in joined:
            areas[name] += 1
            break
    else:
        areas["other: " + frames[0].rsplit(".", 2)[-2] + "." + frames[0].rsplit(".", 1)[-1]] += 1
print(f"{total} Java samples")
for name, count in areas.most_common(20):
    print(f"  {100 * count / total:5.1f}%  {name}")
print("by thread: " + ", ".join(f"{name} {100 * count / total:.0f}%" for name, count in threads.most_common(5)))
