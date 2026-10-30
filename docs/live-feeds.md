# Live feeds: how many open subscriptions one relay holds

Measured 2026-10-29 in a 4-core, 15 GB sandbox: one relay process (`-Xms2g
-Xmx4g`, what compose's 6 GB limit gives), Vespa capped at 6 GB, and the load
(`relay/tools/live-feeds.mjs`, four processes) all on the same four cores, over
loopback, on a near-empty store. Every number here is a reading taken on a date,
not a constant; take scale from a fresh run.

## The method

`relay/tools/live-bench/` runs every measurement below: `cpu.sh` for one step,
`compare.sh` to alternate variants, `memory.sh`, `churn.sh`, `stalled.sh`,
`profile.sh` and `strfry.sh`. Its README says how to read them.

A feed is one `REQ` for `{"kinds":[1],"limit":1,"search":"include:spam"}` held
open after its EOSE: the worst case for fan-out, since every note matches every
feed. Each step opens sockets until it holds the step's feeds, waits for every
EOSE, then one publisher writes kind-1 notes at a fixed rate for the step's
hold. Each step counts the frames every feed was owed and received, and how late
they were (publish sent to frame received).

```bash
RELAY_PID=$(pgrep -f 'java.*vespa-relay') node relay/tools/live-feeds.mjs ws://127.0.0.1:7777
RELAY_PID=… STEPS=20000,40000,60000,80000 PER_SOCKET=5 RATE=2 node relay/tools/live-feeds.mjs ws://127.0.0.1:7777
```

## Readings

One feed a socket, 5 events a second, 20 s a step:

| feeds | frames/s | delivered | p50 | p99 | max | relay CPU |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 5k | 100% | 41 ms | 108 ms | 140 ms | 0.4 cores |
| 4,000 | 20k | 100% | 78 ms | 563 ms | 658 ms | 1.1 |
| 8,000 | 40k | 100% | 127 ms | 288 ms | 370 ms | 1.4 |
| 12,000 | 60k | 100% | 375 ms | 817 ms | 1.1 s | 1.7 |
| 16,000 | 80k | 100% | 630 ms | 1.2 s | 1.5 s | 1.9 |
| 19,000 | 95k | 100% | 889 ms | 1.6 s | 1.9 s | 2.0 |
| 16,000 at 1 ev/s | 16k | 100% | 205 ms | 457 ms | 637 ms | 1.1 |

Five feeds a socket, 2 events a second:

| feeds | sockets | delivered | p50 | p99 | EOSE p50 | relay CPU | RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 20,000 | 4,000 | 100% | 121 ms | 236 ms | 13.6 s | 0.7 | 2.5 GB |
| 40,000 | 8,000 | 100% | 247 ms | 416 ms | 16.6 s | 1.2 | 2.5 GB |
| 60,000 | 12,000 | 100% | 448 ms | 1.1 s | 15.5 s | 1.8 | 2.6 GB |
| 80,000 | 16,000 | 100% | 2.6 s | 6.0 s | 11.7 s | 2.3 | 3.9 GB |

What falls out:

- **Nothing was lost and no socket dropped**, at any step. The ceiling is
  latency, and latency follows frames a second (feeds × matching events), not
  feeds held: 16,000 feeds cost 205 ms at 1 ev/s and 630 ms at 5.
- **The publisher's OK is as late as the frames.** Its p50 tracked delivery
  p50 at every step (55 ms alone, 900 ms at 19,000 feeds), so the write path
  waits on the fan-out: a large fan-out slows every publish, the mirror lane's
  included.
- **The replay is the reconnect wall.** Each `limit:1` REQ is a store query
  before its EOSE. One a socket at 1,000 sockets a second came back in 1–2 s;
  five a socket (5,000 REQs a second) queued to 12–17 s, with `/pressure`
  climbing past 100 s. A relay restart that brings every client back at once is
  this burst.
- **Above ~16,000 sockets the box, not the relay, is the bound:** the load took
  1.25 cores and Vespa the rest. The 80,000 row is a saturated machine.
- **A lensed feed delivers nothing from an author its observer has not
  scored**, by design (`LiveGate`); the run above uses `include:spam` for that
  reason. Measuring the gate's own cost under fan-out needs a seeded trust graph.

## Limits of the sandbox

The sandbox caps a process at 20,000 open files and cannot raise it, so one
relay held at most ~19,000 sockets; past that the runs stack feeds a socket.
One destination address gives ~28,000 local ports; `URLS` spreads the load over
`127.0.0.2…`.

## Against strfry

Same sandbox, same tool, 2026-10-29: 12,000 feeds at 5 events a second, CPU
read from `/proc` over 30 s of publishing. strfry 1.1.2-148 (C++ on
uWebSockets, `ghcr.io/hoytech/strfry`) with compression off and host
networking, `LENS=none` since it has no NIP-50; ours is the native websocket
(`NATIVE_WEBSOCKET=true`), alternated with strfry's two runs.

| | user CPU | kernel CPU | total | delivery p50 | p99 |
|---|---:|---:|---:|---:|---:|
| strfry, run 1 | 0.08 cores | 0.66 | 0.74 | 469 ms | 910 ms |
| strfry, run 2 | 0.08 | 0.69 | 0.77 | 573 ms | 1.15 s |
| vespa-relay, native websocket | 0.77 | 0.80 | 1.57 | 106 ms | 210 ms |

- **strfry spends a tenth of our user-space CPU per frame** (~1.3 µs against
  ~13 µs), but the kernel's share, one socket write per frame, is most of
  both, so the total is about 2× ours per frame, not 10×.
- **It buys its lower kernel time with latency.** Its delivery has a ~100 ms
  floor even at 50 feeds, and at 12,000 a median 4–5× ours: it gathers events
  and writes them together, so more frames share a write. Our native path
  writes each frame as it comes.
- strfry's process held 27 MB; ours runs a 2–4 GB heap sized up front, so
  that pair is not a per-socket comparison. strfry also does none of the
  lens, trust gate or Vespa work an edge relay does.

## The JVM wins, against strfry again

Same setup, later the same day, every build alternated with the others:

| build | user CPU | kernel CPU | total | delivery p50 | p99 |
|---|---:|---:|---:|---:|---:|
| native websocket, before | 0.77 cores | 0.79 | 1.56 | 104 ms | 201 ms |
| + pre-encoded frames, one tick timer per event loop (`02bffcb`) | 0.42–0.44 | 0.68–0.71 | 1.10–1.15 | 97–106 ms | 184–208 ms |
| + Netty's native epoll transport | 0.41 | 0.68 | 1.09 | 98 ms | 194 ms |
| + the quartz fan-out patch (prototype) | 0.32–0.36 | 0.70–0.75 | 1.02–1.11 | 90–101 ms | 179–199 ms |
| strfry | 0.09–0.10 | 0.70–0.73 | 0.79–0.83 | 725–926 ms | 1.6–1.75 s |

- **Pre-encoded frames and one timer per loop took 45% off user CPU.** A
  frame is one pooled buffer holding its header and UTF-8, written from the
  session's place in the pipeline; the per-connection 1 s tick had kept
  thousands of entries in each loop's scheduled-task queue.
- **Epoll changed nothing measurable** and is not added.
- **The quartz patch took another 20%** (measured here by loading the
  patched classes ahead of quartz 2.4.0; since released in quartz 2.7.3, see
  below): kinds matched without boxing, `FilterIndex.candidatesFor` returning
  a lone bucket uncopied, and an optional `LiveFrameSink` that hands the
  transport each live frame's prefix and the event's shared body, so the
  body is encoded once per event loop instead of once per subscription.
  Quartz's own `FilterIndexTest`, `FilterMatcherTest` and
  `FilterMatcherEquivalenceTest` pass against the patched classes.
- **The gap to strfry went from 2.1× to about 1.3× CPU per frame**, all of
  it user space: kernel time is now the same for both. Our median delivery
  stays 7–9× faster, because strfry batches.

## On quartz 2.7.3

The fan-out changes shipped in quartz 2.7.3, and the relay takes them
(`LiveFrameSink` in `NativeWebSocket.kt`). Measured on the release, each
build run twice and alternated with the other and with strfry:

| build | user CPU | kernel CPU | total | delivery p50 | p99 |
|---|---:|---:|---:|---:|---:|
| quartz 2.4.0, before | 0.41–0.47 cores | 0.65–0.69 | 1.06–1.16 | 94–102 ms | 187–204 ms |
| quartz 2.7.3, body encoded once per event loop | 0.32–0.33 | 0.70–0.71 | 1.02–1.04 | 90–92 ms | 183 ms |
| strfry | 0.09 | 0.67 | 0.76 | 477 ms | 963 ms |

The release matches the prototype: user CPU 22–30% lower, latency a little
better, kernel time unchanged. The remaining gap to strfry, about 1.35× per
frame, is all user space, and our median delivery is 5× faster.

## On quartz 2.8.0

Quartz 2.8.0 fans a live event out on the thread that delivers it. The native
websocket declares each socket's event loop (`liveExecutor`): the ingest thread
hands an event once to every loop that holds a match, and the loop matches and
writes its own sockets, flushing each once per event (since batched, see
[One write per socket per flush](#one-write-per-socket-per-flush)). The publisher's OK no
longer waits for the fan-out. Ktor's path declares nothing and stays inline.

At 12,000 feeds and 5 events/s, JDK 27, each build twice and alternated:

| build | user CPU | kernel CPU | delivery p50 | p99 |
|---|---:|---:|---:|---:|
| quartz 2.7.3 | 0.31–0.33 cores | 0.61–0.65 | 78–89 ms | 154–273 ms |
| quartz 2.8.0, fan-out per event loop | 0.31–0.33 | 0.59–0.65 | 80–88 ms | 143–178 ms |

The same CPU: the work moved to the loops, it did not shrink. What moves is
the worst case, every note to every feed at 8 feeds a socket (`ramp.sh`,
alternated, the same day):

| feeds | 2.7.3 p99 | its OK p50 | 2.8.0 p99 | its OK p50 | relay CPU, 2.7.3 / 2.8.0 |
|---:|---:|---:|---:|---:|---:|
| 32,000 | 267 ms | 87 ms | 160 ms | 18 ms | 0.18 / 0.18 cores |
| 64,000 | 248 ms | 180 ms | 223 ms | 17 ms | 0.31 / 0.30 |
| 96,000 | 410 ms | 247 ms | 328 ms | 18 ms | 0.46 / 0.43 |
| 128,000 | 787 ms | 378 ms | 449 ms | 17 ms | 0.69 / 0.58 |
| 144,000 | 622 ms | 408 ms | 697 ms | 17 ms | 0.71 / 0.67 |

- **The publisher's OK no longer grows with the audience**: 17–18 ms at every
  step, where 2.7.3's waited for every delivery and reached 408 ms.
- **Delivery holds the target 32,000 feeds further**, to 128,000 (p99 449 ms
  against 787). At 144,000 both miss with the relay under 0.7 cores: four Node
  processes parsing 144,000 frames a second on the same four cores are the
  bound there, so this box cannot say where 2.8.0's own limit is.
- **At 16 feeds a socket** 2.8.0 delivered 144,000 feeds at p99 402 ms
  (0.51 cores), 192,000 at 549 ms and 256,000 at 724 ms (0.92 cores).
- **Both builds left a few replays unanswered.** From 96,000 feeds on, 1–6
  sockets of up to 18,000 still waited for an EOSE after 60 s, and 2–7 REQs
  were closed with `error: Expected start of the object '{', but had 'EOF'`:
  the store's `/search/` path takes a 2xx with an empty body and fails to
  decode it (vespa-eventstore `VespaEventIndex.queryBody`), seen only with the
  replay queue 20–60 s deep. `live-feeds.mjs` used to count a socket still
  waiting as neither open nor failed; it counts as a failure now, so in these
  runs `ramp.sh`'s strict capacity is 64,000 feeds for 2.7.3 (2 sockets
  unanswered at 96,000; the earlier run under Capacity had none) and 96,000
  for 2.8.0.
- **The first ramp past 20,000 sockets found a bug older than both builds**:
  out of file descriptors, the relay stopped accepting connections for good
  ([docs/traps.md](traps.md); `live-bench/fd-exhaustion.sh` checks it). The
  relay now loads the JDK's zone rules at boot, and `docker-compose.yml`
  sets the relay's `nofile` limit (`RELAY_NOFILE`, 524,288) rather than
  inheriting the engine's.

## One write per socket per flush

2.8.0 as first adopted flushed every socket at the end of each event's
fan-out: one `write()` per socket per event. At 2,000 feeds and 50 events/s
that is 100,000 writes a second, and delivery missed the target in all four
runs (p99 409, 479, 629, 785 ms) where 2.7.3, whose one drain task took
whatever had queued for a socket, held 126–144 ms. Now:

- **A live frame is one buffer**: header, prefix and a copy of the event's
  bytes, which each loop still encodes once. It was a head and a retained
  duplicate of the body, two entries for the socket's write queue to track
  and release. Bodies over 4 KiB keep the shared duplicate: a long-form
  event reaching thousands of sockets is not copied into each.
- **A socket's live frames wait for its loop's flush, in one buffer.** The
  flush is a task queued behind the fan-out: on an idle loop it runs next,
  and under load it waits out the fan-outs already queued, so the events
  those carry reach a socket in one write. Nothing waits on a timer.
  A drain writes the held frames ahead of anything queued after them, and a
  closing socket releases them.
- **A batch is bounded by the socket's high-water mark.** Past it the batch
  goes to the socket unflushed, so the socket turns unwritable and what
  follows takes the outbox, whose slow-consumer limits apply; held frames
  are invisible to those limits, so without the bound a burst queued on a
  loop could grow one socket's batch without end.

Each build twice, alternated, JDK 27:

| load | build | user CPU | kernel CPU | delivery p50 | p99 |
|---|---|---:|---:|---:|---:|
| 12,000 × 5/s | flush per event | 0.32–0.33 cores | 0.64–0.70 | 91–94 ms | 176–194 ms |
| | one write per flush | 0.26–0.31 | 0.63–0.66 | 86–95 ms | 158–177 ms |
| 2,000 × 20/s | flush per event | 0.31–0.33 | 0.35 | 26 ms | 46–52 ms |
| | one write per flush | 0.27–0.30 | 0.34 | 25–26 ms | 49–79 ms |
| 2,000 × 50/s | flush per event | 0.48–0.51 | 0.73–0.81 | 267–338 ms | 629–785 ms |
| | one write per flush | 0.44–0.47 | 0.67–0.68 | 66 ms | 164–174 ms |

- **The 50 events/s regression is gone**: p99 4× lower, back near 2.7.3's,
  and kernel time lower with fewer writes.
- **User CPU is 5–20% lower** at the rates below it; a third pair of runs at
  12,000 × 5 read 0.29–0.31, so the gain there is within the runs' spread of
  about ±0.03 cores.
- **At 8 feeds a socket**, every note to every feed, 128,000 feeds held the
  target in all three runs (p99 482–491 ms; flushing per event, 544 ms). The
  ramp's 96,000 step, here a jump from 2,000 sockets to 12,000, queues
  80,000 replays at once, each waiting 20 s on average for the store with
  Vespa on the same four cores: two runs left 2,709 and 5,421 sockets
  without an EOSE past 60 s (flushing per event, 118), and the third had
  every socket answered and passed, p99 459 ms. Delivery to the sockets that
  were answered stayed in target.
- **What is left on the loops is mostly cache misses.** The profile at
  12,000 × 5 puts 25–28% in Netty's pipeline write and the rest spread over
  the first touch of each socket's objects in an event: 21% landed on
  `context.alloc()`, the first read of each socket's context and channel
  config. Keeping the allocator in the session moved those samples to the
  next touch (`liveEvent`, quartz's `sendLive`) and the CPU did not change, so
  it is not kept. Writing past the pipeline would skip Ktor's handlers and
  the handshaker's encoder that sit in it.

### Against strfry at higher rates

strfry 1.1.2-148, the same tool and day (not alternated with the build
above; vespa-relay figures from the table above):

| load | | user CPU | kernel CPU | delivery p50 | p99 |
|---|---|---:|---:|---:|---:|
| 2,000 × 20/s | strfry | 0.06 cores | 0.28 | 114 ms | 218 ms |
| | vespa-relay | 0.27–0.30 | 0.34 | 25–26 ms | 49–79 ms |
| 2,000 × 50/s | strfry | 0.10 | 0.60 | 359 ms | 621 ms |
| | vespa-relay | 0.44–0.47 | 0.67–0.68 | 66 ms | 164–174 ms |

- strfry's user space stays 4–5× smaller; kernel time is within 10–20%, and
  our delivery is 4–5× faster, as at 5 events/s.
- **4,000 × 50/s (200,000 frames a second) is past the load generator**,
  whose four Node processes read about 144,000 frames a second here. Both
  relays fell behind there and neither number is the relay's: strfry kept
  every socket and buffered (242 MB, 58% delivered, p50 15.5 s); vespa-relay
  closed the readers that stopped reading (75.7% delivered before it did).

## io_uring, tried and not taken

Netty 4.2's io_uring transport, handed to Ktor through the server bootstrap
in place of Java NIO, on the 2.7.3 build above, each alternated twice:

| transport | user CPU | kernel CPU | total | delivery p50 | p99 |
|---|---:|---:|---:|---:|---:|
| Java NIO (the default) | 0.33–0.35 cores | 0.64–0.67 | 0.99–1.00 | 90 ms | 174–187 ms |
| Netty io_uring | 0.41–0.42 | 0.82 | 1.23–1.24 | 102–103 ms | 195–227 ms |

A quarter more CPU, kernel and user both, and higher latency, so it is not
kept. The kernel cost is the TCP send per frame, which every transport pays;
the io_uring servers that gained elsewhere batch whole passes of sends into
one submission themselves, which the stock transport does not do for us.
Native epoll measured no different from NIO either (above).


## JVM settings and runtimes

The quartz 2.7.3 build, native websocket, 12,000 feeds at 5 events/s, each
setting run twice and alternated with the others (`live-bench/compare.sh`):

| setting | user CPU | kernel CPU | total | delivery p99 |
|---|---:|---:|---:|---:|
| G1, `-Xms2g -Xmx4g` | 0.31–0.32 cores | 0.62–0.71 | 0.94–1.02 | 186–234 ms |
| G1, fixed and pre-touched 4 GB | 0.34 | 0.64–0.67 | 0.98–1.01 | 170–190 ms |
| ParallelGC | 0.31–0.32 | 0.66–0.67 | 0.97–0.99 | 171–195 ms |
| generational ZGC | 0.38–0.39 | 0.64–0.68 | 1.02–1.07 | 179–186 ms |
| G1, `-UsePerfData`, `UseSystemMemoryBarrier` | 0.33–0.36 | 0.66–0.68 | 0.99–1.04 | 173–189 ms |

ZGC costs a fifth more user CPU for nothing at this size; the rest are one
another's noise, though a fixed, pre-touched heap trims the tail a little.

The same build on JDK 21, 25 and 27, each with compact object headers off and
on, matched within noise for CPU (0.30–0.35 cores user) and delivery. Memory
did not: live heap per idle feed was 7.65 KB on 21 and 25, and 6.95 KB with
compact headers, which JDK 27 turns on by default (6.97 KB with no flag). The
image runs 27 for that (docs/decisions/build.md).

## Capacity

`live-bench/ramp.sh` adds feeds on one relay until a step misses the target:
every socket open, none dropped, at least 99.9% of owed frames delivered, and
p99 under 500 ms. JDK 27, the 2.7.3 build, 30 s of publishing per step, the
first step a warm-up. The sandbox caps a process near 19,000 sockets, so feeds
are stacked several to a socket.

Every note to every feed (`kinds:[1]`), 1 event/s, 8 feeds a socket (quartz
2.7.3; 2.8.0 is above):

| feeds | p50 | p99 | relay CPU |
|---:|---:|---:|---:|
| 32,000 | 94 ms | 207 ms | 0.22 cores |
| 64,000 | 188 ms | 359 ms | 0.42 |
| 96,000 | 257 ms | 457 ms | 0.55 |
| 128,000 | 306 ms | 598 ms (fails) | 0.68 |

Follow lists: each feed follows 20 of 1,000 authors, so a note reaches about 2%
of feeds; 10 events/s, 25 feeds a socket:

| feeds | p50 | p99 | relay CPU |
|---:|---:|---:|---:|
| 50,000 | 19 ms | 32 ms | 0.25 cores |
| 100,000 | 23 ms | 41 ms | 0.37 |
| 200,000 | 39 ms | 77 ms | 0.69 |
| 300,000 | 49 ms | 106 ms | 1.07 |
| 400,000 | 62 ms | 186 ms | 1.17 |

- **Holding feeds is not the limit.** 400,000 follow-list feeds passed with room
  to spare; that step was the most this box can open (16,000 sockets × 25).
- **The worst case was bounded by fan-out on one thread.** Latency grows with the
  feeds one event matches while CPU stays low: about 3 µs per matching feed on
  the ingest thread, and the publisher's OK waits for it. Quartz 2.8.0 fans out
  per event loop and moved this (above).
- **Opening feeds is the bottleneck to design for.** Every new subscription's
  replay is a Vespa query; at a few thousand REQs a second the EOSE took 15-60 s.
  A reconnect storm is that, all at once.
- **Feeds sharing a socket are cheaper.** 96,000 frames/s cost 0.55 cores at 8
  feeds a socket, against about 1 core for 60,000 at one: a socket's frames go
  out in one write.
