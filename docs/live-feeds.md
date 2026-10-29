# Live feeds: how many open subscriptions one relay holds

Measured 2026-10-29 in a 4-core, 15 GB sandbox: one relay process (`-Xms2g
-Xmx4g`, what compose's 6 GB limit gives), Vespa capped at 6 GB, and the load
(`relay/tools/live-feeds.mjs`, four processes) all on the same four cores, over
loopback, on a near-empty store. Every number here is a reading taken on a date,
not a constant; take scale from a fresh run.

## The method

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
