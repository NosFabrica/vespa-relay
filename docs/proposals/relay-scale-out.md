# Proposal: serving the relay from more than one process

Status: proposal, nothing built. Written 2026-11-09 against `c6951a0`
(quartz `0102e83c81`, vespa-eventstore `116f12e1c87a`).

The mirror and the monitor are processes of their own now, so the serving relay
is the one process left that every client holds a socket to. This note is how to
run more than one of it: what one relay process holds today that pins it to one
copy, the structure that lets websocket connections spread over several, and the
order to build it in.

**The short answer.** Split the relay in two roles: a **writer**, which is
today's relay unchanged and stays the only process that writes the store, and
any number of **edges**, which hold the client websockets, answer reads straight
from Vespa, forward publishes to the writer, and take every accepted event back
from the writer on one internal socket to fan out to their own subscribers. A
deployment with no edges is today's deployment. Sharding the writer by owner is
a later, separate step that this structure leaves room for and does not need.

## What one relay process holds today

Everything below is per process. The question for each is what happens when a
second copy runs beside the first behind a load balancer.

| state | where | with two copies |
|---|---|---|
| **Live fan-out** | quartz `LiveEventStore`: a `FilterIndex` of open REQs, fed only by events that pass through *this* process's `EventIngest` and come back `Accepted` | a note published on copy A never reaches a live REQ on copy B; the mirror's and the monitor's writes reach only the copy they dialled |
| **The single writer** | the store's `writes` mutex orders dedup, supersession and the NIP-09/62 guards; `docs/decisions/ingest.md`, "One writer" | two writers race: two live versions of one replaceable, a covered event admitted beside its kind 5 (vespa-eventstore `docs/multi-node-consistency.md`, "Multiple WRITERS are the threat") |
| **Mirror lane and delete hook** | `MirrorLane`, `MirrorDeleteHandler`; the sync and the monitor dial one `SYNC_INGEST_URL` / `MONITOR_INGEST_URL` | each would need to pick a copy, and every copy would be a writer |
| **Boot and maintenance work that writes** | `deploySchema` (`AUTO_DEPLOY`), `TRUST_RECONCILE_ON_START` (on by default), `REINDEX_FTS_ON_START`, `SWEEP_ORPHAN_SCORES_ON_START`, `VERIFY_TRUST_ON_START`, `RelayProfile` (`store.insert` of the relay's kind 0 and 10002), `ExpirationSweeper` | N deploys racing, N reconciles writing the projection, N copies of every sweep; every restart of any copy repeats them |
| **The corpus rollup** | `StatsRollup` behind `/stats.json` | N times the corpus-wide grouping load on Vespa for one document |
| **NIP-86 state** | `BanStore` in `RELAY_STATE_FILE`; NIP-11 rewrites in `MutableRelayInfo`, in memory | a ban made through one copy applies on that copy only; a rename shows on one copy and is gone at its restart |
| **Serving pressure** | `ServingPressure`, read in process by the mirror lane | the lane yields to the reads of the copy it runs in, not to the reads clients are waiting on elsewhere |

What is already fine, and why:

- **Reads.** A REQ, a COUNT, NIP-77, a NIP-FE HTTP command and an ORE search are
  answered from Vespa with no process state between requests. An acked put is
  visible to the next search from any client of the cluster.
- **Per-connection state.** NIP-42, `SearchGate`, negentropy sessions,
  `TrustNotice`, the slow-consumer rule: a websocket is its own affinity, so a
  load balancer needs no stickiness.
- **The store's caches on a process that does not write.** `TrustCells` re-reads
  a cell after 60 s and nothing invalidates it on write today either; the
  provider pass is rebuilt every `TRUST_PROVIDER_REFRESH_SECONDS`, which exists
  because the sync's 10040s already arrive from another process; the guard cache
  is off (`SHARED_STRICT`).

## Why connections are the thing to scale

From `docs/live-feeds.md` (one relay, 4 cores, 2026-10-29):

- Delivery latency follows frames a second, not feeds held: 16,000 feeds at
  5 ev/s ran p50 630 ms on 1.9 cores. Above about 16,000 sockets the box, not
  the relay, was the bound.
- **The publisher's OK waits on the fan-out**, the mirror lane's included. A
  large fan-out slows every write on the one process that writes.
- **The replay is the reconnect wall**: a restart brings every client back at
  once, and each REQ is a store query before its EOSE. With one process every
  deploy is that burst for every client.

`NATIVE_WEBSOCKET=true` measured 3–4× lower delivery latency and a third less
CPU per frame. It is the lever to pull before any of this, and the edges below
should run it.

## The structure

```
                       clients (ws, NIP-FE http)
                                  │
                         load balancer (L4 or L7 ws)
                ┌─────────────────┼─────────────────┐
             edge 1            edge 2      …     edge N        read-only store
                │  ▲              │  ▲              │  ▲        reads → Vespa
     EVENT/OK   │  │ accepted     │  │              │  │
                ▼  │ events       ▼  │              ▼  │
            ┌──────┴──────────────┴──┴──────────────┴──┴──┐
            │                   writer                    │   today's RelayMain:
            │   publishers' lane · mirror lane · hooks    │   the only store writer,
            │   schema, maintenance, rollup, NIP-86       │   maintenance, bans
            └────────────────────────▲──────────▲─────────┘
                                     │          │
                                   sync      monitor        (dial the writer, as now)
```

### The writer is today's relay

`RelayMain` as it stands: it deploys the schema, runs every maintenance job,
holds the ban file, runs the rollup, owns the mirror lane and the delete hook.
The sync and the monitor keep dialling it at `SYNC_INGEST_URL` and
`MONITOR_INGEST_URL`. Clients may still connect to it directly, which is how a
deployment with no edges keeps working.

What it learns: an **edge key**. Edges sign in (NIP-42) with a key the writer
lists in a new `RELAY_EDGE_PUBKEYS`, the way the mirror lane is chosen by who
signed in and not by a port or a header. A connection signed in as an edge:

- writes on the **publishers' lane**, never the mirror lane: an edge forwards
  clients' publishes, which must not queue behind a backfill;
- is exempt from the per-connection limits (subscriptions, rate), which the edge
  already applied per client;
- may hold the **live link** REQ below, unlensed: a signed-in connection is
  otherwise read through its own pubkey's lens (`ObserverBackend.ranked`), which
  must not gate the link;
- may skip signature verification on the writer (`EventIngest.submit`'s
  `skipVerify`), since the edge verified it. Measure before taking this: it is
  CPU the writer stops spending, and the trust is one key in one env var.

### An edge is a relay that cannot write

A second entrypoint in `:relay`, `EdgeMain`, built from the same
`NostrRelayServer`, policy stack, `HttpServer` and `NativeWebSocket`:

- **The store opens read-only** (`ReadOnlyStore`, as the sync and the monitor
  open it): it waits for the writer's schema, deploys nothing, and cannot write
  by construction. None of the maintenance switches exist on an edge; setting
  one stops its boot, the way a sync setting stops the relay's.
- **Its `EventIngest` forwards.** A client's EVENT runs the edge's whole policy
  stack (bans, allow and deny lists, limits, NIP-42, signature), then goes to the
  writer over a socket signed in with the edge key. The writer's `OK` comes back
  through the existing `outcomeOf` mapping and goes to the client as the store's
  own words. A writer that is down answers `OK false error:` and nothing waits.
  `RelayWriter` already does this batched; the edge needs its one-OK-per-event
  form, with the same redial and back-off.
- **Its live fan-out is fed by the link.** One socket to the writer holds
  `["REQ","live",{"limit":0}]`: no replay, then every event the writer accepts,
  whoever sent it. The edge hands each to its own `LiveEventStore` index to fan
  out to its own subscribers. Quartz has no entry point for "an event another
  process stored"; `fanout` is private and only an accepted `submit` reaches it.
  That belongs upstream (a `LiveEventStore.announce(event)` that fans out
  without writing). Until it lands, the edge's ingest can report the link's
  events `Accepted` without writing them and skip the copy of its own forwarded
  publishes, which it already delivered on the `OK`.
- **A 10040 on the link refreshes the provider pass** (`refreshTrustProviders()`),
  so a reader's new lens applies on every edge as it applies on the writer
  today, not a refresh interval later.
- **What it reads from the writer.** The ban list, the NIP-11 overrides and the
  `/stats.json` document, fetched from the writer on an interval and when the
  writer says they changed. NIP-86 calls that arrive at an edge are proxied to
  the writer as they came (body and `Authorization`); the NIP-98 `u` tag names
  the public URL, which both sides already know. A ban that has not reached an
  edge yet can delay a refusal on reads there, never a write: the writer's own
  `BanListPolicy` still refuses the EVENT.
- **What stays its own.** `/pressure`, `/pulse.json`, the connection count, the
  HTTP command cap (`HTTP_RELAY_TOTAL`) and the search lanes are per edge, and
  say so on their pages. The writer's mirror lane reads every edge's `/pressure`
  and yields to the worst, the way the sync's `PressurePoller` reads one.

### Why the link is a Nostr socket and not a message bus

NATS or Redis Streams would carry the fan-out as well. The link uses NIP-01
because everything it needs is already here: the writer's own fan-out, NIP-42
lanes, `RelayWriter`'s redial, `OK` semantics, and one wire format to debug with
the tools this repo already has (`SYNC_WIRE_LOG`, the probes). The writer fans
out to N edge sockets instead of thousands of client feeds, which takes the
fan-out off the write path, the cost `live-feeds.md` measured on the publisher's
`OK`. If the link becomes the bound, the edge's link reader is one class to
replace.

What the link costs: every edge receives every event the writer accepts, mirror
backfill included, which is what the one relay pushes through its own index
today. At the ingest rates `docs/ingest-benchmark.md` measures that is tens of
MB/s per edge on an internal network during a backfill. A filter on the link
(only events a live REQ could match) is the knob if it shows.

### What the link guarantees, and what it does not

- **No loss for an open subscription.** The writer fans out after the write is
  durable, and Vespa serves an acked put to the next search. The edge registers
  a REQ before its replay, as quartz does now: an event stored before the replay
  reads it, one stored after arrives on the link.
- **A duplicate is possible** in a window as long as the link's latency: an
  event the replay read, then delivered again by a link that was a few
  milliseconds behind. Quartz's replay dedupe settles on the ids its own ingest
  has in flight, and an edge's ingest never had this one. Clients dedupe by id,
  but one relay never sends the duplicate today, so the README's contract has to
  say so if it ships this way.
- **A link that breaks while the writer stays up loses what was accepted in the
  gap.** On redial the edge asks `{"since": <lost at − 60 s>}` and drops ids it
  already delivered, which recovers anything published with a current
  `created_at`. A backdated event (a mirror backfill) accepted in the gap is not
  streamed live; the next REQ serves it, the same trade `LiveGate` makes for a
  match it only reaches by typo. A writer restart loses nothing this way: it
  accepted nothing while it was down.

## Addresses and the load balancer

- `RELAY_URL` is the load balancer's address, and every edge and the writer read
  the same one: NIP-42 and NIP-98 check against it, NIP-11 states it, and the
  sync's boot check of the relay's `self` reads the writer's NIP-11, which
  states the same `RELAY_NSEC` pubkey.
- Any L4 balancer works, or an L7 one that passes the websocket upgrade.
  Nothing needs stickiness. An edge is ready once its store is open and its link
  holds; a balancer that routes before then sends publishes that fail.
- A rolling restart moves one edge's clients at a time. The replay burst that is
  the reconnect wall today becomes 1/N of it.
- The `.onion` service points at the balancer.
- Each edge needs the fd limit the relay has now (`RELAY_NOFILE`), and its own
  heap; an edge holds no ingest queue, so it needs less than the writer.

## Owner lanes, later

When the writer itself is the bound, vespa-eventstore's answer is to shard
writes by owner (`hash(owner) % K`, owner as `EventDoc.owner`: the recipient for
a kind 1059, else the author), since every constraint the store enforces is
scoped to one owner. In this structure that is a routing function in `:peers`,
used by the edge's forwarder, `RelayWriter` and the monitor, and the edge's link
becomes K sockets. It is not part of this proposal, for three reasons:

- One writer's ceiling is not measured to be close. Publishes go through the
  batching `IngestPipeline`; the per-event `writes` lock ceiling (~143/s,
  vespa-eventstore `docs/locking.md`) applies to single inserts. Read
  `lock.ingest.wait` on the writer's pulse first.
- The store's in-process alternative comes first: striped owner locks
  (`docs/locking.md`, "Owner-scoped exclusion"), one process, no routing.
- Some work spans owners: the delete hook's id lists, a NIP-86 purge, the
  expiration sweep, the reconciles, and the trust gate's service walk, which
  orders a newly named service's cards (owner: the service) against a 10040
  (owner: the reader). Each needs an answer from the store before lanes are
  correct, and that is the store's design to make.

## Alternatives considered

- **N full relays behind a balancer, plus a bus.** Every copy writes, which is
  the multi-writer race the forwarding design was built to remove.
- **Owner-routed relays at the balancer.** A balancer cannot see an EVENT's
  owner, and one socket carries many owners' events and REQs; reads and live
  feeds would still need the link. Owner routing has to sit behind something
  that parses frames, which is the edge.
- **A bigger box and `NATIVE_WEBSOCKET`.** The first lever, and enough until
  sockets or fan-out outgrow one JVM. It does nothing for a restart's
  reconnect burst.

## Order of work

1. **Measure.** `relay/tools/live-bench/` on the production shape with
   `NATIVE_WEBSOCKET=true`: where the relay process saturates (sockets, frames,
   CPU) against where Vespa does. If Vespa saturates first, more edges buy
   nothing and the answer is Vespa groups (vespa-eventstore `docs/scaling.md`).
2. **Quartz.** `LiveEventStore.announce(event)`: fan out an event another
   process stored, with the replay dedupe it can offer.
3. **The writer learns the edge key.** `RELAY_EDGE_PUBKEYS`, the publishers'
   lane, the limits exemption, the unlensed link REQ, an internal document for
   bans, NIP-11 overrides and the rollup. Shipped alone it changes nothing for a
   deployment with no edges.
4. **`EdgeMain`.** Read-only store, the forwarding ingest, the link, the writer
   state reader, the NIP-86 proxy; `vespa-edge` beside `vespa-sync` and
   `vespa-monitor` in the image; an `edge` compose profile with replicas and a
   balancer. Tests that name the properties:
   - an event published on edge A reaches a live REQ on edge B, and a mirrored
     event reaches both;
   - a ban made through any edge refuses the author's EVENT on every edge;
   - an edge has no path to write the store and starts no maintenance;
   - a link broken and redialled while the writer stays up delivers the gap's
     current events once.
5. **Owner lanes**, only if step 1 or the writer's pulse says the writer is the
   bound, and after the store's striping.

## Open questions

- Does an edge state its own NIP-11 `self`, or the writer's? The writer's, so
  every address of the relay says one thing; the edge key is internal.
- Is the writer a single point of failure worth fixing? It is one today.
  With edges, reads and open feeds survive a writer restart and publishes fail
  fast; an active/passive writer is a separate design.
- The link filter: send every accepted event, or only what some edge's live
  index could match. Start with every event and measure.
