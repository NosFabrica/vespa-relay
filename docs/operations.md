# Operations

Moved from AGENTS.md on 2026-09-04, unchanged. This is the long form of the AGENTS.md section "Operations".

## A live deployment to pull from

**`https://search-staging.brainstorm.world/`** runs this code against a real
corpus with the router on. It is reachable from here, it answers anonymously,
and it is the cheapest way to get production-shaped input into a local test —
reach for it before inventing a fixture, because the things a fixture gets
wrong (how big a lens is, what a real 10040 names, what a full corpus card
looks like) are exactly what it can hand you.

- **what it holds, counted 2026-08-26**: kind 0 **54.9M**, kind 1 **149.5M**,
  NIP-32 labels (1985) **1.34M**, NIP-85 contact cards (30382) **32.6M**,
  provider lists (10040) **337**, and on the Trusted List kinds **30392: 9,
  30393: 1, 30394: 44, 30395: 0**. Count off **`/stats.json`**, whose `kinds`
  section groups over the whole store — a per-kind COUNT is fine too but a
  corpus this live drifts between asks, and an earlier read of these numbers
  reported the 30392-30395 range as empty when it was not.
  **NONE of those 54 are Tapestry lists**: those kind numbers are squatted by
  an omikuji fortune generator on 30394, WireGuard room records and
  `trusted-attestor:` entries on 30392, an Alexandria manifest on 30393 — and
  nos.lol, relay.damus.io, relay.primal.net, nostr.wine and purplepag.es hold
  the same sort of thing. **The real family is on
  `wss://tapestry.brainstorm.world/relay`** (below), so anything testing it
  needs that relay and anything READING these kinds will meet squatters first.
  The 337 provider lists are the only honest sample of which NIP-85 DIMENSIONS
  are used in the wild — `30382:rank` (328) but also `followers`, `hops`,
  `personalizedGrapeRank_influence`, `personalizedPageRank`, which is why
  anything reading a 10040 for enrolment must take every dimension rather than
  filtering to `rank` the way TrustNotice does for its own narrower question.

- **the relay** — `wss://search-staging.brainstorm.world/`. NIPs 1, 9, 11, 40,
  42, 45, 50, 62, 77, 86; `auth_required` is false, and still false now that
  reads DECLARE A LENS (`LensRequiredPolicy`): both ways past that gate are
  unsigned. Ignoring the AUTH challenge it sends still costs only the ranking
  lens — but an undeclared read is no longer answered at all, so an anonymous
  probe carries `observer:<64-hex>` or `include:spam` on every filter, plain
  NIP-01 ones included. **A `CLOSED … auth-required:` from staging is that
  gate, not a broken relay**; a bare `["REQ","s",{"kinds":[1],"limit":5}]` is
  the shape that gets it. (Staging runs deployed code, so check what it
  actually does before concluding a local change is wrong: `auth-required`
  means the gate has shipped there, an answer means it has not yet.)
- **`wss://tapestry.brainstorm.world/relay` — where the Trusted Lists actually
  are.** 500+ titled kinds 30392/30393 and a couple of 30394, every one of them
  carrying `title`, `metric`, `observer`, `min-rank` and `cutoff` exactly as
  quartz models the family, plus 14 kind-10040s. All 500 are signed by ONE
  publisher, `919ba08af7786892…`. Two things about it cost real time:
  it **answers a filter carrying `search` with silence** rather than a refusal
  — NIP-50 says ignore an unsupported extension, and ignoring the TOKEN is not
  the same as ignoring the FIELD — so a corpus fetched with the search relay's
  mandatory `include:spam` on every filter comes back complete-looking and
  without a single list in it. `fetch-corpus.mjs` therefore asks each relay
  once, with a lensless probe, which kind of reader it wants.
  **The bare-kind delegation is now live on staging.** Observer
  `f8ff11c7a7d3…` publishes a 10040 (2026-08-28) reading `["30382:rank", …],
  ["30382:followers", …], ["30392", "8e901369d450…", …]` — the ADR's generic
  entry, and the ONLY thing naming that publisher. Its 11 titled lists carry
  `title`, `metric`, `observer`, `source-tag`, `cutoff`, `min-rank`, `rigor`
  and `p` members. `ObserverTrustListIT` walks exactly this chain; probed by
  deleting the bare-kind read, the reader's list comes back with all 88 member
  profiles missing and nothing anywhere throwing.

  And **kind 10040 is REPLACEABLE**: merging two relays hands you several
  versions of one author's provider list, only the newest of which the store
  keeps. Exactly one 10040 version anywhere names the Tapestry publisher as a
  service, and its author replaced it in August with one naming somebody else —
  so as of now those lists expand for NOBODY, and a "real trust chain" built out
  of the superseded version is a test that asserts the relay is broken. Group by
  author and take the newest, the way the store does.
- **the diagnostics `:relay` serves** — `/stats.json`, `/stats.html`,
  `/observer_stats.html`, `/pressure`, `/` (NIP-11 with
  `Accept: application/nostr+json`). These are the same pages this repo builds,
  filled with numbers a local store will never reach: on 2026-08-14
  `/stats.json` reported 125 kinds, 132.4M kind 1, 28.7M kind 30382, 28.5M
  kind 0, and `/pressure` `{"meanMs":863,"samples":20}`. Chart tiers there are
  minutes old by design (`counters` every 60s, `charts` every 900s), so a
  `generatedAt` in the past is the tiering working, not a stale relay.

**To simulate an observer, rank through
`460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c`.** It is a
public key — the scores are public, so any client may rank through any lens —
and it is a *usable* one, which most pubkeys are not: it has published a kind
10040 naming `7d7ffd720b90…` at `wss://scores.brainstorm.world` for both
`30382:rank` and `30382:followers`, and that provider's cards actually reached
this store (242,051 kind-30382 events measured the same day, of 301 observers
with a 10040 at all). An observer without those two facts resolves to nothing
and silently ranks like an anonymous read, which is the failure mode this key
exists to avoid.

Node 22's global `WebSocket` is enough to ask it anything — no dependency, same
plain-node rule as `web/src/test/js`:

```js
// node probe.mjs — search, ranked through that observer
const KEY = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";
const ws = new WebSocket("wss://search-staging.brainstorm.world/");
ws.onopen = () => ws.send(JSON.stringify(
  ["REQ", "s", { kinds: [1], search: `bitcoin observer:${KEY} sort:rank`, limit: 5 }]));
ws.onmessage = (e) => console.log(JSON.parse(e.data));
```

Swap the `observer:` token in that same query for `include:spam` and the
answers change — that difference *is* the lens, and it is the one check worth
running when a change claims to touch ranking. (Dropping the token entirely is
no longer the control: that read declares nothing and comes back `CLOSED …
auth-required:`, which is the gate answering rather than the ranking.)

**MEASURED THERE, 2026-08-22**, three REQs down one anonymous socket, and both
halves of the read-lens change rest on it:

```
{kinds:[1],limit:5}                          -> 2f99b9a8 c1657e03 bd730c7f
{kinds:[1],limit:5,search:"include:spam"}    -> 2f99b9a8 c1657e03 bd730c7f   (identical)
{kinds:[1],limit:5,search:"observer:460c25…"} -> 3617a0d0 0f618b5a f078a032   (different)
```

So the waiver is FREE — stamping every anonymous read with `include:spam`
changes what the wire says and nothing about the answer, which is what lets
`shared/lens.js` stamp a whole socket rather than reason per ask — and an
`observer:` really does resolve on a connection that signed nothing, which is
what makes "ranking as" a signed-out reader's ranked answer rather than a
control that needs a key.

To fill a local store rather than read one, point a router stream at it: it
speaks NIP-77, so a narrow filter reconciles rather than downloads.

```hocon
streams { staging {
    dir = "down"
    # A LENS IN BOTH SENSES. `kinds` keeps it off the corpus; the `search`
    # token is what staging's own read gate wants — without it every REQ comes
    # back `CLOSED … auth-required:` and the stream mirrors nothing while
    # looking perfectly healthy (measured: 125k events in the window, 0
    # recovered, `0 ev/s`, and the only sign of it a NOTICE in the wire log).
    filter = { "kinds": [1985], "search": "include:spam" }
    # Neither has a default, and a visit stream needs both.
    negentropySyncThePastSeconds = 604800
    refetchThePastSeconds = 2592000
    urls = [ "wss://search-staging.brainstorm.world" ]
} }
```

**There is no `sync = "negentropy"` any more** — this block carried one for a
while and it is REFUSED at parse time now, with the reason (`gone with the
legacy backfill`). Every stream is visited the same way: page from the band's
edge, live-tail, and re-check the past on the two clocks above.

**WHAT A SEARCH COSTS THERE, 2026-09-03**, and why: the cost is the match set
a relevance profile scores, not the page — `bitcoin` kind 1 took 4.4s at limit
1 and 4.0s at limit 200; `nostr` and `the` 16s; a rare word 0.3s; the same
`bitcoin` under `sort:recent` (the match-phase profile) 0.6s. Three concurrent
searches cost 5.0s each against 3.8s alone. docs/search-latency.md has the
tables, the page's REQ sequence that turned one 3.7s search into a 9.1s first
paint, and the three fixes (the page, `SearchGate`, and the store's
newest-N cut).

Two cautions. It is **shared and live**: read it, don't publish test events to
it — anything written is written to a relay other people are reading, and
NIP-09 does not un-ring that bell. And it is a **moving system**, so every
number above is a reading taken on a date, not a constant to assert against;
pin behaviour in tests, take scale from a fresh call.

## Operations

`docker-compose.yml` runs Vespa, the relay, (behind `--profile sync`) the sync
process and a client-only Tor, and (behind `--profile onion`) a second Tor that
is the relay's own hidden service — one store throughout. Vespa holds
~50M events in this deployment. The JVM memory budget is per process: the sync
container carries the large limit because the negentropy id snapshots live
there; check the machine's total against those three limits when the profile is
on (Tor's rounds to nothing).

**Tor is a container, not a layer.** One image carries both JVM processes on
purpose, so installing a daemon in it would put Tor in the serving relay too,
give the container a second thing to supervise, and make `restart sync` — a
`sync.conf` edit, meant to be cheap — drop every circuit and re-bootstrap.
Its SOCKS port is published nowhere: an open SOCKS proxy on a public interface
is an open proxy.

**Two Tors, because they are two jobs.** `tor` (profile `sync`) is the client
proxy the router dials `.onion` *upstreams* through. `tor-onion` (profile
`onion`) is the relay's own front door: one hidden service forwarding to the
relay's port, so any Tor client can reach it without the clearnet name, a
certificate or a public IP. Same image, different torrc, and wanted
independently — a serving-only box wants a front door and no mirror. Three
things about it are not obvious and each cost a config that would not start:

- **tor refuses a hostname in `HiddenServicePort`** ("Unparseable address in
  hidden service port configuration"), so the entrypoint resolves the compose
  service name itself and appends the line to a generated torrc. Docker hands a
  re-created container a new IP and nothing in Tor re-resolves it, so the same
  script watches the name and restarts the container when it moves — the key is
  on a volume, so the address survives and only the descriptor blinks.
- **the key IS the address.** `tor_onion` holds it, mounted into that container
  and nowhere else; the relay gets a second volume carrying the hostname alone,
  read-only. `docker compose down -v` is what changes the address.
- **the torrc is generated, so tuning is a mounted file** —
  `tor/onion.extra.conf.example`, appended last. Single-hop mode (half the
  latency, and a one-way door on that key), PoW defenses, introduction points:
  each is a decision, so each is off and documented rather than chosen here.
- **the relay has to know its own `.onion`**, because NIP-42 auth events are
  signed against the address the client dialled. Without it reads and publishes
  still work and every AUTH fails, which costs Tor clients their ranking lens
  and looks like nothing at all — the reason the address is handed over in a
  file rather than an env var someone pastes in later.
- **the clearnet endpoint advertises it** with `Onion-Location` on EVERY
  response, the websocket 101 included — a Nostr client may make no other
  request, so a header on `GET /` alone would never reach the clients this is
  for. Tor Browser shows the ".onion available" button; Amethyst caches the
  mapping and rewrites the host when Tor is on. It rewrites the TRANSPORT, not
  the relay's identity, so such a client signs its AUTH with the clearnet url
  on a connection that arrived through the onion: accepting any address we
  answer at, rather than binding the policy to the address dialled, is what
  makes that work.

**Clients-first crosses the process boundary by HTTP.** The relay measures
client read latency into `ServingPressure` and serves the mean on
`GET /pressure`; the sync process polls it (`SYNC_PRESSURE_URL`) and yields
ingest between batches past the threshold. Three failed polls reset the
throttle — a relay that is down has no clients to protect — and both the feed
being off and the feed being lost are said out loud in the sync log.

**The trust reconcile runs in the background.** It used to be
`runBlocking { reconcileTrustWithRetry(store) }` between opening the store and
starting the listener — no websocket, no router and no NIP-11 until it finished,
measured at 12+ minutes on this corpus with Vespa at 356% CPU. It now runs
behind the server: the relay serves immediately and ranked search returns less
until the reconcile lands. `TRUST_RECONCILE_ON_START=false` skips it entirely.

If a start still looks slow, check that before blaming Vespa's transaction-log
replay — that wrong attribution is already in this file's history.

Both processes **deploy the bundled Vespa schema on every boot** (`AUTO_DEPLOY`,
default true), so the cluster always matches the code talking to it. A no-change
deploy is a cheap no-op. This is not decoration: when the schema drifted, Vespa
answered every write with `Status 400 ... Field 'name_parts' is not defined` and
the router counted, dropped and carried on — 2,336,288 good events lost in one
run, unrecoverable, while every status line read healthy.

Structural rejections are surfaced separately from ordinary ones on the health
line (`N event(s) LOST to store errors`), because a bad signature is the event's
fault and dropping it is correct, whereas a batch failing structurally is ours.

**Two levers delete data.** Both default to a dry run that reports and removes
nothing; run that first, and read the number before believing it.

- `SWEEP_ORPHAN_SCORES_ON_START` — kind-30382 cards from providers no stored
  10040 names. On this deployment that was 33.8M of 51.9M, and the dry run
  predicted the figure exactly.
- `deleteMissing` on a stream — records the upstream no longer serves. The
  licence is a COMPLETED reconcile, never a volume: quartz throws rather than
  falling back, so a normal return means every window was compared and an empty
  answer is the relay's answer rather than its silence. There is deliberately no
  size guard, because a mass retraction is exactly the case that matters. It is
  scoped by `ownedKinds` (required), and a stream should ASK for no more than it
  owns — `assertions` is `filter.kinds == ownedKinds == [30382]` now. Measured,
  no NIP-85 provider relay serves its own key's kind 0 or 10002, so a filter
  carrying them earned no band span and re-walked the whole past every visit
  (the band trap below), and the reconcile's own band — stamped against the
  owned kinds — could never narrow a wider one. There WAS a cascade taking a
  wholly-retracted service's kind 0 and 10002 down with its scores; it deleted
  records that arrive from the profile streams, which re-mirror them over a live
  tail, so it never survived its own next walk. Gone.

The counterpart to both: a deletion is not a tombstone. A stream that still asks
by kind re-downloads whatever was freed on its next walk, so reclaiming space and
narrowing the ask are one job.

**Real retractions are the third path, and the only one that leaves a
tombstone.** `contentViaOutbox` mirrors kinds 5 and 62 with the content, and the
store enforces them at insert — so the erase survives the next walk, which is
exactly what the two levers above cannot promise. The catch is that the store's
fast path caches "authors known to have a stored kind 5/62" **per instance**,
which is exact for one writer and wrong for this deployment's two: the relay's
copy never hears about what the router mirrored, and the router's never hears
about what a client published here. Both entrypoints therefore open the store
with `writers = STORE_WRITERS` (`SHARED_STRICT`), so NIP-09/NIP-62 are checked
against the store rather than a half-informed cache. **The topology is an
argument, not an env var** — it is a fact about this deployment that the library
cannot detect, and `:common` holds the one copy of the reasoning. `SHARED` only
bounds the window (its rebuild is a corpus-wide visit, hours here), and
`SINGLE_WRITER` is simply false for us.

The open cost is the guard read on the pure-record bulk path: it asks for every
tombstone by the batch's authors with no narrowing and no limit, so it scales
with one author's whole deletion history rather than with the batch. The mixed
path already narrows the same probe to the batch's own ids — until the record
path does too, a prolific deleter is the thing to watch on the `guards` stage.
