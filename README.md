# vespa-relay

A [Vespa](https://vespa.ai)-backed standalone [Nostr](https://nostr.com) relay
that filters and ranks everything — REQs, COUNTs, and full-text
[NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md) search —
through each connecting user's **NIP-85 web of trust**.

Spam is personal here, not a global blocklist: what your network trusts ranks
high, what it doesn't falls below your floor. And it scales like a search
engine, because it is one — the store is
[vespa-eventstore](https://github.com/NosFabrica/vespa-eventstore).

**What you get:**

- **Trust-ranked search.** Log in with NIP-42 and results are ranked by your own
  NIP-85 web of trust — relevance × how much *you* trust the author, with
  below-floor authors dropped as spam. There is no house lens standing in for
  you, so [every read says whose eyes it is read through](#every-read-says-whose-eyes-it-is-read-through).
- **The subject travels with the pointer.** A search that matches a Trusted
  List, a NIP-85 assertion or a NIP-32 label also answers with the record that
  pointer is *about* — so "podcaster" finds the *Podcaster Trust List* **and**
  the podcasters in it, which hold none of the words searched for.
  [How it is bounded](#a-search-answers-with-what-its-hits-are-about).
- **A relay that fills itself.** The **router** — a sibling process sharing the
  same store — mirrors events from upstream relays: strfry-style `streams` of
  live subscriptions, NIP-77 negentropy backfill where upstreams speak it,
  resumable paged fetch where they don't — and *dynamic* streams that discover
  relays from the store itself (NIP-65 outbox lists, NIP-66 monitors, relay
  hints), so the fan-out widens as the store fills. Its own process on purpose:
  restart or retune the mirror and the relay never drops a client.
- **A full relay, not just search.** NIP-01 filters and publishes, NIP-09
  deletions, NIP-40 expirations, NIP-45 counts, NIP-62 right to vanish, NIP-77
  negentropy for peers, NIP-86 runtime management.
- **Batteries included.** A web search UI with [its own query
  language](#the-search-boxs-own-language) and operator diagnostic pages ship on
  the same port; the Vespa schema deploys itself on boot, so `docker compose up`
  is a working relay; the whole thing also embeds as a library in your own
  Ktor app.

## Run it

One command stands up a single-node Vespa plus the relay:

```bash
docker compose up --build
# relay + web UI on ws://localhost:7777
```

On every start both processes deploy the bundled Vespa schema — so a fresh Vespa
becomes queryable and an upgraded relay carries its schema changes with it — then
serves. Open `http://localhost:7777` for the search UI, or connect a Nostr
client to the websocket.

To run against an existing Vespa, without Docker:

```bash
RELAY_URL=wss://relay.example.com VESPA_URL=http://localhost:8080 ./gradlew :relay:run
```

Vespa is a prerequisite, like a database. `docker compose up` stands one up for you.
Otherwise point `VESPA_URL` at your own.

### …on Tor as well

One profile puts the same relay behind a `.onion` address, so clients that speak
Tor can reach it without the clearnet name, a certificate or a public IP:

```bash
docker compose --profile onion up -d
docker compose logs tor-onion | grep 'reachable at'
# onion: this relay is reachable at ws://<56 chars>.onion
```

Clients dial `ws://<address>.onion`. The address is a key the Tor container
generates once and keeps in a volume, so it survives restarts. The clearnet
endpoint advertises it on every response — `Onion-Location`, the header Tor
Browser turns into the ".onion available" button and Amethyst uses to move a
connection inside the network when Tor is on — so clients that already reach
this relay find the hidden service by themselves. Both addresses authenticate:
see [Serving over Tor](docs/configuration.md#serving-over-tor-a-onion-endpoint).

## Configuration

All configuration is through environment variables — copy
[`.env.example`](.env.example) to `.env` and edit; docker compose picks it up
automatically. The essentials:

| var | meaning | default |
|---|---|---|
| `RELAY_URL` | this relay's own ws url — its NIP-42 identity and NIP-62 vanish scope | **required** |
| `VESPA_URL` | the Vespa query endpoint | `http://localhost:8080` |
| `RELAY_PORT` | port to listen on | `7777` |
| `RELAY_NAME` / `RELAY_DESCRIPTION` / … | NIP-11 identity — and, with `RELAY_NSEC` set, the relay's own kind 0 | — |
| `RELAY_ICON` | one icon everywhere: NIP-11, the relay's kind 0 picture, and the browser tab. Unset ⇒ the bundled mark, published as this relay's own `/favicon.ico` | — |
| `ALLOW_PUBKEYS` / `DENY_PUBKEYS` / `ALLOW_KINDS` / `DENY_KINDS` | write authorization | everyone / all |
| `RELAY_ADMIN_PUBKEYS` | enables the NIP-86 management API — and gates `/pulse.json` | unset ⇒ off |
| `SYNC_CONFIG` / `SYNC_CONFIG_FILE` | the router's stream config, read by the **sync process** — see below | — |
| `RELAY_ONION_URL` / `RELAY_ONION_HOSTNAME_FILE` | the `.onion` this relay also answers at, for NIP-42 — set for you by `--profile onion` | — |

Every variable — limits, tuning, memory sizing, startup migrations, the parse
audit — is documented in [`docs/configuration.md`](docs/configuration.md).

## Search

The `search` field accepts extra tokens beyond the query text. They are stripped
from the query before matching, so they never become search terms:

| token | effect |
|---|---|
| `observer:<pubkey>` | rank as seen by that pubkey's web of trust (scores are public, so any client may rank through any lens) |
| `sort:rank` | order by trust, most trusted first (also `rank:asc`, `followers`, `text`) |
| `sort:recent` | chronological: the same match set a search always recalls, newest first and still trust-gated, with match quality not consulted |
| `filter:rank:gte:N` | drop results below trust rank `N` (0–100) |
| `include:spam` | lift the default trust floor and include everything |
| `-word` / `"exact phrase"` | Google-style exclusions and phrase matching |

A NIP-42 login is an implicit `observer:` on every query: searches rank through
your web of trust, and even plain NIP-01 filters become trusted-only feeds
(newest first, below-floor authors dropped — `include:spam` opts a query out).
By default a search is trust-gated: results below the floor are hidden unless
you lift it. The full grammar — how the tokens stack, where trust scores come
from (NIP-85), what falls back when no observer resolves — is documented in
[vespa-eventstore](https://github.com/NosFabrica/vespa-eventstore), which
implements it.

### Every read says whose eyes it is read through

**Before AUTH, a REQ or COUNT is answered only if it declares a lens.** Each of
its filters must name an `observer:<64-hex>` or waive one with `include:spam`;
anything else is refused with

```
["CLOSED","<subid>","auth-required: this relay answers through a web of trust …"]
```

There are three ways to be answered, and only one of them involves a key:

| | |
|---|---|
| sign a NIP-42 AUTH | the connection's own pubkey is the lens (NIP-42 clients already retry through `auth-required:`) |
| `observer:<64-hex>` | rank through that pubkey's trust — **no signature needed**, scores are public |
| `include:spam` | the whole corpus, unranked, which is what a lensless read always was |

This relay has no house observer, so a read with no lens is not the same answers
unranked — it is a different corpus, with the trust this relay exists to apply
switched off. That answer is a legitimate thing to want; what it must not be is
what a client gets by saying nothing. `include:spam` on a plain NIP-01 filter
costs nothing else: the store maps a termless waiver to ordinary recall.

**NIP-77 is gated too**, and deliberately: a negentropy reconcile hands over the
ids and timestamps of everything matching a filter, which is the lensless read
this rule exists to stop. An undeclared `NEG-OPEN` comes back `NEG-ERR …
auth-required:`; the same filter carrying `include:spam` is admitted — so an
anonymous **peer** cannot mirror from here without declaring.

Unaffected: publishing (`EVENT`), `AUTH` itself, and NIP-11. See
[`docs/configuration.md`](docs/configuration.md) for `REQUIRE_READ_LENS=false`
and the older behaviour.

### A search answers with what its hits are about

Three kinds of event are found by text that describes *something else*: a
Tapestry Trusted List (30392-30395) by its `title`, a NIP-85 Trusted Assertion
(30382-30385) by its `petname` or `summary`, a NIP-32 label (1985) by its label
value. This relay follows the pointer and serves the record beside the hit, at
the hit's own rank discounted by the confidence the hit expressed about it — a
Trusted List scores each member 0..100, so a member its publisher doubts sinks
past the organic hits it would otherwise sit above. A label and an assertion
express no confidence, so their subjects land directly behind them:

```
["REQ","s",{"kinds":[0,30392],"search":"podcaster"}]
  <- ["EVENT","s", … kind 30392 "Podcaster Trust List" …]
  <- ["EVENT","s", … kind 0, a member's profile …]     <- would never match "podcaster"
```

A `p` member (and a label's `p` target, and a 30382's subject) resolves to that
author's kind-0 profile; an `e` or an `a` resolves to that event. Two rules bound
it:

- **A subject must match the subscription's own filter** with the `search` field
  left out of the test — so `{"kinds":[1985],…}` gets labels and nothing else,
  and a client that wants the subjects names their kinds as well.
- **A list or an assertion unpacks only for the reader who enrolled its signer.**
  Those two families are a trust service's computed output, and NIP-85 says how a
  reader picks services: a kind-10040 naming them. So the hit has to be signed by
  one of *this read's observer's* services, or by the observer themselves — an
  anonymous `include:spam` read has no observer and gets no list expansion at
  all. Labels are not gated this way; anyone may label anything, and the label
  still had to survive the trust-ranked search to be a hit.

Operators can tune or disable the whole thing:
[`SEARCH_EXPAND_REFERENCES`](docs/configuration.md#search-the-subject-travels-with-the-pointer).

### The search box's own language

The bundled UI has a small query language of its own, parsed in
`web/shared/query.js`. Every token becomes a **NIP-01 filter field, never a
NIP-50 extension**, so all of it composes with the ranking above and with the
words you typed:

| token | asks for |
|---|---|
| `from:<npub>` / a bare npub | events by that author (`authors`) |
| `to:<npub \| note \| nevent \| naddr>` | events naming that subject — `#p`, `#e` or `#a`, chosen by what the pointer is |
| `since:YYYY-MM-DD` / `until:YYYY-MM-DD` | the time window (ISO only: `06/08/2026` is a different day to half the world) |
| `#hashtag` | `#t`, and `#l` beside it, so a label's own mark is found too |
| `label:<mark>` | the **NIP-32 labels themselves**, at kind 1985 carrying that mark |
| `group:<id>` | a NIP-29 room, plus its kind-39000 metadata |
| `site:` `isbn:` `geo:` `isan:` `doi:` `podcast:*` | the NIP-73 external scopes |

`label:` and `to:` together are what makes a label pill on a card answer its own
question: the `review/app` pill on an app's kind 31990 runs `label:review/app
to:naddr1…` and returns exactly the reviews it counted — text that lives in the
label events' `content` and that no word search would ever recall.

## The router: mirror from upstream relays

The relay serves what is in the store; the router is how the store gets filled
from the network. It runs as its own process (`vespa-sync`) against the same
Vespa, so mirroring can be restarted, reconfigured, or OOM without the relay
noticing. Point `SYNC_CONFIG_FILE` at a strfry-style `streams` config and it
keeps a live subscription open against each upstream, mirroring matching
events into the relay's store — immediately searchable:

```hocon
streams {
  popular {
    dir    = "down"       # down = mirror in; up = publish out; both
    filter = { "kinds": [0, 3, 5, 1984, 10000, 30000] }
    urls   = [ "wss://relay.primal.net", "wss://relay.damus.io", "wss://purplepag.es" ]
  }
}
```

Each stream backfills the history its filter asks for — negentropy set
reconciliation where the upstream speaks NIP-77 (only the difference travels),
resumable paged REQ where it doesn't — then holds a live tail. `up` streams
publish your matching events upstream, with echo-suppression for free. Signature
verification is on by default for everything mirrored.

A stream can also leave `urls` out and take its relay list **from the store**:
`relaySource` scans, say, NIP-65 outbox lists, NIP-66 monitor reports, and
relay hints on notes, unions every relay they name, and syncs against all of
them on a cycle. That is the outbox model as config — *fetch each author's
events from the relays their own 10002 marks write* is one line — and it needs
no per-kind code: any tag that carries a relay url at some offset can be a
source.

Mirroring and measuring are two configs, because they are two planes:
`sync.conf` says what to mirror, `monitor.conf` says which relay urls to probe
and grade, over its own `sources` and its own single `exclude`. To try it under
compose — the `sync` profile is the on-switch:

```bash
cp sync.conf.example sync.conf         # what to mirror: the relay list / filters
cp monitor.conf.example monitor.conf   # what to measure: the relay lists to scan
SYNC_CONFIG_LOCAL=./sync.conf MONITOR_CONFIG_LOCAL=./monitor.conf \
  docker compose --profile sync up -d --build

# after editing either: bounce only the mirror, the relay keeps serving
docker compose --profile sync restart sync
```

[`docs/router.md`](docs/router.md) is the full guide: choosing `negentropy` vs
`fetch` per stream, how sync bands make paged relays resumable, dynamic
`relaySource` scans and bindings (asking each relay only for what its list
names), and `deleteMissing` — mirroring a provider's retractions, with the
guards that make absence trustworthy enough to act on.

## Operator pages

Five pages, on three ports. Each is served by the process that does the work it
describes, so a page that will not load is itself an answer.

| page | port | asks |
|---|---|---|
| [`/stats.html`](#statshtml) | relay, `7777` | what does this store hold, and how is it filling? |
| [`/trust.html`](#trusthtml) | relay, `7777` | is ranked search working — and if not, which part is incomplete? |
| `/observer_stats.html` | relay, `7777` | is the trust *sync* working? Each kind-10040 observer's providers' kind-30382 score counts, here and on the relay its 10040 names, **side by side** — a local count alone reads as healthy until you learn the source holds 45× more |
| [`/pulse.html`](#where-the-resources-go--pulsehtml) | own port, **off by default, admins only** | what does any of it *cost*? |
| [the sync and monitor pages](#the-mirrors-own-pages) | `7778` / `7779` | what is the mirror doing, and which relays may be dialled at all? |

### `/stats.html`

Totals, a per-kind table with distinct authors, events and publishing pubkeys
per UTC day/week/month, the hour-of-day shape, a daily series per kind, the
relays our NIP-65 lists name, zap receipts, how fresh the store is, whether the
**web of trust is actually populated** — a `scoredPubkeys` of zero means ranked
search is silently falling back for every reader — and, last on the page, **every
kind in the store** with its events, distinct authors and last-seen age.

Charted from **`GET /stats.json`**, a public document a background rollup
recomputes with Vespa grouping queries. The JSON is the artifact and the page is
one reader of it — publish it and anyone can chart this relay's coverage, or diff
it against a network-wide dashboard, without scraping markup.

Which is the thing to keep in mind reading it: every number describes **this
relay's store**, not the Nostr network, so a total below a network-wide one is a
mirror's coverage rather than a fault. And a mirror is a **filtered** subset,
which is why the document also carries `sync.mirrors.kinds` — the kinds the
router asks for. Any count taken against this relay has to carry them: measured
against an author's own relay, unfiltered, this store once read as *35%
mirrored* while missing nothing — the entire gap being kinds no stream here
asks for.

### `/trust.html`

Whether ranked search is working on this deployment, and if not which part of
the trust projection is incomplete. **Public**, unlike the pulse: every field
here is a count, a phase, or a query *shape* — never a search term. Four panels,
in the order the question is actually asked:

- **Can people search** — provider lists resolving to a service that carries
  cells. An observer whose lens resolves to an unprojected service gets an
  **empty** ranked page, and the page says so: the gate failing closed is
  correct, the projection is what is incomplete.
- **What is being repaired** — live phases, with a fraction and an ETA only
  where a denominator exists.
- **Why reads come back short** — degraded reads by profile, shape and flags,
  with *served* marked more loudly than refused. A match-phase cut on a recency
  profile is allowed and returned silently, so nothing throws and ranked pages
  quietly get shorter; that is the failure worth seeing first.
- **Explain one pubkey** — over `GET /trust/explain/{pubkey}`. A pubkey is
  public and the relay already serves what it holds about one over NIP-01, so
  this reveals nothing the protocol does not: the answer describes the
  projection, not the person.

**Never measured is not zero**, and the page says which — an unmeasured coverage
drawn as 0% reads as an outage when it means no reconcile has finished. Charted
from **`GET /trust.json`**.

### Where the resources go — `/pulse.html`

On its own port (`PULSE_PORT`, 7780, on the relay; `SYNC_PULSE_PORT`, 7781, on
the mirror), **off by default** and **administrators only**.

The pages above say what this deployment *holds* and what the mirror is *doing*.
This one says what any of it **costs** — read live from the store's own counters,
so there is no rollup and nothing to go stale:

- **What the store is doing** — wall time inside the engine calls this process
  made, grouped by the work that made them, with **calls per document** beside
  each. That ratio is the store's own performance contract in a number: a bulk
  path booking several engine calls per document it writes is the shape "never
  ingest in a loop over `insert()`" exists to prevent, and the page calls it out.
- **What the engine did** — Vespa's own timings per rank profile, and **matched
  against served**: a profile matching 561K documents to serve 53K is doing work
  no client sees.
- **Locks** — what holds a store mutex *at this instant*, and cumulative wait
  split by **what each waiter was queued behind**. That split is the point:
  `lock.ingest.wait 41s` only raises a question, `38s of it behind "derive 500
  subject(s) in 10 chunk(s)"` names a fix.
- **What became of the events offered** — admitted against duplicate, replaced,
  deleted, expired. "81% of what this node is offered is already stored" is what
  tells you to narrow a sync, and no port-level counter can see it: a refused
  event never reaches the index.
- **Right now** — the gauges (feed operations in flight, trust backlog, mutexes
  held), drawn apart from every counter, because a queue depth must never be
  differenced into a rate.

Every total is cumulative since the process started and the page differences two
consecutive polls to recover a rate, so any number of tabs may watch it and
nothing is consumed by being read.

#### Who may read it

The other pages are public because every field in them is a fact about stored
events. This one is not that document: with `PULSE_CLIENT_DETAIL` on it names
the heaviest observer lenses and search terms driving the load, and carries a
slow-read log that **quotes the query**. So `/pulse.json` is served only to an
administrator.

- **The proof is NIP-98** against the same `RELAY_ADMIN_PUBKEYS` the NIP-86
  admin RPC uses — so "who can read this?" has the same answer as "who can ban a
  pubkey?".
- **A port set with no admin keys stops the boot.** "No administrators" and
  "everyone is an administrator" are one mistake apart.
- **In a browser**, press sign in: a NIP-07 extension signs once and the relay
  returns a 30-minute `HttpOnly`, `SameSite=Strict` session cookie. **From a
  script**, sign a kind-27235 event over the request's url and method and send
  `Authorization: Nostr <base64>` — then poll through the session, since NIP-98
  tokens are single-use.
- **Still don't publish this port.** Compose publishes both pulse ports on
  `127.0.0.1` only — like Vespa's, and unlike the status pages — so reach them
  over an SSH tunnel. Behind a reverse proxy set `PULSE_PUBLIC_URL` to the origin
  the browser reaches: the `u` a token is signed over is an operator setting,
  never the `Host` header the caller sent.

`PULSE_CLIENT_DETAIL` stays a separate switch from all of this: sign-in governs
who can *read* those sections, that switch governs whether the store *retains*
them at all, which is the stronger guarantee.

What is measured, what it costs, and what is deliberately left to Vespa's own
metrics proxy is
[`docs/telemetry.md`](https://github.com/NosFabrica/vespa-eventstore/blob/main/docs/telemetry.md)
in the event store.

### The mirror's own pages

Up with the `sync` profile, each served by the process doing the work:

- **`/` on the sync service** (`SYNC_STATUS_PORT`, 7778) — what the mirror is
  doing right now, charted from that service's own **`GET /stats.json`**.

  It opens with **`prime relays`** — one row per relay a stream is allowed to
  dial, on two independent axes. **How current** we are: the age of the newest
  event we hold from it, and whether a live tail is carrying its present. **How
  far back** the walk has got: `complete`, `paging` (with how deep and how much
  of what it owes is settled), `refused` with the reason, or `hasn't started`.
  The two are not the same question — a relay can be `complete` and nine days
  cold — and the headline answers the first one, because that is the one an
  operator arrives with.

  Beside them, **on what terms** that relay lets us sync: whether the monitor
  measured it as speaking negentropy (without it, its history can never be
  reconciled), the filter width its own refusal taught us, and the last thing it
  said when it turned us away. Rows that need somebody come first, and the counts
  above the table stay complete even when the list is cut.

- **`/` on the monitor** (`MONITOR_STATUS_PORT`, 7779) — what this router has
  decided about the relay urls it discovers: which are one server wearing several
  addresses, which cannot answer the same question twice, which are graded
  `prime`, and which are unreachable. Below the passes, a panel reads the signed
  **kind-30166** records themselves out of the relay over its own websocket,
  which makes it a protocol check as much as a view: a verdict that cannot be
  read there cannot be read by any client either.

  Its own page because it asks a different question in a different unit: sync
  coverage is measured in events and asks whether the mirror is keeping up; this
  is measured in relay urls and asks which of them may be dialled at all.

### Three services, one hostname

Each of the three binds its own port, but nothing requires three hostnames and
three certificates to read them. Every reference the pages make is
**document-relative** — the assets under `web/…`, and the `stats.json` each page
charts — so a service can be mounted behind a path prefix with a plain strip
rewrite and nothing else:

```nginx
location /sync/    { proxy_pass http://sync:7778/;    }
location /monitor/ { proxy_pass http://monitor:7779/; }
```

The **trailing slash matters on both sides**. `https://host/sync/` has `/sync/`
as its base directory and every asset is asked for under it; `https://host/sync`
has the ROOT as its base, and the page then asks the *relay* for its modules —
which the relay answers, 200, with its own copy of the same file names. Redirect
the bare prefix to the slashed one, the way ingresses normally do.

The search UI is the exception and is root-only: it is a single-page app whose
history writes are anchored at `/` by construction, so a prefix would survive
the first load and be lost by the first navigation.

## Supported NIPs

| NIP | | In this relay |
|---|---|---|
| [01](https://github.com/nostr-protocol/nips/blob/master/01.md) | Core protocol | Filters, publishes, subscriptions |
| [09](https://github.com/nostr-protocol/nips/blob/master/09.md) | Event deletion | |
| [11](https://github.com/nostr-protocol/nips/blob/master/11.md) | Relay info document | Identity and limits, served on the same port |
| [40](https://github.com/nostr-protocol/nips/blob/master/40.md) | Expiration timestamps | Expired events are swept on a timer |
| [42](https://github.com/nostr-protocol/nips/blob/master/42.md) | Authentication | Login switches search to your own web of trust |
| [45](https://github.com/nostr-protocol/nips/blob/master/45.md) | Event counts | `COUNT` |
| [50](https://github.com/nostr-protocol/nips/blob/master/50.md) | Search | Full-text, trust-ranked — the core feature |
| [62](https://github.com/nostr-protocol/nips/blob/master/62.md) | Right to vanish | |
| [77](https://github.com/nostr-protocol/nips/blob/master/77.md) | Negentropy sync | |
| [86](https://github.com/nostr-protocol/nips/blob/master/86.md) | Relay management | Ban/allow pubkeys, events, kinds; edit identity at runtime. Only when `RELAY_ADMIN_PUBKEYS` is set |

## Embed it

The relay also runs inside your own JVM/Ktor app. `serveRelay(relay, port, ...)` binds a
port batteries-included, or `Route.nostrRelay(relay)` and friends mount the pieces in an
existing server. See `server/NostrRelayServer.kt` and `server/HttpServer.kt`.

## Build

```bash
./gradlew build              # compile + tests + spotlessCheck, all modules
./gradlew :relay:run         # the serving relay (VESPA_URL / RELAY_URL from the environment)
./gradlew :sync:run          # the mirror (adds SYNC_CONFIG_FILE=…)
./gradlew :relay:installDist # runnable distributions under relay/build/install/vespa-relay
./gradlew :sync:installDist  #   …and sync/build/install/vespa-sync
```

Kotlin 2.4 / JDK 21. Quartz and the [vespa-eventstore](https://github.com/NosFabrica/vespa-eventstore)
store come from JitPack, pinned by commit in `gradle/libs.versions.toml`.

## License

MIT © NosFabrica
