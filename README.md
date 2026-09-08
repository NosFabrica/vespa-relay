# vespa-relay

[![build](https://github.com/NosFabrica/vespa-relay/actions/workflows/build.yml/badge.svg)](https://github.com/NosFabrica/vespa-relay/actions/workflows/build.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Kotlin 2.4](https://img.shields.io/badge/Kotlin-2.4-7F52FF.svg)](https://kotlinlang.org)
[![JDK 21](https://img.shields.io/badge/JDK-21-orange.svg)](https://openjdk.org/projects/jdk/21/)

A [Vespa](https://vespa.ai)-backed standalone [Nostr](https://nostr.com) relay
that filters and ranks everything — REQs, COUNTs, and full-text
[NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md) search —
through each connecting user's **NIP-85 web of trust**.

Spam is personal here, not a global blocklist: what your network trusts ranks
high, what it doesn't falls below your floor. And it scales like a search
engine, because it is one — the store is
[vespa-eventstore](https://github.com/NosFabrica/vespa-eventstore).

## The difference, in one exchange

This relay has no house observer, so a read has to say whose eyes it is read
through. Say nothing and you are not answered:

```jsonc
["REQ","s",{"kinds":[0,30392],"search":"podcaster"}]
  <- ["CLOSED","s","auth-required: this relay answers through a web of trust …"]
```

Name a lens — anybody's, no key and no signature, because trust scores are
public — and the same query ranks, and answers with what its hits are *about*:

```jsonc
["REQ","s",{"kinds":[0,30392],"search":"podcaster observer:<64-hex>"}]
  <- ["EVENT","s", … kind 30392 "Podcaster Trust List" …]  // ranked by that lens
  <- ["EVENT","s", … kind 0, a member's profile …]         // holds no "podcaster"
```

The profile is there because the list *points* at it, placed by the list's own
rank discounted by the 0..100 confidence the list expressed in that member — so
a member its publisher doubts sinks past the organic hits. Sign a NIP-42 AUTH
instead and the lens is your own pubkey, on every query, including plain NIP-01
filters. `include:spam` waives the lens and takes the whole corpus, unranked.

## Who this is for

- **Running a relay?** [`docker compose up`](#run-it) is a working, self-filling
  relay — schema deployed, [web UI](#the-search-boxs-own-language) and
  [operator pages](#operator-pages) included.
- **Filling one?** The [router](#the-router-mirror-from-upstream-relays) mirrors
  from upstream relays and discovers new ones from the store as it fills.
- **Building on one?** The whole thing [embeds](#embed-it) in your own Ktor app,
  and every diagnostic page is a JSON document you can read instead.

**What you get:**

- **Trust-ranked search** — relevance × how much *you* trust the author, with
  below-floor authors dropped as spam, and
  [no answer at all to a read that names no lens](#every-read-says-whose-eyes-it-is-read-through).
- **The subject travels with the pointer** — a hit on a list, an assertion or a
  label also answers with the record it points at,
  [bounded two ways](#a-search-answers-with-what-its-hits-are-about).
- **A relay that fills itself** — the **router** mirrors from upstream relays
  (strfry-style `streams`, NIP-77 backfill where they speak it, resumable paged
  fetch where they don't) and *discovers* new ones from the store itself, so the
  fan-out widens as it fills. A sibling process on purpose: retune or restart
  the mirror and the relay never drops a client.
- **A full relay, not just search** — NIP-01 filters and publishes, NIP-09
  deletions, NIP-40 expirations, NIP-45 counts, NIP-62 right to vanish, NIP-77
  negentropy for peers, NIP-86 runtime management.
- **Batteries included** — the Vespa schema deploys itself on boot, and the
  search UI, its [own query language](#the-search-boxs-own-language) and the
  operator pages all ship on the relay's own port.

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

Five diagnostic pages, on three ports, each served by the process that does the
work it describes — so a page that will not load is itself an answer.

| page | port | asks |
|---|---|---|
| `/stats.html` | relay, `7777` | what does this store hold, and how is it filling? |
| `/trust.html` | relay, `7777` | is ranked search working — and if not, which part is incomplete? |
| `/observer_stats.html` | relay, `7777` | is the trust *sync* working — does a provider hold 45× what we mirrored? |
| `/pulse.html` | own port, off by default | what does any of it *cost*? |
| `/` on sync / monitor | `7778` / `7779` | what is the mirror doing, and which relays may be dialled at all? |

Each charts a JSON document served beside it — `/stats.json`, `/trust.json`,
`/pulse.json` — and the JSON is the artifact: publish it and anyone can chart
this relay's coverage without scraping markup. Every number describes **this
relay's store**, not the Nostr network, so a total below a network-wide one is a
mirror's coverage rather than a fault.

Two things to know before opening a port. **`/pulse.html` is the exception to
all of the above**: off by default, administrators only over NIP-98, and
published on `127.0.0.1` by compose — it names the observer lenses and search
terms driving the load, so it is the one page that is not a fact about stored
events. And the three services can share **one hostname**: every reference the
pages make is document-relative, so a plain strip rewrite behind a path prefix
works — mind the trailing slash on both sides.

[**`docs/operator-pages.md`**](docs/operator-pages.md) is the full guide: what
each panel shows and why, the pulse's sign-in and what it retains, the mirror's
`prime relays` table, and the proxy layout.

## Supported NIPs

| NIP | | In this relay |
|---|---|---|
| [01](https://github.com/nostr-protocol/nips/blob/master/01.md) | Core protocol | Filters, publishes, subscriptions |
| [09](https://github.com/nostr-protocol/nips/blob/master/09.md) | Event deletion | Enforced at insert, so a re-mirrored event stays deleted |
| [11](https://github.com/nostr-protocol/nips/blob/master/11.md) | Relay info document | Identity and limits, served on the same port |
| [40](https://github.com/nostr-protocol/nips/blob/master/40.md) | Expiration timestamps | Expired events are swept on a timer |
| [42](https://github.com/nostr-protocol/nips/blob/master/42.md) | Authentication | Login switches search to your own web of trust |
| [45](https://github.com/nostr-protocol/nips/blob/master/45.md) | Event counts | `COUNT` |
| [50](https://github.com/nostr-protocol/nips/blob/master/50.md) | Search | Full-text, trust-ranked — the core feature |
| [62](https://github.com/nostr-protocol/nips/blob/master/62.md) | Right to vanish | Scoped by this relay's own `RELAY_URL` |
| [77](https://github.com/nostr-protocol/nips/blob/master/77.md) | Negentropy sync | Peers reconcile — a `NEG-OPEN` declares a lens like any other read |
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
