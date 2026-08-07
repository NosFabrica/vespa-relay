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
  below-floor authors dropped as spam. Anonymous reads are the whole corpus,
  unranked: there is no house lens standing in for you. Any client can pick one
  explicitly with `observer:<pubkey>`, since the scores are public.
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
- **Batteries included.** A web search UI and operator diagnostic pages ship on
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
| `RELAY_NAME` / `RELAY_DESCRIPTION` / … | NIP-11 identity | — |
| `ALLOW_PUBKEYS` / `DENY_PUBKEYS` / `ALLOW_KINDS` / `DENY_KINDS` | write authorization | everyone / all |
| `RELAY_ADMIN_PUBKEYS` | enables the NIP-86 management API | unset ⇒ off |
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

To try it under compose — the `sync` profile is the on-switch:

```bash
cp router.conf.example router.conf   # then edit the relay list / filters
SYNC_CONFIG_LOCAL=./router.conf docker compose --profile sync up -d --build

# after editing router.conf: bounce only the mirror, the relay keeps serving
docker compose --profile sync restart sync
```

[`docs/router.md`](docs/router.md) is the full guide: choosing `negentropy` vs
`fetch` per stream, how sync bands make paged relays resumable, dynamic
`relaySource` scans and bindings (asking each relay only for what its list
names), and `deleteMissing` — mirroring a provider's retractions, with the
guards that make absence trustworthy enough to act on.

## Operator pages

Served on the relay's own port, next to the search UI:

- **`/observer_stats.html`** — every kind-10040 observer, with its providers'
  kind-30382 score counts here and on the relay its 10040 names, side by side.
  This answers "is the trust sync actually working" — a local count alone reads
  as healthy until you learn the source holds 45× more.
- **`/kind_stats.html`** — events per kind, counted over the relay's own
  websocket with anonymous NIP-45 COUNTs — so the page also *tests* NIP-45 the
  way a client would.
- **`/relay_stats.html`** — what the store holds and how it is filling: totals,
  a per-kind table with distinct authors, events and publishing pubkeys per UTC
  day/week/month, the hour-of-day shape, a daily series per kind, the relays our
  NIP-65 lists name, and zap receipts. Charted from **`GET /stats.json`**, a
  public document a background rollup recomputes with Vespa grouping queries.

  The JSON is the artifact and the page is one reader of it — publish it and
  anyone can chart this relay's coverage, or diff it against a network-wide
  dashboard, without scraping markup. Which is the thing to keep in mind reading
  it: every number describes **this relay's store**, not the Nostr network, so a
  total below a network-wide one is a mirror's coverage rather than a fault.

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
