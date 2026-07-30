# vespa-relay

A standalone Nostr relay with trust-ranked search. It serves full-text
[NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md) search, ranked by
the searcher's web of trust, backed by [Vespa](https://vespa.ai).

Point a [Nostr](https://nostr.com) client at it. It speaks NIP-01 filters and NIP-50
search over websockets. Log in with NIP-42 to rank results by your own web of trust.
Anonymous searches use the operator's default. A web search UI ships on the same port.

It serves what is in the store. To fill the store from the network, the built-in
**router** mirrors events from upstream relays — a strfry-style `streams` config
of live subscriptions plus negentropy backfill (see [The router](#the-router-mirror-from-upstream-relays)).
Large-scale crawling and trust-sync remain a separate job.

## Run it

One command stands up a single-node Vespa plus the relay:

```bash
docker compose up --build
# relay + web UI on ws://localhost:7777
```

On first run the relay deploys the bundled Vespa schema, then serves. Open
`http://localhost:7777` for the search UI, or connect a Nostr client to the websocket.

To run against an existing Vespa, without Docker:

```bash
RELAY_URL=wss://relay.example.com VESPA_URL=http://localhost:8080 ./gradlew :relay:run
```

Vespa is a prerequisite, like a database. `docker compose up` stands one up for you.
Otherwise point `VESPA_URL` at your own.

## Configuration

All configuration is through environment variables.

### Core

| var | meaning | default |
|---|---|---|
| `RELAY_URL` | this relay's own ws url — its NIP-42 identity and NIP-62 vanish scope | **required** |
| `VESPA_URL` | the Vespa query endpoint | `http://localhost:8080` |
| `RELAY_PORT` | port to listen on | `7777` |
| `DEFAULT_OBSERVER` | 64-hex pubkey whose web of trust ranks anonymous searches | unset ⇒ untrusted |
| `AUTO_DEPLOY` | deploy the bundled schema on first run | `true` |
| `LOG_CONNECTIONS` | log the live connection count on connect/disconnect | `false` |

### Relay identity (NIP-11)

| var | meaning | default |
|---|---|---|
| `RELAY_NAME` / `RELAY_DESCRIPTION` / `RELAY_ICON` / `RELAY_BANNER` | how the relay presents itself | — |
| `RELAY_CONTACT` | a human contact | — |
| `RELAY_CONTACT_PUBKEY` / `RELAY_SELF_PUBKEY` | the relay's contact and self pubkeys | — |
| `RELAY_VERSION` | overrides the build version | — |
| `RELAY_POSTING_POLICY` / `RELAY_PRIVACY_POLICY` / `RELAY_TERMS_OF_SERVICE` | policy urls | — |

### Limits

| var | meaning | default |
|---|---|---|
| `MAX_MESSAGE_LENGTH` / `MAX_SUBSCRIPTIONS` / `MAX_FILTERS` / `MAX_LIMIT` / `DEFAULT_LIMIT` / `MAX_SUBID_LENGTH` / `MAX_EVENT_TAGS` / `MAX_CONTENT_LENGTH` / `MIN_POW_DIFFICULTY` / `CREATED_AT_LOWER_LIMIT` / `CREATED_AT_UPPER_LIMIT` | protection limits, enforced by the engine and shown in the NIP-11 `limitation` block | sane defaults |
| `NEG_FRAME_SIZE_LIMIT` / `NEG_MAX_SYNC_EVENTS` / `NEG_MAX_SESSIONS_PER_CONNECTION` | NIP-77 negentropy tuning (`NEG_MAX_SYNC_EVENTS` caps how many ids one reconciliation walks) | strfry-parity |

### Access control

| var | meaning | default |
|---|---|---|
| `ALLOW_PUBKEYS` / `DENY_PUBKEYS` | write authorization by pubkey — allowlist (empty ⇒ everyone) minus denylist, 64-hex, comma/space-separated | — |
| `ALLOW_KINDS` / `DENY_KINDS` | write authorization by kind — allow (empty ⇒ all) minus deny | — |
| `REJECT_FUTURE_SECONDS` | reject events dated more than N seconds in the future | `0` (off) |
| `EXPIRATION_SWEEP_SECONDS` | how often to prune NIP-40 expired events | `3600` (0 ⇒ off) |

### Admin (NIP-86)

| var | meaning | default |
|---|---|---|
| `RELAY_ADMIN_PUBKEYS` | comma/space-separated 64-hex admin keys; when set, enables the NIP-86 management API (`POST /`, NIP-98 auth) | unset ⇒ off |
| `RELAY_STATE_FILE` | path where NIP-86 ban/allow lists are persisted (survives restart) | unset ⇒ in-memory |
| `RELAY_HTTP_URL` | the http(s) url NIP-98 auth events must be tagged with | derived from `RELAY_URL` |

### Router (upstream mirror)

| var | meaning | default |
|---|---|---|
| `ROUTER_CONFIG` | the router `streams { }` config, inline (HOCON). When set, the relay mirrors upstream events into its store | unset ⇒ router off |
| `ROUTER_CONFIG_FILE` | path to a file holding that config, as an alternative to `ROUTER_CONFIG` | — |
| `ROUTER_BACKFILL_SECONDS` | default history window a stream negentropy-backfills before its live tail; per-stream `backfillSeconds` overrides it | `0` (live-tail only) |
| `ROUTER_UP_INTERVAL_SECONDS` | how often `up`/`both` streams re-reconcile to push newly-arrived local events upstream | `300` |
| `ROUTER_NEG_TIMEOUT_SECONDS` | hard cap on a single negentropy reconciliation, per upstream (they run in parallel). This is not the stuck-upstream guard — a session with no frames for 30s aborts itself — so it only needs to be large enough for a legitimate fill of `ROUTER_BACKFILL_SECONDS` to finish. Too tight and big upstreams get truncated to live-tail | `14400` (4h) |
| `ROUTER_INGEST_BATCH` / `ROUTER_INGEST_CONCURRENCY` | mirrored events are drained in batches and written through the store's bulk path. The store serializes writes, so throughput comes from the batch size (a sweet spot near the default — much larger stalls on long mutex holds), not the worker count. Lower the batch to cut memory | `1000` / `2` |
| `ROUTER_DYNAMIC_REFRESH_SECONDS` | default period between cycles of a `relaySource { }` stream (re-read the relay lists, re-sync every relay) | `21600` (6h) |
| `ROUTER_DYNAMIC_MAX_LISTS` | default cap on how many relay-list events one cycle scans, newest first | `50000` |
| `ROUTER_DYNAMIC_MAX_RELAYS` | default cap on how many discovered relays one cycle syncs, best-referenced first (`0` = all) | `500` |
| `ROUTER_DYNAMIC_MIN_REFERENCES` | default floor on how many lists must name a relay before it is synced | `1` |
| `ROUTER_DYNAMIC_CONCURRENCY` | default number of discovered relays synced at the same time | `8` |

## The router: mirror from upstream relays

Point `ROUTER_CONFIG` (or `ROUTER_CONFIG_FILE`) at a strfry-style `streams`
config and the relay keeps a live subscription open against each upstream,
mirroring matching events into the same store it serves:

```hocon
connectionTimeout = 20
streams {
  popular {
    dir    = "down"
    filter = { "kinds": [0, 3, 5, 1984, 10000, 30000] }
    urls   = [ "wss://relay.primal.net", "wss://relay.damus.io", "wss://purplepag.es" ]
  }
  mirrors {
    dir             = "down"
    filter          = { "kinds": [0, 3, 5, 1984, 10000, 30000] }
    backfillSeconds = 86400   # negentropy-reconcile the last day, then live-tail
    urls            = [ "wss://profiles.nostr1.com", "wss://directory.yabu.me", "wss://relay.ditto.pub" ]
  }
}
```

Each named stream mirrors a NIP-01 `filter` from a set of `urls`. Per stream:

- **`dir`** — `down` mirrors upstream events into our store; `up` publishes our
  matching events to the upstream; `both` does each on the same relay.
- **`filter`** — the NIP-01 filter to mirror (kinds, authors, `#tags`, …).
- **`backfillSeconds`** *(optional)* — history window to negentropy-backfill
  before the live tail takes over. Upstreams without NIP-77 fall back to paged
  REQ automatically. `0` (default, or `ROUTER_BACKFILL_SECONDS`) is live-only.
- **`trusted`** *(optional)* — skip signature verification for this upstream's
  events. Off by default; every mirrored event is verified and re-checked
  against the stream filter before it enters the store.

The router shares the relay's Vespa store, so mirrored events are immediately
searchable. It runs one outbound connection per upstream (reconnect and
re-subscribe are handled for you) and logs unreachable upstreams rather than
failing — a paused or down relay in the list is skipped, not fatal.

**Down** keeps a live subscription open and, with a backfill window, first
negentropy-reconciles history. **Up** re-reconciles the store against the
upstream every `ROUTER_UP_INTERVAL_SECONDS` and publishes only what the
upstream is missing — set reconciliation gives echo-suppression for free, so an
event just pulled *down* from a relay is never pushed back *up* to it.

The **live tail works against every relay**; the **negentropy backfill depends
on the upstream**. Some relays advertise NIP-77 but their reconciliation never
converges. Two independent bounds handle that: a session with no protocol frames
for 30 seconds aborts itself — that is what catches a *stuck* upstream — and
`ROUTER_NEG_TIMEOUT_SECONDS` hard-caps a session that keeps talking without
converging. Either way the upstream logs it and leans on its live tail, while
relays that reconcile cleanly backfill in full. So a brand-new store is filled
forward from connect universally, and backfilled historically for the relays
whose NIP-77 cooperates.

Because the idle timeout is the real liveness guard, size the hard cap for your
backfill window, not defensively: a multi-year fill across many relays needs
hours, and a cap below that silently truncates every upstream that was working.
Events already downloaded when a cap fires are kept — the session abandons only
the remainder — though the progress line undercounts that upstream's total.

While backfilling, the router logs progress and an ETA to "useful" (backfill
complete), so you can tell how long the initial fill will take:

```
router: backfill 4/12 upstream(s), 12,340/29,110 events (42%), 851/s, ETA ~0:03:17 to useful
router: backfill complete — 41,880 events from 12 upstream(s) in 0:04:52; live tail now streaming
```

### Dynamic relay lists: the outbox, and its NIP-85 twin

A stream can leave `urls` out entirely and take its relay list from the store
instead. Two configs ship with this shape — `router.conf.outbox.example` and
`router.conf.assertions.example` — and they differ only in which list they read:

```hocon
outbox {
  dir             = "down"
  filter          = { "kinds": [0, 3, 10002, 10040] }
  backfillSeconds = 172800
  relaySource {
    kind           = 10002    # NIP-65 relay lists  (10040 = NIP-85 trust providers)
    marker         = "write"  # write + unmarked; "read" or "any" to widen
    refreshSeconds = 21600
    maxRelays      = 500
    minReferences  = 2
    concurrency    = 8
  }
}
```

Each cycle reads the relay-list events already in our store, takes the urls out
of them, ranks each relay by how many lists name it, and negentropy-syncs the
stream `filter` against the top `maxRelays` of them, `concurrency` at a time
(paged REQ where NIP-77 is missing, same as a backfill). Then it sleeps
`refreshSeconds` and does it again — so the fan-out widens on its own as more
relay lists land in the store.

- **`kind = 10002`** — the outbox. `["r", "<url>", "write"]` and unmarked `["r",
  "<url>"]` tags (unmarked means read *and* write) are the relays a user's events
  are published to, so they are the relays to pull from.
- **`kind = 10040`** — NIP-85 trusted assertions. Each `["30382:rank", "<pubkey>",
  "<url>"]` tag names a provider *and* the relay it publishes on; those relays are
  the ones worth syncing trust data from. `marker` doesn't apply — 10040 has no
  read/write sides.

Both need relay lists to exist before they can fan out over them, so pair them
with an ordinary `down` stream on a few indexer relays — that seed stream is
what fills the store with 10002s and 10040s. Both example configs include one.

Some notes on the knobs:

- **`backfillSeconds`** is the history window each cycle reconciles. Keep it
  longer than `refreshSeconds` so consecutive cycles overlap; leave it unset and
  every cycle reconciles the filter's whole history, which is correct but much
  more expensive once the fan-out is wide.
- **`minReferences`** matters more than it looks. The tail of a 10002 scan is
  thousands of hosts exactly one person named, most of them long dead; `2` cuts
  that off. Assertion providers are scarce enough that `1` is right for 10040.
- **`maxLists`** caps the scan, not the network — raise it on a large store.
- These streams have **no live tail**. Holding hundreds of subscriptions open is
  what the periodic sync exists to avoid, so each relay's socket is dropped again
  as soon as its sync returns. `dir` must be `down`, and a `relaySource { }`
  stream can't also carry static `urls` — split those into two streams.

Every cycle logs what it did, including why the unreachable relays were
unreachable (a relay list is full of dead hosts — the tally is how you tell
"normal" from "the whole cycle is broken"):

```
router: outbox syncing 500 relay(s) from kind 10002 lists (top: wss://relay.damus.io/ x8214, ...)
router: outbox cycle done — 214,880 event(s) from 431/500 relay(s) in 12:41; unreachable: timeout x38, Connection refused x21; next in 21600s
```

### Enabling it under docker compose

`docker-compose.yml` wires the router env through and mounts `./router.conf`.
Copy the bundled example, then start with the router on:

```bash
cp router.conf.example router.conf   # then edit the relay list / filters
ROUTER_CONFIG_LOCAL=./router.conf \
ROUTER_CONFIG_FILE=/etc/vespa-relay/router.conf \
ROUTER_BACKFILL_SECONDS=86400 \
docker compose up --build
```

Leave `ROUTER_CONFIG_FILE` unset (the default) and the relay serves without
mirroring.

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

### Search extensions (NIP-50)

The `search` field accepts extra tokens beyond the query text. They are stripped from
the query before matching, so they never become search terms.

| token | effect |
|---|---|
| `sort:rank` | order by trust, most trusted first (also `rank:asc`, `followers`, `text`) |
| `filter:rank:gte:N` | drop results below trust rank `N` |
| `include:spam` | lift the default trust floor and include everything |
| `observer:<pubkey>` | rank as seen by that pubkey's web of trust |

By default a search is trust-gated: results below the floor are hidden unless you lift it.

## Embed it

The relay also runs inside your own JVM/Ktor app. `serveRelay(relay, port, ...)` binds a
port batteries-included, or `Route.nostrRelay(relay)` and friends mount the pieces in an
existing server. See `NostrRelayServer` and `RelayApp.kt`.

## Build

```bash
./gradlew build              # compile + tests + spotlessCheck
./gradlew :relay:run         # run against VESPA_URL / RELAY_URL from the environment
./gradlew :relay:installDist # a runnable distribution under relay/build/install/vespa-relay
```

Kotlin 2.4 / JDK 21. Quartz and the [vespa-eventstore](https://github.com/vitorpamplona/vespa-eventstore)
store come from JitPack, pinned by commit in `gradle/libs.versions.toml`.

## License

MIT © Vitor Pamplona
