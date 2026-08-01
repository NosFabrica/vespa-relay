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
| `DEFAULT_OBSERVER` | the `npub1…` whose web of trust ranks anonymous searches — somebody's public key, usually the NIP-85 provider you trust, not this relay's. Hex parses too, but a bad value stops the relay rather than being ignored | unset ⇒ untrusted |
| `AUTO_DEPLOY` | deploy the bundled schema on first run | `true` |
| `LOG_CONNECTIONS` | log the live connection count on connect/disconnect | `false` |

### Relay identity (NIP-11)

| var | meaning | default |
|---|---|---|
| `RELAY_NAME` / `RELAY_DESCRIPTION` / `RELAY_ICON` / `RELAY_BANNER` | how the relay presents itself | — |
| `RELAY_CONTACT` | a human contact | — |
| `RELAY_CONTACT_PUBKEY` | the human operator's pubkey, for NIP-11 contact. The relay's own `self` is derived from `RELAY_NSEC`, not set here | — |
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
| `ALLOW_PUBKEYS` / `DENY_PUBKEYS` | write authorization by pubkey — allowlist (empty ⇒ everyone) minus denylist. `npub1…` or 64-hex, comma/space-separated. An entry that cannot be read stops the relay instead of being dropped: a ban that is not enforced looks exactly like one that was never configured | — |
| `ALLOW_KINDS` / `DENY_KINDS` | write authorization by kind — allow (empty ⇒ all) minus deny | — |
| `REJECT_FUTURE_SECONDS` | reject events dated more than N seconds in the future | `0` (off) |
| `EXPIRATION_SWEEP_SECONDS` | how often to prune NIP-40 expired events | `3600` (0 ⇒ off) |

### Admin (NIP-86)

| var | meaning | default |
|---|---|---|
| `RELAY_ADMIN_PUBKEYS` | comma/space-separated admin keys, `npub1…` or 64-hex; when set, enables the NIP-86 management API (`POST /`, NIP-98 auth). An unreadable entry fails startup rather than yielding an admin who silently cannot administer | unset ⇒ off |
| `RELAY_STATE_FILE` | path where NIP-86 ban/allow lists are persisted (survives restart) | unset ⇒ in-memory |
| `RELAY_HTTP_URL` | the http(s) url NIP-98 auth events must be tagged with | derived from `RELAY_URL` |

### Router (upstream mirror)

| var | meaning | default |
|---|---|---|
| `ROUTER_CONFIG` | the router `streams { }` config, inline (HOCON). When set, the relay mirrors upstream events into its store | unset ⇒ router off |
| `ROUTER_CONFIG_FILE` | path to a file holding that config, as an alternative to `ROUTER_CONFIG` | — |
| `ROUTER_UP_INTERVAL_SECONDS` | how often `up`/`both` streams re-reconcile to push newly-arrived local events upstream | `300` |
| `VESPA_MEM_LIMIT` / `RELAY_MEM_LIMIT` | container memory limits. Not cosmetic: `MaxRAMPercentage` reads the **cgroup**, so without a limit the relay's JVM sizes its heap against the whole host — 45% of 47 GiB — while the engine independently grows to 32 GiB, entitling both to more than the machine has. Bounding the relay also makes ingest backpressure work instead of letting it grow into the engine's memory | `34g` / `12g` |
| `RELAY_NSEC` | this relay's own keypair (`nsec1…` or 64 hex), used everywhere it acts as itself: the NIP-11 `self` it advertises (**derived**, so it is provable rather than merely asserted), the NIP-42 challenges it answers, and the NIP-66 kind-30166 liveness records it signs. Relays that gate reads behind AUTH are indistinguishable from empty ones without it. Unset ⇒ anonymous — reading other monitors' 30166s still works and needs no key. Malformed ⇒ startup fails | unset ⇒ anonymous |
| `ROUTER_FULL_RESYNC_SECONDS` | how long a recorded sync window may narrow work before the router walks the whole filter again. A finished negentropy reconcile covers its filter's entire range, so the next run asks only for what arrived since — which is what keeps a dynamic cycle's shared id snapshot from being the entire corpus. Relays do gain old events, so the claim is re-tested on this period. Nothing is ever capped; the full pass is periodic, not skipped | `604800` (7 days) |
| `ROUTER_SYNC_STATE_FILE` | where the per-(relay, filter) synced `created_at` band is kept. A relay without NIP-77 has no memory of what it already sent, so without this every restart re-downloads its whole corpus; with it the router asks only for what falls outside the band it already walked. Keyed by filter — edit a stream's filter and that stream starts over | unset ⇒ in memory only |
| `ROUTER_INGEST_BATCH` / `ROUTER_INGEST_CONCURRENCY` | mirrored events are drained in batches and written through the store's bulk path. The store serializes writes, so throughput comes from the batch size (a sweet spot near the default — much larger stalls on long mutex holds), not the worker count. Lower the batch to cut memory | `1000` / `2` |
| `ROUTER_DYNAMIC_REFRESH_SECONDS` | default period between cycles of a `relaySource = [...]` stream (re-read the sources, re-sync every relay) | `21600` (6h) |
| `ROUTER_DYNAMIC_CONCURRENCY` | default number of discovered relays synced at the same time | `8` |

### Parse audit (what quartz cannot read)

Mirroring profiles replays every malformed kind 0 ever published through quartz's
`UserMetadata` deserializer, because `SearchableEvent.indexableContent()` is what
builds the NIP-50 search text. Quartz reports what it cannot read, one line per
event, which buries the router's own logging:

```
[MetadataEvent] Content Parse Error: nostr:naddr1… Expected start of the object '{', but had 'EOF' instead
[TolerantStringSerializer] Ignoring non-primitive string field (JsonObject)
[BirthdayTolerantSerializer] Ignoring non-object birthday (JsonLiteral)
```

| var | meaning | default |
|---|---|---|
| `PARSE_AUDIT_FILE` | collect those failures into a JSON report at this path instead of logging each one. Unset ⇒ off | unset |
| `PARSE_AUDIT_SAMPLES` | raw events kept per distinct failure, for a quartz regression test | `5` |
| `PARSE_AUDIT_INTERVAL_SECONDS` | how often the report is rewritten while running | `60` |
| `QUARTZ_LOG_LEVEL` | quartz's own log floor — `DEBUG` / `INFO` / `WARN` / `ERROR`. Quartz defaults to `DEBUG`, which is why the parse reports are so loud. Works with or without the audit | quartz's default |

The report groups by failure rather than by event, so "the same quartz gap" is one
entry with a count however many events hit it, each carrying a few whole events:

```json
{
  "inspected": 412330, "eventsWithFindings": 1876, "distinctFindings": 4,
  "findings": [
    { "tag": "MetadataEvent", "count": 1204,
      "message": "Content Parse Error: <event> Expected start of the object '{', but had 'EOF' instead at path: $",
      "samples": [ { "eventId": "…", "pubkey": "…", "event": { "…the whole event…" } } ] }
  ]
}
```

Note the severity split. `MetadataEvent Content Parse Error` means the content was
not a JSON object at all, so there is no metadata to index and that profile is not
findable by name. The tolerant-serializer entries mean the parse *succeeded* and one
wrongly-typed field was skipped by design — noise, unless quartz should be widening
what it accepts.

The audit runs each parse itself, on the ingest worker, because a `LogSink` receives
only `(level, tag, message, throwable)` — no event. That is also why it is opt-in: it
costs one extra parse per mirrored event. See `ParseAudit`.

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
    dir    = "down"
    # since/until are the ordinary NIP-01 fields: this one reaches back a day,
    # while the stream above names neither and so asks for the whole history.
    filter = { "kinds": [0, 3, 5, 1984, 10000, 30000], "since": 1785000000 }
    urls   = [ "wss://profiles.nostr1.com", "wss://directory.yabu.me", "wss://relay.ditto.pub" ]
  }
}
```

Each named stream mirrors a NIP-01 `filter` from a set of `urls`. Per stream:

- **`dir`** — `down` mirrors upstream events into our store; `up` publishes our
  matching events to the upstream; `both` does each on the same relay.
- **`filter`** — the NIP-01 filter to mirror (kinds, authors, `#tags`, …),
  including `since` / `until`. They mean what NIP-01 says: absent is unbounded,
  so a stream naming neither backfills the upstream's **whole history**. Bound it
  with `since` when that is not what you want. Upstreams without NIP-77 fall back
  to paged REQ automatically.
- **`trusted`** *(optional)* — skip signature verification for this upstream's
  events. Off by default; every mirrored event is verified and re-checked
  against the stream filter before it enters the store.

The router shares the relay's Vespa store, so mirrored events are immediately
searchable. It runs one outbound connection per upstream (reconnect and
re-subscribe are handled for you) and logs unreachable upstreams rather than
failing — a paused or down relay in the list is skipped, not fatal.

**Down** keeps a live subscription open and first negentropy-reconciles the
history its filter asks for. **Up** re-reconciles the store against the
upstream every `ROUTER_UP_INTERVAL_SECONDS` and publishes only what the
upstream is missing — set reconciliation gives echo-suppression for free, so an
event just pulled *down* from a relay is never pushed back *up* to it.

### Resuming a paged relay

Negentropy relays need no cursor: reconciliation compares id sets and downloads
only the difference, so re-running a sync costs the diff and nothing more. Most
relays do not speak NIP-77 — in one measured run, seven of nine upstreams fell
back to paged REQ — and a paged fetch has no such memory. It walks `created_at`
newest-first and re-reads everything it read last time, every restart.

Set `ROUTER_SYNC_STATE_FILE` and the router remembers the band it has covered per
relay and per filter, then asks only for what lies outside it:

```
stored band:        |<-------- covered -------->|
next fetch:  <------|                           |------>
             older than min                newer than max
```

The newer leg catches what was published while the relay was down. The older leg
keeps walking back into history, which is what makes progress against a relay
that caps its responses: each run reaches a little further instead of re-reading
the same newest events forever.

The two boundary seconds are re-read every run, deliberately: a paged relay cuts
pages by count, so a boundary can fall inside a run of events sharing one
`created_at`, and asking strictly outside the band would strand the rest of that
second forever.

One thing it does not promise: Nostr lets an event be published with any
`created_at`, so one can land inside a band already walked past. The trade is
deliberate — re-reading a corpus every restart is a certain daily cost, while
that hole is occasional and clears the next time the filter changes.

The **live tail works against every relay**; the **negentropy backfill depends
on the upstream**. Some relays advertise NIP-77 but their reconciliation never
converges. One bound handles that: a session with no protocol frames for 30
seconds aborts itself, and the upstream leans on its live tail while relays that
reconcile cleanly backfill in full. There is deliberately no wall-clock deadline —
every timeout is measured from the last message, so a relay that stops answering
is already gone, and one still sending is doing the work we asked for. So a
brand-new store is filled forward from connect universally, and backfilled
historically for the relays whose NIP-77 cooperates.

While backfilling, the router logs progress and an ETA to "useful" (backfill
complete), so you can tell how long the initial fill will take:

```
router: backfill 4/12 upstream(s), 12,340/29,110 events (42%), 851/s, ETA ~0:03:17 to useful
router: backfill complete — 41,880 events from 12 upstream(s) in 0:04:52; live tail now streaming
```

### Dynamic relay lists: the outbox, and everything else that names a relay

A stream can leave `urls` out entirely and take its relay list from the store
instead. `relaySource` is a **list** of places to read urls from, all merged into
one fan-out:

```hocon
outbox {
  dir             = "down"
  filter          = { "kinds": [0, 3, 10002, 10040] }
  refreshSeconds  = 21600
  concurrency     = 8
  exclude         = []
  relaySource = [
    {
      select = [
        # NIP-65 outbox
        {
          kind = 10002
          tag = "r"
          marker = "write"
        }
        # NIP-66 monitor reports
        {
          kind = 30166
          tag = "d"
        }
        # everything else in the scan
        {
          tag = "relay"
        }
      ]
      filter = { "kinds": [10002, 10050, 30002, 30166] }
    }
    {
      select = [
        # relay hints, thread markers only
        {
          tag = "e"
          index = 2
          where = [
            { index = 3, equals = "root" }
            { index = 3, equals = "reply" }
          ]
        }
      ]
      filter = { "kinds": [1], "limit": 100000 }
    }
  ]
}
```

Each entry is one **scan**: a `select` list saying which relay urls to pull out,
and the `filter` saying which events to pull them from. The filter runs once and
every select is applied to what comes back, so a whole shelf of relay-list kinds
costs one query rather than one each.

Each cycle runs every scan, unions the relays they name, and negentropy-syncs the
stream `filter` against **all** of them, `concurrency` at a time (paged REQ where
NIP-77 is missing, same as a backfill). Then it sleeps `refreshSeconds` and does
it again — so the fan-out widens on its own as the store fills.

Nothing truncates that set: no cap on relays synced and no popularity floor.
`concurrency` paces the fan-out, it doesn't bound it, and `exclude` is the only
way to leave a relay out.

**No kind needs its own code.** Every relay list in the protocol is a tag with a
url at a fixed offset, so a select is just that shape:

| field | meaning |
|---|---|
| `kind` | apply this select only to that kind; **omit to apply it to everything the filter collected**. A kind the scan never returns simply never matches |
| `tag` | the tag name to read; **omit for any tag** — that's how you take a whole family like NIP-85's `<kind>:<type>` service tags without naming each one |
| `index` | which element holds the url. `1` for nearly everything; `2` for NIP-85 service tags and for `e`/`p`/`a`/`q` hints, which put an id or pubkey first |
| `where` | conditions on the rest of the tag, shaped like NIP-01 filters: entries in the list **OR** together, the fields inside one entry **AND**. Each entry states any of `index` + `equals` (the element at that position is exactly that string — case-sensitive and untrimmed, and a missing element matches nothing, not even `""`), `minSize`, and `maxSize` (bounds on the tag's length). Omit to keep every tag |
| `marker` | sugar for NIP-65's rule: `write` / `read` expand to the `where` that keeps that side *plus* unmarked tags — with the url at 1, `[ { index = 2, equals = "write" }, { index = 2, equals = "" }, { maxSize = 2 } ]`, the slots following the select's own `index` — and `any` to no conditions. A select states `marker` or `where`, not both |

The scan's `filter` is an ordinary NIP-01 filter — `kinds`, `authors`, `since`,
`until`, `limit`, `#t`-style tag filters — so you can narrow it however you like:
`{ "kinds": [1], "authors": [...] }` harvests hints from your WoT's notes only.

Three things worth knowing:

- **`tag` and `"#t"` are unrelated.** `tag = "e"` is the tag urls are *read
  from*; a `"#e"` entry in the scan's filter narrows *which events are scanned*.
- **Omitting `tag` demands a scheme.** With no tag name to filter on, anything in
  the event could land at `index` — a pet name in a `["p", <pubkey>, "bob"]` tag
  would otherwise normalize to `wss://bob/`. So values must already start with
  `ws://` or `wss://`. Name a tag and scheme-less hosts work again, which is what
  NIP-65 lists in the wild need.
- **Regular kinds must narrow their scan.** Relay hints live on kind 1, and
  scanning that kind whole would load every note in the store into one list. The
  replaceable and addressable kinds hold one event per author, which is what makes
  them safe to scan outright; anything else needs `limit`, `since` or `authors`,
  and the parser rejects it rather than let you find out in production. Prefer
  `limit` on a repeating cycle — `since`/`until` are absolute unix seconds, so a
  fixed `since` only ages, while `until` alone bounds nothing at all.

All of it needs events to fan out over, so pair these with an ordinary `down`
stream on a few relays — that seed stream is what fills the store. The example
config's static streams do exactly that, which is why they come first in the file.

Some notes on the other knobs:

- **the filter's `since`** is the history each cycle reconciles. Keep it
  longer than `refreshSeconds` so consecutive cycles overlap. Leave it unset and
  every cycle reconciles the filter's whole history — cheap enough over
  negentropy, which diffs against what we already hold, but a relay *without*
  NIP-77 falls back to paged REQ, which carries no such state and re-pages its
  entire history on every cycle.
- **`concurrency`** is the one dial on cost. The union of every scan on a full
  store is a large set — plenty of it long-dead hosts that will each burn a
  connect timeout — so the cycle is as long as it needs to be, and this decides
  how much of the network it talks to at once.
- These streams have **no live tail**. Holding hundreds of subscriptions open is
  what the periodic sync exists to avoid, so each relay's socket is dropped again
  as soon as its sync returns. `dir` must be `down`, and a `relaySource` stream
  can't also carry static `urls` — split those into two streams.

Every cycle logs what it did, including why the unreachable relays were
unreachable (a relay list is full of dead hosts — the tally is how you tell
"normal" from "the whole cycle is broken") and what the rejections were. Expect
rejections to *outnumber* accepts on a wide fan-out: a thousand relays asked for
the same replaceable profiles means the store discards nearly every copy as
already-held, which is the system working, not failing. The breakdown is there so
a bad signature or a failing store doesn't hide inside that number:

```
router: outbox syncing 3184 relay(s) from [kinds 10002/10050/30002/30166 x3 select(s), kinds 1 x1 select(s)] against 88412 local id(s) (top: wss://relay.damus.io/ x8214, ...)
router: outbox cycle done — 214,880 event(s) from 1102/3184 relay(s) in 1:12:41; unreachable: timeout x938, Connection refused x421; next in 21600s
router: ingested 214880 accepted, 402113 rejected [duplicate: already have this event x401980]; 14 relay(s) connected, 12 pinned + dynamic
```

### Enabling it under docker compose

`docker-compose.yml` wires the router env through and mounts `./router.conf`.
Copy the bundled example, then start with the router on:

```bash
cp router.conf.example router.conf   # then edit the relay list / filters
ROUTER_CONFIG_LOCAL=./router.conf \
ROUTER_CONFIG_FILE=/etc/vespa-relay/router.conf \
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
