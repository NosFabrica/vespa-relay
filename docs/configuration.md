# Configuration reference

All configuration is through environment variables. `.env.example` in the repo
root is a commented, copyable starting point for docker compose; this file is
the complete reference. The router's stream config format is documented in
[`router.md`](router.md).

There are two processes. The **relay** (`vespa-relay`, `RelayMain`) serves; the
**sync process** (`vespa-sync`, `SyncMain`) — the router — mirrors upstream
relays into the same store, as its own container so it can be restarted or
retuned without the relay dropping a client. The sections below say which
process reads what: Core and everything through Admin is the relay (the sync
process also reads `VESPA_URL`, `RELAY_URL`, `RELAY_NSEC`, `AUTO_DEPLOY`,
`VESPA_CONFIG_URL`, `VESPA_QUERY_FANOUT` and `JAVA_TOOL_OPTIONS`); the Router
section and the parse audit are the sync process. Aiming `SYNC_CONFIG`/`SYNC_CONFIG_FILE` at the
relay fails its boot on purpose — the setting would once have started the
mirror there, and accepting-but-ignoring it is the silent inertness this
codebase forbids.

## Core

| var | meaning | default |
|---|---|---|
| `RELAY_URL` | this relay's own ws url — its NIP-42 identity and NIP-62 vanish scope | **required** |
| `VESPA_URL` | the Vespa query endpoint | `http://localhost:8080` |
| `RELAY_PORT` | port to listen on | `7777` |
| `FTS_CURSOR_FILE` | where the reindex saves its position, so a restart resumes rather than redoing the corpus | `/var/lib/vespa-relay/fts-cursor.txt` |
| `REINDEX_FTS_ON_START` | re-derive every event's search fields once, in the background. Needed after a store upgrade that changes `SearchExtractors` or adds *fed* search fields — a Vespa reindex cannot produce those, only a re-put can. Walks the whole corpus, so leave it off except for the boot that performs the migration | `false` |
| `TRUST_RECONCILE_ON_START` | reconcile the trust projection at startup, in the **background** — the relay serves immediately and ranked search returns less until it finishes. `false` skips it entirely | `true` |
| `AUTO_DEPLOY` | deploy the bundled Vespa schema on **every** boot, so the cluster always matches the schema this build expects. Both processes do it — the sync process is the one whose writes a drifted schema silently discards, and a sync-only box has no relay to deploy for it. A no-change deploy is a cheap no-op. If the deploy fails while Vespa is already serving a schema, the process warns and keeps running on it — an unreachable config server must not take down a relay that ran fine yesterday — and writes carrying fields that schema lacks stay rejected until a deploy succeeds. On a fresh Vespa there is nothing to fall back to, so it is fatal | `true` |
| `VESPA_CONFIG_URL` | Vespa's config server, for the deploy above | `VESPA_URL` on `:19071` |
| `VESPA_PORT` / `VESPA_CONFIG_PORT` | ports published on the **host** for the two above. Compose only — nothing inside the containers moves | `8080` / `19071` |
| `VESPA_QUERY_FANOUT` | concurrent queries the store issues per bulk operation. Higher makes ingest faster; lower leaves more of the engine for the people searching | `4` |
| `JAVA_TOOL_OPTIONS` / `RELAY_JAVA_TOOL_OPTIONS` / `SYNC_JAVA_TOOL_OPTIONS` | each JVM's own flags, in practice its heap. The bare name reaches **both** containers under compose; the prefixed spellings override per process — the two JVMs are sized 2× apart, so an absolute `-Xmx` tuned for the sync's reconcile heap would kill the smaller relay container. For the sync process this is what decides whether a large reconcile finishes or dies with `OutOfMemoryError`. The percentage is of the container's own mem limit, because `MaxRAMPercentage` reads the cgroup | `-XX:MaxRAMPercentage=70` |
| `SWEEP_ORPHAN_SCORES_ON_START` | **deletes data.** Removes every kind-30382 signed by a provider that no stored kind-10040 names — cards nothing can rank with and nobody reads, which a by-kind mirror accrues by the million. Any value other than `true` is a **dry run** that reports what it would remove and removes nothing. Pair it with narrowing the sync (see [Binding filter fields to a relay](router.md#binding-filter-fields-to-a-relay)) or the next pass re-downloads what it freed | unset ⇒ off |
| `GUARD_OWNERS_DISABLE` | **nothing to set.** Both processes tell the store their writer topology in code (`STORE_WRITERS` = `SHARED_STRICT`), so NIP-09 and NIP-62 are checked against the store on every insert — see [Mirroring the deletions themselves](router.md#mirroring-the-deletions-themselves). This variable can only force that same floor, never weaken it, and the store now fails to open on a value it cannot parse | unset ⇒ the code's choice stands |
| `LOG_CONNECTIONS` | log the live connection count on connect/disconnect | `false` |

## Corpus statistics (`GET /stats.json`, `/stats.html`)

A background rollup counts what this relay's store holds with Vespa grouping
queries, publishes it as a public JSON document, and charts it on a page:

- **corpus** — events, distinct pubkeys, distinct kinds, the age of the newest
  event, and how many events are dated in the *future* (clock skew and spam;
  every freshness number excludes them)
- **trust** — observers, score providers, stored scores, and pubkeys carrying
  projected trust state. The one section reading a second document type, and the
  only place a silently-empty web of trust shows up: `scoredPubkeys` at zero
  means every ranked search is falling back to unranked, which looks identical
  to a healthy relay on every other panel
- **kinds** — EVERY kind in the store, with distinct authors and the
  `created_at` span. This replaced `/kind_stats.html` (whose url now 301s to the
  page): that page asked one NIP-45 `COUNT` per kind it already knew to name, so
  a kind nobody had registered was invisible on the only page that would have
  revealed it — a grouping histogram enumerates instead
- **activity** — events and publishing pubkeys per UTC day, week and month, plus
  the hour-of-day shape. Three granularities and not one re-aggregated: events
  sum across buckets, distinct authors do not, so a weekly author count has to
  be asked of the engine per week
- **kindActivity** — a daily series per kind, for the largest few
- **relayDistribution** — the relays this store's NIP-65 lists name
- **zaps** — kind-9735 receipt counts, and the wallets that signed them

Per-kind `lastSeen` and the corpus-wide newest event are both bounded to the
present, and the page renders them as **ages** rather than dates: "kind 1 four
minutes ago, kind 30023 six days ago" localises which stream stalled, where a
column of timestamps makes the reader do the subtraction.

Everything is counted **anonymously**: the store gates an authenticated reader to
authors that reader has scored, so the same query under an operator's lens would
answer a smaller, different question under an identical label.

The numbers describe **this relay's store**, not the Nostr network. Read against
a network-wide dashboard they are a coverage ratio, which is the useful number
for a mirroring relay — but a total below one is not a fault, and the document
says so in its own `scope` field.

Two sections are computed but incomplete, and carry a `note` saying what they leave
out. **Zaps** has receipt counts but no satoshis: the amount lives in the
`bolt11` tag and in the kind-9734 request nested in `description`, both
multi-character tag names that `tag_index` cannot address, and `content` is
summary-only — so no grouping query can reach it. **Relay distribution** counts
how many lists name each relay but cannot split read from write: that marker is
an `r` tag's *third* element, and `tag_index` stores only `<letter>:<value>`.
Both want a walk over their kind, which is the job neither has yet.

| var | meaning | default |
|---|---|---|
| `STATS_INTERVAL_SECONDS` | how often to recompute. `0` or negative disables the rollup entirely — a legitimate choice on a busy box, since the grouping competes with client reads; `/stats.json` then serves whatever the state file holds, or 503. The first pass on a large corpus takes minutes and runs **behind** the server, so a restart never waits on it | `900` |
| `STATS_FILE` | where the document is persisted, so a restart serves the last one instead of a blank page while the first rollup runs. Written atomically (temp file + move). Readable on the host through the same bind mount as the FTS cursor and the parse audit | `/var/lib/vespa-relay/stats.json` |

`GET /stats.json` is public, like `GET /pressure` and the other stats pages, and
publishing it is most of the point — anyone can chart this relay's coverage
without scraping the page's markup. It carries an `ETag` and answers `304` to a
conditional request, which is what makes polling it cheap. The one rule to hold
when adding fields: everything in the document is a fact about **stored events**,
never about clients. `/pressure` caps its `samples` field for exactly that
reason.

## Relay identity (NIP-11)

| var | meaning | default |
|---|---|---|
| `RELAY_NAME` / `RELAY_DESCRIPTION` / `RELAY_ICON` / `RELAY_BANNER` | how the relay presents itself | — |
| `RELAY_CONTACT` | a human contact | — |
| `RELAY_CONTACT_PUBKEY` | the human operator's pubkey, for NIP-11 contact. The relay's own `self` is derived from `RELAY_NSEC`, not set here | — |
| `RELAY_VERSION` | overrides the build version | — |
| `RELAY_POSTING_POLICY` / `RELAY_PRIVACY_POLICY` / `RELAY_TERMS_OF_SERVICE` | policy urls | — |

## Limits

| var | meaning | default |
|---|---|---|
| `MAX_MESSAGE_LENGTH` / `MAX_SUBSCRIPTIONS` / `MAX_FILTERS` / `MAX_LIMIT` / `DEFAULT_LIMIT` / `MAX_SUBID_LENGTH` / `MAX_EVENT_TAGS` / `MAX_CONTENT_LENGTH` / `MIN_POW_DIFFICULTY` / `CREATED_AT_LOWER_LIMIT` / `CREATED_AT_UPPER_LIMIT` | protection limits, enforced by the engine and shown in the NIP-11 `limitation` block | sane defaults |
| `NEG_FRAME_SIZE_LIMIT` / `NEG_MAX_SYNC_EVENTS` / `NEG_MAX_SESSIONS_PER_CONNECTION` | NIP-77 negentropy tuning (`NEG_MAX_SYNC_EVENTS` caps how many ids one reconciliation walks) | strfry-parity |

## Access control

| var | meaning | default |
|---|---|---|
| `ALLOW_PUBKEYS` / `DENY_PUBKEYS` | write authorization by pubkey — allowlist (empty ⇒ everyone) minus denylist. `npub1…`, comma/space-separated (bare hex is refused — it has no checksum). An entry that cannot be read stops the relay instead of being dropped: a ban that is not enforced looks exactly like one that was never configured | — |
| `ALLOW_KINDS` / `DENY_KINDS` | write authorization by kind — allow (empty ⇒ all) minus deny | — |
| `REJECT_FUTURE_SECONDS` | reject events dated more than N seconds in the future | `0` (off) |
| `EXPIRATION_SWEEP_SECONDS` | how often to prune NIP-40 expired events | `3600` (0 ⇒ off) |

## Admin (NIP-86)

| var | meaning | default |
|---|---|---|
| `RELAY_ADMIN_PUBKEYS` | comma/space-separated admin keys, `npub1…`; when set, enables the NIP-86 management API (`POST /`, NIP-98 auth). An unreadable entry fails startup rather than yielding an admin who silently cannot administer | unset ⇒ off |
| `RELAY_STATE_FILE` | path where NIP-86 ban/allow lists are persisted (survives restart) | unset ⇒ in-memory |
| `RELAY_HTTP_URL` | the http(s) url NIP-98 auth events must be tagged with | derived from `RELAY_URL` |

## Serving over Tor (a `.onion` endpoint)

The relay can answer on a Tor hidden service as well as on its clearnet url —
the same port, the same store, the same web UI, reachable by any client that
speaks Tor and without a certificate or a public IP. Under docker compose it is
one profile:

```bash
docker compose --profile onion up -d
docker compose logs tor-onion | grep 'reachable at'
# onion: this relay is reachable at ws://<56 chars>.onion (published to /var/lib/onion/hostname)
```

That address is the public half of a key the `tor-onion` container generates on
first start and keeps in its own volume: it survives restarts and rebuilds, and
`docker compose down -v` — which removes volumes — is what changes it. Nothing
publishes the address for you; hand it to clients yourself.

The `tor-onion` service is separate from the `tor` service the router dials
`.onion` **upstreams** through (`--profile sync`, `SYNC_TOR_SOCKS`). They are
wanted independently: a serving-only relay can have a front door on Tor without
mirroring anything, and a mirror can reach hidden services without being one.

Clients dial `ws://<address>.onion` — port 80, no TLS. Tor's own encryption is
what TLS would have provided, and no CA issues certificates for `.onion` names,
so there is no `wss://` to offer.

| var | meaning | default |
|---|---|---|
| `RELAY_ONION_HOSTNAME_FILE` | file the hidden service's container writes its hostname into. The relay reads it to accept NIP-42 from Tor clients, who sign the address they dialled and have never heard of `RELAY_URL` — without it those clients still read and publish, but every AUTH fails and their search silently loses its web-of-trust lens. Looked at on demand rather than once at boot, so an address minted after the relay started is picked up within a second of the next connection; absent is normal and says nothing | compose: `/var/lib/onion/hostname`; bare: unset |
| `RELAY_ONION_ADVERTISE` | whether the clearnet endpoint names the hidden service in `Onion-Location` (below). `false` keeps an onion unlisted — the relay still authenticates clients that dial it, it just stops handing out the address, which clients cache for a day | `true` |
| `RELAY_ONION_URL` | the same thing declared by hand, for a hidden service run outside this compose file. Malformed ⇒ startup fails, rather than costing Tor clients their ranking lens for a reason nothing reports | unset |

Both may be set; the relay answers to `RELAY_URL` and to every address either
one names. The published file is re-read when its timestamp moves, at most once
a second however much traffic asks, so an address minted after boot — or
rotated by a new key — lands within a second rather than at the next restart.

### Telling clients the address exists

The clearnet endpoint advertises the hidden service on **every** response —
`Onion-Location: http://<address>.onion/`, the Tor Project's header. Nothing to
configure: it appears as soon as the relay knows its own address and names
whatever the `tor-onion` container published.

Two things read it. Tor Browser turns it into the ".onion available" button on
the web UI. Amethyst records it from any response its OkHttp client sees —
including the **WebSocket 101 handshake**, which is often the only request a
Nostr client makes — and, when the user has Tor enabled, dials the `.onion`
instead, so the connection never crosses an exit node. The value is an `http`
url rather than `ws` because both parse it with an http url parser; a `ws://`
value parses to null in okhttp and the advertisement would vanish silently.

A request that already arrived over the hidden service is not told about it,
and `RELAY_ONION_ADVERTISE=false` turns the whole advertisement off for an
onion meant to stay unlisted.

Clients that move a connection this way keep signing NIP-42 with the address
they were configured with — Amethyst rewrites the host at the transport layer,
not the relay's identity — so the relay has to accept the clearnet url on a
connection that arrived through the onion, and the reverse. It accepts any
address it answers at, whichever door the connection came through.

One thing does **not** follow the relay onto Tor: the NIP-86 management API.
Its NIP-98 tokens are bound to the single `RELAY_HTTP_URL`, deliberately — the
alternative is trusting the `Host` header, which would let anyone bind a signed
admin token to any url. An admin working over Tor either administers through
the clearnet url or points `RELAY_HTTP_URL` at the `.onion`; it is one or the
other, and a token minted for the wrong one is refused rather than ignored.

### Tuning the hidden service

Anything the bundled torrc does not carry goes in a mounted file, appended to
the generated config: copy [`tor/onion.extra.conf.example`](../tor/onion.extra.conf.example),
uncomment what you want, and point `ONION_EXTRA_LOCAL` at your copy. It
documents the three worth knowing about — **single-hop mode**, which roughly
halves latency by dropping the service's own three hops to one (and is a
one-way door on that key: tor refuses to launch a service from a directory
whose anonymity mode changed), tor's **proof-of-work defenses** against
introduction flooding, and the **number of introduction points**. A file of
only comments is skipped, so the default mount changes nothing.

## Router (the sync process)

These configure `vespa-sync`, the mirror's own process — under docker compose,
the `sync` service behind the `--profile sync` switch. Restarting it (say,
after a `router.conf` edit) never touches the relay, and `SYNC_STATE_FILE`
makes the re-run resume instead of re-downloading.

| var | meaning | default |
|---|---|---|
| `SYNC_CONFIG` | the router `streams { }` config, inline (HOCON). **Required** (or `SYNC_CONFIG_FILE`): a sync process with nothing to sync refuses to start rather than idle in a way that reads as "syncing". Every `SYNC_*` variable also accepts its pre-rename `ROUTER_*` spelling, with a warning | — |
| `SYNC_CONFIG_FILE` | path to a file holding that config, as an alternative to `SYNC_CONFIG` | compose: `/etc/vespa-relay/router.conf` |
| `SYNC_CONFIG_LOCAL` | compose only: the **host** path mounted at `SYNC_CONFIG_FILE` — point it at your copy, or the sync reads the example rather than your config | `./router.conf.example` |
| `SYNC_PRESSURE_URL` | the relay's `GET /pressure` endpoint, polled every 5s so ingest yields when client reads slow down — the clients-first rule, across the process boundary. After ~15s of failed polls the throttle resets (a relay that is down has no clients to protect) and the log says so. Unset ⇒ mirror at full speed, stated at boot | compose: `http://relay:7777/pressure`; bare: unset |
| `SYNC_UP_INTERVAL_SECONDS` | how often `up`/`both` streams re-reconcile to push newly-arrived local events upstream | `300` |
| `VESPA_MEM_LIMIT` / `RELAY_MEM_LIMIT` / `SYNC_MEM_LIMIT` | container memory limits. Not cosmetic: `MaxRAMPercentage` reads the **cgroup**, so without a limit a JVM sizes its heap against the whole host — 70% of 47 GiB — while the engine independently grows to 32 GiB, entitling the set to more than the machine has. The sync process carries the largest JVM share because the negentropy id snapshots live there; bounding it also makes ingest backpressure work instead of letting it grow into the engine's memory | `34g` / `6g` / `12g` |
| `RELAY_NSEC` | the relay's own keypair (`nsec1…` only), used everywhere it acts as itself: the NIP-11 `self` it advertises (**derived**, so it is provable rather than merely asserted), the NIP-42 challenges it answers, and the NIP-66 kind-30166 liveness records it signs. Give the sync process the **same** key — its upstream AUTH answers and monitor records then speak as the relay it feeds. Relays that gate reads behind AUTH are indistinguishable from empty ones without it. Unset ⇒ anonymous — reading other monitors' 30166s still works and needs no key. Malformed ⇒ startup fails | unset ⇒ anonymous |
| `SYNC_FULL_RESYNC_SECONDS` | how long a recorded sync window may narrow work before the router walks the whole filter again. A finished negentropy reconcile covers its filter's entire range, so the next run asks only for what arrived since — which is what keeps a dynamic cycle's shared id snapshot from being the entire corpus. Relays do gain old events, so the claim is re-tested on this period. Nothing is ever capped; the full pass is periodic, not skipped | `604800` (7 days) |
| `SYNC_STATE_FILE` | where the synced `created_at` band is kept, nested `{stream: {filter: {relay: {…}}}}`. A relay without NIP-77 has no memory of what it already sent, so without this every restart re-downloads its whole corpus; with it the router asks only for what falls outside the band it already walked. Keyed by filter — edit a stream's filter and that stream starts over — and by stream, so two streams asking one relay the same filter each keep their own progress. A file written before the format nested still loads: its keys name no stream, so each is handed to the first stream that asks for that (relay, filter) and rewritten under that name. **Also read by the relay**, off the shared `/var/lib/vespa-relay` mount, for the Sync coverage card on `/stats.html`: set the same path on both services (the compose default already matches) or the card charts where the router used to write. Read-only there — the router stays the only writer | unset ⇒ in memory only |
| `SYNC_INGEST_BATCH` / `SYNC_INGEST_CONCURRENCY` | mirrored events are drained in batches and written through the store's bulk path. The store serializes writes, so write throughput comes from the batch size (a sweet spot near the default — much larger stalls on long mutex holds), not the worker count. Batch width also decides whether a batch is wide enough to earn its dedup probe (128 events needing verification): each batch drops what the store already holds *before* signature-checking it, so a narrow batch verifies more copies. Lower the batch to cut memory | `1000` / `2` |
| `SYNC_DYNAMIC_REFRESH_SECONDS` | default period between cycles of a `relaySource = [...]` stream (re-read the sources, re-sync every relay) | `21600` (6h) |
| `SYNC_DYNAMIC_CONCURRENCY` | default number of discovered relays synced at the same time | `8` |
| `SERVING_PRESSURE_THRESHOLD_MS` | mean client-read latency above which the mirror starts yielding to clients — the mean itself arrives over `SYNC_PRESSURE_URL`, measured by the relay. Reads against a 50M-event store run ~400ms healthy; ingest pauses between batches once the mean passes this | `2000` |
| `SYNC_WIRE_LOG` | what to log of the upstream conversation. Empty still logs `NOTICE`, `CLOSED` and failed sends — the relay's own account of why it stopped. `sent` adds every command sent; `full` adds every message received (one line per event) | *(errors only)* |
| `SYNC_NEG_MIN_EVENTS` | for `sync = "auto"` streams: reconcile once **we** hold at least this many events on the stream's filter, otherwise page. Only our own count decides — a reconcile transfers the difference, so it pays when our set is already most of theirs, and our store answers that for free. Asking the relay as well cost a NIP-45 COUNT per relay per cycle for a worse answer, since COUNT is optional and slow where implemented | `5000` |
| `SYNC_NEG_PAGE_TARGET` | how many events one negentropy reconcile window aims to hold. A stream where we hold more than this is swept in windows instead of one whole-filter pass, which is what keeps the local id snapshot bounded (measured at 14.9M ids for a single stream otherwise) and what makes a killed sync resumable. It is a **starting** size: the sweep shrinks it when a peer refuses or has to split a window itself, and grows it back on clean ones, per peer. `0` turns windowing off and restores the single shared snapshot per stream | `100000` |
| `SYNC_NEG_PAGE_MIN` / `SYNC_NEG_PAGE_MAX` | floor and ceiling for that learned per-peer size. A peer that refuses the floor is refusing negentropy, not sizing it | `1000` / `1000000` |
| `SYNC_NEG_PAGE_SLACK_SECONDS` | how far below `now` a sweep stops. A window is only checkpointable while it is immutable, and the top of the range is still receiving events — this is the seam between the sweep and the live subscription that covers the head | `60` |
| `SYNC_SWEEP_STATE_FILE` | where the per-peer window size (and the cap a peer stated in a rejection) and the in-progress sweep cursor are kept — cursors nested `{stream: {filter: {relay: {…}}}}` like the bands, window sizes flat under `peers` because a peer's cap belongs to its config and not to any ask. Distinct from `SYNC_STATE_FILE`: bands are the long-lived record of what a relay has given us, this is working state a finished sweep throws away. Unset, both are re-learned on every boot — correct, but it pays the sizing ladder again and restarts a partial sweep from the top. Read by the relay too, on the same terms as `SYNC_STATE_FILE` — it is what draws the in-flight sweeps on the coverage card | unset ⇒ in memory only |
| `SYNC_STREAMS` | run only these streams (comma-separated), to tune one part of the sync without the rest competing for the same sockets, heap and ingest queue. The router prints which streams it is *not* running on startup | every stream in the config |
| `SYNC_TOR_SOCKS` | a Tor SOCKS5 proxy (`host:port`) — the transport `.onion` upstreams are dialled through, and the only way one is reachable at all. The hostname is resolved **inside** Tor rather than here, which is what makes a hidden service resolve and what keeps the local resolver from learning which ones this relay syncs with. Unset ⇒ discovery drops every `.onion` it finds and a `.onion` in a stream's `urls` **refuses to boot**, naming the urls — a stream that quietly mirrors nothing is indistinguishable from one that is failing. Malformed ⇒ startup fails rather than degrading to "no Tor". Only hidden services take this route; clearnet relays keep the direct client | compose: `tor:9050`; bare: unset |
| `SYNC_TOR_ALL` | send **every** upstream through Tor, not only the hidden services. A different deployment — no relay learns this box's address — rather than a stronger default: a 20,000-relay dynamic cycle over Tor is a fraction of the throughput, and some large relays refuse exit traffic outright | `false` |
| `SYNC_TOR_CONNECT_TIMEOUT_SECONDS` | connect timeout for Tor dials. A circuit plus a rendezvous is seconds of work before the first byte, where `connectionTimeout` in `router.conf` sizes a clearnet TCP handshake. Transfers are governed by idle windows that reset per message, so they need no Tor-specific value | `90` |
| `SYNC_TOR_MAX_SOCKETS` | how many dials Tor carries at once. Deliberately its own budget rather than a share of the clearnet fan-out's 1024: Tor builds a circuit per stream, and onion relays are a handful, not a fan-out | `32` |

## Parse audit (what quartz cannot read)

The audit rides ingest, so these belong to the **sync process** —
`QUARTZ_LOG_LEVEL` alone is read by both processes.

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
| `PARSE_AUDIT_LOCAL_DIR` | compose only: a **host** directory to mount so the report can be read. Without it the file is written inside the container, which is the one place it is no use | unset |
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
