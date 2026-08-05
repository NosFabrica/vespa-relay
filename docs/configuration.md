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
| `LOG_CONNECTIONS` | log the live connection count on connect/disconnect | `false` |

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
| `SYNC_STATE_FILE` | where the per-(relay, filter) synced `created_at` band is kept. A relay without NIP-77 has no memory of what it already sent, so without this every restart re-downloads its whole corpus; with it the router asks only for what falls outside the band it already walked. Keyed by filter — edit a stream's filter and that stream starts over | unset ⇒ in memory only |
| `SYNC_INGEST_BATCH` / `SYNC_INGEST_CONCURRENCY` | mirrored events are drained in batches and written through the store's bulk path. The store serializes writes, so throughput comes from the batch size (a sweet spot near the default — much larger stalls on long mutex holds), not the worker count. Lower the batch to cut memory | `1000` / `2` |
| `SYNC_DYNAMIC_REFRESH_SECONDS` | default period between cycles of a `relaySource = [...]` stream (re-read the sources, re-sync every relay) | `21600` (6h) |
| `SYNC_DYNAMIC_CONCURRENCY` | default number of discovered relays synced at the same time | `8` |
| `SERVING_PRESSURE_THRESHOLD_MS` | mean client-read latency above which the mirror starts yielding to clients — the mean itself arrives over `SYNC_PRESSURE_URL`, measured by the relay. Reads against a 50M-event store run ~400ms healthy; ingest pauses between batches once the mean passes this | `2000` |
| `SYNC_WIRE_LOG` | what to log of the upstream conversation. Empty still logs `NOTICE`, `CLOSED` and failed sends — the relay's own account of why it stopped. `sent` adds every command sent; `full` adds every message received (one line per event) | *(errors only)* |
| `SYNC_NEG_MIN_EVENTS` | for `sync = "auto"` streams: reconcile once **we** hold at least this many events on the stream's filter, otherwise page. Only our own count decides — a reconcile transfers the difference, so it pays when our set is already most of theirs, and our store answers that for free. Asking the relay as well cost a NIP-45 COUNT per relay per cycle for a worse answer, since COUNT is optional and slow where implemented | `5000` |
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
