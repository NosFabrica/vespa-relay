# The router: mirror from upstream relays

The router is its own process — `vespa-sync`, the `:sync` module — writing
into the same Vespa store the relay serves. The split is the point: restart it
with a new config, retune it, or lose it to an OOM and the relay never drops a
client, Vespa never replays a transaction log, and the id snapshots a
reconcile holds live in a heap the serving side does not share.

Point `SYNC_CONFIG` (or `SYNC_CONFIG_FILE`) at a strfry-style `streams`
config and the sync process keeps a live subscription open against each
upstream, mirroring matching events into the relay's store:

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
- **`sync`** — how the stream asks for what it is missing: `negentropy`, `fetch`,
  or `auto` (the default). This is a property of the **data**, not of the relay,
  and no measurement can infer it:

  | | use when | because |
  |---|---|---|
  | `negentropy` | the same event lives on many relays — profiles, relay lists, follow lists | reconciling id sets transfers only the difference; fetching re-sends everything the other relays already gave you |
  | `fetch` | each relay holds its own events and nobody else's, or the store is empty and there is nothing to compare against yet | comparing two sets that barely overlap costs more than downloading, and it builds a huge local id snapshot to do it. A sync band answers "what is new since we last asked" instead |
  | `auto` | you genuinely do not know | reconcile once **we** hold more than `SYNC_NEG_MIN_EVENTS` for the filter, otherwise page. A reconcile transfers the difference, so it pays when our set is already most of theirs and loses when we start from nothing — which our own store answers for free. Note it measures the WHOLE filter: on a mixed-kind stream a few large kinds can clear the floor while we hold none of the rest |

  A `fetch` stream never builds the local id set at all, which is the single most
  expensive thing the router does.

  NIP-85 assertions used to be the example of `fetch` here, and they are worth
  keeping as a caution: the answer depends on **how the stream asks**, not on the
  kind. Asked by kind alone, a provider relay serves every provider publishing on
  it and overlap with our store is poor. Asked per (relay, provider) — see
  [Binding filter fields to a relay](#binding-filter-fields-to-a-relay) — the two
  sides hold the same data and `negentropy` becomes the right answer. Narrowing
  the ask inverted the choice.
- **`trusted`** *(optional)* — skip signature verification for this upstream's
  events. Off by default; every mirrored event is verified and re-checked
  against the stream filter before it enters the store.
- **`deleteMissing`** *(optional, DELETES DATA)* — drop records this relay holds
  that the upstream no longer serves. Only for a stream whose upstream owns the
  records in the ask, and only with `sync = "negentropy"`. See
  [Deleting what an upstream retracted](#deleting-what-an-upstream-retracted).
- **`ownedKinds`** *(required by `deleteMissing`)* — which of the filter's kinds
  the upstream is the source of truth for, and therefore the only ones absence
  may delete. See [`ownedKinds`, and the cascade](#ownedkinds-and-the-cascade).
- **`authorsPerLeg`** *(optional)* — how many bound `authors` go into one ask, and
  therefore into one sync band. See
  [Binding filter fields to a relay](#binding-filter-fields-to-a-relay).

The router shares the relay's Vespa store, so mirrored events are immediately
searchable. It runs one outbound connection per upstream (reconnect and
re-subscribe are handled for you) and logs unreachable upstreams rather than
failing — a paused or down relay in the list is skipped, not fatal.

Clients still come first across the process boundary: the relay serves its
mean read latency on `GET /pressure`, and the sync process polls it
(`SYNC_PRESSURE_URL`) to yield ingest between batches when searches slow down —
the two share one Vespa, and a mirror batch's queries queue in the same engine
a client's REQ does. Leave the url unset to mirror at full speed; the boot log
says which regime you are in.

**Down** keeps a live subscription open and first negentropy-reconciles the
history its filter asks for. **Up** re-reconciles the store against the
upstream every `SYNC_UP_INTERVAL_SECONDS` and publishes only what the
upstream is missing — set reconciliation gives echo-suppression for free, so an
event just pulled *down* from a relay is never pushed back *up* to it.

## Resuming a paged relay

Negentropy relays need no band: reconciliation compares id sets and downloads
only the difference, so re-running a sync costs the diff and nothing more. Most
relays do not speak NIP-77 — in one measured run, seven of nine upstreams fell
back to paged REQ — and a paged fetch has no such memory. It walks `created_at`
newest-first and re-reads everything it read last time, every restart.

Set `SYNC_STATE_FILE` and the router remembers the band it has covered per
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

## Paging a negentropy sync

A NEG-OPEN is all-or-nothing at both ends of the wire, and neither end can see
the other's size. Ours: a reconcile needs our matching ids up front, so a
whole-filter pass materialises the stream's entire id set — 14.9M ids for one
stream here, and enough that concurrent streams are serialised behind a
semaphore to keep them from summing on the heap. Theirs: past
`max_sync_events` a relay refuses the whole thing rather than answering part of
it.

So above `SYNC_NEG_PAGE_TARGET` local events (default 100,000) a static
backfill stops asking for the whole filter and sweeps it in windows instead.
The boundary is a `created_at` timestamp — the only axis a Nostr filter can be
cut on — but it is **decided by a count**, and two independent things may cut a
window:

1. **We are dense.** Our own count for the window, taken from the store before
   any round trip is spent. This is what bounds our snapshot: a window that
   passes the check is at most `target` ids, so peak memory becomes a property
   of the target rather than of the corpus.
2. **They are dense.** The relay refuses, or had to split the window itself to
   answer. Either way the window size shrinks — to their stated cap when they
   send one (strfry puts the number in the rejection: `blocked: query matches
   too many records (2431002 > 1000000)`), by halving when they do not. A clean
   window grows it back, so a sweep settles on the largest size that peer will
   take instead of a size an operator guessed.

Neither source knows anything about the other and the same work stack absorbs
both, which is what makes this automatic rather than tuned. What the relay
learns is remembered per peer in `SYNC_SWEEP_STATE_FILE`, so the next sync
starts at the right size instead of rediscovering it.

The split with quartz is worth knowing when reading the logs: quartz owns
everything inside one reconcile — sub-splitting a window it cannot answer,
bounding what it reads from the store, and draining a second no window size
will fit — and reports the peer's stated cap back. This router owns what has to
survive that call: the cursor, the learned size, and the order windows are
walked in.

Three details that matter more than they look:

- **The filter shape stays byte-identical across windows** — only `since`/`until`
  vary. strfry matches a declared negentropy tree by comparing canonicalised
  filter JSON, so a stable shape rides their index for the whole sweep while
  sub-partitioning on any other axis drops the window onto the capped snapshot
  path. That is why splitting a window by kind is an escape hatch below and not
  a strategy.
- **Bisection bottoms out at one second.** `created_at` has second granularity
  and is author-controlled, so a single second holding more than a relay's cap
  is reachable and cannot be cut further on the time axis. When one shows up it
  is handed back mid-reconcile: the sweep retries that second per kind
  (accepting the tree miss) and pages it over plain REQ if that still does not
  fit, while everything around it in the same window goes on reconciling. Rare,
  and it costs that second's guarantee rather than the window's.
- **Windows are walked newest-first and checkpointed one at a time.** The
  finished region is therefore always one contiguous slice growing downward
  from the top of the range, which is what lets the cursor be a single
  timestamp. A killed sweep resumes at the window it reached; without that, a
  crash at 80% of a multi-day sync costs the whole sync.

The top of the range is deliberately left alone: a sweep stops
`SYNC_NEG_PAGE_SLACK_SECONDS` below `now`, because a window still receiving
events cannot be checkpointed honestly. The live subscription covers the head.

Set `SYNC_NEG_PAGE_TARGET=0` to turn all of this off and go back to one shared
snapshot per stream — which is also what happens on any stream small enough
that a single window would hold it, where sharing one id walk across the group
is strictly cheaper.

## Dynamic relay lists: the outbox, and everything else that names a relay

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
| `relay` | which element holds the url. `1` for nearly everything; `2` for NIP-85 service tags and for `e`/`p`/`a`/`q` hints, which put an id or pubkey first. `index` is the older name for the same slot and still works |
| `authors`, `ids`, `kinds`, `#p`, `#e`, … | **narrow what this relay is asked for**, reading the value out of the *same tag* that named the url. The value is a tag element number, or `"pubkey"` / `"id"` for the scanned event's own — see below |
| `where` | conditions on the rest of the tag, shaped like NIP-01 filters: entries in the list **OR** together, the fields inside one entry **AND**. Each entry states any of `index` + `equals` (the element at that position is exactly that string — case-sensitive and untrimmed, and a missing element matches nothing, not even `""`), `minSize`, and `maxSize` (bounds on the tag's length). Omit to keep every tag |
| `marker` | sugar for NIP-65's rule: `write` / `read` expand to the `where` that keeps that side *plus* unmarked tags — with the url at 1, `[ { index = 2, equals = "write" }, { index = 2, equals = "" }, { maxSize = 2 } ]`, the slots following the select's own `index` — and `any` to no conditions. A select states `marker` or `where`, not both |

The scan's `filter` is an ordinary NIP-01 filter — `kinds`, `authors`, `since`,
`until`, `limit`, `#t`-style tag filters — so you can narrow it however you like:
`{ "kinds": [1], "authors": [...] }` harvests hints from your WoT's notes only.

## Mirroring the deletions themselves

Kinds **5** (NIP-09 deletion request) and **62** (NIP-62 request to vanish) are
ordinary events, so a `down` stream mirrors them by naming them in its filter —
the shipped `contentViaOutbox` stream does, alongside the content they retract:

```hocon
filter = { "kinds": [0, 1, 5, 9, 11, ..., 62, ...] }
```

Nothing else is needed, because the **store** enforces both at insert: a
mirrored kind 5 erases the targets it names (same author, time-guarded, by id
and by address), and a kind 62 sweeps that author's history off this relay. An
author who deleted a note on their own write relay therefore does not keep a
copy here.

Storing the request is the half that lasts. A mirror re-asks its filter every
cycle, so the upstream will hand the note back — and it is the stored tombstone
that the re-download is checked against and rejected by. This is exactly what
absence-based `deleteMissing` below cannot do: it frees space and leaves nothing
behind, so the next walk re-downloads what it dropped.

Two consequences worth knowing before you add them to a running stream:

- **Adding kinds re-keys the sync bands.** They are keyed by the whole filter,
  so the first cycle after the edit re-walks every relay in the fan-out from
  scratch instead of resuming. That is the intended way to force a re-walk, but
  on a wide dynamic stream it is a long cycle.
- **Both processes check every insert, and it is stated in code, not configured.**
  The store's fast path keeps a per-instance set of the authors known to have a
  stored kind 5/62 and skips the guard queries for everyone else — exact for one
  writer, wrong for this deployment's two: tombstones the *sync* process mirrors
  never reach the *relay* process's copy of that set, and deletions clients
  publish to the relay never reach the router's, so each would re-admit what the
  other erased. Both entrypoints therefore open the store with
  `writers = SHARED_STRICT` (`STORE_WRITERS` in `:common`, where the reasoning
  lives), which is the store's own default and the only mode that needs no
  assertion to be correct. `SHARED` would bound the exposure to a refresh
  interval rather than remove it, and its rebuild is a corpus-wide visit — hours
  on ours — so the window would be set by the rebuild, not by the interval. The
  measured cost of being strict is −4.5% on per-event inserts with p50 unchanged
  and nothing measurable on `batchInsert`; the router's share shows up as the
  `guards` stage on the `router: ingest …` line.

## Deleting what an upstream retracted

`deleteMissing` makes a stream drop records **we** hold that the upstream no
longer serves:

```hocon
sync = "negentropy"
deleteMissing = "dryRun"        # false (default) | "dryRun" | true
ownedKinds = [30382]            # required whenever deleteMissing is on
```

Only correct when that upstream is the *source of truth* for the records in the
ask — a NIP-85 provider's own relay for its own scores. For a general mirror,
"this relay does not have it" means nothing at all: relays hold different subsets
by design.

It is **absence-based**, not NIP-09. Nothing is published upstream to learn it —
quartz's `NegentropyStoreSync` can propagate real kind:5 retractions instead, and
that is strictly safer, but arming it means uploading your events to someone
else's relay and reading the rejections. Pulling the retractions down — the
section above — is safer still and costs nothing but two kinds in the filter;
what it cannot cover is a record withdrawn *without* a kind 5, which is the case
`deleteMissing` exists for.

The cost of that choice is that absence has innocent causes — a retention window,
a relay gating reads behind AUTH, a half-served reconcile — and each looks exactly
like "they retracted everything". So:

| guard | behaviour |
|---|---|
| `sync` must be `negentropy` | refused at parse time on `fetch`/`auto`. A paged fetch asks only *outside* its sync band, so "not seen" there means "not asked for", and deleting on it would take the entire history below the band |
| the reconcile must have **completed** | quartz never silently falls back — it throws when a window cannot be reconciled over NIP-77, including "this relay does not speak it". A normal return therefore means every window was compared end to end. On a throw the ask is paged instead (so the mirror still fills) and nothing is deleted |
| the reconcile must have covered ≥1 window | zero windows compared zero range |
| local ids | read from the *ask itself*, never the cycle's shared snapshot — quartz's own warning is that entries outside the filter come back as false "have" ids, and the shared snapshot spans every service on the stream |
| deletes | issued by id, inside the ask, so they cannot reach past what the reconcile compared |
| only `ownedKinds` | see below — the rest of the filter is mirrored from the same relay and never judged by its absence there |
| the author's **sole** upstream | an author this cycle found at more than one relay is mirrored and never deleted for: one relay's silence does not retract what a sibling may still serve. Measured, 3 of 266 services are bound to several relays, and two of those name general relays that will never carry their scores |

**There is deliberately no size guard.** An earlier version refused when a relay
served nothing, and again when a cycle would drop more than half an ask. Both
fired constantly, and both protected the wrong thing: they protect *stored
records* from a bad answer, when what needs protecting is a *reader* from a stale
score. A provider retracts a subject when that subject turns out to be a
scammer — precisely the score that must not survive — and a mass retraction is
exactly when the whole set goes. A volume guard blocks the case that matters most
while the harmless ones sail through.

The consequence is accepted, not overlooked: if a 10040 names a relay that never
carried those scores, the relay reconciles empty and we drop them. That is a
misconfigured provider list costing a re-download, weighed against serving a
retracted score forever. The completed reconcile is what makes "empty"
trustworthy enough to act on.

### `ownedKinds`, and the cascade

A stream's filter usually holds more than the upstream is authoritative for.
`assertions` mirrors kinds 0, 10002 and 30382 from each provider's own relay —
but the provider owns only its **scores**. NIP-85 says a service should publish
a kind 0 and 10002 for its key; measured on 12 (service, relay) pairs, not one
provider relay actually serves them. They reach us from the indexers instead.
Judged by absence, every healthy provider on the stream would lose its profile.

So deletion is licensed per kind, and saying so is mandatory:

```hocon
sync = "negentropy"
deleteMissing = "dryRun"
ownedKinds = [30382]            # required — the parse fails without it
filter = { "kinds": [0, 10002, 30382] }
```

`ownedKinds` is refused when it names a kind the filter never asks for, refused
on a filter with no `kinds` at all (the protected set would be open-ended), and
refused on a stream that does not delete — a licence sitting unused is a trap
for whoever turns deletion on later. Everything in the filter outside it is
**attached**: fetched from the same relay by the ordinary paged path, never
deleted for being missing there.

Attached records do get deleted, in one case. When a service's *entire* owned
set is retracted — we held scores, its relay now serves none of them, and it
offers nothing in their place — the attached kinds go with them. A service key
exists to sign scores; once every score is withdrawn, its kind 0 and 10002
describe a provider that provides nothing, kept alive in search by nothing but
our own copy. They are meant to go together.

The distinction that makes this safe is `needIds`. An addressable score a
provider *replaces* arrives as its old id retracted and a new id offered — the
same "we hold ids it doesn't" shape as a withdrawal. Only an empty `needIds`
separates "this provider published a fresh score" from "this provider is gone".
A service we never held scores for retracts nothing, whatever its relay serves
today, so the cascade needs a non-empty local set too.

Deletions are counted separately on the health line — it is the only number the
router prints that goes down.

## Binding filter fields to a relay

Without a binding, every relay a source names is asked for the stream's whole
filter. That is right for a relay list and wrong for a *provider* list: asking
`{ "kinds": [30382] }` of a NIP-85 relay pulls the scores of every service
publishing there, not the ones your 10040s name. Measured on a real store, 757
services, of which 587 were named by no stored 10040 — 33.8M cards that rank
nothing and nobody reads.

A binding reads a second slot out of the **same tag occurrence**, so the pair
stays together:

```hocon
select = [
  { tag = "30382:rank",      relay = 2, authors = 1 }   # ["30382:rank", service, relay]
  { tag = "30382:followers", relay = 2, authors = 1 }
]
```

Reading the two into separate lists instead would give the cross product — every
relay asked for every service, ~96% of those asks empty on the store above. The
tag is the unit, not the value.

A value may also come from outside the tag. `"pubkey"` is the scanned event's own
author, which is what makes NIP-65's outbox model expressible — *fetch this
author's events from the relays their own 10002 marks write*:

```hocon
{ kind = 10002, tag = "r", marker = "write", relay = 1, authors = "pubkey" }
```

Two things to know before using one:

- **A bound select cannot use the store's tag projection.** That projection
  answers with the distinct values at one index — a set, with the tag each came
  from already discarded, which is exactly the pairing a binding exists to keep.
  So a bound select pages the events instead. Scanning kind 10040 is nothing;
  scanning millions of kind-10002s is the walk the projection was introduced to
  replace. Narrow the small sources first.
- **`authorsPerLeg` decides how often a sync band survives.** A band is keyed
  on its filter, so a changing author set invalidates it and re-walks that
  relay's history. `authorsPerLeg = 1` gives one band per (relay, author), which
  never invalidates — a new provider list adds a band instead. Leave it out and
  all of a relay's authors go in one ask, which is the only workable choice when
  the fan-out is millions of authors wide.

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

## Enabling it under docker compose

The router is the `sync` service, behind the `sync` profile — the profile is
the on-switch. Copy the bundled example, then start with the mirror on:

```bash
cp router.conf.example router.conf   # then edit the relay list / filters
SYNC_CONFIG_LOCAL=./router.conf docker compose --profile sync up -d --build
```

Plain `docker compose up` serves without mirroring. Edited `router.conf`?
Restart only the mirror — the relay keeps serving, and the sync cursors make
the re-run cost a diff, not a corpus:

```bash
docker compose --profile sync restart sync
```

Setting `SYNC_CONFIG` / `SYNC_CONFIG_FILE` on the **relay** fails its boot
deliberately: it once meant "run the mirror in-process", and a config that is
read, accepted and does nothing is how a mirror quietly stops mirroring.

## Syncing with .onion relays

The same profile starts a `tor` service — a client-only Tor whose SOCKS port
is reachable from the compose network and published nowhere. The router dials
hidden services through it and everything else directly, chosen per url:

```hocon
streams {
  hidden {
    dir    = "down"
    filter = { "kinds": [1] }
    urls   = [ "ws://somerelayaddress…xyz.onion" ]
  }
}
```

`ws://`, not `wss://` — Tor already authenticates and encrypts to the service,
and a hidden service rarely carries a CA certificate for its own name. The url
normalizer knows this and leaves a bare `.onion` on `ws://`, so a relay list
that names one needs no special handling.

Nothing else changes: bands, `deleteMissing`, `relaySource` discovery and the
NIP-66 monitor all work the same. Three things behave differently, on purpose.

**The name never leaves this box.** OkHttp hands the hostname to the proxy
instead of resolving it, so `.onion` resolution happens inside Tor. That is
both the only way a hidden service resolves and what stops the local resolver
from learning which ones you sync with.

**A `.onion` with no `SYNC_TOR_SOCKS` refuses to boot** — it names the urls and
the setting. Discovered ones are dropped instead, and counted in the log: a
relay list is not something you typed, so the fix is a setting rather than an
edit.

**No verdict of ours is published about anything reached through Tor.** The
router synthesises two — a host struck out for silence, and "unreachable" from
a failed transfer — and both are suppressed for a proxied relay, because
silence arriving through three relays and a rendezvous is as likely to be our
circuit as their server. Under `SYNC_TOR_ALL` that covers every relay: the
weakness is the transport, not the address.

What still gets published is quartz's own connection-level observation: a dial
that fails is recorded as unreachable whatever the transport. That is the
reason the router probes its **own** SOCKS port before dialling anything it
routes through Tor — a proxy that is down or restarting would otherwise turn a
whole fan-out into signed claims about other people's servers. Those relays are
skipped instead, counted on the cycle line. The answer is cached for 30s rather
than taken once per cycle, so a Tor container that restarts is picked up inside
the running cycle instead of at the next refresh, six hours later.

Onion relays are slow to dial — seconds, not milliseconds — so give them their
own stream first, and measure it alone:

```bash
SYNC_STREAMS=hidden SYNC_WIRE_LOG=sent docker compose --profile sync up -d sync
docker compose logs tor --since 5m     # "Bootstrapped 100%" ⇒ dials can succeed
docker compose logs sync --since 5m
```

For a deployment where no relay should learn this box's address, `SYNC_TOR_ALL=true`
sends clearnet upstreams through Tor as well. Expect a fraction of the
throughput, and some large relays refuse exit traffic outright — it is a
different deployment, not a stronger setting.
