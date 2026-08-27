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
- **`negentropySyncThePastSeconds`** *(optional, was `auditSeconds`)* — how
  often to reconcile the covered past over NIP-77, against the relays a verdict
  says can answer one. See
  [`refetchThePastSeconds` and the reconcile](#refetchthepastseconds-and-the-audit).
- **`refetchThePastSeconds`** *(optional)* — how often this stream's bands
  expire, putting its whole filter back on the walk — the same job for the
  relays that cannot reconcile. Defaults to
  `SYNC_REFETCH_THE_PAST_SECONDS` (7 days). See
  [`refetchThePastSeconds` and the audit](#refetchthepastseconds-and-the-audit).
- **`ownedKinds`** *(required by `deleteMissing`)* — which of the filter's kinds
  the upstream is the source of truth for, and therefore the only ones absence
  may delete. See
  [`ownedKinds`](#ownedkinds).
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
stream, per filter and per relay — the three levels the file nests under — then
asks only for what lies outside it:

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

### `refetchThePastSeconds` and the audit

A band narrows work; it never expires on its own evidence. So it can carry a
period: once a band is older than the stream's `refetchThePastSeconds`, it is
discarded and the whole filter is walked again from the plausible floor. That is the only thing that can re-read a
window a relay back-filled after we passed it, and for a stream with no
`negentropySyncThePastSeconds` it is the only full re-check there is.

**Which of the two a relay gets is the monitor's verdict, not a guess.** The
fitness pass probes NEG-OPEN and signs the answer onto the same kind-30166
record the roster admits the relay by. A relay measured as refusing one is never
asked for a reconcile — the attempt cannot succeed, and a failed audit advances
no clock, so it was retried every six hours per ask forever; those are counted
as `auditsSkipped` on `/stats.json`. A relay nobody has measured is not a
refusal: the ask tries and finds out. So a stream whose relays are mixed wants
both periods — the reconcile short, the re-fetch long — and a stream that sets
only the reconcile leaves its non-NIP-77 relays' history never re-read, which is
what the skipped count is there to make visible.

**There is no default, and no environment knob either — deliberately.** A
stream that does not name a period never re-fetches its past. Re-reading a whole
history is the most expensive thing this router does on a schedule, and it was
running on quartz's week in every deployment that had never heard of the knob;
one number across every stream could only ever be wrong for most of them, since
a 130-kind content mirror and a five-relay bootstrap do not want the same
period. `SYNC_REFETCH_THE_PAST_SECONDS` and the two names before it are refused
at boot rather than ignored, with a message naming the replacement. Streams left
with neither a re-fetch period nor a reconcile are named at boot:

```
router: stream(s) indexers have neither `negentropySyncThePastSeconds` nor `refetchThePastSeconds` — they page
forward only, and nothing will re-read the history they have already walked. Set one if a relay
of theirs can back-fill
```

Where an audit does run, the two are the same job at different prices: the audit
reconciles the covered history and downloads the difference; the re-walk
downloads the history. They also collide, because a visit runs its catch-up
*before* its audit — leave both at a week and the stream re-pages everything and
then reconciles the same ground in one visit. Give a stream that audits a period
well above its `negentropySyncThePastSeconds`; the example config runs the two outbox streams
monthly against a weekly audit. Below its own `negentropySyncThePastSeconds` the loader says so
at boot, because there the audit can never be the cheaper path.

The **live tail works against every relay**; the **reconcile depends on the
upstream**. Some relays advertise NIP-77 but their reconciliation never
converges. One bound handles that: a session with no protocol frames for 30
seconds aborts itself, and the upstream leans on its live tail and its paged
catch-up while relays that reconcile cleanly compare in full. There is
deliberately no wall-clock deadline — every timeout is measured from the last
message, so a relay that stops answering is already gone, and one still sending
is doing the work we asked for.

**One engine walks every relay.** A stream naming `urls` and a stream
discovering them from the store differ only in where the list comes from; after
that both are the pool's, and a relay from either is visited the same way — a
catch-up that pages forward from the band's edge, the reconcile of the past
where it is due, then a live tail on the socket the visit already holds. The
`urls` half used to run a separate backfill that walked each relay once per
process and then live-tailed, which is why it could not re-check its own past on
any clock; its `sync` knob (and `SYNC_NEG_MIN_EVENTS`, which sized its `auto`
mode) are refused at parse time now rather than accepted and ignored. What the
pool is doing is in `/stats.json` — the streams' phases, their in-flight rows
split by `pool`, and the `live` list of held tails — rather than in a boot-time
ETA line.

## Paging a negentropy sync

A NEG-OPEN is all-or-nothing at both ends of the wire, and neither end can see
the other's size. Ours: a reconcile needs our matching ids up front, so a
whole-filter pass materialises the stream's entire id set — 14.9M ids for one
stream here, and enough that concurrent streams are serialised behind a
semaphore to keep them from summing on the heap. Theirs: past
`max_sync_events` a relay refuses the whole thing rather than answering part of
it.

So above `SYNC_NEG_PAGE_TARGET` local events (default 100,000) a reconcile
stops asking for the whole filter and sweeps it in windows instead.
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

Every scan's urls are unioned into the stream's roster, and the pool visits
**all** of them — so the fan-out widens on its own as the store fills. The
sources are re-read on their own `refreshSeconds`; the visiting never stops.

### The pool is a rotation, not a batch

There is no pass over a relay list. Every relay a stream names — declared in
`urls` or discovered by a scan — goes into one roster, and the pool visits them
continuously: `visitConcurrency` bounds how many are being dialled at once, each
visit is a catch-up over that relay's outstanding legs followed by the reconcile
of its past where one is due, and the socket it already holds becomes a live
tail if the stream has budget for one (`maxLiveConcurrency`). A relay's next visit is
paced by what it has been yielding lately, not by a shared clock, so a relay
with a real backlog is not holding anything else up.

**The unit of work is a (relay, stream) PAIR.** Many streams may work one relay
at the same time — they share its socket, since `RelaySockets` refcounts claims
— while each stream sees that relay in exactly one state: catching up,
re-fetching, or negentropy-auditing. A live tail is held *across* those states
rather than being a fourth alternative to them, so a stream keeps the live edge
through a multi-minute audit.

That split is the correctness boundary as much as a scheduling one. Bands key
on `(stream, url, filter)`, so two streams on one relay touch disjoint state and
need no lock, while two jobs of the *same* stream on one relay would write the
same band. It also means each stream gets its own revisit clock per relay
instead of a fast stream and a slow one sharing one, and that `tails` /
`maxLiveConcurrency` are exact: one subscription serves one stream, where it
used to carry every wanting stream's filter and be charged to all of them.

Counted in units: `rosterVisits` is the roster in pairs, and it — not `roster` —
is what `visiting` and `awaitingVisit` are parts of.

Two consequences worth knowing:

- **A relay's whole outstanding history is one worker's job, per stream.** `bands.legs()`
  hands that worker every region outside the relay's band — the newer leg above
  it and the older leg below — and it walks all of them before releasing the
  slot. A relay with ten hours of history to pull is one worker running for ten
  hours, and every other relay on the stream is unaffected.
- **What each relay is doing is per relay.** The stream row carries its phase,
  the size of its roster and how many tails it holds; which relay is where is
  the in-flight list under it (`doing`, `heldForSec`, `events`, `quietForSec`,
  and the cursor). A stream-wide percentage would be an average over workers
  doing unrelated things.
- **…and which of four jobs it is doing is `pool`.** One rotating pool runs all
  of them, so every count over it added them together: `visiting` covered a
  catch-up, a history audit and a whole-corpus re-walk alike, while `tails`
  counted the fourth and named nobody. Each held row now carries a stable word
  beside its `doing` sentence — `catching-up` (paging forward from the band's
  edge), `re-fetching` (paging over history the band already covers, because
  `refetchThePastSeconds` expired it), `auditing` (reconciling that history over
  negentropy), `live` (a held tail) — and the status page draws one table per
  word. The live pool is the document's own `live` member at the root rather
  than a per-stream list, because a tail carries every wanting stream's filter
  and counts its arrivals at the url. A row in none of the four (claiming a
  socket, draining the healer) carries no `pool` and is drawn under its own
  `doing`.

### Bounding what each stream costs

`visitConcurrency` is a **dial width** — how many relays are visited at once —
and it says nothing about what those visits are doing. The four jobs behind it
do not cost the same: a catch-up page is parse and ingest, a negentropy audit
builds and compares id sets per window and is the one genuinely CPU-bound job
here, and a re-fetch is a catch-up over history already held. One number for
all four can only be set for the worst of them.

So every budget is **per stream**, including the two that used to be
router-wide:

```hocon
streams {
  content {
    visitConcurrency     = 96   # relays visited for this stream at once
    maxLiveConcurrency   = 500  # live subscriptions it may keep open
    refetchConcurrency   = 4    # …and what its visits may be doing
    negentropyConcurrency = 4
  }
  indexers {
    visitConcurrency   = 8
    maxLiveConcurrency = 8
  }
}
```

There is deliberately no router-wide version of any of them, and the old
top-level `visitConcurrency` / `tailBudget` are **refused at parse time**
rather than ignored — accepted and dropped, they would leave a deployment
believing it had bounded its dials. What every stream may take between them is
the sum of what each may take, written where the stream that pays it is
configured; a second ceiling over the top would be a number to keep in step
with the shares by hand, and the failure it causes — a stream inside its own
share, refused anyway, by a limit named nowhere near it — is the one these
exist to make legible.

Two consequences of the sums:

- **The pool's worker count is derived**, as the sum of the streams' dial
  widths (a stream naming none contributes the 128 the router-wide setting
  defaulted to). Fewer workers would leave a configured share unreachable; more
  could never get a permit.
- **The socket ceiling is a sum now, so the router says it.** Dial widths plus
  live budgets plus `monitor.dialConcurrency` and the static upstreams share
  OkHttp's ~1,024 dispatcher limit. At boot the pool adds up the first two and
  prints a line if they crowd it — printed rather than refused, since a tuned
  dispatcher is a legitimate deployment. The sum is an **upper bound**: a tail
  is one subscription carrying every wanting stream's filter, so a relay two
  streams both want is one socket charged to both budgets.

`visitConcurrency` is admission like the rest: a visit that gets no permit for
any stream wanting the relay **never dials**, which is what makes it a bound on
simultaneous TLS handshakes and not merely on work. Unlike the tail's budget it
admits partially — the asks are served in turn over one connection, so a visit
that can serve one stream and not another is still a useful visit.

Unset means **uncapped**, not zero — a deployment that has never heard of these
behaves exactly as it did, bounded by `visitConcurrency` alone. A configured `0`
is floored to 1, for the same reason the pool's socket numbers are: zero is an
off switch wearing a tuning knob's name.

**A cap is admission, not a queue.** A visit that cannot get a permit skips
that job and carries on; the work stays due and the next visit takes it. Every
job here is due-gated and idempotent, so a full cap costs a revisit delay and
nothing else. Waiting would be worse: a visit holds a socket and one of
`visitConcurrency`'s slots for its whole life, so blocking on a permit would
idle both. Each skip is counted — `deferred`, per stream and job, in
`/stats.json` and on the status page — because a cap that silently drops work
is indistinguishable from work that was never due.

### Certifying the past is only re-read on schedule

Two jobs re-read history on a clock: the negentropy audit
(`negentropySyncThePastSeconds`) and the re-fetch (`refetchThePastSeconds`).
`auditsRun` says work happened; it cannot say the work was **due**. That is
what each stream's `schedule` rows carry, over every ask it has:

| member | means |
|---|---|
| `everySec` | the stream's configured period for that job |
| `due` | clock run out, waiting for a visit to pick it up |
| `neverRun` | no completed pass behind it — due *by definition* |
| `waiting` | inside the period, nothing to do |
| `nextInSec` | until the nearest waiting ask comes due |

An ask leaves `waiting` by its clock running out and by nothing else, so a
`waiting` that drains at the period is the schedule working, and one that holds
steady while the audit counters climb is a rule being broken.

**`neverRun` is the honest exception.** An ask with no completed pass is always
due — `SyncBands.auditDue` short-circuits on a zero clock, so a relay's first
audit happens on its first visit rather than a period later. That is deliberate,
and it is also why a fresh deployment audits everything at once. It is counted
apart from `due` so that storm reads as what it is: scheduled work whose
schedule has not started yet. It is also the case `negentropyConcurrency` exists to
bound — the storm is capped rather than avoided, and it never recurs for the
same ask. Each audit's own log line names the clock it ran on
(`last verified 812043s ago`, or `never`).

### Re-deriving the relay list

Deriving a discovered relay list is the only expensive thing between a stream
waking up and the first downloaded byte: the scans above, a normalisation pass
over every url they carry, an alias `apply` (one `#d` query per 500 urls), then
the `exclude` filter. On a full store that is minutes of work to produce a list
that differs from the last one by a handful of urls, so it is cached per SOURCE
on that source's own `refreshSeconds` — a 30166 verdict read is one indexed
query and can run every two minutes, while a corpus scan for relay hints cannot.
The list is also rebuilt as soon as the alias monitor publishes a fold verdict,
since a list built before it would go on dialling urls now known to be one relay.

Nothing about *dialling* is cached. The NIP-66 known-dead set is re-read on every
rebuild and host strikes do not persist, so a relay that died an hour ago is
skipped on a reused list exactly as it would be on a fresh one.

Nothing truncates that set: no cap on relays synced and no popularity floor.
`concurrency` paces the fan-out, it doesn't bound it, and `exclude` is the only
way to leave a relay out. A plain url entry excludes exactly the relay it
names, compared by normalized form — host case, a redundant `:443`, or a
missing trailing slash don't matter. An entry carrying a regex metacharacter (a dot
doesn't count — urls are full of them) is instead a regex that must match the
whole discovered url, ignoring case: `wss://filter.nostr.wine/npub.*` drops
every per-user url that host mints (`wss://filter.nostr.wine/npub1…`) while
leaving the relay itself in — a shape no literal list could keep up with.

**No kind needs its own code.** Every relay list in the protocol is a tag with a
url at a fixed offset, so a select is just that shape:

| field | meaning |
|---|---|
| `kind` | apply this select only to that kind; **omit to apply it to everything the filter collected**. A kind the scan never returns simply never matches |
| `tag` | the tag name to read; **omit for any tag** — that's how you take a whole family like NIP-85's `<kind>:<type>` service tags without naming each one |
| `relay` | which element holds the url. `1` for nearly everything; `2` for NIP-85 service tags, for `e`/`p`/`a`/`q` hints, which put an id or pubkey first, and for NIP-51's `group` tags, which put the group id first. `index` is the older name for the same slot and still works |
| `authors`, `ids`, `kinds`, `#p`, `#e`, … | **narrow what this relay is asked for**, reading the value out of the *same tag* that named the url. The value is a tag element number, or `"pubkey"` / `"id"` for the scanned event's own — see below |
| `where` | conditions on the rest of the tag, shaped like NIP-01 filters: entries in the list **OR** together, the fields inside one entry **AND**. Each entry states any of `index` + `equals` (the element at that position is exactly that string — case-sensitive and untrimmed, and a missing element matches nothing, not even `""`), `minSize`, and `maxSize` (bounds on the tag's length). Omit to keep every tag |
| `marker` | sugar for NIP-65's rule: `write` / `read` expand to the `where` that keeps that side *plus* unmarked tags — with the url at 1, `[ { index = 2, equals = "write" }, { index = 2, equals = "" }, { maxSize = 2 } ]`, the slots following the select's own `index` — and `any` to no conditions. A select states `marker` or `where`, not both |

The scan's `filter` is an ordinary NIP-01 filter — `kinds`, `authors`, `since`,
`until`, `limit`, `#t`-style tag filters — so you can narrow it however you like:
`{ "kinds": [1], "authors": [...] }` harvests hints from your WoT's notes only.

### NIP-29 group hosts

The example config uses a select whose only unusual part is `relay = 2`:

```hocon
{
  select = [ { kind = 10009, tag = "group", relay = 2 } ]
  filter = { "kinds": [10009] }
}
```

A NIP-51 simple group list writes `["group", <id>, <relay url>, <name?>]`, so the
url is at 2 and **element 1 is the group id**. Reading 1 does not fail loudly: the
ids go to the url normalizer, which rejects them one at a time and silently, and
the stream ends up with no relays, no error and nothing in the log to say why.

Two things about this source are worth knowing before you copy it:

- **It only sees the group lists you already mirror.** Kind 10009 has to be in
  some stream's `filter`, or the scan reads an empty set forever — which is the
  same silent nothing. The example puts it on the content stream beside 10003.
- **A group's own id is not global.** A group is the pair *(id, host relay)* and
  its posts carry only the bare id in an `h` tag, so a mirror holding two relays'
  `general` cannot tell their posts apart afterwards. That is a property of
  NIP-29 rather than of this router — the search UI says so where a reader can
  see it — but it is the reason to think before pointing a mirror at every group
  host on the network.

The select can also **bind** the group id, so each host is asked only for the
groups somebody listed:

```hocon
{ kind = 10009, tag = "group", relay = 2, "#h" = 1 }
```

That is one leg per *(relay, group)* rather than per relay, and each leg's band
stays valid because a group id does not change. The cost is the tag projection:
a bound select has to page whole events (see [Binding filter fields to a
relay](#binding-filter-fields-to-a-relay)), and it will not find a public group
that nobody has listed. The example leaves it unbound for that second reason —
a search relay wants the groups nobody has told it about.

Private groups are not mirrored either way. NIP-29 gates their reads behind
NIP-42, and the identity this router signs with is not a member of anybody's
group; the failure is membership, not reachability, so no amount of config
reaches them.

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
| only `ownedKinds` | see below — nothing outside them is ever judged by its absence, and a stream should ask for no more than it owns |
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

### `ownedKinds`

A NIP-85 provider owns its **scores** and nothing else. NIP-85 says a service
should also publish a kind 0 and 10002 for its key; measured on 12 (service,
relay) pairs, not one provider relay actually serves them. They reach us from
the profile streams instead. Judged by absence here, every healthy provider on
the stream would lose its profile.

So deletion is licensed per kind, and saying so is mandatory:

```hocon
sync = "negentropy"
deleteMissing = "dryRun"
ownedKinds = [30382]            # required — the parse fails without it
filter = { "kinds": [30382] }   # ask for exactly what the upstream owns
```

`ownedKinds` is refused when it names a kind the filter never asks for, refused
on a filter with no `kinds` at all (the protected set would be open-ended), and
refused on a stream that does not delete — a licence sitting unused is a trap
for whoever turns deletion on later. It stays a separate statement from the
filter even when the two coincide, as they do above: the filter is what to ask
for, `ownedKinds` is what absence may destroy, and a kind added to the filter
must never become deletable by having been added.

**Ask for exactly the owned kinds, and nothing else.** Two things follow from a
wider filter, both learned the hard way here. A kind the upstream never serves
returns no event, so it earns no band span, and an empty walk records no band —
its leg re-opens over the *whole past* on every visit, forever, for every
(relay, provider) pair. And the reconcile stamps its band against the filter it
compared (the owned kinds), so a filter wider than `ownedKinds` is a band the
audit can never narrow. Note that a band is keyed by the whole filter, so
editing it starts each walk over once.

**There used to be a cascade**, and it is gone. When a service's entire owned
set was retracted, its kind 0 and 10002 went with the scores — a service key
that signs nothing describes a provider kept alive in search by our own copy
alone. That copy was never ours to drop: no provider relay serves those kinds,
so every one we hold arrived through the profile streams, which mirror them
from relays that do serve them — and re-mirror them, over a live tail, right
after the cascade deleted them. It deleted another stream's records and did not
survive its own next walk. A provider whose scores are all retracted now simply
keeps its profile, like any other pubkey in the store.

The distinction that made the cascade safe is worth keeping in mind for the
ordinary delete too: an addressable score a provider *replaces* arrives as its
old id retracted and a new id offered — the same "we hold ids it doesn't" shape
as a withdrawal. `needIds` is what separates them.

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

- **the filter's `since`** bounds how far back this stream ever asks. Leave it
  unset and the catch-up walks each relay's whole history once, then only what
  is new; what re-checks the covered past after that is
  `negentropySyncThePastSeconds` and `refetchThePastSeconds`, on their own
  clocks.
- **`visitConcurrency`** (top level, not per stream) is the one dial on dialling
  cost. The union of every scan on a full store is a large set — plenty of it
  long-dead hosts that will each burn a connect timeout — and this decides how
  much of the network is dialled at once.
- **`dir` must be `down`.** A stream may carry both `urls` and `relaySource`:
  where a relay came from is the only difference between them.

The pool logs what it did, including why the unreachable relays were
unreachable (a relay list is full of dead hosts) and what the rejections were.
Expect rejections to *outnumber* accepts on a wide fan-out: a thousand relays asked for
the same replaceable profiles means the store discards nearly every copy as
already-held, which is the system working, not failing. The breakdown is there so
a bad signature or a failing store doesn't hide inside that number:

```
router: outbox syncing 3184 relay(s) from [kinds 10002/10050/30002/30166 x3 select(s), kinds 1 x1 select(s)] against 88412 local id(s) (top: wss://relay.damus.io/ x8214, ...)
router: outbox cycle done — 214,880 event(s) from 1102/3184 relay(s) in 1:12:41; unreachable: timeout x938, Connection refused x421; next in 21600s
router: ingested 214880 accepted, 402113 rejected [duplicate: already have this event x401980]; 14 relay(s) connected, 12 pinned + dynamic
```

## Publishing what this router mirrors

The kind list in `filter` exists here and nowhere else, and that is a problem for
anything counting events on the relay this router feeds. "How much of my history
is here yet" gets answered by counting our events for an author against the
author's own relay's total for them, and that is a *filtered* mirror over an
*unfiltered* one: measured at **31,118 here of 89,485 there — 35%** — on a mirror
that was missing nothing it had ever been asked to hold. The entire gap was kinds
3, 4, 5, 6, 7 and 1059, which no stream here asks for, and two of which
(encrypted DMs, gift wraps) it must never hold.

Set `SYNC_MANIFEST_FILE` and the router writes what it is running, once at boot:

```json
{
  "writtenAt": 1770000000,
  "streams": [
    {"name": "content",    "dir": "down", "kinds": [0, 1, 3, 6, 7, 16, 20, …]},
    {"name": "assertions", "dir": "down", "kinds": [30382]},
    {"name": "monitor",    "dir": "up",   "kinds": [30166]}
  ]
}
```

The relay reads it off the shared volume and publishes the union of the `down`
kinds as `sync.mirrors.kinds` on `/stats.json`. A client scopes its remote
`COUNT` to that list, and the percentage becomes one that can reach 100%.

Once at boot is the whole lifecycle: a `router.conf` edit is a `restart sync`, so
there is no other moment this can change, and `writtenAt` is how a reader tells a
live declaration from one left behind by a router that was switched off. What is
written is what is **running** — `SYNC_STREAMS` narrows the file too, because a
stream that is not running mirrors nothing.

## The router's own status page

The manifest says what the mirror *would* hold. Nothing said whether it was
working — that lived only in the log lines `StreamPhases` prints, i.e. in
whatever a container's stderr had not yet rotated away. Three questions had no
answer at all: is the router alive, did the last cycle finish or abort, and what
became of the urls it took on. A production fan-out reported **16,752 relays
discovered** against **5,323 carrying a band**, with no published account of the
other ~11,400.

This process serves the answers itself, at `SYNC_STATUS_PORT` (7778): the page
at `/` and the document behind it at `/stats.json`, rebuilt every
`SYNC_STATUS_INTERVAL_SECONDS`.

It used to be a FILE — `SYNC_PROGRESS_FILE` — written to a volume the serving
relay mounted, read back, re-parsed against an allowlist and re-narrated as two
cards on the relay's `/stats.html`. That cost about 2,500 lines on the relay's
side whose only job was to re-derive what this process already knew, and it
could not answer the first question in the list. **A file says nothing about
whether the process writing it still exists.** So the document carried a
`writtenAt` heartbeat and the relay turned it into a `staleForSec` with a
150-second threshold — and even with all that, a mirror that had been down for a
day published a card that could not be told from one mid-cycle without reading
the timestamp. A page served by the process it describes answers "is it running"
by answering at all, so the knob is refused at boot now and the heartbeat is
gone from the document.

The `progress` half of that document:

```json
{
  "streams": [
    {
      "name": "content",
      "phase": "fetching",
      "phaseForSec": 412,
      "inFlight": {"relays": [{"relay": "wss://slow.example/", "heldForSec": 41400,
                               "transferringForSec": 41390, "events": 2, "quietForSec": 41000}],
                   "omitted": 118},
      "cycle": {
        "startedAt": 1769999000, "outcome": "running",
        "urls":  {"discovered": 16752, "foldedOntoAnother": 11429, "excluded": 0, "taken": 5323},
        "hosts": 850,
        "taken": {"delivered": 2200, "nothingNew": 900, "unreachable": 800,
                  "transferFailed": 100, "noRoute": 1000, "hostStruckOut": 200,
                  "knownDead": 100, "torUnavailable": 0, "pending": 23},
        "foldedOnto": {"relays": [{"relay": "wss://nostr.oxtr.dev/", "urls": 55,
                                   "examples": ["wss://nostr.oxtr.dev/alpha"]}],
                       "omitted": 480},
        "balanced": true,
        "received": 481203
      }
    }
  ]
}
```

Three things are load-bearing here.

**`urls` and `taken` are a partition, not a tally.** `discovered =
foldedOntoAnother + excluded + taken`, and the ten outcomes under `taken` sum to
it exactly. `pending` is what closes the second identity *while the cycle runs*:
it is derived from the other eight rather than counted, so the numbers add up
mid-fan-out instead of only at the end. `balanced` is the router's own check on
them, published rather than asserted, and the relay recomputes it as
`accountedFor` when it draws — the two disagreeing localises the fault.

Two pairs in there are deliberately not one number, because each pair answers
"will it try again, and when" in opposite ways:

| outcome | dialled? | retried |
|---|---|---|
| `hostStruckOut` | no | **next cycle** — a strike is cycle-local and nothing about it persists |
| `knownDead` | no | when our own signed `dead` verdict ages past `StreamWorld.DEAD_TTL_SECONDS` (24h), or immediately if anything on its host delivers |
| `noRoute` | no — the TCP pre-probe was refused | next cycle |
| `torUnavailable` | no — *our* proxy was down | as soon as the SOCKS port answers, within the running cycle |
| `unreachable` | yes, and it never answered | next cycle; this is the only one published about |
| `transferFailed` | yes, then the transfer broke | next cycle; **never** published — the server was there |

They were one number called "skipped as dead", which is exactly as readable as
it sounds.

**`foldedOnto` names which urls folded, not only how many**, grouped by the
survivor that absorbed them — "which server is wearing forty urls" is the
question an operator can act on. Bounded to the biggest few with `omitted`
naming what was left out, because the full list runs to thousands of urls and
this document is fetched on every poll. The complete per-url verdict lives where
it was earned: a signed NIP-66 kind 30166 `same-as` record in this relay's own
store, queryable over the protocol. `excluded` is its own member beside it —
an operator's `exclude` list being obeyed and a duplicate the router worked out
for itself are different facts with different fixes.

**`inFlight` names the relays that are running**, which is the half the counts
never said. `pending` on a production stream read `2` while that stream had
received two events in eleven and a half hours, and nothing anywhere recorded
*which two*: the count is derived by subtraction, a leg that is still going has
earned no band so the coverage card cannot draw it, the `SYNC_DIAGNOSE` line
fires only for the one stream it names, and container logs here rotate inside
the hour. The router was holding both urls the whole time.

Four numbers per row, because a duration on its own is ambiguous — a relay with
a real backlog and a walk that cannot terminate are both "held for hours":

| member | says |
|---|---|
| `heldForSec` | since the rotation CLAIMED it — before the strike checks, the TCP pre-probe and the queue for a transfer slot, not just the download |
| `transferringForSec` | since it took a **transfer slot** — not since it went on a socket, because the connect happens inside the slot and a url that never connects still holds one while it tries (measured). **Absent means no slot**: in the guards, or queued behind other legs, which is where most of a fan-out's workers are. Absent with a large `heldForSec` says *our* pool is saturated |
| `events` | what that leg has received so far, counted as they arrive rather than when the leg ends — the leg worth watching is the one that has not ended |
| `quietForSec` | since the last one, or since the claim if none ever came. **The one that decides**: events still landing is a slot well spent, this climbing is a walk that is not going to end |

It sits beside the cycle rather than inside it because a worker outlives the pass
that handed it out: the same url is this cycle's `pending` if this pass dialled it
and its `busy` if an earlier one did. It is *not* "the pending urls" — `pending`
also counts urls the walk has not reached yet, which have no worker. Bounded to
the longest-held few, `omitted` naming the rest, on the same terms as `foldedOnto`.

**`hosts` sits beside the url counts, never instead of them.** Most relay
software answers on every path, so one server wears many urls and every url-keyed
number is inflated until the alias fold decides them (measured: 3,272 urls on 850
hosts). The gap between the two *is* the disclosure.

All of it is published as `sync.progress` on this service's own `/stats.json`,
beside a `sync.terms` glossary defining every number in the section — including
the three different things the word "done" used to cover: a fan-out leg that
*returned*, a walk that *settled*, and the span every kind has produced
*evidence* for. The glossary ships inside the document rather than in this file,
so a chip on the page can never describe a member in words the router would not
use.

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
