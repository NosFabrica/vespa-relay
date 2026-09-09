# Operator pages

The five diagnostic pages this deployment serves, in full. The README carries
the map — which page answers which question, and on which port; this is what
each one actually shows and why it shows it that way.

Each page is served by the process that does the work it describes, so a page
that will not load is itself an answer.

| page | port | asks |
|---|---|---|
| [`/stats.html`](#statshtml) | relay, `7777` | what does this store hold, and how is it filling? |
| [`/trust.html`](#trusthtml) | relay, `7777` | is ranked search working — and if not, which part is incomplete? |
| [`/observer_stats.html`](#observer_statshtml) | relay, `7777` | is the trust *sync* working? |
| [`/pulse.html`](#where-the-resources-go--pulsehtml) | own port, **off by default, admins only** | what does any of it *cost*? |
| [the sync and monitor pages](#the-mirrors-own-pages) | `7778` / `7779` | what is the mirror doing, and which relays may be dialled at all? |

## `/observer_stats.html`

Every kind-10040 observer, with its providers' kind-30382 score counts **here
and on the relay its own 10040 names, side by side**. That pairing is the whole
point: a local count alone reads as healthy until you learn the source holds 45×
more, and the gap is the trust sync failing quietly rather than loudly.

## `/stats.html`

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

## `/trust.html`

Whether ranked search is working on this deployment, and if not which part of
the trust projection is incomplete. **Public**, unlike the pulse: every field
here is a count, a phase, or a query *shape* — never a search term. Four panels,
in the order the question is actually asked:

- **Can people search** — provider lists resolving to a service that carries
  cells. An observer whose own providers resolve to an unprojected service gets
  an **empty** ranked page, and the page says so: the gate failing closed is
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

## Where the resources go — `/pulse.html`

On its own port (`PULSE_PORT`, 7780, on the relay; `SYNC_PULSE_PORT`, 7781, on
the mirror), **off by default** and **administrators only** — unless
`PULSE_PUBLIC` / `SYNC_PULSE_PUBLIC` opens the operational half to anyone. Both
services take the flag, and both refuse the boot if the matching
`*_CLIENT_DETAIL` switch is also on: what the page may say publicly is decided
at boot, not per request.

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

### Who may read it

The other pages are public because every field in them is a fact about stored
events. This one is not that document: with `PULSE_CLIENT_DETAIL` on it names
the heaviest observers and the search terms driving the load, and carries a
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

## The mirror's own pages

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

## Three services, one hostname

Each of the three binds its own port, but nothing requires three hostnames and
three certificates to read them. Every reference the pages make is
**document-relative** — the assets under `web/…`, and the `stats.json` each page
charts — so a service can be mounted behind a path prefix with a plain strip
rewrite and nothing else:

```nginx
location /sync/    { proxy_pass http://sync:7778/;    }
location /monitor/ { proxy_pass http://sync:7779/;    }
```

The **trailing slash matters on both sides**. `https://host/sync/` has `/sync/`
as its base directory and every asset is asked for under it; `https://host/sync`
has the ROOT as its base, and the page then asks the *relay* for its modules —
which the relay answers, 200, with its own copy of the same file names. Redirect
the bare prefix to the slashed one, the way ingresses normally do.

The search UI is the exception and is root-only: it is a single-page app whose
history writes are anchored at `/` by construction, so a prefix would survive
the first load and be lost by the first navigation.
