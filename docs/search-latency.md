# Search latency: where a 1–5 s search page spends its time

Measured against `search-staging.brainstorm.world` on 2026-09-03 (333M events,
149M kind 1), read-only, from a sandbox ~100 ms away. Every number here is a
reading taken on a date, not a constant; take scale from a fresh call.

## The complaint, reproduced on the wire

One anonymous socket, one REQ at a time, `observer:460c25…` unless noted:

| REQ | events | wall |
|---|---:|---:|
| `bitcoin`, kind 1, limit 20 | 34 | 2.7 s |
| `bitcoin`, kind 1, **limit 1** | 1 | 4.4 s |
| `bitcoin`, kind 1, **limit 200** | 255 | 4.0 s |
| `bitcoin sort:text include:spam` (no trust) | 35 | 2.3 s |
| `bitcoin sort:recent` (the match-phase profile) | 20 | 0.6 s |
| `bitcoin`, kind 1, `since` = 7 days ago | 21 | 1.2 s |
| `"bitcoin"` quoted | 37 | 2.6 s |
| `bitcoin lightning` (two words) | 20 | 1.8 s |
| `lightning` | 23 | 1.5 s |
| `zap` | 58 | 0.9 s |
| `xylophonist` (rare) | 2 | 0.3 s |
| `the` | 20 | **16.4 s** |
| `nostr` | 38 | **16.0 s** |
| `bitcoin`, kind 30023 (long-form, a small corpus) | 20 | 0.8 s |
| `bitcoin`, no kinds | 20 | 3.7 s |
| `jack`, kind 0 (the People tab) | 20 | 0.4 s |
| termless `include:spam`, kind 1, limit 20 (the feed) | 20 | 0.17 s |

Three things fall out:

- **The limit is irrelevant.** Limit 1 and limit 200 cost the same. So it is
  not the summary fetch, not the expansion's splice, not the transfer.
- **The cost is the size of the match set the rank profile has to score.** A
  rare word answers in 300 ms — that is the floor for everything that is not
  the match set. A word in a few percent of 149M notes costs seconds; `the`
  and `nostr` cost sixteen.
- **The only fast shape over a big match set is the one that stops looking.**
  `sort:recent` rides the `recency_gated` profile, whose Vespa match phase
  keeps the newest ~20k candidates and scores only those: 0.6 s for the same
  word that costs 4.4 s ranked.

The store already knew this. `vespa-eventstore`'s `benchmark/README.md`
("Where a common word's seconds go", 2026-09-01) ablated every clause family
of the production query on a 360k-event slice and found no droppable branch;
its fix was to let a relevance search use all four match threads
(`numthreadspersearch`, store PR #97, pinned here at `e1ecd7f23e`). That
roughly halved the cost and is deployed; the numbers above are what remains.

## The engine is shared, so stacking searches makes every one slower

Same word, N sockets each sending one REQ at the same instant:

| concurrent `bitcoin` searches | each answers in |
|---:|---:|
| 1 | 3.8 s |
| 3 | 5.0 s |
| 6 | 6.8 s |

A relevance search takes the cluster's four match threads; a second one on
the same box does not overlap it, it shares it.

## What the page did with that

`web/app.js` fired a ranked search per debounced keystroke (150 ms — under an
ordinary typing cadence, so nearly every keystroke), then another on Enter
(limit 40), then the pager's preload (limit 160). Emulating exactly that
sequence for `bitcoin` typed at 200 ms per key:

```
   512ms REQ  popup "b"(8)         2124ms EOSE  (1.6 s)
   713ms REQ  popup "bi"(8)        2125ms EOSE  (1.4 s)
   914ms REQ  popup "bit"(8)       4360ms EOSE  (3.4 s)
  1115ms REQ  popup "bitc"(8)      7644ms EOSE  (6.5 s)
  1316ms REQ  popup "bitco"(8)     7937ms EOSE  (6.6 s)
  1517ms REQ  popup "bitcoi"(8)    7809ms EOSE  (6.3 s)
  1718ms REQ  popup "bitcoin"(8)   9153ms EOSE  (7.4 s)
  2419ms REQ  full(40)             9131ms EOSE  (6.7 s)   <- the page draws here
  9131ms REQ  preload(160)        13504ms EOSE  (4.4 s)
```

Nine ranked searches for one typed word; first results at **9.1 s** on a relay
that answers the query alone in 3.7 s; the pager settles at 13.5 s. And every
one of the abandoned prefixes runs to completion in the engine — a CLOSE
cancels the relay's coroutine and the store's HTTP call, but Vespa finishes the
query it was given (the bundled query profile sets no timeout and disables the
soft timeout, on purpose, for the unbounded reads).

## What changed

**The page** (`web/app.js`, `web/paging.js`, `web/shared/asks.js`):

- **One type-ahead ask in flight at a time; the latest text runs next, through
  the debounce again.** While a popup search is in flight, keystrokes only
  replace the text waiting for it, and the waiting text runs once the box has
  held it for a pause — not the instant the previous ask lands, which (measured)
  searched `bitco` for four seconds while the reader had already finished
  typing `bitcoin`. The debounce is 250 ms, above an ordinary typing cadence,
  so a word typed without pausing fires once, for the whole word.
- **Enter reuses the popup's answer — or closes it.** The popup asks at the
  results view's first-ask width, and `shared/asks.js` keeps the last ranked
  ask keyed by everything but its limit, so the results view gets the same
  promise instead of a twin search. A type-ahead in flight for a *different*
  text is aborted (`CLOSE` to the relay, which cancels the read and frees the
  connection's lane) rather than left to finish for nobody. Only a complete
  answer is kept: a timed-out or aborted one is asked again.
- **The first ask is the preload's width.** Since the limit is free, "one page
  now, three more behind it" was two full searches for one answer; `firstAsk`
  is now `askLimit`, and `preload()` stands down until the reader turns past
  what it holds.

Net: **one ranked search per typed word** where there were up to nine. The
same emulation, against the same relay, after the change:

```
  1892ms REQ  popup "bitcoin"(160)      <- the one ask, after the last keystroke's pause
  2542ms Enter: the in-flight ask IS this text -> reused
  7488ms EOSE popup "bitcoin" 215 ev, 5598ms
  7489ms results view drawn from the reused ask
```

First results land when the one search lands. What remains — the 5.6 s
itself — is the engine's cost of the word, which no page can change and the
next two sections are about.

**The relay** (`server/SearchGate.kt`, `SEARCH_CONCURRENCY_PER_CONNECTION`):
a connection's ranked reads — terms, a phrase, a `sort:`, and the COUNTs of
either — run one at a time in arrival order; plain reads and lens-only filters
are never held; another connection is another lane. So no client, ours or
anybody's, can stack ranked reads on one socket and make every one of them
slow. The permit is held from the store call to EOSE, not to the end of a REQ
that parks at its live tail.

**The store** (`vespa-eventstore`, a patch prepared in this session and NOT
yet applied — `docs/proposals/vespa-eventstore-newest-n-cut.patch`, one
commit against the pinned `e1ecd7f23e`, its unit suite green, `git am`-able;
the relay takes it by bumping `vespaEventStore` in `gradle/libs.versions.toml`
once it lands): a relevance
search asks Vespa for a **match-phase cut** — `ranking.matchPhase.attribute=
created_at`, descending, `maxHits=VESPA_SEARCH_MATCH_PHASE_MAX_HITS` (default
10,000) — as query parameters, and the client accepts that one degradation on
those profiles. Under the threshold nothing changes and the page is
byte-identical; over it, the engine scores the newest N matches instead of
all of them, and the query costs what N costs rather than what the corpus
costs. A short cut page widens ten-fold once and then runs exact, so a pager
never ends early; a COUNT never carries the cut (a capped match phase caps
`totalCount`). The measurement is the section after next.

## The local reproduction, and what the cut costs and changes

Captured read-only from staging and fed through the store's own write path:
**1.31M events** — 1.19M kind 1 (the newest ~35 days), 100k kind 0, the 367
kind-10040 lists, and the provider `7d7ffd72…`'s kind-30382 cards for every
author in the slice (11.2k cards for 190k distinct authors: **only ~6% of the
notes' authors carry a score from the observer's provider at all**, which is
what the trust gate then deletes the rest by). The store's own `EventYql`
built every query (`searchTrace` printed them); `bench.mjs` in this session's
scratchpad re-sent each with `ranking.matchPhase.*` added, 5 reps, p50 of
Vespa's own `searchtime`, top-40 ids compared against the uncut page. Same
observer, `min_rank 2`, kind 1, limit 40, 4 match threads:

| word | uncut | served | cut 2,000 | 5,000 | 10,000 | 20,000 | 50,000 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `the` | 331 ms | 30.5k | 36 ms · 0/10 | 50 ms · 0/10 | 69 ms · 0/10 | 165 ms · 2/10 | 319 ms · exact |
| `nostr` | 186 ms | 23.8k | 88 ms · 1/10 | 91 ms · 4/10 | 127 ms · 5/10 | 234 ms · exact | 205 ms · exact |
| `bitcoin` | 145 ms | 6.9k | 42 ms · 1/10 | 83 ms · 2/10 | 71 ms · exact | 94 ms · exact | 134 ms · exact |
| `bitc` (a prefix) | 85 ms | 7.2k | 36 ms · 2/10 | 52 ms · 3/10 | exact | exact | exact |
| `love` | 118 ms | 1.7k | 70 ms · 5/10 | exact | exact | exact | exact |
| `lightning` | 55 ms | 861 | exact | exact | exact | exact | exact |
| `zap` | 35 ms | 1.2k | exact | exact | exact | exact | exact |

("served" is the uncut query's `totalCount`, net of the trust gate; "n/10" is
how many of the exact top-10 the cut page still holds; "exact" means the cut
did not engage — count and page byte-identical, `degraded` false.)

Three things to read off it:

- **The cost is the cut, not the corpus.** The window behind a cut of N is
  the newest stretch that holds N matches, walked against every matcher, so
  it costs the same on 1.3M events as on 333M: `the` at 10,000 is 69 ms here
  and `sort:recent` — the same cut on the same engine — was 0.5 s on staging
  with one thread and the relay's floor on top. That is the 200 ms page.
- **Where it does not engage, nothing changes.** Every word under the depth,
  every page, every count — byte-identical. Only words in a few percent of
  the corpus are cut, which are exactly the words that took seconds.
- **Where it engages, the page is a different page** — the best-ranked of the
  newest N, not of the corpus. The trust-ranked top-10 for `the` or `nostr`
  is spread across the whole month, and the newest few thousand matches hold
  0–5 of them. That is the trade, and it is the same trade `sort:recent`
  already makes; the store's own analysis is that an exact top-K over
  millions of matches cannot be had in under a second by any other means
  (`benchmark/README.md`, "What is left"). The patch keeps a short cut page
  from ending a pager early — a degraded page shorter than its limit widens
  the cut ten-fold once, then runs exact — and `VESPA_SEARCH_MATCH_PHASE_MAX_HITS=0`
  is the old relay, exact answers and their seconds included.

Match threads help the uncut shape and nothing under the cut: `the` uncut
629 → 331 ms from one thread to four, `the` at 10,000 53 → 69 ms (noise).

## A cut that does not decide the answer: trust descent (2026-09-03, later)

The newest-N cut above was refused as a product: a configured depth must not
decide what a search answers. So the question became whether the engine can
be made to stop early WITHOUT changing the answer, and the answer is yes —
because of what the ranking is.

**The ranking is trust, then text.** `search` scores `text × wot_mult(trust)
× recency`; text spans ×236 across its bands, trust spans ×250,000 across
0..100 (`wot_mult = 1 + (rank − floor)^2.7`), recency ×1.1. The exact top-10
for `the` under the observer is ten weak-band hashtag hits ordered purely by
their authors' trust (97, 95, 71, 71, 64, 61, 58, 58, 51, 51); for `nostr`,
ten hits by authors ranked 100, 100, 100, 99, 99, 99, 99, 97, 97, 97. So the
docs that can be on a page are, overwhelmingly, the docs of the most trusted
authors — and the reputation parent already knows who those are.

**The mechanism.** A scalar on the reputation document, `max_rank` — the best
rank ANY observer gives this author, kept fresh by the trust projection —
imported into the event as `author_max_rank`. A relevance search then carries
`author_max_rank >= T`, and Vespa drives the AND with THAT range: it walks the
trusted authors' notes and checks the word against each, instead of walking
the word's postings (millions) and checking each against the gate. Measured
on the local slice (1.28M kind-1 notes, the observer's provider's real cards):

| authors ranked ≥ T (by anyone) | share of kind-1 notes |
|---:|---:|
| 2 (everyone the provider scores) | 10.6% |
| 10 | 5.0% |
| 20 | 3.8% |
| 50 | 2.2% |
| 90 | 0.5% |

**The proof.** Every doc the clause excludes has `max_rank < T`, so its score
under THIS observer (whose rank for it is ≤ its max) is at most
`ceiling × wot_mult(T−1) × 1.1`, where `ceiling` is the largest text score a
document can earn (the token tier plus its tails, ~131,100 — a schema
constant). If the page's K-th hit scores at least that, no excluded doc could
have displaced it: the page is the exact page. The K-th score also says how
low T must go for the proof to hold, so the descent is two queries — a first
rung high (T=90, cheap) that reveals the K-th score, then one rung at the T
that score proves — and a page whose K-th hit is by a poorly ranked author
descends to `T = floor`, which is the exact answer walked over the trusted
authors only. The answer is exact at every rung it stops on; T never decides
it, only how fast it was found.

Measured, same corpus, kind 1, K=40, 4 threads (`descent.mjs`, ladder shown
rung by rung; `proven` is the bound above holding):

```
"the"      exact 154ms, served 31,604
  rank>=90  13ms kept  2,138  top10  2/10   rank>=50  22ms kept 7,409  top10 10/10
  rank>=20  30ms kept 11,716  top10 10/10   rank>=10  35ms kept 16,157 PROVEN, page identical
"nostr"    exact  86ms — rank>=90 12ms 10/10 … rank>=20 35ms PROVEN, identical
"bitcoin"  exact  39ms — rank>=90 14ms 10/10 … rank>=20 25ms PROVEN, identical
"love"     exact  32ms — rank>=90 15ms  6/10 … rank>=10 24ms PROVEN, identical
```

Two rungs (T=90, then the proven T) cost 46 ms for `the` against 154 exact.
The slice is 190x smaller than staging and its authors are scored by ONE
provider (so `max_rank` is that provider's rank; on staging it is the max
over 1,023 providers and covers more authors); the transferable numbers are
the shares and the per-doc walk cost (~0.5 µs per trusted-author note
checked, 4 threads). On staging, authors ranked ≥ 20 by anyone are perhaps
5-10% of 149M notes: an exact `the` becomes a ~3-7M-doc walk, roughly
**0.5-1 s where it is 16 s today**, and `bitcoin` **~0.3-0.5 s** — for every
common word alike, since the walk is the trusted corpus rather than the
word. Rare words stay on the text driver and stay fast. That is exact, with
no knob in the answer; it is not 200 ms.

**Why exact cannot be 200 ms**, in one line: 200 ms buys a walk of ~1.5M
notes, which is authors ranked ≥ ~50; the proof at T=50 needs the K-th hit to
score 5.3e9, and no note can (a body hit by a rank-100 author scores 1.3e8).
The ×236 text spread is what defeats a trust-only stop; the recency term is
too weak to stop on at all.

**What the trust key can also do, if the product allows a first answer that
is not the final one:** the T=90 rung answers in ~100 ms on staging's shape
and is the exact top of the page for most words (`nostr`, `bitcoin`: 10/10
already); the proven rung follows. A relay REQ streams, so a page could draw
the first rung and re-sort when the proven one lands. That is a product
decision, not made here.

**What was checked and set aside:** Vespa accepts a match phase keyed on the
imported `author_max_rank` (it works, and at moderate depths it was exact on
every word tried), but its threshold is an ESTIMATE — kept sets held docs of
rank 2 while excluding rank-40 ones — so no bound can be proven over it; the
explicit range clause is what makes the proof sound, and it is as cheap.

**Through this relay, on the same slice** (2026-09-04, the pinned build and
this branch each run over the same local Vespa, one REQ at a time, p50 of 5,
`kinds:[1]`, limit 40, the observer's lens):

| word | pinned relay | this branch | page |
|---|---:|---:|---|
| `the` | 179 ms | 109 ms | identical |
| `nostr` | 144 ms | 129 ms | identical |
| `bitcoin` | 90 ms | 101 ms | identical |
| `lightning` | 58 ms | 84 ms | identical |
| `love` | 70 ms | 82 ms | identical |
| `zap` | 28 ms | 45 ms | identical |
| `bitc` | 56 ms | 60 ms | identical |
| `xylophonist` | 23 ms | 39 ms | identical (no hits) |

Every page byte-identical, and the boot log said `trust descent: on` before
the first ask. On a slice this small the relay's own floor (the expansion's
companion reads, the splice, the wire) is most of every number, so the
engine-side gain (`the` 154 → 46 ms) shows as 179 → 109; what transfers to
staging is the shape — a common word costs the trusted walk, a rare word
pays one extra cheap rung (~15 ms here). Staging itself still runs the old
build as of this writing (`bitcoin` 4.3 s, `nostr` and `the` 16.5 s, re-timed
2026-09-04) and is the measurement that remains.

**Where it lives now:** the store branch `claude/trust-descent-myuucz` on
NosFabrica/vespa-eventstore (merged as PR #98; this branch pins it) — `reputation.max_rank` and its import, the
projection's upkeep and the one-time backfill, `EventQuery.trustFloor`, the
descent and its bound in `TrustDescent`, the mock engine's rung support and
the tests, and the measurement in that repo's `benchmark/README.md` §6. The
relay takes it by bumping `vespaEventStore` in `gradle/libs.versions.toml`
once it lands; nothing on the relay side changes shape, since a REQ still
answers with its events in final rank order and then EOSE. The earlier
depth-cut branch (`claude/search-query-performance-myuucz` there) is
superseded and should be closed unmerged.

## What is NOT the problem

Things measured on the way that are fine and need no change: the WebSocket
and TLS (~100 ms from here, the same for every shape); the summary fetch and
the expansion's splice (a rare word's whole round trip is 0.3 s); the People
tab (`kind 0` searches answer in 0.4 s — the profile corpus is 55M but the
match sets are small); the feed (a termless read is 0.17 s); the `since`
window (7 days cut `bitcoin` 4.4 → 1.2 s, which is the window's candidates
being walked — the cut does that better).
