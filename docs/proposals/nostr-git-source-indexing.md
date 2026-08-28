# Proposal: indexing the source code of announced Nostr git repositories

**Status:** proposal only. Nothing implemented, no schema written. The sizing
numbers in [§9](#9-sizing) are modelled, not measured — [Phase 0](#10-phasing)
exists to replace them with real ones before any of this lands.

**Scope:** two repos. The index (a new Vespa document type and its query
surface) belongs to `vespa-eventstore`; the fetcher — the only part that talks
to the wider internet — belongs here, in a new module and its own process.

## 1. What is being asked

NIP-34 kind **30617** (repository announcement) carries `clone` URLs, `web`
URLs, `name`, `description`, maintainer `p` tags, and an `r <commit> euc` tag
naming the repository's earliest unique commit. Kind **30618** (repository
state) carries `refs/heads/<branch>` → commit id and `HEAD`.

We already index those *events* — 30617 lands in the primary/secondary/website
tiers, 1617/1618/1621/1622/1630-1633 land in the body tier. What we do not index
is the thing the events point at: **the code**. "Which Nostr repo implements
NIP-44 encryption" is not answerable from a repo's `description`.

So: fetch the announced trees, index the files, make them searchable — and,
the hard half of the question, **remove what is no longer current** on two
independent axes (the git tree moves; the Nostr event moves) without leaving
drift behind either one.

## 2. The one idea the whole design rests on

Any indexed line of code is derived from a **triple**:

1. **the announcement** — which 30617 is current for its address, which gives
   the clone URLs and the maintainer set;
2. **the tree** — which commit the announced ref points at, which gives the
   blobs;
3. **the extraction** — which version of *our own* file filter, chunker and
   symbol extractor produced the text.

Every removal question is the same question: *this document was derived from a
triple that is no longer current.* "The git changed", "the Nostr event changed"
and "we changed our own indexing rules" are three faces of one predicate, and
the third one is not in the ask but will bite within a month of shipping —
`FtsReindex`'s KDoc is a monument to exactly that class of mistake.

Collapsing them into one predicate is what keeps this from growing a deletion
path per trigger. Two mechanisms, and deliberately no third:

- **an exact delete by docid**, when we know precisely which paths died (git
  told us);
- **a generation sweep**, `delete where repo_key == K and gen < N`, when we do
  not.

Nothing else deletes from the code index. Every row of the table in
[§7](#7-the-removal-matrix) resolves to one of those two.

## 3. Where the code lives: a new `code` document type

A new schema `code.sd` in the bundled application package, sibling to `event.sd`
and `reputation.sd`.

**Not inside `event`.** Three reasons, in descending order of how much they
would cost us:

- **The NIP-01 contract.** `event` documents are lossless reconstructions of
  signed events, and everything downstream trusts that: negentropy offers their
  ids to peers, REQ serves them to clients that re-verify the signature. A
  source file is not an event and must never be able to leave through those
  paths. Separate document types make that impossible by construction rather
  than by convention.
- **Mutual attribute tax.** `docs/attribute-memory.md` measured that every
  attribute costs ~4.8 B/doc *before it holds anything*, and that `id` alone
  models at 13.6 GiB of enum store across 176.7M events. Code documents in
  `event` would pay for `sig`, `tags`, `tag_index`, `expires_at` and the whole
  near tier; `event` documents would pay a per-doc slot for `path`, `blob_sha`,
  `repo_key`, `gen` and `lang`. Both sides lose.
- **Ranking.** Prose ranking is wrong for code. `stemming: none` is already the
  rule here, but code additionally wants exact identifier matching with no typo
  budget: `foo_bar` must not fuzzy-match `foobaz`, and the fuzzy/prefix ladder
  that makes `vitorp` find *Vitor Pamplona* is actively harmful over a symbol
  table.

Sketch — note the attribute count is deliberately small, per the memory lesson
above:

```
schema code {
    document code {
        field repo_key     type string          # the identity a sweep keys on (§4)
        field repo_address type string          # "30617:<pubkey>:<d>" that announced it
        field owner        type string          # announcing pubkey (vanish / eviction)
        field gen          type long            # index generation — the sweep predicate
        field commit       type string          # tree this was read from
        field blob_sha     type string          # content identity — the write-skip key
        field path         type string          # "store/src/main/kotlin/Foo.kt"
        field lang         type string
        field chunk_no     type int
        field body         type string          # index, stemming: none
        field symbols      type array<string>   # declared identifiers + their parts
    }
}
```

**docid is deterministic:** `hash(repo_key ‖ 0x00 ‖ path ‖ '#' ‖ chunk_no)`.
That is load-bearing twice over — the diff path can delete a vanished file
without first looking it up, and a re-put of the same path overwrites in place
rather than accumulating versions.

**`symbols` reuses machinery we already have.** Making `foo_bar`,
`fooBar` and `FooBar` all reachable from `foo`, `bar` and the whole token is
exactly the problem `SearchFields.nearFields()` solves for names — feed both
granularities into one attribute via `NearText.parts` + `NearText.tokens` +
`mergeNear`. That code is written, tested, and its KDoc explains why the merge
is free.

**No trigram column on `body`.** `search_text_gram` is affordable on events
because a note is short. A gram index over source bodies would be the largest
structure in the cluster by a wide margin, for a substring query
(`->weight`) we have no evidence anyone will run. Ship without it, measure,
and if it turns out to be needed put grams on `symbols` only — never on `body`.

### 3.1 Which content cluster

The tempting answer is to co-tenant `code` in the existing content cluster so it
can `reference<reputation>` and inherit the trust join and the observer gate for
nothing. Vespa's parent/child requires parent and child in the **same** content
cluster, so that is the only way to get the join directly.

Recommend against it, because the trust join is not actually needed on the code
document:

> **Trust applies at the repo tier, not the file tier.** A code query answers
> *which `repo_key`s match, and how strongly*. Repos are 30617 events, which
> live in the event cluster and already carry the trust join, the observer gate
> and the rank floor. So the pipeline is: code cluster scores repos → event
> cluster ranks and gates those repos through the observer → snippets are
> fetched for the survivors. Trust never touches a code document.

That resolution buys a **separate `<content>` cluster in the same application
package**, with its own nodes, its own resource limits and its own feed-block
threshold. Given that #69 was six OOM kills in 28 hours, letting an unbounded
git clone share a feed-block threshold with the relay's serving corpus is a bad
trade. One deploy, one operational surface, two blast radii.

Co-tenanting stays available as the simpler v0 if running a second cluster is
not wanted; the cost is the shared blast radius, and the schema does not change.

## 4. `repo_key`: use the euc, but verify it

NIP-34's `euc` tag exists precisely so forks and multiple maintainers of the
same repository can be recognised as one thing. Keying the index on the euc
means we clone and index a repo **once** no matter how many maintainers
announce it — a real economy at network scale, where a popular repo may carry
half a dozen announcements.

It is also a takeover vector, and an easy one: announce a 30617 carrying a
popular repo's euc and a `clone` URL pointing at your own tree, and a
euc-keyed index serves your code as theirs, for the cost of one event.

**So the euc must be earned, not claimed.** After fetching, the claimed euc must
actually be a root commit of the fetched history:

```
git rev-list --max-parents=0 --all   # must contain the claimed euc
```

If it does not, the announcement is lying about its identity. Do not fold it —
and do not quietly key it on its own address either, because an announcement
that lies about this is not a good-faith one. Refuse the repo and record the
refusal.

Where no euc is present at all (it is optional), key on
`30617:<pubkey>:<d>` and accept the duplication.

**A changed euc on a superseded 30617 is a different repository**, not an
edit — drop the old `repo_key` wholesale and index the new one.

## 5. The fetching plane

A new module `:forge` in this repo, running as **its own process** (`ForgeMain`)
over `:peers`.

Its own process, not a passenger in `:sync`, for the same reason `:sync` and
`:relay` are split: a git clone is unbounded disk and CPU in a way nothing else
here is, and it must be independently killable, restartable and quota-able. Its
disk is a separate volume. It reads the store over plain NIP-01 — the same
process-boundary-survivable seam `AGENTS.md` praises the monitor for keeping.

Nothing about this goes in `vespa-eventstore`. That library is embedded by
client applications, its CI is hermetic on purpose, and "clones arbitrary URLs
supplied by strangers" is not a property a store may acquire.

### 5.1 Clone URLs are hostile input

They are attacker-controlled strings that we hand to a subprocess that speaks
several protocols. Minimum bar:

- **Protocol allowlist.** `protocol.allow=never` plus explicit
  `protocol.https.allow=always`. `ext::` in particular is remote code execution
  by design and must be off; so are `file://`, `ssh://` and `git+ssh`.
- **SSRF.** Resolve first, refuse loopback / link-local / RFC1918 / metadata
  addresses, and refuse redirects into them. `.onion` clone URLs go through the
  existing `TorTransport` and `DialGate`, which already do the socket budgeting.
- **Bare clone, never a checkout.** `git clone --bare --depth 1 --single-branch
  --filter=blob:none`, then read blobs with `git ls-tree -r` +
  `git cat-file --batch`. With no working tree there is no symlink escape, no
  path traversal, no `.gitattributes` filter driver, no hook on disk to execute.
  It is also faster and pulls fewer bytes. `--no-recurse-submodules` — a
  submodule URL is a second unvalidated fetch target.
- **Caps at every axis**: bytes fetched, files, per-file size (skip > 1 MB),
  wall clock, and a per-repo disk quota. `GIT_TERMINAL_PROMPT=0`,
  `credential.helper=` empty, so nothing can ever block on or leak a credential.
- **Skip what is not source**: binaries, minified bundles, lockfiles,
  `node_modules`, and anything `.gitattributes` marks `linguist-vendored` or
  `linguist-generated`.

### 5.2 Admission is a trust decision, and we already own it

We cannot index every repository anyone announces — that is an unbounded
disk-fill for the price of one event. The gate should be the machinery this
deployment is already built around: **only index repos whose announcing pubkey
clears a rank floor under the relay operator's own lens**, which is exactly
`filter:rank:gte:N` against a configured observer. Below the floor, the 30617
stays searchable as an event; its code is simply not fetched.

Then a global byte budget with lowest-trust-first eviction. This is the single
most important sizing knob and it costs nothing new to build.

## 6. Change detection, and the write path

**Detecting that a tree moved, without polling by cloning:**

1. **30618 repo-state events** are push-based and free: the event names the
   commit. Best signal, but most repos will not publish one.
2. **`git ls-remote <url>`** is one cheap round trip returning ref → sha with no
   clone. Poll on a trust-weighted cadence — high-trust hourly, the tail daily.
   Unchanged sha ⇒ zero work.
3. Never poll by cloning.

**Triggers are advisory; the reconciler is authoritative.** The relay's
`IngestPipeline` already sees every mirrored event and `RelayWebSocket` every
client EVENT, so a processor filtering kinds {30617, 30618, 5, 62} enqueues
repo work cheaply. But a stream cannot see supersession *losses*, NIP-40 GC, or
any deletion the store performs internally. So a periodic **anti-join** —
distinct `repo_key`s in the code index against live 30617s in the event store —
is what actually guarantees correctness. At repo scale (10³–10⁵) that is one
grouping query. This is precisely the `TrustProjection`-fast-path /
`TrustReconciler`-backstop split, and it is the shape that has already worked
here.

**The pass itself:**

```
  30618 lands ─┐
ls-remote poll ─┼─► RepoQueue (coalescing, keyed by repo_key)
   reconcile ───┘
                     │
              ForgeWorker (bounded pool)
                     │   bare fetch → ls-tree → cat-file --batch
                     ▼
              gen = N (monotonic per repo)
                     │
       ┌── old commit known and an ancestor of new? ──┐
      yes                                            no
       │                                              │
   DIFF PATH                                      SWAP PATH
   git diff --name-status old..new                put EVERY current file @gen
     A/M  → put @gen                              then, after all acks:
     D/R→ → delete by docid                       DELETE where repo_key == K
     unchanged → untouched, zero writes                     and gen < N
       │                                              │
       └──────────────► write repo state doc ◄────────┘
                        indexed_commit = new, gen = N
```

Three properties worth stating explicitly, because they are what make this
correct rather than merely plausible:

- **The sweep runs on the swap path only.** A swap pass writes every current
  file at `gen = N`, so `gen < N` is exactly the stale set — renames,
  deletions, filter changes and any half-finished earlier pass, all in one
  operation with no bookkeeping. The diff path needs no sweep because git
  already told us precisely what died; and because it leaves unchanged files
  untouched, the common case (a one-file commit in a 5,000-file repo) costs
  five writes, not five thousand.
- **The fallback is not a rare path.** The swap path is what runs on the *first*
  index of every repository, so it is exercised constantly and cannot rot the
  way an untested recovery branch does.
- **`indexed_commit` is written last, after every ack.** A crash mid-pass leaves
  the state doc pointing at the old commit, so the next pass redoes the same
  diff — idempotent, because puts are by deterministic docid and deletes are by
  docid. This is the `DirtLedger` discipline: persist intent before acting,
  clear it only once the projection has caught up. A crash in a swap pass before
  its sweep leaves stale docs at an older `gen`, which the *next* swap's sweep
  removes. Neither failure is permanent drift.

## 7. The removal matrix

The direct answer to "removal of old edits when the git changes or when the
Nostr event changes" — every trigger, and which of the two mechanisms it uses.

| Trigger | Detected by | Action | Mechanism |
|---|---|---|---|
| New commit on the announced ref | 30618 put, else `ls-remote` | diff path | docid delete |
| Force-push / history rewrite | old commit not an ancestor of new | swap path | **gen sweep** |
| 30617 superseded, clone URLs changed | put trigger → reconcile | re-verify euc, re-fetch, swap | **gen sweep** |
| 30617 superseded, **euc changed** | put trigger | different repo: drop old `repo_key` entirely | **gen sweep** (`gen < ∞`) |
| 30617 deleted (NIP-09 kind 5) | reconcile anti-join | drop `repo_key` | **gen sweep** |
| Author vanished (NIP-62) | reconcile anti-join | drop every `repo_key` owned by pubkey | **gen sweep** |
| 30617 expired (NIP-40) | Vespa GC → reconcile | drop `repo_key` | **gen sweep** |
| Clone URL dead / repo gone | N consecutive fetch failures | mark degraded, keep serving, drop after grace | **gen sweep** |
| Announcer falls below the trust floor | reconcile | evict | **gen sweep** |
| Disk budget exceeded | budget check | evict lowest-trust repos | **gen sweep** |
| **Our extraction rules change** | `extract_version` bump | forced swap pass per repo | **gen sweep** |

The last row is the one nobody asks for and everybody needs — it is the
`FtsReindex` lesson, whose KDoc records a whole-corpus walk that reported
success having repaired nothing. Making the extractor version part of the
staleness predicate from day one means a chunker change is a normal re-pass, not
a migration.

Note also that everything detected by a put trigger is *also* caught by the
reconcile anti-join. The triggers exist for latency; the reconciler exists for
correctness.

## 8. Serving: what a search returns

A REQ returns *events*. Code hits are not events, and this is the part of the
design with a genuine constraint rather than a preference. Three options; take
the first two, refuse the third.

**8.1 — Rank real 30617s by their code (the Nostr-native answer).** A REQ with
`kinds:[30617]`, `search: "code:mutex"` matches the code cluster, groups hits by
`repo_key`, aggregates a per-repo score, resolves each `repo_key` to the live
30617s announcing it, and serves **those** — genuine, signed,
client-verifiable events, ordered by how well their source matched and gated by
the observer's web of trust exactly as any other filter is. Protocol-legal, no
new client support needed, and "which Nostr repos contain X" is the
highest-value question anyway.

Four details that are easy to leave implicit and should not be:

- **A match returns the REPOSITORY, not the file.** There is no event for a
  source file, and §8.3 is where the temptation to mint one is refused. Path,
  line and snippet cannot travel through a REQ at all — that loss is the whole
  reason §8.2 exists.
- **`code:` is an extension token, not a redefinition.** `search: "mutex"` on
  kind 30617 already means "match the announcement's name and description", and
  must keep meaning that. Code search gets its own token beside `observer:`,
  `sort:`, `filter:` and `include:`, and composes with them the same way:
  `code:mutex observer:<hex>` is "repos whose source contains mutex, trusted by
  me". Unknown extensions are already ignored, so an old client asking a new
  relay degrades to plain metadata search rather than erroring.
- **One `repo_key` can have several live 30617s** — different maintainers
  announcing the same euc (§4). Serve every one that clears the gate; the trust
  ordering puts the most-trusted maintainer's announcement first, and a client
  that wants one takes the first. Collapsing to a single "winning" announcement
  would be us picking a maintainer on the client's behalf.
- **`limit` and COUNT are denominated in repos, not matches.** `limit: 20` means
  twenty repositories, so the code query must over-fetch and group *before* the
  limit applies — the two-stage query needs its own fan-out budget, distinct
  from the filter's `limit`. A NIP-45 COUNT answers "how many repos contain
  this", which is almost certainly what is wanted, but it is a different
  question from "how many code hits" and should be documented as such rather
  than discovered.

**8.2 — A JSON endpoint for the lines themselves.** `GET /code/search` on the
relay's Ktor server, returning path + line + snippet + a deep link built from
the repo's own `web` tag — everything §8.1 structurally cannot carry — rendered
as a tab in the existing search UI. This is the repo's own
rule applied unchanged: *engines produce documents, `:web` renders them, and the
seam is JSON.* Only reachable from our own front end, which is the honest trade.

**8.3 — Synthesising unsigned kind-1337 rumors per file (rejected).**
Tempting, because everything would work for free: the store deliberately holds
unsigned rumors, so search, the observer gate, addressable supersession, NIP-09
and NIP-62 would all apply to code with no new code at all. Rejected because the
relay would then serve unverifiable fabricated events to clients and offer their
ids to peers over negentropy. The fences that would prevent that — excluding a
kind from snapshots, from upstream push, from REQ unless explicitly asked — are
exactly the kind of fence that breaks silently, and the `:web` absolute-path
failure mode documented in `AGENTS.md` is the local precedent for how that goes.

Recorded here rather than omitted, in the house style: someone will re-propose
it, and it deserves an answer on file.

## 9. Sizing

Modelled, not measured. 10k repos × ~2k indexed files × ~1.2 chunks ≈ **24M
documents** — 14% of the current event corpus by count, but each body is a
source file rather than a note, so the disk index dominates and the attribute
side stays small **if the attribute count stays small**. Eight attributes at
~4.8 B/doc is ~1 GiB of pure per-document slots; the same schema with twenty
attributes is ~2.5 GiB before holding anything. That is the whole reason
[§3](#3-where-the-code-lives-a-new-code-document-type)'s field list is short.

These numbers are the reason for Phase 0 below. `benchmark/attribute_memory.py`
already exists to turn a modelled table into a measured one.

## 10. Phasing

- **Phase 0 — census, no indexing.** Walk every stored 30617, `ls-remote` each
  clone URL, verify each euc claim, record repo count, reachability, tree size
  and file-count distribution. No schema, no fetch, no disk. This replaces every
  modelled number in §9 with a measured one, tells us how many announcements are
  lying about their euc, and can be run against `search-staging` today. Nothing
  else should start before it reports.
- **Phase 1 — the spine.** `code.sd`, the fetcher, **swap path only**, a small
  allowlist of high-trust repos, the `/code/search` JSON endpoint. No diff path,
  no NIP-50 surface. Correct and slow beats fast and drifting.
- **Phase 2 — steady state.** Diff path, 30618 triggers, `ls-remote` polling,
  the reconcile anti-join, eviction and budgets.
- **Phase 3 — reach.** The §8.1 code-ranked 30617 REQ, the UI tab, per-language
  symbol extraction.

## 11. Testing

- **An executable spec.** `InMemoryCodeIndex`, mirroring what
  `InMemoryEventIndex` is for `EventQuery`, so the fetcher's unit tests need no
  Vespa.
- **Fixture repos as git bundles in test resources** — deterministic, offline,
  and covering exactly the cases that break this class of system: first index,
  one-file change, rename, delete, force-push, an euc that does not match the
  history, a symlink, a submodule, a 100 MB blob, a binary, and a crash injected
  between the last put and the sweep.
- **An integration gate**, because only a real Vespa executes a
  delete-by-selection: `CodeSweepIT` asserting the `gen` sweep removes exactly
  the stale set and nothing adjacent — the direct analogue of `OrphanSweepIT`,
  whose whole existence is that a sweep's blast radius cannot be asserted
  against a mock.

## 12. Open questions

- **Which refs?** Recommend the 30618-announced `HEAD` only. Indexing every
  branch multiplies cost for very little search value.
- **Patches (kind 1617) carry diffs.** They are already indexed as event
  `content`. Indexing them *as code* would let a search reach code that was
  proposed but never merged. Probably out of scope; noted so it is a decision
  rather than an oversight.
- **No 30618 at all** — most repos. Recommend indexing the clone URL's default
  HEAD anyway, with a `state_source` distinguishing `30618` from `head`;
  requiring 30618 would gut coverage.
- **Do we serve file content, or only snippets and a link?** Recommend snippets
  plus the repo's `web` URL. Serving whole files makes this a code host, which is
  a different product with different obligations.
- **Mirroring third-party source has licensing implications** we have not looked
  at. Flagging, not solving.
