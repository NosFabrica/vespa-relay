# Schema migrations on a cluster that already holds data

Most store bumps need nothing here. `AUTO_DEPLOY` posts the bundled package on
every boot, a no-change deploy is a cheap no-op, and a fresh cluster is always
correct by construction.

A cluster that already holds events is the case this file is about. Some schema
changes cannot be applied by the deploy alone, and **the deploy will not fail —
it will report success and leave the corpus half-migrated**. Vespa says what it
wants in the deploy's *response body*, under `configChangeActions`, which
nothing in this repo reads:

```json
"configChangeActions": {
  "restart": [], "refeed": [],
  "reindex": [{"documentType": "event", "clusterName": "content",
    "messages": ["Document type 'event': Non-document field 'search_text_gram'
                  added; this may be populated by reindexing"]}]
}
```

So after a store bump that touches `event.sd`, read that object. `restart`,
`refeed` and `reindex` are three different migrations with three different
repairs, and picking the wrong one is silent.

## Which repair applies — fed vs derived

The distinction that decides everything, and the one that is easy to get
backwards:

| | who writes it | populated by | example |
|---|---|---|---|
| **fed** field | we do, on `put` | a **re-feed** (`REINDEX_FTS_ON_START`) | the near tier (`*_near`) |
| **derived** field | Vespa, at index time | a **Vespa reindex** | `search_text_gram` |

A Vespa reindex cannot produce a fed field — nothing re-derives data we never
sent. A re-feed *can* produce a derived one, but `REINDEX_FTS_ON_START` will
not: its drift check re-puts only documents whose fields changed, a derived
column is not among the fields it compares, so every document looks current and
the walk logs `reindex complete` having re-put nothing. Both tools report
success while doing nothing. Match the tool to the field class.

## Migration: `search_text_gram` (store `e3be81564d`)

The body's partial-word reach, and a **reindex-class** change. Derived by Vespa
from `search_text` at index time, so the deploy adds the column and leaves it
empty for every event already stored. Whole-word search keeps working, nothing
errors, and the entire back catalogue is quietly exact-token-only until
repaired.

Measured end to end on a single-node Vespa 8 (2026-08-15) — old schema, two
documents fed, new schema deployed:

```
partial-word query:  0 hits    <- the whole corpus, silently
whole-word query:    1 hit     <- serving normally the entire time
```

### The procedure

Below, `$BASE` is the config server. Under this repo's compose it is bound to
loopback on the host (`127.0.0.1:19071`); the long path is not optional — a
`POST /reindex` without it is a 404.

```bash
BASE=http://localhost:19071/application/v2/tenant/default/application/default/environment/prod/region/default/instance/default

# 1. Deploy the new schema FIRST. Reindexing can only fill a column the
#    serving schema has. (AUTO_DEPLOY does this on boot; this is the explicit
#    form.) Read configChangeActions in the response.

# 2. Request reindexing of the event type on the content cluster.
curl -X POST "$BASE/reindex?clusterId=content&documentType=event"
# -> {"message":"Reindexing document types [event] in 'content' of application default.default.default"}

# 3. REDEPLOY THE APPLICATION. This is the step that actually starts it —
#    see the warning below.

# 4. Poll until it finishes. This is the only completion signal.
curl "$BASE/reindexing"
# -> ..."ready":{"event":{"state":"pending"}}      still queued
# -> ..."ready":{"event":{"state":"successful"}}   done
```

### The step that is easy to miss

**`POST /reindex` alone does nothing. It queues the job; a deployment
dispatches it.** Measured: after the POST the state sat at `pending` for over
ten minutes with the column still empty, and the partial-word query still
returning 0. Redeploying the same, unchanged package moved it
`pending → successful` in ~60s, and the same query went to 1 hit:

```
state before redeploy: pending           hits: 0
[redeploy the identical package]
[15s] state=pending      hits=0
[60s] state=successful   hits=1
```

An operator who POSTs and then only polls will wait forever and conclude
reindexing is broken. Poll `state`, and if it stays `pending`, deploy again.

The 200 from the POST is not completion either — it means *queued*. Reindexing
walks the corpus, so on a real corpus budget for it accordingly; the ~60s above
is two documents and says nothing about 211M.

### The alternative

A full re-feed also works: a plain `put` re-derives the column even with
byte-identical content. Far more expensive at corpus scale, but it is the
fallback if reindexing will not dispatch. Note this is a genuine re-feed of
every document — **not** `REINDEX_FTS_ON_START`, which skips them all.

## Migration: Trusted List titles (store `eaee359357`)

The first **re-feed-class** migration this file has carried, and the mirror
image of the one above. Kinds 30392-30395 became searchable with no schema
change at all: `SearchExtractors` mirrors Quartz's searchable set, and upstream
implemented `SearchableEvent` on `TrustedListEvent`. So `configChangeActions`
is empty, the deploy has nothing to do, and the corpus is nonetheless
half-migrated — every Trusted List written BEFORE the bump carries no search
text, and nothing about the cluster looks wrong.

`search_text` is data we WRITE on put, so this is the case `REINDEX_FTS_ON_START`
exists for and a Vespa reindex cannot touch. The store's drift check re-puts a
document whose extracted columns differ from the stored ones, which is exactly
what a pre-bump list is.

Measured here (in-memory index, one kind-30392 list titled "Podcaster Trust
List", stored with the search columns a pre-bump write would have left):

```
before reindexFullTextSearch():  0 hits for "podcaster"
after  reindexFullTextSearch():  1 hit
resulting columns: SearchFields(..., text=Podcaster Trust List, ...)
```

Note the column: `text`, the body tier, not `primary`. Upstream gave the family
no `SearchFieldExtractor` branch, so the title rides the generic fallback into
the TERTIARY tier — reached by trigram substring, not by the prefix/typo
attributes other titled kinds get. That is Quartz's call to change, not ours;
see the `vespaEventStore` comment in `gradle/libs.versions.toml`.

### The procedure

One boot with `REINDEX_FTS_ON_START=true`, then turn it back off — it walks the
whole corpus, and the walk is resumable but not free. New writes need nothing;
they are indexed as they arrive.

## Migration: tiers, badges and a rank profile (store `2bc79f5f40`)

The first bump this file carries that needs BOTH a deploy and a repair, and the
repair is not a Vespa reindex. Read it as two independent migrations that happen
to share a commit: they fail independently, and neither failure looks like one.

**The schema half is a plain deploy.** `event.sd` moves — §12.2's split rung,
§12.3's perfect rung, §13.1's delegated member placement — but every changed
line is a rank function, a rank expression or a rank input. Not one `field`,
`indexing`, `attribute` or `match-features` declaration changes, so there should
be no column to add and nothing in `configChangeActions` to act on, and
`AUTO_DEPLOY` on boot is the whole procedure. Read the response object anyway
and confirm it, per the top of this file — the deploy reports success either
way.

What to watch for here is the *reverse* of the usual trap: a NEW store jar
against an OLD serving schema is not an error and does not log one. The store's
own note on §13.1 is that it stays inert until both halves arrive — an older
serving schema, an unranked finding query and the in-memory reference all place
a member exactly as before — so the page quietly serves the order this bump
exists to change, and the only way to tell is to look at the schema Vespa is
actually serving.

**The extraction half is a re-feed**, the same class as the Trusted List titles
above and for the same reason: `search_primary` / `search_secondary` /
`search_text` are fields we WRITE on `put`, so only a re-put re-derives them.
Two changes land in it at once, and both are in derived data:

- the quartz bump that rides with this pin (`cecc3287b2`) gives nineteen kinds
  their own `SearchFieldExtractor` branch, so seventeen of them stop putting a
  title in the body role — the words do not change, the column does;
- a declared NIP-30 shortcode run (`:verified:`) is rewritten to one synthetic
  term in the secondary tier instead of being tokenized as the name word
  `verified`.

Every event stored before the bump keeps its old columns until it is re-put.
Nothing errors, nothing is missing from a page, and the ranking is simply the
old one for the back catalogue while new writes get the new one — a corpus
ranked two ways, which is harder to notice than an outage.

### The procedure

One boot with `REINDEX_FTS_ON_START=true`, then turn it back off. The store's
drift check re-puts exactly the documents whose extracted columns differ from
the stored ones, which after this bump is every event of an affected kind and
every event wearing a declared badge. New writes need nothing.

A Vespa reindex is the WRONG tool here and will report success having done
nothing to these columns — see "Which repair applies" above.

## If a deploy is refused

A validation error naming an override id means Vespa is protecting the corpus
from a destructive change. Add a scoped `validation-overrides.xml` to the
package rather than forcing it, and read what the override protects first.

## Related

- `AUTO_DEPLOY` and `REINDEX_FTS_ON_START` in [configuration.md](configuration.md)
- `FtsReindex.kt` — the re-feed walk, and what it does not cover
- AGENTS.md, "Traps that have cost real time" — the restart-class sibling of
  this trap (`<jvm options>` changes, which a deploy also will not apply)
