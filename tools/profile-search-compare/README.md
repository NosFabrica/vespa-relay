# profile-search-compare

A harness that answers one question: **does this relay's NIP-50 profile search
return the same people as [brainstorm.world](https://brainstorm.world)'s?**

brainstorm.world's profile search is itself a NIP-50 relay
(`wss://search.brainstorm.world`, from its `config.js` `VITE_WOT_SEARCH_RELAY`).
So the comparison is apples-to-apples: send the same `kind:0 search:<term>`
query to both relays and score the overlap. It's plain `node` — Node 22's
built-in `WebSocket`, no dependencies.

## The two tools

| tool | what it does |
|---|---|
| `sync.mjs` | Fills a target vespa-relay with profile data pulled from the relays the `routerConfigOverride` streams down from, scoped to what a profile-search test needs. Writes a pinned reference snapshot. |
| `compare.mjs` | Runs the query battery against your relay and the reference, and reports coverage / recall / conditional-recall / ranking agreement. |

`compare.mjs` stands alone — point it at any two NIP-50 relays. `sync.mjs` is
the stand-in crawl that makes a local relay worth comparing (the real crawl is
the separate [`sot`](https://github.com/vitorpamplona/sot) job; vespa-relay is
the serve half).

## Three numbers, because "we returned different people" has two causes

A raw "how much did the result sets overlap" number hides *why* they differ.
The report separates them:

- **coverage** — of the reference's top-K pubkeys, how many exist in your store
  at all (a direct author lookup, ranking aside). Low coverage is a **sync
  gap**: the crawl never brought the profile in.
- **recall** — of the reference's top-K, how many your search actually returns
  in its own top-K. The headline "do we find the same people" number.
- **conditional-recall** — recall measured only over the profiles you *do* have
  (`recall / coverage`). This isolates **search quality** from crawl
  completeness. Coverage 40% + conditional-recall 95% means the index is fine
  and the crawl is the bottleneck.
- **ranking agreement** — normalized Spearman footrule over the profiles both
  relays return; `100%` = identical order.

## Run it

```bash
# 1. Fill a local vespa-relay from the config's feed relays, pin a snapshot.
node sync.mjs --target ws://localhost:7777 --k 10 --noise 5000 --gold gold.json

# 2. Score your relay against that snapshot (or live against brainstorm).
node compare.mjs --ours ws://localhost:7777 --gold gold.json --json report.json
node compare.mjs --ours ws://localhost:7777 --ref wss://search.brainstorm.world
```

### compare.mjs flags

| flag | default | meaning |
|---|---|---|
| `--ours <ws>` | *required* | the vespa-relay under test |
| `--ref <ws>` | `wss://search.brainstorm.world` | reference relay (ignored if `--gold` given) |
| `--gold <file>` | — | score against a pinned `sync.mjs` snapshot instead of a live reference |
| `--k <n>` | `10` | top-K per query |
| `--queries a,b,c` | built-in battery | override the query set (also `QUERIES=` env) |
| `--ours-search "<tokens>"` | `include:spam` | NIP-50 extension tokens added to **our** query only |
| `--json <file>` | — | write the full per-query breakdown |

`--ours-search` is the knob for *which* search you're comparing. It defaults to
`include:spam`, which lifts vespa-relay's default trust floor so an anonymous,
observer-less search is scored on **raw text relevance**. To compare a
web-of-trust vantage point instead, pass `--ours-search "observer:<pubkey>
sort:rank"` — but see the caveat below.

## What "match" means here, and the WoT caveat

vespa-relay ranks profile search by the **searcher's web of trust**, built from
`kind:3` contact lists in the store, seen from an observer — a NIP-42 login, or
an `observer:` term in the search itself. Anonymous searches are untrusted: the
relay dropped its operator-wide default observer, so with no observer there is
no lens and the whole corpus ranks flat. brainstorm ranks by *its* web of trust.
Two things follow:

1. **Finding the same people** (coverage + recall on text relevance) is a fair,
   sandbox-scale comparison. This is the default the harness runs.
2. **Ranking them the same way** needs the same observer *and* a follow graph of
   comparable depth. `sync.mjs` pulls `kind:3` only for the profiles under test,
   so the local trust graph is shallow — `sort:rank` on it underperforms plain
   text relevance. A fair ranking comparison needs the full `kind:3` crawl, not
   this scoped stand-in. The harness plumbs the WoT dimension through; the data
   to exercise it at parity is the crawl's job.

## Reference run (sandbox, 18-query battery, K=10)

Local single-node Vespa + vespa-relay, filled by `sync.mjs` (177 reference
profiles backfilled by author from the feed relays + ~5k noise profiles),
scored against a pinned `search.brainstorm.world` snapshot:

| mode (`--ours-search`) | coverage | recall | conditional-recall | ranking agreement |
|---|---|---|---|---|
| `include:spam` (text relevance) | 100% | **89%** | 89% | 63% |
| `include:spam sort:rank` (thin WoT) | 100% | 77% | 77% | 58% |

Reading it: the configured feed relays carry **every** profile brainstorm
returned (coverage 100%), and vespa-relay's text search surfaces **89%** of
brainstorm's top-10 people. The misses are ranking, not absence — e.g. for
`alice`, all 10 of brainstorm's Alices are in the store, but an observer-less
text search returns 10 *other* profiles literally named "Alice"; brainstorm's
web of trust picks the well-connected ones. That gap is what an observer over a
full `kind:3` crawl closes.

## Notes on the feed relays

- `brainstorm.nostr1.com` (a config mirror) currently returns **402 Payment
  Required** ("relay is paused for past-due payment") and is skipped.
- The feed relays reject NIP-50 `search` on `kind:0` (only `relay.ditto.pub`
  and partially `nostr-pub.wellorder.net` answer it) — they carry the profiles,
  vespa-relay builds the search. That's why `sync.mjs` backfills **by author**.
- `directory.yabu.me` and `profiles.nostr1.com` had the best author-lookup
  coverage of the reference set in the reference run.
