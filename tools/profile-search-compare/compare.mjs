/*
 * Profile-search comparison harness.
 *
 * Runs a battery of profile queries against two NIP-50 relays — "ours"
 * (a vespa-relay) and a reference (brainstorm.world's search relay by default)
 * — and reports how closely our profile search matches the reference's.
 *
 * The match is scored on three deliberately separated axes, because "we
 * returned different people" has two very different causes and they need
 * different fixes:
 *
 *   coverage   of the reference's top-K pubkeys for a query, how many exist in
 *              our store at all (a direct author lookup, ranking aside). Low
 *              coverage is a SYNC gap — the crawl never brought the profile in.
 *
 *   recall     of the reference's top-K, how many our search actually returns
 *              in its own top-K. This is the headline "do we find the same
 *              people" number.
 *
 *   conditional-recall   recall measured only over profiles we DO have
 *              (recall / coverage). This isolates SEARCH quality from sync
 *              completeness: if coverage is 40% but conditional-recall is 95%,
 *              the index is fine and the crawl is the bottleneck.
 *
 * plus ranking agreement (order overlap) on the profiles both relays return.
 *
 * Usage:
 *   node compare.mjs --ours ws://localhost:7777 [--ref wss://search.brainstorm.world]
 *                    [--k 10] [--queries jack,gigi,...] [--ours-search "include:spam"]
 *                    [--gold gold.json] [--json out.json]
 *
 * --ours-search injects NIP-50 extension tokens into OUR query only (the
 * reference gets the bare term). It defaults to "include:spam", which lifts
 * vespa-relay's default trust floor so an anonymous, observer-less search is
 * compared on raw text relevance. Drop it (--ours-search "") to compare with
 * the trust gate on, or pass "observer:<pubkey> sort:rank" to compare a
 * specific web-of-trust vantage point against the reference.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { RelayConn, profileName } from "./nostr.mjs";
import { queriesFrom, BRAINSTORM_SEARCH } from "./queries.mjs";

function parseArgs(argv) {
  const a = { k: 10, oursSearch: "include:spam" };
  for (let i = 0; i < argv.length; i++) {
    const v = argv[i];
    if (v === "--ours") a.ours = argv[++i];
    else if (v === "--ref") a.ref = argv[++i];
    else if (v === "--k") a.k = parseInt(argv[++i], 10);
    else if (v === "--queries") a.queries = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    else if (v === "--ours-search") a.oursSearch = argv[++i];
    else if (v === "--gold") a.gold = argv[++i];
    else if (v === "--json") a.json = argv[++i];
    else if (v === "--timeout") a.timeout = parseInt(argv[++i], 10);
  }
  return a;
}

const pk = (e) => e.pubkey;
const uniqInOrder = (events) => {
  const seen = new Set();
  const out = [];
  for (const e of events) {
    if (seen.has(e.pubkey)) continue;
    seen.add(e.pubkey);
    out.push(e);
  }
  return out;
};

/** Which of `pubkeys` have a kind:0 in `conn`'s store — batched author lookups. */
async function presentPubkeys(conn, pubkeys, timeoutMs) {
  const present = new Set();
  const chunk = 200;
  for (let i = 0; i < pubkeys.length; i += chunk) {
    const authors = pubkeys.slice(i, i + chunk);
    const r = await conn.req({ kinds: [0], authors, limit: authors.length }, { timeoutMs });
    for (const e of r.events) present.add(e.pubkey);
  }
  return present;
}

/** Normalized Spearman footrule over the shared pubkeys: 1.0 = identical order. */
function orderAgreement(refOrder, oursOrder) {
  const shared = refOrder.filter((p) => oursOrder.includes(p));
  if (shared.length < 2) return shared.length === 1 ? 1 : null;
  const rank = (arr) => new Map(arr.filter((p) => shared.includes(p)).map((p, i) => [p, i]));
  const rr = rank(refOrder);
  const or = rank(oursOrder);
  let dist = 0;
  for (const p of shared) dist += Math.abs(rr.get(p) - or.get(p));
  const n = shared.length;
  const maxDist = Math.floor((n * n) / 2); // max footrule for n items
  return maxDist === 0 ? 1 : 1 - dist / maxDist;
}

function fmtPct(x) {
  return x == null ? "  n/a" : (x * 100).toFixed(0).padStart(3) + "%";
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.ours) {
    console.error("error: --ours <ws-url> is required (the vespa-relay under test)");
    process.exit(2);
  }
  const refUrl = args.ref || BRAINSTORM_SEARCH;
  const K = args.k;
  const timeoutMs = args.timeout || 15000;
  const queries = queriesFrom(process.env, args.queries);

  // A pinned reference snapshot (written by sync.mjs) makes coverage/recall
  // reproducible against exactly the profiles that were synced. Without it we
  // query the reference live.
  let gold = null;
  if (args.gold) {
    try {
      gold = JSON.parse(readFileSync(args.gold, "utf8")).byQuery || null;
    } catch (e) {
      console.error(`warn: could not read --gold ${args.gold}: ${e.message}; querying reference live`);
    }
  }

  const ours = await new RelayConn(args.ours, { timeoutMs }).connect();
  const ref = gold ? null : await new RelayConn(refUrl, { timeoutMs }).connect();

  console.log(`# profile-search comparison`);
  console.log(`ours: ${args.ours}    reference: ${gold ? args.gold + " (pinned)" : refUrl}`);
  console.log(`K=${K}   ours-search token: ${JSON.stringify(args.oursSearch)}   queries: ${queries.length}\n`);

  const header =
    "query".padEnd(12) + "ref  ours  cover  recall  cond-recall  rank-agree  ours-top-names";
  console.log(header);
  console.log("-".repeat(header.length + 20));

  const rows = [];
  for (const q of queries) {
    const refEvents = gold
      ? (gold[q] || []).map((p) => ({ pubkey: p }))
      : uniqInOrder((await ref.req({ kinds: [0], search: q, limit: K }, { timeoutMs })).events).slice(0, K);
    const refPk = refEvents.map(pk);

    const oursQuery = args.oursSearch ? `${q} ${args.oursSearch}` : q;
    const oursRes = await ours.req({ kinds: [0], search: oursQuery, limit: K }, { timeoutMs });
    const oursEvents = uniqInOrder(oursRes.events).slice(0, K);
    const oursPk = oursEvents.map(pk);

    const present = await presentPubkeys(ours, refPk, timeoutMs);
    const oursSet = new Set(oursPk);
    const covered = refPk.filter((p) => present.has(p));
    const recalled = refPk.filter((p) => oursSet.has(p));
    const coverage = refPk.length ? covered.length / refPk.length : null;
    const recall = refPk.length ? recalled.length / refPk.length : null;
    const condRecall = covered.length ? recalled.length / covered.length : null;
    const rankAgree = orderAgreement(refPk, oursPk);

    const names = oursEvents.slice(0, 5).map((e) => profileName(e) || e.pubkey.slice(0, 6)).join(", ");
    rows.push({ q, refCount: refPk.length, oursCount: oursPk.length, coverage, recall, condRecall, rankAgree, refPk, oursPk });

    console.log(
      q.padEnd(12) +
        String(refPk.length).padStart(3) +
        String(oursPk.length).padStart(6) +
        "  " + fmtPct(coverage) +
        "  " + fmtPct(recall) +
        "      " + fmtPct(condRecall) +
        "       " + fmtPct(rankAgree) +
        "  " + names.slice(0, 40),
    );
  }

  const mean = (f) => {
    const xs = rows.map(f).filter((x) => x != null);
    return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
  };
  console.log("-".repeat(header.length + 20));
  console.log(
    "MEAN".padEnd(12) +
      "     " +
      "     " +
      "  " + fmtPct(mean((r) => r.coverage)) +
      "  " + fmtPct(mean((r) => r.recall)) +
      "      " + fmtPct(mean((r) => r.condRecall)) +
      "       " + fmtPct(mean((r) => r.rankAgree)),
  );

  console.log(
    `\nsummary: across ${rows.length} queries at K=${K} — ` +
      `coverage ${fmtPct(mean((r) => r.coverage)).trim()} (have the profile), ` +
      `recall ${fmtPct(mean((r) => r.recall)).trim()} (surface it), ` +
      `conditional-recall ${fmtPct(mean((r) => r.condRecall)).trim()} (surface it *given* we have it), ` +
      `ranking ${fmtPct(mean((r) => r.rankAgree)).trim()} agreement on shared results.`,
  );

  if (args.json) {
    writeFileSync(
      args.json,
      JSON.stringify(
        {
          ours: args.ours,
          reference: gold ? args.gold : refUrl,
          k: K,
          oursSearch: args.oursSearch,
          means: {
            coverage: mean((r) => r.coverage),
            recall: mean((r) => r.recall),
            conditionalRecall: mean((r) => r.condRecall),
            rankAgreement: mean((r) => r.rankAgree),
          },
          rows,
        },
        null,
        2,
      ),
    );
    console.log(`\nwrote ${args.json}`);
  }

  ours.close();
  ref && ref.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
