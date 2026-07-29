/*
 * Populate a target relay (a vespa-relay) with profile data pulled from the
 * relays the routerConfigOverride streams down from, so its NIP-50 profile
 * search can be compared against the reference.
 *
 * vespa-relay is the serve half; filling the store is the separate `sot` crawl
 * job. This is a lightweight stand-in for that crawl, scoped to what a profile
 * search test needs and to what a sandbox can pull in a couple of minutes. It
 * does NOT negentropy-mirror the whole corpus — it pulls:
 *
 *   1. backfill  the reference's own top-K pubkeys for each battery query,
 *      fetched BY AUTHOR from the feed relays (kind:0 + kind:3). The feed
 *      relays reject NIP-50 `search` but answer author lookups, which is the
 *      point: they carry the data, vespa-relay builds the search over it. How
 *      many of these come back is itself the coverage the report scores.
 *
 *   2. noise     a bulk sample of kind:0 from the feed relays, paginated back
 *      through time, so the search has realistic distractors to rank against
 *      and recall isn't trivially 100%.
 *
 * It writes a pinned reference snapshot (--gold) so compare.mjs scores against
 * exactly the profile set that was synced.
 *
 * Usage:
 *   node sync.mjs --target ws://localhost:7777 [--ref wss://search.brainstorm.world]
 *                 [--k 10] [--queries jack,...] [--noise 4000] [--gold gold.json]
 */
import { writeFileSync } from "node:fs";
import { RelayConn, profileName } from "./nostr.mjs";
import { queriesFrom, ALL_SOURCES, SOURCES, BRAINSTORM_SEARCH, PROFILE_KINDS } from "./queries.mjs";

function parseArgs(argv) {
  const a = { k: 10, noise: 4000 };
  for (let i = 0; i < argv.length; i++) {
    const v = argv[i];
    if (v === "--target") a.target = argv[++i];
    else if (v === "--ref") a.ref = argv[++i];
    else if (v === "--k") a.k = parseInt(argv[++i], 10);
    else if (v === "--queries") a.queries = argv[++i].split(",").map((s) => s.trim()).filter(Boolean);
    else if (v === "--noise") a.noise = parseInt(argv[++i], 10);
    else if (v === "--gold") a.gold = argv[++i];
    else if (v === "--timeout") a.timeout = parseInt(argv[++i], 10);
  }
  return a;
}

async function tryConnect(url, timeoutMs) {
  try {
    return await new RelayConn(url, { timeoutMs }).connect();
  } catch (e) {
    console.log(`  skip ${url}: ${e.message}`);
    return null;
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.target) {
    console.error("error: --target <ws-url> is required (the vespa-relay to fill)");
    process.exit(2);
  }
  const refUrl = args.ref || BRAINSTORM_SEARCH;
  const K = args.k;
  const timeoutMs = args.timeout || 20000;
  const queries = queriesFrom(process.env, args.queries);

  const target = await new RelayConn(args.target, { timeoutMs }).connect();

  // --- 1. Reference snapshot: what brainstorm returns for each query. -------
  console.log(`# reference snapshot (${refUrl})`);
  const ref = await new RelayConn(refUrl, { timeoutMs }).connect();
  const byQuery = {};
  const goldPubkeys = new Set();
  for (const q of queries) {
    const r = await ref.req({ kinds: [0], search: q, limit: K }, { timeoutMs });
    const seen = new Set();
    const ordered = [];
    for (const e of r.events) {
      if (seen.has(e.pubkey)) continue;
      seen.add(e.pubkey);
      ordered.push(e.pubkey);
    }
    byQuery[q] = ordered.slice(0, K);
    byQuery[q].forEach((p) => goldPubkeys.add(p));
    console.log(`  ${q.padEnd(12)} ${byQuery[q].length} pubkeys`);
  }
  ref.close();
  console.log(`  -> ${goldPubkeys.size} distinct reference pubkeys to backfill\n`);

  // Collect events to ingest, newest-per-(pubkey,kind) wins.
  const collected = new Map(); // key -> event
  const keep = (e) => {
    if (!e || typeof e.id !== "string") return;
    const key = e.kind === 0 || e.kind === 3 ? `${e.kind}:${e.pubkey}` : e.id;
    const prev = collected.get(key);
    if (!prev || (e.created_at || 0) > (prev.created_at || 0)) collected.set(key, e);
  };

  // --- 2. Backfill gold pubkeys BY AUTHOR from the feed relays. --------------
  console.log(`# backfill ${goldPubkeys.size} pubkeys by author from ${ALL_SOURCES.length} feed relays`);
  const authorsAll = [...goldPubkeys];
  const perRelayCovered = {};
  for (const url of ALL_SOURCES) {
    const conn = await tryConnect(url, timeoutMs);
    if (!conn) continue;
    const found = new Set();
    const chunk = 100;
    for (let i = 0; i < authorsAll.length; i += chunk) {
      const authors = authorsAll.slice(i, i + chunk);
      const r = await conn.req({ kinds: [0, 3], authors, limit: authors.length * 2 }, { timeoutMs });
      for (const e of r.events) {
        keep(e);
        if (e.kind === 0) found.add(e.pubkey);
      }
    }
    conn.close();
    perRelayCovered[url] = found.size;
    console.log(`  ${url.padEnd(34)} ${found.size} kind:0`);
  }
  const gotProfile = new Set([...collected.values()].filter((e) => e.kind === 0).map((e) => e.pubkey));
  const coveredGold = [...goldPubkeys].filter((p) => gotProfile.has(p)).length;
  console.log(`  -> backfilled ${coveredGold}/${goldPubkeys.size} reference profiles from the feed\n`);

  // --- 3. Noise: bulk kind:0 sample, paginated back through time. -----------
  if (args.noise > 0) {
    console.log(`# noise sample: up to ${args.noise} kind:0 from feed relays`);
    const noiseRelays = [...SOURCES.popular, "wss://relay.ditto.pub"];
    let noiseCount = 0;
    for (const url of noiseRelays) {
      if (noiseCount >= args.noise) break;
      const conn = await tryConnect(url, timeoutMs);
      if (!conn) continue;
      let until = Math.floor(Date.now() / 1000);
      let relayGot = 0;
      for (let page = 0; page < 8 && noiseCount < args.noise; page++) {
        const r = await conn.req({ kinds: [0], until, limit: 500 }, { timeoutMs });
        if (!r.events.length) break;
        let minTs = until;
        for (const e of r.events) {
          keep(e);
          relayGot++;
          noiseCount++;
          if (e.created_at && e.created_at < minTs) minTs = e.created_at;
        }
        if (minTs >= until) break; // no progress
        until = minTs - 1;
      }
      conn.close();
      console.log(`  ${url.padEnd(34)} ${relayGot} events`);
    }
    console.log(`  -> ${noiseCount} noise events pulled\n`);
  }

  // --- 4. Publish everything into the target relay. -------------------------
  const events = [...collected.values()];
  const kinds = {};
  for (const e of events) kinds[e.kind] = (kinds[e.kind] || 0) + 1;
  console.log(`# ingest ${events.length} events into ${args.target}`);
  console.log(`  by kind: ${Object.entries(kinds).map(([k, v]) => `${k}:${v}`).join("  ")}`);
  let ok = 0;
  let rej = 0;
  let batch = [];
  for (const e of events) {
    batch.push(target.publish(e, { timeoutMs }).then((r) => (r.accepted ? ok++ : rej++)));
    if (batch.length >= 50) {
      await Promise.all(batch);
      batch = [];
    }
  }
  await Promise.all(batch);
  console.log(`  -> accepted ${ok}, rejected ${rej}`);

  target.close();

  const goldOut = {
    reference: refUrl,
    k: K,
    createdAt: Math.floor(Date.now() / 1000),
    coverageOfReference: goldPubkeys.size ? coveredGold / goldPubkeys.size : null,
    perRelayCovered,
    byQuery,
  };
  const goldPath = args.gold || "gold.json";
  writeFileSync(goldPath, JSON.stringify(goldOut, null, 2));
  console.log(`\nwrote reference snapshot -> ${goldPath}`);
  console.log(`give Vespa a few seconds to index, then run compare.mjs.`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
