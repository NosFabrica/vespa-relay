// What the pulse page decides: turning cumulative counters into rates, and the
// judgements that can be wrong silently. The page's DOM is not tested here;
// its arithmetic is, because a rate over the wrong window is a number an
// operator will act on and cannot see is wrong.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  ACTIVITY_LABELS, CHATTY_CALLS_PER_DOC, DOMINANT_SHARE, GAUGE_LABELS, OUTCOME_ORDER, POLL_MS,
  activityRowsOf, admissionOf, dominantOf, engineRowsOf, gaugesOf, locksOf,
  outcomeSplitOf, rateOf, showsClients, slowestOf, stageRowsOf, uncertain, whereOf, windowOf,
} from "../../main/resources/web/shared/pulse.js";

const ok = (name) => console.log(`  ✓ ${name}`);

/** A document as `PulseDocument` publishes one, with only what a case needs. */
const doc = (over = {}) => ({ schema: 1, uptimeSeconds: 100, clientDerived: false, ...over });

// ── the window every rate is measured over ──────────────────────────────────
{
  // The server's own clock, not the browser's: a reader whose clock is minutes
  // off would otherwise see every rate on the page scaled by the error.
  assert.equal(windowOf(doc({ uptimeSeconds: 110 }), doc({ uptimeSeconds: 100 })), 10);
  assert.equal(windowOf(doc(), null), null, "the first poll has no baseline and must show no rate");

  // THE RESTART. Uptime going backwards means every counter reset with it;
  // differencing across that would draw one enormous spike that never happened.
  assert.equal(windowOf(doc({ uptimeSeconds: 3 }), doc({ uptimeSeconds: 9_000 })), null);
  // Two polls inside the same second are not a window either.
  assert.equal(windowOf(doc({ uptimeSeconds: 100 }), doc({ uptimeSeconds: 100 })), null);
  ok("the rate window comes off the server's uptime, and a restart drops the baseline");
}

{
  assert.equal(rateOf(150, 100, 10), 5);
  assert.equal(rateOf(100, null, 10), null, "no previous value is no rate, not a rate of zero");
  assert.equal(rateOf(100, 100, null), null, "no window is no rate");
  // A counter that went backwards without uptime doing so (a ledger reset) is
  // clamped rather than drawn as a negative rate.
  assert.equal(rateOf(10, 100, 10), 0);
  ok("a rate needs both a baseline and a window, and is never negative");
}

// ── activities ──────────────────────────────────────────────────────────────
{
  const now = doc({
    uptimeSeconds: 110,
    activities: [
      { activity: "BatchInsert", calls: 300, ms: 9_000, docs: 60_000, ports: [{ call: "Put", calls: 20, ms: 8_000, docs: 60_000, callsPerDoc: 0.00033 }] },
      { activity: "Query", calls: 500, ms: 1_000, docs: 5_000, ports: [{ call: "Search", calls: 500, ms: 1_000, docs: 5_000, callsPerDoc: 0.1, p50Ms: 1.2, p99Ms: 40, measured: 500 }] },
    ],
  });
  const before = doc({
    uptimeSeconds: 100,
    activities: [
      { activity: "BatchInsert", calls: 200, ms: 5_000, docs: 40_000, ports: [{ call: "Put", calls: 10, ms: 4_500, docs: 40_000, callsPerDoc: 0.00025 }] },
      { activity: "Query", calls: 400, ms: 800, docs: 4_000, ports: [{ call: "Search", calls: 400, ms: 800, docs: 4_000, callsPerDoc: 0.1, p50Ms: 1.2, p99Ms: 40, measured: 400 }] },
    ],
  });
  const rows = activityRowsOf(now, before);

  assert.equal(rows[0].callsPerSec, 10, "100 more calls over a 10s window");
  assert.equal(rows[1].callsPerSec, 10);
  // The share is of PORT TIME — the store's wall time inside the engine calls —
  // which is the only total this page has. Against the engine's own internal
  // time it would be a different number and a different claim.
  assert.equal(rows[0].share, 0.9);
  assert.equal(rows[0].label, ACTIVITY_LABELS.BatchInsert);
  assert.equal(rows[0].ports[0].callsPerSec, 1);
  ok("activity rows carry their share of port time and a rate differenced from the last poll");

  // An activity the store adds and this page has no label for still draws.
  const novel = activityRowsOf(doc({ activities: [{ activity: "Vacuum", calls: 1, ms: 1, docs: 0, ports: [] }] }), null);
  assert.equal(novel[0].label, "Vacuum", "an unlabelled activity draws under its own name rather than vanishing");
  ok("an activity this page has never heard of is still drawn");

  assert.equal(dominantOf(rows).activity, "BatchInsert");
  assert.equal(dominantOf(activityRowsOf(doc({ activities: [
    { activity: "Query", calls: 1, ms: 50, docs: 1, ports: [] },
    { activity: "Count", calls: 1, ms: 50, docs: 1, ports: [] },
  ] }), null)), null, `an even split names no dominant activity (the bar is ${DOMINANT_SHARE})`);
  ok("one activity is called out only when it really is taking most of the time");
}

{
  // A process that has done nothing has no shares, not shares of zero: 0/0 as
  // a bar would draw every activity at nothing and read as a bug in the page.
  const rows = activityRowsOf(doc({ activities: [{ activity: "Query", calls: 3, ms: 0, docs: 0, ports: [] }] }), null);
  assert.equal(rows[0].share, null);
  ok("no time booked yet is no share, not a share of zero");
}

{
  // The store's own contract ("never ingest in a loop over insert()") as a
  // number: a port booking several calls per document is the shape that broke it.
  const rows = activityRowsOf(doc({ activities: [{ activity: "Insert", calls: 12_000, ms: 60_000, docs: 1_000, ports: [
    { call: "Search", calls: 11_000, ms: 50_000, docs: 1_000, callsPerDoc: 11 },
    { call: "Put", calls: 1_000, ms: 10_000, docs: 1_000, callsPerDoc: 1 },
  ] }] }), null);
  assert.equal(rows[0].ports[0].chatty, true, `11 calls per document is over the bar of ${CHATTY_CALLS_PER_DOC}`);
  assert.equal(rows[0].ports[1].chatty, false, "one call per document is a write, not a symptom");
  ok("a chatty port is flagged at the ratio the store's own contract is written in");
}

// ── percentiles ─────────────────────────────────────────────────────────────
{
  // THE BUG THIS IS WRITTEN AGAINST. A write shape keeps no histogram; the
  // document publishes no percentile for it, and the page must not fill the
  // hole with a zero — "p99 0ms" reads as instant when it means unmeasured.
  const worst = slowestOf(doc({ activities: [
    { activity: "BatchInsert", calls: 1, ms: 9_000, docs: 1, ports: [{ call: "Put", calls: 1, ms: 9_000, docs: 1, callsPerDoc: 1 }] },
    { activity: "Query", calls: 1, ms: 5, docs: 1, ports: [{ call: "Search", calls: 1, ms: 5, docs: 1, callsPerDoc: 1, p50Ms: 1, p99Ms: 40, measured: 900 }] },
  ] }));
  assert.equal(worst.call, "Search", "the slowest MEASURED shape, not the slowest total");
  assert.equal(worst.p99Ms, 40);

  assert.equal(slowestOf(doc({ activities: [{ activity: "BatchInsert", calls: 1, ms: 9_000, docs: 1, ports: [{ call: "Put", calls: 1, ms: 9_000, docs: 1, callsPerDoc: 1 }] }] })), null,
    "nothing measured is null, so the tile can say so instead of drawing a zero");
  ok("the headline p99 comes from a measured shape or from nowhere");
}

// ── admission ───────────────────────────────────────────────────────────────
{
  const a = admissionOf(doc({ uptimeSeconds: 110, outcomes: { admitted: 190, offered: 1_000 } }),
                        doc({ uptimeSeconds: 100, outcomes: { admitted: 90, offered: 600 } }));
  assert.equal(a.share, 0.19);
  assert.equal(a.admittedPerSec, 10);
  assert.equal(a.offeredPerSec, 40, "the gap between the two is what says to narrow a sync");

  // A fresh process has no admission rate. Drawing 0% would read as a store
  // refusing everything, which is the opposite of the truth.
  assert.equal(admissionOf(doc({ outcomes: { admitted: 0, offered: 0 } }), null), null);
  assert.equal(admissionOf(doc(), null), null);
  ok("admission is a share of what was offered, and nothing offered is no share");
}

{
  const split = outcomeSplitOf({ activity: "BatchInsert", offered: 1_000, reasons: [
    { reason: "duplicate", events: 700 }, { reason: "admitted", events: 200 }, { reason: "replaced", events: 100 },
  ] });
  assert.deepEqual(split.reasons.map((r) => r.reason), ["admitted", "duplicate", "replaced"],
    "read in the page's order — what got in, then why the rest did not");
  assert.equal(split.reasons[1].share, 0.7);
  assert.deepEqual(OUTCOME_ORDER[0], "admitted");

  // A reason the store adds sorts after the known ones rather than vanishing.
  const novel = outcomeSplitOf({ activity: "Insert", offered: 10, reasons: [{ reason: "quarantined", events: 4 }, { reason: "admitted", events: 6 }] });
  assert.deepEqual(novel.reasons.map((r) => r.reason), ["admitted", "quarantined"]);
  ok("outcome reasons are ordered for reading, and an unknown reason still draws");
}

// ── the engine ──────────────────────────────────────────────────────────────
{
  const rows = engineRowsOf(doc({ uptimeSeconds: 110, engine: [
    { profile: "trusted", queries: 200, engineMs: 4_000, summaryMs: 500, docsMatched: 80_000, hitsServed: 2_000, degraded: 1, rungs: 12 },
  ] }), doc({ uptimeSeconds: 100, engine: [
    { profile: "trusted", queries: 100, engineMs: 2_000, summaryMs: 200, docsMatched: 40_000, hitsServed: 1_000, degraded: 0, rungs: 6 },
  ] }));
  assert.equal(rows[0].queriesPerSec, 10);
  assert.equal(rows[0].meanEngineMs, 20);
  assert.equal(rows[0].servedShare, 0.025, "2.5% of what the engine matched reached a client");
  assert.equal(rows[0].matchedPerQuery, 400);

  // Nothing matched is not "0% served": a query that matched nothing and a
  // relay that ran no queries are different facts and must not draw alike.
  assert.equal(engineRowsOf(doc({ engine: [{ profile: "text", queries: 3, engineMs: 1, summaryMs: 0, docsMatched: 0, hitsServed: 0, degraded: 0, rungs: 0 }] }), null)[0].servedShare, null);
  ok("the engine's matched-against-served is a share, and nothing matched is no share");
}

// ── locks: the causal edge ──────────────────────────────────────────────────
{
  const { held, wait } = locksOf(doc({ locks: {
    held: [{ stage: "lock.gate.hold", heldMs: 20_400, doing: "derive 500 subject(s)", mutex: "trustGate" }],
    wait: [{ stage: "lock.ingest.wait", ms: 40_000, behind: [
      { holder: "derive 500 subject(s)", ms: 38_000 }, { holder: "write", ms: 2_000 },
    ] }],
  } }));
  assert.equal(held[0].heldSec, 20);
  // The whole reason the `behind` member exists: `lock.ingest.wait 40s` only
  // prompts a question; "95% of it behind derive" names a fix.
  assert.equal(wait[0].behind[0].share, 0.95);
  assert.equal(wait[0].behind[1].share, 0.05);

  // A wait attributed to nobody must not divide by zero into NaN — that is
  // exactly the state the store was in before the mutex-identity fix.
  const orphan = locksOf(doc({ locks: { wait: [{ stage: "lock.gate.wait", ms: 0, behind: [{ holder: "?", ms: 0 }] }] } }));
  assert.equal(orphan.wait[0].behind[0].share, 0);
  assert.deepEqual(locksOf(doc()), { held: [], wait: [] });
  ok("lock wait is split by what was holding, and an unattributed wait draws zero rather than NaN");
}

// ── the write path ──────────────────────────────────────────────────────────
{
  const rows = stageRowsOf(
    doc({ uptimeSeconds: 110, stages: [{ stage: "proj.fetch.derive", ms: 20_000, calls: 12, meanMs: 1_666, maxMs: 9_000 }, { stage: "lock.ingest.wait", ms: 5_000 }] }),
    doc({ uptimeSeconds: 100, stages: [{ stage: "proj.fetch.derive", ms: 10_000, calls: 6, meanMs: 1_666, maxMs: 9_000 }, { stage: "lock.ingest.wait", ms: 4_000 }] }),
  );
  // Seconds of work per second of wall clock. 1.0 saturates one thread; above
  // 1.0 is concurrency and not an error, which is why this is not a percentage.
  assert.equal(rows[0].busy, 1, "10s of stage time over a 10s window is one saturated thread");
  assert.equal(rows[1].busy, 0.1);
  assert.equal(rows[1].calls, undefined, "a lock's wait/hold pair carries no call count, and no mean can be made from one");
  assert.equal(stageRowsOf(doc({ stages: [{ stage: "x", ms: 1 }] }), null)[0].busy, null, "and no baseline is no busy figure");
  ok("stage busy-ness is work per wall second, and a stage without calls invents no mean");
}

// ── what a document says about itself ───────────────────────────────────────
{
  // Read from the flag, never inferred from the arrays: a build that serves no
  // client sections and a relay nobody has searched yet both publish nothing,
  // and only the flag tells them apart.
  assert.equal(showsClients(doc({ clientDerived: true })), true);
  assert.equal(showsClients(doc({ clientDerived: false, hotspots: { observers: [], terms: [] } })), false);
  assert.equal(showsClients(doc({})), false, "a document that does not say is treated as not saying it");
  ok("the client sections are announced by the document, not guessed from its shape");
}

{
  // Every YQL this store emits opens with the same projection, so a column of
  // them truncates to the same forty characters and says nothing. What differs
  // between two slow reads is the predicate.
  assert.equal(
    whereOf('select id, pubkey, created_at, kind, tags, content, sig, owner from event where kind in (1) order by created_at desc'),
    "kind in (1) order by created_at desc",
  );
  // A shape this does not recognise must still be readable, not blank.
  assert.equal(whereOf("select * from event"), "select * from event");
  assert.equal(whereOf(""), "");
  assert.equal(whereOf(undefined), "");
  ok("a slow read's query is shown by its predicate, and an unrecognised one is shown whole");
}

{
  // Space-Saving overestimates and publishes by how much; a row whose error is
  // most of its own weight may not belong in the list at all.
  assert.equal(uncertain({ key: "a", weight: 100, error: 90 }), true);
  assert.equal(uncertain({ key: "b", weight: 100, error: 2 }), false);
  assert.equal(uncertain({ key: "c", weight: 0, error: 0 }), false, "a zero-weight row is not 'uncertain', it is empty");
  ok("a sketch row worth doubting is flagged from the bound the sketch itself publishes");
}

{
  // Labelled for reading, but a gauge the store adds and this page has never
  // heard of still draws — under its own key, rather than vanishing.
  const labelled = gaugesOf(doc({ gauges: [{ gauge: "feed.inflight", value: 3 }, { gauge: "vacuum.depth", value: 9 }] }));
  assert.equal(labelled[0].label, GAUGE_LABELS["feed.inflight"]);
  assert.equal(labelled[1].label, "vacuum.depth");
  assert.deepEqual(gaugesOf(doc()), [], "no gauges is an empty list, and a gauge is never differenced");
  assert.ok(POLL_MS > 0 && POLL_MS <= 5_000, "the page polls fast enough that a spike is visible");
  ok("gauges are reached by their own accessor, apart from every counter");
}

// ── the glossary the page and the document have to agree on ─────────────────
{
  // A member renamed in Kotlin empties a panel with no failure in either
  // language. These are the members this page reads by name.
  const kt = readFileSync(new URL("../../../../common/src/main/kotlin/com/nosfabrica/vespa/relay/pulse/PulseDocument.kt", import.meta.url), "utf8");
  for (const member of [
    "activities", "ports", "callsPerDoc", "p50Ms", "p99Ms", "measured",
    "outcomes", "admitted", "offered", "byActivity", "reasons",
    "engine", "docsMatched", "hitsServed", "degraded", "rungs",
    "gauges", "locks", "held", "wait", "behind", "holder", "mutex",
    "stages", "hotspots", "slowReads", "clientDerived", "uptimeSeconds",
  ]) {
    assert.ok(kt.includes(`"${member}"`), `the page reads doc.${member} and PulseDocument.kt no longer publishes it`);
  }
  ok("every member this page reads is one the Kotlin builder still writes");
}

console.log("pulse.test.mjs — all assertions passed");
