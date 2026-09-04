// What the sync card decides: the half that can be wrong silently. The only
// other pins are string greps for member names, which cannot catch a bar
// against the wrong denominator. Each assertion is written in the direction
// its bug failed.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  IN_FLIGHT_SHOWN, MEASURING, POOL_NEGENTROPY, POOL_BETWEEN, POOL_CATCHING_UP,
  POOL_LIVE, POOL_ORDER, POOL_REFETCHING, ROTATING, STUCK_LEG_SEC, constraintOf,
  JOB_VISITING, POOL_LABELS, funnelOf, heldOf, legsOf, limitsOf, measuringOf,
  STARTING, STAGES_SHOWN, STUCK_CALL_SEC, CALLS_SHOWN, jobsOf, poolsOf, probeProgress, rotationOf, scheduleOf, socketsOf,
  stageDeltas, storeOf, streamSections, relayStatusOf, SYNC_STATUSES, FRESHNESS,
} from "../../main/resources/web/shared/sync.js";

const ok = (name) => console.log(`  ✓ ${name}`);

/** A leg as `RelayRotation.held` publishes one. */
const leg = (n, quiet, over = {}) => ({
  relay: `wss://r${n}.example/`, heldForSec: 3600, transferringForSec: 3595,
  events: 1000 * n, quietForSec: quiet, ...over,
});

// ── the words this page and the router have to agree on ─────────────────────
{
  // `VisitPoolTest` binds the pool words to the document's glossary, and
  // nothing bound them to this file: a rename in Kotlin emptied a panel with
  // no failure in either language. The `const val` literals are the contract.
  const pool = readFileSync(new URL("../../../../sync/src/main/kotlin/com/nosfabrica/vespa/relay/sync/VisitPool.kt", import.meta.url), "utf8");
  const declared = Object.fromEntries(
    [...pool.matchAll(/const val (POOL_[A-Z_]+|JOB_[A-Z_]+) = "([^"]+)"/g)].map((m) => [m[1], m[2]]),
  );
  assert.deepEqual(
    { POOL_LIVE: declared.POOL_LIVE, POOL_CATCHING_UP: declared.POOL_CATCHING_UP,
      POOL_REFETCHING: declared.POOL_REFETCHING, POOL_NEGENTROPY: declared.POOL_NEGENTROPY,
      JOB_VISITING: declared.JOB_VISITING },
    { POOL_LIVE, POOL_CATCHING_UP, POOL_REFETCHING, POOL_NEGENTROPY, JOB_VISITING },
    "the router's pool words and this page's have drifted — one of the four tables is about to draw empty",
  );
  // A fifth pool added in Kotlin would otherwise land silently in `between`.
  for (const word of Object.values(declared)) {
    assert.ok(POOL_LABELS[word], `the router publishes pool word "${word}" and this page has no label for it`);
  }
  ok("the four pool words are the router's own, read out of its source");
}

// ── the socket budget ───────────────────────────────────────────────────────
{
  // Near the ceiling is not a fault: a mirror is supposed to sit near its budget.
  const busy = socketsOf({ sockets: 1010, socketCeiling: 1024, socketsRunning: 1010, socketsQueued: 0 });
  assert.equal(busy.starved, false, "99% of the budget with nothing waiting is a mirror doing its job");
  assert.equal(busy.open, 1010);
  assert.equal(busy.ceiling, 1024);

  // A queue is: OkHttp holds admissible calls because the budget is full,
  // which a slow store cannot produce.
  const starved = socketsOf({ sockets: 1024, socketCeiling: 1024, socketsRunning: 1024, socketsQueued: 37 });
  assert.equal(starved.starved, true);
  assert.equal(starved.queued, 37);

  // The queue, not the fullness.
  assert.equal(socketsOf({ sockets: 40, socketCeiling: 1024, socketsQueued: 2 }).starved, true);

  // A router too old to publish the queue says nothing rather than "nothing is queued".
  const old = socketsOf({ sockets: 412, socketCeiling: 1024 });
  assert.equal(old.queued, null);
  assert.equal(old.running, null);
  assert.equal(old.starved, false, "unmeasured cannot raise an alarm, and must not");

  // No ceiling is no mark: `0 of 0` is worse than nothing.
  assert.equal(socketsOf({ sockets: 5 }), null);
  assert.equal(socketsOf({ socketCeiling: 1024 }), null);
  assert.equal(socketsOf(null), null);
  ok("the socket budget is read on its queue, not on how full it is");
}

// ── the constraint ──────────────────────────────────────────────────────────
{
  // The relay rebuilds `health` against an allowlist, so an older router
  // publishes `{}`; guarded on the object, the card drew an empty chip.
  assert.equal(constraintOf({}), null, "an empty health object names no constraint");
  assert.equal(constraintOf(null), null);
  assert.equal(constraintOf({ eventsPerSec: 900 }), null, "gauges are not a verdict");

  const c = constraintOf({ bottleneck: "ingest" });
  assert.equal(c.text, "ingest is the limit");
  assert.equal(c.tone, "warn", "ingest is the only one of the four that is a fault");
  assert.match(c.why, /not at the relays/);
  assert.equal(constraintOf({ bottleneck: "downloads" }).tone, null, "the relays being the limit is not a fault");

  // A word this page has not been taught still says something: the card is served to whoever asks.
  assert.equal(constraintOf({ bottleneck: "novel" }).text, "novel");
  ok("the constraint is guarded on its own member, and names an unknown word rather than dropping it");
}

// ── the legs ────────────────────────────────────────────────────────────────
{
  // The bar is a proportion of the threshold. Scaled by the worst row
  // published, an outlier flattened every bar; scaled by the worst row shown,
  // five healthy legs all rendered full.
  const healthy = legsOf({ relays: [1, 2, 3, 4, 5].map((n) => leg(n, 30)), omitted: 0 });
  assert.deepEqual(healthy.rows.map((r) => r.quietShare), [0.05, 0.05, 0.05, 0.05, 0.05],
    "five healthy legs read as five healthy legs, not as five full bars");
  assert.equal(healthy.rows.every((r) => !r.hot), true);

  const wedged = legsOf({ relays: [leg(1, 30), leg(2, STUCK_LEG_SEC)], omitted: 0 });
  assert.equal(wedged.rows[1].quietShare, 1);
  assert.equal(wedged.rows[1].hot, true, "the threshold itself is stuck");
  // Past the threshold the bar must not overflow its track.
  assert.equal(legsOf({ relays: [leg(1, STUCK_LEG_SEC * 60)] }).rows[0].quietShare, 1);
  ok("the quiet bar is a proportion of the threshold, so an absolute reading survives its neighbours");
}

{
  // The page no longer cuts the list: the router's cap is the only one, and
  // `IN_FLIGHT_SHOWN` defers to the document.
  const many = legsOf({ relays: Array.from({ length: 8 }, (_, i) => leg(i, 30)), omitted: 12 });
  assert.equal(many.rows.length, 8, "every leg the document names is drawn");
  assert.equal(many.more, 12, "what the ROUTER left out is still disclosed");
  assert.equal(IN_FLIGHT_SHOWN, Infinity, "the default defers to the document rather than re-cutting it");
  // A caller that does pass a limit still adds what it drops.
  const capped = legsOf({ relays: Array.from({ length: 8 }, (_, i) => leg(i, 30)), omitted: 12 }, 5);
  assert.equal(capped.rows.length, 5);
  assert.equal(capped.more, 15, "what this side drops is ADDED to what the router already left out");
  assert.equal(legsOf({ relays: [leg(1, 30)], omitted: 0 }).more, 0);
  assert.deepEqual(legsOf(null), { rows: [], more: 0 });

  // The scheme is dropped and nothing else: a truncated relay url is the thing being looked up.
  assert.equal(legsOf({ relays: [leg(1, 0, { relay: "wss://a.example/path" })] }).rows[0].short, "a.example/path");
  assert.equal(legsOf({ relays: [leg(1, 0, { relay: "ws://a.example/" })] }).rows[0].short, "a.example/");

  // Absent `transferringForSec` is "not on a socket at all", our pool rather than their relay.
  assert.equal(legsOf({ relays: [leg(1, 30, { transferringForSec: null })] }).rows[0].slotless, true);
  assert.equal(legsOf({ relays: [leg(1, 30, { transferringForSec: 0 })] }).rows[0].slotless, false,
    "zero seconds on a slot is on a slot");
  ok("the tail is disclosed, and a leg with no slot is told from a slow one");
}

// ── how far a probe pass got ────────────────────────────────────────────────
{
  // The document publishes `unmeasured`, and this returns its complement;
  // backwards, a fold that decided nothing reads as nearly finished.
  const fold = (over = {}) => ({
    name: "aliasFold", phase: "idle", lastPassSec: 6354,
    streams: [{ name: "all streams", candidates: 11693, unmeasured: 7546 }], ...over,
  });
  assert.equal(probeProgress(fold()).checked, 4147, "checked is candidates MINUS unmeasured");
  assert.equal(probeProgress(fold()).candidates, 11693, "the denominator is the candidate set");

  // A folded url is another relay's second address, never dialled, so it leaves both halves.
  const gate = probeProgress({
    name: "consistency", phase: "idle",
    streams: [{ candidates: 16752, foldedAway: 11429, consistent: 583, inconsistent: 12, unmeasured: 4728 }],
  });
  assert.equal(gate.candidates, 5323, "the folded urls leave the denominator");
  assert.equal(gate.checked, 595, "…and the numerator, where they are exactly the two verdict counts");
  // The fold's own row measures no folds away from itself, so nothing moves.
  assert.equal(probeProgress(fold()).checked, 4147, "a row with no partition is the plain complement");
  assert.equal(probeProgress(fold({ streams: [{ candidates: 40, unmeasured: 0 }] })).checked, 40);
  assert.equal(probeProgress(fold({ streams: [{ candidates: 40, unmeasured: 40 }] })).checked, 0);

  // `SyncProgressReport` defaults `unmeasured` to `candidates` on an unreadable
  // row, so a bad read lands on zero checked and must never land below it.
  assert.equal(probeProgress(fold({ streams: [{ candidates: 10, unmeasured: 99 }] })).checked, 0);

  // Summed, not `streams[0]`.
  const two = probeProgress(fold({ streams: [{ candidates: 17000, unmeasured: 9000 }, { candidates: 16, unmeasured: 4 }] }));
  assert.equal(two.candidates, 17016);
  assert.equal(two.checked, 8012, "both rows counted, not the first one");
  assert.equal(probeProgress(fold({ streams: [{ candidates: 10 }, {}] })).checked, 10,
    "a missing member is a zero on its row, not a NaN across the total");
  assert.equal(probeProgress(fold()).newOnly, false, "a row that does not count new urls says so");
  ok("the pass draws what HAS a verdict, summed across rows and never negative");
}

{
  // Where the row says what arrived undecided, that is the denominator:
  // month-old verdicts nothing will re-ask cannot move the position.
  const fresh = probeProgress({
    name: "aliasFold", phase: "idle", lastPassSec: 660,
    streams: [{ name: "all streams", candidates: 11693, newUrls: 1754, unmeasured: 1611 }],
  });
  assert.equal(fresh.candidates, 1754, "the denominator is what arrived with no verdict");
  assert.equal(fresh.checked, 143, "…and the numerator is how many of THOSE left with one");
  assert.equal(fresh.newOnly, true, "the page has a word to add");

  // Presence, not truthiness: a fold that has caught up publishes zero, and
  // falling back to `candidates` would put the whole corpus back exactly when
  // the pass is done. The card branches on this pair too (`PROBE_NONE`).
  const caught = probeProgress({
    name: "aliasFold", phase: "idle",
    streams: [{ candidates: 11693, newUrls: 0, unmeasured: 0 }],
  });
  assert.equal(caught.candidates, 0);
  assert.equal(caught.newOnly, true);

  // A row that omits it counts zero rather than dragging the document back to the old denominator.
  const mixed = probeProgress({
    name: "aliasFold", phase: "idle",
    streams: [{ candidates: 100, newUrls: 40, unmeasured: 30 }, { candidates: 16, unmeasured: 4 }],
  });
  assert.equal(mixed.candidates, 40);
  assert.equal(mixed.checked, 6, "34 unmeasured across both rows, against 40 new");
  ok("a pass that counts what arrived undecided is drawn against that, and says so");
}

{
  // The clock belongs to the last pass that finished.
  const row = (over) => ({ name: "aliasFold", streams: [{ candidates: 10, unmeasured: 2 }], ...over });
  assert.equal(probeProgress(row({ phase: "idle", lastPassSec: 6354 })).tookSec, 6354);
  assert.equal(probeProgress(row({ phase: MEASURING, lastPassSec: 6354 })).tookSec, null);
  assert.equal(MEASURING, "measuring", "the word `Processors.MEASURING` publishes");

  // Before the first pass lands there is no duration, an absence rather than a zero.
  assert.equal(probeProgress(row({ phase: "idle" })).tookSec, null);
  assert.equal(probeProgress(row({ phase: "starting" })).tookSec, null);
  assert.equal(probeProgress(row({ phase: "idle", lastPassSec: 0 })).tookSec, 0, "a pass under a second still ran");

  // Ingest and the healer come through the same renderer and must fall past it.
  assert.equal(probeProgress({ name: "ingest", queued: 8304, capacity: 8192 }), null);
  assert.equal(probeProgress({ name: "aliasFold", streams: [] }), null);
  assert.equal(probeProgress(null), null);
  ok("the duration is the last FINISHED pass, absent while one runs and before the first");
}

// ── where the pass running right now has got to ──────────────────────────────
{
  // `probeProgress` reads the row the last pass left, which stands still for
  // the hours the next one takes.
  const gate = (over) => ({ name: "consistency", phase: MEASURING, measuring: over });

  const run = measuringOf(gate({ unit: "url", attempted: 604, toProbe: 4728, etaSec: 2724 }));
  assert.equal(run.attempted, 604);
  assert.equal(run.toProbe, 4728, "the denominator is what this PASS set out to walk, not the candidate set");
  assert.equal(run.etaSec, 2724);

  // No denominator is no position; the phase word alone is the better draw.
  assert.equal(measuringOf(gate({ unit: "url", attempted: 0, toProbe: 0 })), null);
  assert.equal(measuringOf({ name: "consistency", phase: MEASURING }), null, "a router too old to publish one says nothing");
  assert.equal(measuringOf({ name: "ingest", queued: 12, capacity: 20000 }), null, "the counter-shaped rows fall past it");
  assert.equal(measuringOf(null), null);

  // The estimate is withheld until a unit has landed and once the last one
  // has; a zero would claim the pass is finished.
  assert.equal(measuringOf(gate({ unit: "url", attempted: 0, toProbe: 4728 })).etaSec, null);
  assert.equal(measuringOf(gate({ unit: "url", attempted: 4728, toProbe: 4728 })).etaSec, null);
  assert.equal(measuringOf(gate({ unit: "url", attempted: 12, toProbe: 4728, etaSec: 0 })).etaSec, 0,
    "a real zero from the router is a pass about to end, not a missing estimate");

  // Read off a live pass, so the two halves can be a tick apart.
  assert.equal(measuringOf(gate({ unit: "host", attempted: 99, toProbe: 10 })).attempted, 10);
  assert.equal(measuringOf(gate({ unit: "host", attempted: -4, toProbe: 10 })).attempted, 0);

  // The unit is the router's: the fold decides a host and dials every url of one.
  assert.equal(measuringOf(gate({ unit: "host", attempted: 37, toProbe: 214 })).unit, "host");
  assert.equal(measuringOf(gate({ attempted: 1, toProbe: 2 })).unit, "url", "a row with no unit still renders a sentence");
  ok("the pass in flight publishes both halves of its position, and no estimate it has not earned");
}

// ── a pass that has stopped, and the url that stopped it ────────────────────
{
  // A pass wedged on one url agrees with itself on every number;
  // `quietForSec` is what separates stalled from about to finish.
  const stalled = measuringOf({
    name: "fitness", phase: MEASURING,
    measuring: { unit: "url", attempted: 12373, toProbe: 12374, etaSec: 0, quietForSec: 4454 },
  });
  assert.equal(stalled.etaSec, 0, "the estimate is still what the router sent");
  assert.equal(stalled.quietForSec, 4454, "…and this is the member that separates the two readings");

  // Absent is a router that predates the member; a zero would read as the healthy case.
  assert.equal(measuringOf({ measuring: { unit: "url", attempted: 6, toProbe: 22 } }).quietForSec, null);
  assert.equal(measuringOf({ measuring: { unit: "url", attempted: 6, toProbe: 22, quietForSec: 0 } }).quietForSec, 0);

  const held = (n, sec, over = {}) => ({ relay: `wss://r${n}.example/`, heldForSec: sec, stage: "ask ladder", ...over });

  // The router sorts longest-held first, so the front of the list is the answer.
  const rows = heldOf({ relays: [held(1, 4454), held(2, 12)], omitted: 0 }).rows;
  assert.equal(rows[0].relay, "wss://r1.example/");
  assert.equal(rows[0].heldForSec, 4454);
  assert.equal(rows[0].stage, "ask ladder");
  // The scheme goes and nothing else does.
  assert.equal(heldOf({ relays: [held(1, 9, { relay: "wss://a.example/path" })] }).rows[0].short, "a.example/path");

  // A step the page has not been taught reads as "not known", never as a step.
  assert.equal(heldOf({ relays: [held(1, 9, { stage: undefined })] }).rows[0].stage, null);

  // Cut, and it says so: a probe pass holds hundreds of urls, unlike a stream's legs.
  const many = heldOf({ relays: Array.from({ length: 9 }, (_, i) => held(i, 30)), omitted: 4 }, 3);
  assert.equal(many.rows.length, 3);
  assert.equal(many.more, 10, "what the router left out plus what this cut");
  assert.deepEqual(heldOf(null), { rows: [], more: 0 });
  assert.deepEqual(heldOf({ relays: [] }), { rows: [], more: 0 }, "a pass holding nothing draws nothing");
  ok("a stalled pass is told from one about to finish, and the url holding it is named");
}

// ── what a rotating stream is riding ────────────────────────────────────────
{
  // A visit stream has no pass, fraction or cycle, so without this the row
  // read the same riding four hundred relays or none.
  const riding = rotationOf({ name: "visits", phase: ROTATING, roster: 412, liveHeld: 300 });
  assert.equal(riding.roster, 412);
  assert.equal(riding.tails, 300);
  assert.equal(riding.waiting, false);

  // Zero is the reading worth having: before the first `prime`, a visit stream dials nothing.
  assert.equal(rotationOf({ phase: ROTATING, roster: 0, liveHeld: 0 }).waiting, true);
  assert.equal(rotationOf({ phase: ROTATING, roster: 0 }).tails, null, "no live count is not a claim of none");

  // Every other phase draws the marks it already had.
  assert.equal(rotationOf({ phase: "fetching", running: 128 }), null);
  assert.equal(rotationOf({ phase: ROTATING }), null, "a router too old to publish the pair says nothing");
  assert.equal(rotationOf(null), null);
  assert.equal(ROTATING, "rotating", "the word `StreamPhases.Phase.Rotating` publishes");
  ok("a rotating stream says what it is riding, and an empty roster says so loudest");
}

// ── where a paging leg's cursor is ──────────────────────────────────────────
{
  assert.equal(legsOf({ relays: [leg(1, 30)] }).rows[0].pagingUntil, null, "no walk running is no cursor");
  assert.equal(legsOf({ relays: [leg(1, 30, { pagingUntil: 1689857148 })] }).rows[0].pagingUntil, 1689857148);

  // `created_at = 0` is a real second and the deepest a walk can reach;
  // falsy-coalescing it erases the position that proves a walk got there.
  assert.equal(legsOf({ relays: [leg(1, 30, { pagingUntil: 0 })] }).rows[0].pagingUntil, 0,
    "the epoch is a position, not a missing cursor");
  ok("a paged cursor is carried per leg, and second zero is a position rather than an absence");
}

// ── names off the wire are not property lookups ─────────────────────────────
{
  // `bottleneck` is free text; reaching Object.prototype hands back a function
  // and destructuring one throws the render away.
  for (const hostile of ["constructor", "toString", "__proto__", "hasOwnProperty"]) {
    const c = constraintOf({ bottleneck: hostile });
    assert.equal(c.text, hostile, `${hostile} is an unknown word, not a prototype member`);
    assert.equal(c.why, "");
  }
  ok("a bottleneck word this page has not been taught cannot reach Object.prototype");
}

// ── the candidate set, as a tree ────────────────────────────────────────────
{
  // A live-shaped document, holding the identities
  // `sourced = excluded + heldOutDead + candidates` and
  // `candidates = foldedAway + consistent + inconsistent + unmeasured`.
  const gate = (over = {}, row = {}) => ({
    name: "consistency", phase: "idle", lastPassSec: 9720,
    sourced: 17584, excluded: 3, heldOutDead: 829,
    streams: [{
      name: "all streams", candidates: 16752, foldedAway: 11429, consistent: 583, inconsistent: 12,
      unmeasured: 4728, dialled: 4728, decided: 74,
      undecided: {
        reasons: [
          { reason: "never answered a REQ", urls: 3902, hosts: 2201,
            top: [{ host: "dead.example", urls: 61 }, { host: "gone.example", urls: 44 }] },
          { reason: "too few events to judge on", urls: 826, hosts: 611,
            top: [{ host: "thin.example", urls: 12 }] },
        ],
        omitted: 0,
      },
      ...row,
    }],
    ...over,
  });

  const f = funnelOf(gate());
  const at = (key) => f.rows.find((r) => r.key === key);
  assert.equal(f.total, 17584, "the root is every relay url this router knows of");

  // Depth is the relationship, so it is the thing to assert.
  assert.deepEqual(f.rows.map((r) => [r.depth, r.key]), [
    [0, "corpus"],
    [1, "dropped"], [2, "excluded"], [2, "heldOutDead"],
    [1, "candidates"], [2, "foldedAway"], [2, "consistent"], [2, "inconsistent"], [2, "unmeasured"],
    [3, "never answered a REQ"],
    [3, "too few events to judge on"],
  ]);

  // A reason is a leaf: one row per host would be one row per server on a corpus of thousands.
  assert.equal(f.rows.some((r) => r.key === "dead.example"), false, "no row per host");
  // What that list was for survives as two numbers: the same urls at 61 per
  // host and at 3,000 per host are different findings.
  assert.equal(at("never answered a REQ").hosts, 2201);
  assert.equal(at("never answered a REQ").largest, 61, "the widest host's share, not a list of them");
  assert.deepEqual(at("never answered a REQ").examples, ["dead.example", "gone.example"],
    "…and the names ride along for the row's title, which costs no height");
  assert.equal(at("never answered a REQ").unnamed, 2199,
    "the names it did NOT fit are a count, because 2,201 hosts arrived as two names");

  // Every bar against the root, never the parent, or a deep host would draw at the width the corpus gets.
  assert.equal(at("candidates").share, 16752 / 17584);
  assert.equal(at("never answered a REQ").share, 3902 / 17584);

  // A `│` is drawn at every ancestor that still has a sibling below it, which
  // computing from depth alone loses.
  assert.equal(at("dropped").prefix, "├─ ");
  assert.equal(at("excluded").prefix, "│  ├─ ", "inside a branch that is not the last");
  assert.equal(at("heldOutDead").prefix, "│  └─ ");
  assert.equal(at("candidates").prefix, "└─ ", "the last child of the root");
  assert.equal(at("unmeasured").prefix, "   └─ ", "…so nothing is drawn below it");
  assert.equal(at("never answered a REQ").prefix, "      ├─ ", "its sibling is below it, so the trunk continues");
  assert.equal(at("too few events to judge on").prefix, "      └─ ");

  // Tones are claims, and only one row on the whole tree is a fault.
  assert.equal(at("consistent").tone, "good");
  assert.equal(at("inconsistent").tone, "warn");
  assert.equal(at("never answered a REQ").tone, null, "a relay that will not answer is not our fault");
  assert.equal(at("never answered a REQ").hosts, 2201, "the url count's resolution into servers rides along");
  ok("the tree nests by depth, guides its own branches, and scales every bar to the root");
}

{
  // A url leaves the relay lists and every measurement of it stays in the
  // store, so rooted at `sourced` alone the tree lost most of its corpus.
  const shrunk = (over = {}) => funnelOf({
    name: "consistency", sourced: 1754, excluded: 4, heldOutDead: 50, recordedOnly: 15830,
    streams: [{ candidates: 1700, foldedAway: 600, consistent: 100, inconsistent: 0, unmeasured: 1000 }],
    ...over,
  });
  const f = shrunk();
  const at = (key) => f.rows.find((r) => r.key === key);
  assert.equal(f.total, 17584, "the mouth is what was named PLUS what only our records know");
  assert.equal(at("recordedOnly").value, 15830);
  assert.equal(at("recordedOnly").depth, 1, "a sibling of the candidate set, not a slice of it");
  assert.equal(at("recordedOnly").tone, "mute", "nothing was decided against them — nobody asked");
  // The three children still divide the root exactly once.
  assert.equal(at("dropped").value + at("recordedOnly").value + at("candidates").value, f.total);
  assert.equal(f.rows.some((r) => r.key === "unattributed"), false, "the partition still closes");

  // A router older than this member must not be shown a zero row claiming it measured that corpus.
  const old = shrunk({ recordedOnly: undefined });
  assert.equal(old.total, 1754);
  assert.equal(old.rows.some((r) => r.key === "recordedOnly"), false);
  ok("the tree's mouth is every url the router knows of, not what one derivation named");
}

{
  // A node whose children do not sum gets a named child in the fault tone rather than a short bar.
  const f = funnelOf({
    name: "consistency", sourced: 100, excluded: 0, heldOutDead: 0,
    streams: [{ candidates: 100, foldedAway: 0, consistent: 10, inconsistent: 0, unmeasured: 90,
      undecided: { reasons: [{ reason: "never answered a REQ", urls: 40, hosts: 4 }], omitted: 3 } }],
  });
  const short = f.rows.find((r) => r.key === "unattributed");
  assert.equal(short.value, 50, "the reasons cover 40 of the 90 with no verdict");
  assert.equal(short.depth, 3, "…and it is a child of the node that did not close, not of the root");
  assert.equal(short.tone, "warn", "an unclosed partition must look wrong");
  assert.equal(f.omitted, 3, "and the rows the router itself dropped are carried through");
  ok("a node whose children do not sum names the remainder rather than drawing a short bar");
}

{
  // A pass that publishes none of the three verdict members measures no
  // verdicts; read as zeroes, every url with a verdict lands in the fault row.
  assert.equal(funnelOf({
    name: "aliasFold", phase: "idle", sourced: 17584, heldOutDead: 832,
    streams: [{ name: "all streams", candidates: 16752, unmeasured: 4021, dialled: 2000, decided: 118 }],
  }), null, "a pass that publishes no partition is not given one");

  assert.equal(funnelOf(null), null);
  assert.equal(funnelOf({ name: "consistency" }), null, "a row with no streams has no tree");
  assert.equal(funnelOf({ name: "consistency", streams: [{ candidates: 0, consistent: 0 }] }), null,
    "an empty candidate set is not a tree of zeroes");

  // With no `sourced` the root is what can still be accounted for.
  const bare = funnelOf({ name: "consistency", streams: [{ candidates: 40, consistent: 10, unmeasured: 30 }] });
  assert.equal(bare.total, 40);
  assert.equal(bare.rows.find((r) => r.key === "dropped").value, 0);

  // Summed across rows, never `streams[0]`.
  const two = funnelOf({
    name: "consistency", sourced: 60,
    streams: [
      { candidates: 40, foldedAway: 10, consistent: 10, inconsistent: 0, unmeasured: 20 },
      { candidates: 20, foldedAway: 0, consistent: 5, inconsistent: 5, unmeasured: 10 },
    ],
  });
  assert.equal(two.candidates, 60);
  assert.equal(two.rows.find((r) => r.key === "inconsistent").value, 5);
  ok("an absent partition is no tree, an absent member is a zero, and rows are summed");
}

{
  // A reason and a hostname are free text used as keys for the tone lookup.
  for (const hostile of ["constructor", "toString", "__proto__", "hasOwnProperty"]) {
    const f = funnelOf({
      name: "consistency", sourced: 10,
      streams: [{ candidates: 10, foldedAway: 0, consistent: 2, inconsistent: 0, unmeasured: 8,
        undecided: { reasons: [{ reason: hostile, urls: 8, hosts: 1, top: [{ host: hostile, urls: 8 }] }], omitted: 0 } }],
    });
    const rows = f.rows.filter((r) => r.key === hostile);
    assert.equal(rows.length, 1, "the reason; its hosts are numbers on it rather than rows");
    assert.equal(rows[0].tone, null, `${hostile} is unknown text, not a prototype member`);
  }
  ok("a reason or host this page has not been taught cannot reach Object.prototype");
}

// ── a reason that refines another reason ────────────────────────────────────
{
  // The router publishes refinements as a flat list naming their parent, so
  // the rows still sum to `unmeasured` whatever the shape.
  const f = funnelOf({
    name: "consistency", sourced: 1000,
    streams: [{
      candidates: 1000, foldedAway: 0, consistent: 100, inconsistent: 0, unmeasured: 900,
      undecided: {
        reasons: [
          { reason: "the name does not resolve", parent: "never answered a REQ", urls: 500, hosts: 480,
            top: [{ host: "gone.example", urls: 20 }] },
          { reason: "the connection was refused", parent: "never answered a REQ", urls: 200, hosts: 90 },
          { reason: "too few events to judge on", urls: 200, hosts: 150 },
        ],
        omitted: 0,
      },
    }],
  });
  const at = (key) => f.rows.find((r) => r.key === key);

  // The parent is synthesised: every url it covers is already in a child, and
  // a published row beside them would double-count.
  assert.equal(at("never answered a REQ").value, 700, "the sum of its children, not a published number");
  assert.equal(at("never answered a REQ").depth, 3);
  assert.equal(at("the name does not resolve").depth, 4, "a refinement sits under what it refines");
  assert.equal(at("the name does not resolve").largest, 20, "and its widest host is a number on it, not a row under it");
  assert.equal(f.rows.some((r) => r.depth > 4), false, "a refinement is the deepest thing drawn");

  // Widest first among siblings, the synthesised parent competing on its own total.
  assert.deepEqual(
    f.rows.filter((r) => r.depth === 3).map((r) => r.key),
    ["never answered a REQ", "too few events to judge on"],
  );

  // The partition still closes: 900 is 700 + 200.
  assert.equal(f.rows.some((r) => r.key === "unattributed"), false);
  ok("a row that refines another is nested under it, and the parent is summed rather than trusted");
}

{
  // A lone refinement must not vanish into a group that was never opened.
  const f = funnelOf({
    name: "consistency", sourced: 100,
    streams: [{
      candidates: 100, foldedAway: 0, consistent: 10, inconsistent: 0, unmeasured: 90,
      undecided: { reasons: [{ reason: "the TLS handshake failed", parent: "never answered a REQ", urls: 90, hosts: 9 }], omitted: 0 },
    }],
  });
  assert.equal(f.rows.find((r) => r.key === "never answered a REQ").value, 90);
  assert.equal(f.rows.find((r) => r.key === "the TLS handshake failed").value, 90);
  ok("a lone refinement keeps both its own row and the group it belongs to");
}

// ── does the document still add up ──────────────────────────────────────────
{
  // `unattributed` can only report children that fall short; rows that
  // overshoot leave no slice, so the relay's own check rides along.
  const doc = (over) => ({
    name: "consistency", sourced: 100,
    streams: [{ candidates: 100, foldedAway: 0, consistent: 10, inconsistent: 0, unmeasured: 90,
      undecided: { reasons: [{ reason: "never answered a REQ", urls: 90, hosts: 9 }], omitted: 0 }, ...over }],
  });
  assert.equal(funnelOf(doc({ accountedFor: true })).accountedFor, true);
  assert.equal(funnelOf(doc({ accountedFor: false })).accountedFor, false);
  // A router too old to make the claim is not a router making a false one.
  assert.equal(funnelOf(doc({})).accountedFor, null, "absent is not a verdict either way");
  ok("the relay's own arithmetic check rides on the tree, and absent is not false");
}

// ── the four pools ──────────────────────────────────────────────────────────
{
  // One rotating pool runs four workloads, and a number that adds them together names nobody.
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  const doc = {
    streams: [
      { name: "content", inFlight: { relays: [
        held("wss://a.example/", { doing: "catching up (paging)", pool: POOL_CATCHING_UP, quietForSec: 1 }),
        held("wss://b.example/", { doing: "negentropy sync of the past", pool: POOL_NEGENTROPY, quietForSec: 900 }),
      ], omitted: 0 } },
      { name: "indexers", inFlight: { relays: [
        held("wss://c.example/", { doing: "re-fetching the past (paging)", pool: POOL_REFETCHING }),
        held("wss://d.example/", { doing: "catching up (paging)", pool: POOL_CATCHING_UP, quietForSec: 400 }),
        held("wss://e.example/", { doing: "claiming the socket" }),
      ], omitted: 0 } },
    ],
    live: { relays: [held("wss://f.example/", { doing: "holding a live tail", pool: POOL_LIVE, stream: "content" })], omitted: 0 },
  };
  const pools = poolsOf(doc);
  const at = (key) => pools.groups.find((g) => g.key === key);

  assert.deepEqual(pools.groups.map((g) => g.key), [...POOL_ORDER, POOL_BETWEEN],
    "the four the router names, in one order, and the leftovers last");
  assert.deepEqual(at(POOL_CATCHING_UP).rows.map((r) => r.relay), ["wss://d.example/", "wss://a.example/"],
    "quietest first ACROSS streams — the merge interleaves two lists that were each sorted alone");
  assert.deepEqual(at(POOL_NEGENTROPY).rows.map((r) => r.relay), ["wss://b.example/"]);
  assert.deepEqual(at(POOL_REFETCHING).rows.map((r) => r.relay), ["wss://c.example/"]);
  assert.deepEqual(at(POOL_LIVE).rows.map((r) => r.relay), ["wss://f.example/"]);

  // A visit between jobs publishes no pool word, and that row is the one an operator is chasing.
  assert.deepEqual(at(POOL_BETWEEN).rows.map((r) => r.relay), ["wss://e.example/"]);
  assert.equal(pools.groups.reduce((a, g) => a + g.rows.length, 0), 6, "every row published is in exactly one group");

  // The stream rides on the row: one visit serves every stream's asks, so a pool spans streams.
  assert.deepEqual(at(POOL_CATCHING_UP).rows.map((r) => r.stream), ["indexers", "content"]);
  // A tail names its own stream: one subscription serves one stream.
  assert.equal(at(POOL_LIVE).rows[0].stream, "content");
  assert.equal(at(POOL_LIVE).streams, true, "so the live table draws a stream column like the rest");
  assert.equal(at(POOL_CATCHING_UP).streams, true);
  ok("every held relay lands in exactly one pool, quietest first, and none is dropped for want of a word");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // The pool's word is lifted into its heading when every row agrees, and
  // kept as a column where they do not.
  const one = poolsOf({ streams: [{ name: "content", inFlight: { relays: [
    held("wss://a.example/", { doing: "catching up (paging)", pool: POOL_CATCHING_UP }),
    held("wss://b.example/", { doing: "catching up (paging)", pool: POOL_CATCHING_UP }),
  ], omitted: 0 } }] });
  assert.equal(one.groups.find((g) => g.key === POOL_CATCHING_UP).doing, "catching up (paging)");

  const two = poolsOf({ streams: [{ name: "content", inFlight: { relays: [
    held("wss://a.example/", { doing: "negentropy sync of the past", pool: POOL_NEGENTROPY }),
    held("wss://b.example/", { doing: "negentropy sync of the provider's own records", pool: POOL_NEGENTROPY }),
  ], omitted: 0 } }] });
  assert.equal(two.groups.find((g) => g.key === POOL_NEGENTROPY).doing, null, "two stages keep their column");
  ok("a pool's stage word is lifted out of the table only where every row agrees on it");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // Empty is an answer: a pool that vanished when empty would read as a build with no such pool.
  const sparse = poolsOf({ live: { relays: [held("wss://f.example/", { pool: POOL_LIVE })], omitted: 0 } });
  assert.deepEqual(sparse.groups.map((g) => g.key), POOL_ORDER, "the four are always drawn…");
  assert.deepEqual(sparse.groups.find((g) => g.key === POOL_NEGENTROPY).rows, []);
  // The fifth group is not one of them: an empty `between` is the healthy case.
  assert.equal(sparse.groups.some((g) => g.key === POOL_BETWEEN), false);

  // Holding nothing at all is the one state where four empty tables say less than no panel.
  assert.equal(poolsOf({ streams: [{ name: "content" }] }), null);
  assert.equal(poolsOf(null), null);

  // A row the router left out has no pool by definition, so it is summed, never attributed.
  const cut = poolsOf({
    streams: [{ name: "content", inFlight: { relays: [held("wss://a.example/", { pool: POOL_CATCHING_UP })], omitted: 7 } }],
    live: { relays: [held("wss://f.example/", { pool: POOL_LIVE })], omitted: 2 },
  });
  assert.equal(cut.omitted, 9);
  ok("an empty pool still says so, an empty mirror draws no panel, and what was cut is counted once");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // A word off the wire is not a heading, and `__proto__` cannot reach
  // Object.prototype through the label map.
  const odd = poolsOf({ streams: [{ name: "content", inFlight: { relays: [
    held("wss://a.example/", { pool: "quantum-sync" }),
    held("wss://b.example/", { pool: "__proto__" }),
  ], omitted: 0 } }] });
  assert.deepEqual(odd.groups.find((g) => g.key === POOL_BETWEEN).rows.map((r) => r.relay),
    ["wss://a.example/", "wss://b.example/"]);
  assert.equal(odd.groups.every((g) => typeof g.label === "string"), true);
  ok("a pool word this page has not been taught is drawn with the unpooled, never as a group of its own");
}

// ── how big the pool is ─────────────────────────────────────────────────────
{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // One pool shared by every visit-mode stream, so its size comes off the
  // pool's own row; summing the streams' rosters would double-count.
  const doc = {
    processors: [
      { name: "ingest", queued: 3 },
      { name: "visits", roster: 412, rosterVisits: 431, awaitingVisit: 7, visiting: 3, liveHeld: 2 },
    ],
    streams: [
      { name: "content", inFlight: { relays: [
        held("wss://a.example/", { pool: POOL_CATCHING_UP }),
        held("wss://b.example/", { pool: POOL_NEGENTROPY }),
      ], omitted: 0 } },
      { name: "indexers", inFlight: { relays: [held("wss://c.example/", { pool: POOL_REFETCHING })], omitted: 0 } },
    ],
    live: { relays: [held("wss://f.example/", { pool: POOL_LIVE }), held("wss://g.example/", { pool: POOL_LIVE })], omitted: 0 },
  };
  const { totals } = poolsOf(doc);
  assert.equal(totals.relays, 412, "the pool's whole world, from the pool's own row");
  // The unit of work is a (relay, stream) pair, so a relay three streams want
  // is three units; subtracting pair counts from a relay count is nonsense.
  assert.equal(totals.units, 431);
  assert.equal(totals.working, 3, "counted off the ROWS, so the summary cannot disagree with the tables under it");
  assert.equal(totals.queued, 7);
  assert.equal(totals.waiting, 421, "the remainder: on a revisit timer, neither running nor queued");
  assert.equal(totals.units, totals.working + totals.queued + totals.waiting, "those three partition the UNITS");
  assert.notEqual(totals.relays, totals.units, "…and the relay count is context beside them, not the whole");
  // A tailed relay keeps its tail while revisited, so the tail count crosses the partition.
  assert.equal(totals.tailed, 2);
  ok("the pool's size is one number off one row, and three of the four marks partition it");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // A router that publishes no pool row is not a router with an empty pool.
  const silent = poolsOf({ streams: [{ name: "content", inFlight: { relays: [held("wss://a.example/", { pool: POOL_CATCHING_UP })], omitted: 0 } }] }).totals;
  assert.equal(silent.relays, null);
  assert.equal(silent.units, null);
  assert.equal(silent.queued, null);
  assert.equal(silent.waiting, null, "no roster is no remainder to compute");
  assert.equal(silent.working, 1, "what the rows say is still said");
  assert.equal(silent.tailed, 0);

  // The three counts are read at one tick but not one instant, so a roster
  // that shrank between them can leave the subtraction short.
  const raced = poolsOf({
    processors: [{ name: "visits", roster: 1, rosterVisits: 1, awaitingVisit: 4 }],
    streams: [{ name: "content", inFlight: { relays: [held("wss://a.example/", { pool: POOL_CATCHING_UP })], omitted: 0 } }],
  }).totals;
  assert.equal(raced.waiting, 0, "the remainder floors at zero rather than going negative");
  ok("a pool the document does not size says nothing, and a raced subtraction never goes negative");
}

// ── the same rows, cut by stream, and everything else about that stream ─────
{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // Four independent walks of `progress.streams` named one stream four
  // times; this is the join, done once.
  const doc = {
    processors: [{ name: "visits", roster: 3, rosterVisits: 4, awaitingVisit: 9 }],
    streams: [
      { name: "content", phase: ROTATING, phaseForSec: 900, roster: 100, liveHeld: 1, awaitingVisit: 6,
        inFlight: { relays: [
          held("wss://a.example/", { pool: POOL_CATCHING_UP }),
          held("wss://b.example/", { pool: POOL_NEGENTROPY }),
        ], omitted: 0 },
        limits: [{ job: POOL_NEGENTROPY, streamCap: 4, inUse: 4, deferred: 91 }],
        schedule: [{ job: POOL_NEGENTROPY, everySec: 604800, due: 3, neverRun: 0, waiting: 12, nextInSec: 900 }] },
      { name: "indexers", phase: ROTATING, roster: 40, liveHeld: 0, awaitingVisit: 3, inFlight: { relays: [
        held("wss://a.example/", { pool: POOL_REFETCHING }),
      ], omitted: 0 } },
    ],
    live: { relays: [held("wss://f.example/", { pool: POOL_LIVE, stream: "content" })], omitted: 0 },
  };
  const cut = streamSections(doc);
  assert.deepEqual(cut.map((c) => c.stream), ["content", "indexers"], "one section per stream, in the document's order");
  const rowsIn = (c, key) => c.groups.find((g) => g.key === key).rows.map((r) => r.relay);

  // The unit of work is a (relay, stream) pair, so one url is legitimately
  // catching up for one stream and re-fetching for another.
  assert.deepEqual(rowsIn(cut[0], POOL_CATCHING_UP), ["wss://a.example/"]);
  assert.deepEqual(rowsIn(cut[1], POOL_REFETCHING), ["wss://a.example/"]);
  assert.deepEqual(rowsIn(cut[0], POOL_NEGENTROPY), ["wss://b.example/"]);
  assert.deepEqual(rowsIn(cut[1], POOL_NEGENTROPY), [], "and an empty pool under a stream still says so");

  // A tail is held per (relay, stream) pair and names its owner.
  assert.deepEqual(rowsIn(cut[0], POOL_LIVE), ["wss://f.example/"]);
  assert.deepEqual(rowsIn(cut[1], POOL_LIVE), []);

  // The sections partition the mirror's rows, so the summary above them is drawn off the same read.
  const total = (list) => list.reduce((a, c) => a + c.holding, 0);
  assert.equal(total(cut), poolsOf(doc).groups.reduce((a, g) => a + g.rows.length, 0));
  assert.equal(cut[0].holding, 3, "…and `holding` is that count per section, so the card need not sum the groups");

  assert.equal(cut[0].phase, ROTATING);
  assert.equal(cut[0].phaseForSec, 900);
  assert.equal(cut[0].rotation.roster, 100, "and the judgement about that roster, which is not a number");

  // The config rows, already joined on the job they share.
  assert.deepEqual(cut[0].jobs.map((j) => j.job), [POOL_NEGENTROPY]);
  assert.equal(cut[0].jobs[0].limit.deferred, 91);
  assert.equal(cut[0].jobs[0].schedule.due, 3);
  assert.deepEqual(cut[1].jobs, [], "a stream the router publishes no cap or clock for gets no rows, not empty ones");

  // A section's heading already names the stream, so the column would be that heading copied down.
  assert.equal(cut[0].groups.every((g) => g.streams === false), true);
  assert.equal(poolsOf(doc).groups.find((g) => g.key === POOL_LIVE).streams, true, "…where the mirror's does draw it");
  ok("a stream is one section: its phase, its rows, its caps and its clocks, joined once");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // The pool-wide totals cannot be divided into a stream's share, so the
  // stream row publishes its own; inside one stream a relay is one unit.
  const [content] = streamSections({
    streams: [{ name: "content", phase: ROTATING, roster: 100, awaitingVisit: 6, inFlight: { relays: [
      held("wss://a.example/", { pool: POOL_CATCHING_UP }),
      held("wss://b.example/", { pool: POOL_NEGENTROPY }),
    ], omitted: 0 } }],
    live: { relays: [held("wss://f.example/", { pool: POOL_LIVE, stream: "content" })], omitted: 0 },
  });
  assert.equal(content.totals.relays, 100);
  assert.equal(content.totals.units, 100, "one relay is one unit for one stream");
  assert.equal(content.totals.working, 2, "counted off the rows, like the mirror's");
  assert.equal(content.totals.queued, 6);
  assert.equal(content.totals.waiting, 92);
  assert.equal(content.totals.relays, content.totals.working + content.totals.queued + content.totals.waiting);
  assert.equal(content.totals.tailed, 1, "and the tail count crosses them rather than joining them");

  // A remainder computed without the queued share would count the queue as sitting between visits.
  const [old] = streamSections({
    streams: [{ name: "content", phase: ROTATING, roster: 100, inFlight: { relays: [
      held("wss://a.example/", { pool: POOL_CATCHING_UP }),
    ], omitted: 0 } }],
  });
  assert.equal(old.totals.queued, null);
  assert.equal(old.totals.waiting, null);
  assert.equal(old.totals.working, 1, "what the rows say is still said");
  ok("a stream's share of the roster comes off its own row, and says nothing where that row does not");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // Every configured stream gets a section: one in `router.conf` that never
  // came up is the one an operator goes looking for.
  const cut = streamSections({
    streams: [
      { name: "content", phase: ROTATING, roster: 100, awaitingVisit: 100 },
      { name: "notStarted", phase: "starting" },
    ],
  });
  assert.deepEqual(cut.map((c) => c.stream), ["content", "notStarted"]);
  assert.deepEqual(cut[0].groups.map((g) => g.key), POOL_ORDER);
  assert.equal(cut[0].totals.working, 0);
  assert.equal(cut[0].holding, 0, "a stream riding the pool and holding nothing says so in one number");
  assert.equal(cut[1].rotation, null, "…and one that is not rotating has no roster to be a share of");
  assert.equal(cut[1].totals.relays, null, "which is said as absent, never as zero");

  // A configured stream always carries a phase word, or a section would render its name and nothing.
  const [silent] = streamSections({ streams: [{ name: "content" }] });
  assert.equal(silent.phase, STARTING);
  assert.equal(silent.phaseForSec, null, "…and no clock is invented to go with it");

  // A row no configured stream claims gets a section of its own: a tail
  // naming a retired stream is worth seeing.
  const orphan = streamSections({
    streams: [{ name: "content", phase: ROTATING, roster: 1, awaitingVisit: 0 }],
    live: { relays: [held("wss://f.example/", { pool: POOL_LIVE, stream: "retired" })], omitted: 0 },
  });
  assert.deepEqual(orphan.map((c) => c.stream), ["content", null]);
  // The one section with no phase: it is not a stream and it is not starting.
  assert.equal(orphan[1].phase, null, "the unattributed rows are not a stream and get no phase word");
  assert.deepEqual(orphan[1].groups.find((g) => g.key === POOL_LIVE).rows.map((r) => r.relay), ["wss://f.example/"]);
  assert.equal(orphan[1].groups.find((g) => g.key === POOL_LIVE).streams, true,
    "and it keeps its stream column: the heading says only that nothing claimed it");
  assert.equal(streamSections(null).length, 0);
  assert.equal(streamSections({ streams: [{ name: "content", phase: ROTATING, roster: 1 }] }).length, 1,
    "…and an unattributed section appears only when there is something in it");
  ok("every configured stream keeps its section, and a row no stream claims gets one of its own");
}

// ── the two config lists, joined on the job they share ──────────────────────
{
  // The caps and the clocks were two tables keyed by (stream, job), and the
  // reading that matters spans them: a cap at its ceiling with work backing
  // up behind it.
  const rows = jobsOf(
    limitsOf({ streams: [{ name: "content", limits: [
      { job: POOL_NEGENTROPY, streamCap: 4, inUse: 4, deferred: 91 },
      { job: JOB_VISITING, streamCap: 64, inUse: 18, deferred: 0 },
    ] }] }),
    scheduleOf({ streams: [{ name: "content", schedule: [
      { job: POOL_NEGENTROPY, everySec: 604800, due: 3, neverRun: 0, waiting: 12, nextInSec: 900 },
      { job: POOL_REFETCHING, everySec: 2592000, due: 0, neverRun: 4, waiting: 40, nextInSec: 90000 },
    ] }] }),
  );

  // The router's order, not alphabetical.
  assert.deepEqual(rows.map((r) => r.job), [JOB_VISITING, POOL_REFETCHING, POOL_NEGENTROPY]);
  assert.equal(rows.find((r) => r.job === POOL_NEGENTROPY).limit.deferred, 91);
  assert.equal(rows.find((r) => r.job === POOL_NEGENTROPY).schedule.due, 3);

  // Half a row is still a row: a dial width has no clock, and `limitsOf` drops an uncapped job.
  assert.equal(rows.find((r) => r.job === JOB_VISITING).schedule, null);
  assert.equal(rows.find((r) => r.job === POOL_REFETCHING).limit, null,
    "an uncapped job that is nonetheless scheduled keeps its clock");
  assert.equal(rows.find((r) => r.job === POOL_REFETCHING).schedule.waiting, 40);

  // The label is the page's word for the job.
  assert.equal(rows.find((r) => r.job === POOL_NEGENTROPY).label, POOL_LABELS[POOL_NEGENTROPY][0]);

  // A job word this page has not been taught is kept under its own name, the rule the fifth pool group has.
  const odd = jobsOf([{ stream: "content", job: "compaction", label: "compaction", streamCap: 2, inUse: 0, deferred: 0 }], []);
  assert.deepEqual(odd.map((r) => r.job), ["compaction"]);
  assert.deepEqual(jobsOf([], []), []);
  ok("the caps and the clocks are one row per job, either half may be absent, and an unknown job is kept");
}

// ── what each stream may spend ──────────────────────────────────────────────
{
  // One pool, shared, so what a stream may cost is a property of the admission gates.
  const doc = {
    streams: [
      { name: "content", limits: [
        { job: POOL_NEGENTROPY, streamCap: 4, inUse: 4, deferred: 91 },
        { job: POOL_CATCHING_UP, streamCap: 48, inUse: 12, deferred: 0 },
        { job: POOL_LIVE, deferred: 0 },
      ] },
      { name: "indexers", limits: [
        { job: POOL_NEGENTROPY, streamCap: 2, inUse: 0, deferred: 0 },
        { job: POOL_REFETCHING, deferred: 7 },
      ] },
    ],
  };
  const rows = limitsOf(doc);

  // A job nothing caps is dropped: a table of unlimited rows reads the same on every deployment.
  assert.deepEqual(rows.map((r) => `${r.stream}/${r.job}`),
    ["content/negentropy", "content/catching-up", "indexers/negentropy", "indexers/re-fetching"]);

  // Except one that has turned work away, which cannot happen without a cap.
  assert.equal(rows.find((r) => r.job === POOL_REFETCHING).streamCap, null);
  assert.equal(rows.find((r) => r.job === POOL_REFETCHING).deferred, 7);

  // At the cap is not a fault; only the row turning work away is marked.
  assert.equal(rows.find((r) => r.stream === "content" && r.job === POOL_NEGENTROPY).biting, true);
  assert.equal(rows.find((r) => r.stream === "content" && r.job === POOL_CATCHING_UP).biting, false,
    "in use below the cap with nothing deferred is a stream inside its budget");

  // `deferred` is cumulative since boot, so on the counter alone a cap would stay hot for the life of the process.
  const past = limitsOf({ streams: [{ name: "content", limits: [
    { job: POOL_NEGENTROPY, streamCap: 4, inUse: 1, deferred: 91 },
  ] }] });
  assert.equal(past[0].biting, false, "room at the cap now — whatever it turned away earlier");
  assert.equal(past[0].deferred, 91, "…and the count is still published, because it is still the reading");

  // Zero permits out is a reading (capped and using none), not a gap.
  assert.equal(rows.find((r) => r.stream === "indexers" && r.job === POOL_NEGENTROPY).inUse, 0);
  assert.equal(rows.find((r) => r.job === POOL_REFETCHING).inUse, null, "no cap of its own is no `in use` to report");
  assert.deepEqual(limitsOf(null), []);
  ok("the caps are a table across streams, an uncapped job is dropped unless it has refused work, and only a biting cap is marked");
}

// ── when the past is re-read ────────────────────────────────────────────────
{
  // Only `waiting` draining at the period says work happened because it was
  // due, so the whole distribution is published.
  const doc = {
    streams: [
      { name: "content", schedule: [
        { job: POOL_NEGENTROPY, everySec: 604800, due: 3, neverRun: 12, waiting: 397, nextInSec: 41200 },
        { job: POOL_REFETCHING, everySec: 2592000, due: 0, neverRun: 0, waiting: 412, nextInSec: 900000 },
      ] },
    ],
  };
  const rows = scheduleOf(doc);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].waiting, 397, "the healthy majority — nothing to do, which is what a period is FOR");
  assert.equal(rows[0].nextInSec, 41200);
  assert.equal(rows[0].backedUp, false, "due work with a queue waiting behind it is a schedule turning over");

  // Never run is its own number: a fresh deployment is all due and perfectly healthy.
  const fresh = scheduleOf({ streams: [{ name: "content", schedule: [
    { job: POOL_NEGENTROPY, everySec: 604800, due: 0, neverRun: 412, waiting: 0 },
  ] }] })[0];
  assert.equal(fresh.neverRun, 412);
  assert.equal(fresh.due, 0);
  assert.equal(fresh.nextInSec, null, "nothing waiting is no countdown, and must not read as due now");
  assert.equal(fresh.backedUp, false, "a fresh deployment is ALL due and perfectly healthy");

  // The shape worth a colour: elapsed for everything, nothing waiting, none of it a first pass.
  const stuck = scheduleOf({ streams: [{ name: "content", schedule: [
    { job: POOL_NEGENTROPY, everySec: 604800, due: 412, neverRun: 0, waiting: 0 },
  ] }] })[0];
  assert.equal(stuck.backedUp, true);
  assert.deepEqual(scheduleOf(null), []);
  ok("the schedule publishes the whole distribution, a first pass is told from an elapsed one, and only backed-up work is marked");
}

{
  // The document serves totals, so the subtraction is the page's job. Shares,
  // not durations: the poll window varies, so two readings of "write 45s"
  // are not comparable.
  const before = [{ stage: "write", ms: 10_000 }, { stage: "dedup", ms: 4_000 }];
  const now = [{ stage: "write", ms: 22_000 }, { stage: "dedup", ms: 4_500 }, { stage: "verify", ms: 300 }];
  const rows = stageDeltas(now, before);
  assert.deepEqual(rows.map((r) => [r.stage, r.ms]), [["write", 12_000], ["dedup", 500]],
    "busiest first, and a stage with no previous total is not a delta");
  assert.equal(Math.round(rows[0].share * 100), 96, "share is of the interval, not of the row");

  // Falling back to the cumulative totals on a first load would put an hour
  // of history under "since the last refresh".
  assert.deepEqual(stageDeltas(now, null), []);
  assert.deepEqual(stageDeltas(null, before), []);

  // A counter that went backwards is a restarted process, so the row is dropped rather than clamped.
  assert.deepEqual(stageDeltas([{ stage: "write", ms: 5 }], [{ stage: "write", ms: 10_000 }]), []);
  assert.deepEqual(stageDeltas(before, before), []);

  // Junk rows are skipped rather than drawn as NaN.
  assert.deepEqual(stageDeltas([{ stage: "w", ms: "x" }, null], [{ stage: "w", ms: 0 }]), []);

  // A lock wait never appears alone: its `hold` and `write` are pulled past
  // the cut whenever the wait is shown.
  const zeros = (names) => names.map((stage) => ({ stage, ms: 0 }));
  const names = ["lock.ingest.wait", "a", "b", "c", "d", "lock.ingest.hold", "write"];
  const busy = [900, 800, 700, 600, 500, 400, 300].map((ms, i) => ({ stage: names[i], ms }));
  const shown = stageDeltas(busy, zeros(names)).map((r) => r.stage);
  assert.ok(shown.includes("lock.ingest.hold"), `the matching hold must come with the wait, got ${shown}`);
  assert.ok(shown.includes("write"), `write must come with the wait, got ${shown}`);
  assert.ok(!shown.includes("d"), "the companions come past the cut, they do not widen it for everyone");

  // With no wait on screen, it is a plain ranked cut.
  const plain = stageDeltas(busy.slice(1), zeros(names.slice(1))).map((r) => r.stage);
  assert.deepEqual(plain, ["a", "b", "c", "d"], `no wait shown means no companions pulled up, got ${plain}`);

  // Shares are of the whole interval, so a truncated list sums to less than 100%.
  assert.ok(stageDeltas(busy, zeros(names)).reduce((n, r) => n + r.share, 0) < 1);
  ok("the ingest stage split is a share between polls, empty on a first load, and never shows a lock wait alone");
}

// ── which store calls are out, and whose ────────────────────────────────────
{
  // A batch pass makes three different store calls and the pipeline row
  // reports them as one number. `hot` is a colour off a threshold, `more`
  // closes a truncation, and the bands are a partition.
  const call = (over = {}) => ({
    caller: "ingest.dedup", op: "existingIds", asked: "2048 id(s)",
    issuedAt: 1_769_998_206, elapsedSec: 794, outstandingAtIssue: 2, ...over,
  });

  // "This router does not say" and "nothing is outstanding" are opposite claims.
  assert.equal(storeOf(undefined), null);
  assert.equal(storeOf(null), null);
  assert.equal(storeOf("store"), null);

  const s = storeOf({
    outstanding: 3, slowAfterSec: 60, issued: 918_233, returned: 918_230, failed: 2, cancelled: 1,
    calls: [call(), call({ caller: "heal.resolve", op: "query", elapsedSec: 45 }), call({ elapsedSec: 0, asked: "" })],
    omitted: 0,
    callers: [{ caller: "ingest.dedup", issued: 41_022, returned: 41_020, failed: 0, cancelled: 0, outstanding: 2, oldestOutstandingSec: 794 }],
    ages: [{ fromSec: 0, calls: 1 }, { fromSec: 1, calls: 0 }, { fromSec: 10, calls: 1 },
           { fromSec: 60, calls: 0 }, { fromSec: 300, calls: 1 }, { fromSec: 900, calls: 0 }],
  });

  assert.equal(s.outstanding, 3);
  // Only calls past the bound the log warns at are coloured.
  assert.deepEqual(s.rows.map((r) => r.hot), [true, false, false]);

  // The router's own bound marks the row: `SYNC_STORE_SLOW_SEC` is an
  // operator's to change, and the log and the colour must mean one thing.
  const tuned = { outstanding: 1, slowAfterSec: 300, calls: [call({ elapsedSec: 120 })], ages: [{ fromSec: 60, calls: 1 }] };
  assert.equal(storeOf(tuned).rows[0].hot, false, "120s is ordinary under a 300s bound");
  assert.equal(storeOf(tuned).ages[0].hot, false, "…and so is the band it falls in");
  assert.equal(storeOf(tuned).stuckSec, 300);
  assert.equal(storeOf({ ...tuned, slowAfterSec: 60 }).rows[0].hot, true, "…and marked once the bound is back under it");
  // A router too old to publish the bound, and one whose operator set it to
  // 0 (which turns the log off), both fall back to the default.
  assert.equal(storeOf({ calls: [call({ elapsedSec: 61 })] }).rows[0].hot, true);
  assert.equal(storeOf({ slowAfterSec: 0, calls: [call({ elapsedSec: 61 })] }).rows[0].hot, true);
  assert.equal(STUCK_CALL_SEC, 60, "the fallback is the router's own default");
  // An empty `asked` is not a filter summary; only a null tells the card to draw "no filter".
  assert.equal(s.rows[2].asked, null);
  // Zero is the reading (it did not queue behind us); null is a router that declined to say.
  assert.equal(storeOf({ calls: [call({ outstandingAtIssue: 0 })] }).rows[0].outstandingAtIssue, 0);
  assert.equal(storeOf({ calls: [call({ outstandingAtIssue: undefined })] }).rows[0].outstandingAtIssue, null);

  // Empty bands are dropped for the reader and counted for the check.
  assert.deepEqual(s.ages.map((a) => [a.fromSec, a.calls]), [[0, 1], [10, 1], [300, 1]]);
  assert.deepEqual(s.ages.map((a) => a.hot), [false, false, true]);
  assert.equal(s.accountedFor, true);
  // Shares are of the whole outstanding set, not of the surviving bands.
  assert.ok(Math.abs(s.ages.reduce((n, a) => n + a.share, 0) - 1) < 1e-9);

  // A partition that does not close is reported rather than smoothed.
  assert.equal(storeOf({ outstanding: 9, ages: [{ fromSec: 0, calls: 1 }] }).accountedFor, false);
  // A router publishing no bands is not a router whose bands are wrong.
  assert.equal(storeOf({ outstanding: 9 }).accountedFor, true);

  // Two shapes worth acting on: calls that threw, and one held past the bound.
  const callers = storeOf({ callers: [
    { caller: "a", failed: 1 },
    { caller: "b", oldestOutstandingSec: 794 },
    { caller: "c", issued: 900, returned: 900 },
  ] }).callers;
  assert.deepEqual(callers.map((c) => c.hot), [true, true, false]);
  // Junk rows never reach a table.
  assert.deepEqual(storeOf({ calls: [null, { op: "query" }], callers: [null, {}] }), storeOf({}));

  // The cut is disclosed twice: what the router left out plus what this cut did.
  const many = storeOf({ outstanding: 40, calls: Array.from({ length: 20 }, () => call()), omitted: 7 });
  assert.equal(many.rows.length, CALLS_SHOWN);
  assert.equal(many.more, 7 + (20 - CALLS_SHOWN));
  ok("the store's outstanding calls are named longest-first, coloured off the log's own bound, and every cut is disclosed");
}

// ── the per-relay table: which relays are being synced at all ────────────────
//
// Everything else the mirror publishes is an aggregate, and the two states
// that matter most (never reached, refused on every visit) have no band.
{
  const relays = {
    pairs: 2712,
    // Deliberately not the sum of the rows below: the document cuts `rows` and not this.
    statuses: [
      { syncStatus: "refused", pairs: 54 },
      { syncStatus: "notStarted", pairs: 6 },
      { syncStatus: "paging", pairs: 1200 },
      { syncStatus: "complete", pairs: 1452 },
    ],
    freshness: [
      { behind: "current", pairs: 1400 }, { behind: "today", pairs: 900 },
      { behind: "thisWeek", pairs: 300 }, { behind: "older", pairs: 106 }, { behind: "nothing", pairs: 6 },
    ],
    rows: [
      { relay: "wss://walled.example/", stream: "content", syncStatus: "refused", behind: "nothing", fault: true,
        refusedFor: "the relay would not accept our NIP-42 identity",
        relaySaid: "auth-required: you are not authorized to perform reqs", refusedAgoSec: 900 },
      { relay: "wss://fresh.example/", stream: "content", syncStatus: "notStarted", behind: "nothing", fault: true },
      { relay: "wss://deep.example/", stream: "content", syncStatus: "paging", behind: "current", behindSec: 90,
        coveredFrom: 1600000000, coveredTo: 1700000000, visiting: true, asks: 40, settled: 3,
        negentropy: false, kindCap: 8 },
      { relay: "wss://done.example/", stream: "indexers", syncStatus: "complete", behind: "current", behindSec: 5,
        coveredFrom: 1500000000, coveredTo: 1700000000, verifiedAgoSec: 41200, tailed: true, negentropy: true },
    ],
    omitted: 1712,
  };
  const t = relayStatusOf(relays);

  assert.equal(t.pairs, 2712);
  // The partition comes off `statuses`, not off the cut rows.
  assert.deepEqual(t.chips.map((c) => [c.key, c.pairs]),
    [["refused", 54], ["notStarted", 6], ["paging", 1200], ["complete", 1452]]);
  ok("the status chips are read off the document's partition, never off the cut rows");

  // Worst first, and every status present even at zero.
  const empty = relayStatusOf({ pairs: 3, statuses: [], rows: [] });
  assert.deepEqual(empty.chips.map((c) => c.pairs), [0, 0, 0, 0]);
  assert.deepEqual(empty.chips.map((c) => c.key), ["refused", "notStarted", "paging", "complete"]);
  ok("a status nothing is in is drawn as zero, in the document's own order");

  // The router's own verdict, not re-derived: it spans both axes, and
  // `syncStatus` alone would leave a stale `complete` uncoloured.
  assert.deepEqual(t.rows.map((r) => r.hot), [true, true, false, false]);
  ok("the fault mark is the document's, so the page cannot disagree about which rows matter");

  // Off `freshness` for the same reason the statuses are off `statuses`.
  assert.deepEqual(t.freshness.map((c) => [c.key, c.pairs]),
    [["current", 1400], ["today", 900], ["thisWeek", 300], ["older", 106], ["nothing", 6]]);
  assert.equal(t.current, 1400);
  assert.equal(Math.round(t.currentShare * 100), 52);
  ok("the freshness partition and the current share are read off the document");

  assert.deepEqual(FRESHNESS.map(([k]) => k), ["current", "today", "thisWeek", "older", "nothing"]);
  ok("the five freshness buckets are the document's, in the document's order");

  // `negentropy` is a tri-state: absent means unmeasured and must not collapse to false.
  assert.equal(t.rows[2].negentropy, false);
  assert.equal(t.rows[3].negentropy, true);
  assert.equal(t.rows[0].negentropy, null, "unmeasured is neither true nor false");
  assert.equal(t.rows[2].kindCap, 8);
  assert.equal(t.rows[3].kindCap, null);
  ok("the terms a relay serves us on reach the row, and unmeasured stays absent");

  // The router's reading of which wall, and the relay's own sentence.
  assert.match(t.rows[0].why, /NIP-42 identity — auth-required: you are not authorized/);
  assert.equal(t.rows[1].why, null, "a pair that has never been visited has nothing to quote");
  ok("the refusal cell carries the router's reading and the relay's own words");

  // Absent is null and never zero: a 1970 in either column would read as a walk that reached the epoch.
  assert.equal(t.rows[1].coveredFrom, null);
  assert.equal(t.rows[1].verifiedAgoSec, null);
  assert.equal(t.rows[2].coveredFrom, 1600000000);
  assert.equal(t.rows[3].verifiedAgoSec, 41200);
  ok("a pair with no coverage has null edges, not epoch ones");

  // Not statuses, and both true at once is legal.
  assert.equal(t.rows[2].visiting, true);
  assert.equal(t.rows[2].tailed, false);
  assert.equal(t.rows[3].tailed, true);
  ok("visiting and tailed ride beside the status rather than being values of it");

  // `paging` covers 39-of-40 and 1-of-40 alike, and a unit owes one ask per bound provider.
  assert.equal(t.rows[2].progress, "3/40");
  assert.equal(t.rows[0].progress, null, "a unit with no asks reported says nothing rather than 0/0");
  assert.equal(t.rows[3].progress, null, "and `complete` is by definition all of them");
  ok("a paging row says how much of what it owes is settled");

  assert.equal(t.rows[0].short, "walled.example/", "the scheme is dropped and nothing else is");
  assert.equal(t.rows[3].label, "complete");
  assert.equal(t.rows[1].label, "hasn't started", "the page's words, not the member name");
  ok("a row carries the label the page shows and the url it looks up");

  // The same literal list `RelayStatusReportTest` pins on the Kotlin side; neither test can be the only one.
  assert.deepEqual(SYNC_STATUSES.map(([k]) => k), ["refused", "notStarted", "paging", "complete"]);
  ok("the four status words are the document's, in the document's order");

  assert.equal(t.omitted, 1712);
  // A router with no visit streams publishes no section; an empty table would read as a lost roster.
  assert.equal(relayStatusOf(null), null);
  assert.equal(relayStatusOf({ pairs: 0 }), null);
  ok("no prime relays draws no table at all");
}
