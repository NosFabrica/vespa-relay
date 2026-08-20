// WHAT THE SYNC CARD DECIDES — the half that can be wrong silently.
//
// This suite exists because of what it would have caught. Until the decisions
// were pulled out of stats.html the only pins over them were string greps:
// `SyncProgressReportTest` reads the page as text and asserts that every
// published member NAME appears somewhere in it. That catches a member nobody
// drew. It cannot catch a bar drawn against the wrong denominator, an empty
// object rendered as a chip, or a percentage of a number that is not there —
// and five of those shipped to a live deployment before an audit found them.
//
// Each assertion below is written in the direction its bug failed.
import assert from "node:assert/strict";
import {
  IN_FLIGHT_SHOWN, MEASURING, POOL_AUDITING, POOL_BETWEEN, POOL_CATCHING_UP,
  POOL_LIVE, POOL_ORDER, POOL_REFETCHING, ROTATING, STUCK_LEG_SEC, constraintOf,
  funnelOf, heldOf, legsOf, measuringOf, poolsOf, probeProgress, rotationOf,
} from "../../main/resources/web/shared/sync.js";

const ok = (name) => console.log(`  ✓ ${name}`);

/** A leg as `RelayRotation.held` publishes one. */
const leg = (n, quiet, over = {}) => ({
  relay: `wss://r${n}.example/`, heldForSec: 3600, transferringForSec: 3595,
  events: 1000 * n, quietForSec: quiet, ...over,
});

// ── the constraint ──────────────────────────────────────────────────────────
{
  // THE BUG: the relay rebuilt `health` member by member against an allowlist,
  // so a router older than these gauges cleared every one of them and `{}` was
  // published anyway. The card guarded on the OBJECT, so it drew a chip with no
  // text in it beside the live one.
  assert.equal(constraintOf({}), null, "an empty health object names no constraint");
  assert.equal(constraintOf(null), null);
  assert.equal(constraintOf({ eventsPerSec: 900 }), null, "gauges are not a verdict");

  const c = constraintOf({ bottleneck: "ingest" });
  assert.equal(c.text, "ingest is the limit");
  assert.equal(c.tone, "warn", "ingest is the only one of the four that is a fault");
  assert.match(c.why, /not at the relays/);
  assert.equal(constraintOf({ bottleneck: "downloads" }).tone, null, "the relays being the limit is not a fault");

  // A word this page has not been taught still says something rather than
  // rendering as undefined — the relay allowlists it, but the card is served to
  // whoever asks.
  assert.equal(constraintOf({ bottleneck: "novel" }).text, "novel");
  ok("the constraint is guarded on its own member, and names an unknown word rather than dropping it");
}

// ── the legs ────────────────────────────────────────────────────────────────
{
  // THE BUG, and the one that mattered most. The bar was a proportion of the
  // worst row PUBLISHED while only five were drawn, so an outlier outside those
  // five flattened every visible bar to nothing. Scaling by the worst row SHOWN
  // was worse: five legs each quiet a healthy thirty seconds all rendered full,
  // which is the reading that means stuck.
  const healthy = legsOf({ relays: [1, 2, 3, 4, 5].map((n) => leg(n, 30)), omitted: 0 });
  assert.deepEqual(healthy.rows.map((r) => r.quietShare), [0.05, 0.05, 0.05, 0.05, 0.05],
    "five healthy legs read as five healthy legs, not as five full bars");
  assert.equal(healthy.rows.every((r) => !r.hot), true);

  const wedged = legsOf({ relays: [leg(1, 30), leg(2, STUCK_LEG_SEC)], omitted: 0 });
  assert.equal(wedged.rows[1].quietShare, 1);
  assert.equal(wedged.rows[1].hot, true, "the threshold itself is stuck");
  // Past the threshold the bar cannot say more than "stuck", and must not
  // overflow its track saying it.
  assert.equal(legsOf({ relays: [leg(1, STUCK_LEG_SEC * 60)] }).rows[0].quietShare, 1);
  ok("the quiet bar is a proportion of the threshold, so an absolute reading survives its neighbours");
}

{
  // A truncated list that does not say it is truncated reads as the whole
  // answer, and here the whole answer is what an operator is chasing.
  // The PAGE no longer cuts the list: the leg being chased is by construction
  // not in the healthy head of it, so the router's own cap is now the only one
  // and `IN_FLIGHT_SHOWN` defers to whatever the document carries.
  const many = legsOf({ relays: Array.from({ length: 8 }, (_, i) => leg(i, 30)), omitted: 12 });
  assert.equal(many.rows.length, 8, "every leg the document names is drawn");
  assert.equal(many.more, 12, "what the ROUTER left out is still disclosed");
  assert.equal(IN_FLIGHT_SHOWN, Infinity, "the default defers to the document rather than re-cutting it");
  // …and a caller that does pass a limit still adds what it drops, which is the
  // property this pin has always been about.
  const capped = legsOf({ relays: Array.from({ length: 8 }, (_, i) => leg(i, 30)), omitted: 12 }, 5);
  assert.equal(capped.rows.length, 5);
  assert.equal(capped.more, 15, "what this side drops is ADDED to what the router already left out");
  assert.equal(legsOf({ relays: [leg(1, 30)], omitted: 0 }).more, 0);
  assert.deepEqual(legsOf(null), { rows: [], more: 0 });

  // The scheme is dropped and nothing else is: a truncated relay url is not a
  // relay url, and it is the thing being looked up.
  assert.equal(legsOf({ relays: [leg(1, 0, { relay: "wss://a.example/path" })] }).rows[0].short, "a.example/path");
  assert.equal(legsOf({ relays: [leg(1, 0, { relay: "ws://a.example/" })] }).rows[0].short, "a.example/");

  // Absent `transferringForSec` means "not on a socket at all", which is OUR
  // pool rather than their relay — a different fault from a slow download.
  assert.equal(legsOf({ relays: [leg(1, 30, { transferringForSec: null })] }).rows[0].slotless, true);
  assert.equal(legsOf({ relays: [leg(1, 30, { transferringForSec: 0 })] }).rows[0].slotless, false,
    "zero seconds on a slot is on a slot");
  ok("the tail is disclosed, and a leg with no slot is told from a slow one");
}

// ── how far a probe pass got ────────────────────────────────────────────────
{
  // THE DIRECTION. The document publishes `unmeasured` — what still has NO
  // verdict — and this returns its complement. Backwards, a fold that decided
  // almost nothing renders as one that nearly finished, and both are plausible.
  const fold = (over = {}) => ({
    name: "aliasFold", phase: "idle", lastPassSec: 6354,
    streams: [{ name: "all streams", candidates: 11693, unmeasured: 7546 }], ...over,
  });
  assert.equal(probeProgress(fold()).checked, 4147, "checked is candidates MINUS unmeasured");
  assert.equal(probeProgress(fold()).candidates, 11693, "the denominator is the candidate set");

  // A FOLDED URL IS NOT A CHECKED ONE. The gate never dials one — it is another
  // relay's second address — so it belongs to neither half. Drawn from the bare
  // complement, the real card read `12,024 of 16,752 checked for consistency`
  // beside its own tree showing 583 consistent and 12 inconsistent.
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

  // Summed, not `streams[0]` — the bug predates today's single merged row.
  const two = probeProgress(fold({ streams: [{ candidates: 17000, unmeasured: 9000 }, { candidates: 16, unmeasured: 4 }] }));
  assert.equal(two.candidates, 17016);
  assert.equal(two.checked, 8012, "both rows counted, not the first one");
  assert.equal(probeProgress(fold({ streams: [{ candidates: 10 }, {}] })).checked, 10,
    "a missing member is a zero on its row, not a NaN across the total");
  assert.equal(probeProgress(fold()).newOnly, false, "a row that does not count new urls says so");
  ok("the pass draws what HAS a verdict, summed across rows and never negative");
}

{
  // WHERE THE ROW SAYS WHAT ARRIVED UNDECIDED, that is the denominator — and
  // the card gets a word for it. The real fold read `143 of 1,754 relay(s)
  // checked` after an eleven-minute pass, where 1,611 of that denominator were
  // urls carrying month-old verdicts nothing was going to re-ask: the position
  // could not move whatever the pass achieved.
  const fresh = probeProgress({
    name: "aliasFold", phase: "idle", lastPassSec: 660,
    streams: [{ name: "all streams", candidates: 11693, newUrls: 1754, unmeasured: 1611 }],
  });
  assert.equal(fresh.candidates, 1754, "the denominator is what arrived with no verdict");
  assert.equal(fresh.checked, 143, "…and the numerator is how many of THOSE left with one");
  assert.equal(fresh.newOnly, true, "the page has a word to add");

  // Presence, not truthiness: a fold that has caught up publishes zero, which
  // is an answer and not an absence. Falling back to `candidates` there would
  // put the whole corpus back in the denominator exactly when the pass is done.
  //
  // This pair is also what the CARD branches on to stop saying `0 of 0 new
  // relay(s) checked` — see `PROBE_NONE` in stats.html. It is not a rare state:
  // it is the one both passes work towards, and a settled corpus holds it for
  // most of a monthly TTL.
  const caught = probeProgress({
    name: "aliasFold", phase: "idle",
    streams: [{ candidates: 11693, newUrls: 0, unmeasured: 0 }],
  });
  assert.equal(caught.candidates, 0);
  assert.equal(caught.newOnly, true);

  // Summed across rows like every other member, and a row that omits it counts
  // zero rather than dragging the whole document back to the old denominator.
  const mixed = probeProgress({
    name: "aliasFold", phase: "idle",
    streams: [{ candidates: 100, newUrls: 40, unmeasured: 30 }, { candidates: 16, unmeasured: 4 }],
  });
  assert.equal(mixed.candidates, 40);
  assert.equal(mixed.checked, 6, "34 unmeasured across both rows, against 40 new");
  ok("a pass that counts what arrived undecided is drawn against that, and says so");
}

{
  // The clock belongs to the last pass that FINISHED, so a fold two hours into
  // the next one must not show the previous one's duration as its elapsed time.
  const row = (over) => ({ name: "aliasFold", streams: [{ candidates: 10, unmeasured: 2 }], ...over });
  assert.equal(probeProgress(row({ phase: "idle", lastPassSec: 6354 })).tookSec, 6354);
  assert.equal(probeProgress(row({ phase: MEASURING, lastPassSec: 6354 })).tookSec, null);
  assert.equal(MEASURING, "measuring", "the word `Processors.MEASURING` publishes");

  // Before the first pass lands there is no duration — the whole of a cold boot
  // — and that is an absence rather than a zero.
  assert.equal(probeProgress(row({ phase: "idle" })).tookSec, null);
  assert.equal(probeProgress(row({ phase: "starting" })).tookSec, null);
  assert.equal(probeProgress(row({ phase: "idle", lastPassSec: 0 })).tookSec, 0, "a pass under a second still ran");

  // Ingest and the healer come through the same renderer and must fall past it.
  assert.equal(probeProgress({ name: "ingest", queued: 8304, capacity: 8192 }), null);
  assert.equal(probeProgress({ name: "aliasFold", streams: [] }), null);
  assert.equal(probeProgress(null), null);
  ok("the duration is the last FINISHED pass, absent while one runs and before the first");
}

// ── where the pass RUNNING right now has got to ──────────────────────────────
{
  // The gap this fills, and the reason it is a second function rather than a
  // member of the one above: `probeProgress` reads the row the LAST pass left,
  // which stands still for the hours the next one takes. With the sweep's
  // countdown unset while it runs — the monitor cannot promise a time nobody
  // has computed — the row carried the word `measuring` and no number at all.
  const gate = (over) => ({ name: "consistency", phase: MEASURING, measuring: over });

  const run = measuringOf(gate({ unit: "url", attempted: 604, toProbe: 4728, etaSec: 2724 }));
  assert.equal(run.attempted, 604);
  assert.equal(run.toProbe, 4728, "the denominator is what this PASS set out to walk, not the candidate set");
  assert.equal(run.etaSec, 2724);

  // No denominator is no position. A share of zero is the division this module
  // exists to keep out of the page, and the phase word alone is the better draw.
  assert.equal(measuringOf(gate({ unit: "url", attempted: 0, toProbe: 0 })), null);
  assert.equal(measuringOf({ name: "consistency", phase: MEASURING }), null, "a router too old to publish one says nothing");
  assert.equal(measuringOf({ name: "ingest", queued: 12, capacity: 20000 }), null, "the counter-shaped rows fall past it");
  assert.equal(measuringOf(null), null);

  // The estimate is WITHHELD until a unit has landed and once the last one has,
  // and both absences mean "no estimate" — where a zero would claim the pass is
  // finished. This is the failure `paging progress` in AGENTS.md is remembered
  // for: a predecessor divided a number by itself and printed `ETA ~0:00` for
  // hours.
  assert.equal(measuringOf(gate({ unit: "url", attempted: 0, toProbe: 4728 })).etaSec, null);
  assert.equal(measuringOf(gate({ unit: "url", attempted: 4728, toProbe: 4728 })).etaSec, null);
  assert.equal(measuringOf(gate({ unit: "url", attempted: 12, toProbe: 4728, etaSec: 0 })).etaSec, 0,
    "a real zero from the router is a pass about to end, not a missing estimate");

  // Read off a LIVE pass, so the two halves can be a tick apart. `4,729 of
  // 4,728` is a rendering fault rather than a finding.
  assert.equal(measuringOf(gate({ unit: "host", attempted: 99, toProbe: 10 })).attempted, 10);
  assert.equal(measuringOf(gate({ unit: "host", attempted: -4, toProbe: 10 })).attempted, 0);

  // The unit is the router's, because the passes do not count the same thing —
  // the fold decides a HOST and dials every url of one to do it.
  assert.equal(measuringOf(gate({ unit: "host", attempted: 37, toProbe: 214 })).unit, "host");
  assert.equal(measuringOf(gate({ attempted: 1, toProbe: 2 })).unit, "url", "a row with no unit still renders a sentence");
  ok("the pass in flight publishes both halves of its position, and no estimate it has not earned");
}

// ── a pass that has stopped, and the url that stopped it ────────────────────
{
  // THE BUG: a fitness pass sat at `12,373 of 12,374, ~0s left` for 74 minutes
  // on one wedged url. The estimate was correct arithmetic — one unit at the
  // rate so far rounds to nothing — so every number on the row agreed with
  // every other one, and nothing published said the pass had stopped. Nor was
  // the url nameable: not from this document, not from the log, not from a
  // thread dump, since a suspended coroutine has no stack frame.
  const stalled = measuringOf({
    name: "fitness", phase: MEASURING,
    measuring: { unit: "url", attempted: 12373, toProbe: 12374, etaSec: 0, quietForSec: 4454 },
  });
  assert.equal(stalled.etaSec, 0, "the estimate is still what the router sent");
  assert.equal(stalled.quietForSec, 4454, "…and this is the member that separates the two readings");

  // Absent is a router that predates the member, which is "not known" — never
  // a pass that just moved. A zero here would read as the healthy case.
  assert.equal(measuringOf({ measuring: { unit: "url", attempted: 6, toProbe: 22 } }).quietForSec, null);
  assert.equal(measuringOf({ measuring: { unit: "url", attempted: 6, toProbe: 22, quietForSec: 0 } }).quietForSec, 0);

  const held = (n, sec, over = {}) => ({ relay: `wss://r${n}.example/`, heldForSec: sec, stage: "ask ladder", ...over });

  // The router sorts longest-held FIRST — the reverse of a stream's legs,
  // because a probe leg is bounded by a deadline and a long one is the anomaly
  // — so the card can draw the front of the list and be drawing the answer.
  const rows = heldOf({ relays: [held(1, 4454), held(2, 12)], omitted: 0 }).rows;
  assert.equal(rows[0].relay, "wss://r1.example/");
  assert.equal(rows[0].heldForSec, 4454);
  assert.equal(rows[0].stage, "ask ladder");
  // The scheme goes and nothing else does: a truncated relay url is not a
  // relay url, and it is the thing being looked up.
  assert.equal(heldOf({ relays: [held(1, 9, { relay: "wss://a.example/path" })] }).rows[0].short, "a.example/path");

  // A step the page has not been taught reads as "not known", never as a step.
  assert.equal(heldOf({ relays: [held(1, 9, { stage: undefined })] }).rows[0].stage, null);

  // Cut, and it says so — unlike a stream's legs, which are published whole.
  // A probe pass at the monitor's default dial concurrency holds five hundred
  // urls, and a list that hides its truncation reads as the whole answer.
  const many = heldOf({ relays: Array.from({ length: 9 }, (_, i) => held(i, 30)), omitted: 4 }, 3);
  assert.equal(many.rows.length, 3);
  assert.equal(many.more, 10, "what the router left out plus what this cut");
  assert.deepEqual(heldOf(null), { rows: [], more: 0 });
  assert.deepEqual(heldOf({ relays: [] }), { rows: [], more: 0 }, "a pass holding nothing draws nothing");
  ok("a stalled pass is told from one about to finish, and the url holding it is named");
}

// ── what a rotating stream is riding ────────────────────────────────────────
{
  // THE COMPLAINT: `rotating for 58m` and nothing else. A visit stream has no
  // pass, no fraction and no cycle, so every other mark on its row is absent —
  // and the line reads the same whether it is riding four hundred relays or
  // none.
  const riding = rotationOf({ name: "visits", phase: ROTATING, roster: 412, tails: 300 });
  assert.equal(riding.roster, 412);
  assert.equal(riding.tails, 300);
  assert.equal(riding.waiting, false);

  // ZERO IS THE READING WORTH HAVING: before the fitness pass signs its first
  // `prime`, a visit stream is a stream with an empty world — busy-looking
  // and dialling nothing.
  assert.equal(rotationOf({ phase: ROTATING, roster: 0, tails: 0 }).waiting, true);
  assert.equal(rotationOf({ phase: ROTATING, roster: 0 }).tails, null, "no tail count is not a claim of none");

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

  // `created_at = 0` IS A REAL SECOND — purplepag.es holds twelve events
  // stamped with it — and the deepest a walk can reach. Falsy-coalescing it
  // erases the one position that proves a walk got all the way down.
  assert.equal(legsOf({ relays: [leg(1, 30, { pagingUntil: 0 })] }).rows[0].pagingUntil, 0,
    "the epoch is a position, not a missing cursor");
  ok("a paged cursor is carried per leg, and second zero is a position rather than an absence");
}

// ── names off the wire are not property lookups ─────────────────────────────
{
  // `bottleneck` is free text the card is served for whoever asks. Reaching
  // Object.prototype hands back a function, and destructuring one throws the
  // whole render away — a worse outcome than the unknown word it came from.
  for (const hostile of ["constructor", "toString", "__proto__", "hasOwnProperty"]) {
    const c = constraintOf({ bottleneck: hostile });
    assert.equal(c.text, hostile, `${hostile} is an unknown word, not a prototype member`);
    assert.equal(c.why, "");
  }
  ok("a bottleneck word this page has not been taught cannot reach Object.prototype");
}

// ── the candidate set, as a tree ────────────────────────────────────────────
{
  // A live-shaped document: a corpus mostly made of urls that cannot be
  // measured at all, which is the reading the whole tree exists to make
  // visible. The numbers are the identities
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

  // THE SHAPE. Depth is the relationship, so it is the thing to assert: a host
  // is under its reason, a reason under `no verdict`, that under the candidate
  // set, and the two dropped kinds under one branch of their own.
  assert.deepEqual(f.rows.map((r) => [r.depth, r.key]), [
    [0, "corpus"],
    [1, "dropped"], [2, "excluded"], [2, "heldOutDead"],
    [1, "candidates"], [2, "foldedAway"], [2, "consistent"], [2, "inconsistent"], [2, "unmeasured"],
    [3, "never answered a REQ"],
    [3, "too few events to judge on"],
  ]);

  // A REASON IS A LEAF. The hosts under it are published and deliberately not
  // drawn: one row per host is one row per SERVER on a corpus of two thousand
  // of them, and the ranked head is short only because the router capped it.
  assert.equal(f.rows.some((r) => r.key === "dead.example"), false, "no row per host");
  // What that list was FOR survives as two numbers on the reason's own row —
  // 3,902 urls on 2,201 hosts with the largest at 61 is a dead network spread
  // thin; the same urls with the largest at 3,000 would be three servers.
  assert.equal(at("never answered a REQ").hosts, 2201);
  assert.equal(at("never answered a REQ").largest, 61, "the widest host's share, not a list of them");
  assert.deepEqual(at("never answered a REQ").examples, ["dead.example", "gone.example"],
    "…and the names ride along for the row's title, which costs no height");
  // A short list is not cut, and says it was not.
  assert.equal(at("never answered a REQ").unnamed, 2199,
    "the names it did NOT fit are a count, because 2,201 hosts arrived as two names");

  // EVERY BAR AGAINST THE ROOT, never against the parent — against its parent a
  // host with 61 urls under a reason with 3,902 would draw at the width the
  // whole corpus gets, contradicting the indentation that already says it is
  // deep in a subtree.
  assert.equal(at("candidates").share, 16752 / 17584);
  assert.equal(at("never answered a REQ").share, 3902 / 17584);

  // THE GUIDES. A `│` is drawn at every ancestor that still has a sibling
  // below it, which is exactly the fact a flattened list loses — computed from
  // depth alone the tree still renders, with dangling verticals under the last
  // branch.
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
  // THE CORPUS IS NOT ONE DERIVATION'S YIELD. A url leaves the relay lists for
  // reasons of its own and every measurement of it stays in the store — the
  // fold still groups new urls against it — so rooted at `sourced` alone the
  // tree lost it silently, on a card captioned "every relay url this router
  // knows of". A deployment holding records for 17,584 urls whose current lists
  // name 1,754 drew a tenth of its own corpus.
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

  // Absent, and the tree is exactly what it always was — a router older than
  // this member never measured that corpus and must not be shown a zero row
  // claiming it did.
  const old = shrunk({ recordedOnly: undefined });
  assert.equal(old.total, 1754);
  assert.equal(old.rows.some((r) => r.key === "recordedOnly"), false);
  ok("the tree's mouth is every url the router knows of, not what one derivation named");
}

{
  // A NODE WHOSE CHILDREN DO NOT SUM TO IT gets a named child rather than a
  // short bar — any arithmetic slip, and any reason list either side truncated,
  // surfaces as a row in the fault tone instead of quietly shrinking the tree.
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
  // ABSENT IS NOT ZERO. A pass that publishes none of the three verdict members
  // measures no verdicts — the alias fold, and any router older than the
  // partition — and read as zeroes, every url WITH a verdict lands in `not
  // accounted for`, in the fault tone, on a pass that is working. Caught in a
  // screenshot of the real card: 12,731 of the fold's 16,752 urls drawn as an
  // arithmetic error.
  assert.equal(funnelOf({
    name: "aliasFold", phase: "idle", sourced: 17584, heldOutDead: 832,
    streams: [{ name: "all streams", candidates: 16752, unmeasured: 4021, dialled: 2000, decided: 118 }],
  }), null, "a pass that publishes no partition is not given one");

  assert.equal(funnelOf(null), null);
  assert.equal(funnelOf({ name: "consistency" }), null, "a row with no streams has no tree");
  assert.equal(funnelOf({ name: "consistency", streams: [{ candidates: 0, consistent: 0 }] }), null,
    "an empty candidate set is not a tree of zeroes");

  // With no `sourced` the root is what can still be accounted for, rather than
  // a mouth invented for it.
  const bare = funnelOf({ name: "consistency", streams: [{ candidates: 40, consistent: 10, unmeasured: 30 }] });
  assert.equal(bare.total, 40);
  assert.equal(bare.rows.find((r) => r.key === "dropped").value, 0);

  // Summed across rows, never `streams[0]` — the bug that shipped on the line
  // this tree sits under.
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
  // A reason and a hostname are free text off the wire, and both are used as
  // KEYS for the tone lookup. Reaching Object.prototype hands back a function,
  // which renders as a class name and throws the row's colours away.
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
  // `never answered a REQ` covers four findings with four different responses,
  // and the router publishes them as a FLAT list of rows naming the reason they
  // refine — nesting on the wire would put the one property the tree rests on,
  // that the rows sum to `unmeasured`, at the mercy of a shape.
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

  // THE PARENT IS SYNTHESISED, because it has no urls of its own: every url it
  // covers is already in a child, and a published row for it beside them would
  // double-count the lot.
  assert.equal(at("never answered a REQ").value, 700, "the sum of its children, not a published number");
  assert.equal(at("never answered a REQ").depth, 3);
  assert.equal(at("the name does not resolve").depth, 4, "a refinement sits under what it refines");
  assert.equal(at("the name does not resolve").largest, 20, "and its widest host is a number on it, not a row under it");
  assert.equal(f.rows.some((r) => r.depth > 4), false, "a refinement is the deepest thing drawn");

  // Widest first among siblings, and the synthesised parent competes on its own
  // total rather than on whichever child happened to be published first.
  assert.deepEqual(
    f.rows.filter((r) => r.depth === 3).map((r) => r.key),
    ["never answered a REQ", "too few events to judge on"],
  );

  // The partition still closes: `unmeasured` is 900 and its children are 700 +
  // 200, so nothing is unattributed.
  assert.equal(f.rows.some((r) => r.key === "unattributed"), false);
  ok("a row that refines another is nested under it, and the parent is summed rather than trusted");
}

{
  // A refinement whose parent nothing else claims still stands on its own — a
  // router that publishes one sub-cause and no siblings must not have it
  // vanish into a group that was never opened.
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
  // The relay recomputes both identities on the way out, and the tree carries
  // its verdict — because `unattributed` can only report children that fall
  // SHORT of their parent. Rows that OVERSHOOT leave no slice at all, which is
  // the shape a document carrying both a group and its children produces.
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
  // THE COMPLAINT this answers: one rotating pool runs four workloads, and
  // every number describing it added them together. `visiting: 100` covered a
  // catch-up, a history audit and a whole-corpus re-walk — a mirror keeping up
  // and one re-downloading years read identically — while `tails: 412` counted
  // the fourth and named nobody.
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  const doc = {
    streams: [
      { name: "content", inFlight: { relays: [
        held("wss://a.example/", { doing: "catching up (paging)", pool: POOL_CATCHING_UP, quietForSec: 1 }),
        held("wss://b.example/", { doing: "auditing history (negentropy)", pool: POOL_AUDITING, quietForSec: 900 }),
      ], omitted: 0 } },
      { name: "indexers", inFlight: { relays: [
        held("wss://c.example/", { doing: "re-fetching the past (paging)", pool: POOL_REFETCHING }),
        held("wss://d.example/", { doing: "catching up (paging)", pool: POOL_CATCHING_UP, quietForSec: 400 }),
        held("wss://e.example/", { doing: "claiming the socket" }),
      ], omitted: 0 } },
    ],
    live: { relays: [held("wss://f.example/", { doing: "holding a live tail", pool: POOL_LIVE })], omitted: 0 },
  };
  const pools = poolsOf(doc);
  const at = (key) => pools.groups.find((g) => g.key === key);

  assert.deepEqual(pools.groups.map((g) => g.key), [...POOL_ORDER, POOL_BETWEEN],
    "the four the router names, in one order, and the leftovers last");
  assert.deepEqual(at(POOL_CATCHING_UP).rows.map((r) => r.relay), ["wss://d.example/", "wss://a.example/"],
    "quietest first ACROSS streams — the merge interleaves two lists that were each sorted alone");
  assert.deepEqual(at(POOL_AUDITING).rows.map((r) => r.relay), ["wss://b.example/"]);
  assert.deepEqual(at(POOL_REFETCHING).rows.map((r) => r.relay), ["wss://c.example/"]);
  assert.deepEqual(at(POOL_LIVE).rows.map((r) => r.relay), ["wss://f.example/"]);

  // NOTHING IS DROPPED. A visit between jobs publishes no pool word, and the
  // row an operator is chasing — held for an hour, still "claiming the socket"
  // — is exactly the one a four-pool panel could lose.
  assert.deepEqual(at(POOL_BETWEEN).rows.map((r) => r.relay), ["wss://e.example/"]);
  assert.equal(pools.groups.reduce((a, g) => a + g.rows.length, 0), 6, "every row published is in exactly one group");

  // The stream a row came from rides on the ROW: one visit serves every
  // stream's asks over one dial, so a pool spans streams.
  assert.deepEqual(at(POOL_CATCHING_UP).rows.map((r) => r.stream), ["indexers", "content"]);
  // …and a tail belongs to no stream at all — it carries every wanting
  // stream's filter — which is a fact about tails and not a missing value.
  assert.equal(at(POOL_LIVE).rows[0].stream, null);
  assert.equal(at(POOL_LIVE).streams, false, "so the live table draws no stream column");
  assert.equal(at(POOL_CATCHING_UP).streams, true);
  ok("every held relay lands in exactly one pool, quietest first, and none is dropped for want of a word");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // A COLUMN WHOSE EVERY CELL READS THE SAME IS NOT A COLUMN. The pool's word
  // is lifted into its heading when the rows agree, and kept as a column where
  // they do not — the audit pool's two stages are a history sweep and a
  // provider's retraction comparison, which is the distinction worth the width.
  const one = poolsOf({ streams: [{ name: "content", inFlight: { relays: [
    held("wss://a.example/", { doing: "catching up (paging)", pool: POOL_CATCHING_UP }),
    held("wss://b.example/", { doing: "catching up (paging)", pool: POOL_CATCHING_UP }),
  ], omitted: 0 } }] });
  assert.equal(one.groups.find((g) => g.key === POOL_CATCHING_UP).doing, "catching up (paging)");

  const two = poolsOf({ streams: [{ name: "content", inFlight: { relays: [
    held("wss://a.example/", { doing: "auditing history (negentropy)", pool: POOL_AUDITING }),
    held("wss://b.example/", { doing: "auditing the provider's own records (negentropy)", pool: POOL_AUDITING }),
  ], omitted: 0 } }] });
  assert.equal(two.groups.find((g) => g.key === POOL_AUDITING).doing, null, "two stages keep their column");
  ok("a pool's stage word is lifted out of the table only where every row agrees on it");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // EMPTY IS AN ANSWER. "No relay is auditing right now" is a finding; a pool
  // that vanished when it emptied would be indistinguishable from a build with
  // no such pool, which is the reading the four names exist to prevent.
  const sparse = poolsOf({ live: { relays: [held("wss://f.example/", { pool: POOL_LIVE })], omitted: 0 } });
  assert.deepEqual(sparse.groups.map((g) => g.key), POOL_ORDER, "the four are always drawn…");
  assert.deepEqual(sparse.groups.find((g) => g.key === POOL_AUDITING).rows, []);
  // …and the fifth group is not one of them: an empty `between` is the healthy
  // case, and a heading for it on every tick is a mark that never varies.
  assert.equal(sparse.groups.some((g) => g.key === POOL_BETWEEN), false);

  // Holding nothing at all is the one state where four empty tables say less
  // than no panel.
  assert.equal(poolsOf({ streams: [{ name: "content" }] }), null);
  assert.equal(poolsOf(null), null);

  // WHAT NO POOL CAN ACCOUNT FOR is summed, never attributed: a row the router
  // left out has no pool by definition, and filing it under one would invent
  // the fact that is missing.
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
  // A word off the wire is not a heading. A router naming a pool this page has
  // not been taught puts the row with the unpooled rather than inventing a
  // group from a string — and `__proto__` cannot reach Object.prototype
  // through the label map on the way.
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
  // ONE POOL, shared by every visit-mode stream, so its SIZE is a count of
  // urls off the rotating pool's own row. Adding the stream rows' `roster`
  // shares would double-count every relay two streams both want, which on this
  // deployment is most of them.
  const doc = {
    processors: [
      { name: "ingest", queued: 3 },
      { name: "visits", roster: 412, awaitingVisit: 7, visiting: 3, tails: 2 },
    ],
    streams: [
      { name: "content", inFlight: { relays: [
        held("wss://a.example/", { pool: POOL_CATCHING_UP }),
        held("wss://b.example/", { pool: POOL_AUDITING }),
      ], omitted: 0 } },
      { name: "indexers", inFlight: { relays: [held("wss://c.example/", { pool: POOL_REFETCHING })], omitted: 0 } },
    ],
    live: { relays: [held("wss://f.example/", { pool: POOL_LIVE }), held("wss://g.example/", { pool: POOL_LIVE })], omitted: 0 },
  };
  const { totals } = poolsOf(doc);
  assert.equal(totals.roster, 412, "the pool's whole world, from the pool's own row");
  assert.equal(totals.working, 3, "counted off the ROWS, so the summary cannot disagree with the tables under it");
  assert.equal(totals.queued, 7);
  assert.equal(totals.waiting, 402, "the remainder: on a revisit timer, neither running nor queued");
  assert.equal(totals.roster, totals.working + totals.queued + totals.waiting, "those three partition the roster");
  // …and the tail count crosses them rather than joining them: a tailed relay
  // keeps its tail while it is revisited, so it is in both at once.
  assert.equal(totals.tailed, 2);
  ok("the pool's size is one number off one row, and three of the four marks partition it");
}

{
  const held = (relay, over = {}) => ({
    relay, heldForSec: 60, transferringForSec: 60, events: 10, quietForSec: 5, ...over,
  });
  // A ROUTER THAT PUBLISHES NO POOL ROW IS NOT A ROUTER WITH AN EMPTY POOL.
  // `0 in the pool` beside a table of relays is the arithmetic that gets a
  // whole panel disbelieved, so the total goes away instead.
  const silent = poolsOf({ streams: [{ name: "content", inFlight: { relays: [held("wss://a.example/", { pool: POOL_CATCHING_UP })], omitted: 0 } }] }).totals;
  assert.equal(silent.roster, null);
  assert.equal(silent.queued, null);
  assert.equal(silent.waiting, null, "no roster is no remainder to compute");
  assert.equal(silent.working, 1, "what the rows say is still said");
  assert.equal(silent.tailed, 0);

  // The three counts are read at one tick but not one instant, so a roster
  // that shrank between them can leave the subtraction short. `-2 between
  // visits` reads as a bug in the router rather than as the rounding it is.
  const raced = poolsOf({
    processors: [{ name: "visits", roster: 1, awaitingVisit: 4 }],
    streams: [{ name: "content", inFlight: { relays: [held("wss://a.example/", { pool: POOL_CATCHING_UP })], omitted: 0 } }],
  }).totals;
  assert.equal(raced.waiting, 0, "the remainder floors at zero rather than going negative");
  ok("a pool the document does not size says nothing, and a raced subtraction never goes negative");
}
