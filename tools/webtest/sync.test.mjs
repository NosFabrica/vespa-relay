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
  HEARTBEAT_STALE_SEC, IN_FLIGHT_SHOWN, MEASURING, STUCK_LEG_SEC, constraintOf, isLive, legsOf,
  probeProgress,
} from "../../relay/src/main/resources/web/shared/sync.js";

const ok = (name) => console.log(`  ✓ ${name}`);

/** A leg as `RelayRotation.held` publishes one. */
const leg = (n, quiet, over = {}) => ({
  relay: `wss://r${n}.example/`, heldForSec: 3600, transferringForSec: 3595,
  events: 1000 * n, quietForSec: quiet, ...over,
});

// ── is it running ───────────────────────────────────────────────────────────
{
  assert.equal(isLive({ staleForSec: 3 }), true);
  assert.equal(isLive({ staleForSec: HEARTBEAT_STALE_SEC }), true, "the threshold itself is still alive");
  assert.equal(isLive({ staleForSec: HEARTBEAT_STALE_SEC + 1 }), false);
  // A document with no heartbeat is not a live router with an unknown age.
  assert.equal(isLive({}), false);
  assert.equal(isLive(null), false);
  ok("a heartbeat past the threshold, or missing, is not a running router");
}

// ── the constraint ──────────────────────────────────────────────────────────
{
  // THE BUG: the relay rebuilt `health` member by member against an allowlist,
  // so a router older than these gauges cleared every one of them and `{}` was
  // published anyway. The card guarded on the OBJECT, so it drew a chip with no
  // text in it beside the live one.
  assert.equal(constraintOf({}, true), null, "an empty health object names no constraint");
  assert.equal(constraintOf(null, true), null);
  assert.equal(constraintOf({ eventsPerSec: 900 }, true), null, "gauges are not a verdict");

  const c = constraintOf({ bottleneck: "ingest" }, true);
  assert.equal(c.text, "ingest is the limit");
  assert.equal(c.tone, "warn", "ingest is the only one of the four that is a fault");
  assert.match(c.why, /not at the relays/);
  assert.equal(constraintOf({ bottleneck: "downloads" }, true).tone, null, "the relays being the limit is not a fault");

  // A verdict on a router that has stopped is a post-mortem, not a diagnosis.
  assert.equal(constraintOf({ bottleneck: "ingest" }, false).text, "ingest is the limit, when it stopped");

  // A word this page has not been taught still says something rather than
  // rendering as undefined — the relay allowlists it, but the card is served to
  // whoever asks.
  assert.equal(constraintOf({ bottleneck: "novel" }, true).text, "novel");
  ok("the constraint is guarded on its own member, and reads past-tense once the router stops");
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
  ok("the pass draws what HAS a verdict, summed across rows and never negative");
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
    const c = constraintOf({ bottleneck: hostile }, true);
    assert.equal(c.text, hostile, `${hostile} is an unknown word, not a prototype member`);
    assert.equal(c.why, "");
  }
  ok("a bottleneck word this page has not been taught cannot reach Object.prototype");
}
