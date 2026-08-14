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
  HEARTBEAT_STALE_SEC, IN_FLIGHT_SHOWN, STUCK_LEG_SEC, constraintOf, isLive, legsOf,
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
