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
  DISCOVERY, DISPOSITION, HEAP_TIGHT, HEARTBEAT_STALE_SEC, IN_FLIGHT_SHOWN, STUCK_LEG_SEC,
  constraintOf, dividesOn, gaugesOf, heldCount, isLive, legsOf, meterOf, partitionOf, passHeld, passLabelOf,
  pendingMeaning, sparkOf, trendOf, TREND_NOISE,
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
  assert.equal(c.text, "store is the limit");
  assert.equal(c.tone, "warn", "ingest is the only one of the four that is a fault");
  assert.match(c.why, /Look at the store, not at the relays/);
  assert.equal(constraintOf({ bottleneck: "downloads" }, true).tone, null, "the relays being the limit is not a fault");

  // A verdict on a router that has stopped is a post-mortem, not a diagnosis.
  assert.equal(constraintOf({ bottleneck: "ingest" }, false).text, "store is the limit, when it stopped");

  // A word this page has not been taught still says something rather than
  // rendering as undefined — the relay allowlists it, but the card is served to
  // whoever asks.
  assert.equal(constraintOf({ bottleneck: "novel" }, true).text, "novel");
  ok("the constraint is guarded on its own member, and reads past-tense once the router stops");
}

// ── the gauges ──────────────────────────────────────────────────────────────
{
  // THE BUG: the pairs are copied independently, so a document can carry a
  // ceiling with no reading — and `heap ${undefined / 2048}` renders "NaN%",
  // which reads as a broken page rather than as a missing number.
  assert.deepEqual(gaugesOf({ heapMaxMb: 2048 }), [], "a ceiling with no reading draws nothing");
  assert.deepEqual(gaugesOf({ heapUsedMb: 900 }), [], "and a reading with no ceiling is not a percentage");
  assert.deepEqual(gaugesOf({ socketCeiling: 1024 }), []);
  assert.deepEqual(gaugesOf(null), []);

  // …but a missing verdict must not cost a good reading.
  const g = gaugesOf({ heapUsedMb: 512, heapMaxMb: 2048 });
  assert.equal(g.length, 1);
  assert.equal(g[0].pct, 25);
  assert.equal(g[0].tone, null);

  const tight = gaugesOf({ heapUsedMb: 2048 * HEAP_TIGHT, heapMaxMb: 2048 })[0];
  assert.equal(tight.tone, "warn", "at the ceiling the collector runs continuously and nothing fails");

  const sock = gaugesOf({ sockets: 1024, socketCeiling: 1024 })[0];
  assert.equal(sock.tone, "warn", "every new leg now waits for one to close");
  // Zero is a real reading, not an absent one: a router serving nothing has a
  // 0ms mean client read, and `0 ev/s` is the upstream constraint's evidence.
  assert.equal(gaugesOf({ eventsPerSec: 0, servingMs: 0 }).length, 2);
  ok("a gauge needs both halves of its pair, and zero is a reading");
}

// ── the partitions ──────────────────────────────────────────────────────────
{
  const segs = partitionOf(DISPOSITION, { delivered: 25, pending: 75, noRoute: 0 }, 100);
  assert.deepEqual(segs.map((s) => s.member), ["delivered", "pending"], "zero is not a segment");
  assert.deepEqual(segs.map((s) => s.share), [0.25, 0.75]);
  // The whole is PUBLISHED, never the sum: when they disagree the card says so
  // rather than rescaling to hide it, so the shares must not be renormalised.
  const short = partitionOf(DISPOSITION, { delivered: 25 }, 100);
  assert.equal(short[0].share, 0.25, "a partition that does not fill its total must under-fill");
  // A total of zero cannot divide by itself.
  assert.equal(partitionOf(DISPOSITION, { delivered: 5 }, 0)[0].share, 5);
  assert.deepEqual(partitionOf(DISPOSITION, null, 10), []);
  ok("segments are the non-zero members, over a total that is published rather than summed");
}

{
  // THE BUG: on a stream whose relay list is named by hand nothing folds,
  // nothing is refused and nothing is excluded — so the bar was one full-width
  // segment reading "5 taken on", which is a rule with a number on it.
  assert.equal(dividesOn({ discovered: 5, taken: 5 }), false);
  assert.equal(dividesOn({ discovered: 5, foldedOntoAnother: 0, refusedUnstable: 0, excluded: 0, taken: 5 }), false);
  assert.equal(dividesOn({ discovered: 200, foldedOntoAnother: 40, taken: 160 }), true);
  assert.equal(dividesOn({}), false);
  assert.equal(dividesOn(null), false);
  ok("the url partition is drawn only where it actually divides");
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
  const many = legsOf({ relays: Array.from({ length: 8 }, (_, i) => leg(i, 30)), omitted: 12 });
  assert.equal(many.rows.length, IN_FLIGHT_SHOWN);
  assert.equal(many.more, 15, "what this side drops is ADDED to what the router already left out");
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

// ── the passes ──────────────────────────────────────────────────────────────
{
  // THE BUG: `fmt` is `Number(n).toLocaleString()`, so the "?" fallback for an
  // unnumbered pass rendered as the string "NaN" — on the one label whose whole
  // job is to say which walk a bar belongs to.
  assert.equal(passLabelOf({}).number, "?");
  assert.equal(passLabelOf({ number: 0 }).number, "0", "pass zero is a pass");
  assert.equal(passLabelOf({ number: 12 }).number, "12");

  // A walk ends when its last url is handed out, not when its last worker
  // returns, so a finished pass with legs still running is the rotation working.
  assert.equal(passLabelOf({ outcome: "running" }).state, "walking");
  assert.equal(passLabelOf({ outcome: "completed", taken: { pending: 41 } }).state, "finishing");
  assert.equal(passLabelOf({ outcome: "completed", taken: { pending: 0 } }).state, "completed");
  assert.equal(passLabelOf({ outcome: "completed" }).outstanding, 0);
  // `dynamic` is the ordinary case and says nothing; `static` is the surprising
  // one, because a stream carrying both runs two walks under one name.
  assert.equal(passLabelOf({ owner: "dynamic" }).owner, null);
  assert.equal(passLabelOf({ owner: "static" }).owner, "static");
  assert.equal(passHeld(null), 0);
  ok("a pass says which walk it is, including the one nothing numbered");
}

{
  // THE BUG this reading replaced: a `completed` pass called 285 urls "never got
  // a verdict" directly above three of those 285 downloading at 20,000 events
  // each. The outcome alone cannot separate the two cases; what is still
  // RUNNING can.
  assert.equal(pendingMeaning("running", 0), null, "a running pass's pending needs no gloss");
  assert.equal(pendingMeaning(null, 0), null);
  assert.match(pendingMeaning("completed", 41), /still running/);
  assert.match(pendingMeaning("completed", 0), /ended before these got a verdict/);
  assert.equal(heldCount({ relays: [1, 2], omitted: 38 }), 40, "named and unnamed both count as held");
  assert.equal(heldCount(null), 0);
  ok("what `pending` means depends on whether anything is still running");
}

// ── the processor meters ────────────────────────────────────────────────────
{
  // THE BUG: a full bar meant "done" on the fold's row and "backpressured" on
  // ingest's, in the same column, in the same tone.
  const fold = meterOf({ streams: [{ candidates: 100, unmeasured: 40 }] });
  assert.equal(fold.kind, "progress");
  assert.equal(fold.share, 0.6);
  assert.equal(fold.left, 40);
  assert.equal(fold.tone, null, "progress is never a fault, however little of it there is");

  const queue = meterOf({ capacity: 4096, queued: 1024 });
  assert.equal(queue.kind, "level", "a queue depth is not a fraction completed");
  assert.equal(queue.share, 0.25);
  assert.equal(queue.tone, null, "a quarter-full queue is a healthy queue");

  const full = meterOf({ capacity: 4096, queued: 4096 });
  assert.equal(full.full, true);
  assert.equal(full.tone, "warn", "at the ceiling a level becomes a fault");
  // The router reports a depth ABOVE capacity — offers in flight are counted
  // before the queue accepts them — and a bar must not overflow its track.
  assert.equal(meterOf({ capacity: 4096, queued: 4101 }).share, 1);
  ok("progress and occupancy are different kinds of meter, and only one of them alarms when full");
}

{
  // THE BUG: capacity zero divided by itself into a NaN width.
  assert.equal(meterOf({ capacity: 0, queued: 0 }), null, "no denominator is not a full queue");
  // Different populations: `knownDead` is read from the store and `observed`
  // counts this process's sockets since boot. A restarted mirror reports 1,609
  // against 5, and any bar dividing them is arithmetic on unrelated sets.
  assert.equal(meterOf({ observed: 5, knownDead: 1609 }), null);
  assert.equal(meterOf({ pushed: 40 }), null, "a total with no target is not a proportion");
  assert.equal(meterOf({}), null, "built and never run — an empty meter would claim zero progress");
  assert.equal(meterOf(null), null);
  ok("nothing is drawn where there is no whole to be a part of");
}

// ── the two tables the card is a key to ─────────────────────────────────────
{
  // The stack is drawn from members that sum to their total BY CONSTRUCTION, so
  // a missing one does not fail — it under-fills, and the count disappears from
  // a card that still says the numbers add up. This is the JS side of the same
  // pin `SyncProgressReportTest` holds on the Kotlin side.
  assert.equal(DISPOSITION.length, 10, "ten outcomes close the partition");
  assert.deepEqual(DISCOVERY.map(([m]) => m), ["taken", "foldedOntoAnother", "refusedUnstable", "excluded"]);
  for (const [member, label] of [...DISPOSITION, ...DISCOVERY]) {
    assert.equal(typeof member, "string");
    assert.ok(label && label.length < 20, `${member}'s key label is short enough to sit on one line`);
  }
  ok("every outcome has a segment, and every label is short enough for the key");
}

// ── the last hour of a gauge ────────────────────────────────────────────────
{
  // `x` comes from each sample's OWN clock, not from its index. The rollup
  // cadence is an operator's env var and a restart leaves a hole, so evenly
  // spacing the points would draw a smooth line straight through a gap.
  const uneven = sparkOf({ at: [0, 10, 600], v: [1, 2, 3] }, "v");
  assert.deepEqual(uneven.points.map((p) => Number(p.x.toFixed(3))), [0, 0.017, 1],
    "ten seconds and ten minutes are not the same distance");
  assert.equal(uneven.span, 600);

  const rising = sparkOf({ at: [0, 60, 120, 180], v: [10, 20, 30, 40] }, "v");
  assert.deepEqual(rising.points.map((p) => Number(p.y.toFixed(2))), [0, 0.33, 0.67, 1]);
  assert.equal(rising.lo, 10);
  assert.equal(rising.hi, 40);
  ok("a sample sits where its clock puts it, scaled to the series' own range");
}

{
  // A queue 4,000 deep for an hour and one empty for an hour are both flat.
  // Drawing either along the floor says "nothing" about one and "zero" about
  // the other, so a flat series sits in the middle.
  const flat = sparkOf({ at: [0, 60, 120], v: [4000, 4000, 4000] }, "v");
  assert.deepEqual(flat.points.map((p) => p.y), [0.5, 0.5, 0.5]);
  assert.equal(flat.flat, true);
  assert.equal(trendOf(flat), "steady");
  assert.deepEqual(sparkOf({ at: [0, 60, 120], v: [0, 0, 0] }, "v").points.map((p) => p.y), [0.5, 0.5, 0.5]);
  ok("a flat series rides the middle, so empty and full do not both read as zero");
}

{
  // A gap is a HOLE. Joining across it draws a straight line through an outage,
  // which is the one shape a reader takes as evidence that nothing happened.
  const gap = sparkOf({ at: [0, 60, 120, 180], v: [10, null, 30, 40] }, "v");
  assert.equal(gap.points[1], null, "the router said nothing, which is not a value");
  assert.equal(gap.points[0].y, 0);
  assert.equal(gap.lo, 10, "the missing sample is not a zero pulling the floor down");
  ok("a missing sample is a hole in the line, not a dive to the floor");
}

{
  // One reading is a level, not a trend, and a lone dot on an empty track
  // invites reading its position as a value.
  assert.equal(sparkOf({ at: [0, 60], v: [5, null] }, "v"), null);
  assert.equal(sparkOf({ at: [0], v: [5] }, "v"), null, "one sample is where every series starts");
  assert.equal(sparkOf(null, "v"), null);
  assert.equal(sparkOf({ at: [0, 60] }, "v"), null, "a member the series does not carry");
  assert.equal(sparkOf({ at: [0, 60], v: [1, 2, 3] }, "v").points.length, 2, "values are read against the clocks");
  ok("nothing is drawn where there is no shape to draw");
}

{
  // Deliberately coarse: the reader has the shape in front of them, so a
  // sentence only earns its place for a move the eye would not already see.
  // A spike in the middle sets the range at 100; ending five above where it
  // started is a 5% move, inside the noise floor.
  const noisy = sparkOf({ at: [0, 60, 120], v: [100, 200, 105] }, "v");
  assert.ok((noisy.last - noisy.first) / (noisy.hi - noisy.lo) < TREND_NOISE);
  assert.equal(trendOf(noisy), "steady", "a wobble inside the noise floor is not a direction");
  assert.equal(trendOf(sparkOf({ at: [0, 60], v: [10, 90] }, "v")), "climbing");
  assert.equal(trendOf(sparkOf({ at: [0, 60], v: [90, 10] }, "v")), "falling");
  assert.equal(trendOf(null), "steady");
  ok("the trend word is only spent on a move worth a word");
}
