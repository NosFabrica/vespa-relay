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
  IN_FLIGHT_SHOWN, MEASURING, ROTATING, STUCK_LEG_SEC, constraintOf, funnelOf,
  heldOf, legsOf, measuringOf, passesOf, probeProgress, rotationOf,
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

  // ONE ROW, NEVER THE SUM. `unmeasured` is a standing count over a whole
  // candidate set, so the sweep's row and the fast lane's are two overlapping
  // views of one corpus — added, they count every url the two share twice. The
  // router says which row is the corpus; the widest is the fallback for a
  // document that predates the member.
  const two = probeProgress(fold({
    streams: [
      { name: "all streams", whole: true, candidates: 17000, unmeasured: 9000 },
      { name: "fast lane", whole: false, candidates: 16, unmeasured: 4 },
    ],
  }));
  assert.equal(two.candidates, 17000, "the corpus row, not it plus a slice of itself");
  assert.equal(two.checked, 8000);
  assert.equal(probeProgress(fold({ streams: [{ candidates: 17000, unmeasured: 9000 }, { candidates: 16, unmeasured: 4 }] })).candidates,
    17000, "…and with no `whole` published either way, the widest set");
  assert.equal(probeProgress(fold({ streams: [{ candidates: 10 }, {}] })).checked, 10,
    "a missing member is a zero on its row, not a NaN");
  assert.equal(probeProgress(fold()).newOnly, false, "a row that does not count new urls says so");
  ok("the pass draws what HAS a verdict, from the corpus row and never negative");
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

  // Read off the corpus row like every other member here — a lane tick's own
  // `newUrls` is what THAT run was handed, and adding it to the sweep's would
  // put a slice of the corpus into a denominator that is already the corpus.
  const mixed = probeProgress({
    name: "aliasFold", phase: "idle",
    streams: [
      { name: "all streams", whole: true, candidates: 100, newUrls: 40, unmeasured: 30 },
      { name: "fast lane", whole: false, candidates: 16, newUrls: 16, unmeasured: 4 },
    ],
  });
  assert.equal(mixed.candidates, 40);
  assert.equal(mixed.checked, 10, "30 of the sweep's 40 still undecided");
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
  //
  // WHAT IS NOT HERE any more is why the reasons under `no verdict` are not:
  // this tree is the CORPUS, true between passes and for weeks at a time, and
  // a breakdown of why one RUN could not decide belongs to that run. It is
  // `passesOf` below.
  const gate = (over = {}, row = {}) => ({
    name: "consistency", phase: "idle", lastPassSec: 9720,
    sourced: 17584, excluded: 3, heldOutDead: 829,
    streams: [{
      name: "all streams", whole: true, candidates: 16752, foldedAway: 11429, consistent: 583, inconsistent: 12,
      unmeasured: 4728, dialled: 4728, decided: 74,
      ...row,
    }],
    ...over,
  });

  const f = funnelOf(gate());
  const at = (key) => f.rows.find((r) => r.key === key);
  assert.equal(f.total, 17584, "the root is every relay url this router knows of");

  // THE SHAPE. Depth is the relationship, so it is the thing to assert: the
  // partition under the candidate set, and the two dropped kinds under one
  // branch of their own.
  assert.deepEqual(f.rows.map((r) => [r.depth, r.key]), [
    [0, "corpus"],
    [1, "dropped"], [2, "excluded"], [2, "heldOutDead"],
    [1, "candidates"], [2, "foldedAway"], [2, "consistent"], [2, "inconsistent"], [2, "unmeasured"],
  ]);

  // EVERY BAR AGAINST THE ROOT, never against the parent — against its parent a
  // slice of a subtree would draw at the width the whole corpus gets,
  // contradicting the indentation that already says it is deep in one.
  assert.equal(at("candidates").share, 16752 / 17584);
  assert.equal(at("unmeasured").share, 4728 / 17584);

  // THE GUIDES. A `│` is drawn at every ancestor that still has a sibling
  // below it, which is exactly the fact a flattened list loses — computed from
  // depth alone the tree still renders, with dangling verticals under the last
  // branch.
  assert.equal(at("dropped").prefix, "├─ ");
  assert.equal(at("excluded").prefix, "│  ├─ ", "inside a branch that is not the last");
  assert.equal(at("heldOutDead").prefix, "│  └─ ");
  assert.equal(at("candidates").prefix, "└─ ", "the last child of the root");
  assert.equal(at("unmeasured").prefix, "   └─ ", "…so nothing is drawn below it");

  // Tones are claims, and only one row on the whole tree is a fault.
  assert.equal(at("consistent").tone, "good");
  assert.equal(at("inconsistent").tone, "warn");
  assert.equal(at("foldedAway").tone, "mute", "a duplicate url leaving the fan-out is the fold working");
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
  // short bar — any arithmetic slip surfaces as a row in the fault tone
  // instead of quietly shrinking the tree.
  const f = funnelOf({
    name: "consistency", sourced: 100, excluded: 0, heldOutDead: 0,
    streams: [{ candidates: 100, foldedAway: 0, consistent: 10, inconsistent: 0, unmeasured: 40 }],
  });
  const short = f.rows.find((r) => r.key === "unattributed");
  assert.equal(short.value, 50, "the partition covers 50 of the 100 in reach");
  assert.equal(short.depth, 2, "…and it is a child of the node that did not close, not of the root");
  assert.equal(short.tone, "warn", "an unclosed partition must look wrong");
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

  ok("an absent partition is no tree and an absent member is a zero");
}

{
  // TWO ROWS ARE TWO VIEWS OF ONE CORPUS, NOT TWO HALVES OF IT.
  //
  // The passes measure the union of every stream and publish it as `all
  // streams` — and then the FAST LANE runs the same passes over the urls named
  // since its last look and records a second row beside it. Summed, the live
  // card drew `12,611` urls in reach under a round-up line reading `11,021
  // handed to the passes`, and every reason twice: `too few events to judge on`
  // at 309 beside `too few events to judge on` at 226.
  const sweep = {
    name: "all streams", whole: true, candidates: 11021, foldedAway: 6257, consistent: 2320, inconsistent: 6,
    unmeasured: 2438, dialled: 2438, decided: 74, tookSec: 180, endedAt: 1_776_038_400,
    undecided: {
      reasons: [
        { reason: "never answered a REQ", urls: 2129, hosts: 1204 },
        { reason: "too few events to judge on", urls: 309, hosts: 115 },
      ],
      omitted: 0,
    },
    accountedFor: true,
  };
  const lane = {
    name: "fast lane", whole: false, candidates: 1590, foldedAway: 800, consistent: 300, inconsistent: 0,
    unmeasured: 490, dialled: 490, decided: 12, tookSec: 12, endedAt: 1_776_049_200,
    undecided: {
      reasons: [
        { reason: "never answered a REQ", urls: 264, hosts: 151 },
        { reason: "too few events to judge on", urls: 226, hosts: 109 },
      ],
      omitted: 0,
    },
    accountedFor: true,
  };
  const f = funnelOf({ name: "consistency", sourced: 17808, excluded: 403, heldOutDead: 8632, streams: [sweep, lane] });
  const at = (key) => f.rows.find((r) => r.key === key);
  assert.equal(f.candidates, 11021, "the candidate set is the corpus the sweep walked, not it plus a slice of itself");
  assert.equal(at("foldedAway").value, 6257, "…and so is every member of the partition under it");
  assert.equal(f.rows.some((r) => r.key === "unattributed"), false, "the partition still closes");

  // The router SAYS which row is the corpus, whichever order they arrive in.
  assert.equal(funnelOf({ name: "consistency", sourced: 17808, streams: [lane, sweep] }).candidates, 11021);
  // …and with neither row claiming either way — a router older than `whole` —
  // the widest set is the fallback, the lane's being always a slice of it.
  assert.equal(funnelOf({
    name: "consistency", sourced: 17808,
    streams: [{ ...lane, whole: undefined }, { ...sweep, whole: undefined }],
  }).candidates, 11021);
  // A lane tick that has run and a sweep that has not is the one case the
  // fallback gets wrong and the published member gets right.
  assert.equal(funnelOf({
    name: "consistency", sourced: 17808,
    streams: [lane, { ...sweep, candidates: 0 }],
  }), null, "an empty corpus row is no tree, rather than the lane's slice drawn as one");
  ok("the tree is drawn from the row that walked the whole corpus, never from the sum of the rows");
}

// ── what one RUN of a pass did ──────────────────────────────────────────────
{
  // THE OTHER HALF OF THE SPLIT. The corpus tree above answers "what is the
  // state of every relay this router knows of", which is true between passes
  // and for weeks at a time. This answers "what happened when the pass last
  // ran" — a different question about a different population, and the two were
  // one chart nobody could read.
  const doc = {
    name: "consistency", phase: "idle",
    streams: [
      { name: "fast lane", whole: false, candidates: 1590, unmeasured: 490, dialled: 490, decided: 12,
        newUrls: 502, tookSec: 12, endedAt: 1_776_049_200,
        undecided: { reasons: [{ reason: "never answered a REQ", urls: 490, hosts: 151 }], omitted: 0 } },
      { name: "all streams", whole: true, candidates: 11021, unmeasured: 2438, dialled: 2438, decided: 74,
        tookSec: 180, endedAt: 1_776_038_400,
        undecided: {
          reasons: [
            { reason: "never answered a REQ", urls: 2129, hosts: 1204,
              top: [{ host: "dead.example", urls: 61 }, { host: "gone.example", urls: 44 }] },
            { reason: "too few events to judge on", urls: 309, hosts: 115, top: [{ host: "thin.example", urls: 12 }] },
          ],
          omitted: 2,
        } },
    ],
  };
  const blocks = passesOf(doc);
  assert.equal(blocks.length, 2, "one block per row — never merged, and never summed");
  assert.deepEqual(blocks.map((b) => b.name), ["all streams", "fast lane"],
    "the corpus row first: it is the run the tree above is about");

  const [sweep, lane] = blocks;
  // EVERY NUMBER IS THAT RUN'S OWN. This is what the one merged chart could not
  // say: the lane's 490 undecided and the sweep's 2,438 are two runs, not a
  // corpus and a contradiction of it.
  assert.equal(sweep.candidates, 11021);
  assert.equal(sweep.dialled, 2438);
  assert.equal(sweep.decided, 74);
  assert.equal(lane.candidates, 1590);
  assert.equal(lane.decided, 12);
  assert.equal(lane.whole, false, "…and each says which of the two readings it is");

  // ITS OWN CLOCK, which is the member that makes a stale row legible AS stale:
  // a lane tick sits in the document until the next tick replaces it, so two
  // rows on one card can be minutes and hours old with nothing saying which.
  assert.equal(sweep.tookSec, 180);
  assert.equal(sweep.endedAt, 1_776_038_400);
  assert.equal(lane.endedAt, 1_776_049_200, "three hours after the sweep, on the same card");

  // THE TREE IS ROOTED AT `unmeasured`, because that is the only node the
  // reasons partition — `dialled` and `decided` are spends rather than slices,
  // and a bar under them would be a subtotal of nothing.
  assert.deepEqual(sweep.rows.map((r) => [r.depth, r.key]), [
    [0, "unmeasured"],
    [1, "never answered a REQ"],
    [1, "too few events to judge on"],
  ]);
  assert.equal(sweep.rows[0].value, 2438);
  // …and against the run's OWN undecided count, not against a corpus it never
  // walked: scaled to the corpus, every row of a fast-lane block draws at a
  // width nobody can see.
  assert.equal(sweep.rows[1].share, 2129 / 2438);
  assert.equal(lane.rows[1].share, 490 / 490);

  // A REASON IS A LEAF. The hosts under it are published and deliberately not
  // drawn: one row per host is one row per SERVER on a corpus of two thousand
  // of them.
  assert.equal(sweep.rows.some((r) => r.key === "dead.example"), false, "no row per host");
  // What that list was FOR survives as two numbers on the reason's own row —
  // 2,129 urls on 1,204 hosts with the largest at 61 is a dead network spread
  // thin; the same urls with the largest at 2,000 would be one server.
  const req = sweep.rows.find((r) => r.key === "never answered a REQ");
  assert.equal(req.hosts, 1204);
  assert.equal(req.largest, 61, "the widest host's share, not a list of them");
  assert.deepEqual(req.examples, ["dead.example", "gone.example"],
    "…and the names ride along for the row's title, which costs no height");
  assert.equal(req.unnamed, 1202, "the names it did NOT fit are a count, because 1,204 hosts arrived as two names");
  assert.equal(req.tone, null, "a relay that will not answer is not our fault");

  // Whatever the ROUTER dropped is carried through, on the block that dropped
  // it: a truncated breakdown that does not disclose its cut reads as whole.
  assert.equal(sweep.omitted, 2);
  assert.equal(lane.omitted, 0);
  ok("each run is its own block, with its own clock, its own numbers and its own reasons");
}

{
  // NOTHING LEFT UNDECIDED IS NOT AN EMPTY CHART. It is the state both passes
  // work towards and hold for most of a monthly TTL, and a root row reading
  // `left with no verdict 0` is a line to read past on every poll forever.
  const [done] = passesOf({ name: "consistency", streams: [{ name: "all streams", candidates: 900, unmeasured: 0, dialled: 4, decided: 4 }] });
  assert.deepEqual(done.rows, []);
  assert.equal(done.candidates, 900, "…and the block still states what the run was handed");

  // A row nothing ran over at all is not a block. A processor with no rows is
  // not a chart.
  assert.equal(passesOf({ name: "consistency", streams: [{ name: "all streams" }] }), null);
  assert.equal(passesOf({ name: "consistency", streams: [] }), null);
  assert.equal(passesOf(null), null);

  // Presence, not truthiness, on the two members that can honestly be zero: a
  // pass that has caught up publishes `newUrls: 0`, and a router that does not
  // time itself publishes no clock at all.
  const [caught] = passesOf({ name: "aliasFold", streams: [{ name: "all streams", candidates: 40, newUrls: 0, unmeasured: 4 }] });
  assert.equal(caught.newUrls, 0);
  assert.equal(caught.tookSec, null, "absent is a router that predates the member, not a pass that took no time");
  assert.equal(caught.endedAt, null);
  ok("a run with nothing left undecided draws no tree, and an absent member is not a zero");
}

{
  // ROWS THAT REFINE ANOTHER ROW GO UNDER IT. `never answered a REQ` covers
  // four findings with four different responses, and the router publishes them
  // as a FLAT list naming the reason they refine — nesting on the wire would
  // put the one property the tree rests on, that the rows sum to `unmeasured`,
  // at the mercy of a shape.
  const [b] = passesOf({
    name: "consistency",
    streams: [{
      name: "all streams", candidates: 1000, unmeasured: 900, dialled: 900, decided: 10,
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
  assert.deepEqual(b.rows.map((r) => [r.depth, r.key]), [
    [0, "unmeasured"],
    [1, "never answered a REQ"],
    [2, "the name does not resolve"],
    [2, "the connection was refused"],
    [1, "too few events to judge on"],
  ]);
  // The parent is SYNTHESISED from its children rather than published: it has
  // no urls of its own, and a published row beside them would double-count the
  // lot.
  assert.equal(b.rows.find((r) => r.key === "never answered a REQ").value, 700);
  assert.equal(b.rows.some((r) => r.key === "unattributed"), false, "700 + 200 closes the 900");

  // A refinement whose parent nothing else claims still stands on its own — a
  // router that publishes one sub-cause and no siblings must not have it
  // vanish into a group that was never opened.
  const [lone] = passesOf({
    name: "consistency",
    streams: [{
      name: "all streams", candidates: 100, unmeasured: 90, dialled: 90, decided: 0,
      undecided: { reasons: [{ reason: "the TLS handshake failed", parent: "never answered a REQ", urls: 90, hosts: 9 }], omitted: 0 },
    }],
  });
  assert.equal(lone.rows.find((r) => r.key === "never answered a REQ").value, 90);
  assert.equal(lone.rows.find((r) => r.key === "the TLS handshake failed").value, 90);

  // A REASON LIST THAT FALLS SHORT of its own `unmeasured` gets a named child
  // in the fault tone rather than a short bar — a truncated list must never
  // quietly shrink the tree.
  const [gap] = passesOf({
    name: "consistency",
    streams: [{
      name: "all streams", candidates: 100, unmeasured: 90, dialled: 90, decided: 0,
      undecided: { reasons: [{ reason: "never answered a REQ", urls: 40, hosts: 4 }], omitted: 3 },
    }],
  });
  const short = gap.rows.find((r) => r.key === "unattributed");
  assert.equal(short.value, 50, "the reasons cover 40 of the 90 left with no verdict");
  assert.equal(short.tone, "warn");
  assert.equal(gap.omitted, 3);
  ok("a refinement sits under what it refines, and a reason list that falls short says so");
}

{
  // A reason and a hostname are free text off the wire, and both are used as
  // KEYS for the tone lookup. Reaching Object.prototype hands back a function,
  // which renders as a class name and throws the row's colours away.
  for (const hostile of ["constructor", "toString", "__proto__", "hasOwnProperty"]) {
    const [b] = passesOf({
      name: "consistency",
      streams: [{ name: "all streams", candidates: 10, unmeasured: 8, dialled: 8, decided: 2,
        undecided: { reasons: [{ reason: hostile, urls: 8, hosts: 1, top: [{ host: hostile, urls: 8 }] }], omitted: 0 } }],
    });
    const rows = b.rows.filter((r) => r.key === hostile);
    assert.equal(rows.length, 1, "the reason; its hosts are numbers on it rather than rows");
    assert.equal(rows[0].tone, null, `${hostile} is unknown text, not a prototype member`);
  }
  ok("a reason or host this page has not been taught cannot reach Object.prototype");
}

{
  // The two reasons that are about US rather than about the relay keep their
  // own tone, on the block where they belong: a pass that could not carry a url
  // through our own transport is reporting a fact about this router.
  const [b] = passesOf({
    name: "consistency",
    streams: [{ name: "all streams", candidates: 10, unmeasured: 10, dialled: 0, decided: 0,
      undecided: { reasons: [
        { reason: "declined by our own transport", urls: 6, hosts: 2 },
        { reason: "the probe failed mid-walk", urls: 4, hosts: 1 },
      ], omitted: 0 } }],
  });
  assert.deepEqual(b.rows.filter((r) => r.depth === 1).map((r) => r.tone), ["ours", "ours"]);
  ok("a reason that is about this router is toned as ours, not as the relay's fault");
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
