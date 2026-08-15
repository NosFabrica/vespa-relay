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
  HEARTBEAT_STALE_SEC, IN_FLIGHT_SHOWN, MEASURING, STUCK_LEG_SEC, constraintOf, funnelOf, isLive, legsOf,
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
  assert.equal(f.total, 17584, "the root is every url the streams named");

  // THE SHAPE. Depth is the relationship, so it is the thing to assert: a host
  // is under its reason, a reason under `no verdict`, that under the candidate
  // set, and the two dropped kinds under one branch of their own.
  assert.deepEqual(f.rows.map((r) => [r.depth, r.key]), [
    [0, "sourced"],
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
