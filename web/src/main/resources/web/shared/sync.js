// WHAT THE SYNC CARD DECIDES, apart from how it draws it.
//
// The card's live half is marks over the router's progress document, and the
// judgements behind them — is the router alive, where is the constraint, which
// legs are worth naming and when is one stuck — lived inline in stats.html,
// where nothing could reach them.
//
// The cost was measured rather than assumed. The only pins over that code were
// string greps — `SyncProgressReportTest` reads stats.html as text and asserts
// that each published member NAME appears somewhere in it — and a grep cannot
// see a wrong denominator. An audit against crafted documents found five bugs
// that had all shipped: an empty health object drawing an empty chip, a
// percentage of an absent numerator rendering `NaN%`, a quiet bar scaled by a
// row that was not on screen, a division by a zero capacity, and two meters in
// one column whose full ends meant opposite things.
//
// So the decisions live here, as functions over plain data, and stats.html is
// left with DOM and the glossary. Numbers come out; the page formats them and
// hangs the document's own words on them. `web/src/test/js/sync.test.mjs` is the
// half that can now be asserted.
//
// It is SMALL on purpose. The card's rebuild dropped the per-leg table, the
// outcome partition, the gauges and the per-relay coverage rows, and fourteen
// exports here went with them — they had survived only because their own tests
// still referenced them, which is how a module keeps answering questions
// nothing asks.

/**
 * Past this, a leg is not slow, it is stuck.
 *
 * Ten minutes, the same floor the router's own log line uses, and for the same
 * reason: the slowest HEALTHY leg measured on this deployment is the full
 * purplepag.es `indexers` walk at ~10.8 minutes for 1.49M events. Anything
 * lower marks legs doing exactly what they should.
 */
export const STUCK_LEG_SEC = 600;

/**
 * How many held relays the table names before deferring to the JSON.
 *
 * It was FIVE, which on a live fan-out named five legs and deferred five
 * hundred — enough to see that something was held and never enough to see
 * which, and the one being looked for is by definition not in the healthy
 * head of the list. The router caps its own rows at the widest admission gate
 * (`RelayRotation.DEFAULT_IN_FLIGHT_ROWS`), so this defers to what the
 * document carries rather than cutting it again; `more` still discloses
 * whatever the ROUTER left out.
 */
export const IN_FLIGHT_SHOWN = Infinity;

/**
 * Past this, a probe pass is not slow, it has stopped.
 *
 * Measured against what a unit costs rather than borrowed from `STUCK_LEG_SEC`:
 * a probe job is bounded by the monitor's per-url deadline — twelve idle
 * windows, four minutes at the default `connectionTimeout = 20` — so a pass
 * that has finished nothing in five is a pass whose remaining jobs are all
 * outliving a bound that is supposed to end them. Under it the number is
 * ordinary and the line is already long, which is why it is drawn only past it.
 */
export const STUCK_PASS_SEC = 300;

/**
 * How many held urls a processor's line names.
 *
 * Not `IN_FLIGHT_SHOWN`. That one is `Infinity` because a stream's legs are
 * bounded by its transfer pool and every row is interesting; a probe pass at
 * the monitor's default dial concurrency is holding five hundred urls, of which
 * 499 are ordinary dials a second old. The router sorts them longest-held
 * first, so the few at the front are the answer and `more` discloses the rest.
 */
export const HELD_SHOWN = 3;

/**
 * How many host names a monitor reason puts in its hover title.
 *
 * The one cut on this page that is a PRESENTATION cut rather than a data one,
 * and it exists because the two moved in opposite directions. The router now
 * publishes up to a hundred names per reason so `/stats.json` can answer which
 * servers will not fold — an inventory, deliberately. A native `title` is not
 * a place to put an inventory: it is one run of text, unwrapped, truncated by
 * some browsers at lengths they do not agree on.
 *
 * Twelve, measured rather than guessed. On production's widest reason — 186
 * hosts — the tooltip ran to 1,740 characters at a hundred names against 159
 * at six, and nothing about the longer one is more readable. Twelve is double
 * the old head and still one glance. What the row cannot show, it says: the
 * remainder is named as a count, and `hosts` on the visible label was always
 * the honest total.
 */
export const NAMES_IN_TOOLTIP = 12;

/**
 * WHERE THE CONSTRAINT IS — the first question a mirror that feels slow gets,
 * and the one the router answers itself every 60 seconds, in a line that used
 * to reach only a container's stderr.
 *
 * The four states are not degrees of one thing. They are different faults with
 * different fixes, so each names what to look at next.
 */
export const BOTTLENECK = {
  // The key is a word off the wire, and the relay allowlists it but the card is
  // served to whoever asks. Without this, `bottleneck: "constructor"` reaches
  // Object.prototype, and destructuring a function below throws out the render.
  __proto__: null,
  ingest: ["ingest is the limit", "Ingest's queue is full, so every download is backpressured behind it. Look at ingest and the store behind it, not at the relays."],
  downloads: ["relays are the limit", "Ingest drains as fast as it fills. The mirror is going as fast as the upstreams will serve it."],
  upstream: ["nothing arriving", "The queue is empty and no events are reaching it — look at discovery, the guards and the transport, not at ingest."],
  mixed: ["keeping up", "The queue is neither full nor empty: nothing here is the constraint."],
};

/*
 * THERE IS NO `isLive` HERE ANY MORE, and its absence is the point.
 *
 * The mirror used to write its state to a file the serving relay read, so the
 * page had to infer whether the writing process still existed: the document
 * carried a `writtenAt` heartbeat, the relay turned it into a `staleForSec`,
 * and this module decided at 150 seconds. The mirror serves its own page now.
 * A page that renders is a process that answered, so the question is asked by
 * the fetch and the three pieces of machinery that used to answer it — the
 * heartbeat, the threshold and the stale-verdict past tense below — are gone
 * rather than left computing a constant.
 */
/**
 * The constraint verdict, or null where the document does not carry one.
 *
 * Guarded on its OWN member rather than on the health object: every gauge is
 * copied independently against an allowlist on the relay side, so a document
 * can carry the numbers with no word or the word with no numbers, and each has
 * to stand without the others.
 *
 * Present tense, unconditionally. It used to have a past-tense form for a
 * router whose heartbeat had gone stale; a document served by the process it
 * describes cannot be in that state.
 */
export function constraintOf(health) {
  const word = health?.bottleneck;
  if (!word) return null;
  const [text, why] = BOTTLENECK[word] || [word, ""];
  return { word, text, why, tone: word === "ingest" ? "warn" : null };
}

/** The phase word a processor carries while a pass is dialling — `Processors.MEASURING`. */
export const MEASURING = "measuring";

/**
 * HOW FAR A PROBE PASS HAS GOT — the fold's row and the stability gate's, which
 * publish the same shape.
 *
 * The document's number is `unmeasured`, what still has NO verdict; this returns
 * its COMPLEMENT, which rises as the pass gets somewhere. The two read in
 * opposite directions, so the subtraction lives here rather than in the page.
 *
 * Clamped at zero because `SyncProgressReport` defaults `unmeasured` to
 * `candidates` on an unreadable row. Summed rather than `streams[0]`, which once
 * reported a 16-url stream's residue as the whole picture. `lastPassSec` is
 * withheld while a pass runs: it belongs to the previous one.
 *
 * **A FOLDED URL IS NOT A CHECKED ONE, and is out of BOTH halves.** The row's
 * partition is `candidates = foldedAway + consistent + inconsistent +
 * unmeasured`, so the bare complement of `unmeasured` counts every url the fold
 * removed as one the stability gate checked — which is the opposite of what the
 * gate does with them: a folded url is deliberately never dialled, because it is
 * another relay's second address. On the real card that read `12,024 of 16,752
 * relay(s) checked for consistency` beside a tree showing 583 consistent and 12
 * inconsistent, from the same document, in the same tick. The gate's own line is
 * `595 of 5,323`, and the missing 11,429 are named one row above it.
 *
 * Only where the row publishes `foldedAway` at all — the fold's own row measures
 * no folds away from itself, and there the complement is exactly right.
 *
 * **WHERE THE ROW SAYS HOW MANY URLS ARRIVED UNDECIDED, that is the denominator
 * instead, and `newOnly` says so** — the caller has a word to add. Neither half
 * of the older pair described the PASS: the denominator was every url it was
 * handed, most of which carry a verdict from weeks ago that nothing re-asks
 * until it ages out, and the numerator was every url that holds one at all —
 * folds made a month ago in another process included. On the real card a fold
 * that had just run for eleven minutes read `143 of 1,754 relay(s) checked`,
 * and neither number moved with the work. `newUrls` is the set the pass is FOR
 * and `unmeasured` is that same set once it has run, so the pair is a fraction
 * of one population: of the urls that arrived undecided, how many left decided.
 */
export function probeProgress(p) {
  const streams = p?.streams || [];
  if (!streams.length) return null;
  const sum = (member) => streams.reduce((a, w) => a + (w[member] || 0), 0);
  const folded = sum("foldedAway");
  // Presence, not truthiness: a pass that saw no new urls publishes zero, and
  // that is an answer — `|| ` there would silently fall back to the whole
  // candidate set exactly when the fold has caught up with the corpus.
  const fresh = streams.some((w) => w.newUrls != null) ? sum("newUrls") : null;
  const candidates = fresh ?? Math.max(0, sum("candidates") - folded);
  const unmeasured = sum("unmeasured");
  return {
    candidates,
    checked: Math.max(0, candidates - unmeasured),
    newOnly: fresh != null,
    tookSec: p.phase === MEASURING ? null : (p.lastPassSec ?? null),
  };
}

/**
 * WHERE THE PASS RUNNING RIGHT NOW HAS GOT TO — the live half of `probeProgress`.
 *
 * `probeProgress` reads the row the last pass LEFT, which is the right answer
 * for twenty-nine days of a monthly TTL and the wrong one for the hours a pass
 * is actually running: the numbers stand still, `lastPassSec` is withheld
 * because it belongs to the pass before, and the sweep unsets `nextInSec` while
 * it runs because nothing has computed when the next one is due. The row said
 * `measuring` and carried no size, no position and no end.
 *
 * Returns null unless the router published a real denominator. A share of zero
 * candidates is the division this module exists to keep out of the page, and a
 * position with nothing to be a position IN is worse than the phase word alone.
 *
 * `attempted` is clamped INTO the denominator rather than trusted: the two are
 * read at the same instant from the same entry, but they are read off a live
 * pass, and `4,729 of 4,728` is a rendering bug rather than a finding.
 *
 * NO `share`. This returned one — the position as a 0..1 fraction, ready for a
 * bar — and nothing drew it: the card states the pair in words, and the bar
 * would have to live in a three-column grid whose third column is already the
 * row's facts. A computed member with one caller in its own test is how this
 * module grew the fourteen exports its rebuild deleted.
 */
export function measuringOf(p) {
  const m = p?.measuring;
  if (!m || !(m.toProbe > 0)) return null;
  return {
    unit: m.unit || "url",
    attempted: Math.max(0, Math.min(m.attempted || 0, m.toProbe)),
    toProbe: m.toProbe,
    // `??`, not `||`: the router omits this until a unit has landed and again
    // once the last one has, and both absences are "no estimate" — where a
    // zero would be a claim that the pass is done.
    etaSec: m.etaSec ?? null,
    // HOW LONG SINCE A UNIT LAST ENDED, and the reason it is here rather than
    // inferred: `etaSec` reads 0 both for a pass one url from done and for a
    // pass whose last url has wedged, so the estimate alone cannot tell them
    // apart. `??` again — absent is a router that predates the member, not a
    // pass that just moved.
    quietForSec: m.quietForSec ?? null,
  };
}

/**
 * WHICH URLS A PROBE PASS IS HOLDING — `legsOf` for a job that is a ladder
 * rather than a transfer.
 *
 * Its own reader rather than a second caller of that one, because the rows are
 * a different shape and the difference is the point: a stream leg is decided by
 * whether events are still arriving, and a probe leg has no events to speak of
 * and is decided by which STEP it is on. Reusing `legsOf` would draw `0 events,
 * quiet 0s` beside every row, which reads as a stalled transfer.
 *
 * The router sorts LONGEST-HELD FIRST — the reverse of a stream's legs, because
 * a probe leg is bounded by a deadline and a long one is the anomaly — so the
 * first row is the one to draw when there is room for one.
 */
export function heldOf(inFlight, limit = IN_FLIGHT_SHOWN) {
  const all = inFlight?.relays || [];
  const rows = all.slice(0, limit).map((r) => ({
    relay: r.relay,
    // The scheme is dropped and nothing else is, exactly as in `legsOf`: a
    // truncated relay url is not a relay url, and it is the thing being
    // looked up.
    short: String(r.relay || "").replace(/^wss?:\/\//, ""),
    heldForSec: r.heldForSec || 0,
    // Null on a router that predates the member, which reads as "not known"
    // and never as a step.
    stage: r.stage || null,
  }));
  return { rows, more: (inFlight?.omitted || 0) + (all.length - rows.length) };
}

/*
 * THERE IS NO `splitProcessors` HERE ANY MORE.
 *
 * Both planes' rows used to arrive in one `processors` array, because one
 * `Processors` object served both, so the page had to sort them by name into
 * the mirror's card and the monitor's — with an allowlist, and with anything
 * unrecognised routed to the mirror's card rather than dropped, since dropping
 * a row to keep a card tidy is how a new job runs unwatched for a year.
 *
 * Each plane keeps its own report and publishes its own document now. A row
 * belongs to the object that registered it, the sort has nothing left to sort,
 * and the allowlist that could go stale against a newly registered processor
 * is gone with it.
 */

/** The phase word a visit-mode stream carries — `StreamPhases.Phase.Rotating`. */
export const ROTATING = "rotating";

/**
 * WHAT A ROTATING STREAM IS ACTUALLY RIDING, which its row could not say.
 *
 * A stream's engine is the pool and its phase lasts the life of the process, so
 * the row rendered as `rotating for 58m` and nothing else. That line is the
 * same whether the stream is riding four hundred relays or none, and "none" is
 * the state worth seeing: before the fitness pass has signed its first `prime`,
 * a stream on discovered relays is a stream with an empty world.
 *
 * So `waiting` is called here rather than left to the page: it is the one
 * reading that changes what an operator does next, and it is a judgement about
 * a number rather than a number.
 */
export function rotationOf(s) {
  if (s?.phase !== ROTATING || s.roster == null) return null;
  return { roster: s.roster, tails: s.tails ?? null, waiting: s.roster === 0 };
}

/**
 * What each slice of the funnel MEANS — keyed by the router's own words.
 *
 * The partition's members are ours and glossed by the document, but the
 * `undecided` reasons are free text off the wire, so the tone is looked up
 * rather than derived: an unrecognised reason draws neutral and still gets its
 * segment, because a slice the page cannot colour is not a slice it may drop.
 *
 * `__proto__: null` for the same reason `PROBE_FOR` has it: the key is a string
 * a router chose, and `constructor` must not resolve to a function.
 */
const FUNNEL_TONE = {
  __proto__: null,
  // The two verdicts. Only one of them is a fault, and it is the router's
  // fault to report rather than the relay's to be blamed for — see the
  // glossary's `inconsistent`.
  consistent: "good",
  inconsistent: "warn",
  // Neither a fault nor a finding: a duplicate url leaving the fan-out is the
  // fold working, a url held out on a signed record is one we already measured,
  // and a url only our records know is one nobody asked for this round.
  foldedAway: "mute",
  heldOutDead: "mute",
  recordedOnly: "mute",
  // Ours, in both senses: we could not carry it, or our probe broke.
  "declined by our own transport": "ours",
  "the probe failed mid-walk": "ours",
  // The arithmetic not closing is neither of those and must LOOK wrong.
  unattributed: "warn",
};

/**
 * EVERY DISCOVERED URL, ONCE, INTO WHAT BECAME OF IT — as a tree.
 *
 * ## Why a tree and not the stacked levels this replaces
 *
 * It was an icicle: one row per level, every level a share of one width, a
 * child sitting under the parent it subdivides. It was correct and it needed
 * three captions and a legend to say what indentation says for free — the
 * nesting was carried by horizontal offset, which is the one visual channel
 * already spent on proportion. Rendered on the real card, the levels read as
 * four unrelated bars.
 *
 * The same numbers as `parent → children` need no captions: depth IS the
 * relationship, the label sits next to its own count, and a fifth level costs
 * one more indent rather than a new alignment rule. What the icicle was good at
 * — comparing two slices at a glance — is kept as a bar per row, all against
 * the SAME root total, so a host under a reason is still visibly a sliver of
 * the corpus and not of its parent.
 *
 * ## Why this and not the one number beside it
 *
 * `probeProgress` answers "how much has a verdict", which on a discovered
 * corpus sits at a few hundred out of several thousand and reads as a gate that
 * is stuck. It is not: the pass dials its whole set every time, and most of
 * that set is urls that cannot be measured at all — dead hosts, auth walls,
 * relays holding nine events. Those are different problems with different
 * fixes, and they were one undifferentiated number.
 *
 * ## The rules it is held to
 *
 * **A node whose children do not sum to it gets an `unattributed` child rather
 * than a short bar.** Any arithmetic slip, and any reason list either side
 * truncated, surfaces as a named row in the fault tone instead of quietly
 * shrinking the tree.
 *
 * **Absent is not zero.** A pass that publishes none of the three verdict
 * members measures no verdicts — the alias fold, and any router older than the
 * partition — and gets NO tree, rather than one claiming every url it checked
 * is unaccounted for. That bug shipped and a screenshot of the real card caught
 * it.
 *
 * **Nothing is invented from a missing member**, and a subtree nobody can fill
 * simply does not appear.
 */
export function funnelOf(p) {
  const streams = p?.streams || [];
  if (!streams.length) return null;
  const sum = (member) => streams.reduce((a, w) => a + (w[member] || 0), 0);
  const candidates = sum("candidates");
  if (!candidates) return null;
  // ABSENT IS NOT ZERO, and this is the one place in the module where the
  // difference is load-bearing — see the header. `sum` cannot tell a missing
  // member from a real zero, so the question is asked of the rows directly.
  if (!streams.some((w) => w.foldedAway != null || w.consistent != null || w.inconsistent != null)) return null;

  const excluded = Math.max(0, p.excluded || 0);
  const heldOutDead = Math.max(0, p.heldOutDead || 0);
  const dropped = excluded + heldOutDead;
  // …AND WHAT THE STREAMS DID NOT NAME. A url leaves the relay lists for
  // reasons of its own — the author who listed it revised their 10002, a source
  // was reconfigured — and every measurement this router took of it is still in
  // the store, still read by the fold. Rooted at `sourced` alone the tree lost
  // those without a word, on a card whose caption says "every relay url this
  // router knows of": a deployment holding records for five figures of urls
  // whose current lists name a couple of thousand drew an eighth of its corpus.
  const recordedOnly = Math.max(0, p.recordedOnly || 0);
  // The root: everything this router knows of. `sourced` is the honest count of
  // what was named when the router publishes it; without it the root is what we
  // can still account for, and the tree simply starts lower rather than
  // inventing a mouth.
  const total = Math.max(candidates + dropped, p.sourced || 0) + recordedOnly;

  /** One node. `children` is built by the callers below, never inferred. */
  const node = (key, label, value, children = []) => ({
    key, label, value,
    share: total ? value / total : 0,
    tone: FUNNEL_TONE[key] || null,
    children,
  });

  // A REASON IS A LEAF. The hosts under it are published — `undecided[].top`,
  // ranked, with their url counts — and they are deliberately NOT drawn: a row
  // per host is a row per SERVER on a corpus of two thousand of them. The tree
  // would grow by a page to say what two numbers on the reason's own row
  // already say.
  //
  // That argument used to lean on the router's cap being short. It is not
  // short any more — the ranked head runs to a hundred so the document can
  // answer WHICH servers — and the argument survives the change intact,
  // because it never rested on the cap: it rests on a unit change inside a
  // tree of url counts reading as a subtotal. What did have to move is the
  // tooltip; see [NAMES_IN_TOOLTIP].
  //
  // So the ranking survives as those two numbers rather than as a list.
  // `hosts` is how many servers the reason's urls resolve to and `largest` is
  // the widest one's share, which together answer the question the pair raises
  // and a list would answer at forty times the height: 3,902 urls on 2,201
  // hosts with the largest at 61 is a dead network spread thin, and the same
  // urls with the largest at 3,000 is three servers. The names go on the row's
  // title, where they cost no space at all.
  const asReason = (row) => {
    const value = Math.max(0, row.urls || 0);
    const top = (row.top || []).filter((h) => h && h.host && h.urls > 0);
    const named = row.examples?.length ? row.examples : top.map((h) => h.host);
    return {
      ...node(row.reason, row.reason, value),
      hosts: row.hosts || 0,
      largest: top[0]?.urls || 0,
      // CUT FOR THE TOOLTIP, not for the document. The router publishes these
      // to a ceiling of a hundred so `/stats.json` is an inventory of which
      // servers a reason holds — but the only place the names are DRAWN is a
      // native `title`, and a title is one unwrapped run of text that several
      // browsers truncate on their own terms. Measured against production the
      // day the router's cap moved: the widest reason's tooltip went from 159
      // characters to 1,740, which is not more legible than six names, it is
      // less. So the row keeps a readable handful and says how many it did not
      // name; the inventory is one fetch away and `hosts` is the count.
      examples: named.slice(0, NAMES_IN_TOOLTIP),
      unnamed: Math.max(0, (row.hosts || named.length) - Math.min(named.length, NAMES_IN_TOOLTIP)),
    };
  };

  // ROWS THAT REFINE ANOTHER ROW GO UNDER IT. The router publishes a FLAT list
  // that sums to `unmeasured` — nesting on the wire would put the one property
  // the whole tree rests on at the mercy of a shape — and each row names the
  // reason it refines. `never answered a REQ` has four of those: a name that
  // does not resolve, a refusal, a failed handshake, a window that lapsed.
  //
  // The parent is SYNTHESISED from its children rather than published, because
  // it has no urls of its own: every url it covers is already in a child, and a
  // row for the parent beside them would double-count the lot.
  const all = firstReasons(streams).filter((r) => (r.urls || 0) > 0);
  const children = new Map();
  for (const row of all) {
    if (!row.parent) continue;
    if (!children.has(row.parent)) children.set(row.parent, []);
    children.get(row.parent).push(row);
  }
  const reasons = [];
  const drawn = new Set();
  for (const row of all) {
    const group = row.parent || (children.has(row.reason) ? row.reason : null);
    if (group) {
      // A row whose NAME is also a parent is consumed as that parent rather
      // than drawn beside it — otherwise a document carrying both the group and
      // its children counts every url under it twice, and a sum that comes out
      // OVER its own total is the one error the `unattributed` slice cannot
      // report. The router never publishes both; the card is served to whoever
      // asks, and this file's own rule is not to trust the writer.
      if (drawn.has(group)) continue;
      drawn.add(group);
      const kids = children.get(group).map(asReason);
      reasons.push(node(group, group, kids.reduce((a, k) => a + k.value, 0), kids));
      continue;
    }
    reasons.push(asReason(row));
  }
  reasons.sort((a, b) => b.value - a.value);

  const kept = [
    node("foldedAway", "folded onto another url", sum("foldedAway")),
    node("consistent", "consistent", sum("consistent")),
    node("inconsistent", "inconsistent — refused", sum("inconsistent")),
    node("unmeasured", "no verdict", sum("unmeasured"), reasons),
  ];
  // A branch only where the router counted it: a zero row under a mouth that
  // has always been "what the streams named" is a claim about a corpus a router
  // this old never measured.
  const beyond = recordedOnly
    ? [node("recordedOnly", "known from our own records — no relay list names it now", recordedOnly)]
    : [];
  // KEYED `corpus`, NOT `sourced`. `sourced` is a published member with an
  // exact meaning — what the streams named this round — and the root is now
  // that plus what only our records know. One key, one meaning: a root labelled
  // "everything this router knows of" while hanging `sourced`'s glossary entry
  // would document the wrong number for the biggest row on the card.
  const root =
    node("corpus", "every relay url this router knows of", total, [
      node("dropped", "dropped before a pass could see it", dropped, [
        node("excluded", "excluded by config, or our own url", excluded),
        node("heldOutDead", "known dead — a signed unreachability record", heldOutDead),
      ]),
      ...beyond,
      node("candidates", "in reach — the candidate set", candidates, kept),
    ]);
  // WHAT THE RELAY THINKS OF THE ARITHMETIC, which is not the same question as
  // what this function's own subtraction found. `unattributed` can only report a
  // parent whose children fall SHORT; rows that overshoot their parent — the
  // shape a document carrying both a group and its children produces — leave no
  // slice at all. The relay recomputes both identities on the way out, so a
  // false here is drawn as a note even when every bar looks whole.
  const claimed = streams.map((w) => w.accountedFor).filter((v) => v != null);
  return {
    total, candidates, root, rows: flatten(root), omitted: firstOmitted(streams),
    accountedFor: claimed.length ? claimed.every(Boolean) : null,
  };
}

/**
 * The tree as rows a page can draw, depth-first, each carrying the box-drawing
 * prefix that makes the nesting readable without the page knowing the shape.
 *
 * The guides are built HERE rather than from a depth counter in the renderer
 * because they are not a function of depth alone: a `│` is drawn at every
 * ancestor that still has a sibling below it, and that is exactly the fact a
 * flattened list loses. Computed wrong, the tree still renders — with dangling
 * verticals under the last branch — which is the class of bug this module
 * exists to keep out of the page.
 *
 * A node whose children do not account for it gets an `unattributed` child on
 * the way out, so the check runs once, on the finished tree, and cannot be
 * forgotten by whoever adds the next level.
 */
function flatten(root) {
  const rows = [];
  const walk = (n, depth, prefix, last) => {
    rows.push({ ...n, depth, prefix: depth === 0 ? "" : prefix + (last ? "└─ " : "├─ ") });
    const kids = n.children.slice();
    const named = kids.reduce((a, k) => a + k.value, 0);
    if (kids.length && n.value > named) {
      kids.push({ key: "unattributed", label: "not accounted for", value: n.value - named,
                  share: root.value ? (n.value - named) / root.value : 0, tone: "warn", children: [] });
    }
    const below = depth === 0 ? "" : prefix + (last ? "   " : "│  ");
    kids.forEach((k, i) => walk(k, depth + 1, below, i === kids.length - 1));
  };
  walk(root, 0, "", true);
  return rows;
}

/**
 * The `undecided` rows across every stream row, widest first.
 *
 * Concatenated rather than merged by reason: the rows are per stream row and
 * today there is exactly one (the passes measure the union of every stream, and
 * publish it as `all streams`). Merging would be the right call the moment that
 * changes, and inventing the merge now would be untested code standing between
 * a reader and the only shape that exists.
 */
function firstReasons(streams) {
  const rows = streams.flatMap((w) => (w.undecided?.reasons || []).filter((r) => r && r.reason));
  return rows.sort((a, b) => (b.urls || 0) - (a.urls || 0));
}

/** Reasons either side dropped, so a truncated breakdown never reads as the whole one. */
function firstOmitted(streams) {
  return streams.reduce((a, w) => a + (w.undecided?.omitted || 0), 0);
}

/**
 * The legs to draw, and how full each quiet bar is.
 *
 * ## The denominator
 *
 * The bar is a proportion of [STUCK_LEG_SEC] — the same threshold its colour
 * keys off — rather than of anything about the rows themselves. Both relative
 * readings were tried and both lie. Scaled against the worst row PUBLISHED,
 * five bars rendered at 0.08% each when the outlier sat at row eight, outside
 * the five drawn: the comparison the bar exists to make, hidden. Scaled against
 * the worst row SHOWN, five legs each quiet a healthy thirty seconds rendered
 * full, which is the reading that means stuck. A proportion needs a denominator
 * that means something on its own, and the only one here is the threshold.
 *
 * ## The rows
 *
 * The router publishes them QUIETEST first (see `RelayRotation.held`), so the
 * first few are the legs worth looking at and `omitted` is the tail. This adds
 * whatever it drops to that count, because a truncated list that does not say
 * so reads as the whole answer.
 */
export function legsOf(inFlight, limit = IN_FLIGHT_SHOWN) {
  const all = inFlight?.relays || [];
  const rows = all.slice(0, limit).map((r) => {
    const quiet = r.quietForSec || 0;
    return {
      relay: r.relay,
      // The scheme is dropped and nothing else is: a truncated relay url is not
      // a relay url, and it is the thing being looked up.
      short: String(r.relay || "").replace(/^wss?:\/\//, ""),
      pass: r.pass != null ? String(r.pass) : null,
      heldForSec: r.heldForSec || 0,
      events: r.events || 0,
      quietForSec: quiet,
      quietShare: Math.min(1, quiet / STUCK_LEG_SEC),
      hot: quiet >= STUCK_LEG_SEC,
      // Absent means "not on a socket", which is a different fault from a slow
      // download and must not read as one.
      slotless: r.transferringForSec == null,
      transferringForSec: r.transferringForSec ?? null,
      // WHAT IT IS DOING, straight from the router — see `doing` in the
      // glossary. Null on a router that predates the member, which reads as
      // "not known" and never as a stage.
      doing: r.doing || null,
      // `??`, not `||`: `created_at = 0` is a real second relays serve and the
      // deepest a walk can reach, not a leg with no cursor.
      pagingUntil: r.pagingUntil ?? null,
    };
  });
  return { rows, more: (inFlight?.omitted || 0) + (all.length - rows.length) };
}


/**
 * WHAT COUNTS AS "SYNCED LATELY" — the window the relay table's own view is
 * built on, and the one it colours a row for reaching.
 *
 * An hour, taken from the pool's own cadences rather than picked: an untailed
 * relay's revisit base is five minutes and a tailed one's is thirty, so every
 * relay the pool is actually rotating over should be re-visited well inside an
 * hour. A row that has not, has missed several turns.
 */
export const SYNCED_RECENTLY_SEC = 60 * 60;

/**
 * Past this without a clean visit, a relay is not slow, it has stopped syncing.
 *
 * A day, and deliberately far above [SYNCED_RECENTLY_SEC] rather than a second
 * threshold near it. Between the two sits everything ordinary — a relay whose
 * visits are failing but whose tail still delivers, a wide roster whose
 * rotation is genuinely slower than its base cadence — and calling that broken
 * is how a warning stops being read. Past a day the pool has had dozens of
 * turns at it and none of them finished.
 */
export const NOT_SYNCING_SEC = 24 * 60 * 60;

/**
 * How many relay rows the table draws before deferring to the filter box.
 *
 * Not `Infinity`, unlike `IN_FLIGHT_SHOWN`, and the difference is the same
 * argument in the other direction: an in-flight list is bounded by the pool's
 * worker count, and this one is bounded by the ROSTER — several thousand rows
 * on a discovered corpus, each with a chip and seven cells. The row being
 * looked for is reachable two ways that a scroll of three thousand is not: the
 * default view is the ones that are not syncing, and the filter box takes a
 * host. What is cut is named, as everywhere else here.
 */
export const VISIT_ROWS_SHOWN = 300;

/**
 * The three questions the relay table gets asked, as views over one list.
 *
 * `problems` is the default because it is the question that brings someone to
 * this card: "which relays are not being synced". `recent` is the same data
 * read the other way — what HAS synced lately, newest first — and it exists
 * because "is anything working at all" is answered by the shape of that list
 * rather than by any one row. `all` is the fallback for looking a specific url
 * up, which is what the filter box is for.
 */
export const VIEW_PROBLEMS = "problems";
export const VIEW_RECENT = "recent";
export const VIEW_ALL = "all";

/**
 * WHY THIS RELAY IS NOT BEING SYNCED — one line, or null when it is.
 *
 * The order is the order the answers rule each other out, and it is the whole
 * reason this is a function rather than a chip colour:
 *
 *  - **Off the roster first.** Nothing here will visit it again until a verdict
 *    brings it back, so every other reading on the row is history. This is the
 *    answer that gets mistaken for a broken relay most often — "it stopped
 *    syncing" is as often this router declining to dial it.
 *  - **Then the last visit's own words.** `detail` is the router's, published
 *    beside the outcome; a page that reworded it would be a second vocabulary
 *    to keep in step, and one that dropped it would leave the operator with a
 *    word and no cause.
 *  - **Then never-visited**, which on a relay admitted a minute ago is ordinary
 *    and on one admitted yesterday is not — so it says which it cannot tell.
 *  - **Then staleness**, which is the only one of the four that is inferred
 *    from a clock rather than stated by the router.
 */
export function whyNotSyncing(r, nowSec) {
  if (!r) return null;
  if (r.onRoster === false) {
    return "Not on the roster: the monitor no longer certifies it, so nothing here will dial it until a verdict brings it back.";
  }
  if (r.failures > 0) {
    return r.detail || `The last ${r.failures} visit(s) did not finish (${r.outcome}).`;
  }
  if (r.syncedAt == null) {
    // The one state this page cannot date: the row exists because the roster
    // named it, and the roster does not say when.
    return r.heldForSec != null
      ? "Being synced right now — its first visit has not finished yet."
      : "No visit has finished yet. Ordinary for a relay just admitted to the roster; not, if it has been here a while.";
  }
  const since = nowSec - r.syncedAt;
  if (since >= NOT_SYNCING_SEC) return `Last clean visit was over ${Math.floor(since / 3600)}h ago, with nothing failing since — the pool is not getting back to it.`;
  return null;
}

/**
 * THE PER-RELAY TABLE: when each relay was last synced, and why it is not.
 *
 * ## Why this exists at all
 *
 * Everything else on this card is a live position or a total. `inFlight` names
 * the relays a worker is on this instant and forgets them when the visit ends;
 * the pool's counters say how many visits ran without saying against what; the
 * coverage strip says how far back the bands reach, so a walk that finished in
 * March and one that finished a minute ago draw the same bar. "When was
 * wss://relay.example last synced, and if it is not being synced, why" was
 * answerable only from a log line that had usually rotated away.
 *
 * ## The two clocks, kept apart
 *
 * `syncedAt` is the last CLEAN VISIT and `lastEventAt` is the last ARRIVAL, and
 * folding them would be wrong in both directions. A tailed relay delivers
 * continuously between visits, so arrivals alone would call every relay synced;
 * and a healthy relay this router asks a narrow filter of may honestly have
 * nothing to send for weeks, so visits alone would call it dead. Both are
 * carried, and `fresh` — the one the views are built on — is the VISIT, because
 * that is what "synced properly" means.
 *
 * ## What it decides, and what the page draws
 *
 * The filtering, the ordering, the freshness verdict and the reason line. The
 * counts are computed over the WHOLE list before any of that, so a view that
 * shows nothing still says how many rows the other two hold — a filtered table
 * that reads as an empty one is how a mirror looks broken to its own operator.
 */
export function visitsOf(visits, options = {}, at = Date.now() / 1000) {
  const { q = "", view = VIEW_PROBLEMS, limit = VISIT_ROWS_SHOWN } = options;
  // FLOORED ONCE, here. `Date.now() / 1000` is fractional, and every duration
  // below is a difference against it — un-floored, a row reads
  // `44.07599997520447s`, which is what shipped to a screenshot before this
  // line existed. Every clock in the document is a whole second.
  const nowSec = Math.floor(at);
  const all = (visits?.relays || []).map((r) => {
    const why = whyNotSyncing(r, nowSec);
    const syncedForSec = r.syncedAt != null ? Math.max(0, nowSec - r.syncedAt) : null;
    return {
      relay: r.relay,
      // The scheme and nothing else, exactly as in `legsOf` and `heldOf`: a
      // truncated relay url is not a relay url, and it is the thing being
      // looked up.
      short: String(r.relay || "").replace(/^wss?:\/\//, ""),
      outcome: r.outcome || "never",
      detail: r.detail || null,
      why,
      syncedAt: r.syncedAt ?? null,
      syncedForSec,
      // Both forms of the two other clocks: the INSTANT, which the page draws
      // as an age and hangs an exact timestamp off, and the elapsed seconds,
      // which is what a threshold is compared against. Deriving one from the
      // other at each call site is how a reader's clock ends up in a
      // comparison it has no business being in.
      lastVisitAt: r.lastVisitAt ?? null,
      lastVisitForSec: r.lastVisitAt != null ? Math.max(0, nowSec - r.lastVisitAt) : null,
      lastEventAt: r.lastEventAt ?? null,
      lastEventForSec: r.lastEventAt != null ? Math.max(0, nowSec - r.lastEventAt) : null,
      events: r.events || 0,
      failures: r.failures || 0,
      // Presence, not truthiness: a router that predates these members says
      // nothing about the roster or the tail, and `!!undefined` would say no.
      onRoster: r.onRoster !== false,
      tailed: r.tailed === true,
      nextVisitInSec: r.nextVisitInSec ?? null,
      heldForSec: r.heldForSec ?? null,
      streams: r.streams || [],
      // WHAT IT IS DOING, in the order that decides it: a worker on it now
      // beats a countdown, and a relay with neither is in the queue waiting
      // for a worker — which is a state, not a gap.
      state: r.heldForSec != null ? "syncing now" : r.nextVisitInSec != null ? "waiting" : "queued",
      fresh: syncedForSec != null && syncedForSec < SYNCED_RECENTLY_SEC,
    };
  });

  // OVER THE WHOLE LIST, before the filter and the view: a table showing
  // nothing must still be able to say how many rows the other views hold.
  const counts = {
    all: all.length,
    problems: all.filter((r) => r.why).length,
    recent: all.filter((r) => r.fresh).length,
    tailed: all.filter((r) => r.tailed).length,
    offRoster: all.filter((r) => !r.onRoster).length,
    failing: all.filter((r) => r.failures > 0).length,
    never: all.filter((r) => r.syncedAt == null).length,
  };

  const needle = q.trim().toLowerCase();
  let rows = all;
  if (view === VIEW_PROBLEMS) rows = rows.filter((r) => r.why);
  if (view === VIEW_RECENT) {
    // Newest first — the reverse of the document's own order, which is
    // worst-first. This view is the one place the page re-sorts, and it does
    // so because "what has synced lately" read oldest-first is the same list
    // upside down.
    rows = rows.filter((r) => r.fresh).slice().sort((a, b) => b.syncedAt - a.syncedAt);
  }
  if (needle) rows = rows.filter((r) => r.relay.toLowerCase().includes(needle));

  const shown = rows.slice(0, limit);
  const omitted = visits?.omitted || 0;
  const cut = rows.length - shown.length;
  return {
    rows: shown,
    // TWO DIFFERENT CUTS, both disclosed, and separately — they are the same
    // question to a reader ("am I seeing all of it") and different answers to
    // anyone acting on it. `omitted` is rows the ROUTER left out of the
    // document, which no filter here can reach; `cut` is rows this table left
    // off the screen, which narrowing the filter brings back. `more` is the
    // sum, for the one-number line.
    omitted,
    cut,
    more: omitted + cut,
    matched: rows.length,
    counts,
  };
}
