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
 *
 * **The tree is ONE row's, never a sum of them.** A pass's verdict members are
 * standing counts over a whole candidate set, and two rows are two overlapping
 * views of one corpus rather than two halves of it — see `corpusRow`, which is
 * where the duplicated reason rows and the inflated root came from.
 */
export function funnelOf(p) {
  const streams = p?.streams || [];
  if (!streams.length) return null;
  // ONE ROW, NEVER THE SUM — see `corpusRow`.
  const row = corpusRow(streams);
  const member = (name) => row[name] || 0;
  const candidates = member("candidates");
  if (!candidates) return null;
  // ABSENT IS NOT ZERO, and this is the one place in the module where the
  // difference is load-bearing — see the header. `|| 0` cannot tell a missing
  // member from a real zero, so the question is asked of the row directly.
  if (row.foldedAway == null && row.consistent == null && row.inconsistent == null) return null;

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
  const all = firstReasons(row).filter((r) => (r.urls || 0) > 0);
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
    node("foldedAway", "folded onto another url", member("foldedAway")),
    node("consistent", "consistent", member("consistent")),
    node("inconsistent", "inconsistent — refused", member("inconsistent")),
    node("unmeasured", "no verdict", member("unmeasured"), reasons),
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
  return {
    total, candidates, root, rows: flatten(root), omitted: firstOmitted(row),
    accountedFor: row.accountedFor ?? null,
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
 * WHICH ROW DESCRIBES THE WHOLE CANDIDATE SET — and why the others are not
 * added to it.
 *
 * This module used to SUM every member across every row, on the reasoning that
 * the passes measure the union of every stream and publish it once, as `all
 * streams`. That was true when it was written and stopped being true when the
 * FAST LANE landed: the lane runs the same passes over the urls named since its
 * last look and records a second row of its own (`AliasMonitor.FAST_LANE`),
 * which the router keeps beside the sweep's — one entry per stream label, each
 * overwritten by its own next run.
 *
 * The two rows are not two halves of a corpus. They are two overlapping views
 * of ONE corpus, because every url the lane measured is a url the sweep also
 * holds, and `Work`'s verdict members are STANDING counts over the whole
 * candidate set rather than a tally of what one pass touched — see
 * `Processors.Work.consistent`. Summed, a live card read `12,611` urls in reach
 * where the round-up line under it said `11,021 handed to the passes`, and drew
 * every reason twice: `too few events to judge on` at 309 urls beside `too few
 * events to judge on` at 226. The duplicate rows were the visible half; the
 * inflated root was the half nobody could see.
 *
 * Nor can the rows be MERGED. `urls` would sum to a number counting the
 * overlap twice, and `hosts` cannot be added at all — the same host appears in
 * both rows' tallies and the sum would exceed the servers that exist.
 *
 * So the tree is drawn from one row: the widest candidate set, which is the
 * sweep's, because the lane's set is always a slice of the corpus the sweep
 * walked. What the other rows have to show for themselves is a different
 * question — what one PASS did, not what the corpus IS — and it is not this
 * tree's to answer.
 */
function corpusRow(streams) {
  return streams.reduce((best, w) => ((w.candidates || 0) > (best.candidates || 0) ? w : best), streams[0]);
}

/**
 * The `undecided` rows of the row the tree is drawn from, widest first.
 *
 * One row's own reasons, for `corpusRow`'s reason: they sum to that row's
 * `unmeasured` and to nothing else, so a reason list assembled from more than
 * one row would not close against the parent it hangs under.
 */
function firstReasons(row) {
  return (row.undecided?.reasons || [])
    .filter((r) => r && r.reason)
    .sort((a, b) => (b.urls || 0) - (a.urls || 0));
}

/** Reasons the router dropped, so a truncated breakdown never reads as the whole one. */
function firstOmitted(row) {
  return row.undecided?.omitted || 0;
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

