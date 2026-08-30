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
/**
 * HOW MUCH OF THE SOCKET BUDGET IS SPENT, and whether it is the constraint.
 *
 * The pair, because neither number decides alone. `sockets` near the ceiling
 * is not a fault — a mirror whose whole job is to stay connected to every
 * certified relay is SUPPOSED to sit near its budget, and a panel that
 * coloured that would cry wolf on every healthy deployment. `queued` above
 * zero is the fault: those calls are admissible and OkHttp is holding them
 * because the budget is full.
 *
 * That distinction is the reason this exists. Every other symptom of a full
 * dispatcher — a long ETA, a pool that looks idle, relays never reached — is
 * shared with a slow store, a saturated thread pool and a roster of dead
 * hosts, and telling them apart used to take a measurement. It took one once:
 * the budget is 1024 because at OkHttp's stock 64 a 20,340-relay cycle
 * projected 330 hours.
 *
 * Null where the router does not publish the ceiling — a mark reading "0 of 0"
 * is worse than no mark.
 */
export function socketsOf(health) {
  const ceiling = Number.isFinite(health?.socketCeiling) ? health.socketCeiling : null;
  if (ceiling == null || !Number.isFinite(health?.sockets)) return null;
  // `??`, not `||`: a router too old to publish the queue says nothing, and
  // that must not read as "nothing is queued".
  const queued = Number.isFinite(health?.socketsQueued) ? health.socketsQueued : null;
  return {
    open: health.sockets,
    ceiling,
    running: Number.isFinite(health?.socketsRunning) ? health.socketsRunning : null,
    queued,
    share: Math.min(1, health.sockets / Math.max(1, ceiling)),
    // THE ONLY READING WORTH A COLOUR. Not "near the ceiling": that is the
    // healthy steady state of a mirror that stays connected.
    starved: (queued || 0) > 0,
  };
}

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
  return { roster: s.roster, tails: s.liveHeld ?? null, waiting: s.roster === 0 };
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
      // WHOSE row it is, where the document says — the root `live` list names
      // an owner; a stream's own `inFlight` does not, and `poolsOf` supplies
      // it from the row's position instead.
      stream: r.stream || null,
      // WHAT IT IS DOING, straight from the router — see `doing` in the
      // glossary. Null on a router that predates the member, which reads as
      // "not known" and never as a stage.
      doing: r.doing || null,
      // …and WHICH POOL that puts it in — the stable word `poolsOf` groups by,
      // never the sentence above it. Null is a row in none of the four, which
      // is a state (claiming a socket, draining the healer) and not a gap.
      pool: r.pool || null,
      // `??`, not `||`: `created_at = 0` is a real second relays serve and the
      // deepest a walk can reach, not a leg with no cursor.
      pagingUntil: r.pagingUntil ?? null,
    };
  });
  return { rows, more: (inFlight?.omitted || 0) + (all.length - rows.length) };
}


/**
 * THE FOUR POOLS, as the router names them — `pool` on every held row, and the
 * word this module groups by. See `VisitPool.POOL_LIVE` and its neighbours.
 *
 * Read off the document rather than derived from `doing`. The stage sentences
 * are written to be read once and have been reworded twice; grouping rows by
 * prose would put a table's contents at the mercy of an edit to a sentence,
 * which is the failure the router split these two members to avoid.
 */
export const POOL_LIVE = "live";
export const POOL_CATCHING_UP = "catching-up";
export const POOL_REFETCHING = "re-fetching";
export const POOL_NEGENTROPY = "negentropy";

/**
 * …and the fifth group, which is NOT a pool: a visit still claiming its socket
 * or draining the healer's queue on its way out is in none of the four, and the
 * router says so by publishing no `pool` for it.
 *
 * It is a group rather than a filter because the alternative is dropping rows.
 * A relay held for an hour "claiming the socket" is exactly the row an operator
 * is looking for, and a panel that showed only the four named pools would be
 * the one place it could not appear.
 */
export const POOL_BETWEEN = "between";

/**
 * …and the fifth BUDGETED job, which is not a pool either: a stream's dial
 * width. No held row ever carries it — a row is in one of the four pools or
 * between them — so it appears only in the limits table, which is where a dial
 * width belongs. Named here so that table can label it like the rest.
 */
export const JOB_VISITING = "visiting";

/** The order the panel draws them in: the steady state first, then the work. */
export const POOL_ORDER = [POOL_LIVE, POOL_CATCHING_UP, POOL_REFETCHING, POOL_NEGENTROPY];

/** …and those four plus the leftovers, which is the order the panel walks. */
const GROUP_ORDER = [...POOL_ORDER, POOL_BETWEEN];

/**
 * What each one is called on the page, and what it MEANS — the sentence a
 * heading cannot carry.
 *
 * Held here rather than in the card for the same reason every other judgement
 * in this module is: the four keys and the four descriptions have to agree, and
 * a mapping split across two files agrees until someone edits one of them.
 */
export const POOL_LABELS = {
  __proto__: null,
  [POOL_LIVE]: ["live", "Tail subscriptions held open. No worker sits on these — events arrive the moment they exist, and the socket is the whole cost."],
  [POOL_CATCHING_UP]: ["catching up", "Paging forward over what each relay's band does not cover yet — the ordinary sync, newest-first towards the last pass."],
  [POOL_REFETCHING]: ["re-fetching the past", "Paging over history the band ALREADY covers, because the stream's `refetchThePastSeconds` expired it. Same walk as a catch-up and a completely different bill: these relays are re-downloading years."],
  [POOL_NEGENTROPY]: ["negentropy", "Reconciling the covered past over NIP-77 and downloading only the difference — the pass that finds what no catch-up ever saw. `negentropyConcurrency` is its budget."],
  [JOB_VISITING]: ["visits", "How many relays may be VISITED for this stream at once — its share of the dial width. A visit that cannot get one of these does not dial at all, so this bounds simultaneous TLS handshakes and not merely work."],
  [POOL_BETWEEN]: ["between jobs", "In none of the four: claiming a socket, working out what an ask still owes, or draining the healer's queue on the way out of a visit. Ordinary and usually brief — a row that sits here is one to look at."],
};

/**
 * EVERY RELAY THIS MIRROR IS HOLDING, SPLIT BY WHAT IT IS BEING ASKED FOR.
 *
 * ## The question
 *
 * One rotating pool runs all four workloads (see `VisitPool`), so every number
 * that used to describe it added them together: `visiting: 100` counted a
 * catch-up, a history audit and a whole-corpus re-walk as one, and `tails: 412`
 * counted the fourth without naming anybody. Those are not degrees of one
 * thing — a mirror paging forward is keeping up, and the same mirror
 * re-fetching is spending its whole budget re-downloading history it already
 * has. Four lists is the shape of the question actually being asked.
 *
 * ## Where the rows come from
 *
 * The visiting three are per stream in the document (`streams[].inFlight`), so
 * a row takes its owner from its position; the live pool is one list at the
 * ROOT and each of its rows NAMES its stream, because a tail is held per
 * (relay, stream) pair. Either way every row here knows whose it is, which is
 * what lets the same rows be grouped by pool or by stream — see
 * [streamSections].
 *
 * ## What it will not do
 *
 * DROP A ROW. Every held relay the document names appears in exactly one group,
 * including one whose `pool` the router did not publish — see [POOL_BETWEEN].
 * And `omitted` is summed rather than attributed: what a truncated list left
 * out has no pool by definition, so counting it against one would be inventing
 * the very fact it is missing.
 *
 * Empty groups are KEPT. "Nothing is auditing right now" is an answer, and a
 * panel that drew only the non-empty pools would answer it by looking identical
 * to a build that had no audit pool at all.
 *
 * Null when the mirror is holding nothing anywhere — no visit, no tail — which
 * is the one state where four empty tables say less than no panel.
 *
 * ## …and how big the pool is
 *
 * `totals` is the denominator every group count is a share of, because the
 * tables alone answer "how many are working" and never "out of how many". The
 * pool is ONE pool — one queue, one set of workers, one tail budget, shared by
 * every visit-mode stream — so its size is a count of URLS and comes from the
 * rotating pool's own row (`roster`, `awaitingVisit`), not from adding the
 * streams' shares, which double-count every relay two streams both want.
 *
 * The rows are what the tables draw, so `working` and `tailed` are counted off
 * the groups rather than read from `visiting`/`tails`: a summary that
 * disagreed with the tables under it would be worse than no summary. See
 * [poolTotals].
 */
export function poolsOf(progress, held = heldRows(progress)) {
  if (!held.rows.length) return null;
  const groups = groupByPool(held.rows);
  return { groups, omitted: held.omitted, totals: poolTotals(progress, groups) };
}

/**
 * EVERY HELD RELAY THE DOCUMENT NAMES, flattened into one list where each row
 * knows its stream and its pool.
 *
 * Separated from the grouping because the grouping is done twice over the same
 * rows — once for the mirror and once per stream — and a second collector
 * would be a second place for a row to be dropped from.
 */
export function heldRows(progress) {
  const rows = [];
  let omitted = 0;
  for (const s of progress?.streams || []) {
    const legs = legsOf(s.inFlight);
    omitted += legs.more;
    // A visiting row takes its owner from its POSITION: it is published under
    // the stream whose ask the visit is serving, and says nothing itself.
    for (const r of legs.rows) rows.push({ ...r, stream: s.name || null });
  }
  const live = legsOf(progress?.live);
  omitted += live.more;
  // A live row NAMES its own, and `legsOf` has already normalised it: one
  // subscription is held per (relay, stream) pair and carries that stream's
  // filter alone. It used to be null here, when a tail carried every wanting
  // stream's filter at once and belonged to none of them — and a per-stream
  // live row was not a thing that existed.
  rows.push(...live.rows);
  return { rows, omitted };
}

/**
 * …and the four pools those rows fall into, in the panel's order.
 *
 * Takes rows rather than the document so the caller decides WHICH rows: the
 * whole mirror's, or one stream's. [owner] is the one thing the rows cannot
 * say — the stream whose heading they are already under, if any — because a
 * column repeating a heading is not a column, and a column carrying the only
 * attribution a row has is the point. A NAME rather than a flag: the caller
 * has one, and passing it says which heading is doing the naming.
 */
function groupByPool(rows, owner = null) {
  const byPool = new Map(GROUP_ORDER.map((key) => [key, []]));
  for (const r of rows) {
    // An unknown word lands with the unpooled rather than making a group of its
    // own: a page inventing a heading from a string off the wire is how a typo
    // becomes a pool.
    byPool.get(POOL_ORDER.includes(r.pool) ? r.pool : POOL_BETWEEN).push(r);
  }

  const groups = [];
  for (const key of GROUP_ORDER) {
    const found = byPool.get(key);
    // The leftover group appears only when something is in it; the four named
    // pools appear always. An empty `between` is the healthy case and a heading
    // for it every tick would be a mark that reads the same every time.
    if (key === POOL_BETWEEN && !found.length) continue;
    // Quietest first, the router's own order — and re-applied here because the
    // merge across streams interleaves lists that were each sorted alone.
    found.sort((a, b) => b.quietForSec - a.quietForSec || b.heldForSec - a.heldForSec || a.relay.localeCompare(b.relay));
    const [label, what] = POOL_LABELS[key] || [key, ""];
    groups.push({
      key,
      label,
      what,
      rows: found,
      // ONE WORD FOR THE WHOLE GROUP, or null where its rows disagree. A column
      // whose every cell reads `holding a live tail` is not a column, so the
      // page lifts it into the heading instead — and the negentropy pool, whose
      // two stages are a history sweep and a provider's retraction comparison,
      // keeps the column that tells them apart.
      doing: found.length && found.every((r) => r.doing === found[0].doing) ? found[0].doing : null,
      // …and the stream column, which is drawn wherever a row can name its
      // owner — EXCEPT under a heading that has already named it, where the
      // column would be that heading copied down the table. Not "do the rows
      // disagree": a pool holding one row is the case where the attribution is
      // hardest to get any other way, and dropping the column there would lose
      // it exactly when it is scarcest.
      streams: !owner && found.some((r) => r.stream),
    });
  }
  return groups;
}

/**
 * THE ORDER THE JOBS ARE DRAWN IN — the pool's own, cheapest first.
 *
 * `PoolLimits.JOBS` publishes them in this order and `AuditSchedule` publishes
 * a subset, so a merged table has to choose one. The router's is the right one
 * and it is not alphabetical: a dial width, a tail budget, then the two walks
 * that spend real bandwidth.
 */
const JOB_ORDER = [JOB_VISITING, POOL_LIVE, POOL_CATCHING_UP, POOL_REFETCHING, POOL_NEGENTROPY];

/**
 * WHAT A STREAM MAY SPEND ON A JOB AND WHEN THAT JOB COMES DUE — one row per
 * job, out of the two lists that each carried half of it.
 *
 * ## Why they are one row
 *
 * They were two tables, and both were keyed by (stream, job): the same four
 * job names down two first columns, in two panels, at two ends of the card.
 * That is not two subjects. `re-fetching the past` is capped at 4, has 4 in
 * use, has turned 1,207 asks away, and has 400 more waiting on a 30-day clock
 * — one sentence about one job, which a reader had to assemble from two tables
 * by matching a word.
 *
 * And the halves only mean anything TOGETHER. A cap at its ceiling is not a
 * fault; a cap at its ceiling with work backing up behind it is the cap
 * biting, and the queue behind it was in the other table. Read across one row
 * it is a glance.
 *
 * ## What it will not do
 *
 * DROP A JOB. The two lists are near-subsets today — every scheduled job is
 * also a capped one — and this does not assume it: the union is walked, in
 * [JOB_ORDER] with anything the router has since added on the end, and a row
 * with only one half draws the other as absent. `limitsOf` already drops
 * uncapped-and-undeferred rows, so a job that is only ever scheduled arrives
 * here with no limit at all, which is a shape this must survive rather than a
 * shape it may assume away.
 */
export function jobsOf(limits, schedule) {
  const by = new Map();
  const seen = [];
  const slot = (job) => {
    if (!by.has(job)) {
      by.set(job, { job, label: POOL_LABELS[job]?.[0] || job || "—", limit: null, schedule: null });
      seen.push(job);
    }
    return by.get(job);
  };
  for (const l of limits) slot(l.job).limit = l;
  for (const r of schedule) slot(r.job).schedule = r;
  // The router's order, then whatever it has since added — never dropped for
  // being a word this page was not taught.
  const order = [...JOB_ORDER.filter((j) => by.has(j)), ...seen.filter((j) => !JOB_ORDER.includes(j))];
  return order.map((j) => by.get(j));
}

/**
 * ONE STREAM, WHOLE — its phase, its share of the roster, its four pools, its
 * budgets and its schedule, in one object per configured stream.
 *
 * ## Why the section and not four cuts of four tables
 *
 * The card used to draw a stream FIVE TIMES: a one-line block under *streams*
 * saying what it was riding, a pool section repeating that same line and
 * holding its tables, and a row per (stream, job) in each of two card-level
 * tables whose first column was the name again. Nothing joined them — they
 * were four independent walks of `progress.streams` — so an operator asking
 * "what is `content` doing" read four places and did the join by eye, and the
 * two roster numbers in the first two of them came off different members and
 * could disagree.
 *
 * A stream is one subject. It gets one section, and the join that used to be
 * the reader's is done here, once, where it can be checked.
 *
 * ## What it promises
 *
 * A SECTION PER CONFIGURED STREAM, in the document's order, whatever the
 * stream is doing. The cut this replaced left out a stream that had not
 * started rotating, which was right when a section was only a place to hang
 * pool tables and wrong now that it is the only place a stream appears at all:
 * a stream in `router.conf` that has never come up is exactly the one an
 * operator goes looking for, and it would have been on no card.
 *
 * AND EVERY ROW UNDER EXACTLY ONE OF THEM. Held rows, limit rows and schedule
 * rows are all attributed by stream name, and whatever no configured stream
 * claims goes to a section of its own rather than being dropped — a tail
 * naming a stream that has left the config is the row worth seeing. The three
 * are filtered against ONE `claimed` set for that reason: two of them can only
 * come from `progress.streams` today, and a promise that holds by construction
 * is one that stops holding silently when the construction changes.
 *
 * [held] is taken rather than read, for the reason [poolsOf] takes it: the two
 * are drawn on one card off one document, and a second walk would both allocate
 * a second object per held relay and give the summary a chance to disagree with
 * the sections under it.
 *
 * `jobs` is the two config lists merged on the job they are both keyed by —
 * see [jobsOf], which is where that join and its no-dropping promise live.
 *
 * `holding` is the row count across the four pools, so the card can tell a
 * stream that is spending nothing from one that is spending a little without
 * summing the groups itself — four empty tables and a line saying so are the
 * same answer, and only one of them is worth 200 pixels.
 */
export function streamSections(progress, held = heldRows(progress)) {
  const limits = limitsOf(progress);
  const schedule = scheduleOf(progress);
  const out = [];
  const claimed = new Set();
  const mine = (list, name) => list.filter((r) => r.stream === name);
  for (const s of progress?.streams || []) {
    const name = s?.name || null;
    claimed.add(name);
    const groups = groupByPool(mine(held.rows, name), name);
    out.push({
      stream: name,
      phase: s?.phase || null,
      phaseForSec: num(s?.phaseForSec),
      // The one reading here that is a judgement rather than a number: a
      // rotating stream with an empty roster is waiting on the fitness pass,
      // and it looks exactly like a busy one from every other member.
      rotation: rotationOf(s),
      groups,
      totals: streamTotals(s, groups),
      jobs: jobsOf(mine(limits, name), mine(schedule, name)),
      holding: groups.reduce((a, g) => a + g.rows.length, 0),
    });
  }
  // …and whatever no configured stream claimed. Only when there IS something:
  // an empty "unattributed" heading every tick is a mark that never moves.
  const looseRows = held.rows.filter((r) => !claimed.has(r.stream));
  const looseLimits = limits.filter((r) => !claimed.has(r.stream));
  const looseSchedule = schedule.filter((r) => !claimed.has(r.stream));
  if (looseRows.length || looseLimits.length || looseSchedule.length) {
    // NOT `named` for this one: its heading says only that nothing claimed
    // these, so whatever name a row does carry is worth a column.
    const groups = groupByPool(looseRows);
    out.push({
      stream: null, phase: null, phaseForSec: null, rotation: null,
      groups, totals: streamTotals(null, groups),
      jobs: jobsOf(looseLimits, looseSchedule), holding: looseRows.length,
    });
  }
  return out;
}

/** The rotating pool's own processor row — the only place its SIZE is published. */
const VISITS_PROCESSOR = "visits";

/**
 * HOW BIG THE POOL IS, and how its relays are split right now.
 *
 * Four numbers and one subtraction:
 *
 *  - `relays`      every relay in rotation. Context, and NOT the denominator:
 *                  the pool's unit of work is a (relay, stream) PAIR, so a
 *                  relay three streams want is one relay and three units.
 *  - `units`       that same roster in units — `rosterVisits`. THIS is what
 *                  the three below partition, and mixing the two is the
 *                  arithmetic this comment exists to stop: subtracting pair
 *                  counts from a relay count reads fine and is nonsense.
 *  - `working`     has a worker this instant — the visit tables, which
 *                  partition it.
 *  - `queued`      waiting for a worker, `awaitingVisit`.
 *  - `waiting`     the remainder: on a revisit timer, neither running nor
 *                  queued, which is where most of a healthy roster sits.
 *  - `tailed`      holds a live subscription. NOT a fourth share of the same
 *                  whole — a live unit keeps its subscription while it is
 *                  revisited, so it is in this number AND in `working` at the
 *                  same time. The three above sum to `units`; this one
 *                  crosses them.
 *
 * Null members rather than zeroes wherever the document does not say. A router
 * that publishes no pool row is not a router with an empty pool, and "0 in the
 * pool" beside four tables of relays is the kind of arithmetic that gets a
 * panel disbelieved.
 */
export function poolTotals(progress, groups) {
  const row = (progress?.processors || []).find((p) => p && p.name === VISITS_PROCESSOR);
  return totalsOf(groups, {
    relays: num(row?.roster),
    units: num(row?.rosterVisits),
    queued: num(row?.awaitingVisit),
  });
}

/** A published member, or null where the document does not carry it. */
const num = (v) => (Number.isFinite(v) ? v : null);

/**
 * THE ARITHMETIC BOTH CUTS SHARE: what the groups are holding, and what is
 * left of the denominator once they are taken out.
 *
 * One function because the two summaries sit on one card and are read against
 * each other — the per-stream lines must add up to the pool's. Written twice
 * they had already drifted on the rule that matters most here: with `queued`
 * absent, one rendered a remainder anyway (silently counting the queue as
 * sitting between visits) and the other said nothing. Saying nothing is right,
 * and now it is right in both.
 */
function totalsOf(groups, { relays, units, queued }) {
  const working = groups.reduce((a, g) => a + (g.key === POOL_LIVE ? 0 : g.rows.length), 0);
  return {
    relays,
    units,
    working,
    queued,
    // A tailed unit keeps its tail while it is revisited, so this CROSSES the
    // three above rather than joining them.
    tailed: groups.find((g) => g.key === POOL_LIVE)?.rows.length ?? 0,
    // Off UNITS, never off relays — pool-wide those are different
    // denominators. Never negative either: the counts are read at one tick but
    // not one instant, so a roster that shrank between them can leave the
    // subtraction short, and "-2 between visits" reads as a bug in the router
    // rather than as the rounding it is.
    waiting: units == null || queued == null ? null : Math.max(0, units - working - queued),
  };
}

/**
 * …and the same five numbers for ONE stream, off that stream's own row.
 *
 * The pool-wide totals cannot be divided into these — `rosterVisits` is a sum
 * over streams and `awaitingVisit` on the pool's row is the queue entire — so
 * the stream row publishes its own `roster`, `liveHeld` and `awaitingVisit`
 * and this reads them there.
 *
 * `units` IS `relays` here, and that is not a shortcut. The pool's unit of
 * work is a (relay, stream) pair, so pool-wide the two differ by however much
 * the streams overlap; inside ONE stream a relay is exactly one unit, which is
 * what makes this subtraction sound where the pool-wide one needs a second
 * denominator. It is filled in rather than left null because the pool blocks
 * draw their `n of m` from it.
 *
 * Nulls where the row does not say — a stream published by a router too old
 * for `awaitingVisit` gets no queued mark and no remainder, rather than a zero
 * that would read as "nothing waiting".
 */
function streamTotals(s, groups) {
  const relays = num(s?.roster);
  // `units: relays` — inside ONE stream a relay is exactly one unit of work,
  // which is what makes the remainder sound here where the pool-wide one needs
  // a second denominator.
  return totalsOf(groups, { relays, units: relays, queued: num(s?.awaitingVisit) });
}

/**
 * WHAT EACH STREAM MAY SPEND on each of the pool's jobs, and what it has spent
 * — one row per (stream, job) the router publishes a limit for.
 *
 * Flattened across streams because the caps are read as a TABLE: "who may have
 * how much of the audits" is a comparison between streams, and a per-stream
 * block would make it one paragraph each. The stream stays on the row.
 *
 * Uncapped rows are dropped: the router publishes every job for every stream so
 * that "bounded by the dial width alone" is sayable, but a table of unlimited
 * rows is the mark that reads the same on every deployment. What survives is
 * what somebody configured — plus anything that has been deferred, which
 * cannot happen without a cap and so is a row that has already earned itself.
 */
export function limitsOf(progress) {
  const rows = [];
  for (const s of progress?.streams || []) {
    for (const l of s.limits || []) {
      const streamCap = Number.isFinite(l.streamCap) ? l.streamCap : null;
      const deferred = l.deferred || 0;
      if (streamCap == null && !deferred) continue;
      rows.push({
        stream: s.name || null,
        job: l.job || null,
        label: POOL_LABELS[l.job]?.[0] || l.job || "—",
        streamCap,
        // `??`, not `||`: zero permits out is a real reading — the stream is
        // capped and using none of it — and not a missing number.
        inUse: Number.isFinite(l.inUse) ? l.inUse : null,
        deferred,
        // AT THE CAP is not a fault; at the cap WITH work being turned away is
        // the cap biting, and only that is worth a colour. BOTH halves, which
        // the comment has always said and the predicate did not: `deferred` is
        // cumulative since boot, so on its own a single refusal at boot paints
        // the row hot for the life of the process. Paired with "full right
        // now" it says what an operator can act on — this cap is the reason
        // work is not happening, at this moment.
        //
        // Uncapped rows only reach here by having deferred something, which
        // cannot happen without a cap; they are marked on that alone.
        biting: deferred > 0 && (streamCap == null || l.inUse >= streamCap),
      });
    }
  }
  return rows;
}

/**
 * WHEN EACH STREAM'S SCHEDULED RE-READS COME DUE — the audit's clock and the
 * re-fetch's, over every ask.
 *
 * This is the half the counters cannot supply. `auditsRun` climbing says work
 * happened; only `waiting` draining at the period says it happened BECAUSE it
 * was due. So the row is published whole — due, never-run, waiting — rather
 * than as a single "N due" that could not be checked against anything.
 *
 * `neverRun` is deliberately its own number and not folded into `due`. An ask
 * with no completed pass is due by definition, which is the whole of a fresh
 * deployment; folded together, a mirror that has never audited anything and
 * one whose period has elapsed would read identically, and only the second is
 * the schedule doing something.
 */
export function scheduleOf(progress) {
  const rows = [];
  for (const s of progress?.streams || []) {
    for (const r of s.schedule || []) {
      const due = r.due || 0;
      const neverRun = r.neverRun || 0;
      rows.push({
        stream: s.name || null,
        job: r.job || null,
        label: POOL_LABELS[r.job]?.[0] || r.job || "—",
        everySec: Number.isFinite(r.everySec) ? r.everySec : null,
        due,
        neverRun,
        waiting: r.waiting || 0,
        // Absent means nothing is waiting — every ask is already due — which
        // is a state and not a zero countdown.
        nextInSec: Number.isFinite(r.nextInSec) ? r.nextInSec : null,
        // Work that is due and not moving is the one shape worth a colour, and
        // it cannot be read off `due` alone: a fresh deployment is ALL due and
        // perfectly healthy. Backed up means due work with nothing waiting
        // behind it — the period has elapsed for everything and the pool is
        // not getting to it.
        backedUp: due > 0 && r.waiting === 0 && neverRun === 0,
      });
    }
  }
  return rows;
}
