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
// hangs the document's own words on them. `tools/webtest/sync.test.mjs` is the
// half that can now be asserted.
//
// It is SMALL on purpose. The card's rebuild dropped the per-leg table, the
// outcome partition, the gauges and the per-relay coverage rows, and fourteen
// exports here went with them — they had survived only because their own tests
// still referenced them, which is how a module keeps answering questions
// nothing asks.

/** Past this, a router that has not written its heartbeat is not running. */
export const HEARTBEAT_STALE_SEC = 150;

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

/**
 * Is the router running, as of the rollup that wrote this document?
 *
 * Measured against the ROLLUP's clock, not the reader's, because that is the
 * only clock that saw the file. Folding in however long the document has been
 * cached would report a working router as dead every time a rollup ran late.
 */
export function isLive(progress) {
  return progress?.staleForSec != null && progress.staleForSec <= HEARTBEAT_STALE_SEC;
}

/**
 * The constraint verdict, or null where the document does not carry one.
 *
 * Guarded on its OWN member rather than on the health object: every gauge is
 * copied independently against an allowlist on the relay side, so a document
 * can carry the numbers with no word or the word with no numbers, and each has
 * to stand without the others.
 *
 * Past tense once the heartbeat is stale. The verdict is worth keeping on a
 * router that has stopped — the last reading is most of a post-mortem — but a
 * live diagnosis beside "not running" claims a process that is gone is still
 * constrained.
 */
export function constraintOf(health, live) {
  const word = health?.bottleneck;
  if (!word) return null;
  const [text, why] = BOTTLENECK[word] || [word, ""];
  return { word, text: live ? text : `${text}, when it stopped`, why, tone: word === "ingest" ? "warn" : null };
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
 */
export function probeProgress(p) {
  const streams = p?.streams || [];
  if (!streams.length) return null;
  const sum = (member) => streams.reduce((a, w) => a + (w[member] || 0), 0);
  const folded = sum("foldedAway");
  const candidates = Math.max(0, sum("candidates") - folded);
  const unmeasured = sum("unmeasured");
  return {
    candidates,
    checked: Math.max(0, candidates - unmeasured),
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
  };
}

/**
 * THE MONITOR'S WORK, AND THE SYNC'S — one list of processor rows, split by
 * what each of them DECIDES.
 *
 * They were one panel because they arrive in one array, and reading it meant
 * holding two unrelated questions at once. The passes named here answer *which
 * relay urls are worth dialling at all*: the fold collapses one server's several
 * addresses, the stability gate refuses a url that answers a filter two ways,
 * and the fitness pass signs the `prime` certificate every visit-mode
 * stream's relay list is made of — including the `dead` verdict that holds an
 * unreachable url out of every fan-out. All three run on the alias monitor's own
 * clock, none of them is configured by a stream, and their unit is a RELAY. What
 * is left — the rotating pool, ingest, the healer, the upstream push — moves
 * EVENTS, on the streams' clock, and is where a slow mirror is actually
 * diagnosed.
 *
 * A fourth used to sit here: a passive NIP-66 watcher signing a record per
 * socket. It is gone — the passes own the record now — and if a watcher ever
 * returns, its row belongs in this list rather than beside ingest.
 *
 * `MONITOR_PROCESSORS` is a list rather than a Set literal for the reason
 * `BOTTLENECK` carries `__proto__: null`: the names are strings off the wire,
 * and a lookup that can reach `Object.prototype` answers `true` for
 * `constructor`.
 *
 * **AN UNKNOWN NAME GOES TO THE PIPELINE, never nowhere.** A router that starts
 * publishing a processor this page has not been taught must still draw it: the
 * two lists are a PARTITION of what the document carries, and the sync card is
 * the one that already carries the status line and the leftovers. Dropping a row
 * to keep a card tidy is how a new job runs unwatched for a year.
 */
export const MONITOR_PROCESSORS = ["aliasFold", "consistency", "fitness"];

export function splitProcessors(progress) {
  const monitor = [];
  const pipeline = [];
  for (const p of progress?.processors || []) {
    if (p) (MONITOR_PROCESSORS.includes(p.name) ? monitor : pipeline).push(p);
  }
  return { monitor, pipeline };
}

/** The phase word a visit-mode stream carries — `StreamPhases.Phase.Rotating`. */
export const ROTATING = "rotating";

/**
 * WHAT A ROTATING STREAM IS ACTUALLY RIDING, which its row could not say.
 *
 * A visit-mode stream has no pass, no fraction and no cycle — its world is the
 * monitor's verdicts and its engine is the pool — so every mark `streamBlock`
 * draws is absent and the row rendered as `rotating for 58m` and nothing else.
 * That line is the same whether the stream is riding four hundred relays or
 * none, and "none" is the state worth seeing: before the fitness pass has
 * signed its first `prime`, a visit stream is a stream with an empty world.
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
  // fold working, and a url held out on a signed record is one we already
  // measured.
  foldedAway: "mute",
  heldOutDead: "mute",
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
  // The root: everything the streams named. `sourced` is the honest one when
  // the router publishes it; without it the root is what we can still account
  // for, and the tree simply starts lower rather than inventing a mouth.
  const total = Math.max(candidates + dropped, p.sourced || 0);

  /** One node. `children` is built by the callers below, never inferred. */
  const node = (key, label, value, children = []) => ({
    key, label, value,
    share: total ? value / total : 0,
    tone: FUNNEL_TONE[key] || null,
    children,
  });

  // A REASON IS A LEAF. The hosts under it are published — `undecided[].top`,
  // ranked, with their url counts — and they are deliberately NOT drawn: a row
  // per host is a row per SERVER on a corpus of two thousand of them, and the
  // ranked head is short only because the router capped it. The tree would grow
  // by a page to say what two numbers on the reason's own row already say.
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
    return {
      ...node(row.reason, row.reason, value),
      hosts: row.hosts || 0,
      largest: top[0]?.urls || 0,
      examples: row.examples?.length ? row.examples : top.map((h) => h.host),
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
  const root =
    node("sourced", "every url the streams named", total, [
      node("dropped", "dropped before a pass could see it", dropped, [
        node("excluded", "excluded by config, or our own url", excluded),
        node("heldOutDead", "known dead — a signed unreachability record", heldOutDead),
      ]),
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

