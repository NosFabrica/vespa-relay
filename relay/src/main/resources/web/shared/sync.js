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
 */
export function probeProgress(p) {
  const streams = p?.streams || [];
  if (!streams.length) return null;
  const sum = (member) => streams.reduce((a, w) => a + (w[member] || 0), 0);
  const candidates = sum("candidates");
  const unmeasured = sum("unmeasured");
  return {
    candidates,
    checked: Math.max(0, candidates - unmeasured),
    // The two VERDICTS inside that checked count, or null on a pass that does
    // not publish them (the fold, and any router older than the partition).
    // Drawn beside it because "checked" on the stability gate silently includes
    // urls the FOLD removed — they are checked in the sense that nothing more
    // will be asked of them, and not in the sense a reader assumes — and
    // because `inconsistent` is the one number on this row that says what the
    // gate is FOR, and it appeared nowhere on the page at all.
    consistent: streams.some((w) => w.consistent != null) ? sum("consistent") : null,
    inconsistent: streams.some((w) => w.inconsistent != null) ? sum("inconsistent") : null,
    tookSec: p.phase === MEASURING ? null : (p.lastPassSec ?? null),
  };
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
  // The tail a ranked head was taken from, which is DISCLOSED truncation and
  // not a fault — the opposite of `unattributed`, and toned apart from it so
  // the two are never read as the same thing.
  moreHosts: "mute",
};

/** Every level of the funnel is drawn against one width, so a child sits under its parent. */
const FUNNEL_LEVELS = [
  ["reach", "every url the streams named", [["candidates", "in reach"], ["heldOutDead", "known dead, held out"]]],
  ["verdict", "…what is known about each", [["foldedAway", "folded onto another url"], ["consistent", "consistent"],
    ["inconsistent", "inconsistent — refused"], ["unmeasured", "no verdict"]]],
];

/**
 * THE WHOLE CANDIDATE SET, DIVIDED — every url a stream would dial, once, into
 * the category that decided its fate, and then into why.
 *
 * ## Why this and not the one number beside it
 *
 * `probeProgress` answers "how much has a verdict", which on a discovered corpus
 * sits at a few hundred out of several thousand and reads as a gate that is
 * stuck. It is not: the pass dials its whole set every time, and most of that
 * set is urls that cannot be measured at all — dead hosts, auth walls, relays
 * holding nine events. Those are four different problems with four different
 * fixes and they were one undifferentiated number, so the honest reading and
 * the alarming one were indistinguishable. This is the breakdown that separates
 * them.
 *
 * ## The rules it is held to
 *
 * **Every level is drawn against the SAME width**, with a `lead` offset, so a
 * level's segments sit under the parent segment they subdivide. A level scaled
 * to its own total would draw `unmeasured`'s seven reasons as wide as the whole
 * corpus, which is the reading this exists to prevent.
 *
 * **A level that does not sum gets an `unattributed` slice rather than a gap.**
 * Three ways that happens and all three are real: a router older than the
 * partition publishes `candidates` and `unmeasured` and nothing between them; a
 * reason list truncated by either side leaves urls in no row; and any future
 * arithmetic slip. A gap reads as "nothing there", a named slice reads as "not
 * accounted for", and only the second is true. It is toned as a fault so it
 * cannot be mistaken for a finding.
 *
 * **Nothing is invented from a missing member.** Absent reads as zero, never as
 * a share of something else, and a document with no `sourced` simply loses the
 * first level instead of having one guessed for it.
 */
export function funnelOf(p) {
  const streams = p?.streams || [];
  if (!streams.length) return null;
  const sum = (member) => streams.reduce((a, w) => a + (w[member] || 0), 0);
  const candidates = sum("candidates");
  if (!candidates) return null;
  const heldOutDead = Math.max(0, p.heldOutDead || 0);
  // The width every level is a share of. `sourced` is the honest root when the
  // router publishes it; without it the root is the candidate set itself, and
  // the level naming what was held out is dropped rather than drawn empty.
  const total = Math.max(candidates + heldOutDead, p.sourced || 0);
  const values = {
    candidates,
    // As PUBLISHED, never as `total - candidates`. The two numbers are written
    // by different clocks — the derivation's and the pass's — so a source that
    // re-derived between them would silently inflate this slice with urls that
    // were never held out. Any gap shows up as `unattributed`, which is what
    // that slice is for.
    heldOutDead,
    foldedAway: sum("foldedAway"),
    consistent: sum("consistent"),
    inconsistent: sum("inconsistent"),
    unmeasured: sum("unmeasured"),
  };

  const seg = (key, label, value, lead) => ({
    key, label, value,
    share: total ? value / total : 0,
    lead: total ? lead / total : 0,
    tone: FUNNEL_TONE[key] || null,
  });

  const levels = [];
  for (const [key, title, members] of FUNNEL_LEVELS) {
    // Both levels here subdivide the FIRST segment of the one above them, whose
    // lead is zero, so theirs is too.
    //
    // NOT a general rule, and the hierarchy is not modelled as one: the parent
    // is picked by name below and the reasons' lead is computed by hand. A
    // level that subdivided anything but a leading segment, or a second branch
    // splitting on the same level, would need both of those to become a real
    // parent link — see the `why` level, which is the shape that would have to
    // generalise.
    let at = 0;
    const segments = [];
    for (const [member, label] of members) {
      const value = Math.max(0, values[member] || 0);
      if (value > 0) segments.push(seg(member, label, value, at));
      at += value;
    }
    // The parent of this level, as a width: level one divides `total`, and
    // every level after it divides its predecessor's first member.
    const parent = key === "reach" ? total : candidates;
    const short = parent - segments.reduce((a, s) => a + s.value, 0);
    if (short > 0) segments.push(seg("unattributed", "not accounted for", short, at));
    if (segments.length) levels.push({ key, title, segments });
  }

  // …and the reasons, which subdivide `unmeasured` and therefore start where it
  // starts: after everything that DOES have a verdict.
  const lead = values.foldedAway + values.consistent + values.inconsistent;
  const reasons = [];
  // The fourth level, built in the SAME walk as the third, because each host
  // row is positioned under its own reason rather than under the level as a
  // whole. This is the first level whose segments do not share one parent, and
  // computing it separately would mean re-deriving every reason's offset from
  // its position in a list — the arithmetic the levels above get away with
  // because their parent is always the leading segment.
  const hosts = [];
  let at = lead;
  for (const row of firstReasons(streams)) {
    const value = Math.max(0, row.urls || 0);
    if (!value) continue;
    const top = (row.top || []).filter((h) => h && h.host);
    reasons.push({
      ...seg(row.reason, row.reason, value, at),
      hosts: row.hosts || 0,
      // Names for the tooltip: whichever the pass had to give. A pass that
      // counts publishes `top` and no `examples`, so asking for both here is
      // what keeps one tooltip working for the fold and the gate alike.
      examples: row.examples?.length ? row.examples : top.map((h) => h.host),
    });
    // UNDER THIS REASON, starting where it starts.
    let within = at;
    for (const h of top) {
      const urls = Math.max(0, h.urls || 0);
      if (!urls) continue;
      hosts.push({ ...seg(h.host, h.host, urls, within), parent: row.reason });
      within += urls;
    }
    // The tail this ranked head was taken from. Drawn, and NOT as a fault: a
    // reason spread across two thousand hosts with none above a dozen urls is
    // the normal shape of a dead corpus, and it is also the finding — so the
    // remainder has to be visible rather than left as empty track.
    const rest = value - (within - at);
    if (top.length && rest > 0) {
      hosts.push({ ...seg("moreHosts", `other hosts under "${row.reason}"`, rest, within), parent: row.reason });
    }
    at += value;
  }
  if (reasons.length) {
    const short = values.unmeasured - reasons.reduce((a, s) => a + s.value, 0);
    if (short > 0) reasons.push(seg("unattributed", "not accounted for", short, at));
    levels.push({ key: "why", title: "…and why the rest has none", segments: reasons });
  }
  if (hosts.length) levels.push({ key: "hosts", title: "…and which hosts those are", segments: hosts });
  // A CHART THAT DIVIDES NOTHING IS NOT A CHART. The alias fold publishes
  // `candidates` and `unmeasured` and counts its undecided rows in HOSTS, so
  // every level of its funnel is one full-width bar restating the sentence
  // above it. One level somewhere has to actually split for this to earn the
  // space.
  if (!levels.some((l) => l.segments.length > 1)) return null;
  return { total, candidates, levels, omitted: firstOmitted(streams) };
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

