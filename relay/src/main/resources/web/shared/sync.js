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
  const candidates = streams.reduce((a, w) => a + (w.candidates || 0), 0);
  const unmeasured = streams.reduce((a, w) => a + (w.unmeasured || 0), 0);
  return {
    candidates,
    checked: Math.max(0, candidates - unmeasured),
    tookSec: p.phase === MEASURING ? null : (p.lastPassSec ?? null),
  };
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

