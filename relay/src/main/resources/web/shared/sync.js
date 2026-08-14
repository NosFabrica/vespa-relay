// WHAT THE SYNC CARD DECIDES, apart from how it draws it.
//
// The card's live half is marks over the router's progress document, and every
// mark is a judgement: which legs are worth naming, what a bar is a proportion
// OF, whether a partition divides at all, whether a health object is drawable.
// Those judgements lived inline in stats.html, where nothing could reach them.
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
 * Past this share of the heap the router is collecting rather than mirroring —
 * the same 90% its own health line shouts at, and for the same reason: nothing
 * FAILS at the ceiling, throughput just quietly stops.
 */
export const HEAP_TIGHT = 0.9;

/** How many held relays the table names before deferring to the JSON. */
export const IN_FLIGHT_SHOWN = 5;

/**
 * The ten terminal outcomes, in stacking order, with the short label the key
 * prints. The tone is a CSS class per outcome, shared with the key's swatch so
 * the two cannot drift; the glossary supplies the definition on the title.
 */
export const DISPOSITION = [
  ["delivered", "delivered"],
  ["nothingNew", "in sync"],
  ["pending", "pending"],
  // Beside `pending` because both are facts about OUR pool rather than verdicts
  // about a relay. It was missing entirely once: the router publishes it, the
  // relay sums it into `accountedFor`, and the bar drew nothing — so a stream
  // with a large `busy` rendered a stack that silently failed to fill its own
  // total.
  ["busy", "busy"],
  ["noRoute", "no route"],
  ["unreachable", "unreachable"],
  ["hostStruckOut", "host struck out"],
  ["knownDead", "known dead"],
  ["transferFailed", "transfer failed"],
  ["torUnavailable", "tor down"],
];

/**
 * The url partition — what discovery found, before anything was dialled.
 *
 * Its own bar, above the disposition, because they divide different wholes:
 * this one divides `discovered`, the other divides `taken`. Drawing them as one
 * bar was the reading that made 16,752 discovered look like 16,752 dialled.
 */
export const DISCOVERY = [
  ["taken", "taken on"],
  ["foldedOntoAnother", "folded"],
  ["refusedUnstable", "refused"],
  ["excluded", "excluded"],
];

/**
 * WHERE THE CONSTRAINT IS — the first question a mirror that feels slow gets,
 * and the one the router answers itself every 60 seconds, in a line that used
 * to reach only a container's stderr.
 *
 * The four states are not degrees of one thing. They are different faults with
 * different fixes, so each names what to look at next.
 */
export const BOTTLENECK = {
  ingest: ["store is the limit", "Ingest's queue is full, so every download is backpressured behind it. Look at the store, not at the relays."],
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

/**
 * The gauges the constraint was decided from, plus the two ceilings a mirror
 * hits, as readings rather than strings.
 *
 * BOTH halves of a pair or neither. The relay copies these member by member, so
 * a document can carry a ceiling with no reading — and a percentage of an
 * absent numerator renders `NaN%`, which reads as a broken page rather than as
 * a missing number.
 */
export function gaugesOf(health) {
  if (!health) return [];
  const out = [];
  if (health.eventsPerSec != null) out.push({ id: "eventsPerSec", value: health.eventsPerSec, tone: null });
  if (health.heapMaxMb && health.heapUsedMb != null) {
    const share = health.heapUsedMb / health.heapMaxMb;
    out.push({ id: "heap", value: health.heapUsedMb, of: health.heapMaxMb, pct: Math.round(share * 100),
               tone: share >= HEAP_TIGHT ? "warn" : null });
  }
  if (health.socketCeiling && health.sockets != null) {
    out.push({ id: "sockets", value: health.sockets, of: health.socketCeiling,
               tone: health.sockets >= health.socketCeiling ? "warn" : null });
  }
  // Deliberate slowness, named as such: ingest yields when the relay's own
  // reads get slow, on purpose, and from throughput alone that is identical to
  // being stuck.
  if (health.servingMs != null) out.push({ id: "servingMs", value: health.servingMs, tone: null });
  return out;
}

/**
 * One partition's segments — the members that are non-zero, with the share each
 * takes of a whole that is PUBLISHED rather than summed.
 *
 * The total is never assumed to be the sum: it is its own member, and when the
 * two disagree the card says so rather than rescaling to hide it.
 */
export function partitionOf(rows, counts, total) {
  const whole = Math.max(1, total || 0);
  return rows
    .map(([member, label]) => ({ member, label, n: (counts || {})[member] || 0 }))
    .filter((s) => s.n > 0)
    .map((s) => ({ ...s, share: s.n / whole }));
}

/**
 * Does the url partition actually divide?
 *
 * On a stream whose relay list is named by hand nothing folds, nothing is
 * refused and nothing is excluded, so the bar is one full-width segment reading
 * "5 taken on" — a bar that cannot vary is not a mark, it is a rule with a
 * number on it, and the count is already on the pass footer.
 */
export function dividesOn(urls) {
  return !!urls?.discovered && DISCOVERY.filter(([m]) => urls[m]).length > 1;
}

/**
 * What `pending` means for THIS pass.
 *
 * It is `taken` minus the nine other outcomes, so it always closes the
 * partition — but the same number means different things depending on whether
 * anything is still RUNNING, and the pass's outcome alone cannot say. A live
 * run showed the naive reading lying: a `completed` pass called 285 urls "never
 * got a verdict" directly above three of those 285 downloading at 20,000 events
 * each. A pass ends when its last url is HANDED OUT, not when its last worker
 * returns.
 */
export function pendingMeaning(outcome, held) {
  if (outcome === "running" || outcome == null) return null;
  return held > 0
    ? "the pass finished handing out and these legs are still running — the rotation working, not a fault"
    : "the cycle ended before these got a verdict; dialled again next cycle";
}

/** A pass's own `pending` — its urls with no verdict yet, which on a finished walk is its tail. */
export function passHeld(pass) {
  return (pass?.taken || {}).pending || 0;
}

/**
 * Which walk this is and what it is doing, as parts rather than a sentence.
 *
 * `number` is a string because a pass nothing numbered is `?` — running it
 * through a number formatter produced the literal `NaN` on the one label whose
 * whole job is to say which walk a bar belongs to.
 */
export function passLabelOf(pass) {
  const outstanding = passHeld(pass);
  const state =
    pass?.outcome === "running" ? "walking"
      : outstanding > 0 ? "finishing"
        : pass?.outcome || "ended";
  return {
    number: pass?.number != null ? String(pass.number) : "?",
    owner: pass?.owner && pass.owner !== "dynamic" ? pass.owner : null,
    state,
    outstanding,
  };
}

/**
 * How many relays a stream is holding, named or not — what `pending` is read
 * against.
 */
export function heldCount(inFlight) {
  if (!inFlight) return 0;
  return ((inFlight.relays || []).length) + (inFlight.omitted || 0);
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
    };
  });
  return { rows, more: (inFlight?.omitted || 0) + (all.length - rows.length) };
}

/**
 * A processor's meter: WHICH KIND it is, and how full.
 *
 * Two kinds, and they must never be read as one. `progress` is a fraction
 * completed — monotonic, full means done. `level` is occupancy — full means
 * backpressured. Drawn identically they were: a fold at 50% and an ingest queue
 * at 50% were the same picture in the same column meaning unrelated things, and
 * a full bar meant "finished" on one row and "the store is the limit" on the
 * next.
 *
 * `null` where there is no whole to be a part of. That is the honest mark for
 * three different situations and an empty meter would be a claim of zero
 * progress in all of them: a pass built and never run; the NIP-66 monitor,
 * whose `knownDead` is read from the STORE while `observed` counts this
 * process's sockets since boot, so there is no denominator at all; and the
 * upstream push, which has a total and no target.
 */
export function meterOf(p) {
  const w = (p?.streams || [])[0];
  if (w) {
    const candidates = w.candidates || 0;
    // `unmeasured` falling pass over pass is the fold getting somewhere;
    // standing still while the decided count climbs is a set of hosts that
    // cannot be decided.
    const decided = Math.max(0, candidates - (w.unmeasured || 0));
    return { kind: "progress", share: decided / Math.max(1, candidates), decided, candidates,
             left: w.unmeasured || 0, tone: null };
  }
  // Falsy rather than `!= null`: a capacity of zero is not a full queue, it is
  // no denominator, and dividing by it produced a NaN width.
  if (p?.capacity) {
    const queued = p.queued || 0;
    const full = queued >= p.capacity;
    return { kind: "level", share: Math.min(1, queued / p.capacity), queued, capacity: p.capacity,
             full, tone: full ? "warn" : null };
  }
  return null;
}
