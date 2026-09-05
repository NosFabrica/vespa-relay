// What the pulse page decides, apart from the DOM that draws it: turning a
// document of CUMULATIVE counters into the RATES an operator reads, and the
// handful of judgements ("this is the expensive activity", "this wait is
// behind that holder") that can be wrong silently.
//
// Pure, and tested in node by `pulse.test.mjs`. The page is the only caller.

/**
 * How often the page polls. Far faster than the stats page's 30s floor, and
 * it costs the relay nothing comparable: `/pulse.json` is a read of in-process
 * counters, not a rollup of Vespa queries. Two seconds is also what makes the
 * differenced rates readable — a 30s window smooths away the spike an operator
 * opened the page to see.
 */
export const POLL_MS = 2000;

/**
 * What each activity IS, in the operator's words rather than the enum's. An
 * activity the store adds and this table does not know still draws, under its
 * own name: the label is a courtesy, never a gate. `pulse.test.mjs` pins that.
 */
export const ACTIVITY_LABELS = {
  Insert: "single inserts",
  BatchInsert: "bulk ingest",
  Query: "REQ / filter reads",
  Count: "NIP-45 counts",
  Delete: "deletions",
  Snapshot: "negentropy snapshots",
  Drain: "trust drain",
  Reconcile: "trust reconcile",
  Sweep: "sweeps",
  GuardRefresh: "guard refresh",
  Backfill: "max_rank backfill",
  Other: "everything else",
};

/**
 * The admission outcomes, in the order an operator reads them: what got in
 * first, then the reasons it did not, commonest first. A reason the store adds
 * sorts after these under its own name rather than vanishing.
 */
export const OUTCOME_ORDER = ["admitted", "duplicate", "replaced", "deleted", "expired", "vanished", "blocked", "unstorable", "failed"];

/**
 * Past this share of the store's time, one activity is called out rather than
 * merely listed. Strictly past: an even two-way split is exactly 0.5 and has
 * no dominant half, so naming one there would be a sentence the table
 * contradicts.
 */
export const DOMINANT_SHARE = 0.5;

/**
 * Past this, a port is talking to the engine more than the work justifies —
 * the store's own contract ("never ingest in a loop over insert()") in a
 * number. One call per document is the floor for a read that returns what it
 * asked for; two is a probe plus a write.
 */
export const CHATTY_CALLS_PER_DOC = 4;

/**
 * The window between two documents, in seconds, or null when there is no
 * usable one.
 *
 * MEASURED ON THE SERVER'S OWN CLOCK (`uptimeSeconds`), not the browser's: a
 * reader whose clock is minutes off would otherwise see every rate scaled by
 * the error. Uptime going backwards means the process restarted, and every
 * counter with it — the baseline is dropped rather than differenced into a
 * huge negative rate.
 */
export function windowOf(now, prev) {
  if (!prev || !now) return null;
  const secs = now.uptimeSeconds - prev.uptimeSeconds;
  if (!(secs > 0)) return null;
  return secs;
}

/** `now - prev` over the window, or null when there is no baseline to difference. */
export const rateOf = (now, prev, secs) => (secs == null || prev == null ? null : Math.max(0, now - prev) / secs);

/**
 * The activity table: one row per activity, with its share of the process's
 * total port time and the rate its calls are arriving at.
 *
 * `share` is of PORT TIME, which is the store's own wall time inside the
 * engine calls — not of the engine's internal time, which is a different
 * quantity published separately. Busiest first.
 */
export function activityRowsOf(doc, prev) {
  const rows = doc.activities || [];
  const secs = windowOf(doc, prev);
  const was = new Map((prev?.activities || []).map((a) => [a.activity, a]));
  const total = rows.reduce((sum, a) => sum + a.ms, 0);
  return rows.map((a) => {
    const before = was.get(a.activity);
    return {
      activity: a.activity,
      label: ACTIVITY_LABELS[a.activity] || a.activity,
      calls: a.calls,
      ms: a.ms,
      docs: a.docs,
      // A process that has done nothing has no shares, not shares of zero.
      share: total > 0 ? a.ms / total : null,
      callsPerSec: rateOf(a.calls, before?.calls, secs),
      docsPerSec: rateOf(a.docs, before?.docs, secs),
      ports: (a.ports || []).map((p) => ({
        ...p,
        chatty: p.docs > 0 && p.callsPerDoc >= CHATTY_CALLS_PER_DOC,
        callsPerSec: rateOf(p.calls, (before?.ports || []).find((q) => q.call === p.call)?.calls, secs),
      })),
    };
  });
}

/** The one activity taking most of the port time, or null when none dominates. */
export function dominantOf(rows) {
  const top = rows.find((r) => r.share != null && r.share > DOMINANT_SHARE);
  return top || null;
}

/**
 * The slowest read shape on the page: the highest p99 across every measured
 * port, with the activity it belongs to. Null when nothing has a histogram —
 * which is not the same as nothing being slow, and the page says so.
 */
export function slowestOf(doc) {
  let worst = null;
  for (const a of doc.activities || []) {
    for (const p of a.ports || []) {
      if (p.p99Ms == null) continue;
      if (!worst || p.p99Ms > worst.p99Ms) worst = { activity: a.activity, label: ACTIVITY_LABELS[a.activity] || a.activity, call: p.call, p99Ms: p.p99Ms, p50Ms: p.p50Ms, measured: p.measured };
    }
  }
  return worst;
}

/**
 * Admission, as the share it is. `admitted / offered` is the number that tells
 * an operator to narrow a sync; the reasons under it say which sync.
 *
 * Returns null when nothing has been offered: a fresh process has no admission
 * rate, and drawing 0% would read as a store refusing everything.
 */
export function admissionOf(doc, prev) {
  const o = doc.outcomes;
  if (!o || !o.offered) return null;
  const secs = windowOf(doc, prev);
  return {
    admitted: o.admitted,
    offered: o.offered,
    share: o.admitted / o.offered,
    admittedPerSec: rateOf(o.admitted, prev?.outcomes?.admitted, secs),
    offeredPerSec: rateOf(o.offered, prev?.outcomes?.offered, secs),
  };
}

/**
 * One activity's outcome split as a bar: every reason, widest first within the
 * page's reading order, each with its share of that activity's offered total.
 */
export function outcomeSplitOf(row) {
  const total = row.offered || row.reasons.reduce((s, r) => s + r.events, 0);
  const rank = (reason) => {
    const at = OUTCOME_ORDER.indexOf(reason);
    return at < 0 ? OUTCOME_ORDER.length : at;
  };
  return {
    activity: row.activity,
    label: ACTIVITY_LABELS[row.activity] || row.activity,
    offered: total,
    reasons: [...row.reasons]
      .sort((a, b) => rank(a.reason) - rank(b.reason) || b.events - a.events)
      .map((r) => ({ ...r, share: total > 0 ? r.events / total : 0 })),
  };
}

/**
 * The engine's own view, per rank profile. `matched` against `served` is the
 * recall-versus-page picture: far more matched than served is a query doing
 * work the client never sees, and it is exactly what the observer gate moves.
 */
export function engineRowsOf(doc, prev) {
  const secs = windowOf(doc, prev);
  const was = new Map((prev?.engine || []).map((e) => [e.profile, e]));
  return (doc.engine || []).map((e) => {
    const before = was.get(e.profile);
    return {
      ...e,
      queriesPerSec: rateOf(e.queries, before?.queries, secs),
      meanEngineMs: e.queries > 0 ? e.engineMs / e.queries : null,
      // How much of what the engine matched ever reached a client. Null rather
      // than 0 with nothing matched: no queries and a wide-open query that
      // matched nothing are different facts.
      servedShare: e.docsMatched > 0 ? e.hitsServed / e.docsMatched : null,
      matchedPerQuery: e.queries > 0 ? e.docsMatched / e.queries : null,
    };
  });
}

/**
 * The lock picture, present tense and cumulative. `held` is what holds a store
 * mutex right now; `wait` is where the waiting went, and — the part worth the
 * page — WHO IT WAS BEHIND.
 *
 * First-holder attribution, which the page states rather than implies: over a
 * long wait the lock may change hands several times and all of that wait is
 * charged to whoever held it when the waiter arrived.
 */
export function locksOf(doc) {
  const locks = doc.locks || {};
  return {
    held: (locks.held || []).map((h) => ({ ...h, heldSec: Math.round(h.heldMs / 1000) })),
    wait: (locks.wait || []).map((w) => ({
      stage: w.stage,
      ms: w.ms,
      behind: (w.behind || []).map((b) => ({ ...b, share: w.ms > 0 ? b.ms / w.ms : 0 })),
    })),
  };
}

/**
 * The ingest stage split. A stage booked from a duration measured elsewhere —
 * a lock's wait/hold pair — carries no call count, and this keeps `calls`
 * undefined there rather than inventing a mean over a denominator that does
 * not exist.
 */
export function stageRowsOf(doc, prev) {
  const secs = windowOf(doc, prev);
  const was = new Map((prev?.stages || []).map((s) => [s.stage, s]));
  return (doc.stages || []).map((s) => {
    const msPerSec = rateOf(s.ms, was.get(s.stage)?.ms, secs);
    return {
      ...s,
      // Seconds of work per second of wall clock: 1.0 is a stage saturating one
      // thread, and above 1.0 is concurrency, not an error.
      busy: msPerSec == null ? null : msPerSec / 1000,
    };
  });
}

/**
 * What each gauge IS, in the operator's words. A gauge the store adds and this
 * table does not know still draws, under its own key.
 */
export const GAUGE_LABELS = {
  "feed.inflight": "feed operations in flight",
  "lock.held": "store mutexes held",
  "trust.pending.subjects": "subjects waiting to be re-derived",
  "trust.pending.services": "score services waiting to be re-derived",
};

/**
 * The gauges, which a reader must NEVER difference: a queue depth is not a
 * rate, and "total ever queued" answers nothing. Kept as a separate accessor
 * from every counter above so the distinction survives a refactor — and drawn
 * in their own panel for the same reason, never mixed into the rate strip.
 */
export const gaugesOf = (doc) => (doc.gauges || []).map((g) => ({ ...g, label: GAUGE_LABELS[g.gauge] || g.gauge }));

/**
 * Whether this document carries the sections that describe the relay's users
 * rather than the relay — top observers, top search terms, and a slow-read log
 * that quotes the query.
 *
 * Read from the document's own flag, not from whether the arrays are present:
 * a build that serves no client sections and a relay nobody has searched yet
 * both produce nothing, and only the flag tells them apart.
 */
export const showsClients = (doc) => doc.clientDerived === true;

/**
 * A slow read's query, with the part that is the same on every row taken off.
 *
 * Every YQL this store emits opens with the identical projection — `select id,
 * pubkey, created_at, kind, tags, content, sig, owner from event where …` — so
 * a column of them truncates to the same forty characters and tells the reader
 * nothing at all. What distinguishes one slow read from another is the
 * predicate, so that is what the column shows; the whole statement stays on
 * the row's tooltip.
 *
 * Falls back to the untouched text when there is no `where` to cut at: a shape
 * this does not recognise must still be readable, not blank.
 */
export function whereOf(yql) {
  if (!yql) return "";
  const at = yql.indexOf(" where ");
  return at < 0 ? yql : yql.slice(at + 7);
}

/**
 * A sketch row worth doubting. Space-Saving overestimates, and publishes by
 * how much; a row whose error is a large share of its own weight may not
 * belong in the list at all.
 */
export const uncertain = (hit) => hit.weight > 0 && hit.error / hit.weight > 0.5;
