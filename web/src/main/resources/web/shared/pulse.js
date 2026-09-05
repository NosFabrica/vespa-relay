// What the pulse page decides, apart from the DOM that draws it: turning a document of
// cumulative counters into the rates an operator reads, and the judgements that can be wrong
// silently. Pure; the page is the only caller.

/** How often the page polls; `/pulse.json` is a read of in-process counters, so it can be fast. */
export const POLL_MS = 2000;

/** What each activity is, in the operator's words. An unknown activity still draws under its own name. */
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
 * The admission outcomes in reading order: what got in, then the reasons it did not. An
 * unknown reason sorts after these under its own name.
 */
export const OUTCOME_ORDER = ["admitted", "duplicate", "replaced", "deleted", "expired", "vanished", "blocked", "unstorable", "failed"];

/**
 * Past this share of the store's time, one activity is called out rather than merely listed.
 * Strictly past: an even split has no dominant half.
 */
export const DOMINANT_SHARE = 0.5;

/**
 * Past this many engine calls per document, a port is talking to the engine more than the work
 * justifies.
 */
export const CHATTY_CALLS_PER_DOC = 4;

/**
 * The window between two documents in seconds, on the server's own clock (`uptimeSeconds`),
 * or null. Uptime going backwards is a restart, and the baseline is dropped with it.
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
 * The activity table, busiest first: each activity's share of the process's port time (the
 * store's wall time inside engine calls, not the engine's own time) and its call rate.
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
 * The slowest read shape on the page: the highest p99 across every measured port. Null when
 * nothing has a histogram, which is not the same as nothing being slow.
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
 * Admission as the share it is, or null when nothing has been offered: a fresh process has no
 * admission rate, and 0% would read as a store refusing everything.
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
 * The engine's own view, per rank profile. `matched` against `served` is the recall-versus-page
 * picture.
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
      // Null rather than 0 with nothing matched: no queries and a query that matched nothing differ.
      servedShare: e.docsMatched > 0 ? e.hitsServed / e.docsMatched : null,
      matchedPerQuery: e.queries > 0 ? e.docsMatched / e.queries : null,
    };
  });
}

/**
 * The lock picture: `held` is what holds a store mutex now; `wait` is where the waiting went
 * and who it was behind. A wait is charged to whoever held the lock when the waiter arrived.
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
 * The ingest stage split. A stage booked from a lock's wait/hold pair carries no call count,
 * so `calls` stays undefined there rather than a mean over nothing.
 */
export function stageRowsOf(doc, prev) {
  const secs = windowOf(doc, prev);
  const was = new Map((prev?.stages || []).map((s) => [s.stage, s]));
  return (doc.stages || []).map((s) => {
    const msPerSec = rateOf(s.ms, was.get(s.stage)?.ms, secs);
    return {
      ...s,
      // Seconds of work per second of wall clock: above 1.0 is concurrency, not an error.
      busy: msPerSec == null ? null : msPerSec / 1000,
    };
  });
}

/** What each gauge is, in the operator's words. An unknown gauge still draws under its own key. */
export const GAUGE_LABELS = {
  "feed.inflight": "feed operations in flight",
  "lock.held": "store mutexes held",
  "trust.pending.subjects": "subjects waiting to be re-derived",
  "trust.pending.services": "score services waiting to be re-derived",
};

/**
 * The gauges, which a reader must never difference: a queue depth is not a rate. Their own
 * accessor and their own panel, so the distinction survives a refactor.
 */
export const gaugesOf = (doc) => (doc.gauges || []).map((g) => ({ ...g, label: GAUGE_LABELS[g.gauge] || g.gauge }));

/**
 * Whether this document carries the sections that describe the relay's users. Read from the
 * document's own flag, not from whether the arrays are present: an unsearched relay is empty too.
 */
export const showsClients = (doc) => doc.clientDerived === true;

/**
 * A slow read's predicate: every YQL opens with the same projection, so the column shows what
 * follows `where`. Untouched when there is no `where` to cut at.
 */
export function whereOf(yql) {
  if (!yql) return "";
  const at = yql.indexOf(" where ");
  return at < 0 ? yql : yql.slice(at + 7);
}

/** A sketch row worth doubting: Space-Saving overestimates, and publishes by how much. */
export const uncertain = (hit) => hit.weight > 0 && hit.error / hit.weight > 0.5;
